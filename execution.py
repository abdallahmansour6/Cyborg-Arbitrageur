"""
Synchronized Smart Slicing executor.

Pure-ish module. Operates on the engine's RAM order book caches and dispatches
orders through CCXT exchange handles passed in by the engine. Holds no
long-lived state of its own — the engine owns state; this module owns logic.

Unit discipline: ALL state in this module operates strictly in BASE TOKENS.
CCXT receipts return `filled` in exchange units (contracts on contract-sized
venues, base tokens on native venues). Every value coming off a receipt is
funneled through _to_base_qty before it touches loop accounting.
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from utils import log


async def _wait_or_abort(seconds: float, abort_event: Optional[asyncio.Event]) -> bool:
    """
    Sleep for `seconds`, but return early if abort_event fires.
    Returns True if aborted, False if the timeout elapsed naturally.
    """
    if abort_event is None:
        await asyncio.sleep(seconds)
        return False
    try:
        await asyncio.wait_for(abort_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


# Phantom-liquidity haircut on the theoretically-safe slice size derived from
# VWAP-by-depth. Protects against spoofed levels and microsecond stale-book lag.
DEPTH_DISCOUNT = 0.5

# Yield window between slices — lets HFT makers replenish consumed liquidity
# and lets the WS book caches drain pending deltas.
SLICE_COOLDOWN_S = 1.5

# Idle wait when books are missing or no slice satisfies the floor.
IDLE_RETRY_S = 0.20

# Binary search iteration count. ~20 halvings over [0, remaining_qty]
# resolves S to ~1e-6 of the upper bound — far below any realistic dust threshold.
BSEARCH_ITERS = 20


@dataclass
class SliceQuote:
    """One synchronized slice, post-discount, ready for IOC dispatch."""
    size: float                  # base qty actually dispatched (= safe_size * DEPTH_DISCOUNT)
    safe_size: float             # pre-discount binary-search ceiling — the largest S that
                                 # satisfied the basis floor on the snapshot. Exposed so the
                                 # operator can see the DEPTH_DISCOUNT haircut delta in logs
                                 # and use it to tune the discount factor over time.
    limit_long: float            # IOC limit for the long leg (deepest level at safe_size)
    limit_short: float           # IOC limit for the short leg (deepest level at safe_size)
    projected_basis_bps: float   # basis at safe_size (the worst-case fill if both IOCs walked
                                 # all the way down to the limits — actual realized basis is
                                 # usually tighter since fills typically stop before the limit)
    p_mid: float                 # 4-point reference mid at projection time


@dataclass
class RecoveryFill:
    """Result of imbalance recovery on the lagging leg."""
    leg: str         # "long" | "short" — which leg got recovered
    base_qty: float  # base tokens filled by the recovery market order
    vwap: float      # average fill price


@dataclass
class LoopResult:
    """Aggregate output of run_slicing_loop. Quantities in base, prices in quote/base."""
    filled_base: float          # symmetric (delta-neutral) qty added to position
    halt_reason: str            # "target" | "deadline" | "aborted" | "dust"
    qty_long: float             # cumulative base filled on long leg (incl. recovery)
    qty_short: float            # cumulative base filled on short leg (incl. recovery)
    vwap_long: float            # qty-weighted VWAP for long leg
    vwap_short: float           # qty-weighted VWAP for short leg
    realized_basis_bps: float   # signed by side; entry: (vwap_S-vwap_L); exit: (vwap_L-vwap_S)


# ---------- Helpers ----------

def _to_base_qty(engine, ex_id: str, symbol: str, exchange_qty) -> float:
    """
    Inverse of engine.normalize_amount: converts CCXT receipt['filled']
    (exchange-native units, e.g. contracts on Bybit) back into base tokens.
    Loop accounting MUST always operate in base.
    """
    market = engine.exchanges[ex_id].markets[symbol]
    contract_size = market.get("contractSize") or 1
    return float(exchange_qty or 0) * contract_size


def _fill_vwap(receipt) -> float:
    """
    Average fill price from a CCXT receipt with two fallbacks. Some venues
    populate `average`; others only `cost`+`filled`; some require walking the
    trades list. Returns 0.0 only if no price data is recoverable.
    """
    avg = receipt.get("average")
    if avg is not None and float(avg) > 0:
        return float(avg)
    cost = receipt.get("cost")
    filled = receipt.get("filled")
    if cost is not None and filled and float(filled) > 0:
        return float(cost) / float(filled)
    trades = receipt.get("trades") or []
    if trades:
        total_notional = 0.0
        total_qty = 0.0
        for t in trades:
            qty = float(t.get("amount") or 0)
            price = float(t.get("price") or 0)
            cost_t = t.get("cost")
            total_notional += float(cost_t) if cost_t is not None else price * qty
            total_qty += qty
        if total_qty > 0:
            return total_notional / total_qty
    return 0.0


def vwap_by_depth(
    levels, target_size: float
) -> Tuple[float, float, float]:
    """
    Walks one L2 side accumulating size up to target_size.

    `levels` is oriented in execution direction:
      - For BUYING:  asks ascending  [[price, size], ...]
      - For SELLING: bids descending [[price, size], ...]

    Returns (vwap_price, deepest_consumed_price, achievable_size).
    Returns (0.0, 0.0, 0.0) if levels is empty or target_size <= 0.
    achievable_size < target_size if the book is thinner than requested
    — the caller can detect thinness and skip rather than erroring.
    """
    if not levels or target_size <= 0:
        return (0.0, 0.0, 0.0)

    accumulated_qty = 0.0
    accumulated_notional = 0.0
    deepest_price = float(levels[0][0])

    for price, size in levels:
        price = float(price)
        size = float(size)
        take = min(size, target_size - accumulated_qty)
        if take <= 0:
            break
        accumulated_notional += price * take
        accumulated_qty += take
        deepest_price = price
        if accumulated_qty >= target_size:
            break

    if accumulated_qty <= 0:
        return (0.0, 0.0, 0.0)

    vwap = accumulated_notional / accumulated_qty
    return (vwap, deepest_price, accumulated_qty)


# ---------- The Dynamic Depth Oracle ----------

def project_slice(
    book_long,
    book_short,
    basis_floor_bps: float,
    remaining_qty: float,
    side: str,  # "entry" | "exit"
) -> Optional[SliceQuote]:
    """
    Binary-searches for the maximum slice S in [0, remaining_qty] such that
    the projected Net Basis at S still satisfies the floor:

        entry: B = (VWAP_short_bid - VWAP_long_ask)  / P_mid >= floor
        exit:  B = (VWAP_long_bid  - VWAP_short_ask) / P_mid >= floor

    P_mid is the four-point average of both books' top of book — stable
    against single-sided transient skew.

    Limit prices on the returned SliceQuote are the deepest levels touched
    at the locked S (pre-discount). The matching engine will reject any
    fill beyond those prices, physically enforcing the basis floor.
    DEPTH_DISCOUNT haircut applies to dispatched size only, not to limits.

    Returns None if no positive S satisfies the floor.
    """
    long_bids = book_long.get("bids") or []
    long_asks = book_long.get("asks") or []
    short_bids = book_short.get("bids") or []
    short_asks = book_short.get("asks") or []

    if not (long_bids and long_asks and short_bids and short_asks):
        return None

    long_best_bid = float(long_bids[0][0])
    long_best_ask = float(long_asks[0][0])
    short_best_bid = float(short_bids[0][0])
    short_best_ask = float(short_asks[0][0])
    p_mid = (long_best_bid + long_best_ask + short_best_bid + short_best_ask) / 4.0
    if p_mid <= 0:
        return None

    if side == "entry":
        long_levels = long_asks   # long buys into asks (cost)
        short_levels = short_bids  # short sells into bids (revenue)
    elif side == "exit":
        long_levels = long_bids   # long sells into bids (revenue)
        short_levels = short_asks  # short buys at asks (cost)
    else:
        raise ValueError(f"side must be 'entry' or 'exit', got {side!r}")

    # Cap the search at what's actually consumable on the thinner book
    total_long = sum(float(l[1]) for l in long_levels)
    total_short = sum(float(l[1]) for l in short_levels)
    hi = min(remaining_qty, total_long, total_short)
    if hi <= 0:
        return None

    floor_frac = basis_floor_bps / 10000.0

    def basis_at(s):
        l_vwap, l_deep, l_ach = vwap_by_depth(long_levels, s)
        s_vwap, s_deep, s_ach = vwap_by_depth(short_levels, s)
        if l_ach <= 0 or s_ach <= 0:
            return None, None, None
        if side == "entry":
            b = (s_vwap - l_vwap) / p_mid
        else:
            b = (l_vwap - s_vwap) / p_mid
        return b, l_deep, s_deep

    # Binary search: lo is always a passing S; hi is always failing (or initial cap)
    lo = 0.0
    for _ in range(BSEARCH_ITERS):
        if hi - lo < 1e-12:
            break
        mid = (lo + hi) / 2.0
        b, _, _ = basis_at(mid)
        if b is not None and b >= floor_frac:
            lo = mid
        else:
            hi = mid

    if lo <= 0:
        return None

    # Re-evaluate at the locked S to capture deepest-level prices
    b_final, deep_long, deep_short = basis_at(lo)
    if b_final is None or b_final < floor_frac:
        return None

    s_dispatch = lo * DEPTH_DISCOUNT
    if s_dispatch <= 0:
        return None

    return SliceQuote(
        size=s_dispatch,
        safe_size=lo,
        limit_long=deep_long,
        limit_short=deep_short,
        projected_basis_bps=b_final * 10000.0,
        p_mid=p_mid,
    )


# ---------- IOC Dispatch ----------

async def dispatch_ioc_pair(
    engine,
    long_ex_id: str,
    short_ex_id: str,
    symbol: str,
    quote: SliceQuote,
    side: str,
):
    """
    Concurrent IOC limit dispatch. Returns (receipt_long, receipt_short).
    Returns (None, None) if the slice rounds to sub-lot on either leg —
    no order is placed; caller should treat as a skipped cycle.

    Uses gather(return_exceptions=True) so BOTH legs always run to completion
    before we decide what to do. Otherwise an orphaned coroutine could place
    an order after we've already started unwinding state. If either leg raised,
    we re-raise as a RuntimeError — the engine will fire Pushover P2 and let
    the operator reconcile (we cannot safely assume 0 fill on a failed call).
    """
    ex_long = engine.exchanges[long_ex_id]
    ex_short = engine.exchanges[short_ex_id]

    if side == "entry":
        long_side, short_side = "buy", "sell"
        reduce_only = False
    elif side == "exit":
        long_side, short_side = "sell", "buy"
        reduce_only = True
    else:
        raise ValueError(f"side must be 'entry' or 'exit', got {side!r}")

    amt_long = engine.normalize_amount(ex_long, symbol, quote.size)
    amt_short = engine.normalize_amount(ex_short, symbol, quote.size)

    if amt_long <= 0 or amt_short <= 0:
        log(
            f"Slice {quote.size:.8f} rounds to sub-lot "
            f"(L={amt_long}, S={amt_short}). Skipping cycle.",
            "SLICE",
        )
        return None, None

    params = {"timeInForce": "IOC"}
    if reduce_only:
        params["reduceOnly"] = True

    results = await asyncio.gather(
        ex_long.create_order(symbol, "limit", long_side, amt_long, quote.limit_long, params=dict(params)),
        ex_short.create_order(symbol, "limit", short_side, amt_short, quote.limit_short, params=dict(params)),
        return_exceptions=True,
    )

    receipt_long, receipt_short = results
    err_long = isinstance(receipt_long, Exception)
    err_short = isinstance(receipt_short, Exception)
    if err_long or err_short:
        log(
            f"IOC dispatch failure | long={'ERR: '+repr(receipt_long) if err_long else 'OK'} "
            f"| short={'ERR: '+repr(receipt_short) if err_short else 'OK'}",
            "CRITICAL",
        )
        raise RuntimeError(
            f"IOC dispatch failed (long_err={err_long}, short_err={err_short}). "
            f"long={receipt_long!r} short={receipt_short!r}"
        )

    return receipt_long, receipt_short


# ---------- Imbalance Recovery ----------

async def recover_imbalance(
    engine,
    long_ex_id: str,
    short_ex_id: str,
    symbol: str,
    base_filled_long: float,
    base_filled_short: float,
    side: str,
) -> Optional[RecoveryFill]:
    """
    Diff fills (in BASE tokens). If asymmetric beyond the dust floor, fire an
    uncapped market order on the lagging leg to restore delta-neutrality.
    Bypasses basis gating — neutrality > marginal cost on a fractional remainder.

    Returns RecoveryFill (leg, base_qty, vwap) on dispatch, None if asymmetry
    is below dust / zero. Re-raises on dispatch failure; engine fires Pushover P2.
    """
    delta = base_filled_long - base_filled_short
    dust = max(
        engine._min_amount_in_base(long_ex_id, symbol),
        engine._min_amount_in_base(short_ex_id, symbol),
    )
    if abs(delta) < dust:
        return None

    # Direction map:
    #   entry: long BUY, short SELL
    #     delta>0 (long ahead) -> short SELL more
    #     delta<0 (short ahead) -> long BUY more
    #   exit:  long SELL, short BUY (reduceOnly)
    #     delta>0 (long ahead) -> short BUY more
    #     delta<0 (short ahead) -> long SELL more
    if delta > 0:
        target_ex_id = short_ex_id
        target_leg = "short"
        market_side = "sell" if side == "entry" else "buy"
    else:
        target_ex_id = long_ex_id
        target_leg = "long"
        market_side = "buy" if side == "entry" else "sell"

    ex = engine.exchanges[target_ex_id]
    abs_delta_base = abs(delta)
    amt_native = engine.normalize_amount(ex, symbol, abs_delta_base)
    if amt_native <= 0:
        # Normalized down to zero (precision rounding) — treat as untradeable
        return None

    params = {"reduceOnly": True} if side == "exit" else {}

    try:
        receipt = await ex.create_market_order(symbol, market_side, amt_native, params=params)
    except Exception as e:
        log(f"Recovery FAILED on {target_ex_id}: {e}", "CRITICAL")
        raise

    fill_qty_base = _to_base_qty(engine, target_ex_id, symbol, receipt.get("filled"))
    fill_vwap = _fill_vwap(receipt)

    log(
        f"Recovery: {market_side} {amt_native} {symbol} on {target_ex_id} @{fill_vwap:.8f} "
        f"(filled_base={fill_qty_base:.8f}, delta={delta:.8f}, side={side})",
        "RECOVERY",
    )

    return RecoveryFill(leg=target_leg, base_qty=fill_qty_base, vwap=fill_vwap)


# ---------- Main loop ----------

async def run_slicing_loop(
    engine,
    symbol: str,
    long_ex_id: str,
    short_ex_id: str,
    target_qty: float,
    basis_floor_bps: float,
    max_duration_s: float,
    side: str,
    abort_event: Optional[asyncio.Event] = None,
) -> LoopResult:
    """
    The Iterative Slicing Loop.

    Each cycle:
      1. Snapshot both books from engine.order_books
      2. project_slice() -> SliceQuote | None  (binary search + basis gate + discount)
      3. None? short sleep, retry
      4. dispatch_ioc_pair (concurrent IOCs) -> receipts
      5. _to_base_qty + _fill_vwap on receipt['filled']/['average'] for each leg
      6. recover_imbalance — restores neutrality on asymmetric fill (RecoveryFill | None)
      7. Accumulate qty + notional per leg (incl. recovery contribution); compute
         per-cycle realized basis and log it live
      8. SLICE_COOLDOWN_S yield to let MMs replenish

    Halts gracefully on any of: filled_total >= target_qty, deadline expires,
    remaining drops below dust, or abort_event fires. Abort is checked at
    cycle boundaries only — dispatch+recovery is atomic, never interruptible
    mid-flight. This guarantees neutrality on halt.

    Returns a LoopResult with cumulative VWAPs, realized basis, and halt reason.
    A structural failure raises (engine fires Pushover P2).
    """
    deadline = time.monotonic() + max_duration_s
    filled_total = 0.0
    halt_reason: Optional[str] = None

    # Cumulative accumulators (base tokens / quote-currency notional)
    notional_long = 0.0
    notional_short = 0.0
    qty_long = 0.0
    qty_short = 0.0

    dust = max(
        engine._min_amount_in_base(long_ex_id, symbol),
        engine._min_amount_in_base(short_ex_id, symbol),
    )

    log(
        f"Slicing loop START side={side} symbol={symbol} target={target_qty} "
        f"floor={basis_floor_bps}bps duration={max_duration_s}s dust={dust}",
        "SLICE",
    )

    while filled_total < target_qty and time.monotonic() < deadline:
        # Cycle-boundary abort check (cheap path before any I/O)
        if abort_event is not None and abort_event.is_set():
            halt_reason = "aborted"
            break

        remaining = target_qty - filled_total
        if remaining < dust:
            halt_reason = "dust"
            break

        book_long = engine.order_books.get((long_ex_id, symbol))
        book_short = engine.order_books.get((short_ex_id, symbol))
        if not book_long or not book_short:
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        quote = project_slice(book_long, book_short, basis_floor_bps, remaining, side)
        if quote is None:
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        # Pre-dispatch telemetry: market context first, then engine verdict.
        #
        # raw_basis is the basis at top-of-book with no depth walking. Comparing
        # it to proj_basis (basis at safe_size, post-walk) tells the operator
        # whether the slice is firing against free top-of-book spread (raw ≈
        # proj) or chasing depth into the book (proj << raw, the binary search
        # walked deeper to find a larger safe S).
        #
        # The fire=post/safe (×DEPTH_DISCOUNT) format on the SLICE line surfaces
        # the haircut explicitly. Compare 'fire' (what we sent) against the
        # post-dispatch 'filled L=…@… S=…@…' line: if filled qty consistently
        # lands at fire, DEPTH_DISCOUNT is conservative and could be raised; if
        # it lands well below, the haircut is absorbing real phantom liquidity.
        l_bid = float(book_long["bids"][0][0])
        l_ask = float(book_long["asks"][0][0])
        s_bid = float(book_short["bids"][0][0])
        s_ask = float(book_short["asks"][0][0])
        l_spread_bps = (l_ask - l_bid) / ((l_ask + l_bid) / 2.0) * 10000.0
        s_spread_bps = (s_ask - s_bid) / ((s_ask + s_bid) / 2.0) * 10000.0
        if side == "entry":
            raw_basis_bps = (s_bid - l_ask) / quote.p_mid * 10000.0
        else:
            raw_basis_bps = (l_bid - s_ask) / quote.p_mid * 10000.0

        log(
            f"L: bid={l_bid:.8f} ask={l_ask:.8f} (sp={l_spread_bps:.2f}bps)  "
            f"S: bid={s_bid:.8f} ask={s_ask:.8f} (sp={s_spread_bps:.2f}bps)  "
            f"raw_basis={raw_basis_bps:.2f}bps",
            "BOOK",
        )
        log(
            f"IOC slice fire={quote.size:.8f}/{quote.safe_size:.8f} (×{DEPTH_DISCOUNT}) "
            f"limits L={quote.limit_long} S={quote.limit_short} "
            f"proj_basis={quote.projected_basis_bps:.2f}bps",
            "SLICE",
        )

        # Dispatch + recovery is the atomic unit — abort is NOT honored here.
        # Raises on transient errors (engine halts + alerts).
        receipt_long, receipt_short = await dispatch_ioc_pair(
            engine, long_ex_id, short_ex_id, symbol, quote, side
        )

        # Sub-lot slice — dispatch was skipped, no fill to account for.
        # Same handling as quote=None: brief idle, retry next cycle.
        if receipt_long is None or receipt_short is None:
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        base_filled_long = _to_base_qty(engine, long_ex_id, symbol, receipt_long.get("filled"))
        base_filled_short = _to_base_qty(engine, short_ex_id, symbol, receipt_short.get("filled"))
        vwap_long_ioc = _fill_vwap(receipt_long) if base_filled_long > 0 else 0.0
        vwap_short_ioc = _fill_vwap(receipt_short) if base_filled_short > 0 else 0.0

        recovery = await recover_imbalance(
            engine, long_ex_id, short_ex_id, symbol,
            base_filled_long, base_filled_short, side,
        )

        # Per-cycle leg totals (IOC + any recovery contribution to the lagging leg)
        cycle_qty_long = base_filled_long
        cycle_qty_short = base_filled_short
        cycle_notional_long = vwap_long_ioc * base_filled_long
        cycle_notional_short = vwap_short_ioc * base_filled_short

        if recovery is not None:
            if recovery.leg == "long":
                cycle_qty_long += recovery.base_qty
                cycle_notional_long += recovery.vwap * recovery.base_qty
            else:
                cycle_qty_short += recovery.base_qty
                cycle_notional_short += recovery.vwap * recovery.base_qty

            # Recovery underfill check — exchange-side asymmetric residual is invisible
            # to position state. Loud-log it so the operator can reconcile manually.
            expected_recovery = abs(base_filled_long - base_filled_short)
            if abs(recovery.base_qty - expected_recovery) > dust:
                log(
                    f"Recovery UNDERFILL: requested {expected_recovery:.8f} got {recovery.base_qty:.8f}. "
                    f"Asymmetric exchange-side residual {expected_recovery - recovery.base_qty:.8f} base.",
                    "WARNING",
                )

        # Symmetric (delta-neutral) contribution to the position
        cycle_filled = min(cycle_qty_long, cycle_qty_short)
        filled_total += cycle_filled

        notional_long += cycle_notional_long
        notional_short += cycle_notional_short
        qty_long += cycle_qty_long
        qty_short += cycle_qty_short

        # Per-cycle realized basis (post-recovery combined VWAPs)
        if cycle_qty_long > 0 and cycle_qty_short > 0:
            cycle_vwap_long = cycle_notional_long / cycle_qty_long
            cycle_vwap_short = cycle_notional_short / cycle_qty_short
            p_ref = (cycle_vwap_long + cycle_vwap_short) / 2.0
            if side == "entry":
                cycle_basis_bps = (cycle_vwap_short - cycle_vwap_long) / p_ref * 10000.0
            else:
                cycle_basis_bps = (cycle_vwap_long - cycle_vwap_short) / p_ref * 10000.0
            log(
                f"filled L={cycle_qty_long:.8f}@{cycle_vwap_long:.8f} "
                f"S={cycle_qty_short:.8f}@{cycle_vwap_short:.8f} "
                f"real_basis={cycle_basis_bps:.2f}bps "
                f"recovered={recovery is not None} "
                f"cum={filled_total:.8f}/{target_qty}",
                "SLICE",
            )
        else:
            log(
                f"filled 0/0 (IOCs rejected entirely) cum={filled_total:.8f}/{target_qty}",
                "SLICE",
            )

        if await _wait_or_abort(SLICE_COOLDOWN_S, abort_event):
            halt_reason = "aborted"
            break

    if halt_reason is None:
        halt_reason = "target" if filled_total >= target_qty else "deadline"

    cum_vwap_long = notional_long / qty_long if qty_long > 0 else 0.0
    cum_vwap_short = notional_short / qty_short if qty_short > 0 else 0.0
    if cum_vwap_long > 0 and cum_vwap_short > 0:
        cum_p_ref = (cum_vwap_long + cum_vwap_short) / 2.0
        if side == "entry":
            cum_basis_bps = (cum_vwap_short - cum_vwap_long) / cum_p_ref * 10000.0
        else:
            cum_basis_bps = (cum_vwap_long - cum_vwap_short) / cum_p_ref * 10000.0
    else:
        cum_basis_bps = 0.0

    log(
        f"Slicing loop END filled={filled_total:.8f}/{target_qty} halt_reason={halt_reason} "
        f"cum_vwap_L={cum_vwap_long:.8f} cum_vwap_S={cum_vwap_short:.8f} "
        f"cum_real_basis={cum_basis_bps:.2f}bps",
        "SLICE",
    )

    return LoopResult(
        filled_base=filled_total,
        halt_reason=halt_reason,
        qty_long=qty_long,
        qty_short=qty_short,
        vwap_long=cum_vwap_long,
        vwap_short=cum_vwap_short,
        realized_basis_bps=cum_basis_bps,
    )
