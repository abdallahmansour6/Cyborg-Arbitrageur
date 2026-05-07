"""
Synchronized Smart Slicing executor.

Pure-ish module. Operates on the engine's RAM order book caches and dispatches
orders through CCXT exchange handles passed in by the engine. Holds no
long-lived state of its own — the engine owns state; this module owns logic.

Unit discipline: ALL state in this module operates strictly in BASE TOKENS
(true 1x of the underlying coin). Per-leg native conversions happen exactly
twice per cycle — at the L2-walk and at the IOC dispatch — and once on each
receipt. Cross-leg basis math is in per-1x-base price units, normalized via
ExecutionLeg.to_base_price so multiplier-prefixed venues compare cleanly.
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from primitives import ExecutionLeg, ExecutionPair
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
BINARY_SEARCH_ITERATIONS = 20


@dataclass
class SliceQuote:
    """One synchronized slice, post-discount, ready for IOC dispatch.

    Sizes are in BASE tokens (the universal accounting unit). Limit prices
    stay in venue-native units because the IOC must be priced as the
    matching engine sees it.
    """
    size_base: float             # base qty actually dispatched (= safe_size_base * DEPTH_DISCOUNT)
    safe_size_base: float        # pre-discount binary-search ceiling — the largest S (in base)
                                 # that satisfied the basis floor on the snapshot. Exposed so the
                                 # operator can see the DEPTH_DISCOUNT haircut delta in logs
                                 # and use it to tune the discount factor over time.
    limit_long_native: float     # IOC limit for long leg (deepest level walked at safe_size_base),
                                 # in venue-native price units
    limit_short_native: float    # IOC limit for short leg (deepest level walked at safe_size_base),
                                 # in venue-native price units
    projected_basis_bps: float   # basis at safe_size_base (worst-case fill if both IOCs walked
                                 # all the way down to the limits — actual realized basis is
                                 # usually tighter since fills typically stop before the limit)
    mid_price_base: float        # 4-point reference mid in per-1x-base price units (log readability)


@dataclass
class RecoveryFill:
    """Result of imbalance recovery on the lagging leg."""
    leg: str           # "long" | "short" — which leg got recovered
    base_qty: float    # base tokens filled by the recovery market order
    vwap_base: float   # average fill price in per-1x-base units


@dataclass
class LoopResult:
    """Aggregate output of run_slicing_loop. Quantities in base; prices per-1x-base."""
    filled_base: float          # symmetric (delta-neutral) qty added to position
    halt_reason: str            # "target" | "deadline" | "aborted" | "dust"
    qty_long_base: float        # cumulative base filled on long leg (incl. recovery)
    qty_short_base: float       # cumulative base filled on short leg (incl. recovery)
    vwap_long_base: float       # qty-weighted VWAP for long leg in per-1x-base
    vwap_short_base: float      # qty-weighted VWAP for short leg in per-1x-base
    realized_basis_bps: float   # signed by side; entry: (vwap_S-vwap_L); exit: (vwap_L-vwap_S)


# ---------- Helpers ----------

def _fill_vwap(receipt) -> float:
    """
    Average fill price from a CCXT receipt with two fallbacks. Some venues
    populate `average`; others only `cost`+`filled`; some require walking the
    trades list. Returns 0.0 only if no price data is recoverable.

    Returned value is in venue-native price units. Callers normalize to
    per-1x-base via ExecutionLeg.to_base_price before cross-leg math.
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

    Operates in venue-native units throughout — this is the one place that
    intentionally stays native, because L2 levels are native by definition.
    Callers convert base<->native at the boundary.

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
    pair: ExecutionPair,
    basis_floor_bps: float,
    remaining_qty_base: float,
    side: str,  # "entry" | "exit"
) -> Optional[SliceQuote]:
    """
    Binary-searches for the maximum slice S (in BASE tokens) in
    [0, remaining_qty_base] such that the projected Net Basis at S still
    satisfies the floor. All cross-leg arithmetic is in per-1x-base price
    units, normalized via leg.to_base_price — multiplier-prefixed venues
    are made unit-coherent here:

        entry: B = (VWAP_short_bid_base - VWAP_long_ask_base)  / mid_price_base >= floor
        exit:  B = (VWAP_long_bid_base  - VWAP_short_ask_base) / mid_price_base >= floor

    mid_price_base is the four-point average of both books' top of book in
    per-1x-base — stable against single-sided transient skew.

    Limit prices on the returned SliceQuote are the deepest levels touched
    at the locked S (pre-discount), in venue-native units. The matching
    engine will reject any fill beyond those prices, physically enforcing
    the basis floor. DEPTH_DISCOUNT haircut applies to dispatched size only,
    not to limits.

    Returns None if no positive S satisfies the floor.
    """
    long_bids = book_long.get("bids") or []
    long_asks = book_long.get("asks") or []
    short_bids = book_short.get("bids") or []
    short_asks = book_short.get("asks") or []

    if not (long_bids and long_asks and short_bids and short_asks):
        return None

    # Top-of-book in per-1x-base price units (cross-leg arithmetic must agree on units)
    long_best_bid_base = pair.long.to_base_price(float(long_bids[0][0]))
    long_best_ask_base = pair.long.to_base_price(float(long_asks[0][0]))
    short_best_bid_base = pair.short.to_base_price(float(short_bids[0][0]))
    short_best_ask_base = pair.short.to_base_price(float(short_asks[0][0]))
    mid_price_base = (long_best_bid_base + long_best_ask_base + short_best_bid_base + short_best_ask_base) / 4.0
    if mid_price_base <= 0:
        return None

    if side == "entry":
        long_levels = long_asks   # long buys into asks (cost)
        short_levels = short_bids  # short sells into bids (revenue)
    elif side == "exit":
        long_levels = long_bids   # long sells into bids (revenue)
        short_levels = short_asks  # short buys at asks (cost)
    else:
        raise ValueError(f"side must be 'entry' or 'exit', got {side!r}")

    # Cap the search at what's actually consumable on the thinner book, in BASE.
    # Each leg's L2 size totals are native; convert to base via the leg.
    total_long_base = sum(float(l[1]) for l in long_levels) * pair.long.base_per_native
    total_short_base = sum(float(l[1]) for l in short_levels) * pair.short.base_per_native
    hi_base = min(remaining_qty_base, total_long_base, total_short_base)
    if hi_base <= 0:
        return None

    floor_frac = basis_floor_bps / 10000.0

    def basis_at(s_base: float):
        # Convert the base-target into each leg's native walk, then renormalize
        # the resulting VWAPs to per-1x-base for the cross-leg subtraction.
        s_long_native = pair.long.to_native_qty(s_base)
        s_short_native = pair.short.to_native_qty(s_base)
        l_vwap_native, l_deep_native, l_ach_native = vwap_by_depth(long_levels, s_long_native)
        s_vwap_native, s_deep_native, s_ach_native = vwap_by_depth(short_levels, s_short_native)
        if l_ach_native <= 0 or s_ach_native <= 0:
            return None, None, None
        l_vwap_base = pair.long.to_base_price(l_vwap_native)
        s_vwap_base = pair.short.to_base_price(s_vwap_native)
        if side == "entry":
            basis = (s_vwap_base - l_vwap_base) / mid_price_base
        else:
            basis = (l_vwap_base - s_vwap_base) / mid_price_base
        return basis, l_deep_native, s_deep_native

    # Binary search: lo_base is always a passing S (in base); hi_search is always failing
    # (or the initial cap)
    lo_base = 0.0
    hi_search = hi_base
    for _ in range(BINARY_SEARCH_ITERATIONS):
        if hi_search - lo_base < 1e-12:
            break
        midpoint_base = (lo_base + hi_search) / 2.0
        basis, _, _ = basis_at(midpoint_base)
        if basis is not None and basis >= floor_frac:
            lo_base = midpoint_base
        else:
            hi_search = midpoint_base

    if lo_base <= 0:
        return None

    # Re-evaluate at the locked S to capture deepest-level prices
    final_basis, deep_long_native, deep_short_native = basis_at(lo_base)
    if final_basis is None or final_basis < floor_frac:
        return None

    dispatch_size_base = lo_base * DEPTH_DISCOUNT
    if dispatch_size_base <= 0:
        return None

    return SliceQuote(
        size_base=dispatch_size_base,
        safe_size_base=lo_base,
        limit_long_native=deep_long_native,
        limit_short_native=deep_short_native,
        projected_basis_bps=final_basis * 10000.0,
        mid_price_base=mid_price_base,
    )


# ---------- IOC Dispatch ----------

async def dispatch_ioc_pair(
    engine,
    pair: ExecutionPair,
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
    if side == "entry":
        long_side, short_side = "buy", "sell"
        reduce_only = False
    elif side == "exit":
        long_side, short_side = "sell", "buy"
        reduce_only = True
    else:
        raise ValueError(f"side must be 'entry' or 'exit', got {side!r}")

    # Convert base qty -> native qty per leg, then apply CCXT precision
    qty_long_native = engine._to_native_qty(pair.long, quote.size_base)
    qty_short_native = engine._to_native_qty(pair.short, quote.size_base)

    if qty_long_native <= 0 or qty_short_native <= 0:
        log(
            f"Slice {quote.size_base:.8f} (base) rounds to sub-lot "
            f"(long_native={qty_long_native}, short_native={qty_short_native}). Skipping cycle.",
            "SLICE",
        )
        return None, None

    params = {"timeInForce": "IOC"}
    if reduce_only:
        params["reduceOnly"] = True

    ex_long = engine.exchanges[pair.long.exchange]
    ex_short = engine.exchanges[pair.short.exchange]

    results = await asyncio.gather(
        ex_long.create_order(
            pair.long.symbol, "limit", long_side,
            qty_long_native,
            quote.limit_long_native,
            params=dict(params),
        ),
        ex_short.create_order(
            pair.short.symbol, "limit", short_side,
            qty_short_native,
            quote.limit_short_native,
            params=dict(params),
        ),
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
    pair: ExecutionPair,
    base_filled_long: float,
    base_filled_short: float,
    side: str,
) -> Optional[RecoveryFill]:
    """
    Diff fills (in BASE tokens). If asymmetric beyond the dust floor, fire an
    uncapped market order on the lagging leg to restore delta-neutrality.
    Bypasses basis gating — neutrality > marginal cost on a fractional remainder.

    Returns RecoveryFill (leg, base_qty, vwap_base) on dispatch, None if asymmetry
    is below dust / zero. Re-raises on dispatch failure; engine fires Pushover P2.
    """
    delta_base = base_filled_long - base_filled_short
    dust_base = engine._pair_dust(pair)
    if abs(delta_base) < dust_base:
        return None

    # Direction map:
    #   entry: long BUY, short SELL
    #     delta>0 (long ahead) -> short SELL more
    #     delta<0 (short ahead) -> long BUY more
    #   exit:  long SELL, short BUY (reduceOnly)
    #     delta>0 (long ahead) -> short BUY more
    #     delta<0 (short ahead) -> long SELL more
    if delta_base > 0:
        target_leg: ExecutionLeg = pair.short
        target_name = "short"
        market_side = "sell" if side == "entry" else "buy"
    else:
        target_leg = pair.long
        target_name = "long"
        market_side = "buy" if side == "entry" else "sell"

    abs_delta_base = abs(delta_base)
    qty_native = engine._to_native_qty(target_leg, abs_delta_base)
    if qty_native <= 0:
        # Normalized down to zero (precision rounding) — treat as untradeable
        return None

    params = {"reduceOnly": True} if side == "exit" else {}
    ex = engine.exchanges[target_leg.exchange]

    try:
        receipt = await ex.create_market_order(target_leg.symbol, market_side, qty_native, params=params)
    except Exception as e:
        log(f"Recovery FAILED on {target_leg.exchange}:{target_leg.symbol}: {e}", "CRITICAL")
        raise

    fill_qty_base = target_leg.to_base_qty(receipt.get("filled"))
    fill_vwap_native = _fill_vwap(receipt)
    fill_vwap_base = target_leg.to_base_price(fill_vwap_native) if fill_vwap_native > 0 else 0.0

    log(
        f"Recovery: {market_side} {qty_native} {target_leg.symbol} on {target_leg.exchange} "
        f"@native={fill_vwap_native:.10f} @base={fill_vwap_base:.10f} "
        f"(filled_base={fill_qty_base:.8f}, delta_base={delta_base:.8f}, side={side})",
        "RECOVERY",
    )

    return RecoveryFill(leg=target_name, base_qty=fill_qty_base, vwap_base=fill_vwap_base)


# ---------- Main loop ----------

async def run_slicing_loop(
    engine,
    pair: ExecutionPair,
    target_qty_base: float,
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
      5. leg.to_base_qty + _fill_vwap (then leg.to_base_price) on each receipt
      6. recover_imbalance — restores neutrality on asymmetric fill (RecoveryFill | None)
      7. Accumulate qty + notional per leg in BASE units (incl. recovery contribution);
         compute per-cycle realized basis and log it live
      8. SLICE_COOLDOWN_S yield to let MMs replenish

    Halts gracefully on any of: filled_total >= target_qty_base, deadline expires,
    remaining drops below dust, or abort_event fires. Abort is checked at
    cycle boundaries only — dispatch+recovery is atomic, never interruptible
    mid-flight. This guarantees neutrality on halt.

    Returns a LoopResult with cumulative VWAPs (per-1x-base), realized basis,
    and halt reason. A structural failure raises (engine fires Pushover P2).
    """
    deadline = time.monotonic() + max_duration_s
    filled_total = 0.0
    halt_reason: Optional[str] = None

    # Cumulative accumulators — BASE quantity, per-1x-base notional
    cumulative_notional_long_base = 0.0
    cumulative_notional_short_base = 0.0
    cumulative_qty_long_base = 0.0
    cumulative_qty_short_base = 0.0

    dust_base = engine._pair_dust(pair)

    log(
        f"Slicing loop START side={side} pair={pair.key} "
        f"long={pair.long.exchange}:{pair.long.symbol} short={pair.short.exchange}:{pair.short.symbol} "
        f"target_base={target_qty_base} basis_floor={basis_floor_bps}bps "
        f"duration={max_duration_s}s dust_base={dust_base}",
        "SLICE",
    )

    while filled_total < target_qty_base and time.monotonic() < deadline:
        # Cycle-boundary abort check (cheap path before any I/O)
        if abort_event is not None and abort_event.is_set():
            halt_reason = "aborted"
            break

        remaining_base = target_qty_base - filled_total
        if remaining_base < dust_base:
            halt_reason = "dust"
            break

        book_long = engine.order_books.get((pair.long.exchange, pair.long.symbol))
        book_short = engine.order_books.get((pair.short.exchange, pair.short.symbol))
        if not book_long or not book_short:
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        quote = project_slice(book_long, book_short, pair, basis_floor_bps, remaining_base, side)
        if quote is None:
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        # Pre-dispatch telemetry: market context first, then engine verdict.
        #
        # All bid/ask prints are normalized to per-1x-base via leg.to_base_price
        # so symmetric and asymmetric pairs read uniformly. Spread bps is
        # multiplier-invariant (ratio cancels) so stays as-is.
        #
        # raw_basis is the basis at top-of-book with no depth walking. Comparing
        # it to projected_basis (basis at safe_ceiling_base, post-walk) tells the
        # operator whether the slice is firing against free top-of-book spread
        # (raw ≈ projected) or chasing depth into the book (projected << raw, the
        # binary search walked deeper to find a larger safe S).
        #
        # The dispatch_base / safe_ceiling_base (haircut=×DEPTH_DISCOUNT) format
        # on the SLICE line surfaces the haircut explicitly. Compare dispatch_base
        # (what we sent) against the post-dispatch 'filled long=…@… short=…@…'
        # line: if filled qty consistently lands at dispatch_base, DEPTH_DISCOUNT
        # is conservative and could be raised; if it lands well below, the haircut
        # is absorbing real phantom liquidity.
        long_bid_native = float(book_long["bids"][0][0])
        long_ask_native = float(book_long["asks"][0][0])
        short_bid_native = float(book_short["bids"][0][0])
        short_ask_native = float(book_short["asks"][0][0])
        long_bid_base = pair.long.to_base_price(long_bid_native)
        long_ask_base = pair.long.to_base_price(long_ask_native)
        short_bid_base = pair.short.to_base_price(short_bid_native)
        short_ask_base = pair.short.to_base_price(short_ask_native)
        long_spread_bps = (long_ask_base - long_bid_base) / ((long_ask_base + long_bid_base) / 2.0) * 10000.0
        short_spread_bps = (short_ask_base - short_bid_base) / ((short_ask_base + short_bid_base) / 2.0) * 10000.0
        if side == "entry":
            raw_basis_bps = (short_bid_base - long_ask_base) / quote.mid_price_base * 10000.0
        else:
            raw_basis_bps = (long_bid_base - short_ask_base) / quote.mid_price_base * 10000.0

        log(
            f"LONG:  bid={long_bid_base:.10f} ask={long_ask_base:.10f} (spread={long_spread_bps:.2f}bps)  "
            f"SHORT: bid={short_bid_base:.10f} ask={short_ask_base:.10f} (spread={short_spread_bps:.2f}bps)  "
            f"raw_basis={raw_basis_bps:.2f}bps",
            "BOOK",
        )
        log(
            f"IOC slice dispatch_base={quote.size_base:.8f} safe_ceiling_base={quote.safe_size_base:.8f} "
            f"(haircut=×{DEPTH_DISCOUNT}) "
            f"limits long_native={quote.limit_long_native} short_native={quote.limit_short_native} "
            f"projected_basis={quote.projected_basis_bps:.2f}bps",
            "SLICE",
        )

        # Dispatch + recovery is the atomic unit — abort is NOT honored here.
        # Raises on transient errors (engine halts + alerts).
        receipt_long, receipt_short = await dispatch_ioc_pair(engine, pair, quote, side)

        # Sub-lot slice — dispatch was skipped, no fill to account for.
        # Same handling as quote=None: brief idle, retry next cycle.
        if receipt_long is None or receipt_short is None:
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        base_filled_long = pair.long.to_base_qty(receipt_long.get("filled"))
        base_filled_short = pair.short.to_base_qty(receipt_short.get("filled"))
        vwap_long_native = _fill_vwap(receipt_long) if base_filled_long > 0 else 0.0
        vwap_short_native = _fill_vwap(receipt_short) if base_filled_short > 0 else 0.0
        vwap_long_base = pair.long.to_base_price(vwap_long_native) if vwap_long_native > 0 else 0.0
        vwap_short_base = pair.short.to_base_price(vwap_short_native) if vwap_short_native > 0 else 0.0

        recovery = await recover_imbalance(engine, pair, base_filled_long, base_filled_short, side)

        # Per-cycle leg totals (IOC + any recovery contribution to the lagging leg)
        cycle_qty_long_base = base_filled_long
        cycle_qty_short_base = base_filled_short
        cycle_notional_long_base = vwap_long_base * base_filled_long
        cycle_notional_short_base = vwap_short_base * base_filled_short

        if recovery is not None:
            if recovery.leg == "long":
                cycle_qty_long_base += recovery.base_qty
                cycle_notional_long_base += recovery.vwap_base * recovery.base_qty
            else:
                cycle_qty_short_base += recovery.base_qty
                cycle_notional_short_base += recovery.vwap_base * recovery.base_qty

            # Recovery underfill check — exchange-side asymmetric residual is invisible
            # to position state. Loud-log it so the operator can reconcile manually.
            expected_recovery = abs(base_filled_long - base_filled_short)
            if abs(recovery.base_qty - expected_recovery) > dust_base:
                log(
                    f"Recovery UNDERFILL: requested {expected_recovery:.8f} got {recovery.base_qty:.8f}. "
                    f"Asymmetric exchange-side residual {expected_recovery - recovery.base_qty:.8f} base.",
                    "WARNING",
                )

        # Symmetric (delta-neutral) contribution to the position
        cycle_filled = min(cycle_qty_long_base, cycle_qty_short_base)
        filled_total += cycle_filled

        cumulative_notional_long_base += cycle_notional_long_base
        cumulative_notional_short_base += cycle_notional_short_base
        cumulative_qty_long_base += cycle_qty_long_base
        cumulative_qty_short_base += cycle_qty_short_base

        # Per-cycle realized basis (post-recovery combined VWAPs, per-1x-base)
        if cycle_qty_long_base > 0 and cycle_qty_short_base > 0:
            cycle_vwap_long_base = cycle_notional_long_base / cycle_qty_long_base
            cycle_vwap_short_base = cycle_notional_short_base / cycle_qty_short_base
            reference_price_base = (cycle_vwap_long_base + cycle_vwap_short_base) / 2.0
            if side == "entry":
                cycle_basis_bps = (cycle_vwap_short_base - cycle_vwap_long_base) / reference_price_base * 10000.0
            else:
                cycle_basis_bps = (cycle_vwap_long_base - cycle_vwap_short_base) / reference_price_base * 10000.0
            log(
                f"filled long={cycle_qty_long_base:.8f}@{cycle_vwap_long_base:.10f} "
                f"short={cycle_qty_short_base:.8f}@{cycle_vwap_short_base:.10f} "
                f"realized_basis={cycle_basis_bps:.2f}bps "
                f"recovered={recovery is not None} "
                f"cumulative={filled_total:.8f}/{target_qty_base}",
                "SLICE",
            )
        else:
            log(
                f"filled 0/0 (IOCs rejected entirely) cumulative={filled_total:.8f}/{target_qty_base}",
                "SLICE",
            )

        if await _wait_or_abort(SLICE_COOLDOWN_S, abort_event):
            halt_reason = "aborted"
            break

    if halt_reason is None:
        halt_reason = "target" if filled_total >= target_qty_base else "deadline"

    cumulative_vwap_long_base = (
        cumulative_notional_long_base / cumulative_qty_long_base
        if cumulative_qty_long_base > 0 else 0.0
    )
    cumulative_vwap_short_base = (
        cumulative_notional_short_base / cumulative_qty_short_base
        if cumulative_qty_short_base > 0 else 0.0
    )
    if cumulative_vwap_long_base > 0 and cumulative_vwap_short_base > 0:
        cumulative_reference_price_base = (cumulative_vwap_long_base + cumulative_vwap_short_base) / 2.0
        if side == "entry":
            cumulative_basis_bps = (
                (cumulative_vwap_short_base - cumulative_vwap_long_base)
                / cumulative_reference_price_base * 10000.0
            )
        else:
            cumulative_basis_bps = (
                (cumulative_vwap_long_base - cumulative_vwap_short_base)
                / cumulative_reference_price_base * 10000.0
            )
    else:
        cumulative_basis_bps = 0.0

    log(
        f"Slicing loop END filled={filled_total:.8f}/{target_qty_base} halt_reason={halt_reason} "
        f"cumulative_vwap_long_base={cumulative_vwap_long_base:.10f} "
        f"cumulative_vwap_short_base={cumulative_vwap_short_base:.10f} "
        f"cumulative_realized_basis={cumulative_basis_bps:.2f}bps",
        "SLICE",
    )

    return LoopResult(
        filled_base=filled_total,
        halt_reason=halt_reason,
        qty_long_base=cumulative_qty_long_base,
        qty_short_base=cumulative_qty_short_base,
        vwap_long_base=cumulative_vwap_long_base,
        vwap_short_base=cumulative_vwap_short_base,
        realized_basis_bps=cumulative_basis_bps,
    )
