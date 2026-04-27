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
IDLE_RETRY_S = 0.25

# Binary search iteration count. ~20 halvings over [0, remaining_qty]
# resolves S to ~1e-6 of the upper bound — far below any realistic dust threshold.
BSEARCH_ITERS = 20


@dataclass
class SliceQuote:
    """One synchronized slice, post-discount, ready for IOC dispatch."""
    size: float                  # base qty for this slice (post-DEPTH_DISCOUNT)
    limit_long: float            # IOC limit for the long leg
    limit_short: float           # IOC limit for the short leg
    projected_basis_bps: float   # basis at the pre-discount S boundary (for logs)


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
        limit_long=deep_long,
        limit_short=deep_short,
        projected_basis_bps=b_final * 10000.0,
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
):
    """
    Diff fills (in BASE tokens). If asymmetric beyond the dust floor, fire an
    uncapped market order on the lagging leg to restore delta-neutrality.
    Bypasses basis gating — neutrality > marginal cost on a fractional remainder.

    Returns the recovery receipt or None if asymmetry is below dust / zero.
    Re-raises on dispatch failure; engine will halt + Pushover P2.
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
        market_side = "sell" if side == "entry" else "buy"
    else:
        target_ex_id = long_ex_id
        market_side = "buy" if side == "entry" else "sell"

    ex = engine.exchanges[target_ex_id]
    abs_delta_base = abs(delta)
    amt_native = engine.normalize_amount(ex, symbol, abs_delta_base)
    if amt_native <= 0:
        # Normalized down to zero (precision rounding) — treat as untradeable
        return None

    params = {"reduceOnly": True} if side == "exit" else {}

    log(
        f"Recovery: {market_side} {amt_native} {symbol} on {target_ex_id} "
        f"(delta={delta:.8f} base, side={side})",
        "RECOVERY",
    )

    try:
        receipt = await ex.create_market_order(symbol, market_side, amt_native, params=params)
        return receipt
    except Exception as e:
        log(f"Recovery FAILED on {target_ex_id}: {e}", "CRITICAL")
        raise


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
) -> Tuple[float, str]:
    """
    The Iterative Slicing Loop.

    Each cycle:
      1. Snapshot both books from engine.order_books
      2. project_slice() -> SliceQuote | None  (binary search + basis gate + discount)
      3. None? short sleep, retry
      4. dispatch_ioc_pair (concurrent IOCs) -> receipts
      5. _to_base_qty on receipt['filled'] for each leg
      6. recover_imbalance — restores neutrality on asymmetric fill
      7. Increment filled_total by the symmetric base-token contribution
      8. SLICE_COOLDOWN_S yield to let MMs replenish

    Halts gracefully on any of: filled_total >= target_qty, deadline expires,
    remaining drops below dust, or abort_event fires. Abort is checked at
    cycle boundaries only — dispatch+recovery is atomic, never interruptible
    mid-flight. This guarantees neutrality on halt.

    Returns (total_filled_base, halt_reason).
      halt_reason in {"target", "deadline", "aborted", "dust"}
    A structural failure raises (engine fires Pushover P2).
    """
    deadline = time.monotonic() + max_duration_s
    filled_total = 0.0
    halt_reason: Optional[str] = None

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

        log(
            f"IOC slice size={quote.size:.8f} "
            f"limits L={quote.limit_long} S={quote.limit_short} "
            f"projected_basis={quote.projected_basis_bps:.2f}bps",
            "SLICE",
        )

        # Dispatch + recovery is the atomic unit — abort is NOT honored here.
        # Raises on transient errors (engine halts + alerts).
        receipt_long, receipt_short = await dispatch_ioc_pair(
            engine, long_ex_id, short_ex_id, symbol, quote, side
        )

        base_filled_long = _to_base_qty(
            engine, long_ex_id, symbol, receipt_long.get("filled")
        )
        base_filled_short = _to_base_qty(
            engine, short_ex_id, symbol, receipt_short.get("filled")
        )

        recovery_receipt = await recover_imbalance(
            engine, long_ex_id, short_ex_id, symbol,
            base_filled_long, base_filled_short, side,
        )

        if recovery_receipt is not None:
            cycle_filled = max(base_filled_long, base_filled_short)
        else:
            cycle_filled = min(base_filled_long, base_filled_short)

        filled_total += cycle_filled

        log(
            f"Cycle filled={cycle_filled:.8f} cumulative={filled_total:.8f}/{target_qty} "
            f"L={base_filled_long:.8f} S={base_filled_short:.8f} "
            f"recovered={recovery_receipt is not None}",
            "SLICE",
        )

        if await _wait_or_abort(SLICE_COOLDOWN_S, abort_event):
            halt_reason = "aborted"
            break

    if halt_reason is None:
        halt_reason = "target" if filled_total >= target_qty else "deadline"

    log(
        f"Slicing loop END filled={filled_total:.8f}/{target_qty} "
        f"halt_reason={halt_reason}",
        "SLICE",
    )
    return filled_total, halt_reason
