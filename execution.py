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

Refactor R1+R2 (2026-05-09): all CCXT receipts now flow through
`receipt_resolver.resolve_receipt()` — the engine never reads
`receipt.get('filled')` directly. All order-book reads consume
`BookSnapshot` objects and pass through the liveness/sufficiency gates
before any cross-leg arithmetic.

Refactor R1.5 (2026-05-09 evening, post-smoketest): cross-leg lot-size
discipline. `_compute_symmetric_dispatch` snaps the slice's base
quantity to the largest value that survives precision-rounding
identically on both legs, before the IOCs are dispatched. Recovery's
dust threshold is now the target-leg's min-lot (not pair-dust), so
asymmetric residuals smaller than the larger leg's min but tradeable
on the smaller leg are caught and recovered. Anchor: 2026-05-09
22:13 KuCoin × OKX silent 9-XRP residual — ENGINE_FIELD_NOTES.md.

Refactor R1.6 (2026-05-10): per-cycle composite dispatch floor.
`_compute_dispatch_floor_base` combines the static lot floor with the
price-dependent notional floor and rounds the result UP to the next
symmetric-snap step. Used both as the loop's halt-on-dust threshold
and as `min_dispatch_base` for `project_slice`. Recovery's dust
threshold extended to the same composite (lot + notional + step).
Anchor: 2026-05-10 mexc × bitget — 2-XRP slice ($2.84) rejected by
both venues at the unpublished 5-USDT floor.
"""
import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Optional, Tuple

from primitives import BookSnapshot, ExecutionLeg, ExecutionPair, FillReceipt
from receipt_resolver import resolve_receipt
from utils import log, now_str
from venue_overrides import (
    ioc_limit_params_for,
    market_order_params_for,
    max_book_age_ms_for,
)


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

# Minimum depth on the consumed side of the book before a slice is allowed.
# Empirical: most venues post 20+ levels at the touch on a calm BTC sample,
# but during fast moves liquidity collapses to a handful. Five levels is the
# floor that says "the book has structure, not just a wisp at the touch."
# Tunable per-venue via venue_overrides if a specific venue diverges.
MIN_LEVELS_FOR_SLICE = 5


def _now_ms() -> int:
    """Local monotonic clock in ms. Single time source per cycle —
    callers grab once and pass to every BookSnapshot.is_fresh() check
    so two legs in the same cycle compare against the SAME wall clock."""
    return int(time.monotonic() * 1000)


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
    min_dispatch_base: float     # Composite per-cycle floor (lot + notional + snap-safe rounding)
                                 # propagated from the slicing-loop's `_compute_dispatch_floor_base`.
                                 # `dispatch_ioc_pair` uses it for a defensive post-snap notional
                                 # gate — should never trigger for sane inputs (project_slice's
                                 # bounds + snap convergence guarantee post-snap ≥ this), but the
                                 # gate catches arithmetic edge cases without venue-side rejection.


@dataclass
class RecoveryFill:
    """Result of imbalance recovery on the lagging leg."""
    leg: str           # "long" | "short" — which leg got recovered
    base_qty: float    # base tokens filled by the recovery market order
    vwap_base: float   # average fill price in per-1x-base units
    order_record: dict # pre-built order record (kind="recovery") — appended by
                       # run_slicing_loop into the cycle's order_records accumulator
                       # so engine.handle_entry/handle_exit can persist it into
                       # positions.json and ultimately into closed_trades.json for
                       # off-engine pnl.py analysis.


@dataclass
class LoopResult:
    """Aggregate output of run_slicing_loop. Quantities in base; prices per-1x-base."""
    filled_base: float          # symmetric (delta-neutral) qty added to position
    halt_reason: str            # "target" | "deadline" | "aborted" | "dust" | "asymmetric_residual"
                                #
                                # asymmetric_residual: post-recovery cycle ended with
                                # cycle_qty_long_base != cycle_qty_short_base by a
                                # tradeable amount. Engine halted to prevent further
                                # naked exposure accumulation. qty_long_base /
                                # qty_short_base capture the actual venue exposure;
                                # the difference is the naked residual the operator
                                # must reconcile manually. handle_entry / handle_exit
                                # surface this via Pushover P2.
    qty_long_base: float        # cumulative base filled on long leg (incl. recovery)
    qty_short_base: float       # cumulative base filled on short leg (incl. recovery)
    vwap_long_base: float       # qty-weighted VWAP for long leg in per-1x-base
    vwap_short_base: float      # qty-weighted VWAP for short leg in per-1x-base
    realized_basis_bps: float   # signed by side; entry: (vwap_S-vwap_L); exit: (vwap_L-vwap_S)
    order_records: list = field(default_factory=list)
                                # Per-order capture for the True PnL primitive (Phase 1).
                                # One dict per IOC + per recovery market order. Each
                                # carries: order_id, leg ("long"|"short"), kind
                                # ("ioc"|"recovery"), side ("buy"|"sell"), venue, symbol,
                                # filled_native, filled_base, vwap_native, vwap_base,
                                # fees (CCXT-extracted), ts. handle_entry/handle_exit
                                # extend positions.json[entry|exit_order_records] with
                                # this list; closed_trades.json folds them in on
                                # archival. Default factory because dataclass equality
                                # tests with mutable defaults are a footgun.


def _order_record_from_fill(
    fr: FillReceipt,
    *,
    leg_role: str,
    kind: str,
    side: str,
) -> dict:
    """Build one self-sufficient order record from a resolved FillReceipt.

    `leg_role` is the POSITION-side role ("long" | "short"), NOT the
    order-side. An entry cycle dispatches one leg_role="long" with
    side="buy" and one leg_role="short" with side="sell". A recovery
    on the lagging short leg might be leg_role="short", side="buy"
    (entry recovery) or leg_role="short", side="sell" (exit recovery).

    `kind` ∈ {"ioc", "recovery"}. The two together (`leg`, `kind`)
    are sufficient for pnl.py to attribute each fill to the correct
    side of the basis trade.

    Includes BOTH native + base fields for forensic auditing — operator
    can cross-check the venue UI's native qty/price against `filled_native`
    /`vwap_native` while pnl.py operates on `filled_base`/`vwap_base`
    directly (no re-conversion needed). `fees` is the normalized CCXT
    fee list (see `_fees_from_receipt` in receipt_resolver.py).

    `ts` is the engine's wall clock at record-build time. Sufficient
    granularity for funding-boundary attribution (funding boundaries
    are hourly+, engine ts is millisecond)."""
    return {
        "order_id": fr.order_id,
        "leg": leg_role,
        "kind": kind,
        "side": side,
        "venue": fr.leg.exchange,
        "symbol": fr.leg.symbol,
        "filled_native": fr.filled_native,
        "filled_base": fr.filled_base,
        "vwap_native": fr.vwap_native,
        "vwap_base": fr.vwap_base,
        "fees": fr.fees,
        "ts": now_str(),
    }


# ---------- Helpers ----------

# `_fill_vwap` was removed in the R1 refactor (2026-05-09). Its three-tier
# extraction logic now lives in `receipt_resolver._vwap_from_receipt`,
# called automatically during FillReceipt construction. Engine code reads
# `fill_receipt.vwap_native` / `fill_receipt.vwap_base` directly — there
# is one place that knows how to extract VWAP from a CCXT receipt.


def _gate_book(book: Optional[BookSnapshot], leg: ExecutionLeg, now_ms: int) -> Optional[str]:
    """Return None if the book passes all liveness + sufficiency gates,
    otherwise a short reason string suitable for a single-line log.

    Gates, in order:
      1. presence              — cache slot must be populated
      2. has_top_of_book       — both bid and ask sides non-empty
      3. is_fresh              — received_ts_ms within per-venue threshold
      4. is_crossed (NOT)      — best_bid < best_ask
      5. min levels            — both sides have ≥ MIN_LEVELS_FOR_SLICE entries

    Order matters: a `None` book means the watch loop popped the cache on
    a stream error, so we report 'no book' rather than crashing on an
    attribute access. has_top_of_book before the freshness check protects
    `book.bids[0][0]` reads in is_crossed downstream."""
    if book is None:
        return f"no book ({leg.exchange}:{leg.symbol}) — stream silent or reconnecting"
    if not book.has_top_of_book():
        return f"empty top-of-book ({leg.exchange}:{leg.symbol})"
    max_age = max_book_age_ms_for(leg.exchange)
    age_ms = now_ms - book.received_ts_ms
    if age_ms > max_age:
        return f"stale book ({leg.exchange}:{leg.symbol}) age={age_ms}ms > {max_age}ms"
    if book.is_crossed():
        return (
            f"crossed book ({leg.exchange}:{leg.symbol}) "
            f"bid={book.bids[0][0]} ask={book.asks[0][0]}"
        )
    if len(book.bids) < MIN_LEVELS_FOR_SLICE or len(book.asks) < MIN_LEVELS_FOR_SLICE:
        return (
            f"thin book ({leg.exchange}:{leg.symbol}) "
            f"levels={len(book.bids)}bid/{len(book.asks)}ask < {MIN_LEVELS_FOR_SLICE}"
        )
    return None


def vwap_by_depth(
    levels, target_size: float
) -> Tuple[float, float, float]:
    """
    Walks one L2 side accumulating size up to target_size.

    Operates in venue-native units throughout — this is the one place that
    intentionally stays native, because L2 levels are native by definition.
    Callers convert base<->native at the boundary.

    `levels` is oriented in execution direction:
      - For BUYING:  asks ascending  [[price, size, ...], ...]
      - For SELLING: bids descending [[price, size, ...], ...]

    Index-access (`level[0]`, `level[1]`) NOT tuple-unpack — some CCXT
    Pro adapters ship 3-element levels. MEXC swap (verified anchor:
    2026-05-09 22:15:53 mexc × bitget smoketest) yields `[price, amount,
    order_count]` per CCXT pro/mexc.py:799-811. The legacy
    `for price, size in levels` raised "too many values to unpack
    (expected 2)" and halted the entry. Index-access tolerates any
    level length ≥ 2 — applies to all 13 venues uniformly.

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

    for level in levels:
        price = float(level[0])
        size = float(level[1])
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


# ---------- Composite dispatch-floor (lot + notional + snap-safe) ----------


def _compute_dispatch_floor_base(
    engine,
    pair: ExecutionPair,
    mid_price_base: float,
) -> Tuple[float, float, float]:
    """Per-cycle dispatch floor in base tokens.

    Composes three constraints:
      1. Lot floor — `max(both legs' min_lot_base)`. Static; below this,
         no leg can place any order.
      2. Notional floor — `max(both legs' min_notional_usdt) /
         mid_price_base`. Price-dependent; below this in BASE, the venue
         will reject with InsufficientNotional even though the lot-size
         check passes. Anchor: 2026-05-10 mexc × bitget — 2-XRP slice
         (~$2.84) rejected by both venues at 5-USDT floor.
      3. Snap-safe rounding — composite floor rounded UP to the next
         multiple of `max(both legs' base step)`. Without this, the
         symmetric snap in `_compute_symmetric_dispatch` could drop the
         dispatched size below the composite floor by up to one step,
         causing venue-side notional rejection. Example: bybit XRP at
         step=0.1 with floor=$5/$1.42=3.52 base would snap 3.52 → 3.5
         and fail; ceiling to 3.6 gives one snap-safe step of margin.

    Returns `(lot_floor, dispatch_floor, notional_floor_base)`:
      * `lot_floor` — for diagnostics / SLICE START log line.
      * `dispatch_floor` — the actual operating floor (composite +
        snap-safe). Use as both the loop's halt-on-dust threshold and
        as `min_dispatch_base` arg to `project_slice`.
      * `notional_floor_base` — for diagnostic logging only.

    Returns `(lot_floor, lot_floor, 0)` if `mid_price_base <= 0`
    (degenerate book) — caller treats as "no notional gate this cycle"."""
    lot_long_base = engine._min_base_for_leg(pair.long)
    lot_short_base = engine._min_base_for_leg(pair.short)
    lot_floor = max(lot_long_base, lot_short_base)

    if mid_price_base <= 0:
        return (lot_floor, lot_floor, 0.0)

    notional_long_usdt = engine._min_notional_for_leg(pair.long)
    notional_short_usdt = engine._min_notional_for_leg(pair.short)
    notional_long_base = notional_long_usdt / mid_price_base
    notional_short_base = notional_short_usdt / mid_price_base
    notional_floor = max(notional_long_base, notional_short_base)

    composite_floor = max(lot_floor, notional_floor)

    step_long = engine._step_base_for_leg(pair.long)
    step_short = engine._step_base_for_leg(pair.short)
    max_step = max(step_long, step_short)
    if max_step > 0:
        # Round UP to next step boundary so the symmetric snap (which
        # rounds DOWN to a multiple of max_step) lands at or above
        # composite_floor. Defensive against the bybit 0.1-step snap-
        # to-3.5-base-vs-floor-3.52 edge case.
        dispatch_floor = math.ceil(composite_floor / max_step) * max_step
    else:
        dispatch_floor = composite_floor

    return (lot_floor, dispatch_floor, notional_floor)


def _mid_price_base_from_books(
    book_long: BookSnapshot,
    book_short: BookSnapshot,
    pair: ExecutionPair,
) -> float:
    """Same 4-point mid formula `project_slice` uses internally — extracted
    so the slicing loop can compute the dispatch floor BEFORE the slice
    quote (which itself depends on the floor)."""
    if not (book_long.has_top_of_book() and book_short.has_top_of_book()):
        return 0.0
    long_bid_base = pair.long.to_base_price(float(book_long.bids[0][0]))
    long_ask_base = pair.long.to_base_price(float(book_long.asks[0][0]))
    short_bid_base = pair.short.to_base_price(float(book_short.bids[0][0]))
    short_ask_base = pair.short.to_base_price(float(book_short.asks[0][0]))
    return (long_bid_base + long_ask_base + short_bid_base + short_ask_base) / 4.0


# ---------- The Dynamic Depth Oracle ----------

def project_slice(
    book_long: BookSnapshot,
    book_short: BookSnapshot,
    pair: ExecutionPair,
    basis_floor_bps: float,
    remaining_qty_base: float,
    min_dispatch_base: float,
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

    Caller is expected to have run `_gate_book()` on both inputs before
    calling — `project_slice` defensively checks emptiness but does not
    re-validate freshness, crossedness, or level depth.

    Limit prices on the returned SliceQuote are the deepest levels touched
    at the locked S (pre-discount), in venue-native units. The matching
    engine will reject any fill beyond those prices, physically enforcing
    the basis floor. DEPTH_DISCOUNT haircut applies to dispatched size only,
    not to limits.

    `min_dispatch_base` is the dust floor — the largest of the two legs' min
    lot sizes expressed in base tokens. Two interactions:
      1. Returned None if even the safe ceiling is below min — book is too
         thin to trade.
      2. If DEPTH_DISCOUNT × safe_ceiling would drop below min, fall back to
         dispatching at the full safe_ceiling. Sacrifices the phantom-liquidity
         buffer on this slice in exchange for staying tradeable. Naturally
         applies to residuals and tiny smoketest sizes.

    Returns None if no positive S satisfies the floor.
    """
    long_bids = book_long.bids
    long_asks = book_long.asks
    short_bids = book_short.bids
    short_asks = book_short.asks

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

    # Binary search: lo_base is always a passing S (in base); hi_search is always
    # failing (or the initial cap, which we snap onto at the end if it also passes).
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

    # Final-step snap: halvings never land lo_base exactly at hi_search — they
    # leave a ~hi_base/2^iters fence-post gap. That gap is invisible to haircut
    # math but trips the min_dispatch_base guard below when target ≈ dust
    # (residuals, smoketest-tiny entries). If hi_search would also pass, snap
    # lo_base up to it. Closes the fence-post at the source.
    if hi_search > lo_base:
        basis_at_top, _, _ = basis_at(hi_search)
        if basis_at_top is not None and basis_at_top >= floor_frac:
            lo_base = hi_search

    if lo_base <= 0:
        return None

    # Re-evaluate at the locked S to capture deepest-level prices
    final_basis, deep_long_native, deep_short_native = basis_at(lo_base)
    if final_basis is None or final_basis < floor_frac:
        return None

    # Even the safe ceiling is below tradeable minimum — book too thin to slice.
    if lo_base < min_dispatch_base:
        return None

    # Dust-floor-aware haircut: fall back to no-haircut dispatch when the
    # discounted size would be untradeable. Standard for residuals and tiny
    # smoketest sizes; the operator accepts loss of phantom-liquidity buffer
    # on this slice rather than no fill at all.
    dispatch_size_base = lo_base * DEPTH_DISCOUNT
    if dispatch_size_base < min_dispatch_base:
        dispatch_size_base = lo_base

    return SliceQuote(
        size_base=dispatch_size_base,
        safe_size_base=lo_base,
        limit_long_native=deep_long_native,
        limit_short_native=deep_short_native,
        projected_basis_bps=final_basis * 10000.0,
        mid_price_base=mid_price_base,
        min_dispatch_base=min_dispatch_base,
    )


# ---------- Symmetric dispatch sizing ----------

# Bounded retry count for the symmetric-dispatch convergence loop.
# Each iteration is monotonically non-increasing in the candidate base;
# for venues whose precision-steps are integer multiples of one another
# (every one of the 12 verified venues), convergence happens in ≤ 2
# iterations. The third pass is forensic margin for venues with
# pathological non-commensurate step sizes — accept the final converged
# value rather than looping indefinitely.
SYMMETRIC_DISPATCH_MAX_ITERATIONS = 3

# Convergence tolerance for the iterative snap. Sub-femtobase deltas
# imply we've hit a precision-floor that won't decrease further; treat
# as converged.
SYMMETRIC_DISPATCH_EPSILON = 1e-12


def _compute_symmetric_dispatch(
    engine,
    pair: ExecutionPair,
    dispatch_base: float,
) -> Tuple[float, float, float]:
    """Find the largest base quantity ≤ dispatch_base that produces the
    SAME post-precision base quantity on both legs.

    Different venues advertise different lot precisions in BASE-equivalent
    units. KuCoin XRP perp's contract_size=10 with native precision 1
    means a base step of 10. OKX's contract_size=100 with native precision
    0.01 means a base step of 1. Naïvely converting `dispatch_base` to
    native on each leg and applying `amount_to_precision` produces
    asymmetric IOC quantities — both legs were intended to send the same
    base, but each leg's precision-floor truncates differently.

    Anchor: 2026-05-09 22:13:50 cross_venue_smoketest XRP run on
    `kucoinfutures × okx`. Engine intended to dispatch 19 base on both
    legs. KuCoin's precision rounded 1.9 contracts to 1 (= 10 base);
    OKX's precision kept 0.19 contracts (= 19 base). Both IOCs filled
    fully → asymmetric position: 10 long on KuCoin vs 19 short on OKX.
    The 9-XRP residual was below pair-dust (max(10, 1) = 10) so the
    recovery path skipped silently. Result: 9 XRP naked short on OKX,
    invisible to engine state, surfaced only via operator UI inspection.

    Iteration: convert base→native on each leg via `amount_to_precision`,
    convert each native back to base, take min, feed back. Monotonically
    non-increasing — converges to the largest base that snaps cleanly on
    both legs. Returns `(0, 0, 0)` if the converged value rounds either
    leg's native qty to zero (sub-lot) — caller treats as "skip cycle".

    For venues with non-commensurate precision steps (rare; not seen on
    our 13 verified venues), we accept the bounded-iteration final value
    and let the recovery path handle any leftover asymmetry — recovery's
    dust check is now per-target-leg (`recover_imbalance` post-fix),
    so it CAN absorb sub-pair-dust residuals on the smaller-step leg."""
    ex_long = engine.exchanges[pair.long.exchange]
    ex_short = engine.exchanges[pair.short.exchange]

    current_base = float(dispatch_base)
    last_native_long = 0.0
    last_native_short = 0.0
    converged = False
    for _ in range(SYMMETRIC_DISPATCH_MAX_ITERATIONS):
        native_long = float(ex_long.amount_to_precision(
            pair.long.symbol, pair.long.to_native_qty(current_base)))
        native_short = float(ex_short.amount_to_precision(
            pair.short.symbol, pair.short.to_native_qty(current_base)))
        if native_long <= 0 or native_short <= 0:
            return (0.0, 0.0, 0.0)
        base_long = native_long * pair.long.base_per_native
        base_short = native_short * pair.short.base_per_native
        next_base = min(base_long, base_short)
        last_native_long, last_native_short = native_long, native_short
        if abs(next_base - current_base) < SYMMETRIC_DISPATCH_EPSILON:
            converged = True
            current_base = next_base
            break
        current_base = next_base

    if not converged:
        # Bounded iterations exhausted without convergence — implies
        # the two legs' precisions are not integer multiples of one
        # another. Loud-log so the operator can flag the venue pairing.
        # Recovery path will reconcile any asymmetric fill (now safe
        # under per-target-leg dust threshold).
        log(
            f"symmetric_dispatch did not converge for "
            f"{pair.long.exchange}:{pair.long.symbol} × "
            f"{pair.short.exchange}:{pair.short.symbol}. "
            f"Final native: long={last_native_long}, short={last_native_short}. "
            f"Recovery path will reconcile any residual asymmetry.",
            "DISPATCH_WARNING",
        )

    return (current_base, last_native_long, last_native_short)


# ---------- IOC Dispatch ----------

async def dispatch_ioc_pair(
    engine,
    pair: ExecutionPair,
    quote: SliceQuote,
    side: str,
) -> Tuple[Optional[FillReceipt], Optional[FillReceipt]]:
    """
    Concurrent IOC limit dispatch + per-venue receipt resolution.
    Returns (FillReceipt_long, FillReceipt_short) — both fully resolved
    (sync-null venues went through fetch_order before the receipt is
    handed back).

    Returns (None, None) if the slice rounds to sub-lot on either leg —
    no order is placed; caller should treat as a skipped cycle.

    Two-stage gather pattern:
      1. create_order on both legs concurrently. return_exceptions=True so
         BOTH legs always run to completion before we decide what to do —
         an orphaned coroutine could otherwise place an order after we've
         started unwinding state.
      2. resolve_receipt on both legs concurrently. Same return_exceptions
         policy: if a sync-null venue's fetch_order fails, we still want
         to know the OTHER leg's resolved state before halting.

    On any leg's create_order or resolve_receipt failure: log loud, raise
    RuntimeError. Engine fires Pushover P2; operator reconciles manually.
    We cannot assume 0 fill on a failed call — the venue may have placed
    AND filled the order between our request and the failure.

    Per-venue create_order params come from `venue_overrides.ioc_limit_params_for`
    (the empirically verified single source of truth — kucoinfutures
    needs `marginMode: cross`, mexc needs `type: 3` for IOC).
    """
    if side == "entry":
        long_side, short_side = "buy", "sell"
        reduce_only = False
    elif side == "exit":
        long_side, short_side = "sell", "buy"
        reduce_only = True
    else:
        raise ValueError(f"side must be 'entry' or 'exit', got {side!r}")

    # Symmetric dispatch — derive native quantities such that
    # post-precision base on BOTH legs is identical. Anchor:
    # 2026-05-09 KuCoin × OKX (KuCoin contract=10x truncated 1.9 → 1
    # while OKX contract=100x kept 0.19, leaving a 9-XRP asymmetric
    # residual the recovery path silently dropped under pair-dust). See
    # `_compute_symmetric_dispatch` for the convergence algorithm.
    symmetric_base, qty_long_native, qty_short_native = (
        _compute_symmetric_dispatch(engine, pair, quote.size_base)
    )

    if qty_long_native <= 0 or qty_short_native <= 0:
        log(
            f"Slice {quote.size_base:.8f} (base) symmetric-snap rounds to sub-lot "
            f"on {pair.long.exchange}/{pair.short.exchange} "
            f"(long_native={qty_long_native}, short_native={qty_short_native}). Skipping cycle.",
            "SLICE",
        )
        return None, None

    # Defensive notional gate: if symmetric_base falls below the per-cycle
    # composite dispatch floor (lot + notional + snap-safe), the venue
    # would reject with InsufficientNotional. Should never fire for sane
    # inputs — `project_slice` enforces lo_base ≥ min_dispatch_base AND
    # the floor is rounded UP to the next snap step, so post-snap is
    # guaranteed to land at or above min_dispatch_base. This catches
    # arithmetic edge cases (precision wobble, non-commensurate steps)
    # before the wire instead of after a venue 5-USDT-floor rejection.
    if symmetric_base < quote.min_dispatch_base * 0.999:  # 0.1% float tolerance
        log(
            f"Slice {quote.size_base:.8f} → {symmetric_base:.8f} after snap "
            f"is below cycle floor {quote.min_dispatch_base:.8f} "
            f"(would hit venue InsufficientNotional). Skipping cycle.",
            "SLICE",
        )
        return None, None

    # Surface the symmetric snap when it rounded the slice DOWN — operator
    # eyeballs whether the haircut on this venue pairing is severe.
    if symmetric_base < quote.size_base * 0.99:
        log(
            f"Symmetric snap: {quote.size_base:.8f} → {symmetric_base:.8f} base "
            f"(long_native={qty_long_native}, short_native={qty_short_native}) — "
            f"asymmetric leg precisions on {pair.long.exchange}/{pair.short.exchange}.",
            "SLICE",
        )

    base_params: dict = {"timeInForce": "IOC"}
    if reduce_only:
        base_params["reduceOnly"] = True

    long_params = ioc_limit_params_for(pair.long.exchange, base_params)
    short_params = ioc_limit_params_for(pair.short.exchange, base_params)

    ex_long = engine.exchanges[pair.long.exchange]
    ex_short = engine.exchanges[pair.short.exchange]

    placement = await asyncio.gather(
        ex_long.create_order(
            pair.long.symbol, "limit", long_side,
            qty_long_native,
            quote.limit_long_native,
            params=long_params,
        ),
        ex_short.create_order(
            pair.short.symbol, "limit", short_side,
            qty_short_native,
            quote.limit_short_native,
            params=short_params,
        ),
        return_exceptions=True,
    )

    raw_long, raw_short = placement
    err_long = isinstance(raw_long, Exception)
    err_short = isinstance(raw_short, Exception)
    if err_long or err_short:
        log(
            f"IOC dispatch failure | long={'ERR: '+repr(raw_long) if err_long else 'OK'} "
            f"| short={'ERR: '+repr(raw_short) if err_short else 'OK'}",
            "CRITICAL",
        )
        raise RuntimeError(
            f"IOC dispatch failed (long_err={err_long}, short_err={err_short}). "
            f"long={raw_long!r} short={raw_short!r}"
        )

    # Both placements succeeded — resolve concurrently. sync-zero venues
    # short-circuit (no extra round-trip); sync-null venues do one
    # fetch_order. Concurrent resolution means slicing-loop latency is
    # max(left, right) not sum.
    resolution = await asyncio.gather(
        resolve_receipt(pair.long, raw_long, ex_long),
        resolve_receipt(pair.short, raw_short, ex_short),
        return_exceptions=True,
    )

    fr_long, fr_short = resolution
    err_resolve_long = isinstance(fr_long, Exception)
    err_resolve_short = isinstance(fr_short, Exception)
    if err_resolve_long or err_resolve_short:
        log(
            f"Receipt resolution failure | "
            f"long={'ERR: '+repr(fr_long) if err_resolve_long else 'OK'} "
            f"| short={'ERR: '+repr(fr_short) if err_resolve_short else 'OK'} "
            f"(placements: long_id={raw_long.get('id')!r} short_id={raw_short.get('id')!r})",
            "CRITICAL",
        )
        raise RuntimeError(
            f"Receipt resolution failed "
            f"(long_err={err_resolve_long}, short_err={err_resolve_short}). "
            f"Placements went through — manual reconcile required."
        )

    return fr_long, fr_short


# ---------- Imbalance Recovery ----------

async def recover_imbalance(
    engine,
    pair: ExecutionPair,
    base_filled_long: float,
    base_filled_short: float,
    side: str,
    mid_price_base: float = 0.0,
) -> Optional[RecoveryFill]:
    """
    Diff fills (in BASE tokens). If asymmetric beyond the dust floor, fire an
    uncapped market order on the lagging leg to restore delta-neutrality.
    Bypasses basis gating — neutrality > marginal cost on a fractional remainder.

    Recovery uses market orders (not IOC limits), so per-venue overrides come
    from `venue_overrides.market_order_params_for` — NOT the IOC-specific map.
    Mexc's `type: 3` IOC override would force IOC on a market order; the
    market_order map omits it.

    Receipt resolution flows through `receipt_resolver.resolve_receipt` —
    same R-Mode catalog as IOC dispatch. Sync-null venues (the majority)
    require fetch_order(id) follow-up to read the actual fill state; without
    it, recovery would silently see filled=None=0 and the slicing loop's
    next cycle would re-fire the same recovery, accumulating duplicate fills.

    Returns RecoveryFill (leg, base_qty, vwap_base) on dispatch, None if
    asymmetry is below the TARGET LEG's min-lot (recovery is impossible
    on the lagging venue). Re-raises on dispatch failure; engine fires
    Pushover P2.
    """
    delta_base = base_filled_long - base_filled_short
    if delta_base == 0:
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
        residual_leg = pair.long  # over-fill site
    else:
        target_leg = pair.long
        target_name = "long"
        market_side = "buy" if side == "entry" else "sell"
        residual_leg = pair.short  # over-fill site

    # Dust check uses TARGET LEG's COMPOSITE floor (lot + notional),
    # NOT pair-max. Two distinct fixes layered:
    #   1. (2026-05-09) per-leg lot floor — the pair-max was wrong, an
    #      imbalance smaller than the larger leg's min-lot but LARGER
    #      than the target leg's min-lot was dropped silently. Anchor:
    #      9-XRP naked short on OKX (KuCoin lot=10, OKX lot=1).
    #   2. (2026-05-10) per-leg notional floor — recovery's market order
    #      also has to clear the venue's min-notional. A 4-XRP imbalance
    #      ($5.68 at $1.42) clears 5-USDT floor; a 3-XRP imbalance ($4.26)
    #      doesn't. Without this, recovery would request 3 XRP and hit
    #      InsufficientNotional venue-side.
    #   3. snap-safe step rounding — the venue's amount_to_precision
    #      truncates to a step boundary (TRUNCATE on most CCXT impls).
    #      If composite=3.52 and step=0.1, recovery of 3.55 native rounds
    #      to 3.5 → $4.97 → reject. Ceil to next step → 3.6 → $5.11 ✓.
    target_lot_base = engine._min_base_for_leg(target_leg)
    target_notional_usdt = engine._min_notional_for_leg(target_leg)
    target_step_base = engine._step_base_for_leg(target_leg)
    if mid_price_base > 0:
        target_notional_base = target_notional_usdt / mid_price_base
    else:
        target_notional_base = 0.0
    target_composite = max(target_lot_base, target_notional_base)
    if target_step_base > 0:
        target_dust_base = math.ceil(target_composite / target_step_base) * target_step_base
    else:
        target_dust_base = target_composite

    abs_delta_base = abs(delta_base)
    if abs_delta_base < target_dust_base:
        # Recovery genuinely impossible — the target venue won't accept
        # the residual at lot+notional+step constraints. Loud-log: this
        # is the silent-leak signal the operator must see at runtime,
        # not in post-mortem UI inspection.
        log(
            f"Imbalance {abs_delta_base:.8f} below {target_leg.exchange}:{target_leg.symbol} "
            f"composite floor {target_dust_base:.8f} "
            f"(lot={target_lot_base} notional_base={target_notional_base:.4f} step={target_step_base}); "
            f"recovery NOT POSSIBLE. Asymmetric residual {delta_base:+.8f} base "
            f"persists on {residual_leg.exchange}:{residual_leg.symbol} (side={side}). "
            f"Manual reconciliation required.",
            "WARNING",
        )
        return None

    qty_native = engine._to_native_qty(target_leg, abs_delta_base)
    if qty_native <= 0:
        # Normalized down to zero (precision rounding) — treat as untradeable.
        # Same warning path as the dust gate above.
        log(
            f"Imbalance {abs_delta_base:.8f} on {target_leg.exchange}:{target_leg.symbol} "
            f"rounds to zero native after CCXT precision; recovery NOT POSSIBLE. "
            f"Asymmetric residual {delta_base:+.8f} base on {residual_leg.exchange}.",
            "WARNING",
        )
        return None

    base_params: dict = {"reduceOnly": True} if side == "exit" else {}
    params = market_order_params_for(target_leg.exchange, base_params)
    ex = engine.exchanges[target_leg.exchange]

    try:
        raw_receipt = await ex.create_market_order(
            target_leg.symbol, market_side, qty_native, params=params
        )
    except Exception as e:
        log(f"Recovery FAILED on {target_leg.exchange}:{target_leg.symbol}: {e}", "CRITICAL")
        raise

    # Resolve through the per-venue R-Mode catalog. sync-null venues
    # (the majority — see ENGINE_FIELD_NOTES.md Table B) require
    # fetch_order before fill state is trustable.
    fr = await resolve_receipt(target_leg, raw_receipt, ex)

    log(
        f"Recovery: {market_side} {qty_native} {target_leg.symbol} on {target_leg.exchange} "
        f"@native={fr.vwap_native:.10f} @base={fr.vwap_base:.10f} "
        f"(filled_base={fr.filled_base:.8f}, delta_base={delta_base:.8f}, "
        f"side={side}, r_mode={fr.r_mode}, path={fr.resolution_path})",
        "RECOVERY",
    )

    return RecoveryFill(
        leg=target_name,
        base_qty=fr.filled_base,
        vwap_base=fr.vwap_base,
        order_record=_order_record_from_fill(
            fr, leg_role=target_name, kind="recovery", side=market_side,
        ),
    )


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
      1. Snapshot both BookSnapshots from engine.order_books
      2. _gate_book() on each — presence + freshness + non-crossed + min levels
      3. Gate failure or no project_slice quote? short sleep, retry
      4. dispatch_ioc_pair (concurrent IOCs + concurrent receipt resolution)
         -> (FillReceipt, FillReceipt) — sync-null venues already resolved via fetch_order
      5. recover_imbalance — restores neutrality on asymmetric fill (RecoveryFill | None)
      6. Accumulate qty + notional per leg in BASE units (incl. recovery contribution)
         using FillReceipt.filled_base / FillReceipt.vwap_base directly
      7. SLICE_COOLDOWN_S yield to let MMs replenish

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

    # Per-order capture for the True PnL primitive (Phase 1). Appended
    # to inside the cycle: 2 IOC records + optional 1 recovery record.
    # Persisted by handle_entry/handle_exit into positions.json so a
    # closing exit can fold both sides into closed_trades.json for
    # off-engine pnl.py analysis. NEVER cleared mid-loop — even on
    # asymmetric_residual halt the records are returned so the operator
    # has full forensic trail of what fired before the halt.
    order_records: list = []

    # Static lot-only floor — for the SLICE START log line and as the
    # informational anchor. The OPERATING floor combines lot + notional
    # + snap-safe rounding and is computed per-cycle (depends on mid price).
    lot_floor_base = engine._pair_dust(pair)

    log(
        f"Slicing loop START side={side} pair={pair.key} "
        f"long={pair.long.exchange}:{pair.long.symbol} short={pair.short.exchange}:{pair.short.symbol} "
        f"target_base={target_qty_base} basis_floor={basis_floor_bps}bps "
        f"duration={max_duration_s}s lot_floor_base={lot_floor_base}",
        "SLICE",
    )

    while filled_total < target_qty_base and time.monotonic() < deadline:
        # Cycle-boundary abort check (cheap path before any I/O)
        if abort_event is not None and abort_event.is_set():
            halt_reason = "aborted"
            break

        book_long = engine.order_books.get((pair.long.exchange, pair.long.symbol))
        book_short = engine.order_books.get((pair.short.exchange, pair.short.symbol))
        # Single time source per cycle — both legs gate against the same wall
        # clock to avoid sub-ms drift causing one leg to pass and the other fail.
        cycle_now_ms = _now_ms()
        gate_failure = (
            _gate_book(book_long, pair.long, cycle_now_ms)
            or _gate_book(book_short, pair.short, cycle_now_ms)
        )
        if gate_failure is not None:
            log(f"Skip cycle: {gate_failure}", "BOOK")
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        # Per-cycle composite dispatch floor — combines lot + notional
        # + snap-safe rounding. Anchor: 2026-05-10 mexc × bitget crash
        # (2-XRP slice rejected at 5-USDT venue floor). Notional is
        # price-dependent so this MUST be recomputed per cycle.
        cycle_mid_price_base = _mid_price_base_from_books(book_long, book_short, pair)
        _, dispatch_floor_base, notional_floor_base = _compute_dispatch_floor_base(
            engine, pair, cycle_mid_price_base
        )

        # Termination check — uses the COMPOSITE floor (not just lot).
        # If remaining is below the floor, no further slice is dispatchable.
        remaining_base = target_qty_base - filled_total
        if remaining_base < dispatch_floor_base:
            halt_reason = "dust"
            break

        quote = project_slice(
            book_long, book_short, pair, basis_floor_bps,
            remaining_base, dispatch_floor_base, side,
        )
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
        long_bid_native = float(book_long.bids[0][0])
        long_ask_native = float(book_long.asks[0][0])
        short_bid_native = float(book_short.bids[0][0])
        short_ask_native = float(book_short.asks[0][0])
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
        # Effective haircut: usually DEPTH_DISCOUNT, but reverts to 1.0 when the
        # discounted size would fall below the dust floor (residuals / tiny sizes).
        effective_haircut = quote.size_base / quote.safe_size_base if quote.safe_size_base > 0 else 0.0
        log(
            f"IOC slice dispatch_base={quote.size_base:.8f} safe_ceiling_base={quote.safe_size_base:.8f} "
            f"(haircut=×{effective_haircut:.2f}) "
            f"limits long_native={quote.limit_long_native} short_native={quote.limit_short_native} "
            f"projected_basis={quote.projected_basis_bps:.2f}bps",
            "SLICE",
        )

        # Dispatch + recovery is the atomic unit — abort is NOT honored here.
        # Raises on transient errors (engine halts + alerts).
        # Returns fully-resolved FillReceipt objects: sync-null venues have
        # already gone through fetch_order(id) inside the resolver.
        fr_long, fr_short = await dispatch_ioc_pair(engine, pair, quote, side)

        # Sub-lot slice — dispatch was skipped, no fill to account for.
        # Same handling as quote=None: brief idle, retry next cycle.
        if fr_long is None or fr_short is None:
            if await _wait_or_abort(IDLE_RETRY_S, abort_event):
                halt_reason = "aborted"
                break
            continue

        # Direct reads from FillReceipt — no more `_fill_vwap` or
        # `to_base_qty(receipt.get('filled'))` scattered through the loop.
        # The resolver handled R-Mode-aware extraction + base/native
        # conversion at construction time.
        base_filled_long = fr_long.filled_base
        base_filled_short = fr_short.filled_base
        vwap_long_base = fr_long.vwap_base
        vwap_short_base = fr_short.vwap_base

        # Capture IOC order records for downstream PnL analysis. Side
        # mapping mirrors dispatch_ioc_pair: entry → long buys, short sells;
        # exit → long sells, short buys.
        ioc_long_side = "buy" if side == "entry" else "sell"
        ioc_short_side = "sell" if side == "entry" else "buy"
        order_records.append(_order_record_from_fill(
            fr_long, leg_role="long", kind="ioc", side=ioc_long_side,
        ))
        order_records.append(_order_record_from_fill(
            fr_short, leg_role="short", kind="ioc", side=ioc_short_side,
        ))

        recovery = await recover_imbalance(
            engine, pair, base_filled_long, base_filled_short, side,
            mid_price_base=cycle_mid_price_base,
        )

        # Per-cycle leg totals (IOC + any recovery contribution to the lagging leg)
        cycle_qty_long_base = base_filled_long
        cycle_qty_short_base = base_filled_short
        cycle_notional_long_base = vwap_long_base * base_filled_long
        cycle_notional_short_base = vwap_short_base * base_filled_short

        if recovery is not None:
            order_records.append(recovery.order_record)
            if recovery.leg == "long":
                cycle_qty_long_base += recovery.base_qty
                cycle_notional_long_base += recovery.vwap_base * recovery.base_qty
                recovery_target_leg = pair.long
            else:
                cycle_qty_short_base += recovery.base_qty
                cycle_notional_short_base += recovery.vwap_base * recovery.base_qty
                recovery_target_leg = pair.short

            # Recovery underfill log — informational. The HALT decision below
            # uses the same residual signal but on the canonical post-recovery
            # delta. Threshold here is the TARGET LEG's COMPOSITE floor
            # (lot + notional). The earlier two-line log lets the operator see
            # the recovery attempt's outcome before the halt verdict prints.
            expected_recovery = abs(base_filled_long - base_filled_short)
            target_lot = engine._min_base_for_leg(recovery_target_leg)
            target_notional_base = (
                engine._min_notional_for_leg(recovery_target_leg) / cycle_mid_price_base
                if cycle_mid_price_base > 0 else 0.0
            )
            target_min = max(target_lot, target_notional_base)
            if abs(recovery.base_qty - expected_recovery) >= target_min:
                log(
                    f"Recovery UNDERFILL on {recovery_target_leg.exchange}:{recovery_target_leg.symbol}: "
                    f"requested {expected_recovery:.8f} got {recovery.base_qty:.8f}. "
                    f"Asymmetric exchange-side residual {expected_recovery - recovery.base_qty:.8f} base.",
                    "WARNING",
                )

        # ----- Asymmetric residual halt — the cycle invariant -----
        # After dispatch + recovery, cycle_qty_long_base SHOULD equal
        # cycle_qty_short_base. If they differ by a tradeable amount,
        # the venue position state has DRIFTED from the engine's intent
        # and continuing would compound naked exposure on the over-fill
        # leg. Halt immediately and surface to operator as CRITICAL.
        #
        # Anchor: 2026-05-10 02:52 bingx × xt — XT's IOC SELL silently
        # returned filled=0 (likely positionMode/funding mismatch).
        # BingX BUY filled 10 XRP. Recovery's market sell on XT also
        # returned filled=0. Old behavior: WARNING + continue. NEXT cycle
        # added another 10 XRP naked long on BingX before the recovery
        # finally errored with insufficient_balance. With this halt, the
        # FIRST asymmetric cycle stops the loop — operator deals with
        # 10 XRP of exposure, not 20.
        #
        # Threshold: the SMALLER leg's min-lot in base. If residual is
        # tradeable on EITHER venue, it's real exposure. Use the smaller
        # to err on the side of halting (false-positive halt is just
        # operator inconvenience; false-negative is silent capital drift).
        cycle_residual_base = abs(cycle_qty_long_base - cycle_qty_short_base)
        residual_threshold = min(
            engine._min_base_for_leg(pair.long),
            engine._min_base_for_leg(pair.short),
        )
        # Floor the threshold so a venue with min_lot=0 (e.g. gate XRP)
        # doesn't degenerate to "any non-zero residual halts on float noise".
        residual_threshold = max(residual_threshold, 1e-9)
        if cycle_residual_base >= residual_threshold:
            # Identify the over-fill leg (where the naked exposure sits).
            if cycle_qty_long_base > cycle_qty_short_base:
                residual_leg = pair.long
                residual_side_word = "LONG" if side == "entry" else "UNDER-EXITED LONG"
            else:
                residual_leg = pair.short
                residual_side_word = "SHORT" if side == "entry" else "UNDER-EXITED SHORT"

            # Record the cycle's full state (symmetric + asymmetric) into
            # cumulative so the operator's post-halt LoopResult shows the
            # actual fills on each venue.
            cumulative_notional_long_base += cycle_notional_long_base
            cumulative_notional_short_base += cycle_notional_short_base
            cumulative_qty_long_base += cycle_qty_long_base
            cumulative_qty_short_base += cycle_qty_short_base
            cycle_filled = min(cycle_qty_long_base, cycle_qty_short_base)
            filled_total += cycle_filled

            log(
                f"ASYMMETRIC RESIDUAL {cycle_residual_base:.8f} base after recovery — "
                f"long_filled={cycle_qty_long_base:.8f} short_filled={cycle_qty_short_base:.8f}. "
                f"Naked {residual_side_word} exposure on {residual_leg.exchange}:{residual_leg.symbol}. "
                f"HALTING {side} to prevent compounding. Manual reconciliation required.",
                "CRITICAL",
            )
            halt_reason = "asymmetric_residual"
            break

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
            # At least one leg filled zero — old log called this "IOCs
            # rejected entirely" but that's misleading when ONE leg
            # filled and the other didn't. Show the actual per-leg
            # quantities so the operator sees the asymmetry directly.
            # (The CRITICAL halt above already surfaced this; this log
            # remains for the rare case where both legs are exactly 0
            # and recovery either wasn't requested or recovered nothing.)
            log(
                f"filled long={cycle_qty_long_base:.8f} short={cycle_qty_short_base:.8f} "
                f"(asymmetric or zero) cumulative={filled_total:.8f}/{target_qty_base}",
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
        order_records=order_records,
    )
