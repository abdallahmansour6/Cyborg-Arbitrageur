"""
Receipt resolution — convert raw CCXT `create_order` responses into
fill-resolved `FillReceipt` objects.

The contract enforced by this module is the load-bearing fix for the
May 7 bybit silent-fill incident (`transaction.log` 2026-05-07 21:05:39):

  - For sync-zero / sync-final venues, the placement receipt IS the
    authoritative state. No follow-up call.
  - For sync-null / eventual / unknown venues, `fetch_order(id)` is
    MANDATORY. Without it, the engine sees `filled=None` and treats
    it as zero — silently accumulating asymmetric exposure.

For sync-null venues with eventual-consistency on their order-by-id
index (kucoinfutures, anchor: 2026-05-09 20:39:23 XRP IOC), the
fetch_order call may transiently raise `OrderNotFound` for ~50–500ms
after placement. The resolver catches and retries with exponential
backoff for up to `fetch_order_indexing_lag_s_for(venue)` seconds
before propagating. Without this retry, the engine would mis-classify
a successful (and potentially partially-filled) order as a structural
failure and halt with asymmetric exchange-side exposure.

For sync-zero / sync-final venues, placement SHOULD be authoritative
(R-Mode catalog says so). But the catalog was built from non-filling
IOCs via the Class-2 receipt_shape probe, and a venue may behave
differently on actual fills. Anchor (2026-05-10 04:10): bingx × xt
smoketest — BingX classified sync-zero but returned all-None
placement on a filling BUY IOC. To handle catalog drift defensively,
sync-zero classification is treated as a HINT: if placement.filled is
populated, trust it (true sync-zero); otherwise fall back to
fetch_order with the standard retry path. This means even a
misclassified venue resolves correctly, and the operator gets a
RESOLVER_WARNING log signal to update the catalog.

The R-Mode catalog and the per-venue create/fetch param overrides live
in `venue_overrides.py`. Both this module and `engine_probes.py` import
from there — single source of truth.

This module owns the BASE-vs-NATIVE unit conversion for fill state.
After the engine's R1 refactor lands, `execution.py` will read
`fill_receipt.filled_base` and `fill_receipt.vwap_base` directly —
the legacy `_fill_vwap()` and `pair.long.to_base_qty(receipt.get('filled'))`
call sites get deleted.
"""
import asyncio
import time
from typing import Any

from ccxt.base.errors import OrderNotFound

from primitives import ExecutionLeg, FillReceipt
from utils import log
from venue_overrides import (
    fetch_order_indexing_lag_s_for,
    fetch_order_params_for,
    r_mode_for,
    receipt_filled_is_base,
    requires_fetch_order,
)


# When a venue's R-Mode is `eventual`, poll fetch_order at this cadence
# until the venue returns a terminal status. Bounded by EVENTUAL_MAX_WAIT_S
# to keep a stuck venue from hanging the slicing-loop cycle.
#
# None of the 12 verified venues currently exhibit `eventual` on IOC
# orders — every sync-null venue's first fetch_order returns terminal
# state. These constants are forward-compatibility for venues that may
# acquire eventual semantics in future CCXT releases or for non-IOC
# order types (the engine's recovery path uses market orders).
EVENTUAL_POLL_INTERVAL_S = 0.20
EVENTUAL_MAX_WAIT_S = 3.0

# Initial backoff for the indexing-lag retry phase (KuCoin Futures et al.).
# Grows by INDEXING_LAG_BACKOFF_MULTIPLIER on each retry, bounded by the
# venue-specific deadline from `fetch_order_indexing_lag_s_for`.
#
# 100 ms first try gives the venue time to settle without burning the
# entire deadline on a single sleep; the multiplier produces successive
# waits of 100, 150, 225, 337, 506, 759 ms — covers the observed 245-ms
# kucoinfutures lag with several quick attempts before slowing down.
INDEXING_LAG_INITIAL_BACKOFF_S = 0.10
INDEXING_LAG_BACKOFF_MULTIPLIER = 1.5

# After fetch_order, if `status` is still 'open' but `filled` is
# populated, treat as terminal-with-partial-fill — venues sometimes
# leave the lifecycle 'open' on a partially-filled IOC briefly. The
# engine cares about `filled`, not the lifecycle string.
_TERMINAL_STATUSES = ("closed", "canceled", "expired", "rejected", "filled")


# ---------------------------------------------------------------------------
# VWAP extraction
# ---------------------------------------------------------------------------


def _vwap_from_receipt(receipt: dict) -> float:
    """Three-tier VWAP extraction in venue-native price units.

    Replaces the legacy `_fill_vwap()` in `execution.py`. Lives here
    because future per-venue quirks (a venue that exposes VWAP only
    via `trades` list, or that returns `cost` in a different currency)
    will need a fix in exactly one place.

    Returns 0.0 only when no fill data is recoverable. Callers must
    NOT use 0.0 as a price for arithmetic — it indicates "no fill"
    and should pair with `filled == 0`."""
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
            ct = t.get("cost")
            total_notional += float(ct) if ct is not None else price * qty
            total_qty += qty
        if total_qty > 0:
            return total_notional / total_qty
    return 0.0


# ---------------------------------------------------------------------------
# Resolution paths
# ---------------------------------------------------------------------------


async def _fetch_order_resilient(
    exchange_handle: Any,
    order_id: str,
    symbol: str,
    fetch_params: dict,
    indexing_lag_s: float,
    is_eventual: bool,
) -> dict:
    """Two-phase fetch_order with venue-aware resilience.

    Phase 1 — indexing-lag retry. Some venues are eventually consistent
    on their fetch_order(id) endpoint, with TWO observed signatures:

      (a) `OrderNotFound` raised — kucoinfutures pattern. The order id
          is registered server-side but not yet queryable; raises
          100001 for ~50–500 ms after placement.
          Anchor: 2026-05-09 20:39:23 kucoinfutures XRP IOC.

      (b) Stale all-None response — XT pattern. fetch_order RETURNS
          successfully but every field is None (filled=None,
          status=None, info.executedQty=None, info.state=None) — the
          venue's order index hasn't yet processed the placement.
          Empirically observed: 800ms typical lag; transitions
          atomically from all-None to fully populated.
          Anchor: 2026-05-10 03:52 fill_resolution probe on XT.

    Both signatures retry with exponential backoff until
    `indexing_lag_s` seconds elapse. On deadline:
      * (a) re-raises OrderNotFound — genuine missing order.
      * (b) raises RuntimeError — venue indexing is broken or
        unusually slow; the engine escalates rather than trusting an
        all-None response that would be silently treated as filled=0.

    Phase 2 — eventual poll (existing behavior):
        After Phase 1 yields an authoritative response, if the venue
        is classified as `eventual` and status is still `'open'`, we
        poll fetch_order at EVENTUAL_POLL_INTERVAL_S cadence until
        the venue returns a terminal status or EVENTUAL_MAX_WAIT_S
        elapses.

    For non-lagging venues (`indexing_lag_s=0`) and non-eventual
    venues (`is_eventual=False`), this function reduces to a single
    fetch_order call — zero runtime overhead vs. the previous
    implementation. The stale-detection check itself is a single
    dict-key lookup; no extra wire calls."""
    deadline = time.monotonic() + max(indexing_lag_s, 0.0)
    backoff = INDEXING_LAG_INITIAL_BACKOFF_S
    resolved: dict | None = None
    last_orderNotFound: OrderNotFound | None = None
    while True:
        outcome: str
        try:
            resolved = await exchange_handle.fetch_order(
                order_id, symbol, params=fetch_params
            )
            # Stale detection: XT-style all-None response means the
            # venue's order index hasn't processed our placement yet.
            # Distinguishes from a legit "auto-canceled, no fill" which
            # would have status='canceled' / 'expired' and filled=0.0,
            # not None. Specifically check `is None` (not falsy) so
            # filled=0.0 / status='' doesn't trigger retry.
            if resolved.get("filled") is None and resolved.get("status") is None:
                outcome = "stale"
            else:
                # Authoritative response — exit Phase 1.
                break
        except OrderNotFound as e:
            last_orderNotFound = e
            outcome = "not_found"

        now = time.monotonic()
        if now >= deadline:
            # Phase 1 deadline exhausted. Both outcomes escalate
            # (the engine surfaces CRITICAL + Pushover + halt) rather
            # than silently treating a non-authoritative result as
            # "filled=0", which would silently misclassify real fills.
            if outcome == "not_found":
                raise last_orderNotFound  # type: ignore[misc]
            raise RuntimeError(
                f"fetch_order returned stale response (filled=None, "
                f"status=None) for {order_id} on {symbol} after "
                f"{indexing_lag_s}s retry. Venue's order index may be "
                f"broken or unusually delayed; cannot trust the "
                f"all-None result as filled=0."
            )
        await asyncio.sleep(min(backoff, deadline - now))
        backoff *= INDEXING_LAG_BACKOFF_MULTIPLIER

    if not is_eventual:
        return resolved

    eventual_deadline = time.monotonic() + EVENTUAL_MAX_WAIT_S
    while resolved.get("status") == "open" and time.monotonic() < eventual_deadline:
        await asyncio.sleep(EVENTUAL_POLL_INTERVAL_S)
        try:
            resolved = await exchange_handle.fetch_order(
                order_id, symbol, params=fetch_params
            )
        except Exception:
            # Lost the polling round-trip after a valid response. Caller
            # will see status='open' on the last good state and can
            # decide to escalate.
            break
    return resolved


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


async def resolve_receipt(
    leg: ExecutionLeg,
    raw_create_response: dict,
    exchange_handle: Any,
) -> FillReceipt:
    """Convert a raw CCXT `create_order` response into a fully-resolved
    `FillReceipt`.

    Routing per venue R-Mode (see `venue_overrides.VENUE_R_MODE`):

      sync-zero / sync-final
          Placement is authoritative. Read `filled` and VWAP from the
          create_response directly.

      sync-null
          Placement carries only `id`. Call `fetch_order(id, params=...)`
          synchronously; read fill state from the resolved response.

      eventual
          Same as sync-null, but poll fetch_order until status terminal.

      None (unverified venue)
          Treated as sync-null — safest default. Logged via `r_mode='unknown'`.

    Raises `ValueError` if the placement response lacks an `id` field —
    that's the only field universally populated across our 12 verified
    venues. If a CCXT release ever produces a placement without an id,
    the engine has bigger problems than receipt resolution.

    Re-raises any exception from `fetch_order` — caller (engine) decides
    whether to retry, alert, or halt. The resolver never silently
    swallows errors; structural failures must surface.
    """
    venue = leg.exchange
    order_id = raw_create_response.get("id")
    if not order_id:
        raise ValueError(
            f"Cannot resolve receipt: {venue} placement response has no `id`. "
            f"Raw response keys: {sorted(raw_create_response.keys())}"
        )
    order_id = str(order_id)

    rm = r_mode_for(venue)
    rm_label = rm if rm is not None else "unknown"

    if not requires_fetch_order(venue):
        # sync-zero / sync-final classification: placement SHOULD be
        # authoritative. But verify before trusting — `receipt_shape`
        # built the R-Mode catalog from non-filling IOCs, and a venue
        # may behave differently on actual fills.
        #
        # Anchor (2026-05-10 04:10): bingx × xt smoketest. BingX was
        # classified `sync-zero` but returned `filled=None` (or zero)
        # in the placement of an actually-filling BUY IOC. The engine
        # trusted placement, dispatched recovery, BingX rejected with
        # "Insufficient margin" because it ALREADY had the 10 XRP from
        # the unread fill.
        #
        # Trust placement IFF `filled` is populated (not None). The
        # check `is not None` correctly accepts `filled=0.0` (genuine
        # auto-cancel) as authoritative while triggering fallback on
        # the all-None case. Same disambiguation as the stale-detection
        # logic in `_fetch_order_resilient`.
        if raw_create_response.get("filled") is not None:
            authoritative = raw_create_response
            raw_resolve = None
            resolution_path = "placement"
        else:
            # Misclassified-as-sync-zero — fall back to fetch_order with
            # the standard retry. Default 1.0s indexing lag handles any
            # transient lag in the venue's fetch_order endpoint.
            log(
                f"R-Mode '{rm}' venue {venue} returned all-None placement "
                f"(filled=None) on order {order_id}. Likely behaves sync-null "
                f"on filling IOCs (receipt_shape catalog may need update — "
                f"run fill_resolution to verify). Falling back to fetch_order.",
                "RESOLVER_WARNING",
            )
            fetch_params = fetch_order_params_for(venue)
            indexing_lag_s = fetch_order_indexing_lag_s_for(venue)
            authoritative = await _fetch_order_resilient(
                exchange_handle,
                order_id,
                leg.symbol,
                fetch_params,
                indexing_lag_s=indexing_lag_s,
                is_eventual=False,
            )
            raw_resolve = authoritative
            resolution_path = "fetch_order_fallback"
    else:
        # sync-null / eventual / unknown: fetch_order is mandatory.
        # `indexing_lag_s` controls retry on both eventual-consistency
        # signatures (KuCoin OrderNotFound and XT all-None response).
        fetch_params = fetch_order_params_for(venue)
        indexing_lag_s = fetch_order_indexing_lag_s_for(venue)
        authoritative = await _fetch_order_resilient(
            exchange_handle,
            order_id,
            leg.symbol,
            fetch_params,
            indexing_lag_s=indexing_lag_s,
            is_eventual=(rm == "eventual"),
        )
        raw_resolve = authoritative
        resolution_path = "fetch_order"

    # Convert the authoritative receipt into engine-trustable units.
    #
    # Per-venue divergence on `receipt['filled']` semantics: most CCXT
    # venues return filled in NATIVE contract units (the engine's
    # `leg.to_base_qty(filled)` correctly multiplies by contract_size
    # to get base). XT is the documented exception — its CCXT class
    # pre-multiplies executedQty × contract_size in parse_order, so
    # `filled` is already in BASE units. Anchor: 2026-05-10 BingX × XT
    # smoketest where the double-multiply caused a 90-XRP phantom
    # imbalance and Insufficient-margin recovery cascade. See
    # `venue_overrides.VENUE_RECEIPT_FILLED_IN_BASE` for the catalog.
    ccxt_filled = float(authoritative.get("filled") or 0)
    if receipt_filled_is_base(venue):
        # CCXT for this venue stores filled in BASE units already.
        filled_base = ccxt_filled
        filled_native = (
            ccxt_filled / leg.base_per_native if leg.base_per_native > 0 else 0.0
        )
    else:
        # Standard CCXT convention: filled is in native contract units.
        filled_native = ccxt_filled
        filled_base = leg.to_base_qty(ccxt_filled)
    vwap_native = (
        _vwap_from_receipt(authoritative) if filled_native > 0 else 0.0
    )
    vwap_base = leg.to_base_price(vwap_native) if vwap_native > 0 else 0.0
    status = authoritative.get("status")

    return FillReceipt(
        leg=leg,
        order_id=order_id,
        filled_native=filled_native,
        filled_base=filled_base,
        vwap_native=vwap_native,
        vwap_base=vwap_base,
        status=status,
        r_mode=rm_label,
        resolution_path=resolution_path,
        raw_create_response=raw_create_response,
        raw_resolve_response=raw_resolve,
    )
