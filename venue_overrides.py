"""
Per-venue override catalog — single source of truth.

Every entry below is empirically discovered by the probe suite and
documented in ENGINE_FIELD_NOTES.md. Both the probes (`engine_probes.py`)
and the runtime resolver / execution loop (`receipt_resolver.py`,
`execution.py`) import from here, so a discrepancy can never silently
diverge between probe-time truth and production-time behavior.

When a probe surfaces a new override:

  1. Field-notes Table A or B gains a VERIFIED row.
  2. The override gets added to the appropriate map below with a
     comment citing the CCXT source-line OR the venue documentation
     OR the empirical anchor.
  3. If the override is a CCXT bug (vs a venue convention), a
     regression repro is added to `ccxt_bug_repro.py`.

Catalog as of 2026-05-09:
  * 12 of 13 venues fully classified.
  * phemex marked unverified (R-Mode = None) due to opaque venue 39999;
    callers default unverified venues to "fetch_order required" for safety.
"""

# ---------------------------------------------------------------------------
# R-Mode catalog — placement-receipt resolution mode per venue.
# ---------------------------------------------------------------------------
#
# Determines whether the receipt resolver must call `fetch_order(id)`
# after `create_order` returns. The four R-Modes from
# ENGINE_FIELD_NOTES.md Table B:
#
#   sync-final  Placement carries `filled > 0` AND average AND status terminal.
#               (Only seen on capital-at-risk probes; not present on the
#               non-fillable IOC probe that established this catalog.)
#   sync-zero   Placement carries `filled = 0` and status terminal-or-absent
#               with enough fee/cost metadata to confirm no fill happened.
#               Authoritative — no fetch_order needed.
#   sync-null   Placement is uninformative — every fill field None except
#               the order id. fetch_order(id) MANDATORY before reading state.
#   eventual    Placement returns `status='open'`; venue is still resolving.
#               Poll fetch_order until terminal (or timeout).
#
# Values verified 2026-05-09 against BTC/USDT:USDT IOCs at 1% below bid.
# A `None` value means the venue is unverified; resolver treats as
# "fetch_order required" (safest default).
VENUE_R_MODE: dict[str, str | None] = {
    "binance":       "sync-zero",
    "bingx":         "sync-zero",
    "bitget":        "sync-null",
    "bitmart":       "sync-null",
    "bybit":         "sync-null",
    "coinex":        "sync-zero",
    "gate":          "sync-zero",
    "htx":           "sync-null",
    "kucoinfutures": "sync-null",
    "mexc":          "sync-null",
    "okx":           "sync-null",
    "phemex":        None,         # walked away 2026-05-09 — opaque venue 39999
    "xt":            "sync-null",  # Indexing lag ~800ms — the receipt_shape
                                   # probe (non-filling) classified this as
                                   # sync-null but missed the fact that
                                   # fetch_order returns all-None for ~800ms
                                   # after placement. fill_resolution probe
                                   # (2026-05-10 03:52) measured the lag.
                                   # Handled via VENUE_FETCH_ORDER_INDEXING_LAG_S.
}


# ---------------------------------------------------------------------------
# IOC limit-order param overrides — for create_order calls that send an IOC.
# ---------------------------------------------------------------------------
#
# Every entry corrects a real venue-vs-CCXT discrepancy. Without these,
# the venue rejects or — worse — silently misroutes the order.
#
# **IMPORTANT**: these are IOC-LIMIT-SPECIFIC. Do NOT apply them to
# market orders. Mexc's `type: 3` means IOC; using it on a market-order
# call would force IOC instead of market. The recovery path uses
# `market_order_params_for()` instead, which omits IOC-specific keys.
VENUE_IOC_LIMIT_PARAMS: dict[str, dict] = {
    "kucoinfutures": {
        # 330005 otherwise. Marginmode required per-order; CCXT does not
        # infer it from c.options. Repro: ccxt_bug_repro.py kucoin (3B).
        "marginMode": "cross",
    },
    "mexc": {
        # CCXT 4.5.51 `create_swap_order` (mexc.py:2382) does NOT
        # translate `timeInForce: 'IOC'` into mexc's integer `type=3`.
        # Translation only exists in `create_spot_order_request`
        # (mexc.py:2319-2326). Without this override, mexc swap IOCs
        # silently REST as regular limits — engine accumulates exposure.
        # Mexc swap order-type integers: 1 limit, 2 post-only, 3 IOC,
        # 4 FOK, 5 market, 6 convert.
        "type": 3,
    },
}


# ---------------------------------------------------------------------------
# Market-order param overrides — for the recovery path.
# ---------------------------------------------------------------------------
#
# Recovery uses `create_market_order` (engine's imbalance recovery path).
# CCXT translates `type='market'` to the venue-specific integer internally,
# so we must NOT supply our own `type` — that would override CCXT's
# market-order routing.
#
# What does carry over: anything not order-mode-specific. kucoinfutures'
# `marginMode` applies to both IOC and market.
VENUE_MARKET_ORDER_PARAMS: dict[str, dict] = {
    "kucoinfutures": {"marginMode": "cross"},
}


# ---------------------------------------------------------------------------
# fetch_order param overrides.
# ---------------------------------------------------------------------------
#
# Some venues require flags CCXT treats as mandatory but doesn't
# auto-supply. The resolver merges these into every fetch_order call.
VENUE_FETCH_ORDER_PARAMS: dict[str, dict] = {
    "bybit": {
        # CCXT bybit's `fetch_order` raises `ArgumentsRequired` without
        # `acknowledged: True`. The flag suppresses CCXT's "only the
        # last 500 orders are accessible" warning — useful for our
        # immediate-resolution use case where we always look up an
        # order placed in the same cycle.
        "acknowledged": True,
    },
}


# ---------------------------------------------------------------------------
# BookSnapshot.is_fresh() per-venue thresholds.
# ---------------------------------------------------------------------------
#
# Calibrated from the orderbook_liveness probe (ENGINE_FIELD_NOTES.md
# Table C — 2026-05-09 30 s WS sample on BTC/USDT:USDT).
#
# Coinex's p95 max-Δ was 2983 ms — 50× the median cluster (most venues
# at 100–700 ms). A single staleness threshold can't accommodate both
# without either skipping legit ticks on coinex or letting wedged
# streams pass on the fast venues.
#
# Tier policy: 4 s for coinex; 2 s for everyone else. The 2 s ceiling
# is roughly 5× the worst observed p95 max-Δ on the fast cluster
# (phemex ~444 ms, bingx ~527 ms). Re-tune if Table C drifts.
DEFAULT_MAX_BOOK_AGE_MS = 2000

VENUE_MAX_BOOK_AGE_MS: dict[str, int] = {
    "coinex": 4000,
}


# ---------------------------------------------------------------------------
# Public helpers.
# ---------------------------------------------------------------------------


def ioc_limit_params_for(venue: str, base: dict | None = None) -> dict:
    """Merge venue-specific IOC-limit overrides on top of `base` (typically
    `{'timeInForce': 'IOC'}` plus optional `reduceOnly`). Venue-specific
    keys take precedence — these represent empirical truth that callers
    cannot opt out of without breaking IOC honor."""
    merged = dict(base or {})
    merged.update(VENUE_IOC_LIMIT_PARAMS.get(venue, {}))
    return merged


def market_order_params_for(venue: str, base: dict | None = None) -> dict:
    """Merge venue-specific market-order overrides on top of `base`
    (typically `{}` for entry-side recovery, or `{'reduceOnly': True}`
    for exit-side). IOC-specific keys (mexc's `type`) are NOT included."""
    merged = dict(base or {})
    merged.update(VENUE_MARKET_ORDER_PARAMS.get(venue, {}))
    return merged


def fetch_order_params_for(venue: str, base: dict | None = None) -> dict:
    """Same shape as `ioc_limit_params_for`, applied to fetch_order."""
    merged = dict(base or {})
    merged.update(VENUE_FETCH_ORDER_PARAMS.get(venue, {}))
    return merged


def r_mode_for(venue: str) -> str | None:
    """The empirically classified R-Mode, or None if unverified.

    None means "we have not observed a placement receipt from this venue
    yet". Callers must treat None as a safety signal — typically by
    falling back to the most defensive resolution path (call fetch_order
    regardless of placement contents)."""
    return VENUE_R_MODE.get(venue)


def requires_fetch_order(venue: str) -> bool:
    """True iff the venue's R-Mode requires `fetch_order(id)` follow-up
    after placement.

    A `None` R-Mode (unverified) returns True — the safest default. A
    sync-zero / sync-final venue returns False (placement is authoritative).
    A sync-null / eventual venue returns True (placement uninformative
    or in flight)."""
    rm = r_mode_for(venue)
    if rm is None:
        return True
    return rm in ("sync-null", "eventual")


def max_book_age_ms_for(venue: str) -> int:
    """Per-venue staleness threshold for `BookSnapshot.is_fresh()`.
    Returns DEFAULT_MAX_BOOK_AGE_MS for venues without an explicit tier."""
    return VENUE_MAX_BOOK_AGE_MS.get(venue, DEFAULT_MAX_BOOK_AGE_MS)


# ---------------------------------------------------------------------------
# set_leverage param overrides — per venue, per call.
# ---------------------------------------------------------------------------
#
# Each value is a LIST of param dicts. The engine iterates the list per leg,
# calling `set_leverage(leverage, symbol, params=p)` for each `p`. Most
# venues need a single dict; mexc fans out to two (one per `positionType`)
# because its CCXT impl validates `positionType` client-side and we want
# both sides set whether the operator is in one-way or hedge mode.
#
# Empirically discovered 2026-05-09 during cross_venue_smoketest XRP runs:
#   * mexc:    "setLeverage() requires a positionId parameter or a symbol
#               argument with openType and positionType parameters"
#               (CCXT mexc.py:4148)
#   * bitmart: "setLeverage() requires a marginMode argument, one of
#               (isolated, cross)"
#               (CCXT bitmart.py:4580)
#   * bingx:   "setLeverage() requires a side argument" (CCXT-side
#               check in CCXT bingx.py:5523; venue accepts side ∈
#               {LONG, SHORT, BOTH})
#
# Venues NOT in this map use a single empty-dict call. Verified to work
# stock with no override:
#   * binance, bybit, bitget, coinex, gate, htx, okx, xt, kucoinfutures
#     — all default to cross-margin one-way mode without explicit args.
#     (kucoinfutures setLeverage routes through `set_contract_leverage`
#      which uses cross by default — separate from create_order which DOES
#      need an explicit `marginMode: cross` per VENUE_IOC_LIMIT_PARAMS.)
VENUE_SET_LEVERAGE_PARAMS: dict[str, list[dict]] = {
    "bitmart": [{"marginMode": "cross"}],
    "mexc": [
        # MEXC docs: "positionType is ignored when the position is open
        # in cross mode" — but at warmup-time no position exists yet, so
        # both calls are required to pre-set both directions. CCXT also
        # validates `positionType` client-side regardless of mode.
        # openType: 1 isolated, 2 cross. positionType: 1 long, 2 short.
        {"openType": 2, "positionType": 1},
        {"openType": 2, "positionType": 2},
    ],
    "bingx": [
        # BingX defaults accounts to one-way mode unless the operator
        # has explicitly enabled hedge mode. side=BOTH targets the
        # unified position. If the operator switches an account to hedge
        # mode, this must expand to {"side": "LONG"} and {"side": "SHORT"}.
        {"side": "BOTH"},
    ],
    "xt": [
        # XT (CCXT xt.py:3934) hard-validates `positionSide` ∈
        # {LONG, SHORT}. Unlike BingX, XT has NO "BOTH" option —
        # the API expects per-direction calls regardless of account
        # mode. Fan-out applies leverage to both directions; in
        # one-way mode the second call typically no-ops at the venue.
        # Anchor: 2026-05-10 02:34:50 cross_venue_smoketest bingx × xt.
        {"positionSide": "LONG"},
        {"positionSide": "SHORT"},
    ],
}


def set_leverage_params_for(venue: str) -> list[dict]:
    """Per-venue list of param dicts for set_leverage. Each dict drives
    one set_leverage call against the venue. Venues without an override
    get a single empty-dict call (CCXT's defaults usually suffice —
    binance/bybit/bitget/coinex/gate/htx/okx/xt/kucoinfutures all default
    to cross-margin one-way mode without explicit args)."""
    return VENUE_SET_LEVERAGE_PARAMS.get(venue, [{}])


# ---------------------------------------------------------------------------
# fetch_order indexing-lag retry tolerance.
# ---------------------------------------------------------------------------
#
# Some venues are eventually consistent on their fetch_order(id) endpoint:
# the create_order returns an id that isn't queryable for a brief window
# afterward. The receipt resolver catches `OrderNotFound` on the FIRST
# fetch_order call and retries with exponential backoff for up to this
# many seconds before propagating.
#
# Empirical anchor (kucoinfutures, 2026-05-09 20:39:23 cross_venue_smoketest
# XRP run): an IOC dispatched at 20:39:23.220 hit OrderNotFound on
# fetch_order(id) 245 ms later. The order had partially filled (10 XRP
# visible in venue UI) — KuCoin had matched it but its order-by-id index
# was lagging. CCXT routes kucoinfutures.fetch_order through
# `futuresPrivateGetOrdersOrderId` (kucoin.py:5591) which maps to
# `GET /api/v1/orders/{orderId}` — per docs this serves both active AND
# historical orders, so the OrderNotFound is purely an indexing-layer
# eventual-consistency lag, not a wrong-endpoint issue.
#
# Default is 0.0 — no retry. OrderNotFound on a venue without an entry
# here means a genuinely missing order; the engine surfaces as CRITICAL.
# Empirical baseline: 2026-05-10 fill_resolution Class-3 sweep across
# all 8 sync-null venues. EVERY sync-null venue has some eventual
# consistency on fetch_order — the question is just "how much."
# Measured first-non-stale fetch_order timings:
#
#   bybit          54 ms     (fastest)
#   okx            55 ms
#   htx            93 ms
#   bitget         96 ms
#   bitmart       148 ms
#   mexc          232 ms
#   kucoinfutures 786 ms     (OrderNotFound retry pattern)
#   xt            817 ms     (stale all-None pattern)
#
# Default of 1.0s gives a comfortable cushion for the fast cluster
# against transient spikes (worst was 232ms; 1s is ~4× margin) without
# burning much wall time when the venue actually does respond on the
# first call (resolver doesn't retry if first response is authoritative).
# Sync-zero venues never go through fetch_order so are unaffected.
#
# The slow cluster (kucoinfutures, xt at ~800ms) gets an explicit
# higher value below — 3.0s with ~3.7× margin over their measured lag.
DEFAULT_FETCH_ORDER_INDEXING_LAG_S = 1.0

# Per-venue retry tolerance for both eventual-consistency signatures:
#   * OrderNotFound (kucoinfutures pattern — exception raised)
#   * Stale all-None response (xt pattern — response returns but every
#     fill-state field is None until the venue's order index processes
#     the placement)
#
# Both share the same `indexing_lag_s` budget — `_fetch_order_resilient`
# in receipt_resolver.py handles both cases under one bounded retry loop.
VENUE_FETCH_ORDER_INDEXING_LAG_S: dict[str, float] = {
    "kucoinfutures": 3.0,
    "xt": 3.0,
}


def fetch_order_indexing_lag_s_for(venue: str) -> float:
    """Per-venue tolerance (seconds) for retry-on-OrderNotFound during
    immediate fetch_order(id) after create_order. 0.0 means no retry —
    OrderNotFound propagates instantly."""
    return VENUE_FETCH_ORDER_INDEXING_LAG_S.get(
        venue, DEFAULT_FETCH_ORDER_INDEXING_LAG_S
    )


# ---------------------------------------------------------------------------
# CCXT receipt['filled'] unit semantics — per-venue inconsistency.
# ---------------------------------------------------------------------------
#
# CCXT's unified order schema documents `filled` as "the amount of base
# currency filled" — but the actual behavior is INCONSISTENT across
# venues for swap/contract markets.
#
# Most venues' CCXT parse_order leaves `filled` in NATIVE CONTRACT units
# (e.g., kucoinfutures: 1 contract → filled=1; okx: 0.19 contract → filled=0.19).
# Our engine then computes `filled_base = filled × contract_size × multiplier`
# via `leg.to_base_qty(filled)`. Convention works for these venues.
#
# XT is the documented exception. CCXT xt.py:3469 explicitly multiplies
# `executedQty × contract_size` in parse_order for non-spot markets:
#
#     filled = filledQuantity if (marketType == 'spot') else Precise.string_mul(
#         self.number_to_string(filledQuantity),
#         self.number_to_string(market['contractSize']))
#
# So XT's `filled` is in BASE units already. If we apply
# `leg.to_base_qty(filled)`, we DOUBLE-MULTIPLY by contract_size.
#
# Empirical anchor (2026-05-10 04:35): bingx × xt cross_venue diagnostic.
# Dispatched 1 XT contract (= 10 XRP), XT actually filled 10 XRP.
# CCXT returned `filled=10`. Resolver computed filled_base = 10 × 10 = 100.
# delta = 10 (BingX correct) - 100 (XT wrong) = -90. Recovery dispatched
# BUY 90 on BingX → Insufficient margin.
#
# Resolution: the receipt_resolver checks this set; for venues in it,
# `receipt['filled']` is already in base units and we DON'T multiply.
#
# **Bigger lesson**: CCXT's "unified" order schema isn't fully unified.
# When in doubt, run the `fill_resolution` Class-3 probe and compare
# `receipt['filled']` against the dispatched native quantity. If they
# match (within float epsilon), CCXT returns native. If `filled` ==
# native × contract_size, CCXT returns base — add to this set.
VENUE_RECEIPT_FILLED_IN_BASE: set[str] = {
    "xt",
}


def receipt_filled_is_base(venue: str) -> bool:
    """True iff CCXT's `receipt['filled']` for this venue is already in
    base units (post-contract_size multiplication). When True, the
    receipt_resolver skips `leg.to_base_qty(filled)` to avoid the
    double-multiply bug."""
    return venue in VENUE_RECEIPT_FILLED_IN_BASE


# ---------------------------------------------------------------------------
# Minimum-notional fallback (USDT).
# ---------------------------------------------------------------------------
#
# CCXT's `market.limits.cost.min` SHOULD expose the venue's min-notional
# floor. Empirical survey 2026-05-10 across 13 venues × {XRP, BTC} perps:
#
#   Published cost.min:
#     binance        — 5.0  (XRP)  / 50.0 (BTC)  ← BTC distinct from XRP
#     bingx          — 2.0  / 2.0
#     bitget         — 5.0  / 5.0
#     xt             — 5.0  / 10.0
#
#   Returns None — needs fallback:
#     bitmart, bybit, coinex, gate, htx, kucoinfutures, mexc, okx, phemex
#
# Empirical anchor: 2026-05-10 mexc × bitget cross_venue_smoketest crashed
# with "Cannot be less than the minimum order amount 5 USDT" on a 2-XRP
# slice (~$2.84 notional). Both venues enforce 5 USDT venue-side despite
# CCXT not exposing it.
#
# Resolution policy (`min_notional_usdt_for`):
#   1. Per-venue override (this map) — empty by default.
#   2. CCXT-published `market.limits.cost.min` — when present.
#   3. Global default DEFAULT_MIN_NOTIONAL_USDT — last resort safe under-floor.
#
# 5 USDT is the strictest empirical floor on venues that DON'T publish.
# Lower may slip through (most venues likely have real floor ≤ 5); higher
# would over-gate venues with smaller real floors. Add per-venue overrides
# only when a venue's actual floor is empirically observed to diverge.
DEFAULT_MIN_NOTIONAL_USDT = 5.0

VENUE_MIN_NOTIONAL_USDT: dict[str, float] = {
    # Add venue-specific overrides only when CCXT-published value is wrong
    # AND venue-side enforcement diverges from the 5-USDT default.
}


def min_notional_usdt_for(venue: str, ccxt_value: float | None) -> float:
    """Resolve min-notional USDT floor for a venue.
    Precedence: per-venue override > CCXT-published > DEFAULT_MIN_NOTIONAL_USDT.

    `ccxt_value` is the caller-supplied `market.limits.cost.min` (caller
    has the live market dict; this helper stays free of CCXT plumbing)."""
    if venue in VENUE_MIN_NOTIONAL_USDT:
        return VENUE_MIN_NOTIONAL_USDT[venue]
    if ccxt_value is not None and float(ccxt_value) > 0:
        return float(ccxt_value)
    return DEFAULT_MIN_NOTIONAL_USDT


# ---------------------------------------------------------------------------
# Benign warmup-error signatures (idempotency-as-error pattern).
# ---------------------------------------------------------------------------
#
# Several venues raise an exception when a warmup-time configuration call
# (set_leverage, set_margin_mode, set_position_mode) is invoked with the
# value already in effect. Bybit's `110043 leverage not modified` is the
# canonical case but the pattern is widespread (Binance margin-mode
# `-4046`, BingX equivalents reported anecdotally, etc).
#
# These exceptions are HARMLESS — the engine's intent is "leverage = 1x"
# and the venue's state is "leverage = 1x". The "error" is the venue
# punishing the engine for redundant safety. The architectural fix:
# capture the exception, classify it via this map, log "already set"
# instead of failing warmup.
#
# Signatures are SUBSTRINGS of `str(exception)`. We match on the
# numeric error code where the venue exposes one (most stable;
# language-agnostic, version-stable). Substring matching avoids
# JSON-parsing the exception body — the venue-side payload format
# may shift across CCXT versions, but the embedded code rarely does.
#
# Empirical anchor: 2026-05-10 01:54:00 cross_venue_smoketest
# `binance × bybit` warmup. Bybit threw 110043 because operator's
# bybit account was already at 1x leverage on XRP. The engine
# blocked warmup; this classifier unblocks the benign case.
#
# Each entry: (substring_to_match, human_readable_description).
# Add new venue entries as the operator surfaces them — typical
# discovery path is a `WARMUP_ERROR` log on a re-warmup of an
# already-configured account.
VENUE_BENIGN_WARMUP_ERROR_SIGNATURES: dict[str, list[tuple[str, str]]] = {
    "bybit": [
        ('"retCode":110043', "leverage not modified"),
        # Mirror codes for cross/isolated margin idempotency. Add as
        # operator surfaces them on set_margin_mode warmup calls.
        # ('"retCode":110026', "cross/isolated margin not modified"),
    ],
    # Other venues seeded as operator empirically surfaces them. Per
    # `set_leverage_idempotent` probe stub in engine_probes.py — when
    # implemented, that probe will populate this map systematically.
}


def is_benign_warmup_error(venue: str, exc: BaseException) -> tuple[bool, str | None]:
    """Classify a warmup-time exception as benign-idempotent vs genuinely
    fatal.

    Returns `(True, description)` if `str(exc)` matches a signature
    registered for `venue` in `VENUE_BENIGN_WARMUP_ERROR_SIGNATURES`.
    `description` is a short human-readable string suitable for log
    output (e.g., "leverage not modified").

    Returns `(False, None)` for any unrecognized exception — caller
    treats as fatal (re-raise / fail warmup). When in doubt, fail
    loud — never silently swallow a warmup exception we don't
    explicitly recognize."""
    msg = str(exc)
    for signature, description in VENUE_BENIGN_WARMUP_ERROR_SIGNATURES.get(venue, []):
        if signature in msg:
            return (True, description)
    return (False, None)
