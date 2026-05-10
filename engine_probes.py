"""
Engine-side empirical probes.

Verifies the kinetic physics of every venue we trade — order placement,
fill resolution, position state, stream liveness — against live API.
Mirrors the rigor of the Scanner's `probes.py` but tailored to the
Engine's execution surface, not its data-gathering surface.

Probe safety classes (see ENGINE_FIELD_NOTES.md for the full taxonomy):

  Class 1 — Introspection (zero side effect)
    capabilities             c.has dump + auth probe per venue
    orderbook_liveness [SYMBOL]     WS subscribe-and-measure (public stream)
    clock_skew               local UTC vs fetch_time() per venue
    precision_audit [SYM]    amount_to_precision rounding direction

  Class 2 — Safe-by-construction (controlled side effect; cannot fill)
    ioc_honor [SYMBOL]       far-from-spread IOC at min-lot
    receipt_shape [SYMBOL]   captures full receipt dict per venue
    reduceonly_zero [SYM]    reduceOnly IOC on zero position
    set_leverage_idempotent  set_leverage to current value
    cancel_phantom [SYM]     cancel_order with fabricated id
    rate_limit_signature     burst N requests, capture 429/418
    reconnect_behavior [SYM] force ws close, measure recovery

  Class 3 — Capital-at-risk (real fill; gated)
    min_lot_live [SYM] --venue X --I-AM-FUNDED-AND-AUTHORIZED-FOR-X
    cross_venue_smoketest --long_spec=V:SYM --short_spec=V:SYM
                          --I-AM-FUNDED-AND-AUTHORIZED-FOR-{LONG}
                          --I-AM-FUNDED-AND-AUTHORIZED-FOR-{SHORT}
    fill_resolution --venue=V --I-AM-FUNDED-AND-AUTHORIZED-FOR-V
                          poll fetch_order on a small filling IOC

  Class 4 — Continuous watchdogs
    Live alongside the engine (engine_watchdogs.py — proposed; not
    invoked from this CLI).

Methodology preference (2026-05-10): lean toward Class-3 real-fill
probes for receipt-shape verification. The Class-2 `receipt_shape`
probe used a non-filling IOC and missed XT's 800ms fetch_order
indexing lag — caught only after a smoketest accumulated 30 XRP
naked short. Class-3 `fill_resolution` characterized all 8 sync-null
venues in 90 seconds at $0.10 cost. See ENGINE_FIELD_NOTES.md
"Methodology note — lean toward real-fill probes" for the full
discussion + Table H for the empirical fetch_order timings.

Default symbol: BTC/USDT:USDT (with /USD: and /USDC: fallbacks).

Probe outputs land in `probe_logs/<probe>_<UTC-iso>.jsonl` — one
record per (probe, venue) cell. The field-notes tables in
ENGINE_FIELD_NOTES.md are *generated from* these JSONL files; never
hand-edit a row whose status is VERIFIED — re-run the probe instead.

Requires Singapore-region IP. Every venue 451s/`ExchangeNotAvailable`s
on non-Asian residential IPs (Scanner-verified; Engine inherits).

Usage:
    python3 engine_probes.py                          # show this help
    python3 engine_probes.py capabilities
    python3 engine_probes.py orderbook_liveness BTC/USDT:USDT
    python3 engine_probes.py orderbook_liveness BTC/USDT:USDT --duration-s=60
    python3 engine_probes.py ioc_honor BTC/USDT:USDT
    python3 engine_probes.py min_lot_live BTC/USDT:USDT \\
        --venue=binance --I-AM-FUNDED-AND-AUTHORIZED-FOR-BINANCE
    python3 engine_probes.py cross_venue_smoketest \\
        --long_spec=binance:BTC/USDT:USDT \\
        --short_spec=bybit:BTC/USDT:USDT \\
        --I-AM-FUNDED-AND-AUTHORIZED-FOR-BINANCE \\
        --I-AM-FUNDED-AND-AUTHORIZED-FOR-BYBIT
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import get_exchange
from primitives import ExecutionLeg, ExecutionPair
from venue_overrides import (
    ioc_limit_params_for,
    fetch_order_params_for,
)


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

PROBE_LOG_DIR = Path(__file__).parent / "probe_logs"

# Symbols to try in order if the requested one isn't on a given venue.
# CoinEX runs USD-quoted/USDT-settled; some venues are USDC-only on certain
# pairs. The engine itself is USDT-linear-only, but for probing introspection
# we want to reach as many venues as possible with a single command.
SYMBOL_FALLBACK_SUFFIXES = ("/USDT:USDT", "/USD:USDT", "/USDC:USDC")


def _venues_from_env() -> list[str]:
    """Canonical CCXT class ids derived from `.env` `*_API_KEY` keys.
    Lowercase, deduplicated, sorted. Pushover and other non-venue secrets
    are excluded."""
    NON_VENUE_PREFIXES = {"PUSHOVER"}
    venues = set()
    for key in os.environ:
        if not key.endswith("_API_KEY"):
            continue
        prefix = key[: -len("_API_KEY")]
        if prefix in NON_VENUE_PREFIXES:
            continue
        venues.add(prefix.lower())
    return sorted(venues)


def _resolve_symbol(client, requested: str) -> str | None:
    """Try requested symbol, then USD-quoted variant, then USDC variant.
    Mirrors the Scanner's _resolve_symbol but extends to USDC."""
    base = requested.split("/")[0]
    candidates = [requested]
    for suffix in SYMBOL_FALLBACK_SUFFIXES:
        cand = base + suffix
        if cand != requested:
            candidates.append(cand)
    for s in candidates:
        if s in client.markets:
            return s
    return None


def _utc_iso_filename() -> str:
    """ISO-8601 timestamp safe for filenames (no colons)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def _open_log(probe_name: str) -> Path:
    """Open a JSONL log file for this probe run. The caller appends one
    record per (probe, venue) cell via _emit()."""
    PROBE_LOG_DIR.mkdir(exist_ok=True)
    return PROBE_LOG_DIR / f"{probe_name}_{_utc_iso_filename()}.jsonl"


def _emit(log_path: Path, record: dict) -> None:
    """Append one structured record to the probe's JSONL log. `default=str`
    coerces non-JSON-serializable values (CCXT internal classes) into
    string repr — fine for forensic inspection."""
    record = {"_ts": datetime.now(timezone.utc).isoformat(), **record}
    with log_path.open("a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def _safe_open_client(canonical: str):
    """Instantiate a CCXT Pro client; surface instantiation failures as
    a structured record rather than an exception. Returns either a client
    or a dict with an `error` key — caller branches on type."""
    try:
        return get_exchange(canonical)
    except (AttributeError, KeyError, TypeError) as e:
        # AttributeError: ccxt.pro has no `<canonical>` class.
        # KeyError / TypeError: malformed venue id.
        return {"venue": canonical, "error": f"INSTANTIATION_FAIL: {type(e).__name__}: {e}"}


async def _close_quietly(client) -> None:
    """Close a client without letting the close itself raise."""
    try:
        await client.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Class 1 — Introspection
# ---------------------------------------------------------------------------

CAPABILITY_KEYS = (
    # Public auxiliaries
    "fetchTime",
    "fetchMarkets",
    # Private auth
    "fetchBalance",
    # Order placement / mgmt
    "createOrder",
    "cancelOrder",
    "fetchOrder",
    "fetchOpenOrders",
    "fetchOrders",
    # Position state
    "fetchPositions",
    "fetchPosition",
    "watchPositions",
    # Margin / leverage controls
    "setLeverage",
    "setMarginMode",
    "setPositionMode",
    # Streams
    "watchOrderBook",
    "watchTrades",
    "watchTicker",
)


async def _capabilities_one(canonical: str) -> dict:
    """Class 1. Snapshot c.has + try `fetch_balance` to verify auth wiring.
    No orders, no leverage changes, no position changes."""
    maybe_client = _safe_open_client(canonical)
    if isinstance(maybe_client, dict):
        return maybe_client
    client = maybe_client
    try:
        caps = {k: client.has.get(k) for k in CAPABILITY_KEYS}
        balance_ok: Optional[bool] = None
        balance_err: Optional[str] = None
        try:
            await client.fetch_balance()
            balance_ok = True
        except Exception as e:
            balance_ok = False
            balance_err = f"{type(e).__name__}: {str(e)[:200]}"
        return {
            "venue": canonical,
            "ccxt_class": type(client).__name__,
            "ccxt_id": getattr(client, "id", None),
            "ccxt_version": getattr(client, "version", None),
            "capabilities": caps,
            "auth_probe": {"fetch_balance_ok": balance_ok, "error": balance_err},
        }
    finally:
        await _close_quietly(client)


async def capabilities():
    log_path = _open_log("capabilities")
    venues = _venues_from_env()
    print(f"\n{'=' * 110}")
    print(f"CAPABILITIES — {len(venues)} venues — log → {log_path}")
    print(f"Class 1: zero side effect. c.has + private fetch_balance roundtrip.")
    print("=" * 110)

    results = await asyncio.gather(
        *(_capabilities_one(v) for v in venues), return_exceptions=True
    )

    # Pretty header — abbreviated for terminal width
    print(
        f"\n{'venue':<18}{'auth':>5}{'cOrd':>6}{'fOrd':>6}{'fOpen':>6}"
        f"{'fPos':>6}{'sLev':>6}{'sMM':>5}{'sPM':>5}{'wOB':>5}{'wPos':>5}"
    )
    print("-" * 110)
    for canonical, r in zip(venues, results):
        if isinstance(r, Exception):
            print(f"{canonical:<18}  EXCEPTION: {type(r).__name__}: {r}")
            continue
        if "error" in r:
            print(f"{canonical:<18}  {r['error']}")
            _emit(log_path, r)
            continue
        caps = r["capabilities"]
        auth = "Y" if r["auth_probe"]["fetch_balance_ok"] else "N"
        print(
            f"{canonical:<18}"
            f"{auth:>5}"
            f"{str(caps['createOrder'])[:5]:>6}"
            f"{str(caps['fetchOrder'])[:5]:>6}"
            f"{str(caps['fetchOpenOrders'])[:5]:>6}"
            f"{str(caps['fetchPositions'])[:5]:>6}"
            f"{str(caps['setLeverage'])[:5]:>6}"
            f"{str(caps['setMarginMode'])[:4]:>5}"
            f"{str(caps['setPositionMode'])[:4]:>5}"
            f"{str(caps['watchOrderBook'])[:4]:>5}"
            f"{str(caps['watchPositions'])[:4]:>5}"
        )
        _emit(log_path, r)

    print()
    print(f"Inspect raw records:  jq '.' {log_path}")


# ---------------------------------------------------------------------------
# Class 1 — orderbook_liveness
# ---------------------------------------------------------------------------


async def _orderbook_liveness_one(canonical: str, requested_sym: str, duration_s: float) -> dict:
    """Class 1. Subscribe public WS L2; sample for `duration_s`.

    Captures, per venue:
      - time_to_first_s        : seconds from subscribe to first delta
      - delta_interval_ms_*    : median / p95 / max gap between yields
      - empty_top_of_book_count: yields with bids==[] or asks==[]
      - crossed_book_count     : yields where bids[0] >= asks[0]
      - monotonic_violations   : level ordering broken
      - timestamp_populated_pct: % of yields with non-null book['timestamp']
      - venue_clock_advance_ms : how much venue ts moved during the run
      - wall_advance_ms        : how much wall clock moved during the run
        (delta vs. wall_advance reveals venue ts skew)

    No side effects: pure public stream subscription."""
    maybe_client = _safe_open_client(canonical)
    if isinstance(maybe_client, dict):
        return maybe_client
    client = maybe_client

    try:
        await client.load_markets()
        symbol = _resolve_symbol(client, requested_sym)
        if symbol is None:
            return {
                "venue": canonical,
                "error": f"symbol {requested_sym!r} (and fallbacks) not in markets",
            }

        intervals_ms: list[float] = []
        crossed_count = 0
        empty_count = 0
        monotonic_violations = 0
        timestamp_populated = 0
        total_yields = 0
        first_ts: Optional[float] = None
        last_yield_ts: Optional[float] = None
        venue_ts_first: Optional[int] = None
        venue_ts_last: Optional[int] = None

        start = time.monotonic()
        deadline = start + duration_s

        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                book = await asyncio.wait_for(
                    client.watch_order_book(symbol), timeout=remaining
                )
            except asyncio.TimeoutError:
                break
            except Exception as e:
                # Persistent stream-level error — capture and stop sampling.
                # The probe still reports whatever it gathered before the error.
                return {
                    "venue": canonical,
                    "symbol": symbol,
                    "duration_s": duration_s,
                    "error": f"stream_error: {type(e).__name__}: {str(e)[:300]}",
                    "yields_before_error": total_yields,
                }

            now = time.monotonic()
            total_yields += 1
            if first_ts is None:
                first_ts = now
            elif last_yield_ts is not None:
                intervals_ms.append((now - last_yield_ts) * 1000.0)
            last_yield_ts = now

            bids = book.get("bids") or []
            asks = book.get("asks") or []
            if not bids or not asks:
                empty_count += 1
            else:
                if float(bids[0][0]) >= float(asks[0][0]):
                    crossed_count += 1
                # Monotonicity: bids strictly descending; asks strictly ascending.
                # One violation per book is enough to flag this snapshot.
                for prev, curr in zip(bids, bids[1:]):
                    if float(curr[0]) > float(prev[0]):
                        monotonic_violations += 1
                        break
                for prev, curr in zip(asks, asks[1:]):
                    if float(curr[0]) < float(prev[0]):
                        monotonic_violations += 1
                        break

            ts = book.get("timestamp")
            if ts:
                timestamp_populated += 1
                if venue_ts_first is None:
                    venue_ts_first = int(ts)
                venue_ts_last = int(ts)

        intervals_ms.sort()

        def _pct(vs: list[float], q: float) -> Optional[float]:
            if not vs:
                return None
            idx = min(int(len(vs) * q), len(vs) - 1)
            return vs[idx]

        time_to_first_s = (first_ts - start) if first_ts else None
        wall_advance_ms = (
            (last_yield_ts - first_ts) * 1000.0
            if first_ts and last_yield_ts and last_yield_ts > first_ts
            else None
        )
        venue_clock_advance_ms = (
            venue_ts_last - venue_ts_first
            if venue_ts_first and venue_ts_last and venue_ts_last > venue_ts_first
            else None
        )

        return {
            "venue": canonical,
            "symbol": symbol,
            "duration_s": duration_s,
            "yields": total_yields,
            "time_to_first_s": time_to_first_s,
            "delta_interval_ms_median": _pct(intervals_ms, 0.5),
            "delta_interval_ms_p95": _pct(intervals_ms, 0.95),
            "delta_interval_ms_max": max(intervals_ms) if intervals_ms else None,
            "empty_top_of_book_count": empty_count,
            "crossed_book_count": crossed_count,
            "monotonic_violations": monotonic_violations,
            "timestamp_populated_pct": (
                round(100 * timestamp_populated / total_yields, 1)
                if total_yields
                else None
            ),
            "wall_advance_ms": wall_advance_ms,
            "venue_clock_advance_ms": venue_clock_advance_ms,
        }
    finally:
        await _close_quietly(client)


async def orderbook_liveness(symbol: str = "BTC/USDT:USDT", duration_s: float | str = 30.0):
    duration_s = float(duration_s)
    log_path = _open_log("orderbook_liveness")
    venues = _venues_from_env()

    print(f"\n{'=' * 130}")
    print(f"ORDERBOOK LIVENESS — symbol={symbol} duration={duration_s}s — log → {log_path}")
    print(f"Class 1: public WS stream. Per-venue cadence + book sanity. No auth, no orders, no side effects.")
    print("=" * 130)

    results = await asyncio.gather(
        *(_orderbook_liveness_one(v, symbol, duration_s) for v in venues),
        return_exceptions=True,
    )

    print(
        f"\n{'venue':<18}{'symbol':<24}{'yields':>8}{'first_s':>9}"
        f"{'med_ms':>9}{'p95_ms':>9}{'max_ms':>9}"
        f"{'empty':>7}{'cross':>7}{'mono':>6}{'ts%':>6}"
    )
    print("-" * 130)

    def _fmt(v, w, dp=1):
        if v is None:
            return f"{'-':>{w}}"
        return f"{v:>{w}.{dp}f}"

    for canonical, r in zip(venues, results):
        if isinstance(r, Exception):
            print(f"{canonical:<18}  EXCEPTION: {type(r).__name__}: {r}")
            continue
        if "error" in r:
            print(f"{canonical:<18}  {r['error']}")
            _emit(log_path, r)
            continue
        print(
            f"{canonical:<18}"
            f"{r['symbol']:<24}"
            f"{r['yields']:>8}"
            f"{_fmt(r['time_to_first_s'], 9, 3)}"
            f"{_fmt(r['delta_interval_ms_median'], 9)}"
            f"{_fmt(r['delta_interval_ms_p95'], 9)}"
            f"{_fmt(r['delta_interval_ms_max'], 9)}"
            f"{r['empty_top_of_book_count']:>7}"
            f"{r['crossed_book_count']:>7}"
            f"{r['monotonic_violations']:>6}"
            f"{_fmt(r['timestamp_populated_pct'], 6, 0)}"
        )
        _emit(log_path, r)

    print()
    print(f"Inspect raw records:  jq '.' {log_path}")


# ---------------------------------------------------------------------------
# Class 2 — ioc_honor
# ---------------------------------------------------------------------------

# Limit-price offset for the safe-by-construction IOC. 0.99 = "1% below
# best bid" (for buy probes). The book would have to invert by a full
# 1% during transit to match this price — for liquid USDT-linear perps
# at the venues we trade, that's effectively impossible. 1% is also
# universally inside per-venue price-band limits (typically ±5–20%);
# 50% off the touch trips price-band rejections on Binance and others.
SAFE_BUY_PRICE_FRACTION_OF_BID = 0.99

# Time to wait before consulting `fetch_open_orders` (and, for receipt_shape,
# fetch_order). Long enough for the venue to settle the IOC server-side,
# short enough that a 13-venue probe pass takes seconds, not minutes.
POST_DISPATCH_SETTLE_S = 1.5

# fetch_order_book limit. Empirical: kucoinfutures 4.5.51 rejects anything
# other than 20 or 100. Twenty is universally accepted by the other 12
# venues at the depth we care about (5 levels are enough for top-of-book
# anchoring; 20 is a slight over-fetch but cheap).
PROBE_FETCH_ORDER_BOOK_LIMIT = 20

# Notional floor for safe-by-construction probes. Several venues enforce
# a min-notional that's higher than min-lot × price (e.g., XT.COM rejects
# orders below 10 USDT regardless of contract size). Set well above the
# strictest observed floor so probes land cleanly on every venue.
PROBE_MIN_NOTIONAL_USDT = 15.0

# Venue-specific create_order and fetch_order param overrides live in
# `venue_overrides.py` (the single source of truth shared with the
# upcoming receipt_resolver). Use the `create_order_params_for` /
# `fetch_order_params_for` helpers when constructing call params here.


def _select_probe_amount(client, market: dict, symbol: str, ref_price: float) -> tuple:
    """Choose an amount that:
      1. Lands at or above market.limits.amount.min (when set).
      2. Lands at or above market.precision.amount step (matters for venues
         like htx whose `precision.amount = 1` enforces integer contracts
         while `limits.amount.min` reads as a sub-integer fraction).
      3. Yields a notional ≥ PROBE_MIN_NOTIONAL_USDT at `ref_price` (matters
         for venues like xt that gate on min-notional, not min-lot).
      4. Survives `client.amount_to_precision(symbol, _)` rounding.

    Returns (amount, notional_usdt, source_explanation) or
    (None, None, error_string) on failure. `source_explanation` is a
    short string surfaced to the JSONL for forensic inspection.

    All inputs are venue-native units. notional_usdt is true USDT
    notional (amount × contract_size × ref_price)."""
    cs = float(market.get("contractSize") or 1.0)
    min_amt = market.get("limits", {}).get("amount", {}).get("min") or 0.0
    prec = market.get("precision", {}).get("amount")

    # Per-1×-contract notional in USDT
    per_contract_notional = cs * ref_price
    if per_contract_notional <= 0:
        return None, None, "ref_price or contract_size invalid"

    notional_floor_amount = PROBE_MIN_NOTIONAL_USDT / per_contract_notional

    candidates: list[float] = []
    if min_amt and min_amt > 0:
        candidates.append(min_amt)
    # Precision step is a candidate when the venue exposes one. Treat any
    # positive value as a step size — works for both fractional steps
    # (phemex 0.001, mexc 1) and integer steps (htx 1). Empirically some
    # venues set min_amt to a fraction smaller than precision; the precision
    # value is then the actual smallest acceptable size.
    if prec is not None and prec > 0:
        candidates.append(float(prec))
    # 2x buffer on the notional floor — CCXT's amount_to_precision often
    # TRUNCATEs, which can drop us below the floor after rounding. Doubling
    # the seed ensures we land above floor even after a full step truncation.
    candidates.append(notional_floor_amount * 2.0)

    if not candidates:
        return None, None, "no candidate amount could be derived"

    seed = max(candidates)
    try:
        amt = float(client.amount_to_precision(symbol, seed))
    except Exception as e:
        return None, None, f"amount_to_precision raised: {type(e).__name__}: {e}"
    if amt <= 0:
        return None, None, f"amount_to_precision returned {amt} for seed {seed}"

    # Post-rounding sanity: if the rounded amount fell below notional floor,
    # bump by one step until it clears (capped at 5 iterations to avoid
    # pathological loops).
    for _ in range(5):
        notional = amt * per_contract_notional
        if notional >= PROBE_MIN_NOTIONAL_USDT:
            break
        step = float(prec) if (prec is not None and prec > 0) else amt
        amt_next = float(client.amount_to_precision(symbol, amt + step))
        if amt_next <= amt:
            break
        amt = amt_next
    notional = amt * per_contract_notional

    src = (
        f"seed={seed:.6g} (min={min_amt}, prec={prec}, "
        f"notional_floor={notional_floor_amount:.6g})"
    )
    return amt, notional, src


async def _ioc_honor_one(canonical: str, requested_sym: str) -> dict:
    """Class 2. Place a min-lot IOC BUY at 1% below best_bid.

    Verifies, in order:
      1. The placement returns a receipt (vs. raises a 4xx).
      2. The receipt's `status` is closed/canceled/expired (NOT 'open').
      3. After POST_DISPATCH_SETTLE_S, fetch_open_orders does not return
         our order id.
      4. Defensive cancel if (3) fails — and log loud.

    Verdict: `ioc_honored = True` iff (2) AND (3) both pass. False with
    any failure mode — surfaced to the operator as a venue NOT to trade
    until the issue is resolved or the engine grows a venue-specific
    workaround.

    Side effect: one create_order + one fetch_open_orders per venue.
    Cannot fill at 1% below bid for liquid pairs. If somehow filled
    (e.g., unstable book state during the probe), the probe surfaces
    `unexpected_fill: True` in the record and the operator must unwind
    manually."""
    record: dict = {"venue": canonical, "symbol_requested": requested_sym}

    maybe_client = _safe_open_client(canonical)
    if isinstance(maybe_client, dict):
        return maybe_client
    client = maybe_client

    try:
        await client.load_markets()
        symbol = _resolve_symbol(client, requested_sym)
        if symbol is None:
            record["error"] = f"symbol {requested_sym!r} (and fallbacks) not in markets"
            return record
        record["symbol"] = symbol

        market = client.markets[symbol]
        record["min_amount_native"] = market.get("limits", {}).get("amount", {}).get("min")
        record["precision_amount"] = market.get("precision", {}).get("amount")
        record["contract_size"] = market.get("contractSize")

        # Snapshot the touch — the basis for our safe-by-construction price.
        try:
            ob = await client.fetch_order_book(symbol, limit=PROBE_FETCH_ORDER_BOOK_LIMIT)
        except Exception as e:
            record["error"] = f"fetch_order_book failed: {type(e).__name__}: {str(e)[:200]}"
            return record
        if not ob.get("bids") or not ob.get("asks"):
            record["error"] = "empty top of book at probe time — retry later"
            return record

        best_bid = float(ob["bids"][0][0])
        best_ask = float(ob["asks"][0][0])
        record["best_bid"] = best_bid
        record["best_ask"] = best_ask

        # Pick a probe-tradeable amount that satisfies min-lot, integer
        # precision, AND venue min-notional.
        amount, notional_usdt, amt_source = _select_probe_amount(
            client, market, symbol, best_bid
        )
        if amount is None:
            record["error"] = f"amount selection failed: {amt_source}"
            return record
        record["probe_amount_native"] = amount
        record["probe_notional_usdt"] = notional_usdt
        record["amount_source"] = amt_source

        # Round price through per-venue precision.
        raw_buy_price = best_bid * SAFE_BUY_PRICE_FRACTION_OF_BID
        try:
            buy_price = float(client.price_to_precision(symbol, raw_buy_price))
        except Exception as e:
            record["error"] = f"price_to_precision failed: {type(e).__name__}: {e}"
            return record
        record["probe_buy_price"] = buy_price

        # Sanity: rounding can drift the price up — re-verify it's still
        # below the bid by a meaningful margin. If not, abort the probe
        # for this venue (precision-grain too coarse for our safety policy).
        if buy_price >= best_bid * 0.999:
            record["error"] = (
                f"price_to_precision drifted price up to {buy_price} "
                f"(bid={best_bid}); precision too coarse for safe IOC at this size"
            )
            return record

        params = ioc_limit_params_for(canonical, {"timeInForce": "IOC"})
        record["create_order_params"] = params
        try:
            buy_receipt = await client.create_order(
                symbol, "limit", "buy", amount, buy_price, params=params
            )
        except Exception as e:
            record["create_error"] = f"{type(e).__name__}: {str(e)[:300]}"
            # Some venues reject sub-min-notional or out-of-band price even at
            # our 1%-off safety level. Rejection IS information — surface the
            # error class + message so the venue can be flagged in the catalog.
            return record

        # Capture the placement-receipt shape — feeds the future
        # `receipt_shape` probe's catalog directly.
        record["buy_receipt"] = {
            "id": buy_receipt.get("id"),
            "status": buy_receipt.get("status"),
            "filled": buy_receipt.get("filled"),
            "average": buy_receipt.get("average"),
            "cost": buy_receipt.get("cost"),
            "remaining": buy_receipt.get("remaining"),
            "trades_count": len(buy_receipt.get("trades") or []),
            "populated_keys": sorted(
                k for k, v in buy_receipt.items() if v is not None and v != []
            ),
        }
        order_id = buy_receipt.get("id")

        # Defensive: if filled > 0 — should NEVER happen at 1% below bid.
        # Surface loud and continue (the record will alert the operator).
        if buy_receipt.get("filled") and float(buy_receipt.get("filled") or 0) > 0:
            record["unexpected_fill"] = True
            record["unexpected_filled_native"] = float(buy_receipt["filled"])

        # Wait for the venue to settle the IOC server-side, then verify
        # the order is not in fetch_open_orders.
        await asyncio.sleep(POST_DISPATCH_SETTLE_S)
        if order_id and client.has.get("fetchOpenOrders"):
            try:
                open_orders = await client.fetch_open_orders(symbol)
                still_open = [o for o in open_orders if o.get("id") == order_id]
                record["still_open_after_settle"] = len(still_open) > 0
                record["open_orders_count_after_settle"] = len(open_orders)
            except Exception as e:
                record["fetch_open_orders_error"] = (
                    f"{type(e).__name__}: {str(e)[:200]}"
                )
                record["still_open_after_settle"] = None  # unknown — cannot verify
        else:
            record["still_open_after_settle"] = None

        # Defensive cancel if the venue rested it. Logs the cancel result
        # so the operator can see whether the venue at least HONORS cancel.
        if record.get("still_open_after_settle"):
            try:
                cancel_response = await client.cancel_order(order_id, symbol)
                record["defensive_cancel"] = {
                    "ok": True,
                    "id": cancel_response.get("id"),
                    "status": cancel_response.get("status"),
                }
            except Exception as e:
                record["defensive_cancel"] = {
                    "ok": False,
                    "error": f"{type(e).__name__}: {str(e)[:200]}",
                }

        # Verdict: IOC honored iff the order is NOT in open_orders 1.5s after
        # placement. `still_open_after_settle` is the primary signal —
        # receipt `status` is diagnostic but unreliable for sync-null venues
        # (bybit returns status=None on a successfully-canceled IOC, so
        # gating verdict on `terminal_status` would false-FAIL them).
        status = record["buy_receipt"]["status"]
        terminal_status = status in ("closed", "canceled", "expired")
        still_open = record.get("still_open_after_settle")
        if still_open is None:
            record["ioc_honored"] = None  # cannot verify
            record["ioc_honored_caveat"] = (
                "fetch_open_orders unavailable or failed; cannot verify auto-cancel"
            )
        elif still_open is False:
            record["ioc_honored"] = True
            if not terminal_status:
                # Order did auto-cancel but receipt didn't surface it.
                # Important diagnostic: this venue is likely sync-null and
                # the engine MUST follow up with fetch_order(id) on every
                # IOC to read the real outcome.
                record["ioc_honored_caveat"] = (
                    f"order auto-canceled (still_open=False) but receipt "
                    f"status={status!r}; sync-null receipt — fetch_order(id) "
                    f"required for fill resolution"
                )
        else:
            # still_open is True — IOC NOT honored. The order is resting on
            # the venue. This is a structural incompatibility with the
            # engine's slicing model.
            record["ioc_honored"] = False
            record["ioc_honored_caveat"] = (
                f"order STILL OPEN 1.5s after IOC placement "
                f"(status={status!r}); IOC param shape may need verification"
            )

        return record
    except Exception as e:
        record["error"] = f"unhandled: {type(e).__name__}: {str(e)[:300]}"
        return record
    finally:
        await _close_quietly(client)


async def ioc_honor(symbol: str = "BTC/USDT:USDT"):
    log_path = _open_log("ioc_honor")
    venues = _venues_from_env()

    print(f"\n{'=' * 130}")
    print(f"IOC HONOR — symbol={symbol} — log → {log_path}")
    print(
        f"Class 2 (safe-by-construction): "
        f"min-lot IOC BUY at {int((1 - SAFE_BUY_PRICE_FRACTION_OF_BID) * 100)}% below best_bid."
    )
    print(
        "Cannot fill at this price for liquid pairs (book would have to "
        "invert by 1% during transit)."
    )
    print(
        "Verifies: create_order success, status=closed|canceled|expired, "
        "fetch_open_orders auto-cancel."
    )
    print(
        "!  Trading-scope keys required. Consumes rate-limit budget. "
        "Run during quiet hours; do not run mid-trade."
    )
    print(f"!  {len(venues)} venues will receive a real (non-fillable) order. Ctrl-C now to abort.")
    print("=" * 130)

    # Sequential per venue — avoids burst rate-limit cross-talk between
    # different venues' shared infra, and keeps the audit log human-readable.
    results = []
    for v in venues:
        r = await _ioc_honor_one(v, symbol)
        _emit(log_path, r)
        results.append(r)

    print(
        f"\n{'venue':<18}{'symbol':<24}{'status':>10}{'filled':>10}"
        f"{'still_open':>14}{'ioc_honored':>14}"
    )
    print("-" * 130)
    for r in results:
        if r.get("error"):
            print(f"{r['venue']:<18}  ERROR: {r['error']}")
            continue
        if r.get("create_error"):
            print(
                f"{r['venue']:<18}{r.get('symbol', ''):<24}  "
                f"create_error: {r['create_error']}"
            )
            continue
        rcpt = r.get("buy_receipt", {})
        verdict = r.get("ioc_honored")
        verdict_str = (
            "TRUE" if verdict is True
            else "FALSE !" if verdict is False
            else "UNKNOWN"
        )
        print(
            f"{r['venue']:<18}"
            f"{r.get('symbol', ''):<24}"
            f"{str(rcpt.get('status'))[:9]:>10}"
            f"{str(rcpt.get('filled'))[:9]:>10}"
            f"{str(r.get('still_open_after_settle'))[:13]:>14}"
            f"{verdict_str:>14}"
        )
        if r.get("unexpected_fill"):
            print(
                f"  ! UNEXPECTED FILL on {r['venue']}: "
                f"{r['unexpected_filled_native']} native units. "
                f"Inspect {log_path} and unwind manually."
            )

    print()
    print(f"Inspect raw receipts:  jq '.' {log_path}")


# ---------------------------------------------------------------------------
# Roadmap stubs
# ---------------------------------------------------------------------------
# Each stub raises NotImplementedError with a pointer back to the spec
# section in ENGINE_FIELD_NOTES.md. The stubs ARE part of the public probe
# surface — they show up in --help and the registry — so the operator can
# see at a glance what's planned vs. implemented. Filling them in is the
# next iteration of the probing methodology, not this one.


def _classify_placement(receipt: dict) -> str:
    """Classify a placement receipt's resolution mode.

    The only axis that matters operationally is **does the engine have
    enough info to know the fill outcome from the placement alone, or
    must it call fetch_order(id) before reading state?**

    Concretely:
      filled is None     → engine has no fill state. MUST fetch_order.
      status == 'open'   → venue still resolving. MUST fetch_order (and may
                           need backoff polling until terminal).
      otherwise          → filled is a number; placement is authoritative.

    Returned values match Table B's R-Mode column:
      sync-final  — filled populated AND > 0 (real fill captured in placement).
      sync-zero   — filled populated AND == 0 (auto-canceled IOC with no fill;
                    venue may report status as terminal OR as None — coinex
                    famously omits status but populates filled+cost+fee).
      sync-null   — filled is None. Engine MUST fetch_order(id).
      eventual    — status == 'open'. Venue still working; needs poll.
      unknown     — none of the above; surfaced as-is for forensic review.
    """
    status = receipt.get("status")
    filled = receipt.get("filled")
    if status == "open":
        return "eventual"
    if filled is None:
        return "sync-null"
    if float(filled or 0) == 0:
        return "sync-zero"
    return "sync-final"


def _diff_resolved_keys(placement: dict, resolved: dict) -> dict:
    """Compare which keys gained populated values between placement and
    fetch_order response. Returns a dict listing keys that were None/empty
    in placement but populated in resolved — these are the fields the
    engine GAINED by performing the fetch_order follow-up.

    For sync-final venues the diff is mostly empty. For sync-null venues
    the diff is the full set of fill fields. For eventual venues the
    diff includes `status` transitioning from 'open' → terminal."""
    interesting = (
        "status", "filled", "average", "cost", "remaining",
        "fee", "trades", "price", "amount",
    )

    def _empty(v) -> bool:
        if v is None:
            return True
        if v == [] or v == {}:
            return True
        return False

    gained = {}
    for k in interesting:
        if _empty(placement.get(k)) and not _empty(resolved.get(k)):
            gained[k] = resolved.get(k)
    return gained


async def _receipt_shape_one(canonical: str, requested_sym: str) -> dict:
    """Class 2. Extends ioc_honor — places the same far-from-spread IOC,
    then captures BOTH the placement receipt AND the fetch_order(id)
    response after POST_DISPATCH_SETTLE_S. Classifies the venue's
    resolution mode {sync-final | sync-zero | sync-null | eventual}.

    Same side-effect profile as ioc_honor — no fill expected at 1% below
    bid for liquid pairs.

    Anchor case: bybit BTC/USDT:USDT 2026-05-07 21:05:39 — placement
    receipt all-None. Probe surfaces this as `placement_resolution =
    sync-null` AND `fetch_order_required = True` BEFORE any live trade."""
    record: dict = {"venue": canonical, "symbol_requested": requested_sym}

    maybe_client = _safe_open_client(canonical)
    if isinstance(maybe_client, dict):
        return maybe_client
    client = maybe_client

    try:
        await client.load_markets()
        symbol = _resolve_symbol(client, requested_sym)
        if symbol is None:
            record["error"] = f"symbol {requested_sym!r} (and fallbacks) not in markets"
            return record
        record["symbol"] = symbol

        market = client.markets[symbol]
        record["min_amount_native"] = market.get("limits", {}).get("amount", {}).get("min")
        record["precision_amount"] = market.get("precision", {}).get("amount")
        record["contract_size"] = market.get("contractSize")

        try:
            ob = await client.fetch_order_book(symbol, limit=PROBE_FETCH_ORDER_BOOK_LIMIT)
        except Exception as e:
            record["error"] = f"fetch_order_book failed: {type(e).__name__}: {str(e)[:200]}"
            return record
        if not ob.get("bids") or not ob.get("asks"):
            record["error"] = "empty top of book at probe time — retry later"
            return record
        best_bid = float(ob["bids"][0][0])
        record["best_bid"] = best_bid

        amount, notional_usdt, amt_source = _select_probe_amount(
            client, market, symbol, best_bid
        )
        if amount is None:
            record["error"] = f"amount selection failed: {amt_source}"
            return record
        record["probe_amount_native"] = amount
        record["probe_notional_usdt"] = notional_usdt
        record["amount_source"] = amt_source

        raw_buy_price = best_bid * SAFE_BUY_PRICE_FRACTION_OF_BID
        try:
            buy_price = float(client.price_to_precision(symbol, raw_buy_price))
        except Exception as e:
            record["error"] = f"price_to_precision failed: {type(e).__name__}: {e}"
            return record
        record["probe_buy_price"] = buy_price

        if buy_price >= best_bid * 0.999:
            record["error"] = (
                f"price_to_precision drifted price up to {buy_price} "
                f"(bid={best_bid}); precision too coarse for safe IOC at this size"
            )
            return record

        params = ioc_limit_params_for(canonical, {"timeInForce": "IOC"})
        record["create_order_params"] = params
        try:
            placement = await client.create_order(
                symbol, "limit", "buy", amount, buy_price, params=params
            )
        except Exception as e:
            record["create_error"] = f"{type(e).__name__}: {str(e)[:300]}"
            return record

        # Capture the placement receipt at full fidelity. Strip the `info`
        # blob's nested values into a flat sample (truncated repr) so the
        # JSONL stays human-readable while still surfacing every venue-
        # specific key.
        placement_info = placement.get("info") or {}
        record["placement"] = {
            "unified": {k: placement.get(k) for k in (
                "id", "clientOrderId", "status",
                "side", "type", "timeInForce",
                "price", "amount",
                "filled", "remaining", "average", "cost",
                "fee",
            )},
            "trades_count": len(placement.get("trades") or []),
            "info_keys": sorted(placement_info.keys()),
            "info_sample": {
                k: (str(v)[:80] + "...") if len(str(v)) > 80 else str(v)
                for k, v in placement_info.items()
            },
        }
        record["placement_populated_keys"] = sorted(
            k for k, v in placement.items()
            if v is not None and v != [] and v != {}
        )

        order_id = placement.get("id")
        record["placement_resolution"] = _classify_placement(placement)
        # fetch_order_required is determined by the placement classification —
        # set it BEFORE attempting fetch_order so a fetch_order failure (e.g.,
        # coinex purges canceled IOCs from history within ~1s, hits "order not
        # exists") doesn't leave this field unset.
        record["fetch_order_required"] = record["placement_resolution"] in (
            "sync-null", "eventual",
        )

        # Defensive: if filled > 0 — should NEVER happen at 1% below bid.
        if placement.get("filled") and float(placement.get("filled") or 0) > 0:
            record["unexpected_fill"] = True
            record["unexpected_filled_native"] = float(placement["filled"])

        # Settle, then call fetch_order(id) for the resolved state.
        await asyncio.sleep(POST_DISPATCH_SETTLE_S)
        if not order_id:
            record["fetch_order_skipped"] = "placement returned no id"
        elif not client.has.get("fetchOrder"):
            record["fetch_order_skipped"] = "venue does not advertise fetchOrder"
        else:
            fetch_order_params = fetch_order_params_for(canonical)
            try:
                resolved = await client.fetch_order(
                    order_id, symbol, params=fetch_order_params
                )
                resolved_info = resolved.get("info") or {}
                record["resolved"] = {
                    "unified": {k: resolved.get(k) for k in (
                        "id", "clientOrderId", "status",
                        "side", "type", "timeInForce",
                        "price", "amount",
                        "filled", "remaining", "average", "cost",
                        "fee",
                    )},
                    "trades_count": len(resolved.get("trades") or []),
                    "info_keys": sorted(resolved_info.keys()),
                    "info_sample": {
                        k: (str(v)[:80] + "...") if len(str(v)) > 80 else str(v)
                        for k, v in resolved_info.items()
                    },
                }
                record["resolved_populated_keys"] = sorted(
                    k for k, v in resolved.items()
                    if v is not None and v != [] and v != {}
                )
                record["fields_gained_from_fetch_order"] = _diff_resolved_keys(
                    placement, resolved
                )
                # Refine the verdict if fetch_order surfaced fields that the
                # placement omitted — even sync-zero venues might gain status
                # via fetch_order, but for sync-null this is the load-bearing
                # case.
                gained = record["fields_gained_from_fetch_order"]
                if "filled" in gained or "status" in gained:
                    record["fetch_order_required"] = True
            except Exception as e:
                record["fetch_order_error"] = (
                    f"{type(e).__name__}: {str(e)[:200]}"
                )

        # Defensive cleanup if somehow still open.
        if order_id and client.has.get("fetchOpenOrders"):
            try:
                open_orders = await client.fetch_open_orders(symbol)
                still_open = [o for o in open_orders if o.get("id") == order_id]
                record["still_open_after_fetch"] = len(still_open) > 0
                if still_open:
                    try:
                        await client.cancel_order(order_id, symbol)
                        record["defensive_cancel"] = "ok"
                    except Exception as e:
                        record["defensive_cancel"] = f"FAILED: {type(e).__name__}: {e}"
            except Exception as e:
                record["fetch_open_orders_error"] = (
                    f"{type(e).__name__}: {str(e)[:200]}"
                )

        return record
    except Exception as e:
        record["error"] = f"unhandled: {type(e).__name__}: {str(e)[:300]}"
        return record
    finally:
        await _close_quietly(client)


async def receipt_shape(symbol: str = "BTC/USDT:USDT"):
    """Class 2. Per-venue receipt-resolution catalog. Places a
    safe-by-construction IOC, captures the full placement receipt, then
    follows up with fetch_order(id) and captures that response. Classifies
    each venue {sync-final | sync-zero | sync-null | eventual}."""
    log_path = _open_log("receipt_shape")
    venues = _venues_from_env()

    print(f"\n{'=' * 130}")
    print(f"RECEIPT SHAPE — symbol={symbol} — log → {log_path}")
    print("Class 2 (safe-by-construction): same min-lot far-from-spread IOC as ioc_honor,")
    print("plus a fetch_order(id) follow-up after settle. Classifies per-venue R-Mode.")
    print("Inherits ioc_honor's safety guarantees (1% below bid; cannot fill on liquid pairs).")
    print(f"!  {len(venues)} venues will receive a real (non-fillable) order. Ctrl-C to abort.")
    print("=" * 130)

    results = []
    for v in venues:
        r = await _receipt_shape_one(v, symbol)
        _emit(log_path, r)
        results.append(r)

    print(
        f"\n{'venue':<18}{'symbol':<24}{'placement R-Mode':>18}"
        f"{'fetch_order req':>18}{'gained_fields':>30}"
    )
    print("-" * 130)
    for r in results:
        if r.get("error"):
            print(f"{r['venue']:<18}  ERROR: {r['error']}")
            continue
        if r.get("create_error"):
            print(
                f"{r['venue']:<18}{r.get('symbol', ''):<24}  "
                f"create_error: {r['create_error'][:80]}"
            )
            continue
        gained = r.get("fields_gained_from_fetch_order") or {}
        gained_keys = ",".join(sorted(gained.keys())) if gained else "(none)"
        print(
            f"{r['venue']:<18}"
            f"{r.get('symbol', ''):<24}"
            f"{r.get('placement_resolution', '?'):>18}"
            f"{str(r.get('fetch_order_required', '?')):>18}"
            f"{gained_keys[:28]:>30}"
        )
        if r.get("unexpected_fill"):
            print(
                f"  ! UNEXPECTED FILL on {r['venue']}: "
                f"{r['unexpected_filled_native']} native units. Inspect log + unwind."
            )

    print()
    print(f"Inspect raw receipts:  jq '.' {log_path}")


async def reduceonly_zero(symbol: str = "BTC/USDT:USDT"):
    """Class 2 stub. Place reduceOnly IOC on a venue with zero position.
    Captures the rejection's CCXT exception class, venue-side error code,
    and whether the receipt surfaces the rejection or silently succeeds.

    Critical for hedge-mode venues (BingX): without `positionSide`, a
    reduceOnly IOC on zero position must reject. If a venue silently
    OPENS a counter-position instead, the engine's exit path is broken."""
    raise NotImplementedError(
        "reduceonly_zero is roadmap. See ENGINE_FIELD_NOTES.md → "
        "'Open empirical questions Q3'."
    )


async def set_leverage_idempotent(symbol: str = "BTC/USDT:USDT"):
    """Class 2 stub. set_leverage twice with the same value; verify
    idempotency and that the response shape is consistent. Catches venues
    that silently rotate to a tier-max default or that need
    `marginMode`/`posSide` qualifiers to set leverage at all."""
    raise NotImplementedError(
        "set_leverage_idempotent is roadmap. See ENGINE_FIELD_NOTES.md → "
        "'Per-venue execution spec — Table A'."
    )


async def precision_audit(symbol: str = "BTC/USDT:USDT"):
    """Class 1 stub. For each venue, exercise amount_to_precision() on
    inputs at, just-above, and just-below min-lot. Reveals the rounding
    direction (TRUNCATE / ROUND / ROUND_UP). A ROUND_UP venue at
    binary-search ceiling can produce values exceeding the safe ceiling."""
    raise NotImplementedError(
        "precision_audit is roadmap. See ENGINE_FIELD_NOTES.md → "
        "'Open empirical questions Q5'."
    )


async def cancel_phantom(symbol: str = "BTC/USDT:USDT"):
    """Class 2 stub. cancel_order with a fabricated nonexistent id.
    Catalogs the rejection semantics: 200-OK silent, OrderNotFound, raw 4xx.
    Matters for engine abort flows that race the venue's auto-cancel of an IOC."""
    raise NotImplementedError("cancel_phantom is roadmap.")


async def rate_limit_signature(symbol: str = "BTC/USDT:USDT"):
    """Class 2 stub. Burst N (configurable) cheap REST calls per venue,
    capture the first 429/418 response. Reveals per-venue sustained-burst
    thresholds and whether `Retry-After` is honored by CCXT."""
    raise NotImplementedError(
        "rate_limit_signature is roadmap. See ENGINE_FIELD_NOTES.md → "
        "'Open empirical questions Q8'."
    )


async def clock_skew():
    """Class 1 stub. Compare local UTC against fetch_time() per venue.
    Drift > 500 ms causes the staleness watchdog to fire false positives."""
    raise NotImplementedError(
        "clock_skew is roadmap. See ENGINE_FIELD_NOTES.md → "
        "'Open empirical questions Q9'."
    )


async def reconnect_behavior(symbol: str = "BTC/USDT:USDT"):
    """Class 2 stub. Subscribe L2; capture baseline; force socket close;
    measure: time-to-first-delta after reconnect, whether the cached book
    matches pre-disconnect state (stale resume) or differs (re-snapshot)."""
    raise NotImplementedError(
        "reconnect_behavior is roadmap. See ENGINE_FIELD_NOTES.md → "
        "'Open empirical questions Q7'."
    )


async def min_lot_live(
    symbol: str = "BTC/USDT:USDT",
    *,
    venue: str | None = None,
    authorized: bool = False,
):
    """Class 3 — capital at risk. Place a min-lot IOC at top-of-book on
    one venue, capture the receipt, then immediately unwind with a
    reduceOnly IOC. Costs a real fill (fees + temporary delta).

    Gated behind --I-AM-FUNDED-AND-AUTHORIZED-FOR-{VENUE}. Single venue
    per invocation; no batch fan-out.

    REQUIRES: trading-scope API keys + funded perp account on `venue`."""
    if not authorized:
        raise PermissionError(
            f"min_lot_live is capital-at-risk (Class 3). Required handshake: "
            f"--I-AM-FUNDED-AND-AUTHORIZED-FOR-{(venue or 'X').upper()}. "
            f"See ENGINE_FIELD_NOTES.md → 'Probe safety classes — Class 3'."
        )
    raise NotImplementedError(
        "min_lot_live is roadmap. Implementation: place single min-lot IOC at "
        "top-of-book; capture placement receipt; follow up with fetch_order(id); "
        "dispatch reduceOnly IOC to unwind. Asymmetric residual triggers "
        "Pushover P2 + halt."
    )


# ---------------------------------------------------------------------------
# Class 3 — fill_resolution diagnostic
# ---------------------------------------------------------------------------

# Notional target for the diagnostic IOC. Small enough to keep operator
# capital risk bounded; large enough to clear venue min-notional floors.
FILL_DIAG_NOTIONAL_USDT = 8.0

# Polling cadence for fetch_order. 100ms balances precision (catch lag
# transitions) with rate-limit budget.
FILL_DIAG_POLL_INTERVAL_S = 0.10
FILL_DIAG_POLL_DURATION_S = 5.0


async def fill_resolution(
    symbol: str = "XRP/USDT:USDT",
    *,
    venue: str | None = None,
    authorized: bool = False,
):
    """Class 3 — capital at risk. Verifies that fetch_order(id) actually
    surfaces fills for IOC orders that fill server-side.

    Methodology gap fixed: the original `receipt_shape` probe used a
    1%-below-bid (NON-filling) IOC, so it could only verify the
    receipt-resolution PATH worked, not whether the resolution
    actually returned correct fill state for filling orders. Anchor:
    2026-05-10 BingX × XT silent-residual incident — XT's fetch_order
    returned `executedQty: 0` for IOCs that ACTUALLY FILLED 10 XRP
    each (operator UI verified 30 XRP short on XT after 3 cycles).
    XT was previously classified `sync-null fetch_order required` by
    receipt_shape — that classification was technically correct
    (placement returned only id) but USELESS because fetch_order
    returns wrong data on filled IOCs.

    This probe:
      1. Places a small SELL IOC at the BEST BID on `venue`. On any
         liquid USDT-linear pair this matches immediately and fills
         (or partial-fills) the IOC.
      2. Polls `fetch_order(id)` at FILL_DIAG_POLL_INTERVAL_S for up to
         FILL_DIAG_POLL_DURATION_S, capturing the `filled` value at
         each step.
      3. After 1.5s, calls `fetch_my_trades(symbol)` once to see if
         the trades-history endpoint surfaces the fill correctly.
      4. Reports: did fetch_order's `filled` ever reflect reality?
         Did fetch_my_trades show the trade?

    Defensive cleanup: if the SELL filled, immediately dispatches a
    reduceOnly BUY at the ask to flatten. If the SELL didn't fill (IOC
    auto-canceled at venue), no cleanup needed.

    REQUIRES: trading-scope keys + funded perp account on `venue`.
    """
    if not venue:
        raise ValueError("fill_resolution requires --venue=X argument")
    if not authorized:
        raise PermissionError(
            f"fill_resolution is capital-at-risk (Class 3). Required handshake: "
            f"--I-AM-FUNDED-AND-AUTHORIZED-FOR-{venue.upper()}."
        )

    log_path = _open_log("fill_resolution")
    print(f"\n{'=' * 110}")
    print(f"FILL RESOLUTION DIAG — venue={venue} symbol={symbol}")
    print(f"  log → {log_path}")
    print(f"  Class 3: places a small filling IOC, polls fetch_order to characterize fill-detection lag.")
    print(f"  Notional target: ~{FILL_DIAG_NOTIONAL_USDT} USDT  poll: {FILL_DIAG_POLL_INTERVAL_S*1000:.0f}ms × {FILL_DIAG_POLL_DURATION_S}s")
    print("=" * 110)

    record: dict = {"venue": venue, "symbol": symbol}

    maybe_client = _safe_open_client(venue)
    if isinstance(maybe_client, dict):
        print(f"  client init failed: {maybe_client}")
        _emit(log_path, {**record, "verdict": "INST_FAIL", "error": maybe_client})
        return
    client = maybe_client

    try:
        await client.load_markets()
        if symbol not in client.markets:
            print(f"  symbol {symbol!r} not in {venue} markets")
            _emit(log_path, {**record, "verdict": "NO_MARKET"})
            return
        market = client.markets[symbol]
        record["contract_size"] = market.get("contractSize")
        record["amount_min"] = market.get("limits", {}).get("amount", {}).get("min")
        record["cost_min"] = market.get("limits", {}).get("cost", {}).get("min")

        # Snapshot the touch.
        ob = await client.fetch_order_book(symbol, limit=20)
        if not ob.get("bids") or not ob.get("asks"):
            print(f"  empty top of book at probe time")
            _emit(log_path, {**record, "verdict": "EMPTY_TOB"})
            return
        best_bid = float(ob["bids"][0][0])
        best_ask = float(ob["asks"][0][0])
        record["best_bid"] = best_bid
        record["best_ask"] = best_ask

        # Size to clear venue min-notional, then convert base→native.
        # Three constraints: notional floor, min-lot, and precision step.
        # gate's XRP perp publishes amount.min=0 but precision.amount=1
        # (integer contracts), so 0.56-contract seed gets rejected by
        # amount_to_precision unless we also bump up to precision.
        cs = float(market.get("contractSize") or 1.0)
        per_contract_notional = cs * best_bid
        amt_min = market.get("limits", {}).get("amount", {}).get("min") or 0
        amt_prec = market.get("precision", {}).get("amount") or 0
        seed = max(
            FILL_DIAG_NOTIONAL_USDT / per_contract_notional,
            amt_min,
            amt_prec,  # precision step is also a floor on smallest valid order
        )
        amount_native = float(client.amount_to_precision(symbol, seed))
        record["amount_native"] = amount_native
        record["amount_notional_usdt"] = amount_native * per_contract_notional

        # Dispatch SELL IOC at the bid. CCXT auto-derives positionSide=SHORT
        # for venues that need it (e.g., XT). Sells AT the bid match against
        # resting bid liquidity = full fill on liquid pairs.
        sell_price = float(client.price_to_precision(symbol, best_bid))
        params = ioc_limit_params_for(venue, {"timeInForce": "IOC"})
        record["sell_price"] = sell_price
        record["sell_params"] = params
        print(f"\n  Placing SELL {amount_native} {symbol} @ {sell_price} (notional ~${record['amount_notional_usdt']:.2f})")
        place_ts = time.monotonic()
        try:
            placement = await client.create_order(
                symbol, "limit", "sell", amount_native, sell_price, params=params
            )
        except Exception as e:
            print(f"  create_order FAILED ({type(e).__name__}): {str(e)[:300]}")
            _emit(log_path, {**record, "verdict": "CREATE_FAIL", "error": str(e)[:500]})
            return
        order_id = placement.get("id")
        record["order_id"] = order_id
        record["placement"] = {
            k: placement.get(k)
            for k in ("status", "filled", "average", "cost", "remaining", "amount")
        }
        print(f"  placement: id={order_id}  status={placement.get('status')}  filled={placement.get('filled')}  remaining={placement.get('remaining')}")

        # Phase 1: poll fetch_order
        print(f"\n  Polling fetch_order at +0ms intervals of {FILL_DIAG_POLL_INTERVAL_S*1000:.0f}ms:")
        poll_results = []
        deadline = place_ts + FILL_DIAG_POLL_DURATION_S
        attempt = 0
        first_nonzero_ms: Optional[float] = None
        fetch_params = fetch_order_params_for(venue)
        while time.monotonic() < deadline:
            attempt += 1
            t = time.monotonic()
            try:
                resolved = await client.fetch_order(order_id, symbol, params=fetch_params)
                lag_ms = (t - place_ts) * 1000.0
                fill = resolved.get("filled")
                status = resolved.get("status")
                # Pull venue-native raw fields too — CCXT's `filled` may be wrong
                # but raw `executedQty`/`leavingQty` may give the truth.
                info = resolved.get("info") or {}
                raw_executed = info.get("executedQty") if isinstance(info, dict) else None
                raw_leaving = info.get("leavingQty") if isinstance(info, dict) else None
                raw_state = info.get("state") if isinstance(info, dict) else None
                row = {
                    "attempt": attempt, "lag_ms": lag_ms,
                    "filled": fill, "status": status,
                    "info_executedQty": raw_executed,
                    "info_leavingQty": raw_leaving,
                    "info_state": raw_state,
                }
                poll_results.append(row)
                print(f"    +{lag_ms:6.0f}ms  filled={fill}  status={status}  "
                      f"info.executedQty={raw_executed}  info.leavingQty={raw_leaving}  info.state={raw_state}")
                if fill is not None and float(fill or 0) > 0 and first_nonzero_ms is None:
                    first_nonzero_ms = lag_ms
            except Exception as e:
                lag_ms = (t - place_ts) * 1000.0
                print(f"    +{lag_ms:6.0f}ms  EXC: {type(e).__name__}: {str(e)[:100]}")
                poll_results.append({"attempt": attempt, "lag_ms": lag_ms, "error": str(e)[:200]})
            await asyncio.sleep(FILL_DIAG_POLL_INTERVAL_S)
        record["poll_results"] = poll_results
        record["fetch_order_first_nonzero_ms"] = first_nonzero_ms

        # Phase 2: fetch_my_trades — alternative endpoint
        print(f"\n  Cross-checking via fetch_my_trades(symbol, since=t-5s):")
        try:
            since_ms = int((place_ts - 5.0) * 1000.0)
            trades = await client.fetch_my_trades(symbol, since=since_ms)
            matching = [t for t in (trades or []) if t.get("order") == order_id]
            record["trades_total_returned"] = len(trades or [])
            record["trades_matching_order"] = len(matching)
            total_filled_via_trades = sum(float(t.get("amount") or 0) for t in matching)
            record["fill_total_via_trades"] = total_filled_via_trades
            print(f"    fetch_my_trades returned {len(trades or [])} total trades; "
                  f"{len(matching)} match order_id={order_id}; sum(amount) = {total_filled_via_trades}")
            if matching:
                for t in matching:
                    print(f"      trade: amount={t.get('amount')} price={t.get('price')} cost={t.get('cost')} ts={t.get('datetime')}")
        except Exception as e:
            print(f"    fetch_my_trades EXC: {type(e).__name__}: {str(e)[:200]}")
            record["fetch_my_trades_error"] = str(e)[:300]

        # Phase 3: defensive cleanup (close any opened short)
        print(f"\n  Checking for open short position to clean up...")
        try:
            positions = await client.fetch_positions([symbol])
            short_size = 0.0
            for p in positions:
                if p.get("symbol") == symbol and (p.get("side") == "short" or float(p.get("contracts") or 0) > 0):
                    short_size = float(p.get("contracts") or 0)
                    break
            print(f"    fetch_positions: short_size={short_size}")
            if short_size > 0:
                close_price = float(client.price_to_precision(symbol, best_ask))
                close_params = ioc_limit_params_for(venue, {"timeInForce": "IOC", "reduceOnly": True})
                print(f"    Closing: BUY {short_size} @ {close_price} (reduceOnly)")
                try:
                    close = await client.create_order(symbol, "limit", "buy", short_size, close_price, params=close_params)
                    print(f"    close placement: id={close.get('id')}  status={close.get('status')}  filled={close.get('filled')}")
                    record["cleanup"] = {"placed": True, "id": close.get("id"), "status": close.get("status"), "filled": close.get("filled")}
                except Exception as e:
                    print(f"    cleanup FAILED: {type(e).__name__}: {str(e)[:200]}")
                    record["cleanup"] = {"placed": False, "error": str(e)[:300]}
        except Exception as e:
            print(f"    fetch_positions EXC: {type(e).__name__}: {str(e)[:200]}")
            record["cleanup_check_error"] = str(e)[:300]

        # Verdict
        print(f"\n{'=' * 110}")
        if first_nonzero_ms is not None:
            print(f"  VERDICT: fetch_order surfaced the fill at +{first_nonzero_ms:.0f}ms.")
            record["verdict"] = "FETCH_ORDER_RELIABLE"
        elif record.get("fill_total_via_trades", 0) > 0:
            print(f"  VERDICT: fetch_order NEVER surfaced the fill (filled stayed 0/None for {FILL_DIAG_POLL_DURATION_S}s)")
            print(f"           BUT fetch_my_trades surfaced {record['fill_total_via_trades']} units of fill on the order.")
            print(f"           → fetch_order is BROKEN on this venue; receipt_resolver must fall back to fetch_my_trades.")
            record["verdict"] = "FETCH_ORDER_BROKEN_TRADES_RELIABLE"
        else:
            print(f"  VERDICT: Neither fetch_order nor fetch_my_trades surfaced any fill.")
            print(f"           Either the IOC auto-canceled at venue (no match), or both endpoints are stale.")
            record["verdict"] = "NO_FILL_DETECTED"
        print("=" * 110)
        _emit(log_path, record)
    except Exception as e:
        record["verdict"] = "EXCEPTION"
        record["exception"] = f"{type(e).__name__}: {e}"
        _emit(log_path, record)
        print(f"\n  EXCEPTION: {type(e).__name__}: {e}")
        raise
    finally:
        await _close_quietly(client)


# ---------------------------------------------------------------------------
# Class 3 — cross_venue_smoketest
# ---------------------------------------------------------------------------

# Mid-of-range of the operator's "~15 to 20 USDT notional" directive. Acts as
# a FLOOR on the smoketest size — large-cap pairs with 0.001-BTC min-lot will
# end up sized off min-lot (~$60), but smaller coins fall back to this.
SMOKETEST_NOTIONAL_USDT = 18.0

# Headroom above the binding floor (whichever of {min_lot_long, min_lot_short,
# notional_floor} dominates). Survives CCXT amount_to_precision rounding —
# precision is usually TRUNCATE; a 1.5× seed lands above floor even after a
# full-step truncation. Also lets DEPTH_DISCOUNT's no-haircut-at-residuals
# fallback (project_slice) dispatch the full slice in one cycle.
SMOKETEST_SAFETY_MULTIPLIER = 1.5

# 1× leverage = minimal capital risk. Operator can bump pre-run if desired.
SMOKETEST_LEVERAGE = 1

# basis_floor_bps low enough to fire immediately regardless of the natural
# spread direction. -5000 bps = -50% — no real cross-venue book ever shows
# this; the engine fires the slice on the first cycle.
SMOKETEST_BASIS_FLOOR_BPS = -5000

# Per-cycle deadline. Long enough for a sync-null venue's fetch_order
# round-trip (~200-500ms each leg) plus SLICE_COOLDOWN_S. With one cycle to
# fill at this size, 30s is generous.
SMOKETEST_MAX_DURATION_S = 30

# Hold time between entry and exit. Just long enough for the operator to
# eyeball the entry log lines before exit fires.
SMOKETEST_INTER_CYCLE_WAIT_S = 5


def _parse_venue_symbol_spec(spec: str) -> tuple[str, str]:
    """Parse 'venue:symbol' from CLI flag. Symbol may itself contain ':'
    (e.g., 'BTC/USDT:USDT' for USDT-linear perps), so split on the FIRST
    colon only. Returns (lowercased_venue, symbol)."""
    if not spec or ":" not in spec:
        raise ValueError(
            f"spec must be 'venue:symbol' (e.g. binance:BTC/USDT:USDT), got {spec!r}"
        )
    venue, symbol = spec.split(":", 1)
    return venue.lower(), symbol


class _FakeRequest:
    """Minimal stand-in for aiohttp.web.Request — supplies an awaitable
    json() that returns the canned payload. Lets the probe drive engine
    handlers (handle_warmup / handle_entry / handle_exit) through the
    same code path as live operator IPC, without spinning up the HTTP
    server or making real socket round-trips."""
    def __init__(self, payload: dict):
        self._payload = payload

    async def json(self) -> dict:
        return self._payload


async def _call_handler(handler, payload: dict) -> tuple[int, dict]:
    """Drive an engine handler with a fake request. Returns (status, body).
    body is the deserialized JSON of the handler's web.json_response."""
    response = await handler(_FakeRequest(payload))
    return response.status, json.loads(response.text)


async def cross_venue_smoketest(
    long_spec: str | None = None,
    short_spec: str | None = None,
    *,
    authorized_long: bool = False,
    authorized_short: bool = False,
):
    """Class 3 — capital at risk. End-to-end live pair-trade through the
    actual production pipeline.

    Architecture: instantiate ArbitrageEngine, drive handle_warmup →
    handle_entry → handle_exit through fake aiohttp requests. NO order
    dispatch is reimplemented here — every line of the production
    execution path runs as it would for a live trade.

    Sizing: max(both legs' min-lot, ~SMOKETEST_NOTIONAL_USDT/mid_price)
    × SMOKETEST_SAFETY_MULTIPLIER. Small enough for discretionary
    capital loss, large enough to clear venue min-notional floors after
    CCXT precision rounding.

    Engine fires with basis_floor_bps=-5000 → slice fires immediately
    on the first cycle regardless of current cross-venue spread.

    Verifies, in order:
      1. Warmup succeeds on both venues (auth + leverage + L2 stream).
      2. Entry achieves filled_base > 0 (delta-neutral position opened).
      3. Exit achieves filled_base ≈ entry filled_base (within dust).
      4. engine.positions[base_coin] is cleared after exit.
      5. positions.json on disk is consistent with cleared in-memory state.

    Aborts pre-trade if positions.json already has a row for the
    smoketest's pair_key — the operator must manually unwind first.

    REQUIRES: trading-scope keys + funded perp accounts on BOTH venues.
    """
    if not long_spec or not short_spec:
        raise ValueError(
            "cross_venue_smoketest requires --long_spec and --short_spec "
            "(e.g. --long_spec=binance:BTC/USDT:USDT "
            "--short_spec=bybit:BTC/USDT:USDT)"
        )
    long_venue, long_symbol = _parse_venue_symbol_spec(long_spec)
    short_venue, short_symbol = _parse_venue_symbol_spec(short_spec)

    if not (authorized_long and authorized_short):
        raise PermissionError(
            f"cross_venue_smoketest is capital-at-risk (Class 3). Required: "
            f"--I-AM-FUNDED-AND-AUTHORIZED-FOR-{long_venue.upper()} AND "
            f"--I-AM-FUNDED-AND-AUTHORIZED-FOR-{short_venue.upper()}."
        )

    log_path = _open_log("cross_venue_smoketest")
    print(f"\n{'=' * 110}")
    print(f"CROSS VENUE SMOKETEST — log → {log_path}")
    print(f"  long:    {long_venue}:{long_symbol}")
    print(f"  short:   {short_venue}:{short_symbol}")
    print(
        f"  basis_floor: {SMOKETEST_BASIS_FLOOR_BPS}bps (fire immediately)  "
        f"max_duration: {SMOKETEST_MAX_DURATION_S}s  "
        f"leverage: {SMOKETEST_LEVERAGE}x"
    )
    print(f"  Class 3: real fill on both legs. Engine logs follow.")
    print("=" * 110)

    # Local imports — engine.py / utils.py are heavy and pull in CCXT Pro
    # transitively. Defer until smoketest is actually invoked.
    from engine import ArbitrageEngine
    from utils import load_state

    record: dict = {
        "long_spec": long_spec,
        "short_spec": short_spec,
    }

    engine = ArbitrageEngine()
    try:
        # --- Pre-flight: refuse to run if our pair already has prior state ---
        # We will verify cleanup against engine.positions; an existing entry
        # would invalidate that check. Other pairs are left untouched.
        # We don't know pos_key yet (need markets loaded). Defer this check
        # to immediately after warmup builds the pair primitives.

        # ----------------------- Warmup -----------------------
        warmup_payload = {
            "legs": [[long_venue, long_symbol], [short_venue, short_symbol]],
            "leverage": SMOKETEST_LEVERAGE,
        }
        status, body = await _call_handler(engine.handle_warmup, warmup_payload)
        record["warmup_status"] = status
        record["warmup_body"] = body
        if status != 200:
            print(f"\nSMOKETEST FAIL (warmup): status={status} body={body}")
            _emit(log_path, {**record, "verdict": "WARMUP_FAILED"})
            return

        # ----------------------- Sizing -----------------------
        long_market = engine.exchanges[long_venue].markets[long_symbol]
        short_market = engine.exchanges[short_venue].markets[short_symbol]
        long_leg = ExecutionLeg.from_market(long_venue, long_market)
        short_leg = ExecutionLeg.from_market(short_venue, short_market)
        pair = ExecutionPair(long=long_leg, short=short_leg)
        pos_key = pair.key

        existing_pos = engine.positions.get(pos_key)
        if existing_pos:
            print(
                f"\nSMOKETEST ABORT: positions.json already has a row for {pos_key!r} "
                f"(amount_base={existing_pos.get('amount_base')}). Manually unwind "
                f"before re-running so the cleanup verification step is valid."
            )
            record["existing_position"] = existing_pos
            _emit(log_path, {**record, "verdict": "ABORT_PRIOR_STATE"})
            return

        book_long = engine.order_books[(long_venue, long_symbol)]
        book_short = engine.order_books[(short_venue, short_symbol)]
        long_mid_native = (float(book_long.bids[0][0]) + float(book_long.asks[0][0])) / 2.0
        short_mid_native = (float(book_short.bids[0][0]) + float(book_short.asks[0][0])) / 2.0
        mid_price_base = (
            long_leg.to_base_price(long_mid_native)
            + short_leg.to_base_price(short_mid_native)
        ) / 2.0

        min_lot_long_base = engine._min_base_for_leg(long_leg)
        min_lot_short_base = engine._min_base_for_leg(short_leg)
        notional_floor_base = SMOKETEST_NOTIONAL_USDT / mid_price_base
        target_qty_base = (
            max(min_lot_long_base, min_lot_short_base, notional_floor_base)
            * SMOKETEST_SAFETY_MULTIPLIER
        )
        target_notional_usdt = target_qty_base * mid_price_base

        record.update({
            "pair_key": pos_key,
            "mid_price_base": mid_price_base,
            "min_lot_long_base": min_lot_long_base,
            "min_lot_short_base": min_lot_short_base,
            "notional_floor_base": notional_floor_base,
            "target_qty_base": target_qty_base,
            "target_notional_usdt": target_notional_usdt,
        })

        print(
            f"\nSizing: target_qty_base={target_qty_base:.10f}  "
            f"(~{target_notional_usdt:.2f} USDT @ mid={mid_price_base:.6f}/base)\n"
            f"  min_lot_long_base={min_lot_long_base:.10f}  "
            f"min_lot_short_base={min_lot_short_base:.10f}  "
            f"notional_floor_base={notional_floor_base:.10f}\n"
        )

        # ----------------------- Entry -----------------------
        entry_payload = {
            "long":  [long_venue, long_symbol],
            "short": [short_venue, short_symbol],
            "base_amount": target_qty_base,
            "min_entry_basis_bps": SMOKETEST_BASIS_FLOOR_BPS,
            "max_duration_s": SMOKETEST_MAX_DURATION_S,
        }
        status, body = await _call_handler(engine.handle_entry, entry_payload)
        record["entry_status"] = status
        record["entry_body"] = body
        if status != 200:
            print(f"\nSMOKETEST FAIL (entry): status={status} body={body}")
            _emit(log_path, {**record, "verdict": "ENTRY_FAILED"})
            return
        if not body.get("filled") or float(body["filled"]) <= 0:
            print(
                f"\nSMOKETEST FAIL (entry filled 0): "
                f"halt_reason={body.get('halt_reason')!r}. "
                f"Both books were either too thin or basis floor not satisfied; "
                f"check engine logs above for gate-failure cycles."
            )
            _emit(log_path, {**record, "verdict": "ENTRY_NO_FILL"})
            return

        record["position_after_entry"] = engine.positions.get(pos_key)

        # ----------------------- Hold -----------------------
        await asyncio.sleep(SMOKETEST_INTER_CYCLE_WAIT_S)

        # ----------------------- Exit -----------------------
        # base_amount is capped to pos.amount_base inside handle_exit, so
        # over-asking is safe and ensures we attempt the full unwind.
        exit_payload = {
            "pair": pos_key,
            "base_amount": target_qty_base,
            "min_exit_basis_bps": SMOKETEST_BASIS_FLOOR_BPS,
            "max_duration_s": SMOKETEST_MAX_DURATION_S,
        }
        status, body = await _call_handler(engine.handle_exit, exit_payload)
        record["exit_status"] = status
        record["exit_body"] = body
        if status != 200:
            print(f"\nSMOKETEST FAIL (exit): status={status} body={body}")
            _emit(log_path, {**record, "verdict": "EXIT_FAILED"})
            return

        # ----------------------- Verify -----------------------
        residual = engine.positions.get(pos_key)
        record["position_after_exit"] = residual

        # Disk consistency — handle_exit calls save_state, so positions.json
        # must reflect the in-memory state. Cross-checks the persistence path.
        disk_state = await asyncio.to_thread(load_state)
        disk_residual = disk_state.get(pos_key)
        record["disk_position_after_exit"] = disk_residual

        in_memory_clear = residual is None
        on_disk_clear = disk_residual is None

        print(f"\n{'=' * 110}")
        if in_memory_clear and on_disk_clear:
            verdict = "PASS"
            print(
                f"SMOKETEST PASS\n"
                f"  entry: filled={record['entry_body']['filled']:.10f} "
                f"vwap_long={record['entry_body']['vwap_long_base']:.10f} "
                f"vwap_short={record['entry_body']['vwap_short_base']:.10f} "
                f"basis_bps={record['entry_body']['realized_basis_bps']:.2f}\n"
                f"  exit:  filled={record['exit_body']['filled']:.10f} "
                f"vwap_long={record['exit_body']['vwap_long_base']:.10f} "
                f"vwap_short={record['exit_body']['vwap_short_base']:.10f} "
                f"basis_bps={record['exit_body']['realized_basis_bps']:.2f}\n"
                f"  position: cleared (in-memory={in_memory_clear}, on-disk={on_disk_clear})"
            )
        else:
            verdict = "FAIL_RESIDUAL"
            print(
                f"SMOKETEST FAIL — residual position\n"
                f"  in-memory cleared: {in_memory_clear} (residual={residual})\n"
                f"  on-disk cleared:   {on_disk_clear} (residual={disk_residual})\n"
                f"  Manual unwind required before re-running."
            )
        print("=" * 110)
        record["verdict"] = verdict
        _emit(log_path, record)

    except Exception as e:
        record["verdict"] = "EXCEPTION"
        record["exception"] = f"{type(e).__name__}: {e}"
        _emit(log_path, record)
        print(f"\nSMOKETEST EXCEPTION: {type(e).__name__}: {e}")
        raise
    finally:
        await engine.shutdown()


# ---------------------------------------------------------------------------
# Registry / CLI
# ---------------------------------------------------------------------------

PROBE_REGISTRY: dict[str, tuple] = {
    # Class 1
    "capabilities":            (capabilities,            "[class 1]  c.has + auth probe"),
    "orderbook_liveness":             (orderbook_liveness,             "[class 1]  WS subscribe-and-measure (public)"),
    "clock_skew":              (clock_skew,              "[class 1]  roadmap (clock drift per venue)"),
    "precision_audit":         (precision_audit,         "[class 1]  roadmap (amount rounding direction)"),
    # Class 2
    "ioc_honor":               (ioc_honor,               "[class 2]  far-from-spread IOC, verify auto-cancel"),
    "receipt_shape":           (receipt_shape,           "[class 2]  full receipt-dict catalog + fetch_order follow-up"),
    "reduceonly_zero":         (reduceonly_zero,         "[class 2]  roadmap (reduceOnly on zero position)"),
    "set_leverage_idempotent": (set_leverage_idempotent, "[class 2]  roadmap (set_leverage idempotency)"),
    "cancel_phantom":          (cancel_phantom,          "[class 2]  roadmap (cancel fabricated id)"),
    "rate_limit_signature":    (rate_limit_signature,    "[class 2]  roadmap (burst → 429/418 threshold)"),
    "reconnect_behavior":      (reconnect_behavior,      "[class 2]  roadmap (forced ws close)"),
    # Class 3 — capital at risk
    "min_lot_live":            (min_lot_live,            "[class 3]  roadmap; gated by --I-AM-FUNDED-AND-AUTHORIZED-FOR-X"),
    "cross_venue_smoketest":   (cross_venue_smoketest,   "[class 3]  end-to-end pair entry+exit; gated by both --I-AM-FUNDED-AND-AUTHORIZED-FOR-X"),
    "fill_resolution":         (fill_resolution,         "[class 3]  diagnose fetch_order vs fetch_my_trades on a filling IOC; gated by --I-AM-FUNDED-AND-AUTHORIZED-FOR-X"),
}


def _print_help():
    print(__doc__)
    print("Probe registry:\n")
    for name, (_, desc) in sorted(PROBE_REGISTRY.items()):
        print(f"  {name:<26} {desc}")
    print()
    print(
        f"Configured venues (from .env): {', '.join(_venues_from_env()) or '(none — set *_API_KEY)'}"
    )
    print(f"Probe log directory:          {PROBE_LOG_DIR}")
    print()


def _parse_argv(argv: list[str]) -> tuple[list, dict]:
    """Parse `--key=value` and `--bool-flag` style args. Capital-at-risk
    handshakes (--I-AM-FUNDED-AND-AUTHORIZED-FOR-{VENUE}) are detected
    here and folded into the kwargs dict as `authorized=True`."""
    args: list = []
    kwargs: dict = {}
    auth_flags: list[str] = []
    for a in argv:
        if a.startswith("--I-AM-FUNDED-AND-AUTHORIZED-FOR-"):
            auth_flags.append(a[len("--I-AM-FUNDED-AND-AUTHORIZED-FOR-"):])
            continue
        if a.startswith("--"):
            body = a[2:]
            if "=" in body:
                k, v = body.split("=", 1)
                kwargs[k.replace("-", "_")] = v
            else:
                kwargs[body.replace("-", "_")] = True
        else:
            args.append(a)

    if auth_flags:
        # cross_venue_smoketest takes per-leg authorization (its signature
        # has no `authorized` kwarg, so we must NOT also set the single-leg
        # flag — that would TypeError at call time).
        if "long_spec" in kwargs or "short_spec" in kwargs:
            kwargs["authorized_long"] = any(
                f.upper() == kwargs.get("long_spec", "").split(":")[0].upper()
                for f in auth_flags
            )
            kwargs["authorized_short"] = any(
                f.upper() == kwargs.get("short_spec", "").split(":")[0].upper()
                for f in auth_flags
            )
        else:
            kwargs["authorized"] = True
        kwargs["_auth_flags"] = auth_flags

    return args, kwargs


async def main():
    if len(sys.argv) < 2:
        _print_help()
        sys.exit(0)

    cmd = sys.argv[1].lower()
    if cmd in {"-h", "--help", "help"}:
        _print_help()
        sys.exit(0)

    if cmd not in PROBE_REGISTRY:
        print(f"Unknown probe: {cmd!r}\n")
        _print_help()
        sys.exit(2)

    fn, _ = PROBE_REGISTRY[cmd]
    args, kwargs = _parse_argv(sys.argv[2:])
    kwargs.pop("_auth_flags", None)  # informational only — drop before fn call
    try:
        await fn(*args, **kwargs)
    except PermissionError as e:
        # Class-3 authorization handshake missing — surface cleanly, no traceback.
        print(f"\n[AUTHORIZATION REQUIRED]\n{e}\n", file=sys.stderr)
        sys.exit(3)
    except NotImplementedError as e:
        # Roadmap stub — surface the pointer to ENGINE_FIELD_NOTES.md.
        print(f"\n[ROADMAP STUB]\n{e}\n", file=sys.stderr)
        sys.exit(4)


if __name__ == "__main__":
    asyncio.run(main())
