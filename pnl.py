"""Off-engine realized PnL analyzer for closed funding-arb trades.

Reads `closed_trades.json` and computes per-round-trip True PnL for the
funding-rate-arbitrage strategy. Three components:

  - **price_pnl**:  qty × ((entry_short - entry_long) + (exit_long - exit_short)) USDT
  - **fees_pnl**:   Σ all order_records' fees in USDT (Phase 2; mutates
                    closed_trades.json via fetch_my_trades enrichment)
  - **funding_pnl**: Σ fetch_funding_history events per leg over
                    [opened_at, closed_at] (Phase 3; signed,
                    positive = received per CCXT convention)

  true_pnl = price_pnl + funding_pnl − fees_pnl

Architecture (per the True PnL primitive design):
  - Engine records facts (handle_entry/handle_exit append per-order data
    to positions.json → closed_trades.json on dust-clear archival).
  - pnl.py interprets. fetch_my_trades happens ONLY here, never in the
    engine — zero added latency on the slicing loop's critical path.
  - Mutates closed_trades.json: writes enriched fees back so subsequent
    runs are fast and venues with short fetch_my_trades retention
    (KuCoin) don't lose data.

Per-venue empirical foundation (probe_fee_shape.py, 2026-05-10):
  - 12/12 venues use `t['order']` for order_id matching in fetch_my_trades.
  - 12/12 venues return single-USDT fee dict per trade (no multi-currency).
  - 11/12 venues honor `since` parameter; KuCoin's is broken (returns
    empty even when trades exist) — we omit `since` and filter
    client-side for venue-agnostic safety.

Race safety:
  - load → enrich-in-memory → re-load + merge-by-(opened_at,base_coin) → atomic-rename.
  - If engine appends a new closed trade during pnl.py's compute, the
    re-load + merge pass preserves it (just not enriched in this pass —
    pnl.py picks it up on next run).

Usage:
  python3 pnl.py
  python3 pnl.py --coin XRP --long binance --short bybit --since 2026-05-01
  python3 pnl.py --format json
  python3 pnl.py --no-enrich --no-write     # pure read, useful for dry-run
"""
import argparse
import asyncio
import csv
import json
import os
import sys
from datetime import datetime
from typing import Optional

from config import get_exchange
from receipt_resolver import _fees_from_receipt, _dedupe_fees


CLOSED_TRADES_FILE = "closed_trades.json"

# fetch_my_trades batch size per (venue, symbol). Sufficient for any single
# round trip's order set (max ~50 records for a heavily-sliced trade).
# Capped at 100 because XT's server-side maximum is 100 — verified live
# 2026-05-10 (limit=200 returns `max_100` error, limit=100 works).
# Other venues accept higher; 100 works universally.
FETCH_MY_TRADES_LIMIT = 100

# fetch_funding_history batch size per (venue, symbol). Funding settlements
# happen at 1h/4h/8h cadence; for a single position lifecycle (typically
# minutes-to-days), 100 events is a wildly safe upper bound. Bitget caps
# limit at <500 (verified probe_funding_shape.py 2026-05-10); 100 works
# universally. Reused FETCH_MY_TRADES_LIMIT pattern for consistency.
FETCH_FUNDING_HISTORY_LIMIT = 100

# Window padding around opened_at when filtering trades client-side.
# Catches trades whose venue-side timestamp drifts vs engine's opened_at
# (some venues stamp trades 100-1000ms before the placement is mirrored
# back). 1h is wildly conservative; cheaper than missing a fee.
WINDOW_PADDING_MS = 3600 * 1000


# ---------------------------------------------------------------------------
# IO + atomic save
# ---------------------------------------------------------------------------


def parse_ts(s: str) -> datetime:
    """Parse the engine's millisecond-precision timestamp format."""
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")


def load_closed_trades() -> list:
    if not os.path.exists(CLOSED_TRADES_FILE):
        return []
    with open(CLOSED_TRADES_FILE) as f:
        return json.load(f)


def merge_and_save_atomic(enriched_trades: list) -> int:
    """Atomic write-temp-rename with re-read merge for engine-append safety.

    Sequence:
      1. Re-load closed_trades.json (capture any new records appended by
         engine since pnl.py's initial load).
      2. For each existing record, if it's in our enriched-set (matched by
         opened_at + base_coin — unique per round trip), copy our enriched
         `fees` lists onto it position-stably (zip on order_records, since
         order_record list is append-only and ordering is preserved).
      3. Write to .tmp, atomic-rename to canonical path.

    Returns the count of records updated. POSIX-atomic on same filesystem;
    the rename is a single inode swap. The race window (between re-read
    and rename) is microseconds — engine's `append_closed_trade` would
    have to fire in that window to lose data, and if it does, the lost
    record is just an unenriched trade that pnl.py picks up next run."""
    current = load_closed_trades()
    enriched_index = {(t["opened_at"], t["base_coin"]): t for t in enriched_trades}

    updates = 0
    for trade in current:
        key = (trade.get("opened_at"), trade.get("base_coin"))
        enriched = enriched_index.get(key)
        if enriched is None:
            continue
        for old_r, new_r in zip(
            trade.get("entry_order_records", []),
            enriched.get("entry_order_records", []),
        ):
            old_r["fees"] = new_r["fees"]
        for old_r, new_r in zip(
            trade.get("exit_order_records", []),
            enriched.get("exit_order_records", []),
        ):
            old_r["fees"] = new_r["fees"]
        # Phase 3: persist funding_history block (whole-replace; keyed by
        # opened_at+base_coin which is unique per round trip).
        if "funding_history" in enriched:
            trade["funding_history"] = enriched["funding_history"]
        updates += 1

    tmp = CLOSED_TRADES_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(current, f, indent=4)
    os.rename(tmp, CLOSED_TRADES_FILE)
    return updates


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def filter_trades(trades: list, args: argparse.Namespace) -> list:
    out = trades
    if args.coin:
        out = [t for t in out if t.get("base_coin") == args.coin]
    if args.long:
        out = [t for t in out if t.get("long", {}).get("exchange") == args.long]
    if args.short:
        out = [t for t in out if t.get("short", {}).get("exchange") == args.short]
    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d")
        out = [t for t in out if parse_ts(t["closed_at"]) >= since_dt]
    return sorted(out, key=lambda t: t["closed_at"])


# ---------------------------------------------------------------------------
# Fee enrichment via fetch_my_trades
# ---------------------------------------------------------------------------


async def get_or_create_exchange(venue: str, cache: dict):
    """Cache exchange handles across enrichment passes — venues are reused
    across many trades, and load_markets is non-trivial latency-wise."""
    if venue not in cache:
        ex = get_exchange(venue)
        await ex.load_markets()
        cache[venue] = ex
    return cache[venue]


async def enrich_trade_fees(
    trade: dict,
    exchanges_cache: dict,
    verbose: bool = False,
) -> dict:
    """In-place fee enrichment for one closed trade — fetch_my_trades is
    canonical, captured receipt fees are fallback only.

    **Architecture (revised 2026-05-10 post-htx×bitmart smoketest)**:
    Receipt-captured fees CANNOT be trusted as authoritative. Empirically:
      - htx fetch_order returns NEGATIVE fees (sign convention vs. cost-to-user)
        AND mixes string + float types in the same payload (dedup-defeating).
      - bingx fetch_order has fees but engine doesn't call fetch_order on
        sync-zero, so engine captures empty.
      - bitmart receipt empty AND fee currency varies by side (USDT on sells,
        base-coin on buys).
    fetch_my_trades is the venue's authoritative trade ledger and returns
    consistent shapes across all 12 venues (single USDT or base-coin entry,
    correct sign, t['order'] for matching). Always enriching from there
    eliminates whole classes of bugs.

    Per (venue, symbol) one batched `fetch_my_trades(symbol, limit=100)`
    call (limit=100 universal cap satisfies okx and xt server limits).
    Omits `since` parameter (KuCoin's is unreliable; client-filter by
    opened_at − WINDOW_PADDING_MS instead).

    A single order_id can span MULTIPLE trade rows (partial-filled IOCs
    return 2-3 trades). We concatenate per-trade `_fees_from_receipt`
    results — each call dedupes within its receipt; no cross-trade dedup
    because two partial fills can legitimately have identical fees.

    Fallback when fetch_my_trades fails or doesn't match an order_id:
    keep the captured fees but run them through `_dedupe_fees` (catches
    legacy double-counts and the htx string/float type mismatch). Better
    than nothing, with a verbose warning so the operator knows the data
    is degraded for that record."""
    opened_at_ms = int(parse_ts(trade["opened_at"]).timestamp() * 1000)

    all_records = (
        trade.get("entry_order_records", []) + trade.get("exit_order_records", [])
    )

    # Group ALL records by (venue, symbol) for batched fetch. Even records
    # with populated captured fees get fresh fetch_my_trades data — see
    # docstring for why captured isn't trusted.
    by_venue_sym: dict = {}
    for r in all_records:
        by_venue_sym.setdefault((r["venue"], r["symbol"]), []).append(r)

    for (venue, symbol), records in by_venue_sym.items():
        try:
            ex = await get_or_create_exchange(venue, exchanges_cache)
            trades = await ex.fetch_my_trades(symbol, limit=FETCH_MY_TRADES_LIMIT)
        except Exception as e:
            if verbose:
                print(
                    f"  WARN: fetch_my_trades({venue}:{symbol}) failed: "
                    f"{type(e).__name__}: {e} — falling back to captured fees + dedup",
                    file=sys.stderr,
                )
            # Fallback: dedup captured (catches both bybit-style and
            # htx-style type-mismatch double-counts).
            for r in records:
                r["fees"] = _dedupe_fees(r.get("fees") or [])
            continue

        in_window = [
            t for t in trades
            if (t.get("timestamp") or 0) >= opened_at_ms - WINDOW_PADDING_MS
        ]

        for r in records:
            matches = [
                t for t in in_window
                if str(t.get("order") or "") == str(r["order_id"])
            ]
            if not matches:
                if verbose:
                    print(
                        f"  WARN: no fetch_my_trades match for "
                        f"{venue} order_id={r['order_id']} — falling back to captured + dedup",
                        file=sys.stderr,
                    )
                # Fallback path again.
                r["fees"] = _dedupe_fees(r.get("fees") or [])
                continue
            collected: list = []
            for m in matches:
                collected.extend(_fees_from_receipt(m))
            r["fees"] = collected

    return trade


# ---------------------------------------------------------------------------
# Funding-history enrichment (Phase 3)
# ---------------------------------------------------------------------------


async def enrich_trade_funding(
    trade: dict,
    exchanges_cache: dict,
    verbose: bool = False,
) -> dict:
    """Per-leg funding-history join over [opened_at, closed_at].

    Calls `fetch_funding_history(symbol, limit=100)` per leg, filters
    client-side by timestamp ∈ [opened_at_ms, closed_at_ms]. Stores
    the per-event details and per-leg signed sum on the trade.

    **Sign convention** (CCXT documented): `event['amount']` is signed
    from the user's perspective — POSITIVE when user RECEIVED funding
    (their leg's funding-rate sign was favorable), NEGATIVE when user
    PAID. Total funding PnL = signed_long + signed_short.

    For a delta-neutral basis trade where user is LONG on venue-A
    (funding rate r_A) and SHORT on venue-B (funding rate r_B):
      - long  leg pays  when r_A > 0, receives when r_A < 0  (signed: -r_A × notional)
      - short leg receives when r_B > 0, pays when r_B < 0  (signed: +r_B × notional)
      - net funding ≈ (r_B − r_A) × notional × (#settlements during hold)

    A profitable funding-arb has r_short > r_long (rate spread > 0),
    so net funding is positive over the hold.

    **Empirical caveat (2026-05-10)**: We have ZERO real funding events
    in our test data because all smoketests held positions for seconds.
    Sign convention is from CCXT documentation, NOT verified per venue.
    First real long-hold trade should be inspected — verbose mode dumps
    the raw event for sanity check. If a venue inverts the sign convention
    (analogous to htx's negative-fee bug on fetch_order), this is where
    we'll catch it.

    Soft-fails on per-venue errors — logs to stderr, leaves leg's
    funding empty so compute_pnl treats as zero. Operator sees the
    warning and decides whether to retry or accept the gap."""
    opened_ms = int(parse_ts(trade["opened_at"]).timestamp() * 1000)
    closed_ms = int(parse_ts(trade["closed_at"]).timestamp() * 1000)

    funding_per_leg: dict = {}
    for side_name in ("long", "short"):
        leg = trade.get(side_name) or {}
        venue = leg.get("exchange")
        symbol = leg.get("symbol")
        if not venue or not symbol:
            funding_per_leg[side_name] = {"events": [], "total_usdt": 0.0,
                                          "error": "missing venue or symbol"}
            continue
        try:
            ex = await get_or_create_exchange(venue, exchanges_cache)
            events = await ex.fetch_funding_history(
                symbol, limit=FETCH_FUNDING_HISTORY_LIMIT,
            )
        except Exception as e:
            if verbose:
                print(
                    f"  WARN: fetch_funding_history({venue}:{symbol}) failed: "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )
            funding_per_leg[side_name] = {"events": [], "total_usdt": 0.0,
                                          "error": f"{type(e).__name__}: {str(e)[:200]}"}
            continue

        in_window: list = []
        non_usdt: list = []
        for e in events:
            ts = e.get("timestamp")
            if ts is None or ts < opened_ms or ts > closed_ms:
                continue
            currency = e.get("code") or e.get("currency")
            try:
                amount = float(e.get("amount") or 0)
            except (TypeError, ValueError):
                continue
            normalized = {
                "ts": ts,
                "datetime": e.get("datetime"),
                "amount": amount,
                "currency": currency,
                "id": str(e.get("id") or ""),
                "symbol": e.get("symbol"),
            }
            if currency != "USDT":
                non_usdt.append(normalized)
            in_window.append(normalized)
            if verbose:
                print(
                    f"  funding event {venue}:{symbol} ts={ts} "
                    f"amount={amount:+.10f} {currency}",
                    file=sys.stderr,
                )

        total_usdt = sum(
            ev["amount"] for ev in in_window if ev["currency"] == "USDT"
        )
        funding_per_leg[side_name] = {
            "events": in_window,
            "total_usdt": total_usdt,
            "non_usdt_events": non_usdt,
        }

    trade["funding_history"] = funding_per_leg
    return trade


# ---------------------------------------------------------------------------
# PnL math
# ---------------------------------------------------------------------------


def compute_pnl(trade: dict) -> dict:
    """Per-round-trip realized PnL.

    Price PnL: per-base-token revenue minus cost across both legs and both
    legs of the round trip:

        long  enters as buy  at entry_long_vwap  (cost)
        short enters as sell at entry_short_vwap (revenue)
        long  exits  as sell at exit_long_vwap   (revenue)
        short exits  as buy  at exit_short_vwap  (cost)

    Per-base-token PnL (USDT):
        (entry_short - entry_long) + (exit_long - exit_short)
        ↑ entry-side basis           ↑ exit-side basis
        Both terms positive → trade was profitable.
        Sum × qty = price PnL in USDT.

    Quantity is min(entry_qty, exit_qty) — the symmetric round-trip
    portion. Any residual_dust_base is unrealized (untradeable; engine
    archives it as forensic anchor) and EXCLUDED from realized PnL.

    Fees PnL: sum of every captured/enriched USDT fee across all entry
    + exit + recovery order records. Non-USDT fees (BNB partial-pay etc.)
    are surfaced separately for operator review — Phase-2 doesn't
    auto-convert (no live FX feed in scope; would require either spot
    cross-rate at trade time or aggregated venue conversion data).

    Returns a dict ready for table/json/csv rendering."""
    qty_closed = min(
        trade.get("entry_qty_base", 0.0) or 0.0,
        trade.get("exit_qty_base", 0.0) or 0.0,
    )

    if qty_closed <= 0:
        # Degenerate close — entry filled but exit didn't, or vice versa.
        # Trade record exists but no symmetric round trip to value; all
        # PnL fields zero. legacy_no_records=False (the absence of records
        # here is structural, not a missing-data warning).
        return {
            "qty_closed": 0.0,
            "notional_usdt": 0.0,
            "price_pnl_usdt": 0.0,
            "fees_pnl_usdt": 0.0,
            "funding_pnl_usdt": 0.0,
            "true_pnl_usdt": 0.0,
            "funding_event_count": 0,
            "funding_non_usdt_count": 0,
            "funding_errors": [],
            "non_usdt_fees": [],
            "fee_records_unmatched": 0,
            "legacy_no_records": False,
            "duration_s": 0.0,
        }

    entry_short = float(trade.get("entry_vwap_short_base") or 0.0)
    entry_long = float(trade.get("entry_vwap_long_base") or 0.0)
    exit_long = float(trade.get("exit_vwap_long_base") or 0.0)
    exit_short = float(trade.get("exit_vwap_short_base") or 0.0)

    price_pnl = qty_closed * ((entry_short - entry_long) + (exit_long - exit_short))

    fees_total = 0.0
    non_usdt: list = []
    unmatched_records = 0
    base_coin = trade.get("base_coin")
    all_records = (
        trade.get("entry_order_records", []) + trade.get("exit_order_records", [])
    )
    # Pre-Phase-1 trades (closed before 2026-05-10) have no order_records at all
    # — the engine schema didn't capture them. fees_pnl will be 0 with no
    # underlying data; flag this distinctly from the post-schema "records exist
    # but couldn't be enriched" case.
    legacy_no_records = (qty_closed > 0 and not all_records)
    for r in all_records:
        if not r.get("fees"):
            unmatched_records += 1
            continue
        for f in r["fees"]:
            cur = f.get("currency")
            cost = float(f.get("cost") or 0)
            if cur == "USDT":
                fees_total += cost
            elif cur == base_coin:
                # Bitmart-style: fee charged in the received currency.
                # On a BUY of XRP, fee is in XRP. Convert to USDT via the
                # order_record's vwap_base (USDT per base unit) — the most
                # accurate per-fill rate. Anchor: 2026-05-10 htx×bitmart
                # smoketest where bitmart's BUY-side fee was 0.128196 XRP.
                vwap_base = float(r.get("vwap_base") or 0)
                if vwap_base > 0:
                    fees_total += cost * vwap_base
                else:
                    # Degenerate: no vwap to convert against (shouldn't
                    # happen for a real fill). Surface to operator.
                    non_usdt.append({**f, "venue": r.get("venue"),
                                     "order_id": r.get("order_id"),
                                     "reason": "no_vwap_for_conversion"})
            else:
                # BNB partial-pay or other exotic — would need an FX feed
                # at trade time to convert. Surface for operator review.
                non_usdt.append({**f, "venue": r.get("venue"),
                                 "order_id": r.get("order_id"),
                                 "reason": "no_fx_feed"})

    notional = qty_closed * ((entry_long + entry_short) / 2.0)

    duration_s = 0.0
    if trade.get("opened_at") and trade.get("closed_at"):
        try:
            duration_s = (
                parse_ts(trade["closed_at"]) - parse_ts(trade["opened_at"])
            ).total_seconds()
        except Exception:
            pass

    # Funding PnL (Phase 3) — signed sum of received minus paid across both
    # legs over the position window. CCXT convention: amount > 0 = received.
    # Profit = long.received + short.received (both signed, can be negative).
    # When `funding_history` is absent (trade hasn't been enriched, or pre-
    # Phase-3 trade), funding_pnl is 0 and we don't subtract anything.
    funding_history = trade.get("funding_history") or {}
    funding_long = (funding_history.get("long") or {}).get("total_usdt", 0.0) or 0.0
    funding_short = (funding_history.get("short") or {}).get("total_usdt", 0.0) or 0.0
    funding_pnl = float(funding_long) + float(funding_short)
    funding_event_count = (
        len((funding_history.get("long") or {}).get("events", []))
        + len((funding_history.get("short") or {}).get("events", []))
    )
    funding_non_usdt_count = (
        len((funding_history.get("long") or {}).get("non_usdt_events", []))
        + len((funding_history.get("short") or {}).get("non_usdt_events", []))
    )
    funding_errors = []
    for side in ("long", "short"):
        err = (funding_history.get(side) or {}).get("error")
        if err:
            funding_errors.append({"side": side, "error": err})

    return {
        "qty_closed": qty_closed,
        "notional_usdt": notional,
        "price_pnl_usdt": price_pnl,
        "fees_pnl_usdt": fees_total,
        "funding_pnl_usdt": funding_pnl,
        "true_pnl_usdt": price_pnl + funding_pnl - fees_total,
        "funding_event_count": funding_event_count,
        "funding_non_usdt_count": funding_non_usdt_count,
        "funding_errors": funding_errors,
        "non_usdt_fees": non_usdt,
        "fee_records_unmatched": unmatched_records,
        "legacy_no_records": legacy_no_records,
        "duration_s": duration_s,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def render_table(rows: list) -> None:
    if not rows:
        print("(no trades match filter)")
        return

    headers = [
        ("closed_at", 19),
        ("pair", 26),
        ("coin", 5),
        ("quantity", 10),
        ("notional", 11),
        ("rt_bps", 8),
        ("price_pnl", 11),
        ("fees", 10),
        ("funding", 11),
        ("true_pnl", 11),
    ]
    line = " | ".join(h.ljust(w) for h, w in headers)
    print(line)
    print("-" * len(line))

    totals = {"price": 0.0, "fees": 0.0, "funding": 0.0, "true": 0.0, "notional": 0.0}
    warnings: list = []

    for r in rows:
        cells = [
            r["closed_at"][:19],
            f"{r['long']['exchange']}:{r['short']['exchange']}",
            r["base_coin"],
            f"{r['qty_closed']:.4f}",
            f"{r['notional_usdt']:.4f}",
            f"{r.get('round_trip_basis_bps', 0):.2f}",
            f"{r['price_pnl_usdt']:+.6f}",
            f"{r['fees_pnl_usdt']:.6f}",
            f"{r.get('funding_pnl_usdt', 0.0):+.6f}",
            f"{r['true_pnl_usdt']:+.6f}",
        ]
        print(" | ".join(c.ljust(w) for c, (_, w) in zip(cells, headers)))
        totals["price"] += r["price_pnl_usdt"]
        totals["fees"] += r["fees_pnl_usdt"]
        totals["funding"] += r.get("funding_pnl_usdt", 0.0)
        totals["true"] += r["true_pnl_usdt"]
        totals["notional"] += r["notional_usdt"]

        if r["non_usdt_fees"]:
            warnings.append(
                f"  {r['closed_at'][:19]} {r['base_coin']}: "
                f"{len(r['non_usdt_fees'])} non-USDT fee(s) excluded "
                f"(no FX conversion in Phase 2): {r['non_usdt_fees']}"
            )
        if r["fee_records_unmatched"]:
            warnings.append(
                f"  {r['closed_at'][:19]} {r['base_coin']} "
                f"({r['long']['exchange']}:{r['short']['exchange']}): "
                f"{r['fee_records_unmatched']} order_record(s) with empty fees "
                f"after enrichment (aged out of fetch_my_trades window?) — "
                f"fees_pnl underestimated"
            )
        if r["legacy_no_records"]:
            warnings.append(
                f"  {r['closed_at'][:19]} {r['base_coin']} "
                f"({r['long']['exchange']}:{r['short']['exchange']}): "
                f"pre-Phase-1 trade — no order_records captured by engine; "
                f"fees_pnl=0 is structural, not enrichable"
            )
        if r.get("funding_errors"):
            for err in r["funding_errors"]:
                warnings.append(
                    f"  {r['closed_at'][:19]} {r['base_coin']} "
                    f"funding fetch failed on {err['side']}: {err['error']}"
                )
        if r.get("funding_non_usdt_count", 0) > 0:
            warnings.append(
                f"  {r['closed_at'][:19]} {r['base_coin']}: "
                f"{r['funding_non_usdt_count']} non-USDT funding event(s) excluded — "
                f"inspect trade.funding_history for details"
            )

    print("-" * len(line))
    summary_prefix_width = sum(w for _, w in headers[:5]) + 4 * 3
    price_str = f"{totals['price']:+.6f}".ljust(headers[6][1])
    fees_str = f"{totals['fees']:.6f}".ljust(headers[7][1])
    funding_str = f"{totals['funding']:+.6f}".ljust(headers[8][1])
    true_str = f"{totals['true']:+.6f}".ljust(headers[9][1])
    rt_blank = "".ljust(headers[5][1])
    print(
        f"{'TOTAL'.ljust(summary_prefix_width)} | {rt_blank} | "
        f"{price_str} | {fees_str} | {funding_str} | {true_str}"
    )

    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print(w)


def render_json(rows: list) -> None:
    print(json.dumps(rows, indent=2, default=str))


def render_csv(rows: list) -> None:
    if not rows:
        return
    writer = csv.writer(sys.stdout)
    writer.writerow([
        "closed_at", "opened_at", "long", "short", "base_coin",
        "qty_closed", "notional_usdt",
        "entry_basis_bps", "exit_basis_bps", "round_trip_basis_bps",
        "price_pnl_usdt", "fees_pnl_usdt", "funding_pnl_usdt", "true_pnl_usdt",
        "duration_s", "fee_records_unmatched", "non_usdt_fee_count",
        "funding_event_count", "funding_non_usdt_count",
    ])
    for r in rows:
        writer.writerow([
            r["closed_at"], r["opened_at"],
            r["long"]["exchange"], r["short"]["exchange"], r["base_coin"],
            r["qty_closed"], r["notional_usdt"],
            r.get("entry_basis_bps", 0), r.get("exit_basis_bps", 0),
            r.get("round_trip_basis_bps", 0),
            r["price_pnl_usdt"], r["fees_pnl_usdt"],
            r.get("funding_pnl_usdt", 0.0), r["true_pnl_usdt"],
            r["duration_s"], r["fee_records_unmatched"], len(r["non_usdt_fees"]),
            r.get("funding_event_count", 0), r.get("funding_non_usdt_count", 0),
        ])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main():
    parser = argparse.ArgumentParser(
        description="Off-engine realized PnL analyzer for closed funding-arb trades."
    )
    parser.add_argument("--coin", help="Filter by base coin (e.g. XRP)")
    parser.add_argument("--long", help="Filter by long venue (e.g. binance)")
    parser.add_argument("--short", help="Filter by short venue (e.g. bybit)")
    parser.add_argument("--since", help="YYYY-MM-DD; only trades closed on/after")
    parser.add_argument("--format", choices=["table", "json", "csv"], default="table")
    parser.add_argument("--no-enrich", action="store_true",
                        help="Skip fetch_my_trades enrichment (use captured fees as-is)")
    parser.add_argument("--no-funding", action="store_true",
                        help="Skip fetch_funding_history enrichment")
    parser.add_argument("--no-write", action="store_true",
                        help="Don't write enriched fees/funding back to closed_trades.json")
    parser.add_argument("--verbose", action="store_true",
                        help="Print enrichment progress + warnings to stderr")
    args = parser.parse_args()

    trades = filter_trades(load_closed_trades(), args)

    if not trades:
        print("(no trades match filter)", file=sys.stderr)
        return

    if args.verbose:
        print(f"Processing {len(trades)} trades...", file=sys.stderr)

    if not args.no_enrich or not args.no_funding:
        exchanges_cache: dict = {}
        try:
            for t in trades:
                if not args.no_enrich:
                    await enrich_trade_fees(t, exchanges_cache, verbose=args.verbose)
                if not args.no_funding:
                    await enrich_trade_funding(t, exchanges_cache, verbose=args.verbose)
            if not args.no_write:
                updates = merge_and_save_atomic(trades)
                if args.verbose:
                    print(
                        f"Persisted enriched fees + funding on {updates} trades to "
                        f"{CLOSED_TRADES_FILE}",
                        file=sys.stderr,
                    )
        finally:
            for ex in exchanges_cache.values():
                try:
                    await ex.close()
                except Exception:
                    pass

    rows = []
    for t in trades:
        pnl = compute_pnl(t)
        rows.append({
            "closed_at": t["closed_at"],
            "opened_at": t.get("opened_at"),
            "long": t["long"],
            "short": t["short"],
            "base_coin": t["base_coin"],
            "entry_basis_bps": t.get("entry_basis_bps", 0.0),
            "exit_basis_bps": t.get("exit_basis_bps", 0.0),
            "round_trip_basis_bps": t.get("round_trip_basis_bps", 0.0),
            **pnl,
        })

    if args.format == "table":
        render_table(rows)
    elif args.format == "json":
        render_json(rows)
    elif args.format == "csv":
        render_csv(rows)


if __name__ == "__main__":
    asyncio.run(main())
