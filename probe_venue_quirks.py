"""Venue-quirks probe — characterize fetch_order + fetch_my_trades behavior
across all 12 verified venues.

Three dimensions per venue:
  1. fetch_order fee shape — does it populate fees on a known-filled order?
     (Some sync-null venues populate post-fetch_order, others don't.
     Anchor: 2026-05-10 we discovered xt doesn't, despite being sync-null.)

  2. fetch_my_trades `limit` cap — what's the max accepted limit value?
     (Anchor: 2026-05-10 we discovered XT caps at 100 — `max_100` server
     error on 200. Other venues may have similar undocumented caps.)

  3. fetch_my_trades `since` parameter honored?
     (Anchor: 2026-05-10 we discovered KuCoin's `since` is broken — returns
     empty even when trades exist. Other venues may have same quirk.)

Methodology:
  - Pull a recent order_id from fetch_my_trades(limit=10) (read-only).
  - Re-query that order via fetch_order to inspect fee surface.
  - Test fetch_my_trades at limit=100 and limit=500 to find caps.
  - Test fetch_my_trades with since=24h-ago, verify all returned trades
    are >= since timestamp (proper filter) vs returning empty (broken).

All probes are read-only. Zero capital at risk. Total ~48 API calls
across 12 venues, well below any rate-limit ceiling.

Output: per-venue summary table + full JSON dump to probe_logs/.
"""
import asyncio
import json
import time
from config import get_exchange
from venue_overrides import fetch_order_params_for


VENUES = [
    "binance", "bybit", "kucoinfutures", "okx", "mexc",
    "bitget", "bingx", "xt", "gate", "htx", "bitmart", "coinex",
]
SYMBOL = "XRP/USDT:USDT"
SINCE_MS_24H = int(time.time() * 1000) - 24 * 3600 * 1000


async def probe_venue(venue: str) -> dict:
    out: dict = {"venue": venue}
    ex = get_exchange(venue)
    try:
        await ex.load_markets()
        if SYMBOL not in ex.markets:
            out["error"] = f"symbol {SYMBOL} not in markets"
            return out

        # ---------- Get a recent order_id ----------
        try:
            recent = await ex.fetch_my_trades(SYMBOL, limit=10)
            out["recent_count"] = len(recent)
            if not recent:
                out["fetch_order_fee"] = "SKIP (no recent trades)"
                out["fetch_order_fees"] = "SKIP"
            else:
                sample = recent[0]
                order_id = str(sample.get("order") or "")
                out["sample_order_id"] = order_id
                # ---------- fetch_order fee shape ----------
                try:
                    params = fetch_order_params_for(venue)
                    order = await ex.fetch_order(order_id, SYMBOL, params=params)
                    fo_fee = order.get("fee")
                    fo_fees = order.get("fees")
                    out["fetch_order_fee"] = fo_fee if fo_fee is not None else "None"
                    out["fetch_order_fees"] = fo_fees if fo_fees else "[]"
                    out["fetch_order_status"] = order.get("status")
                    out["fetch_order_filled"] = order.get("filled")
                    # Did we get usable fee data?
                    has_fee = bool(fo_fee and fo_fee.get("cost") is not None) or bool(fo_fees and any(f.get("cost") is not None for f in fo_fees))
                    out["fetch_order_has_fee"] = has_fee
                except Exception as e:
                    out["fetch_order_err"] = f"{type(e).__name__}: {str(e)[:120]}"
        except Exception as e:
            out["recent_err"] = f"{type(e).__name__}: {str(e)[:120]}"

        # ---------- Limit cap test ----------
        for lim in (100, 500):
            try:
                t = await ex.fetch_my_trades(SYMBOL, limit=lim)
                out[f"limit{lim}"] = f"OK ({len(t)} trades)"
            except Exception as e:
                out[f"limit{lim}"] = f"ERR {type(e).__name__}: {str(e)[:80]}"

        # ---------- since parameter test ----------
        try:
            t_since = await ex.fetch_my_trades(SYMBOL, since=SINCE_MS_24H, limit=10)
            if not t_since:
                # Could be broken-since OR genuinely no trades in 24h.
                # Compare against no-since fetch to disambiguate.
                t_no_since = await ex.fetch_my_trades(SYMBOL, limit=10)
                if t_no_since:
                    # We have recent trades but since=24h returned empty.
                    # If those trades are >= since_ms_24h, since is broken.
                    recent_in_window = [tr for tr in t_no_since if (tr.get("timestamp") or 0) >= SINCE_MS_24H]
                    if recent_in_window:
                        out["since_param"] = (
                            f"BROKEN (no-since returns {len(recent_in_window)} trades "
                            f"in 24h window but since-filter returns 0)"
                        )
                    else:
                        out["since_param"] = "no trades in 24h (inconclusive)"
                else:
                    out["since_param"] = "no trades period (inconclusive)"
            else:
                oldest = min(tr.get("timestamp") or 0 for tr in t_since)
                if oldest >= SINCE_MS_24H:
                    out["since_param"] = f"honored (oldest_ts={oldest})"
                else:
                    out["since_param"] = f"BROKEN — trade ts {oldest} < since {SINCE_MS_24H}"
        except Exception as e:
            out["since_param"] = f"ERR {type(e).__name__}: {str(e)[:80]}"

    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        try:
            await ex.close()
        except Exception:
            pass
    return out


async def main():
    print(f"Probing venue quirks across {len(VENUES)} venues — read-only")
    print(f"Symbol: {SYMBOL}\n")

    results = await asyncio.gather(
        *(probe_venue(v) for v in VENUES), return_exceptions=True
    )

    # Render summary table
    print(f"{'venue':14s} {'fetch_order_fee':40s} {'limit100':12s} {'limit500':40s} {'since':40s}")
    print("-" * 160)
    for r in results:
        if isinstance(r, Exception):
            print(f"  EXCEPTION: {r!r}")
            continue
        if "error" in r:
            print(f"{r['venue']:14s} INIT ERR: {r['error']}")
            continue
        fo_fee = r.get("fetch_order_fee", "?")
        if isinstance(fo_fee, dict):
            fo_summary = f"{{cost={fo_fee.get('cost')}, cur={fo_fee.get('currency')}}}"
        elif fo_fee in ("None", "SKIP (no recent trades)"):
            fo_summary = str(fo_fee)
        else:
            fo_summary = str(fo_fee)[:38]
        l100 = r.get("limit100", "?")[:12]
        l500 = r.get("limit500", "?")[:38]
        sn = r.get("since_param", "?")[:38]
        print(f"{r['venue']:14s} {fo_summary:40s} {l100:12s} {l500:40s} {sn:40s}")

    # Persist full dump
    with open("probe_logs/venue_quirks.json", "w") as f:
        json.dump([r if not isinstance(r, Exception) else str(r) for r in results],
                  f, indent=2, default=str)
    print("\nFull dump → probe_logs/venue_quirks.json")


asyncio.run(main())
