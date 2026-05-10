"""Fee-shape probe across the 12 verified venues.

Uses fetch_my_trades on each venue's recent XRP trade history (no new
orders placed) to characterize:
  - fetch_my_trades availability and trade shape per venue
  - fee/fees field shape (singular dict, list, both with same data, etc.)
  - order_id field name in the trade dict (CCXT `order` vs `info.orderId` etc.)
  - currency the fee is denominated in (USDT, BNB, base coin, etc.)

Drives the Phase-2 pnl.py design: which venues need fetch_my_trades
enrichment because their receipt-captured fees are empty, and what
shape pnl.py must expect when it does enrich.

Probe is read-only and rate-limit-friendly (one call per venue with
limit=5). Free; no capital at risk. Output goes to stdout; caller
can redirect to probe_logs/ if they want to archive.
"""
import asyncio
import json
import time
from config import get_exchange


VENUES = [
    "binance", "bybit", "kucoinfutures", "okx", "mexc",
    "bitget", "bingx", "xt", "gate", "htx", "bitmart", "coinex",
]
SYMBOL = "XRP/USDT:USDT"
SINCE_MS = int(time.time() * 1000) - 48 * 3600 * 1000   # 48h window — wide enough for yesterday's smoketests


async def probe_venue(venue: str) -> dict:
    """Returns a per-venue summary dict for the report table."""
    out = {"venue": venue, "ok": False, "n_trades": 0, "shape": None,
           "order_id_field": None, "first_trade": None, "error": None}
    ex = get_exchange(venue)
    try:
        await ex.load_markets()
        if SYMBOL not in ex.markets:
            out["error"] = f"symbol {SYMBOL} not in markets"
            return out
        trades = await ex.fetch_my_trades(SYMBOL, since=SINCE_MS, limit=5)
        out["n_trades"] = len(trades)
        if not trades:
            out["error"] = "no trades in 48h window (smoketest fills aged out?)"
            return out
        t = trades[0]
        out["ok"] = True

        fee = t.get("fee")
        fees = t.get("fees")
        # Characterize shape
        if fee and fees and len(fees) == 1 and fees[0] == fee:
            shape = "fee=fees[0] (duplicate; needs dedup)"
        elif fee and not fees:
            shape = "fee only (singular)"
        elif fees and not fee:
            shape = f"fees only (list len={len(fees)})"
        elif fee and fees:
            shape = f"fee + fees both populated, distinct (fees len={len(fees)})"
        else:
            shape = "both empty"
        out["shape"] = shape

        # Where does order_id come from? CCXT-unified: `t['order']`. Some
        # venues might have it only in `t['info']`.
        if t.get("order"):
            out["order_id_field"] = "order"
        else:
            for k in ("orderId", "ordId", "order_id"):
                if k in (t.get("info") or {}):
                    out["order_id_field"] = f"info.{k}"
                    break
            if not out["order_id_field"]:
                out["order_id_field"] = "MISSING"

        out["first_trade"] = {
            "fee": fee,
            "fees": fees,
            "amount": t.get("amount"),
            "cost": t.get("cost"),
            "order": t.get("order"),
        }
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:120]}"
    finally:
        try:
            await ex.close()
        except Exception:
            pass
    return out


async def main():
    print(f"Probing fetch_my_trades fee shape across {len(VENUES)} venues")
    print(f"Symbol: {SYMBOL}  since: 48h ago\n")

    results = await asyncio.gather(
        *(probe_venue(v) for v in VENUES), return_exceptions=True
    )

    print(f"{'venue':14s} {'ok':3s} {'n':3s} {'order_id':18s} {'fee_shape':45s} fee_sample")
    print("-" * 130)
    for r in results:
        if isinstance(r, Exception):
            print(f"  EXCEPTION: {r!r}")
            continue
        if r["ok"]:
            ft = r["first_trade"]
            sample = f"{ft['fee']}"
            print(f"{r['venue']:14s} {'Y':3s} {r['n_trades']:>3d} {r['order_id_field']:18s} {r['shape']:45s} {sample[:60]}")
        else:
            print(f"{r['venue']:14s} {'-':3s} {r['n_trades']:>3d} {'-':18s} {'-':45s} ERR: {r['error']}")

    # Persist full dump for forensic depth
    with open("probe_logs/fee_shape.json", "w") as f:
        json.dump([r for r in results if not isinstance(r, Exception)], f, indent=2, default=str)
    print("\nFull dump → probe_logs/fee_shape.json")


asyncio.run(main())
