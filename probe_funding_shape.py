"""Funding history shape probe — characterize fetch_funding_history across
all 12 verified venues.

Phase 3 of the True PnL primitive needs to sum settled funding payments
per leg over the position window [opened_at, closed_at]. Before building,
we characterize each venue's fetch_funding_history endpoint:

  1. Does the venue support it? (CCXT capability check + live attempt)
  2. What's the entry shape per event? Key fields: symbol, timestamp,
     amount, code (currency), info (raw venue payload).
  3. Sign convention: is `amount` POSITIVE when user RECEIVED funding
     (CCXT's stated convention) or paid? Empirical test against past
     known-rate-direction events.
  4. Currency: USDT (consistent with our linear perps) or other?
  5. since/until parameter handling — honored or quirky?
  6. limit cap — like fetch_my_trades, some venues cap.
  7. Symbol identifier field — is it `t['symbol']` (CCXT-unified) or
     buried in `info.contractCode` etc.?

Read-only; uses past account history. Zero capital at risk.
Counterpart to probe_fee_shape.py and probe_venue_quirks.py.
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
SINCE_MS_30D = int(time.time() * 1000) - 30 * 24 * 3600 * 1000


async def probe_venue(venue: str) -> dict:
    out: dict = {"venue": venue}
    ex = get_exchange(venue)
    try:
        await ex.load_markets()

        # Capability check first
        out["has_fetchFundingHistory"] = ex.has.get("fetchFundingHistory")

        if not ex.has.get("fetchFundingHistory"):
            out["error"] = "fetchFundingHistory not in ex.has"
            return out

        if SYMBOL not in ex.markets:
            out["error"] = f"symbol {SYMBOL} not in markets"
            return out

        # ------- Bare call: no params, see what comes back -------
        try:
            history = await ex.fetch_funding_history(SYMBOL, limit=10)
            out["bare_call_count"] = len(history)
            if history:
                sample = history[0]
                out["sample_keys"] = sorted(sample.keys())
                out["sample_symbol"] = sample.get("symbol")
                out["sample_amount"] = sample.get("amount")
                out["sample_currency"] = sample.get("code") or sample.get("currency")
                out["sample_timestamp"] = sample.get("timestamp")
                out["sample_id"] = sample.get("id")
                # Probe info keys so we can find venue-specific signed/raw fields
                info = sample.get("info") or {}
                out["info_keys"] = sorted(info.keys())[:20]
                # Looking for sign-revealing keys
                for k in ("fundingPayment", "fundingRate", "amount", "paid",
                          "receivedAmount", "fundingFee", "income", "settle_amount"):
                    if k in info:
                        out[f"info.{k}"] = info[k]
        except Exception as e:
            out["bare_call_err"] = f"{type(e).__name__}: {str(e)[:200]}"

        # ------- since param test -------
        try:
            with_since = await ex.fetch_funding_history(SYMBOL, since=SINCE_MS_30D, limit=10)
            out["since_count"] = len(with_since)
            if with_since:
                oldest = min(e.get("timestamp") or 0 for e in with_since)
                if oldest >= SINCE_MS_30D:
                    out["since_param"] = "honored"
                else:
                    out["since_param"] = f"BROKEN (oldest_ts={oldest} < since={SINCE_MS_30D})"
            else:
                out["since_param"] = "no events in 30d window (inconclusive)"
        except Exception as e:
            out["since_err"] = f"{type(e).__name__}: {str(e)[:120]}"

        # ------- limit cap test -------
        for lim in (100, 500):
            try:
                t = await ex.fetch_funding_history(SYMBOL, limit=lim)
                out[f"limit{lim}"] = f"OK ({len(t)} events)"
            except Exception as e:
                out[f"limit{lim}"] = f"ERR {type(e).__name__}: {str(e)[:100]}"

    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        try:
            await ex.close()
        except Exception:
            pass
    return out


async def main():
    print(f"Funding-history shape probe across {len(VENUES)} venues — read-only")
    print(f"Symbol: {SYMBOL}\n")

    results = await asyncio.gather(
        *(probe_venue(v) for v in VENUES), return_exceptions=True
    )

    print(f"{'venue':14s} {'has':5s} {'count':6s} {'currency':9s} {'sign sample':14s} {'since':30s} {'limit100':14s} {'limit500':30s}")
    print("-" * 140)
    for r in results:
        if isinstance(r, Exception):
            print(f"  EXCEPTION: {r!r}")
            continue
        if "error" in r and "has_fetchFundingHistory" not in r:
            print(f"{r['venue']:14s} INIT ERR: {r['error']}")
            continue
        has = "Y" if r.get("has_fetchFundingHistory") else "N"
        count = str(r.get("bare_call_count", "—"))
        cur = str(r.get("sample_currency") or "—")
        amt = r.get("sample_amount")
        sign_sample = f"{amt}" if amt is not None else "—"
        since = str(r.get("since_param", "—"))[:30]
        l100 = str(r.get("limit100", "—"))[:14]
        l500 = str(r.get("limit500", "—"))[:30]
        print(f"{r['venue']:14s} {has:5s} {count:6s} {cur:9s} {sign_sample[:14]:14s} {since:30s} {l100:14s} {l500:30s}")
        if r.get("error"):
            print(f"  → error: {r['error']}")
        if r.get("bare_call_err"):
            print(f"  → bare call err: {r['bare_call_err']}")

    with open("probe_logs/funding_shape.json", "w") as f:
        json.dump([r if not isinstance(r, Exception) else str(r) for r in results],
                  f, indent=2, default=str)
    print(f"\nFull dump → probe_logs/funding_shape.json")


asyncio.run(main())
