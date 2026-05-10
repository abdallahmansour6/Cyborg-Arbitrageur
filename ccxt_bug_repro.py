"""
CCXT bug repros — minimal, isolated, captures full tracebacks.

Investigates blockers surfaced by the 2026-05-09 probe and smoketest runs:

  1. bitmart.create_order — AttributeError 'NoneType' has no .lower()
     Suspected location: ccxt/async_support/bitmart.py:5459 (handle_errors).
     Reads `safe_string(response, 'message')`, then calls `.lower()`
     unconditionally. Crashes whenever the venue response omits 'message'.

  2. htx.fetch_balance — ExchangeError 4002 unified_account_info
     Venue requires the v3 unified-account endpoint. CCXT 4.5.51's htx
     class defaults to v1 cross/isolated path. Bypass via params or
     options: 'uta' / 'unified' flag routes to v3.

  3. kucoinfutures.create_order — InvalidOrder 330005 margin mode mismatch
     Order's margin mode doesn't match account-level setting. Try passing
     `params={'marginMode': 'cross'}` to override.

  4. set_leverage ArgumentsRequired across mexc / bitmart / bingx
     CCXT 4.5.51 hard-fails on stock `set_leverage(leverage, symbol)`
     for these venues. mexc needs openType + positionType, bitmart needs
     marginMode, bingx needs side. Repro confirms the param overrides
     pinned in `venue_overrides.VENUE_SET_LEVERAGE_PARAMS`.

  5. kucoinfutures.fetch_order — OrderNotFound (100001) on fresh IOC
     KuCoin's GET /api/v1/orders/{orderId} endpoint is eventually
     consistent — a fresh id raises OrderNotFound for 50–500 ms after
     placement. Repro verifies the lag exists and confirms the
     bounded-retry approach in `receipt_resolver._fetch_order_resilient`.

Each repro:
  - Hits the failing operation
  - Captures and prints the full traceback (CCXT-internal frames included)
  - Tries hypothesis bypasses
  - Reports which bypass worked

Run:
    python3 ccxt_bug_repro.py                            # all
    python3 ccxt_bug_repro.py bitmart                    # bug 1
    python3 ccxt_bug_repro.py htx                        # bug 2
    python3 ccxt_bug_repro.py kucoin                     # bug 3
    python3 ccxt_bug_repro.py leverage                   # bug 4
    python3 ccxt_bug_repro.py kucoin_indexing_lag        # bug 5

This is throwaway infrastructure. Once a bypass is confirmed it gets
folded into config.py / venue_overrides.py / receipt_resolver.py, and
this file can be deleted (or kept as a regression-check on future
CCXT bumps).
"""
import asyncio
import sys
import traceback

import ccxt.pro as ccxtpro
from ccxt.base.errors import ExchangeError

from config import get_exchange


def _print_full_traceback(exc: BaseException) -> None:
    """Full traceback — CCXT-internal frames included. Default print_exc()
    is enough but call the helper to keep the call sites uniform."""
    traceback.print_exception(type(exc), exc, exc.__traceback__)


# ---------------------------------------------------------------------------
# Bug 1 — bitmart create_order AttributeError
# ---------------------------------------------------------------------------


async def repro_bitmart() -> None:
    print("=" * 90)
    print("BUG 1: bitmart.create_order — AttributeError 'NoneType' has no .lower()")
    print("=" * 90)

    # Step 1 — confirm the bug fires on the original probe params
    print("\n--- 1A: stock client + params={'timeInForce': 'IOC'} ---")
    client = get_exchange("bitmart")
    try:
        await client.load_markets()
        symbol = "BTC/USDT:USDT"
        market = client.markets[symbol]
        ob = await client.fetch_order_book(symbol, limit=20)
        best_bid = float(ob["bids"][0][0])
        amount = float(client.amount_to_precision(symbol, market["limits"]["amount"]["min"] or 1))
        price = float(client.price_to_precision(symbol, best_bid * 0.99))
        print(f"    placing: buy {amount} @ {price} ({symbol})")
        try:
            r = await client.create_order(symbol, "limit", "buy", amount, price, params={"timeInForce": "IOC"})
            print(f"    UNEXPECTED SUCCESS: id={r.get('id')} status={r.get('status')}")
        except Exception as e:
            print(f"    FAILED ({type(e).__name__}): {e}")
            print("    Full traceback:")
            _print_full_traceback(e)
    finally:
        await client.close()

    # Step 2 — apply the monkey-patch (subclass with fixed handle_errors)
    # and re-attempt the same order.
    print("\n--- 1B: subclass with patched handle_errors() ---")

    class PatchedBitmart(ccxtpro.bitmart):
        def handle_errors(self, code, reason, url, method, headers, body, response, requestHeaders, requestBody):
            # CCXT 4.5.51 bitmart.py:5459 calls message.lower() before guarding
            # message-is-None. When the venue response omits 'message' (rare
            # but happens e.g. on raw IP-block pages and on some contract
            # endpoints), the unconditional .lower() raises AttributeError.
            # This patch guards .lower() but preserves all other logic.
            if response is None:
                return None
            message = self.safe_string(response, 'message')
            messageLower = message.lower() if message is not None else ''
            isErrorMessage = (
                (message is not None)
                and (messageLower != 'ok')
                and (messageLower != 'success')
            )
            errorCode = self.safe_string(response, 'code')
            isErrorCode = (errorCode is not None) and (errorCode != '1000')
            if isErrorCode or isErrorMessage:
                feedback = self.id + ' ' + (body or '')
                self.throw_exactly_matched_exception(self.exceptions['exact'], message, feedback)
                self.throw_broadly_matched_exception(self.exceptions['broad'], message, feedback)
                self.throw_exactly_matched_exception(self.exceptions['exact'], errorCode, feedback)
                self.throw_broadly_matched_exception(self.exceptions['broad'], errorCode, feedback)
                raise ExchangeError(feedback)
            return None

    # Build a patched client with the same creds the engine uses.
    import os
    creds = {
        "apiKey": os.getenv("BITMART_API_KEY"),
        "secret": os.getenv("BITMART_SECRET"),
        "uid": os.getenv("BITMART_UID"),
        "password": os.getenv("BITMART_PASSWORD") or None,
        "enableRateLimit": False,
        "options": {"defaultType": "swap"},
        "newUpdates": True,
    }
    if not creds["password"]:
        creds.pop("password")
    client = PatchedBitmart({k: v for k, v in creds.items() if v is not None})
    try:
        await client.load_markets()
        symbol = "BTC/USDT:USDT"
        market = client.markets[symbol]
        ob = await client.fetch_order_book(symbol, limit=20)
        best_bid = float(ob["bids"][0][0])
        amount = float(client.amount_to_precision(symbol, market["limits"]["amount"]["min"] or 1))
        price = float(client.price_to_precision(symbol, best_bid * 0.99))
        print(f"    placing: buy {amount} @ {price} ({symbol})")
        try:
            r = await client.create_order(symbol, "limit", "buy", amount, price, params={"timeInForce": "IOC"})
            status = r.get("status")
            order_id = r.get("id")
            filled = r.get("filled")
            print(f"    SUCCESS: id={order_id} status={status} filled={filled}")
            print(f"    receipt populated keys: {sorted(k for k, v in r.items() if v is not None)}")
            # Cleanup
            if order_id:
                await asyncio.sleep(1.0)
                try:
                    open_orders = await client.fetch_open_orders(symbol)
                    if any(o.get("id") == order_id for o in open_orders):
                        await client.cancel_order(order_id, symbol)
                        print(f"    Defensive cancel: ok")
                    else:
                        print(f"    Order auto-canceled (not in open_orders)")
                except Exception as e:
                    print(f"    open-orders check failed: {type(e).__name__}: {e}")
        except Exception as e:
            print(f"    FAILED ({type(e).__name__}): {e}")
            print("    Full traceback:")
            _print_full_traceback(e)
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Bug 2 — htx fetch_balance v3 endpoint
# ---------------------------------------------------------------------------


async def repro_htx() -> None:
    print("\n" + "=" * 90)
    print("BUG 2: htx.fetch_balance — 4002 unified_account_info / v3 endpoint")
    print("=" * 90)

    # Step 1 — confirm the bug
    print("\n--- 2A: stock fetch_balance() ---")
    client = get_exchange("htx")
    try:
        try:
            r = await client.fetch_balance()
            print(f"    UNEXPECTED SUCCESS: keys={list(r.keys())[:8]}")
        except Exception as e:
            print(f"    FAILED ({type(e).__name__}): {str(e)[:300]}")
    finally:
        await client.close()

    # Step 2 — try the documented uta/unified flags
    print("\n--- 2B: fetch_balance(params={'uta': True}) ---")
    client = get_exchange("htx")
    try:
        try:
            r = await client.fetch_balance(params={"uta": True})
            usdt_free = (r.get("USDT") or {}).get("free")
            print(f"    SUCCESS: USDT.free={usdt_free}, total_keys={len(r.keys())}")
        except Exception as e:
            print(f"    FAILED ({type(e).__name__}): {str(e)[:300]}")
    finally:
        await client.close()

    print("\n--- 2C: fetch_balance(params={'unified': True}) ---")
    client = get_exchange("htx")
    try:
        try:
            r = await client.fetch_balance(params={"unified": True})
            usdt_free = (r.get("USDT") or {}).get("free")
            print(f"    SUCCESS: USDT.free={usdt_free}, total_keys={len(r.keys())}")
        except Exception as e:
            print(f"    FAILED ({type(e).__name__}): {str(e)[:300]}")
    finally:
        await client.close()

    # Step 3 — try setting the option at the client level (so EVERY fetch_balance call goes to v3)
    print("\n--- 2D: client.options['fetchBalance']['uta']=True ---")
    client = get_exchange("htx")
    try:
        client.options.setdefault("fetchBalance", {})["uta"] = True
        try:
            r = await client.fetch_balance()
            usdt_free = (r.get("USDT") or {}).get("free")
            print(f"    SUCCESS: USDT.free={usdt_free}, total_keys={len(r.keys())}")
        except Exception as e:
            print(f"    FAILED ({type(e).__name__}): {str(e)[:300]}")
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Bug 3 — kucoinfutures create_order margin mode mismatch
# ---------------------------------------------------------------------------


async def repro_kucoin() -> None:
    print("\n" + "=" * 90)
    print("BUG 3: kucoinfutures.create_order — 330005 margin mode mismatch")
    print("=" * 90)

    async def _try_create(extra_params: dict) -> None:
        client = get_exchange("kucoinfutures")
        try:
            await client.load_markets()
            symbol = "BTC/USDT:USDT"
            market = client.markets[symbol]
            ob = await client.fetch_order_book(symbol, limit=20)
            best_bid = float(ob["bids"][0][0])
            amount = float(client.amount_to_precision(symbol, market["limits"]["amount"]["min"] or 1))
            price = float(client.price_to_precision(symbol, best_bid * 0.99))
            params = {"timeInForce": "IOC", **extra_params}
            print(f"    placing: buy {amount} @ {price} params={params}")
            try:
                r = await client.create_order(symbol, "limit", "buy", amount, price, params=params)
                status = r.get("status")
                order_id = r.get("id")
                filled = r.get("filled")
                print(f"    SUCCESS: id={order_id} status={status} filled={filled}")
                print(f"    receipt populated keys: {sorted(k for k, v in r.items() if v is not None)}")
                if order_id:
                    await asyncio.sleep(1.0)
                    try:
                        open_orders = await client.fetch_open_orders(symbol)
                        if any(o.get("id") == order_id for o in open_orders):
                            await client.cancel_order(order_id, symbol)
                            print(f"    Defensive cancel: ok")
                        else:
                            print(f"    Order auto-canceled (not in open_orders)")
                    except Exception as e:
                        print(f"    open-orders check failed: {type(e).__name__}: {e}")
            except Exception as e:
                print(f"    FAILED ({type(e).__name__}): {str(e)[:250]}")
        finally:
            await client.close()

    print("\n--- 3A: stock IOC, no marginMode override ---")
    await _try_create({})

    print("\n--- 3B: params={'marginMode': 'cross'} ---")
    await _try_create({"marginMode": "cross"})

    print("\n--- 3C: params={'marginMode': 'isolated'} ---")
    await _try_create({"marginMode": "isolated"})

    # KuCoin Futures has its own venue-native field name `crossMode` (bool).
    # Try passing it explicitly in case CCXT's marginMode translation isn't
    # plumbed through the futures create_order path.
    print("\n--- 3D: params={'crossMode': True} (venue-native) ---")
    await _try_create({"crossMode": True})


# ---------------------------------------------------------------------------
# Bug 4 — set_leverage ArgumentsRequired (mexc / bitmart / bingx)
# ---------------------------------------------------------------------------


async def repro_leverage() -> None:
    print("\n" + "=" * 90)
    print("BUG 4: set_leverage ArgumentsRequired across mexc / bitmart / bingx")
    print("=" * 90)

    # Each tuple: (venue, symbol, [(label, params), ...]).
    # First entry per venue is stock CCXT (expected: ArgumentsRequired);
    # subsequent entries are the per-venue overrides we ship in
    # VENUE_SET_LEVERAGE_PARAMS.
    cases = [
        ("mexc", "XRP/USDT:USDT", [
            ("stock (no params)", {}),
            ("override #1: openType=2 positionType=1", {"openType": 2, "positionType": 1}),
            ("override #2: openType=2 positionType=2", {"openType": 2, "positionType": 2}),
        ]),
        ("bitmart", "XRP/USDT:USDT", [
            ("stock (no params)", {}),
            ("override: marginMode=cross", {"marginMode": "cross"}),
        ]),
        ("bingx", "XRP/USDT:USDT", [
            ("stock (no params)", {}),
            ("override: side=BOTH", {"side": "BOTH"}),
        ]),
        ("xt", "XRP/USDT:USDT", [
            ("stock (no params)", {}),
            ("override #1: positionSide=LONG", {"positionSide": "LONG"}),
            ("override #2: positionSide=SHORT", {"positionSide": "SHORT"}),
        ]),
    ]
    leverage = 1  # minimum, harmless

    for venue, symbol, attempts in cases:
        print(f"\n--- {venue} ---")
        client = get_exchange(venue)
        try:
            await client.load_markets()
            for label, params in attempts:
                print(f"  {label}: set_leverage({leverage}, {symbol!r}, params={params})")
                try:
                    r = await client.set_leverage(leverage, symbol, params=params)
                    print(f"    SUCCESS: {str(r)[:200]}")
                except Exception as e:
                    print(f"    FAILED ({type(e).__name__}): {str(e)[:200]}")
        finally:
            await client.close()


# ---------------------------------------------------------------------------
# Bug 5 — kucoinfutures.fetch_order OrderNotFound (100001) on fresh IOC
# ---------------------------------------------------------------------------


async def repro_kucoin_indexing_lag() -> None:
    print("\n" + "=" * 90)
    print("BUG 5: kucoinfutures.fetch_order — OrderNotFound (100001) on fresh IOC")
    print("=" * 90)
    print("""
Anchor: 2026-05-09 20:39:23 cross_venue_smoketest XRP run on
kucoinfutures × okx. IOC dispatched at .220; immediate fetch_order(id)
raised OrderNotFound (code 100001) at .465 — 245 ms later. The order
had partially filled (10 XRP visible in venue UI) — KuCoin matched it
but its order-by-id index was lagging.

This repro: place a far-from-spread IOC (1% below bid; cannot fill on
liquid pairs), then poll fetch_order at 50 ms intervals capturing
exactly when the venue stops returning OrderNotFound. The first
non-OrderNotFound timestamp is the empirical indexing lag.
""")

    from ccxt.base.errors import OrderNotFound
    import time

    client = get_exchange("kucoinfutures")
    try:
        await client.load_markets()
        symbol = "BTC/USDT:USDT"
        market = client.markets[symbol]
        ob = await client.fetch_order_book(symbol, limit=20)
        best_bid = float(ob["bids"][0][0])
        amount = float(client.amount_to_precision(symbol, market["limits"]["amount"]["min"] or 1))
        price = float(client.price_to_precision(symbol, best_bid * 0.99))
        params = {"timeInForce": "IOC", "marginMode": "cross"}

        print(f"  placing: buy {amount} @ {price} params={params}")
        try:
            r = await client.create_order(symbol, "limit", "buy", amount, price, params=params)
        except Exception as e:
            print(f"  create_order FAILED ({type(e).__name__}): {str(e)[:250]}")
            return
        order_id = r.get("id")
        place_ts = time.monotonic()
        print(f"  placement OK: id={order_id} status={r.get('status')} filled={r.get('filled')}")

        # Poll fetch_order at 50 ms intervals for up to 5 s; capture every result.
        results = []
        deadline = place_ts + 5.0
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            t = time.monotonic()
            try:
                resolved = await client.fetch_order(order_id, symbol)
                lag_ms = (t - place_ts) * 1000.0
                print(f"  attempt {attempt} @ +{lag_ms:.0f}ms: SUCCESS status={resolved.get('status')} filled={resolved.get('filled')}")
                results.append(("ok", lag_ms))
                break
            except OrderNotFound:
                lag_ms = (t - place_ts) * 1000.0
                print(f"  attempt {attempt} @ +{lag_ms:.0f}ms: OrderNotFound")
                results.append(("not_found", lag_ms))
            except Exception as e:
                lag_ms = (t - place_ts) * 1000.0
                print(f"  attempt {attempt} @ +{lag_ms:.0f}ms: {type(e).__name__}: {str(e)[:120]}")
                break
            await asyncio.sleep(0.05)

        not_found_count = sum(1 for s, _ in results if s == "not_found")
        if not_found_count > 0:
            last_not_found_ms = max(lag for s, lag in results if s == "not_found")
            print(f"\n  Indexing lag observed: {not_found_count} OrderNotFound responses;"
                  f" last at +{last_not_found_ms:.0f}ms")
        else:
            print("\n  No indexing lag observed on this run (venue may have warmed up).")

        # Defensive cleanup
        try:
            opens = await client.fetch_open_orders(symbol)
            if any(o.get("id") == order_id for o in opens):
                await client.cancel_order(order_id, symbol)
                print(f"  Defensive cancel: ok")
        except Exception:
            pass
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------


async def main():
    targets = sys.argv[1:] if len(sys.argv) > 1 else [
        "bitmart", "htx", "kucoin", "leverage", "kucoin_indexing_lag",
    ]
    for t in targets:
        if t == "bitmart":
            await repro_bitmart()
        elif t == "htx":
            await repro_htx()
        elif t == "kucoin":
            await repro_kucoin()
        elif t == "leverage":
            await repro_leverage()
        elif t == "kucoin_indexing_lag":
            await repro_kucoin_indexing_lag()
        else:
            print(f"unknown repro target: {t!r}")


if __name__ == "__main__":
    asyncio.run(main())
