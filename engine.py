import asyncio
from aiohttp import web
import ccxt.async_support as ccxt_async
from config import get_exchange
from utils import log, load_state, save_state
from notifier import send_pushover


class ArbitrageEngine:
    def __init__(self):
        self.exchanges = {}
        self.balances = {}
        self.positions = load_state()

    async def get_or_create_exchange(self, ex_id):
        if ex_id not in self.exchanges:
            log(f"Initializing {ex_id}...", ex_id)
            self.exchanges[ex_id] = get_exchange(ex_id)
            await self.exchanges[ex_id].load_markets()

            try:
                initial_balance = await self.exchanges[ex_id].fetch_balance()
                self.balances[ex_id] = {
                    "USDT": initial_balance.get("USDT", {}).get("free", 0)
                }
                log(
                    f"Initial USDT Balance seeded: {self.balances[ex_id]['USDT']}",
                    ex_id,
                )
            except Exception as e:
                log(f"Failed to fetch initial balance: {e}", ex_id)
                raise e

            # Start WebSocket Listener
            asyncio.create_task(self.watch_balance_loop(ex_id))
            # Start REST Keep-Alive (Crucial for Latency)
            asyncio.create_task(self.rest_heartbeat_loop(ex_id))

        return self.exchanges[ex_id]

    async def pre_warm(self):
        exchange_ids = {pos["long_ex"] for pos in self.positions.values()} | {
            pos["short_ex"] for pos in self.positions.values()
        }

        if exchange_ids:
            log(
                f"Pre-warming {len(exchange_ids)} exchanges from saved state...",
                "ENGINE",
            )
            await asyncio.gather(
                *(self.get_or_create_exchange(ex) for ex in exchange_ids)
            )
            log("Pre-warming complete. All systems hot.", "ENGINE")

    async def watch_balance_loop(self, ex_id):
        exchange = self.exchanges[ex_id]
        log("Started WebSocket balance watcher.", ex_id)
        while True:
            try:
                balance = await exchange.watch_balance()
                self.balances[ex_id]["USDT"] = balance.get("USDT", {}).get("free", 0)
            except Exception as e:
                log(f"Balance watch error: {e}", ex_id)
                await asyncio.sleep(5)

    async def rest_heartbeat_loop(self, ex_id):
        """Keeps the REST HTTP/TLS socket hot to prevent 100ms connection drops."""
        exchange = self.exchanges[ex_id]
        log("Started REST heartbeat keep-alive.", ex_id)
        while True:
            try:
                await asyncio.sleep(30)
                await exchange.fetch_time()
            except Exception:
                pass  # Ignore transient errors on heartbeat

    async def handle_warmup(self, request):
        data = await request.json()
        symbol, exchanges, leverage = (
            data["symbol"],
            data["exchanges"],
            data["leverage"],
        )
        try:
            tasks = []
            for ex_id in exchanges:
                ex = await self.get_or_create_exchange(ex_id)
                tasks.append(ex.set_leverage(leverage, symbol))

            log(
                f"Setting leverage to {leverage}x for {symbol} on {exchanges}...",
                "WARMUP",
            )

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Audit results for silent exchange-specific API failures
            failed_exchanges = []
            for ex_id, res in zip(exchanges, results):
                if isinstance(res, Exception):
                    log(f"Leverage setup failed for {ex_id}: {res}", "WARMUP_ERROR")
                    failed_exchanges.append(ex_id)

            if failed_exchanges:
                return web.json_response(
                    {"error": f"Warmup failed on {failed_exchanges}. Check logs."},
                    status=400,
                )

            return web.json_response(
                {"message": f"Warmup complete. {symbol} at {leverage}x on {exchanges}"}
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_entry(self, request):
        data = await request.json()
        symbol, long_ex_id, short_ex_id, amount = (
            data["symbol"],
            data["long"],
            data["short"],
            data["amount"],
        )

        # Preflight Checks
        if symbol in self.positions:
            pos = self.positions[symbol]
            if pos["long_ex"] != long_ex_id or pos["short_ex"] != short_ex_id:
                return web.json_response(
                    {
                        "error": f"Active position uses {pos['long_ex']}/{pos['short_ex']}. Cannot mix exchanges."
                    },
                    status=400,
                )

        if long_ex_id not in self.exchanges or short_ex_id not in self.exchanges:
            return web.json_response(
                {"error": "Exchanges not warmed up. Run warmup first."}, status=400
            )

        ex_long = self.exchanges[long_ex_id]
        ex_short = self.exchanges[short_ex_id]

        # 0ms RAM Preflight
        for ex_id, ex in [(long_ex_id, ex_long), (short_ex_id, ex_short)]:
            market = ex.markets.get(symbol)
            if not market:
                return web.json_response(
                    {"error": f"Market {symbol} not found on {ex_id}"}, status=400
                )

            # Safe parsing for min amount
            min_amount = market.get("limits", {}).get("amount", {}).get("min")
            if min_amount is not None and amount < min_amount:
                return web.json_response(
                    {
                        "error": f"Amount {amount} below min lot size {min_amount} on {ex_id}"
                    },
                    status=400,
                )

        log(f"Preflight passed. Firing Market Orders for {amount} {symbol}...", "ENTRY")

        results = await asyncio.gather(
            self.execute_order(ex_long, symbol, "buy", amount),
            self.execute_order(ex_short, symbol, "sell", amount),
            return_exceptions=True,
        )

        res_long, res_short = results
        success_long = not isinstance(res_long, Exception)
        success_short = not isinstance(res_short, Exception)

        if success_long and success_short:
            if symbol in self.positions:
                self.positions[symbol]["amount"] += amount
                log(
                    f"Scaled in. New total size: {self.positions[symbol]['amount']} {symbol}",
                    "ENTRY",
                )
            else:
                self.positions[symbol] = {
                    "long_ex": long_ex_id,
                    "short_ex": short_ex_id,
                    "amount": amount,
                }
                log(f"Delta Neutral established for {symbol}", "ENTRY")

            save_state(self.positions)
            return web.json_response({"message": "Entry successful."})

        # Rollback Logic
        elif success_long and not success_short:
            log(f"Short failed: {res_short}. Rolling back Long.", "CRITICAL")
            await self.execute_order(
                ex_long, symbol, "sell", amount, params={"reduceOnly": True}
            )
            return web.json_response(
                {"error": f"Short failed: {res_short}. Long rolled back."}, status=500
            )
        elif success_short and not success_long:
            log(f"Long failed: {res_long}. Rolling back Short.", "CRITICAL")
            await self.execute_order(ex_short, symbol, "buy", amount)
            return web.json_response(
                {"error": f"Long failed: {res_long}. Short rolled back."}, status=500
            )
        else:
            return web.json_response(
                {"error": f"Both failed. L:{res_long} S:{res_short}"}, status=500
            )

    async def handle_exit(self, request):
        data = await request.json()
        symbol = data["symbol"]

        pos = self.positions.get(symbol)
        if not pos:
            return web.json_response(
                {"error": "No active position found for this symbol."}, status=400
            )

        amount = pos["amount"]
        ex_long = self.exchanges[pos["long_ex"]]
        ex_short = self.exchanges[pos["short_ex"]]

        log(f"Firing Exit Orders for {amount} {symbol}...", "EXIT")
        results = await asyncio.gather(
            self.execute_order(
                ex_long, symbol, "sell", amount, params={"reduceOnly": True}
            ),
            self.execute_order(
                ex_short, symbol, "buy", amount, params={"reduceOnly": True}
            ),
            return_exceptions=True,
        )

        if any(isinstance(r, Exception) for r in results):
            log(f"Exit partially failed: {results}", "CRITICAL")
            send_pushover(
                "CRITICAL: EXIT FAILED", "Manual intervention required.", priority=2
            )
            return web.json_response({"error": "Exit failed. Check logs."}, status=500)

        del self.positions[symbol]
        save_state(self.positions)
        log(f"Position flattened for {symbol}", "EXIT")
        return web.json_response({"message": "Exit successful."})

    async def execute_order(
        self, exchange, symbol, side, amount, params=None, max_retries=3
    ):
        """Executes a market order with transient network error handling."""
        if params is None:
            params = {}

        for attempt in range(1, max_retries + 1):
            try:
                return await exchange.create_market_order(
                    symbol, side, amount, params=params
                )
            except (
                ccxt_async.NetworkError,
                ccxt_async.RateLimitExceeded,
                ccxt_async.RequestTimeout,
            ) as e:
                log(
                    f"Transient error (Attempt {attempt}/{max_retries}): {e}",
                    exchange.id,
                )
                if attempt == max_retries:
                    raise e
                await asyncio.sleep(0.1)  # 100ms micro-pause before retry
            except Exception as e:
                log(f"Structural error: {e}", exchange.id)
                raise e

    async def shutdown(self):
        """Gracefully closes all exchange sockets on exit."""
        log("Shutting down exchange connections...", "ENGINE")
        await asyncio.gather(*(ex.close() for ex in self.exchanges.values()))


async def start_server():
    engine = ArbitrageEngine()
    await engine.pre_warm()

    app = web.Application()
    app.router.add_post("/warmup", engine.handle_warmup)
    app.router.add_post("/entry", engine.handle_entry)
    app.router.add_post("/exit", engine.handle_exit)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()

    log("Engine Online. Awaiting IPC commands on port 8080...", "ENGINE")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await engine.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        log("Engine shutdown by Cyborg.", "ENGINE")
