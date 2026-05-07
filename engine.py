import asyncio
from aiohttp import web
from config import get_exchange
from utils import log, load_state, save_state, append_closed_trade, now_str
from notifier import send_pushover
from execution import run_slicing_loop


# Max wait for the first L2 snapshot to arrive after subscribing.
# Sized for transcontinental WS handshake + initial snapshot fetch.
FIRST_SNAPSHOT_TIMEOUT_S = 10.0


class ArbitrageEngine:
    def __init__(self):
        self.exchanges = {}        # ex_id -> CCXT Pro instance
        self.order_books = {}      # (ex_id, symbol) -> latest L2 snapshot {bids, asks, timestamp}
        self.positions = load_state()  # symbol -> {long_ex, short_ex, amount}
        self._book_tasks = {}      # (ex_id, symbol) -> asyncio.Task (idempotency guard)
        self._abort_events = {}    # symbol -> asyncio.Event. Presence => active slicing loop.
                                   # Doubles as concurrent-execution guard for /entry and /exit.

    async def get_or_create_exchange(self, ex_id):
        """Instantiate, load markets, start REST keep-alive. Idempotent."""
        if ex_id not in self.exchanges:
            log(f"Initializing {ex_id}...", ex_id)
            self.exchanges[ex_id] = get_exchange(ex_id)
            await self.exchanges[ex_id].load_markets()
            asyncio.create_task(self.rest_heartbeat_loop(ex_id))
        return self.exchanges[ex_id]

    async def subscribe_order_book(self, ex_id, symbol):
        """Idempotent. Spawns a watch_order_book streamer for (ex_id, symbol)."""
        key = (ex_id, symbol)
        if key in self._book_tasks and not self._book_tasks[key].done():
            return
        self._book_tasks[key] = asyncio.create_task(self.watch_order_book_loop(ex_id, symbol))

    async def watch_order_book_loop(self, ex_id, symbol):
        """
        Persistent CCXT Pro watch_order_book stream. Updates the RAM cache on
        every delta tick. Survives transient network errors via sleep-retry.
        """
        exchange = self.exchanges[ex_id]
        log(f"Started L2 stream for {symbol}.", ex_id)
        while True:
            try:
                # 300s ceiling on a single await guards against silent socket death:
                # if no deltas arrive for 5 minutes the await is forced to yield
                # and we re-enter watch_order_book, which will reconnect if needed.
                book = await asyncio.wait_for(
                    exchange.watch_order_book(symbol), timeout=300.0
                )
                self.order_books[(ex_id, symbol)] = book
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"L2 stream error for {symbol}: {e}", ex_id)
                await asyncio.sleep(5)

    async def rest_heartbeat_loop(self, ex_id):
        """Keeps REST HTTP/TLS socket hot to prevent ~100ms reconnect on first call."""
        exchange = self.exchanges[ex_id]
        log("Started REST heartbeat keep-alive.", ex_id)
        while True:
            try:
                await asyncio.sleep(30)
                await exchange.fetch_time()
            except Exception:
                pass

    async def _await_first_snapshot(self, ex_id, symbol, timeout=FIRST_SNAPSHOT_TIMEOUT_S):
        """Block until the L2 cache has its first snapshot for this pair."""
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while (ex_id, symbol) not in self.order_books:
            if loop.time() > deadline:
                raise TimeoutError(
                    f"L2 snapshot for {symbol} on {ex_id} not received within {timeout}s"
                )
            await asyncio.sleep(0.05)

    async def pre_warm(self):
        """
        Crash-recovery boot: re-instantiate exchanges and re-subscribe L2 streams
        for every symbol referenced in the saved position ledger.
        """
        if not self.positions:
            return

        ex_ids = {p["long_ex"] for p in self.positions.values()} | {
            p["short_ex"] for p in self.positions.values()
        }
        log(f"Pre-warming {len(ex_ids)} exchanges from saved state...", "ENGINE")
        await asyncio.gather(*(self.get_or_create_exchange(ex) for ex in ex_ids))

        sub_tasks = []
        for symbol, pos in self.positions.items():
            for ex_id in (pos["long_ex"], pos["short_ex"]):
                sub_tasks.append(self.subscribe_order_book(ex_id, symbol))
        await asyncio.gather(*sub_tasks)
        log("Pre-warming complete. All systems hot.", "ENGINE")

    def normalize_amount(self, exchange, symbol, base_amount):
        """Translate raw base token qty into exchange-specific contract+precision format."""
        market = exchange.markets[symbol]
        contract_size = market.get("contractSize")
        if contract_size and contract_size > 0:
            raw_amount = base_amount / contract_size
        else:
            raw_amount = base_amount
        return float(exchange.amount_to_precision(symbol, raw_amount))

    def _min_amount_in_base(self, ex_id, symbol):
        """Min lot size expressed in base tokens (handles contractSize divergence)."""
        market = self.exchanges[ex_id].markets[symbol]
        raw_min = market.get("limits", {}).get("amount", {}).get("min") or 0
        contract_size = market.get("contractSize") or 1
        return raw_min * contract_size

    # -------- IPC Handlers --------

    async def handle_warmup(self, request):
        """
        Authenticate, load markets, set leverage, subscribe L2 streams.
        Blocks until the first L2 snapshot lands on every leg — /entry must
        not race an empty RAM cache.
        Payload: {symbol, exchanges: [...], leverage}
        """
        data = await request.json()
        symbol, exchanges, leverage = data["symbol"], data["exchanges"], data["leverage"]
        try:
            # 1. Instantiate all exchanges concurrently (loads markets, spawns heartbeat)
            await asyncio.gather(*(self.get_or_create_exchange(ex) for ex in exchanges))

            # 2. Set leverage per exchange — audit silent per-venue failures
            log(f"Setting leverage to {leverage}x for {symbol} on {exchanges}...", "WARMUP")
            leverage_results = await asyncio.gather(
                *(self.exchanges[ex].set_leverage(leverage, symbol) for ex in exchanges),
                return_exceptions=True,
            )
            failed = [
                ex for ex, res in zip(exchanges, leverage_results)
                if isinstance(res, Exception)
            ]
            for ex, res in zip(exchanges, leverage_results):
                if isinstance(res, Exception):
                    log(f"Leverage setup failed for {ex}: {res}", "WARMUP_ERROR")
            if failed:
                return web.json_response(
                    {"error": f"Warmup failed on {failed}. Check logs."}, status=400
                )

            # 3. Subscribe L2 streams (idempotent — no-op if already streaming)
            await asyncio.gather(
                *(self.subscribe_order_book(ex, symbol) for ex in exchanges)
            )

            # 4. Block until first snapshot lands on each leg
            log(f"Awaiting first L2 snapshot on {len(exchanges)} legs...", "WARMUP")
            await asyncio.gather(
                *(self._await_first_snapshot(ex, symbol) for ex in exchanges)
            )

            log(f"Warmup complete. {symbol} at {leverage}x on {exchanges} — books live.", "WARMUP")
            return web.json_response(
                {"message": f"Warmup complete. {symbol} at {leverage}x on {exchanges}"}
            )
        except Exception as e:
            log(f"Warmup structural failure: {e}", "WARMUP_ERROR")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_entry(self, request):
        """
        Drives the Synchronized Smart Slicing entry sequence.
        Payload: {symbol, long, short, amount, min_entry_basis_bps, max_duration_s}
        """
        data = await request.json()
        symbol = data["symbol"]
        long_ex_id = data["long"]
        short_ex_id = data["short"]
        target_qty = data["amount"]
        basis_floor_bps = data["min_entry_basis_bps"]
        max_duration_s = data["max_duration_s"]

        # Existing position locks the routing — scale-ins must reuse the same legs
        if symbol in self.positions:
            pos = self.positions[symbol]
            if pos["long_ex"] != long_ex_id or pos["short_ex"] != short_ex_id:
                return web.json_response(
                    {"error": f"Active position uses {pos['long_ex']}/{pos['short_ex']}. Cannot mix exchanges."},
                    status=400,
                )

        if long_ex_id not in self.exchanges or short_ex_id not in self.exchanges:
            return web.json_response(
                {"error": "Exchanges not warmed up. Run warmup first."}, status=400
            )

        if (long_ex_id, symbol) not in self.order_books or (short_ex_id, symbol) not in self.order_books:
            return web.json_response(
                {"error": "L2 books not live. Run warmup first."}, status=400
            )

        if symbol in self._abort_events:
            return web.json_response(
                {"error": f"Slicing loop already in flight for {symbol}. Abort it first."},
                status=409,
            )

        abort_event = asyncio.Event()
        self._abort_events[symbol] = abort_event
        try:
            result = await run_slicing_loop(
                self,
                symbol,
                long_ex_id,
                short_ex_id,
                target_qty,
                basis_floor_bps,
                max_duration_s,
                side="entry",
                abort_event=abort_event,
            )
        except Exception as e:
            log(f"Structural failure during entry on {symbol}: {e}", "CRITICAL")
            asyncio.create_task(
                asyncio.to_thread(
                    send_pushover,
                    f"{e}\nManual intervention required.",
                    f"CRITICAL: ENTRY FAILED on {symbol}",
                    2,
                )
            )
            return web.json_response({"error": f"Entry failed: {e}"}, status=500)
        finally:
            self._abort_events.pop(symbol, None)

        if result.filled_base > 0:
            if symbol in self.positions:
                # Scale-in: qty-weight-blend entry VWAPs against the cumulative entry_qty
                # (NOT the residual amount — exits don't dilute the entry-side average).
                pos = self.positions[symbol]
                old_entry_qty = pos.get("entry_qty", pos["amount"])
                new_entry_qty = old_entry_qty + result.filled_base
                pos["entry_vwap_long"] = (
                    pos.get("entry_vwap_long", 0.0) * old_entry_qty
                    + result.vwap_long * result.filled_base
                ) / new_entry_qty
                pos["entry_vwap_short"] = (
                    pos.get("entry_vwap_short", 0.0) * old_entry_qty
                    + result.vwap_short * result.filled_base
                ) / new_entry_qty
                p_ref = (pos["entry_vwap_long"] + pos["entry_vwap_short"]) / 2.0
                pos["entry_basis_bps"] = (
                    (pos["entry_vwap_short"] - pos["entry_vwap_long"]) / p_ref * 10000.0
                    if p_ref > 0 else 0.0
                )
                pos["entry_qty"] = new_entry_qty
                pos["amount"] = new_entry_qty - pos.get("exit_qty", 0.0)
                log(
                    f"Scaled in. entry_qty={new_entry_qty} amount={pos['amount']} "
                    f"entry_basis={pos['entry_basis_bps']:.2f}bps (halt={result.halt_reason})",
                    "ENTRY",
                )
            else:
                self.positions[symbol] = {
                    "long_ex": long_ex_id,
                    "short_ex": short_ex_id,
                    "amount": result.filled_base,
                    "entry_qty": result.filled_base,
                    "entry_vwap_long": result.vwap_long,
                    "entry_vwap_short": result.vwap_short,
                    "entry_basis_bps": result.realized_basis_bps,
                    "exit_qty": 0.0,
                    "exit_vwap_long": None,
                    "exit_vwap_short": None,
                    "exit_basis_bps": None,
                    "opened_at": now_str(),
                }
                log(
                    f"Delta-neutral established for {symbol} amount={result.filled_base} "
                    f"entry_basis={result.realized_basis_bps:.2f}bps (halt={result.halt_reason})",
                    "ENTRY",
                )
            await asyncio.to_thread(save_state, self.positions)
        else:
            log(
                f"Entry filled 0/{target_qty} {symbol} (halt={result.halt_reason}). No state change.",
                "ENTRY",
            )

        return web.json_response({
            "filled": result.filled_base,
            "target": target_qty,
            "halt_reason": result.halt_reason,
            "vwap_long": result.vwap_long,
            "vwap_short": result.vwap_short,
            "realized_basis_bps": result.realized_basis_bps,
        })

    async def handle_exit(self, request):
        """
        Drives the Synchronized Smart Slicing exit sequence (reduceOnly IOCs).
        Payload: {symbol, amount, min_exit_basis_bps, max_duration_s}
        """
        data = await request.json()
        symbol = data["symbol"]
        target_qty = data["amount"]
        basis_floor_bps = data["min_exit_basis_bps"]
        max_duration_s = data["max_duration_s"]

        pos = self.positions.get(symbol)
        if not pos:
            return web.json_response({"error": "No active position for this symbol."}, status=400)

        long_ex_id, short_ex_id = pos["long_ex"], pos["short_ex"]
        target_qty = min(target_qty, pos["amount"])

        if symbol in self._abort_events:
            return web.json_response(
                {"error": f"Slicing loop already in flight for {symbol}. Abort it first."},
                status=409,
            )

        # Defensive subscribe in case streams dropped between pre_warm and now
        await asyncio.gather(
            self.subscribe_order_book(long_ex_id, symbol),
            self.subscribe_order_book(short_ex_id, symbol),
        )
        try:
            await asyncio.gather(
                self._await_first_snapshot(long_ex_id, symbol, timeout=5.0),
                self._await_first_snapshot(short_ex_id, symbol, timeout=5.0),
            )
        except TimeoutError as e:
            return web.json_response({"error": f"Books not live: {e}"}, status=400)

        abort_event = asyncio.Event()
        self._abort_events[symbol] = abort_event
        try:
            result = await run_slicing_loop(
                self,
                symbol,
                long_ex_id,
                short_ex_id,
                target_qty,
                basis_floor_bps,
                max_duration_s,
                side="exit",
                abort_event=abort_event,
            )
        except Exception as e:
            log(f"Structural failure during exit on {symbol}: {e}", "CRITICAL")
            asyncio.create_task(
                asyncio.to_thread(
                    send_pushover,
                    f"{e}\nManual intervention required.",
                    f"CRITICAL: EXIT FAILED on {symbol}",
                    2,
                )
            )
            return web.json_response({"error": f"Exit failed: {e}"}, status=500)
        finally:
            self._abort_events.pop(symbol, None)

        # Blend exit-side VWAPs against cumulative exit_qty (multi-leg unwinds).
        if result.filled_base > 0:
            old_exit_qty = pos.get("exit_qty", 0.0)
            new_exit_qty = old_exit_qty + result.filled_base
            old_exit_l = pos.get("exit_vwap_long") or 0.0
            old_exit_s = pos.get("exit_vwap_short") or 0.0
            pos["exit_vwap_long"] = (
                old_exit_l * old_exit_qty + result.vwap_long * result.filled_base
            ) / new_exit_qty
            pos["exit_vwap_short"] = (
                old_exit_s * old_exit_qty + result.vwap_short * result.filled_base
            ) / new_exit_qty
            p_ref = (pos["exit_vwap_long"] + pos["exit_vwap_short"]) / 2.0
            pos["exit_basis_bps"] = (
                (pos["exit_vwap_long"] - pos["exit_vwap_short"]) / p_ref * 10000.0
                if p_ref > 0 else 0.0
            )
            pos["exit_qty"] = new_exit_qty
            pos["amount"] -= result.filled_base

        # Dust check: residual below either leg's min lot size means we cannot
        # legally trade it again — archive the closed trade and clear the ledger.
        dust = max(
            self._min_amount_in_base(long_ex_id, symbol),
            self._min_amount_in_base(short_ex_id, symbol),
        )
        if pos["amount"] <= dust:
            entry_basis = pos.get("entry_basis_bps", 0.0) or 0.0
            exit_basis = pos.get("exit_basis_bps", 0.0) or 0.0
            closed_record = {
                "symbol": symbol,
                "long_ex": pos["long_ex"],
                "short_ex": pos["short_ex"],
                "entry_qty": pos.get("entry_qty", 0.0),
                "exit_qty": pos.get("exit_qty", 0.0),
                "residual_dust": pos["amount"],
                "entry_vwap_long": pos.get("entry_vwap_long", 0.0),
                "entry_vwap_short": pos.get("entry_vwap_short", 0.0),
                "entry_basis_bps": entry_basis,
                "exit_vwap_long": pos.get("exit_vwap_long", 0.0),
                "exit_vwap_short": pos.get("exit_vwap_short", 0.0),
                "exit_basis_bps": exit_basis,
                "round_trip_basis_bps": entry_basis + exit_basis,
                "opened_at": pos.get("opened_at"),
                "closed_at": now_str(),
            }
            await asyncio.to_thread(append_closed_trade, closed_record)
            del self.positions[symbol]
            log(
                f"Position closed on {symbol}. round_trip_basis="
                f"{closed_record['round_trip_basis_bps']:.2f}bps. Archived to closed_trades.json.",
                "EXIT",
            )
        else:
            log(
                f"Exit filled {result.filled_base}/{target_qty} {symbol}, residual {pos['amount']}, "
                f"exit_basis={pos.get('exit_basis_bps', 0.0):.2f}bps (halt={result.halt_reason}).",
                "EXIT",
            )

        await asyncio.to_thread(save_state, self.positions)

        return web.json_response({
            "filled": result.filled_base,
            "target": target_qty,
            "remaining": self.positions.get(symbol, {}).get("amount", 0),
            "halt_reason": result.halt_reason,
            "vwap_long": result.vwap_long,
            "vwap_short": result.vwap_short,
            "realized_basis_bps": result.realized_basis_bps,
        })

    async def handle_abort(self, request):
        """
        Signals a graceful halt to an in-flight slicing loop for `symbol`.
        The loop will exit at its next cycle boundary — never mid-IOC, so
        delta-neutrality is preserved. Already-filled qty is kept as a hedged
        position via the normal entry/exit completion path.
        Payload: {symbol}
        """
        data = await request.json()
        symbol = data["symbol"]
        event = self._abort_events.get(symbol)
        if event is None:
            return web.json_response(
                {"error": f"No active slicing loop for {symbol}."}, status=404
            )
        event.set()
        log(f"Abort signaled for {symbol}. Loop will halt at next cycle boundary.", "ABORT")
        return web.json_response(
            {"message": f"Abort signaled for {symbol}. Halting at next cycle boundary."}
        )

    async def shutdown(self):
        """Cancel streamers and close all CCXT sockets."""
        log("Shutting down exchange connections...", "ENGINE")
        for task in self._book_tasks.values():
            task.cancel()
        await asyncio.gather(
            *(ex.close() for ex in self.exchanges.values()), return_exceptions=True
        )


async def start_server():
    engine = ArbitrageEngine()
    await engine.pre_warm()

    app = web.Application()
    app.router.add_post("/warmup", engine.handle_warmup)
    app.router.add_post("/entry", engine.handle_entry)
    app.router.add_post("/exit", engine.handle_exit)
    app.router.add_post("/abort", engine.handle_abort)

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
