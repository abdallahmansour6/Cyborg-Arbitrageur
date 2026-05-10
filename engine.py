import asyncio
import time
from aiohttp import web
from config import get_exchange
from primitives import BookSnapshot, ExecutionLeg, ExecutionPair
from utils import log, load_state, save_state, append_closed_trade, now_str
from notifier import send_pushover
from execution import run_slicing_loop
from venue_overrides import (
    is_benign_warmup_error,
    min_notional_usdt_for,
    set_leverage_params_for,
)


def _now_ms() -> int:
    """Local monotonic clock in ms — authoritative for BookSnapshot
    staleness detection. Independent of venue clock skew."""
    return int(time.monotonic() * 1000)


# Max wait for the first L2 snapshot to arrive after subscribing.
# Sized for transcontinental WS handshake + initial snapshot fetch.
FIRST_SNAPSHOT_TIMEOUT_S = 10.0


class ArbitrageEngine:
    def __init__(self):
        self.exchanges = {}        # ex_id -> CCXT Pro instance
        self.order_books = {}      # (ex_id, symbol) -> latest L2 snapshot {bids, asks, timestamp}
        self.positions = load_state()  # base_coin -> {long: {...}, short: {...}, amount_base, ...}
        self._book_tasks = {}      # (ex_id, symbol) -> asyncio.Task (idempotency guard)
        self._abort_events = {}    # base_coin -> asyncio.Event. Presence => active slicing loop.
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
        Persistent CCXT Pro watch_order_book stream. Wraps each yielded book
        in a BookSnapshot — adds a local-monotonic `received_ts_ms` stamp so
        the slicing loop's `is_fresh()` gate is independent of venue clocks.

        On timeout (300 s of silence) OR any other exception, we POP the cache
        slot. Empirical anchor (ENGINE_FIELD_NOTES.md): a CCXT reconnect
        without re-snapshotting can resume deltas against a stale cached book,
        and the slicing loop would trade on it. Clearing on the exception
        path ensures the loop sees `engine.order_books.get(...) is None` and
        skips cycles until a fresh snapshot lands. The 5-second sleep on
        non-timeout errors also rate-limits reconnect storms.
        """
        exchange = self.exchanges[ex_id]
        delta_count = 0
        log(f"Started L2 stream for {symbol}.", ex_id)
        while True:
            try:
                # 300 s ceiling on a single await guards against silent socket death.
                book = await asyncio.wait_for(
                    exchange.watch_order_book(symbol), timeout=300.0
                )
                delta_count += 1
                self.order_books[(ex_id, symbol)] = BookSnapshot(
                    bids=book.get("bids") or [],
                    asks=book.get("asks") or [],
                    venue_ts_ms=book.get("timestamp"),
                    received_ts_ms=_now_ms(),
                    delta_count=delta_count,
                    sequence=book.get("nonce"),
                )
            except asyncio.TimeoutError:
                # Silent stream — drop the cache slot so any concurrent
                # slicing loop sees no book (and skips its cycle) until a
                # fresh snapshot arrives. The watch loop iterates and
                # re-enters watch_order_book on the next loop turn.
                self.order_books.pop((ex_id, symbol), None)
                log(f"L2 stream silence ≥300 s on {symbol}: cache cleared.", ex_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"L2 stream error for {symbol}: {e}", ex_id)
                # CCXT reconnect may resume on a stale snapshot — null the
                # cache so the slicing loop never trades on a ghost book.
                self.order_books.pop((ex_id, symbol), None)
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
        for every leg referenced in the saved position ledger.

        Reconstructs ExecutionPair objects from live CCXT markets and audits each
        against the multiplier/contract_size persisted at position-open time.
        Live values trump stored ones (stored values are advisory forensics);
        any drift is loud-logged so the operator can reconcile.
        """
        if not self.positions:
            return

        ex_ids = {pos["long"]["exchange"] for pos in self.positions.values()} | {
            pos["short"]["exchange"] for pos in self.positions.values()
        }
        log(f"Pre-warming {len(ex_ids)} exchanges from saved state...", "ENGINE")
        await asyncio.gather(*(self.get_or_create_exchange(ex) for ex in ex_ids))

        sub_tasks = []
        for base_coin, pos in self.positions.items():
            try:
                long_market = self.exchanges[pos["long"]["exchange"]].markets[pos["long"]["symbol"]]
                short_market = self.exchanges[pos["short"]["exchange"]].markets[pos["short"]["symbol"]]
                long_leg = ExecutionLeg.from_market(pos["long"]["exchange"], long_market)
                short_leg = ExecutionLeg.from_market(pos["short"]["exchange"], short_market)
                pair = ExecutionPair(long=long_leg, short=short_leg)
            except (KeyError, ValueError) as e:
                log(
                    f"Pre-warm: cannot rebuild pair for {base_coin}: {e}. Position orphaned.",
                    "PREWARM_ERROR",
                )
                continue

            # Drift detection — venue silently changed contract_size or we stripped a
            # different prefix than at open time. Live values are used; stored values
            # serve as the forensic anchor.
            for tag, leg, saved in (
                ("long", pair.long, pos["long"]),
                ("short", pair.short, pos["short"]),
            ):
                if (leg.multiplier != saved.get("multiplier")
                        or leg.contract_size != saved.get("contract_size")):
                    log(
                        f"Pre-warm DRIFT on {base_coin}/{tag}: stored "
                        f"(multiplier={saved.get('multiplier')}, contract_size={saved.get('contract_size')}) "
                        f"vs live (multiplier={leg.multiplier}, contract_size={leg.contract_size}). "
                        f"Live values used; manual reconciliation recommended.",
                        "PREWARM_WARNING",
                    )

            if pair.key != base_coin:
                log(
                    f"Pre-warm: rebuilt key={pair.key} != stored key={base_coin}. "
                    f"Symbol prefix parsing must have changed. Position orphaned.",
                    "PREWARM_ERROR",
                )
                continue

            sub_tasks.append(self.subscribe_order_book(pair.long.exchange, pair.long.symbol))
            sub_tasks.append(self.subscribe_order_book(pair.short.exchange, pair.short.symbol))

        await asyncio.gather(*sub_tasks)
        log("Pre-warming complete. All systems hot.", "ENGINE")

    # -------- Leg-aware conversion helpers (the only CCXT-precision boundary) --------

    def _to_native_qty(self, leg: ExecutionLeg, base_qty: float) -> float:
        """Translate 1x base qty into exchange-native qty + apply CCXT precision."""
        raw_native = leg.to_native_qty(base_qty)
        return float(self.exchanges[leg.exchange].amount_to_precision(leg.symbol, raw_native))

    def _min_base_for_leg(self, leg: ExecutionLeg) -> float:
        """Min lot size for one leg, expressed in 1x base tokens
        (handles both prefix multiplier and CCXT contract_size)."""
        market = self.exchanges[leg.exchange].markets[leg.symbol]
        raw_min_native = market.get("limits", {}).get("amount", {}).get("min") or 0
        return raw_min_native * leg.multiplier * leg.contract_size

    def _min_notional_for_leg(self, leg: ExecutionLeg) -> float:
        """USDT notional floor for one leg.

        Reads CCXT-published `market.limits.cost.min`; falls back via
        `venue_overrides.min_notional_usdt_for` (per-venue override OR
        global default DEFAULT_MIN_NOTIONAL_USDT) when CCXT exposes None.

        9 of 13 venues return None for cost.min — the survey is in
        ENGINE_FIELD_NOTES.md Table E. The fallback is empirically
        grounded: 2026-05-10 mexc × bitget smoketest crashed at 2-XRP
        slice (~$2.84 notional) with both venues rejecting at 5 USDT,
        even though neither published a value."""
        market = self.exchanges[leg.exchange].markets[leg.symbol]
        ccxt_value = market.get("limits", {}).get("cost", {}).get("min")
        return min_notional_usdt_for(leg.exchange, ccxt_value)

    def _step_base_for_leg(self, leg: ExecutionLeg) -> float:
        """Smallest base-equivalent quantity step the venue accepts.

        Used by the slicing loop's snap-aware floor — the symmetric-snap
        rounds DOWN to a multiple of `max(long_step, short_step)`. If the
        composite dispatch floor isn't already a multiple of that step,
        post-snap can drop us below the floor by up to one step. The
        loop ceils the floor to the next step boundary to compensate.

        Returns native_precision_step × leg.base_per_native. Falls back
        to 0.0 if precision unavailable (caller skips snap-aware rounding)."""
        market = self.exchanges[leg.exchange].markets[leg.symbol]
        native_step = market.get("precision", {}).get("amount") or 0
        return float(native_step) * leg.base_per_native

    def _pair_dust(self, pair: ExecutionPair) -> float:
        """Pair-level lot floor (max of either leg's min lot, in base).

        NOTE: lot-only — does NOT include min-notional or snap-buffer.
        For the full dispatch floor (lot + notional + snap-safe rounding),
        use `execution._compute_dispatch_floor_base` per cycle. This
        method survives for the SLICE START log line and any caller that
        needs the static lot floor specifically."""
        return max(self._min_base_for_leg(pair.long), self._min_base_for_leg(pair.short))

    def _serialize_leg(self, leg: ExecutionLeg) -> dict:
        """Persistence shape for one leg in positions.json."""
        return {
            "exchange": leg.exchange,
            "symbol": leg.symbol,
            "multiplier": leg.multiplier,
            "contract_size": leg.contract_size,
        }

    async def _set_leverage_for_leg(self, leg: ExecutionLeg, leverage: int):
        """Apply venue-specific set_leverage param dicts to one leg.

        Some venues (mexc, bitmart, bingx) require structural params
        CCXT validates client-side. The override map in
        `venue_overrides.set_leverage_params_for` returns a LIST of
        param dicts — each one drives one set_leverage call. Most
        venues are a single empty-dict call; mexc fans out two
        (`positionType=1` then `=2`) to set both directions when in
        hedge mode (no-op in one-way mode).

        Sequential await per call (not gathered) — venues sometimes
        rate-limit close-together calls on the same symbol, and
        leverage setup is one-shot at warmup so the latency is
        immaterial.

        Idempotency-as-error handling: several venues (bybit
        `110043 leverage not modified`, anchor: 2026-05-10 binance ×
        bybit warmup) raise an exception when the leverage is already
        at the requested value. The classifier
        `venue_overrides.is_benign_warmup_error` knows the per-venue
        substring signatures; benign errors are logged and skipped,
        unrecognized exceptions propagate (loud-fail by default).
        New venue signatures are added to
        `venue_overrides.VENUE_BENIGN_WARMUP_ERROR_SIGNATURES` as
        empirically surfaced — engine.py stays free of venue-specific
        string parsing."""
        ex = self.exchanges[leg.exchange]
        for params in set_leverage_params_for(leg.exchange):
            try:
                await ex.set_leverage(leverage, leg.symbol, params=params)
            except Exception as e:
                is_benign, description = is_benign_warmup_error(leg.exchange, e)
                if is_benign:
                    log(
                        f"Leverage already at {leverage}x on {leg.exchange}:{leg.symbol} "
                        f"({leg.exchange}: {description}); proceeding.",
                        "WARMUP",
                    )
                    continue
                # Unrecognized — propagate. handle_warmup catches and
                # surfaces as WARMUP_ERROR + 400 response.
                raise

    # -------- IPC Handlers --------

    async def handle_warmup(self, request):
        """
        Authenticate, load markets, set leverage, subscribe L2 streams.
        Blocks until the first L2 snapshot lands on every leg — /entry must
        not race an empty RAM cache. Logs a per-leg fingerprint so the
        operator can eyeball multiplier/contract_size sanity before firing.

        Payload: {legs: [[ex, sym], ...], leverage}
        """
        data = await request.json()
        # Canonical CCXT ids are lowercase. Normalize at the boundary.
        legs_input = [(ex.lower(), sym) for ex, sym in data["legs"]]
        leverage = data["leverage"]
        try:
            # 1. Instantiate all exchanges concurrently (loads markets, spawns heartbeat)
            ex_ids = list({ex for ex, _ in legs_input})
            await asyncio.gather(*(self.get_or_create_exchange(ex) for ex in ex_ids))

            # 2. Build ExecutionLeg primitives from live CCXT markets
            legs = []
            for ex_id, symbol in legs_input:
                try:
                    market = self.exchanges[ex_id].markets[symbol]
                except KeyError:
                    return web.json_response(
                        {"error": f"Symbol {symbol} not found on {ex_id}. Verify spelling."},
                        status=400,
                    )
                legs.append(ExecutionLeg.from_market(ex_id, market))

            # 3. Set leverage per leg — fan out venue-specific param dicts.
            # `_set_leverage_for_leg` consumes `set_leverage_params_for(venue)`
            # and runs every required call sequentially. Inter-leg parallel
            # via gather; intra-leg sequential inside the helper.
            log(
                f"Setting leverage to {leverage}x on {len(legs)} legs: "
                f"{[(leg.exchange, leg.symbol) for leg in legs]}",
                "WARMUP",
            )
            leverage_results = await asyncio.gather(
                *(self._set_leverage_for_leg(leg, leverage) for leg in legs),
                return_exceptions=True,
            )
            failed = [
                f"{leg.exchange}:{leg.symbol}"
                for leg, res in zip(legs, leverage_results)
                if isinstance(res, Exception)
            ]
            for leg, res in zip(legs, leverage_results):
                if isinstance(res, Exception):
                    log(f"Leverage setup failed for {leg.exchange}:{leg.symbol}: {res}", "WARMUP_ERROR")
            if failed:
                return web.json_response(
                    {"error": f"Warmup failed on {failed}. Check logs."}, status=400
                )

            # 4. Subscribe L2 streams (idempotent — no-op if already streaming)
            await asyncio.gather(
                *(self.subscribe_order_book(leg.exchange, leg.symbol) for leg in legs)
            )

            # 5. Block until first snapshot lands on each leg
            log(f"Awaiting first L2 snapshot on {len(legs)} legs...", "WARMUP")
            await asyncio.gather(
                *(self._await_first_snapshot(leg.exchange, leg.symbol) for leg in legs)
            )

            # 6. Leg fingerprint — human-eyeballable multiplier/contract_size sanity check.
            # If two legs print prices that disagree by orders of magnitude, the
            # operator kills the engine before placing the order. This is the runtime
            # version of the empirical-probing methodology (the warmup IS a probe).
            for leg in legs:
                book = self.order_books[(leg.exchange, leg.symbol)]
                top_of_book_native = (float(book.bids[0][0]) + float(book.asks[0][0])) / 2.0
                log(
                    f"LEG FINGERPRINT {leg.exchange}:{leg.symbol} "
                    f"| base_coin={leg.base_coin} "
                    f"| multiplier={leg.multiplier} contract_size={leg.contract_size} "
                    f"| 1 native_contract = {leg.base_per_native} base_tokens "
                    f"| 1 base_token ≈ ${leg.to_base_price(top_of_book_native):.10f}",
                    "WARMUP",
                )

            log(
                f"Warmup complete. {len(legs)} legs at {leverage}x — books live.",
                "WARMUP",
            )
            return web.json_response(
                {"message": f"Warmup complete. {len(legs)} legs at {leverage}x."}
            )
        except Exception as e:
            log(f"Warmup structural failure: {e}", "WARMUP_ERROR")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_entry(self, request):
        """
        Drives the Synchronized Smart Slicing entry sequence.
        Payload: {long: [ex, sym], short: [ex, sym], base_amount,
                  min_entry_basis_bps, max_duration_s}
        """
        data = await request.json()
        long_ex, long_sym = data["long"]
        short_ex, short_sym = data["short"]
        long_ex = long_ex.lower()
        short_ex = short_ex.lower()
        target_qty_base = data["base_amount"]
        basis_floor_bps = data["min_entry_basis_bps"]
        max_duration_s = data["max_duration_s"]

        if long_ex not in self.exchanges or short_ex not in self.exchanges:
            return web.json_response(
                {"error": "Exchanges not warmed up. Run warmup first."}, status=400
            )

        # Build the pair from live CCXT markets (boundary normalization)
        try:
            long_leg = ExecutionLeg.from_market(long_ex, self.exchanges[long_ex].markets[long_sym])
            short_leg = ExecutionLeg.from_market(short_ex, self.exchanges[short_ex].markets[short_sym])
            pair = ExecutionPair(long=long_leg, short=short_leg)
        except KeyError as e:
            return web.json_response(
                {"error": f"Symbol not found in CCXT markets: {e}. Run warmup first."},
                status=400,
            )
        except ValueError as e:  # base_coin mismatch — different underlyings
            return web.json_response({"error": str(e)}, status=400)

        pos_key = pair.key

        # Existing position locks the routing AND the symbols. An asymmetric variant
        # change (e.g. swapping `1000CHEEMS` for `1MCHEEMS` mid-position) counts as
        # a different routing.
        existing = self.positions.get(pos_key)
        if existing:
            if (existing["long"]["exchange"] != pair.long.exchange
                    or existing["long"]["symbol"] != pair.long.symbol
                    or existing["short"]["exchange"] != pair.short.exchange
                    or existing["short"]["symbol"] != pair.short.symbol):
                return web.json_response(
                    {"error": (
                        f"Active position uses {existing['long']['exchange']}:{existing['long']['symbol']} / "
                        f"{existing['short']['exchange']}:{existing['short']['symbol']}. Cannot mix."
                    )},
                    status=400,
                )

        if (pair.long.exchange, pair.long.symbol) not in self.order_books \
                or (pair.short.exchange, pair.short.symbol) not in self.order_books:
            return web.json_response(
                {"error": "L2 books not live. Run warmup first."}, status=400
            )

        if pos_key in self._abort_events:
            return web.json_response(
                {"error": f"Slicing loop already in flight for {pos_key}. Abort it first."},
                status=409,
            )

        abort_event = asyncio.Event()
        self._abort_events[pos_key] = abort_event
        try:
            result = await run_slicing_loop(
                self,
                pair,
                target_qty_base,
                basis_floor_bps,
                max_duration_s,
                side="entry",
                abort_event=abort_event,
            )
        except Exception as e:
            log(f"Structural failure during entry on {pos_key}: {e}", "CRITICAL")
            asyncio.create_task(
                asyncio.to_thread(
                    send_pushover,
                    f"{e}\nManual intervention required.",
                    f"CRITICAL: ENTRY FAILED on {pos_key}",
                    2,
                )
            )
            return web.json_response({"error": f"Entry failed: {e}"}, status=500)
        finally:
            self._abort_events.pop(pos_key, None)

        # Asymmetric residual halt — slicing loop returned cleanly but
        # detected naked exposure on one venue. Pushover P2 with the
        # exact per-venue exposure so operator can manually reconcile
        # before the position drifts further.
        if result.halt_reason == "asymmetric_residual":
            residual = abs(result.qty_long_base - result.qty_short_base)
            asyncio.create_task(
                asyncio.to_thread(
                    send_pushover,
                    f"Asymmetric residual on {pos_key}: "
                    f"long_filled={result.qty_long_base:.8f} on {pair.long.exchange}, "
                    f"short_filled={result.qty_short_base:.8f} on {pair.short.exchange}, "
                    f"residual={residual:.8f} base. Engine recorded {result.filled_base:.8f} "
                    f"symmetric. Manual reconciliation required.",
                    f"CRITICAL: ASYMMETRIC RESIDUAL on {pos_key} (entry)",
                    2,
                )
            )

        if result.filled_base > 0:
            if pos_key in self.positions:
                # Scale-in: qty-weight-blend entry VWAPs against the cumulative entry_qty_base
                # (NOT the residual amount — exits don't dilute the entry-side average).
                pos = self.positions[pos_key]
                old_entry_qty = pos.get("entry_qty_base", pos["amount_base"])
                new_entry_qty = old_entry_qty + result.filled_base
                pos["entry_vwap_long_base"] = (
                    pos.get("entry_vwap_long_base", 0.0) * old_entry_qty
                    + result.vwap_long_base * result.filled_base
                ) / new_entry_qty
                pos["entry_vwap_short_base"] = (
                    pos.get("entry_vwap_short_base", 0.0) * old_entry_qty
                    + result.vwap_short_base * result.filled_base
                ) / new_entry_qty
                p_ref_base = (pos["entry_vwap_long_base"] + pos["entry_vwap_short_base"]) / 2.0
                pos["entry_basis_bps"] = (
                    (pos["entry_vwap_short_base"] - pos["entry_vwap_long_base"]) / p_ref_base * 10000.0
                    if p_ref_base > 0 else 0.0
                )
                pos["entry_qty_base"] = new_entry_qty
                pos["amount_base"] = new_entry_qty - pos.get("exit_qty_base", 0.0)
                log(
                    f"Scaled in. entry_qty_base={new_entry_qty} amount_base={pos['amount_base']} "
                    f"entry_basis_bps={pos['entry_basis_bps']:.2f} (halt_reason={result.halt_reason})",
                    "ENTRY",
                )
            else:
                self.positions[pos_key] = {
                    "long": self._serialize_leg(pair.long),
                    "short": self._serialize_leg(pair.short),
                    "amount_base": result.filled_base,
                    "entry_qty_base": result.filled_base,
                    "entry_vwap_long_base": result.vwap_long_base,
                    "entry_vwap_short_base": result.vwap_short_base,
                    "entry_basis_bps": result.realized_basis_bps,
                    "exit_qty_base": 0.0,
                    "exit_vwap_long_base": None,
                    "exit_vwap_short_base": None,
                    "exit_basis_bps": None,
                    "opened_at": now_str(),
                }
                log(
                    f"Delta-neutral established for {pos_key} amount_base={result.filled_base} "
                    f"entry_basis_bps={result.realized_basis_bps:.2f} (halt_reason={result.halt_reason})",
                    "ENTRY",
                )
            await asyncio.to_thread(save_state, self.positions)
        else:
            log(
                f"Entry filled 0/{target_qty_base} {pos_key} (halt_reason={result.halt_reason}). No state change.",
                "ENTRY",
            )

        return web.json_response({
            "filled": result.filled_base,
            "target": target_qty_base,
            "halt_reason": result.halt_reason,
            "vwap_long_base": result.vwap_long_base,
            "vwap_short_base": result.vwap_short_base,
            "realized_basis_bps": result.realized_basis_bps,
        })

    async def handle_exit(self, request):
        """
        Drives the Synchronized Smart Slicing exit sequence (reduceOnly IOCs).
        Payload: {pair, base_amount, min_exit_basis_bps, max_duration_s}
        """
        data = await request.json()
        pos_key = data["pair"]
        target_qty_base = data["base_amount"]
        basis_floor_bps = data["min_exit_basis_bps"]
        max_duration_s = data["max_duration_s"]

        pos = self.positions.get(pos_key)
        if not pos:
            return web.json_response({"error": f"No active position for {pos_key}."}, status=400)

        # Reconstruct the pair from saved state — markets must be loaded already
        try:
            long_leg = ExecutionLeg.from_market(
                pos["long"]["exchange"],
                self.exchanges[pos["long"]["exchange"]].markets[pos["long"]["symbol"]],
            )
            short_leg = ExecutionLeg.from_market(
                pos["short"]["exchange"],
                self.exchanges[pos["short"]["exchange"]].markets[pos["short"]["symbol"]],
            )
            pair = ExecutionPair(long=long_leg, short=short_leg)
        except KeyError as e:
            return web.json_response({"error": f"Markets not loaded: {e}"}, status=400)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)

        target_qty_base = min(target_qty_base, pos["amount_base"])

        if pos_key in self._abort_events:
            return web.json_response(
                {"error": f"Slicing loop already in flight for {pos_key}. Abort it first."},
                status=409,
            )

        # Defensive subscribe in case streams dropped between pre_warm and now
        await asyncio.gather(
            self.subscribe_order_book(pair.long.exchange, pair.long.symbol),
            self.subscribe_order_book(pair.short.exchange, pair.short.symbol),
        )
        try:
            await asyncio.gather(
                self._await_first_snapshot(pair.long.exchange, pair.long.symbol, timeout=5.0),
                self._await_first_snapshot(pair.short.exchange, pair.short.symbol, timeout=5.0),
            )
        except TimeoutError as e:
            return web.json_response({"error": f"Books not live: {e}"}, status=400)

        abort_event = asyncio.Event()
        self._abort_events[pos_key] = abort_event
        try:
            result = await run_slicing_loop(
                self,
                pair,
                target_qty_base,
                basis_floor_bps,
                max_duration_s,
                side="exit",
                abort_event=abort_event,
            )
        except Exception as e:
            log(f"Structural failure during exit on {pos_key}: {e}", "CRITICAL")
            asyncio.create_task(
                asyncio.to_thread(
                    send_pushover,
                    f"{e}\nManual intervention required.",
                    f"CRITICAL: EXIT FAILED on {pos_key}",
                    2,
                )
            )
            return web.json_response({"error": f"Exit failed: {e}"}, status=500)
        finally:
            self._abort_events.pop(pos_key, None)

        # Asymmetric residual halt — same Pushover hook as entry.
        # On exit this typically means one leg over-reduced (or under-reduced)
        # vs the other; the operator's net position changed unexpectedly.
        if result.halt_reason == "asymmetric_residual":
            residual = abs(result.qty_long_base - result.qty_short_base)
            asyncio.create_task(
                asyncio.to_thread(
                    send_pushover,
                    f"Asymmetric residual on {pos_key} (exit): "
                    f"long_filled={result.qty_long_base:.8f} on {pair.long.exchange}, "
                    f"short_filled={result.qty_short_base:.8f} on {pair.short.exchange}, "
                    f"residual={residual:.8f} base. Engine recorded {result.filled_base:.8f} "
                    f"symmetric exit. Manual reconciliation required.",
                    f"CRITICAL: ASYMMETRIC RESIDUAL on {pos_key} (exit)",
                    2,
                )
            )

        # Blend exit-side VWAPs against cumulative exit_qty_base (multi-leg unwinds).
        if result.filled_base > 0:
            old_exit_qty = pos.get("exit_qty_base", 0.0)
            new_exit_qty = old_exit_qty + result.filled_base
            old_exit_l = pos.get("exit_vwap_long_base") or 0.0
            old_exit_s = pos.get("exit_vwap_short_base") or 0.0
            pos["exit_vwap_long_base"] = (
                old_exit_l * old_exit_qty + result.vwap_long_base * result.filled_base
            ) / new_exit_qty
            pos["exit_vwap_short_base"] = (
                old_exit_s * old_exit_qty + result.vwap_short_base * result.filled_base
            ) / new_exit_qty
            p_ref_base = (pos["exit_vwap_long_base"] + pos["exit_vwap_short_base"]) / 2.0
            pos["exit_basis_bps"] = (
                (pos["exit_vwap_long_base"] - pos["exit_vwap_short_base"]) / p_ref_base * 10000.0
                if p_ref_base > 0 else 0.0
            )
            pos["exit_qty_base"] = new_exit_qty
            pos["amount_base"] -= result.filled_base

        # Dust check: residual below either leg's min lot size means we cannot
        # legally trade it again — archive the closed trade and clear the ledger.
        dust_base = self._pair_dust(pair)
        if pos["amount_base"] <= dust_base:
            entry_basis = pos.get("entry_basis_bps", 0.0) or 0.0
            exit_basis = pos.get("exit_basis_bps", 0.0) or 0.0
            closed_record = {
                "base_coin": pos_key,
                "long": pos["long"],
                "short": pos["short"],
                "entry_qty_base": pos.get("entry_qty_base", 0.0),
                "exit_qty_base": pos.get("exit_qty_base", 0.0),
                "residual_dust_base": pos["amount_base"],
                "entry_vwap_long_base": pos.get("entry_vwap_long_base", 0.0),
                "entry_vwap_short_base": pos.get("entry_vwap_short_base", 0.0),
                "entry_basis_bps": entry_basis,
                "exit_vwap_long_base": pos.get("exit_vwap_long_base", 0.0),
                "exit_vwap_short_base": pos.get("exit_vwap_short_base", 0.0),
                "exit_basis_bps": exit_basis,
                "round_trip_basis_bps": entry_basis + exit_basis,
                "opened_at": pos.get("opened_at"),
                "closed_at": now_str(),
            }
            await asyncio.to_thread(append_closed_trade, closed_record)
            del self.positions[pos_key]
            log(
                f"Position closed on {pos_key}. round_trip_basis_bps="
                f"{closed_record['round_trip_basis_bps']:.2f}. Archived to closed_trades.json.",
                "EXIT",
            )
        else:
            log(
                f"Exit filled {result.filled_base}/{target_qty_base} {pos_key}, "
                f"residual_base {pos['amount_base']}, "
                f"exit_basis_bps={pos.get('exit_basis_bps', 0.0):.2f} (halt_reason={result.halt_reason}).",
                "EXIT",
            )

        await asyncio.to_thread(save_state, self.positions)

        return web.json_response({
            "filled": result.filled_base,
            "target": target_qty_base,
            "remaining": self.positions.get(pos_key, {}).get("amount_base", 0),
            "halt_reason": result.halt_reason,
            "vwap_long_base": result.vwap_long_base,
            "vwap_short_base": result.vwap_short_base,
            "realized_basis_bps": result.realized_basis_bps,
        })

    async def handle_abort(self, request):
        """
        Signals a graceful halt to an in-flight slicing loop for `pair` (= base_coin).
        The loop will exit at its next cycle boundary — never mid-IOC, so
        delta-neutrality is preserved. Already-filled qty is kept as a hedged
        position via the normal entry/exit completion path.
        Payload: {pair}
        """
        data = await request.json()
        pos_key = data["pair"]
        event = self._abort_events.get(pos_key)
        if event is None:
            return web.json_response(
                {"error": f"No active slicing loop for {pos_key}."}, status=404
            )
        event.set()
        log(f"Abort signaled for {pos_key}. Loop will halt at next cycle boundary.", "ABORT")
        return web.json_response(
            {"message": f"Abort signaled for {pos_key}. Halting at next cycle boundary."}
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
