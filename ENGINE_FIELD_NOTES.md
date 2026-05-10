# Engine Field Notes — execution surface for 12-venue USDT-Linear Perp arb

Empirical truth about the **execution surface** of every venue we trade. Code is the source of truth for what the engine DOES; this document is the source of truth for what each venue DID when probed and what bug classes to anticipate next.

---

## Conventions (read first)

**Timestamping.** Every entry — table row, anchor incident, methodology note — is dated `YYYY-MM-DD`. Append-only at the section level: when a fact is superseded, update in-place but keep the original timestamp visible alongside the new one (e.g., "verified 2026-05-09; re-verified 2026-05-10").

**Status tags.** Each substantive entry carries one tag:

| Tag | Meaning | When to read |
|-----|---------|--------------|
| **ACTIVE** | Still informs current behavior or workflow | Always — this is the live truth surface |
| **PENDING** | Waiting on data, capital, or operator decision | Look here when planning new work |
| **SHIPPED** | Code exists; entry kept as forensic + pattern reference | Skim for context; trust the code over the prose |
| **SUPERSEDED** | Explicit replacement exists (named in the entry) | Read only to understand history |
| **WALKED-AWAY** | Deliberately abandoned (e.g., phemex) | Re-evaluate only if root cause found |

**The mindset — keep trails after the fix.** Anchor incidents and bug patterns stay in the document AFTER the fix lands. The historical archive is a probe-template for similar bugs in future. A bug class surfaced once on one venue is likely to recur on a new venue or a new endpoint. Future-self should arrive armed with awareness — not surprised twice by the same pattern.

When you fix a new bug, write a 4–6 line anchor entry under **ANCHOR INCIDENTS** with: signature (what looked broken) → mechanism (what was actually wrong) → fix (file:function or commit) → lesson (what bug class to anticipate). When you observe a class of bugs that's not yet in **WIDE-AWARENESS ARCHIVE**, add it there.

**Source-of-truth rule.** When this document and the code disagree, the code wins. Run the relevant probe and update this document. The probe outputs (in `probe_logs/`) are the lowest-level forensic artifacts; tables here are summaries.

---

## Project venues — 12 ACTIVE + 1 WALKED-AWAY (2026-05-10)

ACTIVE: `binance`, `bingx`, `bitget`, `bitmart`, `bybit`, `coinex`, `gate`, `htx`, `kucoinfutures`, `mexc`, `okx`, `xt`.

WALKED-AWAY: `phemex` — venue returns `39999 Error in place order` with no diagnostic on every IOC variation we probed (2026-05-09). API-key permissions, IP whitelist, USDT funding all confirmed operator-side. CCXT request body well-formed. Re-add when an operator-side root cause is identified (phemex support ticket).

These are **canonical CCXT class ids** (lowercase) — `kucoinfutures` (not `kucoin`), `gate` (not `gateio`), `htx` (not `huobi`). The `.env` keys use the canonical id directly (`KUCOINFUTURES_API_KEY`, etc.); do not rename for "friendliness" — `config.get_exchange()` does `getattr(ccxtpro, id.lower())`.

---

## Methodology — lean toward real-fill probes (2026-05-10) [ACTIVE]

The Class-2 → Class-3 progression turned out to be the wrong default for venue-receipt verification. Anchor: 2026-05-10 BingX × XT incident — BingX was classified `sync-zero` from a non-filling Class-2 probe; on actually-filling IOCs BingX returned all-None placement (sync-null behavior). Engine trusted the catalog → silent residual. The same pattern reproduced on XT in the same run.

**The property that matters in production** — does fetch_order (or placement) return the correct `filled` value when the order ACTUALLY fills? — was never tested by Class-2 probes.

Lean toward Class-3 `fill_resolution` for new venue work. Class-2 retains value where capital cost would dominate (burst rate-limit tests) but the default for receipt-shape verification is the real-fill probe. The 2026-05-10 sweep characterized 11 of 12 venues in ~3 minutes at ~$1 of fees.

**Defense-in-depth as cultural choice.** The resolver's defensive sync-zero fallback (trust placement IFF `filled` is populated; else fetch_order with retry) is the explicit response to "the catalog WILL be wrong on some venue we haven't probed yet." Catalog data drives the happy path; the fallback handles staleness without silent capital drift. `RESOLVER_WARNING` logs are the feedback loop that keeps the catalog current.

---

## EMPIRICAL TABLES — current source of truth

### Table A — IOC param overrides (2026-05-09, refreshed 2026-05-10)

The engine's `create_order` calls MUST use these param shapes verbatim. Stock CCXT `{timeInForce: 'IOC'}` fails on venues that need a venue-specific override. Single source: `venue_overrides.VENUE_IOC_LIMIT_PARAMS`.

| Venue | Effective IOC params | Status |
|---|---|---|
| `binance` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `bingx` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `bitget` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `bitmart` | `{timeInForce: 'IOC'}` (via `config._VENUE_CREDENTIAL_REMAP['bitmart'] = {'uid': 'PASSWORD'}` for signing) | VERIFIED 2026-05-09 |
| `bybit` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `coinex` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `gate` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `htx` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `kucoinfutures` | `{timeInForce: 'IOC', marginMode: 'cross'}` | VERIFIED 2026-05-09 — `marginMode` required (330005 otherwise) |
| `mexc` | `{timeInForce: 'IOC', type: 3}` | VERIFIED 2026-05-09 — CCXT 4.5.51 swap-create-order does NOT translate IOC; without `type=3`, order rests as regular limit |
| `okx` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `xt` | `{timeInForce: 'IOC'}` | VERIFIED 2026-05-09 |
| `phemex` | — | WALKED-AWAY 2026-05-09 |

### Table B — Receipt R-Mode catalog (2026-05-09, refreshed 2026-05-10)

R-Mode dispatches the resolver's path in `receipt_resolver.resolve_receipt`. Single source: `venue_overrides.VENUE_R_MODE` + `requires_fetch_order(venue)`.

R-Mode legend:
- **sync-zero** — placement has terminal `status` AND `filled = 0`. Engine trusts placement, no fetch_order.
- **sync-null** — `status` AND `filled` both `None` at placement. fetch_order(id) MANDATORY before fill state is read.
- **eventual** — placement has `status='open'` (still resolving). Poll fetch_order until terminal. None of our 12 currently exhibit this on IOCs.

**Defensive override (2026-05-10):** `resolve_receipt` for sync-zero venues trusts placement IFF `filled is not None`. If placement returns all-None, falls back to fetch_order with retry + emits `RESOLVER_WARNING`. This means a misclassified-sync-zero venue (BingX anchor) still resolves correctly; the catalog is a performance optimization, not a correctness dependency.

| Venue | R-Mode | fetch_order req | fetch_order params | Status |
|---|---|---|---|---|
| `binance` | sync-zero | NO | n/a | VERIFIED 2026-05-09 |
| `bingx` | sync-zero | NO (defensive fallback fires on filling IOCs — see Anchor 2026-05-10 BingX) | n/a | VERIFIED 2026-05-09; defensive fallback verified 2026-05-10 |
| `bitget` | sync-null | YES | `{}` | VERIFIED 2026-05-09 |
| `bitmart` | sync-null | YES | `{}` | VERIFIED 2026-05-09 |
| `bybit` | sync-null | YES | `{acknowledged: True}` | VERIFIED 2026-05-09 — only venue requiring non-empty params |
| `coinex` | sync-zero | NO | n/a | VERIFIED 2026-05-09 — auto-purges canceled IOCs from history within ~1s; placement IS authoritative |
| `gate` | sync-zero | NO | n/a | VERIFIED 2026-05-09 |
| `htx` | sync-null | YES | `{}` | VERIFIED 2026-05-09 |
| `kucoinfutures` | sync-null | YES | `{}` | VERIFIED 2026-05-09 — exhibits OrderNotFound eventual consistency, see Table G |
| `mexc` | sync-null | YES | `{}` | VERIFIED 2026-05-09 |
| `okx` | sync-null | YES | `{}` | VERIFIED 2026-05-09 |
| `xt` | sync-null | YES | `{}` | VERIFIED 2026-05-09 — exhibits all-None eventual consistency, see Table G |
| `phemex` | unverified | unverified | — | WALKED-AWAY 2026-05-09 |

**Sync-null is the dominant case** — 8/12 venues. The `FillReceipt` architecture (see *Architecture invariants* below) is a structural necessity, not a Bybit oddity.

### Table C — WS / L2 liveness baselines (2026-05-09)

30 s sample on `BTC/USDT:USDT` from Singapore VPS. Drives `VENUE_MAX_BOOK_AGE_MS` per-venue (default 2000ms; coinex 4000ms). Probe log: `probe_logs/orderbook_liveness_2026-05-09T04-03-41Z.jsonl`.

| Venue | Yields/30s | Median Δ ms | p95 Δ ms | Max Δ ms | Crossed | Status |
|---|---:|---:|---:|---:|---:|---|
| `binance` | 279 | 102 | 112 | 602 | 0 | VERIFIED 2026-05-09 |
| `bingx` | 52 | 500 | 527 | 882 | 0 | VERIFIED 2026-05-09 |
| `bitget` | 252 | 100 | 113 | 610 | 0 | VERIFIED 2026-05-09 |
| `bitmart` | 257 | 40 | 210 | 677 | **3** | VERIFIED 2026-05-09 — crossed-book frames (1.2%); `_gate_book` `is_crossed` check empirically necessary |
| `bybit` | 758 | 21 | 103 | 608 | 0 | VERIFIED 2026-05-09 |
| `coinex` | 42 | 205 | **2984** | **3265** | 0 | VERIFIED 2026-05-09 — 50× outlier; needs 4000ms threshold |
| `gate` | 296 | 100 | 105 | 152 | 0 | VERIFIED 2026-05-09 |
| `htx` | 673 | 32 | 109 | 658 | 0 | VERIFIED 2026-05-09 |
| `kucoinfutures` | **8231** | **2** | **10** | 565 | 0 | VERIFIED 2026-05-09 — 274 yields/sec (broadcasts every internal mutation) |
| `mexc` | 110 | 212 | 250 | 302 | 0 | VERIFIED 2026-05-09 |
| `okx` | 257 | 100 | 199 | 614 | 0 | VERIFIED 2026-05-09 |
| `xt` | 205 | 100 | 202 | 302 | 0 | VERIFIED 2026-05-09 |

`FIRST_SNAPSHOT_TIMEOUT_S = 10.0` is appropriate (worst observed time-to-first-snapshot is 6.75 s on mexc). Re-run `orderbook_liveness` quarterly.

### Table D — set_leverage param overrides (2026-05-10)

Single source: `venue_overrides.VENUE_SET_LEVERAGE_PARAMS`. Stock CCXT `set_leverage(N, sym)` raises `ArgumentsRequired` on 4 venues.

| Venue | Override | Status |
|---|---|---|
| `mexc` | `[{openType: 2, positionType: 1}, {openType: 2, positionType: 2}]` (two-call fan-out) | VERIFIED 2026-05-09 |
| `bitmart` | `[{marginMode: 'cross'}]` | VERIFIED 2026-05-09 |
| `bingx` | `[{side: 'BOTH'}]` (one-way mode; expand to LONG+SHORT in hedge mode) | VERIFIED 2026-05-09 |
| `xt` | `[{positionSide: 'LONG'}, {positionSide: 'SHORT'}]` (no BOTH option) | VERIFIED 2026-05-10 |
| (others) | `[{}]` (stock CCXT works) | VERIFIED 2026-05-10 — surveyed all 12 |

### Table E — Min-notional defaults (2026-05-10)

Single source: `venue_overrides.VENUE_MIN_NOTIONAL_USDT` + `min_notional_usdt_for(venue, ccxt_value)` with `DEFAULT_MIN_NOTIONAL_USDT = 5.0`.

| Venue | CCXT `cost.min` | Engine-effective floor | Status |
|---|---|---|---|
| `binance` | 5.0 (XRP) / 50.0 (BTC) | CCXT-published per-symbol | VERIFIED 2026-05-10 |
| `bingx` | 2.0 | CCXT-published | VERIFIED 2026-05-10 |
| `bitget` | 5.0 | CCXT-published | VERIFIED 2026-05-10 |
| `xt` | 5.0 / 10.0 | CCXT-published | VERIFIED 2026-05-10 |
| `bitmart`, `bybit`, `coinex`, `gate`, `htx`, `kucoinfutures`, `okx` | None | DEFAULT 5.0 | VERIFIED 2026-05-10 |
| `mexc` | None | DEFAULT 5.0 — venue empirically enforces 5 USDT (anchor: 2026-05-10 mexc × bitget rejection) | VERIFIED 2026-05-10 |

### Table F — Benign warmup error signatures (2026-05-10)

Single source: `venue_overrides.VENUE_BENIGN_WARMUP_ERROR_SIGNATURES` + `is_benign_warmup_error(venue, exc)`. Substring matching on stable numeric codes; never JSON-parse exception bodies.

| Venue | Signature substring | Description | Status |
|---|---|---|---|
| `bybit` | `"retCode":110043` | leverage not modified | VERIFIED 2026-05-10 |
| (others) | — populate empirically as encountered | — | PENDING |

### Table G — fetch_order indexing-lag timings (2026-05-10)

From the `fill_resolution` Class-3 sweep across 8 sync-null venues. `_fetch_order_resilient` uses bounded retry per `venue_overrides.VENUE_FETCH_ORDER_INDEXING_LAG_S`.

| Venue | First authoritative fetch_order | Pre-resolution signature | Engine config |
|---|---|---|---|
| `bybit` | +54 ms | (none — first call worked) | DEFAULT 1.0s cushion |
| `okx` | +55 ms | (none) | DEFAULT 1.0s |
| `htx` | +93 ms | (none) | DEFAULT 1.0s |
| `bitget` | +96 ms | (none) | DEFAULT 1.0s |
| `bitmart` | +148 ms | (none) | DEFAULT 1.0s |
| `mexc` | +232 ms | (none) | DEFAULT 1.0s |
| `kucoinfutures` | +786 ms (after 3× OrderNotFound) | `OrderNotFound` raised | EXPLICIT 3.0s |
| `xt` | +817 ms | all-None response (`filled=None, status=None`) | EXPLICIT 3.0s |

Two distinct eventual-consistency signatures observed; both handled in `_fetch_order_resilient` under one bounded-retry loop.

### Table H — fetch_my_trades / fetch_order API quirks (2026-05-10)

From `probe_fee_shape.py` + `probe_venue_quirks.py`. Drives `pnl.py`'s enrichment design (`FETCH_MY_TRADES_LIMIT = 100`, omit `since`, always-enrich).

| Venue | fetch_order fee (fresh) | fetch_my_trades limit cap | `since` honored | order_id field | Notes |
|---|---|---|---|---|---|
| `binance` | EMPTY (None) | ≥500 OK | yes | `t['order']` | fees only via fetch_my_trades |
| `bybit` | populated USDT | ≥500 OK | yes | `t['order']` | needs dedup — `fee == fees[0]` |
| `kucoinfutures` | EMPTY (None) | ≥500 OK | UNRELIABLE (works at 24h, broken at 1h and 7d) | `t['order']` | pnl.py omits since universally |
| `okx` | populated USDT | **CAP 100** | yes | `t['order']` | limit>100 → BadRequest 51000 |
| `mexc` | populated USDT | ≥500 OK | yes | `t['order']` | |
| `bitget` | populated USDT | ≥500 OK | yes | `t['order']` | (also caps fetch_funding_history <500) |
| `bingx` | populated USDT (but engine never calls) | ≥500 OK | yes | `t['order']` | sync-zero path skips fetch_order; receipt-captured stays empty. Also: `t['id'] = None` on every trade — match by `t['order']` |
| `xt` | EMPTY ({None,None}) | **CAP 100** | yes | `t['order']` | dict-shape with None values; limit>100 → max_100 |
| `gate` | EMPTY (None) | ≥500 OK | yes | `t['order']` | |
| `htx` | populated USDT, **NEGATIVE sign + STRING/FLOAT type mismatch** | ≥500 OK | yes | `t['order']` | fetch_my_trades is canonical (positive sign, single type); pnl.py always-enriches |
| `bitmart` | EMPTY on fresh fill | ≥500 OK | yes | `t['order']` | **fees in RECEIVED currency** — USDT on sells, base coin on buys; pnl.py converts via vwap_base |
| `coinex` | populated USDT | ≥500 OK | yes | `t['order']` | |

**Universal facts (12/12):** single-USDT fee per trade in fetch_my_trades (where currency is USDT); `fee == fees[0]` duplication requires dedup; `t['order']` is the universal order_id field.

---

## ARCHITECTURE INVARIANTS — the engine's load-bearing primitives [SHIPPED]

Code is the source of truth. Synopsis here for orientation; trust the files.

### Receipt resolution boundary
Every CCXT receipt flows through `receipt_resolver.resolve_receipt`. Per-venue R-Mode dispatch (sync-zero / sync-null) with **defensive sync-zero fallback** — trust placement IFF `filled is not None`; else `_fetch_order_resilient` with bounded retry. `_fetch_order_resilient` handles BOTH eventual-consistency signatures (OrderNotFound + all-None). `_dedupe_fees` normalizes cost→float before tuple-key dedup (handles htx string/float case). `VENUE_RECEIPT_FILLED_IN_BASE = {"xt"}` flips unit interpretation for venues where CCXT pre-multiplies `filled`. Files: `receipt_resolver.py`, `venue_overrides.py`, `primitives.FillReceipt`.

### BookSnapshot liveness gates
`engine.watch_order_book_loop` writes `BookSnapshot` with `received_ts_ms` on every WS yield; pops the cache slot on any exception (no stale book persists across reconnect). `execution._gate_book` enforces presence + `is_fresh` + `not is_crossed` + `MIN_LEVELS_FOR_SLICE` before any cycle. Per-venue `VENUE_MAX_BOOK_AGE_MS` (default 2000ms; coinex 4000ms). Files: `engine.py`, `execution.py`, `primitives.BookSnapshot`.

### Symmetric dispatch + composite floor
`execution._compute_dispatch_floor_base` composites `max(lot_floor, notional_floor) ceil-to-snap-step` per cycle. `_compute_symmetric_dispatch` iteratively converges to the largest base qty that survives precision-rounding identically on both legs (≤3 iterations for our 12 venues). Recovery dust threshold = the **target leg's** composite floor (NOT pair-max). File: `execution.py`.

### Cycle-invariant halt
After dispatch + recovery, `|cycle_qty_long_base − cycle_qty_short_base|` must be `< min(both legs' min-lot)` or the loop halts with `halt_reason='asymmetric_residual'` and `engine.handle_entry/handle_exit` fires Pushover P2 with the per-venue exposure breakdown. **Replaces the proposed Class-4 watchdogs** — operator declined watchdogs at 2026-05-10 in favor of explicit cycle invariants. File: `execution.run_slicing_loop`.

### True PnL primitive — Phases 1+2+3
- **Phase 1**: engine writes per-order records (`order_id`, `leg`, `kind`, `side`, `venue`, `symbol`, `filled_native`, `filled_base`, `vwap_native`, `vwap_base`, `fees`, `ts`) into `closed_trades.json`. File: `engine.py` + `execution._order_record_from_fill`.
- **Phase 2**: `pnl.py` enriches fees via `fetch_my_trades` per (venue, symbol). **Always-enrich** — receipt-captured fees treated as fallback only after htx negative-sign discovery. Base-coin fees converted to USDT via `vwap_base` (bitmart). File: `pnl.py::enrich_trade_fees`.
- **Phase 3**: `pnl.py` joins funding via `fetch_funding_history` per leg over `[opened_at, closed_at]`. Sign convention assumed CCXT-doc'd (positive = received) but unverified per venue. File: `pnl.py::enrich_trade_funding`.

`true_pnl = price_pnl + funding_pnl − fees_pnl`. Output: table | json | csv. File: `pnl.py`.

---

## PENDING — open empirical questions

### Phase 3 funding sign convention validation [PENDING — operator action]
**Status as of 2026-05-10**: synthetic math validated; zero real funding events in `closed_trades.json` because every smoketest held positions <60s. Validation plan: first real long-hold trade ≥one funding boundary spanned. Inspect via `python3 pnl.py --verbose` — verbose dumps each event to stderr. Expected: long leg of profitable funding-arb shows `amount < 0` (paid), short leg shows `amount > 0` (received). Suspect candidates given htx fetch_order sign-bug history: htx, xt, bitmart specifically.

### Real recovery firing in production [PENDING — production data]
**Status**: code path mock-verified 2026-05-10. Real occurrence requires book thinness or precision asymmetry that produces residual after symmetric snap. All smoketests on deep books showed `recovered=False`. First production recovery: verify `kind='recovery'` record correctly populated + fees deduped + cycle-invariant halt fires on residual.

### htx, bitmart fetch_order fee shape on FRESH order ids [PENDING — observation]
Probe used aged-out order_ids → OrderNotFound. Engine path uses fresh ids so receipt-resolution likely OK; pnl.py always-enrich makes it moot for PnL. Verify on next live trade.

### Position-mode invariant per venue [PENDING — P0 from 2026-05-09]
Does our `.env` set of API keys land us in one-way mode universally, or does any venue default to hedge mode and silently bifurcate positions? BingX prime suspect. Probe: `fetch_position_mode` per venue at engine boot.

### reduceOnly honor on every venue [PENDING — P0 from 2026-05-09]
Does an IOC with `reduceOnly=true` become a no-op when position is already flat, or silently OPEN a counter-position on any venue?

### amount_to_precision rounding direction per venue [PENDING — P1]
TRUNCATE / ROUND / ROUND_UP varies. If a venue uses ROUND_UP and the binary-search ceiling lands at min-lot, rounding up exceeds and the IOC fails.

### Sustained IOC dispatch rate ceilings per venue [PENDING — P2]
Engine's burst is 2 IOCs / 1.5s steady, ~4/s during recovery. Per-venue 429/418 thresholds unknown.

### Position-drift watchdog (R4 in original roadmap) [DEFERRED]
Operator declined Class-4 watchdogs at 2026-05-10 in favor of cycle-invariant halts. Re-evaluate if explicit invariants prove insufficient.

### Warmup probe battery extension (R5) [DEFERRED]
LEG FINGERPRINT line shipped; broader warmup probes (basis sanity, book health baseline) deferred.

---

## ANCHOR INCIDENTS — forensic + pattern-recognition archive

Format: signature → mechanism → fix location → lesson. Kept after fix because each is a probe-template for the analogous bug class.

### 2026-05-07 — Bybit silent-fill on BTC/USDT:USDT IOC [SHIPPED-FIX]
**Signature**: 11 cycles of "filled 0/0" while the venue side actually filled; binance later raised `InsufficientFunds` because long had been silently accumulating. **Mechanism**: bybit IOC placement returns every fill field as `None`; engine read `receipt['filled']` directly without follow-up. **Fix**: `FillReceipt` primitive + `receipt_resolver.resolve_receipt` with mandatory fetch_order on sync-null venues (`primitives.py`, `receipt_resolver.py`, `venue_overrides.VENUE_R_MODE`). **Lesson**: CCXT "unification" is sometimes only structural — the dict shape is consistent but populating it is the venue's responsibility.

### 2026-05-09 — KuCoin Futures OrderNotFound on fresh IOC [SHIPPED-FIX]
**Signature**: `fetch_order(id)` raised `OrderNotFound 100001` 245ms after dispatch on an order that had partially filled (10 XRP visible in venue UI). **Mechanism**: KuCoin's order-by-id endpoint is eventually consistent with the order-placement index (50–500ms typical). **Fix**: `_fetch_order_resilient` Phase 1 — catch `OrderNotFound` and retry with exponential backoff for up to `VENUE_FETCH_ORDER_INDEXING_LAG_S[venue]` seconds (kucoinfutures: 3.0s). **Lesson**: CCXT exception types tell you what the wire returned, not the venue's distributed-systems internal state. A fresh order_id raising OrderNotFound is qualitatively different from a fabricated one.

### 2026-05-09 — Silent 9-XRP residual on KuCoin × OKX (asymmetric dispatch) [SHIPPED-FIX]
**Signature**: `cross_venue_smoketest` reported PASS + position cleared; UI inspection found 9-XRP naked short on OKX. **Mechanism**: KuCoin contract_size=10 native_precision=1 → 1.9 contracts truncated to 1 (10 base); OKX contract_size=100 native_precision=0.01 → 0.19 contracts kept (19 base). Both filled; recovery dust threshold was `max(10, 1) = 10` so 9-XRP imbalance silently dropped. **Fix**: `_compute_symmetric_dispatch` (iterative snap to identical post-precision base on both legs) + recovery dust threshold = target-leg composite (NOT pair-max). **Lesson**: cross-leg invariants must be enforced AT THE DISPATCH SITE; "symmetric" intent in base units becomes asymmetric the moment each leg applies its own precision floor.

### 2026-05-09 — MEXC swap `vwap_by_depth` 3-element level unpack [SHIPPED-FIX]
**Signature**: `too many values to unpack (expected 2)` 1ms after SLICE START. **Mechanism**: MEXC swap WS L2 ships `[price, amount, count]` 3-element levels (CCXT `pro/mexc.py:799`); engine's `for price, size in levels:` raised `ValueError`. **Fix**: index-access in `vwap_by_depth` (`level[0]`, `level[1]`). Tolerates any length ≥ 2 venue-uniformly. **Lesson**: CCXT-Pro's normalization isn't venue-uniform on the swap path. Treat L2 levels as opaque sequences; access by index.

### 2026-05-10 — MEXC × Bitget min-notional rejection on residual slice [SHIPPED-FIX]
**Signature**: 2-XRP slice ($2.84) rejected by both venues with "Cannot be less than the minimum order amount 5 USDT". **Mechanism**: engine's `min_dispatch_base` was lot-only; both venues' `market.limits.cost.min` returns `None`; both empirically enforce 5 USDT. **Fix**: `execution._compute_dispatch_floor_base` (lot + notional + snap-safe step) + `venue_overrides.min_notional_usdt_for` with `DEFAULT_MIN_NOTIONAL_USDT = 5.0`. **Lesson**: CCXT's market-limits unification is incomplete on swap; the engine must carry a fallback default and enforce client-side.

### 2026-05-10 — Bybit `set_leverage` 110043 idempotency-as-error [SHIPPED-FIX]
**Signature**: warmup blocked by `bybit {"retCode":110043,"retMsg":"leverage not modified"}` when the account was already at 1x. **Mechanism**: bybit (and several others — binance margin-mode `-4046`, BingX equivalents) treats redundant config calls as errors. **Fix**: `VENUE_BENIGN_WARMUP_ERROR_SIGNATURES` + `is_benign_warmup_error(venue, exc)` classifier; `_set_leverage_for_leg` catches and consults. Substring match on stable numeric code (not JSON-parse). **Lesson**: venues conflate "operation rejected" with "operation unnecessary." Default-to-fail is the safe direction; explicit allowlist of known-benign signatures.

### 2026-05-10 — XT `set_leverage` requires positionSide fan-out [SHIPPED-FIX]
**Signature**: `xt setLeverage() requires a positionSide argument`. **Mechanism**: XT's API has NO unified-direction option (unlike BingX's "BOTH"); per-direction calls always required. **Fix**: `VENUE_SET_LEVERAGE_PARAMS["xt"] = [{positionSide: 'LONG'}, {positionSide: 'SHORT'}]`. **Lesson**: when a venue surfaces ANY structural-params requirement, proactively survey ALL 12 venues for the same call shape. The survey is cheap and saves the next pair-runs from hitting analogous blocks.

### 2026-05-10 — BingX × XT silent asymmetric accumulation [SHIPPED-FIX]
**Signature**: log line "filled 0/0 (IOCs rejected entirely)" while one leg actually filled. Cycle 2 doubled the asymmetry; recovery hit insufficient_balance. **Mechanism**: TWO bugs compounded — (1) loop continued after recovery underfilled instead of halting; (2) XT's fetch_order returned all-None on actually-filling IOCs (eventual consistency, ~800ms). **Fix**: cycle-invariant halt in `run_slicing_loop` + `_fetch_order_resilient` extended to handle XT's all-None signature + `VENUE_FETCH_ORDER_INDEXING_LAG_S['xt'] = 3.0`. **Lesson**: implicit invariants drift; explicit invariants halt. Same all-None shape that's a "no fill" auto-cancel can also be an "in-flight, indexing-lagged" real fill — distinguish via strict `filled is None and status is None` vs `filled = 0.0, status = 'canceled'`.

### 2026-05-10 — BingX silent-residual: sync-zero misclassification on filling IOCs [SHIPPED-FIX]
**Signature**: recovery on BingX hit "Insufficient margin" because BingX already had the unread fill. **Mechanism**: BingX classified `sync-zero` from a non-filling Class-2 probe; on actually-filling IOCs returns all-None placement (sync-null behavior). Engine trusted the all-None as authoritative. **Fix**: defensive sync-zero fallback in `resolve_receipt` — trust placement IFF `filled is not None`; else fall back to fetch_order with retry. `RESOLVER_WARNING` log on every fallback fire. **Lesson**: a non-filling probe verifies resolution PLUMBING but says nothing about CORRECTNESS on real fills. The R-Mode catalog is now a HINT, not a correctness dependency. Classifications can be wrong; the architecture must absorb wrongness silently.

### 2026-05-10 — XT `receipt['filled']` is in BASE units (CCXT inconsistency) [SHIPPED-FIX]
**Signature**: 10×-over-read of XT fills → phantom 90-XRP imbalance → recovery cascade. **Mechanism**: XT's CCXT class (`xt.py:3469`) is the only venue that pre-multiplies `executedQty × contractSize` in `parse_order` for swap markets. Engine's `leg.to_base_qty(filled)` then multiplied by contract_size AGAIN. **Fix**: `VENUE_RECEIPT_FILLED_IN_BASE = {"xt"}` + `receipt_filled_is_base(venue)` helper; resolver skips the `to_base_qty` multiplication for XT-style venues. **Lesson**: CCXT's "unified" order schema isn't fully unified. The `filled` field's unit semantics differ between XT (base) and the other 11 (native). Verification recipe: compare `receipt['filled']` against dispatched native; if `filled ≈ dispatched × contract_size`, add the venue.

### 2026-05-10 — Bybit `fee` and `fees[0]` duplicate the same payment [SHIPPED-FIX]
**Signature**: pnl.py's first-trade output showed bybit fees 2× the actual payment. **Mechanism**: bybit's CCXT `parseTrade` populates BOTH the singular `fee` dict AND `fees[0]` with IDENTICAL data. Naïve concatenation double-counts. Confirmed UNIVERSAL across 12/12 venues — not bybit-specific. **Fix**: `_dedupe_fees` with full-tuple key (post-float-coercion). **Lesson**: CCXT field redundancy is intentional but undocumented. Always dedup by content, not by which key the data came from.

### 2026-05-10 — binance `fetch_order` has NO fee data [ARCHITECTURAL CHOICE]
**Signature**: binance receipt-captured fees always empty. **Mechanism**: binance's CCXT `parseOrder` doesn't extract fees from EITHER placement OR fetch_order — the futures order endpoint simply doesn't expose them. Fees ONLY surface via `fetch_my_trades`. **"Fix"**: pnl.py's always-enrich pattern — the engine writes whatever fees come naturally from receipt resolution; pnl.py owns interpretation via fetch_my_trades. **Lesson**: receipt-level fee availability is venue×endpoint-specific and arbitrary. Don't make the engine's PnL accuracy depend on it.

### 2026-05-10 — KuCoin `since` parameter is unreliable [SHIPPED-FIX]
**Signature**: `fetch_my_trades(symbol, since=N)` returned 0 trades when trades demonstrably existed within the window. **Mechanism**: KuCoin's CCXT class honors `since` only at certain time depths (works at 24h-back, broken at 1h-back AND 7d-back). Pattern unclear. **Fix**: pnl.py omits `since` universally + client-filters by `opened_at`. Works for all 12 venues. **Lesson**: server-side parameter handling can be selectively buggy. When a parameter's behavior is non-uniform, the safest pattern is "don't use it; filter client-side."

### 2026-05-10 — XT and OKX `fetch_my_trades` cap `limit` at 100 [SHIPPED-FIX]
**Signature**: `xt {"max_100"}` / `okx BadRequest 51000 "Parameter limit error"` on `limit=200`. **Mechanism**: undocumented server-side limit caps. **Fix**: `FETCH_MY_TRADES_LIMIT = 100` universal in pnl.py. **Lesson**: server-side limit caps are usually undocumented. Default to the LOWEST cap discovered across the venue set.

### 2026-05-10 — htx `fetch_order` has THREE bugs in one payload [SHIPPED-FIX]
**Signature**: pnl.py reported phantom +$0.37 profit on a trade that actually lost $0.58. **Mechanism**: (1) NEGATIVE sign convention — htx's `parseOrder` returns fee as flow-to-user (negative = paid); all other 11 venues use cost-to-user. (2) TYPE MISMATCH — same value as string in `fee` and float in `fees[0]`; tuple-key dedup treats them as different → 2× counting. (3) Receipt vs trade-ledger DRIFT — `fetch_order` says `−0.128`, `fetch_my_trades` says `+0.128` for the SAME fill. **Fix**: `_dedupe_fees` coerces cost to float before key building; pnl.py always-enriches via fetch_my_trades (canonical). **Lesson**: a single venue can ship multiple independent bugs in one payload. Defense-in-depth: dedup by content, fetch_my_trades is canonical, type-normalize before any structural compare.

### 2026-05-10 — bitmart fees in RECEIVED currency [SHIPPED-FIX]
**Signature**: bitmart trades' `fee` field had `currency='XRP'` on buy side instead of USDT. **Mechanism**: bitmart charges fee in the currency the user RECEIVES — USDT on sells (you receive USDT), base coin on buys (you receive XRP). Other 11 venues always charge in USDT. **Fix**: `compute_pnl` converts base-coin fees to USDT via the order_record's `vwap_base`. **Lesson**: fee currency assumptions are venue-specific. The "USDT-linear perp = USDT fees" intuition is wrong for at least one venue; expect more.

---

## WIDE-AWARENESS ARCHIVE — bug class taxonomy [the mindset]

Patterns that have surfaced across our 12-venue probing. **When adding a new venue, specifically test for analogues of these.** When a new pattern surfaces, append it here.

### CCXT-level inconsistencies
- "Unified" schemas document INTENT, not actual venue behavior — verify each field empirically
- Same field can carry different units across venues (xt `filled` in BASE vs all others in NATIVE)
- Same field can carry different types in one payload (htx `cost` as string + float)
- Sign conventions can invert per endpoint within one venue (htx fetch_order: paid; htx fetch_my_trades: cost)
- Receipt parsers may not extract all venue-side fields (binance fees absent in `parseOrder`)
- CCXT may pre-multiply or post-divide values inconsistently across venues (xt swap parse_order)

### Eventual consistency
- `fetch_order` can return all-None on a real fill (xt: 800ms typical)
- `fetch_order` can raise `OrderNotFound` on a real order (kucoinfutures: 245ms typical)
- Both signatures handled in `_fetch_order_resilient` with bounded retry per venue

### Server-side undocumented constraints
- Limit caps lower than expected (xt 100, okx 100, bitget <500 on `fetch_funding_history`)
- Min-notional unpublished but enforced (mexc `cost.min = None`, rejects $2.84)
- `since` parameter unreliable across time depths (kucoinfutures: works at 24h, broken at 1h and 7d)

### Configuration call quirks
- `set_leverage` requires venue-specific structural params (mexc, bitmart, bingx, xt)
- Idempotency-as-error: redundant calls raise (bybit 110043, binance margin-mode -4046, etc.)
- Hedge-mode vs one-way mode bifurcation (BingX defaults to hedge; OKX optional)

### Cross-leg invariants that don't hold automatically
- Asymmetric snap silently produces residual when leg precisions differ (kucoin × okx)
- Recovery dust threshold = target leg's composite (NOT pair-max)
- Cycle-invariant halt must be EXPLICIT (asymmetric_residual halt_reason)

### Currency / unit conventions
- Fee currency varies by side on some venues (bitmart base-coin on buys)
- Pre-multiplied filled in some CCXT classes (xt for swap)
- L2 book level shapes differ (mexc 3-element `[price, amount, count]`, others 2-element)

### Receipt vs trade-ledger
- The same venue may disagree with itself across endpoints (htx fetch_order vs fetch_my_trades on fee sign)
- Treat fetch_my_trades as canonical for fee data; treat fetch_order receipt as performance hint only

### Catalog-vs-reality
- A non-filling probe verifies the resolution PATH but says nothing about CORRECTNESS on real fills (bingx sync-zero misclassification anchor)
- Lean toward Class-3 real-fill probes for venue verification
- Any catalog (R-Mode, fee shape, etc.) must have a defensive fallback when the catalog is wrong

---

## PROBES — catalog and cadence

### Class 1 — Introspection (zero side effect)
`engine_probes.py`: `capabilities`, `markets_audit`, `orderbook_liveness`, `clock_skew`, `precision_rounding_audit`. Free.

### Class 2 — Safe-by-construction (controlled side effect, no fills)
`engine_probes.py`: `ioc_honor`, `receipt_shape`, `reduceonly_zero`, `set_leverage_idempotent`, `cancel_phantom`. Real orders that auto-cancel; price floor ≥50% off touch.

⚠️ **`receipt_shape` is SUPERSEDED for receipt R-Mode classification.** Verifies plumbing but not correctness on real fills (anchor: 2026-05-10 BingX). Use Class-3 `fill_resolution` instead. Other Class-2 probes (`ioc_honor`, `cancel_phantom`, etc.) remain useful where capital cost would dominate.

### Class 3 — Capital-at-risk (real fills)
`engine_probes.py`: `min_lot_live`, `fill_resolution`, `cross_venue_smoketest`. Authorization: `--I-AM-FUNDED-AND-AUTHORIZED-FOR-{VENUE}` per run.

### Class 4 — Continuous watchdogs
⚠️ **SUPERSEDED by cycle-invariant halts.** Operator declined Class-4 watchdogs at 2026-05-10 in favor of explicit invariants in the slicing loop. Re-evaluate if needed.

### Read-only characterization probes (added 2026-05-10)
- `probe_fee_shape.py` — fetch_my_trades fee shape across 12 venues
- `probe_venue_quirks.py` — fetch_order fee, fetch_my_trades limit caps, since param
- `probe_funding_shape.py` — fetch_funding_history capability + entry shape

### Re-run cadence

| Cadence | Probes |
|---|---|
| Per CCXT bump | `capabilities`, `ioc_honor`, `receipt_shape`, `probe_fee_shape`, `probe_venue_quirks`, `probe_funding_shape` |
| Quarterly | `orderbook_liveness`, `set_leverage_idempotent` |
| Pre-deploy a new pair | `cross_venue_smoketest` |
| First trade with new venue | `fill_resolution` (verify R-Mode on real fill) |

Probe outputs land in `probe_logs/` (gitignored). Tables here are summaries of those JSONL artifacts; when re-running a probe surfaces a new value that contradicts a row above, update the row in-place AND keep both timestamps visible.

---

## Operational notes — scoped to the engine surface

**Singapore VPS required.** Every venue 451s/`ExchangeNotAvailable`s on non-Asian residential IPs. Engine production lives at `/opt/Cyborg-Arbitrageur/` on the Singapore DigitalOcean droplet (parallels Scanner at `/opt/Arb-Scanalytics/`).

**`enableRateLimit=False`** by design. Engine does not throttle; we let exchange matching engines reject overruns. See PENDING for sustained-rate ceilings.

**`fetch_time` heartbeat** every 30s in `engine.py` keeps the REST socket pool warm.

**Crash recovery.** `positions.json` is the engine's only durable state. `pre_warm()` reconstructs `ExecutionPair` from saved leg specs + live CCXT markets, with drift detection on `multiplier`/`contract_size` (loud-logs `PREWARM_WARNING`, continues with live values).

**Sister project.** `/opt/Arb-Scanalytics/` holds the funding-rate scanner and its own `FIELD_NOTES.md` — different scope (reading the market) but useful cross-reference for funding semantics (last-settled vs upcoming vs forward forecast).

---

*End of file. Update on every probe run that contradicts a row above. Append new anchor incidents under their dated heading; add new bug-class patterns to the WIDE-AWARENESS ARCHIVE.*
