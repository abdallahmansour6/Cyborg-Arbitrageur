# Cyborg Arbitrageur

Discretionary cross-exchange perp-perp funding-rate arbitrage. Three processes:

```
[cli.py]  ──HTTP POST──▶  [engine.py daemon]  ──CCXT Pro──▶  [exchanges]
   stateless                  always-on                       WebSocket + REST
   one-shot                   RAM caches + position ledger             │
                                       │                               │
                                       ▼                               │
                              [closed_trades.json] ◀──reads/mutates──── │
                                                          ▲            │
                                                          │            │
                                                          └─ [pnl.py] ─┘
                                                             on-demand analyzer
```

- **Engine** — pre-loads markets, holds WebSocket level-2 streams, runs the Synchronized Smart Slicing loop, captures per-order facts into `closed_trades.json` on dust-clear.
- **CLI** — stateless one-shot. Each invocation dispatches one local HTTP call and prints the acknowledgment.
- **PnL analyzer** — off-engine. Enriches fees via `fetch_my_trades`, joins funding via `fetch_funding_history`, computes `true_pnl = price + funding − fees`. Engine critical path is never burdened.

---

## Setup

```bash
pip install "ccxt[pro]" aiohttp python-dotenv requests
```

Create `.env` in project root. For each exchange you trade:

```
{EXCHANGE_ID}_API_KEY=...
{EXCHANGE_ID}_SECRET=...
{EXCHANGE_ID}_PASSWORD=...        # only for OKX, KuCoin, Bitget, etc.
PUSHOVER_TOKEN=...
PUSHOVER_USER=...
```

`{EXCHANGE_ID}` is the **canonical CCXT id, uppercased**: `BINANCE_API_KEY`, `KUCOINFUTURES_PASSWORD`, `GATE_SECRET`, etc. Don't rename to "friendlier" names — `config.get_exchange()` matches on the canonical id directly.

12 verified venues: binance, bingx, bitget, bitmart, bybit, coinex, gate, htx, kucoinfutures, mexc, okx, xt. Phemex walked away (see `ENGINE_FIELD_NOTES.md`).

---

## Daily ops

Standard sequence: **boot → warmup → entry → (monitor) → exit → pnl**.

### Boot

```bash
python engine.py
```

Binds `127.0.0.1:8080`. Runs `pre_warm()` (re-subscribes streams for every leg in `positions.json`). Keep the terminal visible — slice-by-slice telemetry prints here.

### Warmup (once per pair, before first entry)

```bash
# Symmetric
python cli.py warmup --legs binance:XRP/USDT:USDT bybit:XRP/USDT:USDT --leverage 1

# Asymmetric (multiplier-prefix divergence)
python cli.py warmup --legs binance:1000CHEEMS/USDT:USDT bybit:1000000CHEEMS/USDT:USDT --leverage 1
```

`--leverage` capped 1x–2x by policy (natural liquidation buffer). Idempotent — safe to re-run.

**Eyeball the LEG FINGERPRINT lines** before firing entry. The two `1 base_token ≈ $X` numbers must agree to single-bp tolerance — if they disagree by orders of magnitude, prefix parsing went wrong or the venue rotated `contract_size`. Kill the engine and reconcile.

```
[WARMUP] LEG FINGERPRINT binance:1000CHEEMS/USDT:USDT  | base_coin=CHEEMS | multiplier=1000     contract_size=1.0 | 1 base_token ≈ $0.0000063
[WARMUP] LEG FINGERPRINT bybit:1000000CHEEMS/USDT:USDT | base_coin=CHEEMS | multiplier=1000000  contract_size=1.0 | 1 base_token ≈ $0.0000063
```

### Entry (Sip)

```bash
python cli.py entry --long binance:XRP/USDT:USDT --short bybit:XRP/USDT:USDT --base-amount 26300 --min-entry-basis-bps -25 --max-duration-s 45
```

| Argument | Meaning |
|---|---|
| `--long` / `--short` | `exchange:symbol`. Long = cheaper-funding venue; short = expensive-funding venue. |
| `--base-amount` | Target quantity in **true 1× base tokens** of the underlying. Not contracts, not prefix-units. |
| `--min-entry-basis-bps` | Net basis floor (per-1×-base price units). Slices below this floor don't dispatch. **Live discretionary knob** — fire tight, observe, retune. |
| `--max-duration-s` | Hard wall-clock deadline. Partial fills stay as a perfectly hedged position; deadline never forces slippage. |

**Net entry basis** = `(VWAP_short_bid_base − VWAP_long_ask_base) / P_mid_base`. Negative = pay inverse basis to enter; positive = require favorable inter-exchange premium.

**Position keying** — by **base coin** (multiplier-stripped). Re-running entry requires `--long` and `--short` (both exchange AND symbol) to match the stored routing exactly. Mismatched legs return `400`.

### Monitor / abort

Watch the engine console for `[SLICE]` lines reporting per-cycle realized basis + cumulative progress. If fill velocity is too slow or the captured edge is tighter than required:

```bash
python cli.py abort --pair CHEEMS
```

Halts at the **next cycle boundary** — never mid-IOC. Accumulated fills stay perfectly hedged. Re-fire entry with retuned basis floor for the remaining quantity.

### Exit (unwind)

```bash
python cli.py exit --pair CHEEMS --base-amount 1000000 --min-exit-basis-bps -8 --max-duration-s 60
```

Position is identified by `--pair` (the base coin); leg specs are looked up from saved state. `--base-amount` is auto-clamped to held amount.

**Net exit basis** = `(VWAP_long_bid_base − VWAP_short_ask_base) / P_mid_base`.

### PnL

After the trade closes (residual ≤ dust → archived to `closed_trades.json`):

```bash
# Default: enrich fees + funding, persist, render table
python3 pnl.py

# Filter by venue pair / coin / date
python3 pnl.py --long binance --short bybit --coin XRP --since 2026-05-01

# Other formats
python3 pnl.py --format json
python3 pnl.py --format csv > pnl.csv

# Skip enrichment (use captured-only)  / skip file mutation
python3 pnl.py --no-enrich --no-funding --no-write

# Verbose — dumps each fetched event to stderr (useful for first
# real funding event to validate sign convention per venue)
python3 pnl.py --verbose
```

Output:

```
closed_at           | pair          | coin | quantity   | notional | rt_bps | price_pnl  | fees      | funding   | true_pnl
2026-05-10 07:46:20 | binance:bybit | XRP  | 19.0000    |  26.9800 |  -1.41 | -0.003800  | 0.056658  | +0.000000 | -0.060458
TOTAL               |               |      |            |          |        | -0.123930  | 0.714931  | +0.000000 | -0.838861
```

`true_pnl = price + funding − fees`. Run within ~24h of trade close — KuCoin's `fetch_my_trades` retention is short. Mutates `closed_trades.json` atomically (write-temp-rename); subsequent runs are idempotent. Race-safe against engine appends.

---

## Halt reasons

Every slicing loop returns one of:

| `halt_reason` | Meaning |
|---|---|
| `target` | Full target `--base-amount` filled. |
| `deadline` | `--max-duration-s` expired. Partial position kept hedged. |
| `aborted` | Operator issued `abort`. |
| `dust` | Remaining quantity below the per-cycle composite floor (lot + notional + snap-safe step) — untradeable. |
| `asymmetric_residual` | Post-recovery cycle ended with `\|cycle_qty_long − cycle_qty_short\|` ≥ smaller-leg minimum-lot. Engine halts to prevent compounding naked exposure. Per-leg quantities reported; manual reconciliation required. **Fires Pushover priority-2 alert.** |

A `target` or `dust` halt on `exit` clears the ledger entry. Otherwise the residual is preserved.

---

## Failure modes & alerts

**Pushover priority 2** (retry/expire bypasses DND):

- **IOC dispatch failure** — either leg's `create_order` raised. Loop halts; one leg may have filled — manual reconcile.
- **Recovery dispatch failure** — uncapped market order on the lagging leg failed. Delta-neutrality not guaranteed; immediate manual intervention.
- **Asymmetric residual halt** — cycle-invariant check detected tradeable imbalance. Per-venue exposure in alert payload.

**Pre-flight 4xx errors do NOT alert** (operator mistakes, not system failures):

| Error | Cause |
|---|---|
| `400 Exchanges not warmed up` | Run `warmup` first. |
| `400 Symbol not found in CCXT markets` | Typo in `--long`/`--short` or symbol not loaded — re-warmup. |
| `400 Pair base mismatch` | Different `base_coin` after prefix-stripping. |
| `400 L2 books not live` | Streams haven't received first snapshot — wait or re-warmup. |
| `400 Active position uses X/Y` | Scale-in routing mismatch. Use original legs or fully exit first. |
| `400 No active position` | `exit` on a `--pair` with no ledger entry. |
| `404 No active slicing loop` | `abort` with nothing in flight. |
| `409 Slicing loop already in flight` | `entry` or `exit` while a loop is running. Abort first. |

---

## Crash recovery

Engine writes `positions.json` after every successful entry/exit. On boot, `pre_warm()`:

1. Reads ledger (keyed by base coin).
2. Re-instantiates exchanges, reconstructs `ExecutionLeg`/`ExecutionPair` from live CCXT markets.
3. **Drift-detects** stored `multiplier`/`contract_size` against live values. Live wins; mismatch → `PREWARM_WARNING`.
4. Re-subscribes L2 streams.
5. Logs `Pre-warming complete. All systems hot.`

After restart you can immediately call `entry` (scale in) or `exit` on any pre-existing position with no warmup — streams are already live. **Warmup is only required for pairs not already in the ledger.**

---

## State schemas

### `positions.json` — open-position ledger

Keyed by **base coin** (multiplier-stripped). All quantities in 1× base tokens; all VWAPs per-1×-base.

```json
{
  "CHEEMS": {
    "long":  {"exchange": "binance", "symbol": "1000CHEEMS/USDT:USDT", "multiplier": 1000, "contract_size": 1.0},
    "short": {"exchange": "bybit", "symbol": "1000000CHEEMS/USDT:USDT", "multiplier": 1000000, "contract_size": 1.0},
    "amount_base": 700000.0,        // currently open (entry_qty_base − exit_qty_base)
    "entry_qty_base": 1000000.0, "entry_vwap_long_base":  6.3e-06, "entry_vwap_short_base": 6.4e-06, "entry_basis_bps": 15.87,
    "exit_qty_base":  300000.0,  "exit_vwap_long_base":   6.4e-06, "exit_vwap_short_base":  6.3e-06, "exit_basis_bps":  -15.87,
    "opened_at": "2026-05-07 14:32:11.456",
    "entry_order_records": [ /* per-IOC + per-recovery records, accumulated across handle_entry calls */ ],
    "exit_order_records":  [ /* same shape, for handle_exit calls */ ]
  }
}
```

Scale-ins blend `entry_*_base` quantity-weighted; partial exits blend `exit_*_base`. Ledger entry is deleted on dust-clear.

### `closed_trades.json` — append-only round-trip archive

```json
[{
  "base_coin": "CHEEMS",
  "long": {...}, "short": {...},
  "entry_qty_base": 1000000.0, "exit_qty_base": 999500.0, "residual_dust_base": 500.0,
  "entry_vwap_long_base":  6.3e-06, "entry_vwap_short_base": 6.4e-06, "entry_basis_bps": 15.87,
  "exit_vwap_long_base":   6.4e-06, "exit_vwap_short_base":  6.3e-06, "exit_basis_bps":  -15.87,
  "round_trip_basis_bps": 0.0,
  "opened_at": "...", "closed_at": "...",

  // Per-order forensic detail (engine-written)
  "entry_order_records": [
    {"order_id": "...", "leg": "long",  "kind": "ioc",      "side": "buy",
     "venue": "binance", "symbol": "1000CHEEMS/USDT:USDT",
     "filled_native": 500.0, "filled_base": 500000.0,
     "vwap_native": 0.0063, "vwap_base": 6.3e-06,
     "fees": [{"cost": 0.158, "currency": "USDT"}],
     "ts": "2026-05-07 14:32:11.523"}
    /* ... and {kind: "recovery"} records when recovery fired ... */
  ],
  "exit_order_records": [ /* same shape, side reversed */ ],

  // Funding history per leg (pnl.py-mutated)
  "funding_history": {
    "long":  {"events": [...], "total_usdt": -0.213, "non_usdt_events": []},
    "short": {"events": [...], "total_usdt":  0.510, "non_usdt_events": []}
  }
}]
```

`round_trip_basis_bps = entry_basis_bps + exit_basis_bps` — the spread component of PnL across the round-trip (both terms are profit-positive by construction). Per-order records and funding events feed `pnl.py`.

---

## File layout

### Engine runtime

| File | Role |
|---|---|
| `engine.py` | Always-on async daemon. |
| `cli.py` | One-shot command-line client (talks to the engine over local HTTP on port 8080). |
| `execution.py` | Slicing logic (project, dispatch, recover, asymmetric-residual halt). |
| `primitives.py` | `ExecutionLeg`, `ExecutionPair`, `FillReceipt`, `BookSnapshot`. |
| `receipt_resolver.py` | Per-venue receipt resolution (R-Mode catalog, fetch_order resilience, fee dedup). |
| `venue_overrides.py` | Single source of truth for per-venue quirks. |
| `config.py` | CCXT Pro instance factory (env-driven creds). |
| `utils.py` | Logging + JSON state I/O. |
| `notifier.py` | Pushover P2 alerts. |
| `ccxt_patches.py` | Monkey-patches for upstream CCXT bugs. |

### Off-engine

| File | Role |
|---|---|
| `pnl.py` | Realized PnL analyzer. |

### State (auto-written)

| File | Written by | Notes |
|---|---|---|
| `positions.json` | engine | Open positions + per-order records. Crash-recovery surface. |
| `closed_trades.json` | engine (append) + pnl.py (mutate) | Round-trip archive + funding enrichment. |
| `transaction.log` | engine | Append-only timestamped event log. |
| `probe_logs/` | probes | Structured JSONL forensic outputs. |

### Reference

- `ENGINE_FIELD_NOTES.md` — empirical truth surface (per-venue quirks, anchor incidents, bug class taxonomy).
- `../Arb-Scanalytics/FIELD_NOTES.md` — sister project's per-venue funding-rate field maps.

---

## Multipliers & asymmetric symbols

Same coin, different per-contract multipliers across venues. CHEEMS appears as `CHEEMS` (1×) on COINEX/GATE, `1000CHEEMS` on BINANCE, `1MCHEEMS` on BITGET, `1000000CHEEMS` on BYBIT — all the same coin. Two scaling factors collapsed into one:

- Symbol prefix multiplier — encoded in `market.base` (`1000CHEEMS` → 1000)
- CCXT `contract_size` — `market.contractSize`

Effective conversion: `1 native contract = (multiplier × contract_size) base tokens`.

**Operator's contract:** `--base-amount` is always 1× of the underlying. Engine derives per-leg native quantity at the wire boundary. Single primitive (`primitives.ExecutionLeg`) owns the math; no inline multiplier arithmetic anywhere else.

**Probe before deploying** any new asymmetric pair — the `LEG FINGERPRINT` line at warmup is the runtime probe; both legs must agree on `1 base ≈ $X` to single-bp tolerance.

---

## Telemetry — reading the slicing loop

Per-cycle output:

```
[BOOK]  LONG: bid=… ask=… (spread=…bps)  SHORT: bid=… ask=… (spread=…bps)  raw_basis=…bps
[SLICE] IOC slice dispatch_base=… safe_ceiling_base=… (haircut=×N) limits long_native=… short_native=… projected_basis=…bps
[RECOVERY] {side} {quantity} on {venue} (filled_base=…, delta_base=…)        ← only if recovery fires
[SLICE] filled long={quantity}@{volume-weighted-avg-price} short={quantity}@{volume-weighted-avg-price} realized_basis=…bps recovered={bool} cumulative=…/…
```

- `raw_basis` — basis at top-of-book (no depth walk).
- `projected_basis` — basis at `safe_ceiling_base` (binary-search ceiling, post-walk). Close to `raw_basis` = firing against top-of-book spread; much lower = walked deeper into the book.
- `dispatch_base` — post-haircut size actually sent. `(haircut=×N)` shows effective ratio: usually `DEPTH_DISCOUNT` (0.5), reverts to `1.00` when discounting would drop below dust (residuals / tiny sizes).
- `realized_basis` — post-recovery combined basis (what you actually captured).

**Tuning DEPTH_DISCOUNT** — compare `dispatch_base` vs the `filled` quantity. Consistently filling near `dispatch_base` → haircut conservative, could be raised. Consistently below → phantom liquidity is real, the discount is earning its keep.

Loop end: `[SLICE] Slicing loop END filled=…/… halt_reason=…`.

---

## Probes

Three classes; full taxonomy + per-class details in `ENGINE_FIELD_NOTES.md`.

### Read-only characterization

```bash
python3 probe_fee_shape.py        # fetch_my_trades fee shape across 12 venues
python3 probe_venue_quirks.py     # fetch_order, limit caps, since-param
python3 probe_funding_shape.py    # fetch_funding_history capability + shape
```

Free, re-runnable. Outputs to `probe_logs/`. Run on CCXT bumps or new-venue addition.

### Operational probes (engine_probes.py)

```bash
# Class 1 — introspection (zero side effect)
python3 engine_probes.py capabilities
python3 engine_probes.py orderbook_liveness BTC/USDT:USDT

# Class 2 — non-filling IOCs (no fills, consumes rate-limit budget)
python3 engine_probes.py ioc_honor BTC/USDT:USDT
# ⚠️ receipt_shape SUPERSEDED for venue verification — use fill_resolution

# Class 3 — capital at risk (real fills)
python3 engine_probes.py fill_resolution --venue=kucoinfutures --I-AM-FUNDED-AND-AUTHORIZED-FOR-KUCOINFUTURES

python3 engine_probes.py cross_venue_smoketest --long_spec=binance:XRP/USDT:USDT --short_spec=bybit:XRP/USDT:USDT --I-AM-FUNDED-AND-AUTHORIZED-FOR-BINANCE --I-AM-FUNDED-AND-AUTHORIZED-FOR-BYBIT
```

The `--I-AM-FUNDED-AND-AUTHORIZED-FOR-{VENUE}` handshake is deliberate friction — Class 3 probes refuse to run without it.

### Class 4 — Watchdogs

⚠️ Empty by design. Operator opted out — explicit cycle-invariants in the slicing loop are the safety net (asymmetric_residual halt, etc.).

---

## Where to look for what

| Want | Look at |
|---|---|
| Live slice-by-slice telemetry | Engine console (stdout) |
| Per-leg multiplier/contract_size sanity (warmup) | `[WARMUP] LEG FINGERPRINT` lines |
| Historical fills, errors, halt reasons | `transaction.log` |
| Open positions + entry/exit VWAPs + per-order records | `positions.json` |
| Closed round-trips with order records + funding | `closed_trades.json` |
| Realized PnL view (price + funding − fees) | `python3 pnl.py` |
| Final fill summary + halt reason | CLI stdout |
| Per-venue execution quirks + anchor incidents | `ENGINE_FIELD_NOTES.md` |
| Per-venue funding-rate field maps | `../Arb-Scanalytics/FIELD_NOTES.md` |
| Probe outputs (forensic JSON) | `probe_logs/` |

CLI acknowledgment on completion: `filled_base=X/Y | halt_reason=Z | realized_basis_bps=…` (plus `remaining_base=R` for exits). Non-zero exit on engine errors or connection failures — safe to chain in scripts.

---

## Operational notes

- **One slicing loop per base coin.** Concurrent `entry`/`exit` on the same `--pair` returns 409. Abort first.
- **USDT-margined perps only.** Inverse/coin-margined introduce non-linear payoff drift (Quanto risk).
- **`--base-amount` is always 1× of the underlying.** Engine handles per-leg conversions.
- **Abort is atomic at cycle boundaries.** Dispatch + recovery is never interrupted mid-flight; abort always leaves a perfectly hedged position.
- **`enableRateLimit=False`** by design — exchange matching engines reject overruns; we don't self-throttle.
- **Recovery bypasses basis gating.** When asymmetric fill occurs, neutrality dominates marginal cost on a fractional remainder.
- **Cycle-invariant halt is the safety net.** After dispatch + recovery, both legs must be symmetric within the smaller leg's min-lot tolerance, or the loop halts (`asymmetric_residual` + Pushover P2).
- **Receipt-captured fees are NOT trusted by pnl.py.** Engine writes whatever fees come naturally; pnl.py always re-fetches via `fetch_my_trades`. Per-venue receipt fee shapes vary unpredictably (htx negative sign, bitmart base-coin currency, xt None values, etc.). Engine stays fast; pnl.py owns interpretation.
- **Composite per-cycle floor.** `max(both legs' min-lot, max(both legs' min-notional)/mid)` ceil-rounded to the next snap step. Used by halt-on-dust threshold AND `min_dispatch_base` AND recovery's target-leg dust check.
- **Symmetric snap before dispatch.** Each cycle's slice is snapped to the largest base quantity that survives precision-rounding identically on BOTH legs. Avoids asymmetric fills from divergent leg precisions (for example, KuCoin contract_size=10 versus OKX contract_size=100).
- **Run pnl.py within ~24h of trade close.** KuCoin's `fetch_my_trades` retention is short. Mutated `closed_trades.json` is then idempotent for subsequent runs.
- **Singapore VPS for production.** Every venue 451s/`ExchangeNotAvailable`s on non-Asian residential IPs. Dev from non-Asia needs NordVPN-Singapore.
