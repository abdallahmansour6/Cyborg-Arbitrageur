# Cyborg Arbitrageur

Discretionary cross-exchange perp-perp funding-rate arbitrage system. Three processes — a long-running async **Engine** that owns all execution state, a stateless **CLI** that pipes operator-driven routing signals to it over local HTTP, and an off-engine **pnl.py** analyzer that interprets closed trades into True PnL (price + funding − fees).

```
[cli.py]  ──HTTP POST──▶  [engine.py daemon]  ──CCXT Pro──▶  [exchanges]
   stateless                  always-on, stateful                  WS + REST
   (one-shot)                 RAM caches + position ledger              │
                                       │                                │
                                       ▼                                │
                              [closed_trades.json] ◀──reads/mutates──── │
                                       ▲                                │
                                       │                                │
                                       └──────── [pnl.py] ──────────────┘
                                                  off-engine analyzer
                                                  (CCXT REST: fetch_my_trades,
                                                   fetch_funding_history)
```

The **Engine** pre-loads market rules, holds persistent WebSocket L2 streams for every active leg, and executes the Synchronized Smart Slicing loop. It also captures per-order forensic detail (order_id, fees, fills) into `closed_trades.json` on every dust-clear close.

The **CLI** never holds state — every invocation just dispatches one IPC call and prints the ack.

The **PnL analyzer** runs on demand. It enriches each closed trade's fees via `fetch_my_trades` (because some venues don't populate fees on the receipt) and joins funding payments via `fetch_funding_history`. Engine critical path is never burdened by these calls — pnl.py is its own process.

---

## Setup

### 1. Install dependencies

```powershell
pip install "ccxt[pro]" aiohttp python-dotenv requests
```

### 2. Configure `.env`

Drop a `.env` file in the project root. For each exchange you intend to trade, set:

```
{EXCHANGE_ID}_API_KEY=...
{EXCHANGE_ID}_SECRET=...
{EXCHANGE_ID}_PASSWORD=...        # only for OKX, KuCoin, Bitget, etc.
```

`{EXCHANGE_ID}` is the uppercase CCXT id. Examples: `BINANCE_API_KEY`, `BYBIT_SECRET`, `OKX_PASSWORD`, `PHEMEX_API_KEY`, `GATE_SECRET`, `KUCOIN_PASSWORD`.

For Pushover alerting on structural failures:

```
PUSHOVER_TOKEN=...
PUSHOVER_USER=...
```

### 3. File layout

#### Engine runtime (always-on daemon)

| File | Role |
|------|------|
| `engine.py` | Always-on async daemon. Run this first. |
| `cli.py` | One-shot IPC client. Run per command. |
| `execution.py` | Slicing logic (project, dispatch, recover, asymmetric-residual halt). |
| `primitives.py` | `ExecutionLeg` / `ExecutionPair` / `FillReceipt` / `BookSnapshot` — typed boundary objects. |
| `receipt_resolver.py` | Per-venue receipt resolution (R-Mode catalog, fetch_order resilience, fee dedup). |
| `venue_overrides.py` | Single source of truth for per-venue quirks (R-Mode, leverage params, IOC params, min notional, etc.). |
| `config.py` | CCXT Pro instance factory (env-driven creds). |
| `utils.py` | Logging + JSON state I/O. |
| `notifier.py` | Pushover P2 alerts on critical failures. |
| `ccxt_patches.py` | Monkey-patches for upstream CCXT bugs. |

#### Off-engine analysis

| File | Role |
|------|------|
| `pnl.py` | Realized PnL analyzer — reads `closed_trades.json`, enriches fees via `fetch_my_trades`, joins funding via `fetch_funding_history`, computes `true_pnl = price + funding − fees`. Run on demand. |

#### Probes

| File | Role |
|------|------|
| `engine_probes.py` | Class 1–4 probe registry (introspection → real-fill smoketests). Multi-command CLI. |
| `probe_fee_shape.py` | Read-only characterization of `fetch_my_trades` fee shape across all 12 venues. |
| `probe_venue_quirks.py` | Read-only characterization of `fetch_order` fees, `fetch_my_trades` limit caps, `since` parameter handling. |
| `probe_funding_shape.py` | Read-only characterization of `fetch_funding_history` capability + shape across all 12 venues. |
| `ccxt_bug_repro.py` | Standalone reproducers for upstream CCXT bugs. |

#### State files (auto-written by engine and pnl.py)

| File | Role |
|------|------|
| `positions.json` | Crash-recovery ledger + open-position trade stats. Includes `entry_order_records` / `exit_order_records` (Phase 1). Asynchronously written. |
| `closed_trades.json` | Append-only archive of fully-closed round-trips. Includes per-order records (Phase 1) and `funding_history` (Phase 3). Mutated by `pnl.py` to enrich fees + funding. |
| `transaction.log` | Append-only timestamped event log. |
| `probe_logs/` | Structured JSONL outputs from probes (forensic anchors). |

#### Reference docs

| File | Role |
|------|------|
| `ENGINE_FIELD_NOTES.md` | Empirical truth surface — every venue quirk, anchor incident, and per-venue table. Updated whenever a probe surfaces something new. |
| `../Arb-Scanalytics/FIELD_NOTES.md` | Sister-project Scanner's per-venue funding-rate field maps. Cross-reference for funding semantics. |

---

## Multipliers & Asymmetric Symbols

The same underlying coin can list under different per-contract multipliers across venues. CHEEMS, for example, appears as `CHEEMS` (1×) on COINEX/GATE, `1000CHEEMS` (1K×) on BINANCE, `1MCHEEMS` (1M×) on BITGET, and `1000000CHEEMS` (1M×) on BYBIT — all the same coin. Two scaling factors are at play and the engine collapses them into one:

- **Symbol prefix multiplier** — encoded in `market.base` (`1000CHEEMS` → multiplier=1000)
- **CCXT `contract_size`** — exchange's native contract size (`market.contractSize`)

Effective conversion: `1 native contract = (multiplier × contract_size) base tokens`.

The operator's contract with the engine is simple: **`--base-amount` is always 1× of the underlying coin**. The engine derives per-leg native quantities by dividing through both factors at the wire boundary, so a single request value translates correctly to wildly different native order sizes per leg.

The single primitive that owns this is `primitives.ExecutionLeg`. Both engine and execution code consume it — no inline multiplier math anywhere else.

**Probe before deploying.** New (venue, symbol) pairs MUST be eyeballed via the `LEG FINGERPRINT` log line at warmup before the first entry is fired. See *Step 1* below.

---

## Operational Workflow

The standard sequence is **warmup → entry → (monitor) → exit**. Discretionary "Sip and Evaluate" iteration sits inside the entry phase — fire with a tight basis floor, observe fill velocity, abort + retune + re-fire if the floor is mispriced against the trade's viable window.

### Step 0 — Boot the Engine

In its own terminal:

```powershell
python engine.py
```

The Engine binds `127.0.0.1:8080`, runs `pre_warm()` (re-subscribes streams for every leg in `positions.json`), and waits for IPC. Keep this terminal visible — slice-by-slice fill telemetry prints here.

### Step 1 — Warm up a pair

Authenticate, load market rules, set leverage, subscribe both L2 streams, block until the first snapshot lands on each leg. Each leg is passed as a single `exchange:symbol` token.

```powershell
# Symmetric (both venues use the same symbol)
python cli.py warmup --legs binance:XRP/USDT:USDT bybit:XRP/USDT:USDT --leverage 1

# Asymmetric (multiplier-prefix divergence)
python cli.py warmup --legs binance:1000CHEEMS/USDT:USDT bybit:1000000CHEEMS/USDT:USDT --leverage 1
```

| Arg | Meaning |
|-----|---------|
| `--legs` | One or more `exchange:symbol` tokens. Two for a hedged pair. |
| `--leverage` | Integer. Capped at 1x–2x by policy for natural liquidation buffer. |

Run warmup once per pair before any entry. Idempotent — safe to re-run.

**LEG FINGERPRINT line.** After both books go live, the warmup emits one fingerprint line per leg. **Eyeball both before firing entry.** The two `1 base_token ≈ $X` numbers must agree to single-bp tolerance — if they're orders of magnitude apart, prefix parsing went wrong or `contract_size` semantics drifted on the venue. Kill the engine and reconcile.

```
[WARMUP] LEG FINGERPRINT binance:1000CHEEMS/USDT:USDT  | base_coin=CHEEMS | multiplier=1000     contract_size=1.0 | 1 native_contract = 1000.0 base_tokens     | 1 base_token ≈ $0.0000063
[WARMUP] LEG FINGERPRINT bybit:1000000CHEEMS/USDT:USDT | base_coin=CHEEMS | multiplier=1000000  contract_size=1.0 | 1 native_contract = 1000000.0 base_tokens | 1 base_token ≈ $0.0000063
```

### Step 2 — Fire entry (Sip)

Drive the Synchronized Smart Slicing entry sequence. The Engine binary-searches the order books each cycle for the maximum slice size (in base tokens) that satisfies the basis floor, dispatches concurrent IOC limits in venue-native units, and recovers any asymmetric fill via uncapped market on the lagging leg.

```powershell
# Symmetric
python cli.py entry --long binance:XRP/USDT:USDT --short bybit:XRP/USDT:USDT --base-amount 26300 --min-entry-basis-bps -25 --max-duration-s 45

# Asymmetric — one --base-amount, engine derives per-leg native qty
python cli.py entry --long binance:1000CHEEMS/USDT:USDT --short bybit:1000000CHEEMS/USDT:USDT --base-amount 1000000 --min-entry-basis-bps 8 --max-duration-s 45
```

| Arg | Meaning |
|-----|---------|
| `--long` | Long-leg spec `exchange:symbol`. Cheaper-funding venue. |
| `--short` | Short-leg spec `exchange:symbol`. Expensive-funding venue. |
| `--base-amount` | Target true-1× base-token quantity of the underlying coin. NOT contracts, NOT prefix-units. |
| `--min-entry-basis-bps` | Net basis floor in bps (per-1×-base price units). Slices below this floor are not dispatched. **Discretionary live knob** — fire tight, observe, retune. |
| `--max-duration-s` | Hard wall-clock deadline. Partial fills are kept as a perfectly hedged position; the deadline never forces a slippage event. |

**Net entry basis** = `(VWAP_short_bid_base − VWAP_long_ask_base) / P_mid_base`. All terms normalized to per-1×-base via each leg's multiplier — symmetric and asymmetric pairs share identical bps math. Negative bps means you accept paying a small inverse basis to enter; positive bps means you require a favorable inter-exchange premium.

**Position keying.** A position is keyed by the **base coin** (multiplier-stripped), not the symbol. `1000CHEEMS@binance / 1000000CHEEMS@bybit` opens under the key `CHEEMS`. Re-running `entry` is permitted only if `--long` and `--short` (both exchange AND symbol) match the stored routing exactly. Mismatched legs return `400`.

### Step 3 — Evaluate, abort if needed

While the slicing loop runs, watch the Engine console for `[SLICE]` lines reporting per-cycle realized basis and cumulative progress. If fill velocity is too slow against the trade's viable window, or the captured edge is tighter than required:

```powershell
python cli.py abort --pair CHEEMS
```

The loop halts at the **next cycle boundary** — never mid-IOC. Accumulated fills are preserved as a perfectly hedged position. The daemon, RAM state, and WebSocket streams stay hot for immediate re-engagement.

Then re-fire `entry` with retuned `--min-entry-basis-bps` for the remaining quantity.

### Step 4 — Exit (unwind)

Reverses the routing. Dispatches reduceOnly IOC slices, captures basis convergence, flattens residual imbalances instantly. Clears the ledger entry on full depletion (residual ≤ dust). Position is identified by `--pair` (the base coin) — leg specs are looked up from saved state, no re-typing.

```powershell
python cli.py exit --pair CHEEMS --base-amount 1000000 --min-exit-basis-bps -8 --max-duration-s 60
```

| Arg | Meaning |
|-----|---------|
| `--pair` | Base coin of the open position (e.g. `XRP`, `CHEEMS`). |
| `--base-amount` | Base qty to unwind (1× of underlying). Auto-clamped to held amount. |
| `--min-exit-basis-bps` | Net exit basis floor. Captures inter-exchange convergence on unwind. |
| `--max-duration-s` | Hard deadline. Residual stays as a hedged position. |

**Net exit basis** = `(VWAP_long_bid_base − VWAP_short_ask_base) / P_mid_base`.

---

## Command Reference (copy-paste templates)

```powershell
# Boot
python engine.py

# Warmup
python cli.py warmup --legs {EX_A}:{SYM_A} {EX_B}:{SYM_B} --leverage {N}

# Entry
python cli.py entry --long {EX_LONG}:{SYM_LONG} --short {EX_SHORT}:{SYM_SHORT} --base-amount {QTY_BASE} --min-entry-basis-bps {BPS} --max-duration-s {SEC}

# Exit
python cli.py exit --pair {BASE_COIN} --base-amount {QTY_BASE} --min-exit-basis-bps {BPS} --max-duration-s {SEC}

# Abort an in-flight slicing loop
python cli.py abort --pair {BASE_COIN}
```

Concrete examples (symmetric):

```powershell
python cli.py warmup --legs binance:XRP/USDT:USDT bybit:XRP/USDT:USDT --leverage 1
python cli.py entry  --long binance:XRP/USDT:USDT --short bybit:XRP/USDT:USDT --base-amount 26300 --min-entry-basis-bps -25 --max-duration-s 45
python cli.py exit   --pair XRP --base-amount 26300 --min-exit-basis-bps -25 --max-duration-s 45
python cli.py abort  --pair XRP
```

Concrete examples (asymmetric):

```powershell
python cli.py warmup --legs binance:1000CHEEMS/USDT:USDT bybit:1000000CHEEMS/USDT:USDT --leverage 1
python cli.py entry  --long binance:1000CHEEMS/USDT:USDT --short bybit:1000000CHEEMS/USDT:USDT --base-amount 1000000 --min-entry-basis-bps 8 --max-duration-s 45
python cli.py exit   --pair CHEEMS --base-amount 1000000 --min-exit-basis-bps -8 --max-duration-s 60
python cli.py abort  --pair CHEEMS
```

---

## Halt Reasons

Every slicing loop returns one of:

| `halt_reason` | Meaning |
|---------------|---------|
| `target` | Full target `--base-amount` filled. |
| `deadline` | `--max-duration-s` expired. Partial position kept hedged. |
| `aborted` | Operator issued `abort`. |
| `dust` | Remaining qty below the per-cycle composite floor (max of either leg's min-lot OR min-notional/price, ceil-rounded to next snap step) — untradeable. |
| `asymmetric_residual` | Post-recovery cycle ended with `cycle_qty_long_base != cycle_qty_short_base` by a tradeable amount. Engine halts to prevent further naked exposure accumulation. Per-leg `qty_long_base` / `qty_short_base` capture the actual venue exposure. Manual reconciliation required. Fires Pushover P2. |

A `target` or `dust` halt on `exit` clears the ledger entry. Otherwise the residual is preserved.

The `asymmetric_residual` halt was added 2026-05-10 after the bingx×xt incident where one leg silently filled while the other returned filled=0 — without the cycle-invariant halt, the loop would have re-fired and accumulated further naked exposure on the over-filling leg. The cycle-level invariant is now enforced explicitly: after dispatch + recovery, both legs must have the same base quantity (within the smaller leg's min-lot tolerance), or the loop halts.

---

## Crash Recovery

The Engine writes `positions.json` after every successful entry/exit. On boot, `pre_warm()`:

1. Reads the ledger (keyed by base coin)
2. Re-instantiates every exchange referenced by the saved per-leg specs
3. Reconstructs `ExecutionLeg` / `ExecutionPair` from live CCXT markets
4. **Drift-detects**: compares stored `multiplier` and `contract_size` against live values. Live values are used; any mismatch is loud-logged as `PREWARM_WARNING` for manual reconciliation.
5. Re-subscribes L2 streams for every leg
6. Logs `Pre-warming complete. All systems hot.`

After an unexpected restart you can immediately call `entry` (to scale in) or `exit` on any pre-existing position with no warmup — the streams are already live. **Warmup is only required for pairs not already in the ledger.**

If a venue silently rotated its `contractSize` between sessions, the stored values serve as the forensic anchor; the engine continues with the live values but the warning gives you a chance to reconcile before placing fresh orders.

---

## Failure Modes & Alerts

The Engine fires Pushover **priority 2** (retry/expire bypasses DND) on:

- **IOC dispatch failure** — either leg's `create_order` raised. The loop halts; the operator must reconcile manually (one leg may have filled).
- **Recovery dispatch failure** — the uncapped market order on the lagging leg failed. Delta-neutrality is no longer guaranteed; immediate manual intervention required.
- **Asymmetric residual halt** — the cycle-invariant check detected a tradeable imbalance after dispatch + recovery (one leg filled X base, the other filled Y, with `|X−Y|` ≥ smaller-leg min-lot). Per-venue exposure is reported in the alert payload (`long_filled=…`, `short_filled=…`, `residual=…`). Operator manually unwinds the over-filling leg via venue UI before re-engaging.

Pre-flight 4xx errors do **not** alert (they're operator mistakes, not system failures):

| Error | Cause |
|-------|-------|
| `400 Exchanges not warmed up` | Run `warmup` first. |
| `400 Symbol not found in CCXT markets` | Typo in `--long` / `--short` or symbol not loaded — re-warmup. |
| `400 Pair base mismatch` | `--long` and `--short` reference different underlyings (different `base_coin` after prefix-stripping). |
| `400 L2 books not live` | Streams haven't received first snapshot — wait or re-warmup. |
| `400 Active position uses X/Y` | Scale-in routing mismatch. Use the original legs (exchange + symbol) or fully exit first. |
| `400 No active position` | `exit` on a `--pair` with no ledger entry. |
| `404 No active slicing loop` | `abort` with nothing in flight. |
| `409 Slicing loop already in flight` | `entry` or `exit` while a loop is running. Abort first. |

---

## Where to look for what

| Want | Look at |
|------|---------|
| Live slice-by-slice fill telemetry (incl. realized VWAP + basis) | Engine console (stdout) |
| Per-leg multiplier / contract_size sanity (warmup) | `[WARMUP] LEG FINGERPRINT` lines |
| Historical fills, errors, halt reasons | `transaction.log` |
| Current open positions + entry/exit VWAPs + realized basis | `positions.json` |
| Round-trip records of fully-closed trades | `closed_trades.json` |
| Per-order forensic detail (id, fees, fills) | `closed_trades.json[*].entry_order_records` / `.exit_order_records` |
| Per-trade funding history events | `closed_trades.json[*].funding_history` |
| Realized PnL view (price + funding − fees) | `python3 pnl.py` (renders to stdout) |
| Final fill summary + halt reason | CLI stdout |
| Per-venue execution quirks + anchor incidents | `ENGINE_FIELD_NOTES.md` |
| Per-venue funding-rate field maps (sister project) | `../Arb-Scanalytics/FIELD_NOTES.md` |
| Probe outputs (forensic JSON) | `probe_logs/` |

The CLI ack summarizes the final state on completion (`filled_base=X/Y | halt_reason=Z | realized_basis_bps=…`, plus `remaining_base=R` for exits). Watch the Engine terminal for live slice-by-slice progress while a loop is running. The CLI exits non-zero on Engine errors and connection failures, so it's safe to chain in scripts.

---

## Trade Tracking

All telemetry prices are normalized to **per-1×-base** so symmetric and asymmetric pairs read uniformly. Every slicing loop emits live telemetry per cycle (no need to wait for loop end):

```
[BOOK]  LONG:  bid=… ask=… (spread=…bps)  SHORT: bid=… ask=… (spread=…bps)  raw_basis=…bps                      ← market context (per-1x-base prices)
[SLICE] IOC slice dispatch_base=… safe_ceiling_base=… (haircut=×DEPTH_DISCOUNT) limits long_native=… short_native=… projected_basis=…bps   ← engine verdict
[RECOVERY] {side} {qty_native} {symbol} on {exchange} @native=… @base=… (filled_base=…, delta_base=…)            ← only if recovery fires
[SLICE] filled long={qty_base}@{vwap_base} short={qty_base}@{vwap_base} realized_basis=…bps recovered={bool} cumulative=…/…   ← realized
```

**Reading the telemetry:**

- `raw_basis` is the basis at top-of-book with no depth walking. `projected_basis` is the basis at `safe_ceiling_base` (the binary-search ceiling, in base tokens). When `raw_basis ≈ projected_basis`, the slice is firing against free top-of-book spread; when `projected_basis << raw_basis`, the search walked deeper into the book to find a larger size that still satisfies the floor.
- `dispatch_base` shows the post-haircut size that was actually sent; `safe_ceiling_base` is the pre-haircut binary-search ceiling. `DEPTH_DISCOUNT` is a phantom-liquidity guard. The `(haircut=×N)` print shows the *effective* ratio — usually `DEPTH_DISCOUNT`, but reverts to `1.00` when the discounted size would fall below the larger leg's min lot (residuals or smoketest-tiny sizes). On a fallback to `×1.00`, this slice gets no phantom-liquidity buffer; it's the deliberate trade for staying tradeable. The IOC limits travel in venue-native price units (`long_native` / `short_native`) because that's what the matching engine consumes.
- `realized_basis` is the **post-recovery combined** basis — what you actually captured on that cycle, accounting for any market-leg drag from imbalance recovery.

**Tuning `DEPTH_DISCOUNT`:** compare `dispatch_base` (post-haircut, what was sent) against the post-dispatch `filled long=…@… short=…@…` qty. If filled consistently lands near `dispatch_base`, the haircut is conservative and could be raised toward 1.0. If filled consistently lands well below `dispatch_base`, phantom liquidity is real and the discount is earning its keep.

At loop end (target / deadline / aborted / dust):

```
[SLICE] Slicing loop END filled=…/… halt_reason=… cumulative_vwap_long_base=… cumulative_vwap_short_base=… cumulative_realized_basis=…bps
```

### Open-position schema (`positions.json`)

Keyed by **base coin** (multiplier-stripped). All quantity fields are in 1× base tokens; all VWAPs are per-1×-base prices. Includes per-order records (Phase 1) so a closing exit can fold them into `closed_trades.json`.

```json
{
  "CHEEMS": {
    "long":  {"exchange": "binance", "symbol": "1000CHEEMS/USDT:USDT",    "multiplier": 1000,    "contract_size": 1.0},
    "short": {"exchange": "bybit",   "symbol": "1000000CHEEMS/USDT:USDT", "multiplier": 1000000, "contract_size": 1.0},
    "amount_base": 700000.0,                  // current open qty (entry_qty_base − exit_qty_base)
    "entry_qty_base": 1000000.0,              // cumulative qty ever entered
    "entry_vwap_long_base":  0.0000063,
    "entry_vwap_short_base": 0.0000064,
    "entry_basis_bps": 15.87,                 // qty-weighted across all entries on this position
    "exit_qty_base": 300000.0,                // cumulative qty ever exited
    "exit_vwap_long_base":  0.0000064,
    "exit_vwap_short_base": 0.0000063,
    "exit_basis_bps": -15.87,                 // qty-weighted across all exits on this position
    "opened_at": "2026-05-07 14:32:11.456",
    "entry_order_records": [ /* Phase 1: appended by every handle_entry call; folded into closed_trades.json on dust-clear */ ],
    "exit_order_records":  [ /* Phase 1: appended by every handle_exit call */ ]
  }
}
```

Per-leg `multiplier` and `contract_size` are persisted as a forensic anchor. The engine always uses live CCXT values during operation; stored values are compared on `pre_warm` and any drift is loud-logged.

Scale-ins blend `entry_*_base` qty-weighted; partial exits blend `exit_*_base` qty-weighted. The ledger entry is deleted when the residual drops below dust (max of either leg's min lot, in base tokens).

### Closed-trade archive (`closed_trades.json`)

Append-only list, one record per fully-closed round-trip. Schema includes Phase 1 per-order records (engine-written) and Phase 3 funding history (pnl.py-mutated):

```json
[
  {
    "base_coin": "CHEEMS",
    "long":  {"exchange": "binance", "symbol": "1000CHEEMS/USDT:USDT",    "multiplier": 1000,    "contract_size": 1.0},
    "short": {"exchange": "bybit",   "symbol": "1000000CHEEMS/USDT:USDT", "multiplier": 1000000, "contract_size": 1.0},
    "entry_qty_base": 1000000.0,
    "exit_qty_base":  999500.0,
    "residual_dust_base": 500.0,
    "entry_vwap_long_base":  0.0000063, "entry_vwap_short_base": 0.0000064, "entry_basis_bps": 15.87,
    "exit_vwap_long_base":   0.0000064, "exit_vwap_short_base":  0.0000063, "exit_basis_bps":  -15.87,
    "round_trip_basis_bps": 0.0,
    "opened_at": "...", "closed_at": "...",

    // Phase 1: per-order forensic + fee-source records (engine-written)
    "entry_order_records": [
      {"order_id": "...", "leg": "long",  "kind": "ioc",      "side": "buy",
       "venue": "binance", "symbol": "1000CHEEMS/USDT:USDT",
       "filled_native": 500.0, "filled_base": 500000.0,
       "vwap_native": 0.0063, "vwap_base": 0.0000063,
       "fees": [{"cost": 0.158, "currency": "USDT"}],
       "ts": "2026-05-07 14:32:11.523"},
      {"order_id": "...", "leg": "short", "kind": "ioc",      "side": "sell", ...},
      {"order_id": "...", "leg": "short", "kind": "recovery", "side": "sell", ...}
    ],
    "exit_order_records":  [ ...same shape, side reversed... ],

    // Phase 3: funding history per leg (pnl.py-mutated)
    "funding_history": {
      "long":  {"events": [{"ts": ..., "amount": -0.071, "currency": "USDT", ...}],
                "total_usdt": -0.213, "non_usdt_events": []},
      "short": {"events": [{"ts": ..., "amount":  0.170, "currency": "USDT", ...}],
                "total_usdt":  0.510, "non_usdt_events": []}
    }
  }
]
```

`round_trip_basis_bps = entry_basis_bps + exit_basis_bps` — the spread component of PnL captured across the round-trip (both terms are profit-positive by construction). Per-record fees and funding events feed the off-engine `pnl.py` analyzer (see "PnL Analysis" below).

---

## PnL Analysis (True PnL primitive)

The engine's job is execution. PnL interpretation is a separate process: `pnl.py` reads `closed_trades.json`, enriches each round-trip with venue-side data, and computes:

```
true_pnl = price_pnl + funding_pnl − fees_pnl
```

Three components, each with its own data source:

| Component | Source | Engine writes? | pnl.py enriches? |
|-----------|--------|----------------|------------------|
| `price_pnl_usdt` | `qty × ((entry_short − entry_long) + (exit_long − exit_short))` from VWAPs in the closed-trade record | yes (VWAPs at engine cycle close) | no — pure math |
| `fees_pnl_usdt` | Σ `order_records[*].fees.cost` in USDT (base-coin fees converted via `vwap_base`) | partial (some venues populate fees on receipt; some don't) | yes — calls `fetch_my_trades(symbol, limit=100)` per (venue, symbol), matches by `t['order']`, replaces captured fees with venue-canonical data |
| `funding_pnl_usdt` | Σ signed `fetch_funding_history` events per leg over `[opened_at, closed_at]` | no | yes — calls `fetch_funding_history(symbol, limit=100)` per leg, client-filter by timestamp |

### Architecture

```
[engine.py] ─writes order facts─▶ [closed_trades.json] ◀─enriches in-place─ [pnl.py]
                                                       └─reads + renders─▶ [terminal/file]
```

The engine NEVER calls `fetch_my_trades` or `fetch_funding_history` — those would add latency to the slicing loop's critical path. pnl.py runs out-of-band whenever the operator wants a PnL view.

### Run

```bash
# Default: enrich + persist + render table
./venv/bin/python3 pnl.py

# Per-exchange-pair filter
./venv/bin/python3 pnl.py --long binance --short bybit --coin XRP --since 2026-05-01

# Format options
./venv/bin/python3 pnl.py --format json
./venv/bin/python3 pnl.py --format csv > pnl.csv

# Dry run — no API calls, no file mutation
./venv/bin/python3 pnl.py --no-enrich --no-funding --no-write

# Verbose — dumps each fetched event to stderr (useful for first
# real funding event to validate sign convention per venue)
./venv/bin/python3 pnl.py --verbose
```

### Output

```
closed_at           | pair          | coin | qty     | notional | rt_bps | price_pnl  | fees      | funding   | true_pnl
2026-05-10 07:46:20 | binance:bybit | XRP  | 19.0000 |  26.9800 |  -1.41 | -0.003800  | 0.056658  | +0.000000 | -0.060458
TOTAL               |               |      |         |          |        | -0.123930  | 0.714931  | +0.000000 | -0.838861

Warnings:
  2026-05-09 19:02:32 BTC: pre-Phase-1 trade — fees_pnl=0 is structural
  2026-05-10 07:46:20 XRP (binance:bybit): non-USDT fee(s) excluded — see funding_history
```

### File mutation

pnl.py writes back enriched `fees` (per order_record) and `funding_history` (per trade) to `closed_trades.json` via atomic write-temp-rename. The merge is race-safe against engine appends — if the engine writes a new closed trade during pnl.py's compute, the re-read merge preserves it (just unenriched until next pnl.py run).

### Empirical foundations (per-venue quirks discovered)

The pnl.py architecture trusts NOTHING per-venue without verification:

- **All 12 venues duplicate `fee` and `fees[0]`** in CCXT receipts → universal dedup needed (`_dedupe_fees` with full-tuple key, type-normalize cost to float).
- **htx `fetch_order` returns NEGATIVE fees** (sign convention vs cost-to-user) AND mixes string + float types → caught only by always-enrich pattern + type-normalize.
- **bitmart fees in received currency**: USDT on sells, base coin (XRP) on buys → converted to USDT via `vwap_base`.
- **xt and okx cap `fetch_my_trades(limit)` at 100** → `FETCH_MY_TRADES_LIMIT = 100` universal.
- **KuCoin `since` parameter is unreliable** across time depths → omit `since`, client-filter by `opened_at`.
- **bitget caps `fetch_funding_history(limit)` below 500** → `FETCH_FUNDING_HISTORY_LIMIT = 100` universal.
- **bingx returns `t['id'] = None`** in fetch_my_trades (only venue) → we match on `t['order']` which IS populated.

Full per-venue tables live in `ENGINE_FIELD_NOTES.md`. Re-runnable probes (`probe_fee_shape.py`, `probe_venue_quirks.py`, `probe_funding_shape.py`) regenerate the catalog on-demand.

### Validation status

- **Phase 1** (engine schema enrichment): live-verified across 6 venue pairs (binance×bybit single + multi-cycle, bingx×xt, htx×bitmart, gate×coinex, kucoinfutures×okx, mexc×bitget). 8 distinct venues confirmed.
- **Phase 2** (fees enrichment): hand-math verified to 4-10 decimals on all 6 pairs; fee rates match published taker rates exactly. Scales correctly through 24 order_records on the multi-cycle test (6 cycles per side).
- **Phase 2 recovery path** (kind='recovery' market orders on lagging leg): verified via mock-CCXT synthetic test. Real recovery has not fired in any smoketest (deep books = no asymmetric residual after symmetric snap); awaits production conditions where book thinness or precision asymmetry produces a residual.
- **Phase 3** (funding integration): synthetic math validated; **awaits empirical sign-convention verification on first real long-hold trade** (≥one funding settlement boundary spanned). All 12 venues confirmed to support `fetchFundingHistory`.

---

## Probes

Probes are runnable scripts that exercise individual code paths or characterize venue behavior. The taxonomy:

### Class 1 — Introspection (read-only, no API calls)

Pure-Python checks of internal state. Free.

### Class 2 — Read-only API (no orders placed)

Probes hit venues' read endpoints (markets, tickers, fetch_order, fetch_my_trades, etc.). Free.

| Script | Purpose |
|--------|---------|
| `probe_fee_shape.py` | Per-venue `fetch_my_trades` fee shape characterization. |
| `probe_venue_quirks.py` | Per-venue `fetch_order` fee shape, `fetch_my_trades` limit caps, `since` param honored. |
| `probe_funding_shape.py` | Per-venue `fetch_funding_history` capability + entry shape. |

Run:

```bash
./venv/bin/python3 probe_fee_shape.py
./venv/bin/python3 probe_venue_quirks.py
./venv/bin/python3 probe_funding_shape.py
```

Re-run on CCXT version updates or when adding a new venue. Outputs land in `probe_logs/` as structured JSON.

### Class 3 — Capital at risk (real fills)

Real orders placed; capital exposure during execution. Used for venue-specific empirical truth that read-only probes can't surface (sign conventions, eventual-consistency lags, etc.).

```bash
# Smoketest a venue pair end-to-end (warmup → entry → exit)
./venv/bin/python3 engine_probes.py cross_venue_smoketest \
  --long_spec=binance:XRP/USDT:USDT \
  --short_spec=bybit:XRP/USDT:USDT \
  --I-AM-FUNDED-AND-AUTHORIZED-FOR-BINANCE \
  --I-AM-FUNDED-AND-AUTHORIZED-FOR-BYBIT

# Diagnose receipt-resolution path on a single venue
./venv/bin/python3 engine_probes.py fill_resolution \
  --venue=kucoinfutures --I-AM-FUNDED-AND-AUTHORIZED-FOR-KUCOINFUTURES
```

The `--I-AM-FUNDED-AND-AUTHORIZED-FOR-{VENUE}` flag is a deliberate handshake — Class 3 probes refuse to run without it.

### Class 4 — Watchdogs

Long-running monitors. Currently empty (operator opted out — atomic-cycle invariants are the safety net).

---

## Field Notes & Empirical Truth

Two markdown documents anchor empirical facts that aren't derivable from code:

| Document | Scope |
|----------|-------|
| `ENGINE_FIELD_NOTES.md` | Per-venue execution behavior: R-Mode catalog (sync-zero / sync-null / eventual placement), receipt fee shape, fetch_my_trades/funding_history quirks, anchor incidents (asymmetric residual halt, htx negative-fee bug, bitmart base-coin fees, etc.). Updated whenever a probe surfaces something new. |
| `../Arb-Scanalytics/FIELD_NOTES.md` | Sister-project Scanner's per-venue funding-rate field maps. Useful cross-reference for funding semantics (last-settled vs upcoming vs forward-forecast). |

Read `ENGINE_FIELD_NOTES.md` before:
- Adding a new venue
- Investigating an unexpected halt
- Changing receipt resolution or fee/funding logic
- Suspecting a per-venue quirk

The methodology is "field notes + probes": empirical observations get anchored in field notes; characterization probes are re-runnable so the catalog stays alive.

---

## Operational Notes

- **One slicing loop per base coin.** Concurrent `entry`/`exit` on the same `--pair` returns `409`. Abort first.
- **USDT-margined perps only.** Inverse/coin-margined perps introduce non-linear payoff drift (Quanto risk).
- **`--base-amount` is always 1× of the underlying.** No prefix-units, no contracts. The engine handles all per-leg conversions.
- **Probe before deploying a new asymmetric pair.** The `LEG FINGERPRINT` line at warmup is the runtime probe; both legs must agree on `1 base ≈ $X` to single-bp tolerance before firing entry.
- **Abort is atomic at cycle boundaries.** Dispatch + recovery is never interrupted mid-flight, so abort always leaves a perfectly hedged position.
- **`enableRateLimit=False`** by design — we let exchange matching engines reject overruns rather than self-throttle.
- **Recovery bypasses basis gating.** When asymmetric fill occurs, neutrality dominates marginal cost on a fractional remainder.
- **Cycle-invariant halt is the safety net.** After dispatch + recovery, both legs must be symmetric within the smaller leg's min-lot tolerance, or the loop halts with `asymmetric_residual` and fires Pushover P2. Implicit invariants drift; explicit invariants halt. (Anchor: 2026-05-10 bingx×xt incident.)
- **Receipt-captured fees are NOT trusted by pnl.py.** The engine writes whatever fees come naturally from receipt resolution, but pnl.py always re-fetches via `fetch_my_trades` because per-venue receipt fee shapes vary unpredictably (htx negative sign, bitmart base-coin currency, xt None values, etc.). Engine stays fast; pnl.py owns interpretation.
- **The slicing loop's per-cycle floor is composite.** `max(both legs' min-lot, max(both legs' min-notional)/mid)` ceil-rounded to the next snap step. Dust threshold uses the same composite; recovery's dust check uses the target-leg's composite.
- **Symmetric snap before dispatch.** Each cycle, the slice's base qty is snapped to the largest value that survives precision-rounding identically on BOTH legs. Without this, asymmetric leg precisions (e.g. KuCoin contract_size=10 vs OKX contract_size=100) silently produced asymmetric fills.
- **Run pnl.py periodically.** For venues with short `fetch_my_trades` retention (KuCoin), running pnl.py within ~24h of trade close ensures fee enrichment captures the data before it ages out. The mutated `closed_trades.json` is then idempotent for subsequent runs.
