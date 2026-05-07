# Cyborg Arbitrageur

Discretionary cross-exchange perp-perp funding-rate arbitrage system. Two processes — a long-running async **Engine** that owns all state, and a stateless **CLI** that pipes operator-driven routing signals to it over local HTTP.

```
[cli.py]  ──HTTP POST──▶  [engine.py daemon] ──CCXT Pro──▶  [exchanges]
   stateless                  always-on, stateful                  WS + REST
   (one-shot)                 RAM caches + position ledger
```

The Engine pre-loads market rules, holds persistent WebSocket L2 streams for every active leg, and executes the Synchronized Smart Slicing loop. The CLI never holds state — every invocation just dispatches one IPC call and prints the ack.

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

| File | Role |
|------|------|
| `engine.py` | Always-on async daemon. Run this first. |
| `cli.py` | One-shot IPC client. Run per command. |
| `execution.py` | Slicing logic (project, dispatch, recover). |
| `config.py` | CCXT Pro instance factory (env-driven creds). |
| `utils.py` | Logging + JSON state I/O. |
| `notifier.py` | Pushover P2 alerts on critical failures. |
| `positions.json` | Crash-recovery ledger + open-position trade stats. Asynchronously written. |
| `closed_trades.json` | Append-only archive of fully-closed round-trips. |
| `transaction.log` | Append-only timestamped event log. |

---

## Operational Workflow

The standard sequence is **warmup → entry → (monitor) → exit**. Discretionary "Sip and Evaluate" iteration sits inside the entry phase — fire with a tight basis floor, observe fill velocity, abort + retune + re-fire if the floor is mispriced against the trade's viable window.

### Step 0 — Boot the Engine

In its own terminal:

```powershell
python engine.py
```

The Engine binds `127.0.0.1:8080`, runs `pre_warm()` (re-subscribes streams for every symbol in `positions.json`), and waits for IPC. Keep this terminal visible — slice-by-slice fill telemetry prints here.

### Step 1 — Warm up a pair

Authenticate, load market rules, set leverage, subscribe both L2 streams, block until the first snapshot lands on each leg.

```powershell
python cli.py warmup --symbol XRP/USDT:USDT --exchanges phemex gate --leverage 1
```

| Arg | Meaning |
|-----|---------|
| `--symbol` | CCXT unified symbol. Always USDT-margined perps (`BASE/USDT:USDT`). |
| `--exchanges` | Space-separated CCXT exchange ids. Order doesn't matter at warmup. |
| `--leverage` | Integer. Capped at 1x–2x by policy for natural liquidation buffer. |

Run warmup once per pair before any entry. Idempotent — safe to re-run.

### Step 2 — Fire entry (Sip)

Drive the Synchronized Smart Slicing entry sequence. The Engine binary-searches the order books each cycle for the maximum slice size that satisfies the basis floor, dispatches concurrent IOC limits, and recovers any asymmetric fill via uncapped market on the lagging leg.

```powershell
python cli.py entry --symbol XRP/USDT:USDT --long phemex --short gate --amount 26300 --min-entry-basis-bps -25 --max-duration-s 45
```

| Arg | Meaning |
|-----|---------|
| `--symbol` | Same as warmup. |
| `--long` | Exchange holding the long leg (cheaper-funding venue). |
| `--short` | Exchange holding the short leg (expensive-funding venue). |
| `--amount` | Target base-token quantity (whole tokens, not contracts). |
| `--min-entry-basis-bps` | Net basis floor in bps. Slices below this floor are not dispatched. **Discretionary live knob** — fire tight, observe, retune. |
| `--max-duration-s` | Hard wall-clock deadline. Partial fills are kept as a perfectly hedged position; the deadline never forces a slippage event. |

**Net entry basis** = `(VWAP_short_bid − VWAP_long_ask) / P_mid`. Negative bps means you accept paying a small inverse basis to enter; positive bps means you require a favorable inter-exchange premium.

**Scale-ins**: re-running `entry` on a symbol with an existing position is allowed *only if* `--long` and `--short` match the stored routing. Mismatched legs return `400`.

### Step 3 — Evaluate, abort if needed

While the slicing loop runs, watch the Engine console for `[SLICE]` lines reporting per-cycle `cycle_filled` and cumulative progress. If fill velocity is too slow against the trade's viable window, or the captured edge is tighter than required:

```powershell
python cli.py abort --symbol XRP/USDT:USDT
```

The loop halts at the **next cycle boundary** — never mid-IOC. Accumulated fills are preserved as a perfectly hedged position. The daemon, RAM state, and WebSocket streams stay hot for immediate re-engagement.

Then re-fire `entry` with retuned `--min-entry-basis-bps` for the remaining quantity.

### Step 4 — Exit (unwind)

Reverses the routing. Dispatches reduceOnly IOC slices, captures basis convergence, flattens residual imbalances instantly. Clears the ledger entry on full depletion (residual ≤ dust).

```powershell
python cli.py exit --symbol XRP/USDT:USDT --amount 26300 --min-exit-basis-bps -25 --max-duration-s 45
```

| Arg | Meaning |
|-----|---------|
| `--symbol` | Symbol of the open position. |
| `--amount` | Base qty to unwind. Auto-clamped to held amount. |
| `--min-exit-basis-bps` | Net exit basis floor. Captures inter-exchange convergence on unwind. |
| `--max-duration-s` | Hard deadline. Residual stays as a hedged position. |

**Net exit basis** = `(VWAP_long_bid − VWAP_short_ask) / P_mid`.

---

## Command Reference (copy-paste templates)

```powershell
# Boot
python engine.py

# Warmup
python cli.py warmup --symbol {SYMBOL} --exchanges {EX1} {EX2} --leverage {N}

# Entry
python cli.py entry --symbol {SYMBOL} --long {EX_LONG} --short {EX_SHORT} --amount {QTY} --min-entry-basis-bps {BPS} --max-duration-s {SEC}

# Exit
python cli.py exit --symbol {SYMBOL} --amount {QTY} --min-exit-basis-bps {BPS} --max-duration-s {SEC}

# Abort an in-flight slicing loop
python cli.py abort --symbol {SYMBOL}
```

Concrete examples:

```powershell
python cli.py warmup --symbol XRP/USDT:USDT --exchanges phemex gate --leverage 1
python cli.py entry  --symbol XRP/USDT:USDT --long phemex --short gate --amount 26300 --min-entry-basis-bps -25 --max-duration-s 45
python cli.py exit   --symbol XRP/USDT:USDT --amount 26300 --min-exit-basis-bps -25 --max-duration-s 45
python cli.py abort  --symbol XRP/USDT:USDT
```

---

## Halt Reasons

Every slicing loop returns one of:

| `halt_reason` | Meaning |
|---------------|---------|
| `target` | Full target quantity filled. |
| `deadline` | `--max-duration-s` expired. Partial position kept hedged. |
| `aborted` | Operator issued `abort`. |
| `dust` | Remaining qty below the larger of either leg's min lot size — untradeable. |

A `target` or `dust` halt on `exit` clears the ledger entry. Otherwise the residual is preserved.

---

## Crash Recovery

The Engine writes `positions.json` after every successful entry/exit. On boot, `pre_warm()`:

1. Reads the ledger
2. Re-instantiates every exchange referenced
3. Re-subscribes L2 streams for every active symbol
4. Logs `Pre-warming complete. All systems hot.`

After an unexpected restart you can immediately call `entry` (to scale in) or `exit` on any pre-existing position with no warmup — the streams are already live. **Warmup is only required for symbols not already in the ledger.**

---

## Failure Modes & Alerts

The Engine fires Pushover **priority 2** (retry/expire bypasses DND) on:

- **IOC dispatch failure** — either leg's `create_order` raised. The loop halts; the operator must reconcile manually (one leg may have filled).
- **Recovery dispatch failure** — the uncapped market order on the lagging leg failed. Delta-neutrality is no longer guaranteed; immediate manual intervention required.

Pre-flight 4xx errors do **not** alert (they're operator mistakes, not system failures):

| Error | Cause |
|-------|-------|
| `400 Exchanges not warmed up` | Run `warmup` first. |
| `400 L2 books not live` | Streams haven't received first snapshot — wait or re-warmup. |
| `400 Active position uses X/Y` | Scale-in routing mismatch. Use the original legs or fully exit first. |
| `400 No active position` | `exit` on a symbol with no ledger entry. |
| `404 No active slicing loop` | `abort` with nothing in flight. |
| `409 Slicing loop already in flight` | `entry` or `exit` while a loop is running. Abort first. |

---

## Where to look for what

| Want | Look at |
|------|---------|
| Live slice-by-slice fill telemetry (incl. realized VWAP + basis) | Engine console (stdout) |
| Historical fills, errors, halt reasons | `transaction.log` |
| Current open positions + entry/exit VWAPs + realized basis | `positions.json` |
| Round-trip records of fully-closed trades | `closed_trades.json` |
| Final fill summary + halt reason | CLI stdout |

The CLI ack summarizes the final state on completion (`filled=X/Y | halt=Z`, plus `remaining=R` for exits). Watch the Engine terminal for live slice-by-slice progress while a loop is running. The CLI exits non-zero on Engine errors and connection failures, so it's safe to chain in scripts.

---

## Trade Tracking

Every slicing loop emits live telemetry per cycle (no need to wait for loop end):

```
[BOOK]  L: bid=… ask=… (sp=…bps)  S: bid=… ask=… (sp=…bps)  raw_basis=…bps   ← market context
[SLICE] IOC slice fire=POST/SAFE (×DEPTH_DISCOUNT) limits L=… S=… proj_basis=…bps  ← engine verdict
[RECOVERY] {side} {qty} {symbol} on {ex} @{vwap} (filled_base=…)              ← only if recovery fires
[SLICE] filled L={qty}@{vwap} S={qty}@{vwap} real_basis=…bps recovered={bool} cum=…/…  ← realized
```

**Reading the telemetry:**

- `raw_basis` is the basis at top-of-book with no depth walking. `proj_basis` is the basis at `safe_size` (the binary-search ceiling). When `raw_basis ≈ proj_basis`, the slice is firing against free top-of-book spread; when `proj_basis << raw_basis`, the search walked deeper into the book to find a larger size that still satisfies the floor.
- `fire=POST/SAFE` shows the `DEPTH_DISCOUNT` haircut — POST is what was actually sent, SAFE is what the engine considered theoretically safe. The haircut is a phantom-liquidity guard.
- `real_basis` is the **post-recovery combined** basis — what you actually captured on that cycle, accounting for any market-leg drag from imbalance recovery.

**Tuning `DEPTH_DISCOUNT`:** compare `fire` (post-discount, what was sent) against the post-dispatch `filled L=…@… S=…@…` qty. If filled consistently lands near POST, the haircut is conservative and could be raised toward 1.0. If filled consistently lands well below POST, phantom liquidity is real and the discount is earning its keep.

At loop end (target / deadline / aborted / dust):

```
[SLICE] Slicing loop END filled=…/… halt_reason=… cum_vwap_L=… cum_vwap_S=… cum_real_basis=…bps
```

### Open-position schema (`positions.json`)

```json
{
  "XRP/USDT:USDT": {
    "long_ex": "phemex", "short_ex": "gate",
    "amount": 70.0,                       // current open qty (entry_qty − exit_qty)
    "entry_qty": 100.0,                   // cumulative qty ever entered
    "entry_vwap_long": 0.6450,
    "entry_vwap_short": 0.6480,
    "entry_basis_bps": 46.20,             // qty-weighted across all entries on this position
    "exit_qty": 30.0,                     // cumulative qty ever exited
    "exit_vwap_long": 0.6470,
    "exit_vwap_short": 0.6460,
    "exit_basis_bps": -15.50,             // qty-weighted across all exits on this position
    "opened_at": "2026-05-07 14:32:11.456"
  }
}
```

Scale-ins blend `entry_*` qty-weighted; partial exits blend `exit_*` qty-weighted. The ledger entry is deleted when the residual drops below dust.

### Closed-trade archive (`closed_trades.json`)

Append-only list, one record per fully-closed round-trip:

```json
[
  {
    "symbol": "XRP/USDT:USDT",
    "long_ex": "phemex", "short_ex": "gate",
    "entry_qty": 100.0, "exit_qty": 99.95, "residual_dust": 0.05,
    "entry_vwap_long": 0.6450, "entry_vwap_short": 0.6480, "entry_basis_bps": 46.20,
    "exit_vwap_long": 0.6470,  "exit_vwap_short": 0.6460,  "exit_basis_bps": -15.50,
    "round_trip_basis_bps": 30.70,
    "opened_at": "...", "closed_at": "..."
  }
]
```

`round_trip_basis_bps = entry_basis_bps + exit_basis_bps` — the spread component of PnL captured across the round-trip (both terms are profit-positive by construction). Funding revenue is layered on top from the exchange statements.

---

## Operational Notes

- **One slicing loop per symbol.** Concurrent `entry`/`exit` on the same symbol returns `409`. Abort first.
- **USDT-margined perps only.** Inverse/coin-margined perps introduce non-linear payoff drift (Quanto risk).
- **Abort is atomic at cycle boundaries.** Dispatch + recovery is never interrupted mid-flight, so abort always leaves a perfectly hedged position.
- **`enableRateLimit=False`** by design — we let exchange matching engines reject overruns rather than self-throttle.
- **Recovery bypasses basis gating.** When asymmetric fill occurs, neutrality dominates marginal cost on a fractional remainder.
