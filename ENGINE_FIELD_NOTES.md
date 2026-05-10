# Engine Field Notes — 13-Venue USDT-Linear Perp Execution

Verified observations about the **execution surface** of every venue we
trade. The Scanner's field notes are about reading the market; this
document is about the kinetic physics of acting on it: order placement,
fill resolution, position state, stream liveness. Different concerns —
different rules — different abstractions.

Each claim below is one of:
- **VERIFIED** (date) — directly probed, evidence in `transaction.log`
  or a probe output file.
- **REVIEWED** — derived from a code-path / CCXT-source read where the
  abstraction is thin enough to trust without a live probe.
- **PROBE PENDING** — assumption currently inherited from CCXT or
  documentation; needs an empirical probe before it's load-bearing.

A claim moves to VERIFIED only when a probe hits the venue and writes
its output into a probe-log artifact. Until then it sits as a roadmap
item in *Probes worth re-running* and *Open empirical questions*.

Scope: USDT-quoted, USDT-settled, linear, swap markets only. Engine
operates exactly two simultaneous legs per trading pair (one long, one
short, same `base_coin` post-prefix-strip). All abstractions assume
**TRUE 1× BASE TOKENS** at the boundary; native units appear only
inside `ExecutionLeg.to_*` and at the IOC dispatch site.

---

## Project venues (13)

`binance`, `bingx`, `bitget`, `bitmart`, `bybit`, `coinex`, `gate`,
`htx`, `kucoinfutures`, `mexc`, `okx`, `phemex`, `xt`.

These are the **canonical CCXT class ids** (lowercase, exactly as the
engine's `config.get_exchange()` consumes them). Matches the Scanner's
13-venue list. Naming gotchas inherited from Scanner field notes:

- `kucoinfutures` (not `kucoin` — separate CCXT class for derivatives)
- `gate` (not `gateio` — verified by Scanner)
- `htx` (not `huobi` — verified by Scanner)

The CCXT class id mismatches are **trap territory** and will silently
mis-instantiate. The engine's `getattr(ccxtpro, exchange_id.lower())`
in `config.py:23` resolves these correctly because `.env` keys use the
canonical id directly (e.g. `KUCOINFUTURES_API_KEY`, not `KUCOIN_API_KEY`).
Do not rename `.env` entries to "friendlier" names.

---

## Probe safety classes — 4 tiers

Every probe falls into exactly one tier. The tier dictates what
authorization, network state, and account state it needs.

### Class 1 — Introspection (zero side effect)

Reads only. Inspects CCXT capability flags, market metadata, public
endpoints. Does **not** touch private endpoints, does not place orders,
does not consume rate-limit budget beyond a normal poll.

- `capabilities` — `c.has` dump per venue, execution-surface subset.
- `markets_audit` — per-symbol `contractSize`, precision, limits, fees.
- `orderbook_liveness` — public WS subscribe-and-measure; no auth.
- `clock_skew` — local clock vs `fetch_time()` per venue.
- `precision_rounding_audit` — pure math test against
  `amount_to_precision()`; reads precision spec, dispatches no orders.

**Network requirement**: Singapore-region IP (Engine matches the
Scanner's geo-block reality).
**Auth requirement**: API keys for private-endpoint extensions
(balance, positions reachability) — keys can be read-only scope.
**Cost**: free.

### Class 2 — Safe-by-construction orders (controlled side effect)

Places real orders that **cannot fill**. Achieved by combining two
properties: (1) the order is IOC (so it auto-cancels if not immediately
filled), and (2) the limit price is set far enough off the book that
no maker would ever match it at the dispatched size. Tests venue
behavior (IOC honor, receipt shape, error semantics) without ever
moving capital.

Key invariant: if any probe in this class causes an actual fill, the
probe is broken — log loud, halt the suite, refund the fill manually.
Always set the price floor at >= 50% off the touch.

- `ioc_honor` — far-from-spread IOC, verifies auto-cancel + receipt
  shape.
- `receipt_shape` — captures the full receipt dict per venue from the
  same far-from-spread IOC; populates the *Receipt shape catalog*.
- `reduceonly_zero` — reduceOnly IOC on a venue with zero position;
  verifies rejection semantics (which CCXT exception class fires, what
  the venue-side message looks like).
- `set_leverage_idempotent` — set leverage to its current value;
  verifies the ack shape and that no silent rotation happens.
- `cancel_phantom` — `cancel_order` on a fabricated id; verifies
  `OrderNotFound` semantics (some venues return 200-OK, some 4xx).

**Network requirement**: Singapore-region IP.
**Auth requirement**: trading-scope API keys.
**Cost**: rate-limit budget + the operational risk that a probe
accidentally fills (mitigated by the price-floor invariant + min-lot
size + IOC).

### Class 3 — Capital-at-risk one-offs (real fill)

Places real orders that **will** fill. Used only where empirical truth
about the post-fill receipt resolution requires an actual fill (some
venues return all-None synchronously even on a real fill — the only
way to verify this is to fill).

- `min_lot_live` — min-lot IOC at top of book on a chosen liquid
  symbol. Costs ~$1–$50 of inventory rotation depending on min lot.
- `cross_venue_smoketest` — full pair execution (entry + exit) at min
  lot on a known coin, end-to-end pipeline validation.
- `funding_settlement_observation` — hold a min-lot position across a
  funding boundary, observe the credit/debit hitting the account.

These probes require **explicit per-run operator authorization**.
Convention: each capital-at-risk probe is gated behind a
`--I-AM-FUNDED-AND-AUTHORIZED-FOR-{VENUE}` flag the operator sets per
session. The flag's presence in the CLI invocation is the
authorization handshake — no implicit defaults, no env-var fallback.

**Network requirement**: Singapore-region IP.
**Auth requirement**: trading-scope API keys + funded perp account
(USDT margin) on the target venue.
**Cost**: real fees + real fills + temporary directional exposure
until the probe's unwind step completes.

### Class 4 — Continuous watchdogs (passive, runs alongside the engine)

Not one-shot probes. Background tasks that run inside `engine.py` and
emit log lines / Pushover alerts when an empirical invariant breaks.
The engine's existing `watch_order_book_loop` 300 s timeout is the
seed of this class.

- `staleness_watchdog` — per-leg "ms since last delta" metric;
  threshold-driven alert.
- `position_drift_watchdog` — periodic `fetch_positions` reconcile
  against `positions.json`; alert on divergence.
- `book_health_watchdog` — emits warnings on crossed books, empty
  top-of-book, level-count collapse.
- `rate_limit_watchdog` — counts 429/418 occurrences in CCXT errors
  observed during normal operation, attributes per venue.

**Network requirement**: same as engine.
**Auth requirement**: same as engine.
**Cost**: no additional rate-limit beyond engine's normal traffic.
Lives in `engine_watchdogs.py` (proposed); imported and started in
`engine.py:start_server()`.

### Methodology note — lean toward real-fill probes (2026-05-10)

The Class-2 → Class-3 progression proved cumbersome in practice. TWO
separate incidents on the same day exposed the same flaw:

  1. **XT silent-residual** (~04:00). 30 XRP naked short on XT plus
     20 XRP naked long on BingX. Root cause: XT's fetch_order is
     eventually consistent (~800ms lag) on filling IOCs but the
     Class-2 `receipt_shape` probe used a non-filling IOC and never
     saw the lag pattern.

  2. **BingX silent-residual** (~04:10). 10 XRP naked long on BingX
     plus 10 XRP naked short on XT. Root cause: BingX was classified
     `sync-zero` by `receipt_shape` (placement returned
     `status='canceled', filled=0.0` on a non-filling IOC). On an
     ACTUALLY-filling IOC, BingX's placement returns all-None
     (sync-null behavior). The engine trusted placement → recorded
     filled=0 → dispatched recovery → BingX rejected with
     "Insufficient margin" because the unread fill had already
     consumed margin.

Both incidents share one root cause: **the R-Mode catalog was built
from non-filling probes that verified the resolution PATH but
couldn't verify resolution CORRECTNESS on real fills.** The property
that matters in production (does fetch_order or placement return the
right `filled` value when the order ACTUALLY fills?) was never
tested for any venue.

The Class-3 `fill_resolution` probe closes the gap. The 2026-05-10
sweep characterized 11 of 13 venues (8 sync-null + 3 sync-zero) in
~3 minutes at ~$1 of fees. Two empirical patterns confirmed:

  * **fetch_order eventual consistency** (XT 817ms, KuCoin 786ms,
    others 50-250ms) — handled by `_fetch_order_resilient` with
    bounded retry on both OrderNotFound and stale all-None signatures.
  * **Misclassified sync-zero on filling IOCs** (BingX) — handled by
    a defensive fallback in `resolve_receipt`: trust placement IFF
    `filled` is populated, else fall back to fetch_order with retry.
    A `RESOLVER_WARNING` log surfaces every fallback so the operator
    can update the R-Mode catalog with empirical truth.

**Going forward**: lean toward Class-3 real-fill probes for new
venue work. Class-2 retains value where capital cost would dominate
(burst rate-limit tests, etc), but the default for receipt-shape
verification is the real-fill probe. The
`--I-AM-FUNDED-AND-AUTHORIZED-FOR-X` handshake is operational
friction without commensurate safety value once the operator has
globally authorized engine work — kept for now because removing it
is a separate refactor, but the spirit is "give me the data faster,
I'll trade a few dollars of fees for clean truth." We will revisit
the probe taxonomy when we redesign the primitives.

**Defense-in-depth as cultural choice**: the resolver's defensive
sync-zero fallback (trust-but-verify, fall back when placement
isn't authoritative) is the explicit response to "the catalog WILL
be wrong on some venue we haven't probed yet." Catalog data drives
behavior for the happy path; the fallback handles the catalog being
stale or wrong without silent capital drift. RESOLVER_WARNING logs
are the feedback loop that keeps the catalog current.

---

## Cardinal Engine concerns — three abstractions to harden

Every assumption the Engine currently makes that's not yet verified
collapses into one of three categories. Each category needs its own
primitive in `primitives.py` before the assumption is safe at scale.

### 1. Receipt resolution (the silent-fill failure mode)

**The single most expensive bug class in the engine.** Verified in
`transaction.log` on 2026-05-07 21:05.

`execution.py` reads `receipt.get("filled")` and assumes it returns a
synchronous-final, accurate, native-unit fill quantity. The Bybit IOC
receipt at 21:05:39 showed:

```python
{'info': {'orderId': '5b6e39ad-…'},
 'id': '5b6e39ad-…', 'clientOrderId': None,
 'timestamp': None, 'datetime': None,
 'symbol': 'BTC/USDT:USDT', 'type': None, 'timeInForce': None,
 'postOnly': None, 'reduceOnly': None, 'side': None,
 'price': None, 'amount': None,
 'cost': None, 'average': None, 'filled': None, 'remaining': None,
 'status': None, 'fee': None, 'trades': [], 'fees': []}
```

**Every numeric field is None.** `_fill_vwap` returns 0, `to_base_qty(None)`
returns 0. From the engine's perspective, no fill happened. From the
venue's perspective, the order *may have* filled — we do not know. The
loop logs "filled 0/0 (IOCs rejected entirely)" and re-dispatches the
same slice next cycle. This pattern repeated for ~17 seconds (11 cycles)
before binance rejected the long with `InsufficientFunds` — the
operator's margin had been silently consumed by accumulating long fills,
even though the engine's books showed cumulative=0.

**Architectural conclusion**: the receipt object cannot be trusted as
fill-resolved. Every receipt must pass through a per-venue resolver
that knows whether the venue is sync-final or eventual-consistent, and
follows up with `fetch_order(id)` in the latter case before the loop
proceeds.

Proposed primitive (`primitives.py`):

```python
@dataclass
class FillReceipt:
    """Resolved fill state. Constructed only after the venue's
    resolution model has been honored."""
    leg: ExecutionLeg
    order_id: str
    filled_base: float        # in 1× base tokens, post-resolution
    vwap_base: float          # per-1×-base, 0.0 only if filled_base==0
    status: str               # 'closed' | 'canceled' | 'filled' | 'rejected'
    resolution_path: str      # 'sync-final' | 'fetch-order' | 'fetch-trades'
    raw_create_response: dict # the original create_order receipt
    raw_resolve_response: dict | None  # follow-up fetch, if performed
```

Engine becomes: `await dispatch_ioc_pair(...)` returns
`(FillReceipt, FillReceipt)` — fully resolved or raised. `to_base_qty`
and `_fill_vwap` move into `FillReceipt` construction (one place,
not scattered across `execution.py:611-616`).

Per-venue receipt-resolution table lives in §*Receipt shape catalog*
below — that's the empirical-truth data this primitive consumes.

### 2. WebSocket stream liveness (the silent-staleness failure mode)

`engine.py:52` wraps `watch_order_book` in `asyncio.wait_for(..., 300.0)`.
That catches a fully-dead socket within 5 minutes. It does NOT catch:

- Deltas arriving but the book state not advancing (CCXT internal
  cache wedge).
- A non-empty book that's actually 30 s stale because deltas resumed
  but missed snapshots aren't re-requested.
- An auto-reconnect that returns deltas keyed off a snapshot from 60 s
  ago rather than re-subscribing to fresh top-of-book.

**Architectural conclusion**: the cached `book` dict needs metadata
beyond `{'bids': ..., 'asks': ...}`. Specifically, "when did the last
delta arrive at our process". Every slice gate must consult that
metadata before reading the book.

Proposed primitive (`primitives.py`):

```python
@dataclass
class BookSnapshot:
    """Time-stamped order book — the cached entry under
    engine.order_books[(ex_id, symbol)]."""
    bids: list[list[float]]
    asks: list[list[float]]
    venue_ts_ms: int | None       # exchange-side timestamp when known
    received_ts_ms: int           # local monotonic-ms when this delta hit our process
    delta_count: int              # incremented per watch_order_book yield
    sequence: int | None          # venue-side sequence number when exposed

    def is_fresh(self, max_age_ms: int) -> bool:
        return (now_ms() - self.received_ts_ms) <= max_age_ms

    def has_top_of_book(self) -> bool:
        return bool(self.bids) and bool(self.asks)

    def is_crossed(self) -> bool:
        return self.has_top_of_book() and self.bids[0][0] >= self.asks[0][0]
```

Engine: `watch_order_book_loop` writes `BookSnapshot` objects with
`received_ts_ms = monotonic_ms()` on every yield.
`run_slicing_loop` (or a new `_pre_slice_sanity` step) calls
`book.is_fresh(SLICE_MAX_BOOK_AGE_MS)` AND `book.has_top_of_book()` AND
`not book.is_crossed()` before passing into `project_slice`. Failure
of any check returns `None` (treat as "skip cycle, retry shortly")
exactly like a basis-floor failure.

Empirical inputs for `SLICE_MAX_BOOK_AGE_MS` come from the
`orderbook_liveness` probe — see §*WebSocket stream baselines*.

### 3. Order book sufficiency (the thin-book failure mode)

`execution.py:217` checks "books non-empty"; that's the only gate
before binary-searching depth. Risks at slice time:

- A 1-level top-of-book passes the gate; binary search finds a
  microscopic safe size, IOC fires for 1 unit, recovery flounders.
- Crossed book trivially "passes" the basis floor (book inversion
  makes the math read like a giant arbitrage).
- A book that's stale-but-non-empty looks indistinguishable from a
  fresh book at this layer.

**Architectural conclusion**: book sufficiency is a separate gate
from book liveness, and both must precede `project_slice`. The
primitive above (`BookSnapshot`) handles liveness; sufficiency wants a
small set of tuneable thresholds that come from the
`book_sufficiency_baseline` probe.

Proposed approach: extend `project_slice` (or wrap it) with a
pre-flight that:

1. Asserts `book.is_fresh(MAX_BOOK_AGE_MS)` on both legs.
2. Asserts `book.has_top_of_book()` on both legs.
3. Asserts `not book.is_crossed()` on both legs.
4. Asserts `len(book.{bids,asks}) >= MIN_LEVELS` on the side being
   walked (asks for buy, bids for sell) — protects against
   1-level-deep books.
5. Optionally asserts a top-of-book spread sanity (spread bps under
   a per-venue ceiling) — protects against pathological liquidity
   evaporation.

Each assertion fires a typed log line on failure; the cycle is
skipped. Aggregating these failures as a watchdog metric gives the
operator empirical data on how often each leg's books are
sub-tradeable, attributable per (venue, symbol).

---

## Per-venue execution spec — Table A: Order placement

IOC param shape (and any per-venue overrides) plus the IOC-honor verdict
per venue. The engine's create_order calls must use the **effective IOC
params** column verbatim — passing only CCXT's unified `{timeInForce: 'IOC'}`
fails on venues that need a venue-specific override.

Verified run: 2026-05-09 against `BTC/USDT:USDT`. Probe log:
`probe_logs/ioc_honor_2026-05-09T05-50-02Z.jsonl` (post-bypasses).
**10 of 13 venues now verified IOC-honored.**

| Venue            | Effective IOC params                              | IOC honor verdict          | Notes                                                                                                                |
|------------------|---------------------------------------------------|----------------------------|----------------------------------------------------------------------------------------------------------------------|
| `binance`        | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | status='expired', filled=0.0. Clean sync-zero receipt.                                                               |
| `bingx`          | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | status='canceled', filled=0.0.                                                                                       |
| `bitget`         | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | sync-null receipt; auto-cancel confirmed.                                                                            |
| `bitmart`        | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | sync-null receipt; auto-cancel confirmed. Cleared 2026-05-09 07:16 via `config.py` credential remap: bitmart's CCXT class signs with `self.uid` (which CCXT maps from `creds['uid']`), so `config._VENUE_CREDENTIAL_REMAP['bitmart'] = {'uid': 'PASSWORD'}` copies the operator's `BITMART_PASSWORD` value (the API memo) into the signing slot. |
| `bybit`          | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | sync-null receipt; auto-cancel confirmed via fetch_open_orders.                                                      |
| `coinex`         | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | sync-zero receipt: status=None but filled=0.0 + cost=0.0 + fee populated.                                            |
| `gate`           | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | status='canceled', filled=0.0. Clean sync-zero receipt.                                                              |
| `htx`            | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | sync-null receipt; auto-cancel confirmed. Cleared 2026-05-09 06:42 after operator deposited additional 80 USDT to satisfy default-leverage margin requirement. |
| `kucoinfutures`  | `{timeInForce: 'IOC', marginMode: 'cross'}`       | **VERIFIED HONORED**       | sync-null receipt. **Required override**: `marginMode: 'cross'` — venue rejects with 330005 otherwise. Repro in `ccxt_bug_repro.py`. |
| `mexc`           | `{timeInForce: 'IOC', type: 3}`                   | **VERIFIED HONORED**       | sync-null receipt. **Required override**: `type: 3` (mexc swap order-type integer for IOC). CCXT 4.5.51 swap-create-order does NOT translate `timeInForce: 'IOC'`; without `type: 3`, the order rests as a regular limit. Empirically verified — see `ccxt_bug_repro.py`. |
| `okx`            | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | sync-null receipt; auto-cancel confirmed.                                                                            |
| `phemex`         | `{timeInForce: 'IOC'}`                            | **WALKED AWAY** 2026-05-09 | After operator regenerated the phemex API key with explicit Contract/Futures permissions, IP whitelisted the VPS, and confirmed USDT in the Contract Trade Account, the venue continues to return `39999 Error in place order` with no diagnostic. CCXT-verbose request body is well-formed; the rejection happens venue-side with no actionable information. Phemex is excluded from the v1 verified catalog. Re-add when an operator-side root cause is identified (phemex support ticket recommended). |
| `xt`             | `{timeInForce: 'IOC'}`                            | **VERIFIED HONORED**       | sync-null receipt; auto-cancel confirmed.                                                                            |

CCXT bypass infrastructure now lives in `ccxt_patches.py` (subclasses)
and `engine_probes.py:_VENUE_CREATE_ORDER_PARAM_OVERRIDES`. The
engine's eventual production path — when wired — must consume the same
override map. Single source of truth.

reduceOnly param shapes, set_leverage shapes, position-mode and
margin-mode defaults remain **PROBE PENDING** for every venue —
the `reduceonly_zero` and `set_leverage_idempotent` probes populate
those columns when run.

Notes:

- **OKX `tdMode`**. OKX requires `tdMode` (`cross`|`isolated`) on every
  order; CCXT defaults to `cross` when `defaultType:'swap'` is set, but
  some CCXT releases have dropped this default — verify on the
  `ioc_honor` probe output.
- **MEXC default isolated**. MEXC margins isolated by default. If we
  trade cross-mode elsewhere, mixing margin modes within a hedged pair
  produces wildly different liquidation distances per leg. Set this
  explicitly at warmup or pin it at account level.
- **BingX hedge mode**. BingX defaults to hedge-mode (separate long
  and short positions per symbol). With hedge mode, `reduceOnly`
  alone is ambiguous — venue rejects without `positionSide`. We need
  to either force one-way mode account-wide, or pass `positionSide`
  on every order. **PROBE BingX FIRST** before any live trade.

### Position mode → reduceOnly disambiguation

In hedge mode (BingX default, OKX optional), a single symbol holds
TWO positions: `LONG` and `SHORT`. `reduceOnly: true` without a
`positionSide` qualifier is ambiguous and the venue rejects. Engine
currently does not pass `positionSide` anywhere. Either:

1. (Easier) Force every venue to one-way mode account-wide. Probe
   verifies via `fetch_position_mode()`.
2. (Robust) Threadthrough a `position_side` enum into `dispatch_ioc_pair`
   and `recover_imbalance`. Required for any venue that doesn't
   respect a one-way-mode account flag.

Option 1 is cheaper and the convention for our trade style; it must
be enforced via a Class-1 introspection probe at engine boot, not
trusted to documentation.

---

## Per-venue execution spec — Table B: Receipt shape catalog

Captures whether `create_order(...)` returns a fill-resolved receipt
synchronously, or only an order ID with all fill fields `None`. The
authoritative source for the engine's planned `FillReceipt` primitive
(`R1` in the refactor roadmap) — every venue listed as **sync-null**
or **eventual** below MUST have `fetch_order(id)` called before the
slicing loop reads fill state.

`R-Mode` legend:
- **sync-final** — receipt has `status` terminal AND `filled` populated
  AND (when filled > 0) `average` populated.
- **sync-zero** — receipt has terminal `status` AND `filled = 0` AND
  `average = None`. Subset of sync-final for non-fillable orders.
  Engine can trust the receipt — no fetch_order needed.
- **sync-null** — `status` AND `filled` both `None`. Receipt useful
  ONLY for `id` extraction. Always requires `fetch_order(id)` follow-up
  before fill-state reads.
- **eventual** — `status = 'open'` synchronously; venue is still
  resolving. Needs `fetch_order(id)` (with backoff) until `status`
  reaches a terminal value.

Verified rows: 2026-05-09 against `BTC/USDT:USDT`. Probe log:
`probe_logs/receipt_shape_2026-05-09T05-53-58Z.jsonl` (post-bypasses).
**10 of 13 venues now classified.**

| Venue            | Placement R-Mode    | fetch_order req         | fetch_order params              | Verified gained fields (resolved minus placement)                          | Status        |
|------------------|---------------------|-------------------------|---------------------------------|----------------------------------------------------------------------------|---------------|
| `binance`        | **sync-zero**       | NO                      | n/a                             | (none — placement & resolved identical)                                    | VERIFIED 2026-05-09 |
| `bingx`          | **sync-zero**       | NO                      | n/a                             | (none — placement & resolved identical)                                    | VERIFIED 2026-05-09 |
| **`bitget`**     | **sync-null**       | **YES — mandatory**     | `{}`                            | `amount, cost, fee, filled, price` (5 fields None at placement)            | VERIFIED 2026-05-09 |
| **`bitmart`**    | **sync-null**       | **YES — mandatory**     | `{}`                            | `cost, filled, remaining, status` (4 fields None at placement)             | VERIFIED 2026-05-09 |
| **`bybit`**      | **sync-null**       | **YES — mandatory**     | **`{acknowledged: True}`**      | `amount, cost, filled, price, remaining` (5 fields None at placement)      | VERIFIED 2026-05-09 |
| `coinex`         | **sync-zero**       | NO                      | n/a                             | (none — placement carries filled=0, cost=0, fee. status=None doesn't matter — placement is authoritative.) Caveat: coinex auto-purges canceled IOCs from order history within ~1 s; fetch_order returns `3103 order not exists`. Confirms sync-zero is the right route. | VERIFIED 2026-05-09 |
| `gate`           | **sync-zero**       | NO                      | n/a                             | (none — placement & resolved identical)                                    | VERIFIED 2026-05-09 |
| **`htx`**        | **sync-null**       | **YES — mandatory**     | `{}`                            | `amount, cost, fee, filled, price` (5 fields None at placement)            | VERIFIED 2026-05-09 |
| **`kucoinfutures`** | **sync-null**    | **YES — mandatory**     | `{}`                            | `amount, cost, filled, price, remaining` (5 fields None at placement)      | VERIFIED 2026-05-09 |
| **`mexc`**       | **sync-null**       | **YES — mandatory**     | `{}`                            | `amount, cost, fee, filled, price` (5 fields None at placement)            | VERIFIED 2026-05-09 |
| **`okx`**        | **sync-null**       | **YES — mandatory**     | `{}`                            | `amount, cost, fee, filled, price` (5 fields None at placement)            | VERIFIED 2026-05-09 |
| `phemex`         | ?                   | ?                       | ?                               | ?                                                                          | **WALKED AWAY** 2026-05-09 — venue returns `39999` regardless of CCXT params |
| **`xt`**         | **sync-null**       | **YES — mandatory**     | `{}`                            | `amount, cost, filled, price, remaining` (5 fields None at placement)      | VERIFIED 2026-05-09 |

### sync-null is the majority case — the FillReceipt architecture imperative

**8 of the 12 catalogued venues are sync-null** — two-thirds of the
engine's traffic returns placement receipts whose every fill field is
`None`. This isn't a bybit oddity; it's the dominant operating mode
across the desk.

| R-Mode        | Count | Venues                                                         |
|---------------|------:|----------------------------------------------------------------|
| sync-zero     |     4 | binance, bingx, coinex, gate                                   |
| sync-null     |     8 | bitget, bitmart, bybit, htx, kucoinfutures, mexc, okx, xt      |
| sync-final    |     0 | (would only appear on capital-at-risk probes)                  |
| eventual      |     0 | none observed at this sample (probe doesn't trigger it)        |
| walked-away   |     1 | phemex (venue 39999, no CCXT-side bypass)                      |

**Implication**: the engine cannot proceed with `receipt.get('filled')`
on placement responses. Without `fetch_order(id)` follow-up on the
eight sync-null venues, the engine sees phantom zero-fills while the
venue silently consumes margin. This was the May 7 incident on bybit —
the empirical truth is that the same failure mode is latent on
**bitget, bitmart, htx, kucoinfutures, mexc, okx, and xt** as well.

The fetch_order follow-up is cheap (~50–200 ms RTT on Singapore
infra) and can be parallelized with the dispatch of the next slice's
binary search. The cost is dwarfed by the alternative: silent
asymmetric exposure accumulating cycle after cycle.

### Bybit anchor case — full receipt diff

Originally documented in `transaction.log` 2026-05-07 21:05:39.
Reproduced by `receipt_shape` on 2026-05-09 with the
`acknowledged: True` follow-up param. Placement receipt populated:

```
id           = '32492707-3cc6-4ee6-8a74-9cd1d3cc7a4e'
info.orderId, info.orderLinkId  (only)
clientOrderId = None
status       = None
side         = None
type         = None
timeInForce  = None
price        = None
amount       = None
filled       = None
remaining    = None
average      = None
cost         = None
fee          = None
trades       = []
```

After `fetch_order(id, params={'acknowledged': True})`:

```
status     = 'canceled'
filled     = 0.0
cost       = 0.0
remaining  = 0.001
price      = 79734.4
amount     = 0.001
```

**Bybit-specific quirk**: fetch_order raises `ArgumentsRequired`
without `params={'acknowledged': True}` — see Table B's "fetch_order
params" column. The empty `{}` for the other five sync-null venues
means CCXT's stock fetch_order call shape works. **bybit is the only
venue requiring a non-empty params dict for fetch_order.**

The `receipt_shape` probe writes one row per venue into this table per
run; structured JSON in `probe_logs/receipt_shape_<ts>.jsonl` so the
table regenerates mechanically.

### Anchor incident — bybit silent-fill (2026-05-07 21:05:22 → 21:05:39)

Documented in `transaction.log`. Engine ran an entry on
`binance:1000CHEEMS:USDT:USDT` × `bybit:1000000CHEEMS:USDT:USDT` for
~17 s (later rerun on `BTC/USDT:USDT`). Each cycle:

1. `BOOK` line healthy: spreads ~0.01 bps per leg, raw_basis ~2 bps.
2. `SLICE` dispatch at `dispatch_base=0.001` BTC.
3. Both `create_order` calls return — binance with a normal receipt,
   bybit with **every fill field `None`** (the anchor receipt above).
4. `_fill_vwap(receipt_bybit)` returns 0. `to_base_qty(None)` returns 0.
5. Recovery sees `delta_base = 0.001 - 0 = 0.001`, fires a market sell
   on bybit for 0.001 BTC. Recovery receipt **also sync-null** —
   `filled_base=0` per the log.
6. Engine logs `filled 0/0 (IOCs rejected entirely)`. Loop continues.
7. After 11 cycles, binance returns
   `InsufficientFunds: Margin is insufficient` — the long had been
   accumulating real fills the whole time. Engine raises
   `RuntimeError`, fires Pushover P2.

**Worst-case interpretation**: 11 × 0.001 BTC = 0.011 BTC long on
binance against a possibly-asymmetric short on bybit, depending on
how many of the bybit IOCs and recoveries actually filled at the
venue. Engine's books showed 0.000. The discrepancy is invisible to
the operator until they check the venue UI.

**Mitigation**: The `FillReceipt` primitive proposed in §1 above. For
any venue listed as sync-null in this table, `fetch_order(id)` runs
synchronously after `create_order` returns and BEFORE the loop reads
fill state. Latency cost: one round-trip per leg per cycle (~50–200ms
on the Singapore VPS). Acceptable given the alternative is silently
accumulating delta-divergent positions.

---

## Per-venue execution spec — Table C: WebSocket / L2 liveness

Empirical baselines for the staleness watchdog and the planned
`MAX_BOOK_AGE_MS` constant. Values from a 30 s WS sample on
`BTC/USDT:USDT` on the engine's Singapore-region IP, 2026-05-09.

Probe log: `probe_logs/orderbook_liveness_2026-05-09T04-03-41Z.jsonl`.
Re-running with `--duration-s=120` gives tighter p95s; the 30 s sample
below is sufficient to set initial thresholds.

All 13 venues passed: zero empty-top-of-book frames, zero monotonicity
violations, 100% timestamp coverage, and only one venue (bitmart)
exhibited any crossed-book frames. Reconnect behavior column remains
**PROBE PENDING** until `reconnect_behavior` probe runs.

| Venue            | Yields/30s | First snap (s) | Median Δ ms | p95 Δ ms | Max Δ ms  | Empty top | Crossed | Mono break | book.ts populated | Status        |
|------------------|-----------:|---------------:|------------:|---------:|----------:|----------:|--------:|-----------:|------------------:|---------------|
| `binance`        |        279 |          0.97  |       102.0 |    112.4 |     601.5 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `bingx`          |         52 |          4.24  |       500.4 |    527.3 |     881.8 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `bitget`         |        252 |          4.53  |        99.9 |    113.1 |     610.3 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `bitmart`        |        257 |          3.05  |        39.9 |    210.0 |     676.6 |         0 |    **3**|          0 |              100% | VERIFIED 2026-05-09 |
| `bybit`          |        758 |          2.18  |        20.8 |    103.1 |     608.0 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `coinex`         |         42 |          3.76  |       204.6 | **2983.5**| **3265.3**|         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `gate`           |        296 |          0.48  |       100.0 |    105.0 |     151.6 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `htx`            |        673 |          0.64  |        31.9 |    108.9 |     657.7 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `kucoinfutures`  |  **8231**  |          2.66  |     **1.9** |  **9.9** |     565.0 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `mexc`           |        110 |          6.75  |       211.9 |    250.0 |     302.0 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `okx`            |        257 |          2.76  |       100.1 |    198.8 |     614.1 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `phemex`         |        267 |          4.88  |        41.6 |    444.1 |    1732.8 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |
| `xt`             |        205 |          5.52  |       100.3 |    201.5 |     301.7 |         0 |       0 |          0 |              100% | VERIFIED 2026-05-09 |

### Calibration takeaways

**`FIRST_SNAPSHOT_TIMEOUT_S` (currently 10.0 s)**. Worst observed
time-to-first-snapshot is 6.75 s (mexc) on a calm sample. Doubling
for headroom suggests the current 10 s value is appropriate but
borderline — consider 12 s if a future `reconnect_behavior` run shows
worse p95 under post-disconnect resubscribe.

**`MAX_BOOK_AGE_MS` (proposed for the BookSnapshot primitive)**. Sets
the staleness gate before any slice. p95 max-Δ across the 13 venues
ranges from 110 ms (bybit, gate) to 2983 ms (coinex). For a tight
slicing loop, **2000 ms** would skip slices on coinex's worst silences
but never on the other 12; **4000 ms** absorbs coinex but lets a real
wedge get past on faster venues. Recommendation: tier the threshold
**per-venue** in the `BookSnapshot` primitive — coinex/phemex wider
window (4000 ms), the rest at 1000–2000 ms.

**Empirical exceptions that already exist in the data**:

- **bitmart had 3 crossed-book frames** in 257 yields (1.2%). On a
  liquid BTC perp, crossed books are matching-engine glitches, not
  real arbitrage — the slicing-loop's basis math would read these as
  giant negative spreads and short-circuit a slice. Refactor R3
  (`is_crossed()` gate) is empirically necessary, not theoretical.
- **kucoinfutures fires 274 yields/sec** (vs. 1.4–25/s on other
  venues). KuCoin's WS broadcasts every internal book mutation, not
  rate-limited deltas. Engine cycles at 1.5 s — KuCoin floods the
  cache faster than the engine reads it. Not a bug, but a CPU/IO
  consideration if more KuCoin pairs get added.
- **coinex's 2.98 s p95 max-Δ** is a 50× outlier from the median
  cluster. Coinex either rate-limits WS deltas, or its book is so
  thin on BTC that real ticks are sparse. Combined with its 1.4
  yields/s, coinex is the brittlest venue for stream liveness in our
  set. Strongly favors `MAX_BOOK_AGE_MS` per-venue tiering.

**Reconnect re-snapshots column** is the missing piece — it tells
us whether after a forced WS close, the venue re-emits a full snapshot
or resumes from a stale cache. Until `reconnect_behavior` runs, the
engine's `watch_order_book_loop` should clear the cached book on
exception (currently it doesn't — the stale book persists).

---

## CCXT capability matrix (execution surface)

Mirror of the Scanner's c.has table, restricted to the verbs the Engine
actually invokes.

`createOrder`, `cancelOrder`, `fetchOrder`, `fetchOpenOrders`,
`fetchPositions`, `fetchPosition`, `setLeverage`, `setMarginMode`,
`setPositionMode`, `watchOrderBook`, `watchTrades`, `watchPositions`,
`fetchBalance`.

Status: **PROBE PENDING**. Populated by `engine_probes.py capabilities`.

The interesting cells are the ones that surprise:

- Some venues advertise `setLeverage: false` in `c.has` but actually
  accept it (CCXT's flag is conservatively wrong).
- `watchPositions` is False for most venues; only a few stream
  position deltas. Engine currently doesn't use it but a future
  position-drift watchdog might.
- `cancelOrder` is universally True but the **rejection semantics on
  unknown-id** vary wildly (200-OK silent, 400, 404, 5xx). Matters for
  abort flows that race the venue's own auto-cancel of an IOC.

---

## Stream behavior — reconnect signature

CCXT Pro's `watch_order_book` is supposed to handle reconnect
internally. The engine's outer wrapper at `engine.py:46-62` adds a
5-second sleep after exception and re-enters the watch loop. PROBE
PENDING:

- After a forced socket close, does CCXT Pro re-emit a full snapshot
  or resume deltas from the prior cached book?
- During the reconnect window (~1 s typical), does the cached
  `engine.order_books[(ex_id, symbol)]` entry hold the stale value,
  or is it cleared?
- Some venues require explicit re-subscription messages on reconnect
  (we should not need to know — that's CCXT Pro's job — but our
  operational testing should *verify* not assume).

The `reconnect_behavior` probe (PROBE PENDING) will:

1. Subscribe to a venue's L2.
2. Wait 30 s for cadence baseline.
3. Force the underlying websocket closed (via `client.close()` or
   `connection.close()` per CCXT's internal API).
4. Measure: how long until a new delta arrives. Whether the new book
   is identical to the old (stale resume) or different (re-snapshot).
   Whether the cache slot was cleared in between.

---

## Order-book sufficiency thresholds

Empirical lower bounds for "tradeable book" gates. Status: **PROBE
PENDING**.

The `book_sufficiency_baseline` probe runs `orderbook_liveness`'s collector
for 2 minutes on each venue and reports:

- *Levels at touch* — count of price levels within 1 bps of the touch.
- *Total qty within 5 bps* — depth available without slipping more
  than 5 bps from mid.
- *Crossed-book frequency* — fraction of snapshots where bids cross asks.
- *Empty-side frequency* — fraction with bids==[] or asks==[].

Outputs feed proposed engine constants:

- `MIN_LEVELS_FOR_SLICE` — minimum number of levels on the consumed
  side before binary-searching depth. Default proposal: 5.
- `MAX_BOOK_AGE_MS` — staleness guard. Default proposal: 2000.
- `MAX_SPREAD_BPS_PER_LEG` — sanity fence. Default proposal: 50.

These are tunable; the empirical measurement informs the defaults.

---

## Rate-limit / network notes

- **Singapore-region IP required**. Scanner field notes verified that
  every venue 451s/`ExchangeNotAvailable`s on non-Asian residential
  IPs. Engine inherits this. The Singapore DigitalOcean droplet at
  `/opt/Cyborg-Arbitrageur/` (production) reaches all 14 cleanly.
  NordVPN-Singapore from local works for probing but adds 2–15× latency.

- **`enableRateLimit=False`** by design (`config.py:13`). The Engine
  does NOT throttle. We rely on the exchange to reject overruns; we
  blast and absorb the rejection. PROBE PENDING:
  - Per-venue 429/418 thresholds during sustained burst.
  - Whether 429 carries a `Retry-After` header that CCXT respects.
  - Whether IP banning kicks in at sustained-load thresholds, and the
    cooldown window for unbanning.

- **Windows + aiohttp + aiodns + VPN**. Scanner field notes: aiohttp
  default DNS resolver fails inside VPN tunnels; fix is
  `aiohttp.ThreadedResolver()`. Engine does NOT have this fix in
  `config.get_exchange()`. PROBE PENDING — does the engine connect
  successfully on a Windows + NordVPN-Singapore developer station?
  If not, the same patch from `Arb-Scanalytics/config.py:open_client`
  ports cleanly here.

- **`fetch_time` heartbeat** (`engine.py:64`). 30 s interval. PROBE
  PENDING:
  - Does this prevent the REST socket pool from closing on every
    venue? aiohttp default keep-alive varies; some load balancers
    terminate idle TCP at 60 s.
  - Does CCXT's REST client share a connection pool with WS, or are
    they independent?

---

## Crash recovery and state drift

`positions.json` is the engine's only durable state. `pre_warm()`
(`engine.py:86`) reconstructs `ExecutionPair` objects from saved leg
specs and live CCXT markets, with drift detection on `multiplier` and
`contract_size`.

Drift modes empirically possible:

1. **Venue rotates `contractSize`** on a symbol mid-session. CCXT's
   loaded markets reflect new value; saved state has old. Drift
   detection logs `PREWARM_WARNING` but engine continues with live
   value. Position quantity stored (in 1× base) stays correct *if
   the venue did not also rotate the position itself* — a
   contract-size rotation usually coincides with an open-positions
   migration on the venue side. PROBE PENDING: any historical
   instances of contract-size rotation on our 13 venues.

2. **Venue rotates symbol prefix**. e.g. `1000CHEEMS` → `10000CHEEMS`
   when the underlying coin's price falls 10×. CCXT's `markets` dict
   reflects the new symbol; the saved state references the old
   symbol → `KeyError` in `pre_warm`. Engine logs
   `PREWARM_ERROR: Position orphaned`.

3. **Position drift**. Operator manually closes position via venue UI;
   `positions.json` still reflects the full amount. Subsequent exit
   IOCs hit reduceOnly rejections.

   *Mitigation roadmap*: a Class-4 `position_drift_watchdog` that
   periodically calls `fetch_positions()` per venue and reconciles
   against `positions.json`. Diffs > dust trigger a Pushover P1.

4. **Asymmetric exchange-side residual after recovery**. Already
   loud-logged in `execution.py:637-642`. PROBE PENDING: what's the
   typical magnitude across our 13 venues? Is the dust threshold
   (`max(leg.min_lot)`) sufficient, or do we want a separate
   `recovery_residual_alert` threshold an order of magnitude tighter?

---

## Engine refactor roadmap

Driven by the cardinal-concerns analysis above. None of these are
in-scope for this pass — they're sequenced for incremental adoption
once the probes have verified their underlying assumptions.

### R1 — `FillReceipt` primitive (highest priority)

Captures resolved fill state. Replaces the scattered
`receipt.get('filled')` + `_fill_vwap` calls in `execution.py`.

- New file: `primitives.py` add `FillReceipt` dataclass.
- New module: `receipt_resolver.py` with one resolver per
  R-Mode (`sync_final`, `sync_zero`, `sync_null`, `eventual`).
- Resolver dispatch keyed off the `Receipt shape catalog` table.
- `dispatch_ioc_pair` returns `(FillReceipt, FillReceipt)` instead of
  raw CCXT receipts.
- `recover_imbalance` accepts `FillReceipt` for `base_filled_long` /
  `base_filled_short` instead of pre-extracted floats.
- `_fill_vwap` is removed; its logic lives inside the resolver.

Unblocks: every other refactor. Until the engine knows whether a
receipt is fill-resolved, none of the downstream logic is safe.

### R2 — `BookSnapshot` primitive

Replaces the bare-dict cache. Adds liveness metadata.

- `primitives.py` adds `BookSnapshot` dataclass.
- `engine.watch_order_book_loop` writes `BookSnapshot(received_ts_ms=...)`
  on every yield.
- `engine.order_books: dict[(str, str), BookSnapshot]`.
- `project_slice` accepts `BookSnapshot` and asserts liveness +
  sufficiency before walking depth.
- Constants `MAX_BOOK_AGE_MS`, `MIN_LEVELS_FOR_SLICE`,
  `MAX_SPREAD_BPS_PER_LEG` introduced and tuned per-venue once
  baselines exist.

Unblocks: silent-staleness alerts, the watchdogs.

### R3 — `staleness_watchdog` (Class-4 watchdog)

Periodic task that scans `engine.order_books` and emits Pushover P1
on any (ex_id, symbol) where `BookSnapshot.received_ts_ms` falls
behind the per-venue `MAX_BOOK_AGE_MS × 5` threshold during an
otherwise-unloaded interval. Distinguishes "engine intentionally
unsubscribed" from "stream silently died" via the engine's own
`_book_tasks` map.

### R4 — `position_drift_watchdog` (Class-4 watchdog)

Every N minutes (default 5), calls `fetch_positions()` on every
venue with an open position in `positions.json`, normalizes via
`ExecutionLeg.to_base_qty`, and diffs against the stored
`amount_base`. Diffs above the pair's dust floor → Pushover P1.

### R5 — Warmup probe battery

The `LEG FINGERPRINT` line is the seed. Extend `handle_warmup` to
emit additional one-shot probe outputs for the specific (venue,
symbol) being warmed:

- *IOC param-shape lookup* — reads the *Receipt shape catalog* and
  prints which resolution path the engine will take for this venue.
  Catches "this venue is sync-null but we haven't run probes yet".
- *Cross-leg basis sanity* — print the top-of-book basis_bps. If it's
  greater than ±100 bps, something is wrong (likely a unit mismatch
  not caught by `LEG FINGERPRINT`).
- *Book health baseline* — depth at top 5 levels per leg, spread bps,
  delta cadence over the last 2 s. Operator-eyeballable.

These extensions live in `engine_warmup_probes.py` (proposed) and are
called from the existing `handle_warmup` after first snapshots land.

---

## Probes worth re-running

| Cadence            | Probe                          | Why                                                                                            |
|--------------------|--------------------------------|------------------------------------------------------------------------------------------------|
| Once per CCXT bump | `capabilities`                 | `c.has` flags shift between releases; new verbs become available, old ones get renamed.        |
| Once per CCXT bump | `ioc_honor`                    | CCXT param-unification regressions are real; verify before any live trade.                     |
| Once per CCXT bump | `receipt_shape`                | Receipt parsers in CCXT change shape between releases; the silent-fill matrix can shift.       |
| Quarterly          | `orderbook_liveness`                  | Venue-side WS infra rotates; tick cadence and reconnect behavior drift slowly.                 |
| Quarterly          | `set_leverage_idempotent`      | Margin-mode and position-mode defaults change as venues add features (cross/isolated, hedge).  |
| Pre-deploy         | `cross_venue_smoketest`        | Capital-at-risk; runs once before any new venue or new symbol gets traded for real.            |
| Continuous         | All Class-4 watchdogs          | Live during engine operation. Existence is the safety net.                                     |

---

## Open empirical questions

The list of "I do not know yet but the engine will eventually trip
over this." Each item is a probe candidate; the priority ordering
reflects blast-radius if the assumption is wrong.

1. **(P0) Bybit `sync-null` blast radius**. Verified for
   `BTC/USDT:USDT`. Does it apply to every Bybit USDT-linear perp,
   or only certain symbols? What about `fetch_order(id)` — does it
   return a fill-resolved receipt synchronously, or does it ALSO
   require waiting for venue-side propagation? If the latter, the
   `sync-null` resolver needs a `wait_for_resolved` loop with backoff.

2. **(P0) Position-mode invariant per venue**. Does our `.env` set
   of API keys land us in one-way mode universally, or does any venue
   default to hedge mode and silently bifurcate positions? BingX is
   the prime suspect.

3. **(P0) `reduceOnly` honor on every venue**. Does an IOC with
   `reduceOnly=true` actually become a no-op when the position is
   already flat, or does it silently OPEN a counter-position on
   any venue?

4. **(P1) Min-notional vs min-lot**. CCXT exposes `market.limits.amount.min`
   (lot size) but venues also enforce min-notional in USDT. The engine's
   `_min_base_for_leg` only consults the lot-size limit. PROBE: does
   any venue reject an IOC at min-lot because the notional is below
   their min-notional floor?

5. **(P1) `amount_to_precision` rounding direction**. CCXT can be
   configured for `TRUNCATE`, `ROUND`, `ROUND_UP`. Defaults vary.
   The engine assumes the rounded value is always tradeable — if a
   venue uses `ROUND_UP` and the binary-search ceiling is exactly at
   min lot, rounding up exceeds the ceiling and the IOC fails.

6. **(P2) Funding-settlement-boundary fill behavior**. What happens
   when the engine dispatches an IOC within ±10 ms of a funding
   settlement on either venue? Some venues halt order matching for
   ~1 s during settlement; behavior is unspecified.

7. **(P2) Order-book reconnect snapshot semantics**. After a forced
   socket close, does the cached book hold stale or get cleared?
   Determines whether the engine should null-out
   `engine.order_books[(ex_id, symbol)]` on the exception path in
   `watch_order_book_loop`.

8. **(P2) Maximum sustained IOC dispatch rate per venue without
   429/418**. Engine fires 2 IOCs every 1.5 s = 1.33 IOCs/s steady
   state; bursts around recovery can hit 4 IOCs/s briefly. PROBE
   PENDING: per-venue sustained-burst thresholds.

9. **(P3) Clock skew engine ↔ venue**. CCXT timestamps mixed with
   `time.monotonic()` deadlines in `run_slicing_loop`. Skew above
   500 ms causes the staleness watchdog to fire false positives.

10. **(P3) WebSocket connector pool exhaustion**. 13 venues × 2 WS
    streams (book + positions, future) × 5 active pairs = 140
    concurrent WS connections. Aiohttp connector defaults handle
    this, but PROBE PENDING what happens at peak.

---

## Probe operational notes

- **Probe outputs land in `probe_logs/`** (gitignored). Each probe
  run writes a JSONL file timestamped at run start. The field-notes
  tables are *generated* from these JSONL files, not hand-edited.

- **Class-3 (capital-at-risk) probes require explicit per-run
  authorization**. The CLI flag is
  `--I-AM-FUNDED-AND-AUTHORIZED-FOR-{VENUE}`. The flag's name is
  deliberately ungrammatical; it's friction by design. No env-var
  shortcut, no implicit default — the operator types it every run.

- **Dry-run mode** is on by default for any probe whose name starts
  with `live_` or `min_lot_`. The probe prints the order it WOULD
  send and exits. The capital-at-risk flag flips dry-run off.

- **Probe fairness**. Probes run sequentially across venues to avoid
  burst-rate-limit interference between probes. Within a probe,
  per-venue calls are concurrent (`asyncio.gather`).

- **Singapore VPS only**. Run probes against Singapore VPS or
  NordVPN-Singapore. Local non-VPN probing will hit
  `ExchangeNotAvailable` on every venue and produce nothing.

---

## Day-to-day execution ops

### Run the introspection probes

```bash
cd /opt/Cyborg-Arbitrageur
python3 engine_probes.py capabilities
python3 engine_probes.py orderbook_liveness BTC/USDT:USDT
```

Both are zero-side-effect; safe to run any time, including against the
live engine's CCXT instances (each probe opens its own short-lived
clients, separate from the engine's).

### Run the safe-by-construction probes

```bash
python3 engine_probes.py ioc_honor BTC/USDT:USDT
```

Places real far-from-spread IOCs that auto-cancel. Costs zero capital
but consumes rate-limit budget on the venues being probed. Run during
quiet hours; do not run mid-trade.

### Run the capital-at-risk probes

```bash
python3 engine_probes.py min_lot_live BTC/USDT:USDT \
    --venue binance \
    --I-AM-FUNDED-AND-AUTHORIZED-FOR-BINANCE
```

One venue per invocation; explicit authorization. Probe places a
min-lot IOC at top-of-book, captures the receipt, and immediately
unwinds the resulting position with a reduceOnly IOC. If the unwind
fails for any reason, the probe halts and pages Pushover P2 — the
operator must manually close the residual.

### Inspect probe outputs

```bash
ls -la probe_logs/
jq '.' probe_logs/receipt_shape_2026-05-09T*.jsonl
```

The JSONL format is one record per (probe, venue) cell. Re-run a
probe and the new file appends; the field-notes tables can be
regenerated by selecting the latest record per cell.

---

## Verified findings — probe runs (2026-05-09)

Two probe-run cycles on 2026-05-09. The second cycle ran with three
CCXT-bypass patches applied (`ccxt_patches.py` for bitmart .lower()
guard; `config.py` for htx UTA-route; `engine_probes.py` for
kucoinfutures `marginMode: cross` and mexc `type: 3`). Latest
authoritative JSONL artifacts:

- `probe_logs/capabilities_2026-05-09T05-46-23Z.jsonl`
- `probe_logs/orderbook_liveness_2026-05-09T04-03-41Z.jsonl` (unchanged)
- `probe_logs/ioc_honor_2026-05-09T05-50-02Z.jsonl`
- `probe_logs/receipt_shape_2026-05-09T05-53-58Z.jsonl`

### Capabilities probe — 13/13 authenticate

All 13 venues now pass `fetch_balance`. The htx v3-endpoint route
required `client.options['fetchBalance']['uta'] = True`; that's now
applied automatically in `config.get_exchange()` for htx.

### IOC honor probe — 12/13 verified honored

| Verdict                  | Venues                                                                                                |
|--------------------------|-------------------------------------------------------------------------------------------------------|
| **VERIFIED HONORED**     | binance, bingx, bitget, bitmart, bybit, coinex, gate, htx, kucoinfutures, mexc, okx, xt              |
| **WALKED AWAY**          | phemex (venue-side `39999` opaque rejection — see below)                                              |

Two new venue-specific bypasses discovered in this turn:

1. **kucoinfutures `marginMode: cross`** — venue rejects with
   `330005 The order's margin mode does not match the selected one`
   unless `marginMode` is supplied per-order. CCXT does not infer it
   from `c.options`. Empirical proof in `ccxt_bug_repro.py`. Captured
   in `engine_probes.py:_VENUE_CREATE_ORDER_PARAM_OVERRIDES`.

2. **mexc `type: 3`** — CCXT 4.5.51's `create_swap_order` (`mexc.py:2382`)
   does NOT translate `timeInForce: 'IOC'` into mexc's integer
   `type=3`. The translation only exists in `create_spot_order_request`
   (mexc.py:2319-2326). Without the override, mexc swap orders REST as
   regular limits — the engine would have silently accumulated
   exposure. Empirical proof in `ccxt_bug_repro.py`. Same captured
   in the override map.

### Receipt shape probe — sync-null is the dominant pattern

12/13 venues classified. **8 are sync-null** (placement returns id only;
`fetch_order(id)` is mandatory): bitget, bitmart, bybit, htx,
kucoinfutures, mexc, okx, xt. Four are sync-zero (placement
authoritative): binance, bingx, coinex, gate. Phemex walked away. See
Table B for the full per-venue gained-fields catalog.

### CCXT bug fixes captured in code

| Bug                                                              | Fix location                                                                              |
|------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| bitmart `handle_errors` AttributeError on missing `message`      | `ccxt_patches.py::PatchedBitmart` — drop-in subclass with guarded `.lower()`              |
| htx `fetch_balance` calling deprecated v1 endpoint               | `config.py::_htx_route_to_v3_unified` — sets `client.options['fetchBalance']['uta']`     |
| kucoinfutures `create_order` margin-mode mismatch                | `venue_overrides.py::VENUE_IOC_LIMIT_PARAMS['kucoinfutures']`                             |
| mexc swap `create_order` silently dropping `timeInForce: 'IOC'`  | `venue_overrides.py::VENUE_IOC_LIMIT_PARAMS['mexc']`                                      |
| bybit `fetch_order` requiring `acknowledged: True`               | `venue_overrides.py::VENUE_FETCH_ORDER_PARAMS['bybit']`                                   |
| bitmart `set_leverage` requiring `marginMode`                    | `venue_overrides.py::VENUE_SET_LEVERAGE_PARAMS['bitmart']`                                |
| mexc `set_leverage` requiring `openType` + `positionType`        | `venue_overrides.py::VENUE_SET_LEVERAGE_PARAMS['mexc']` (two-call fan-out)                |
| bingx `set_leverage` requiring `side` ∈ {LONG,SHORT,BOTH}        | `venue_overrides.py::VENUE_SET_LEVERAGE_PARAMS['bingx']`                                  |
| kucoinfutures `fetch_order` 100001 OrderNotFound on fresh IOC    | `receipt_resolver.py::_fetch_order_resilient` retry-on-OrderNotFound (Phase 1)            |

The first two ship in the engine's instantiation path
(`config.py` + `ccxt_patches.py`) and apply to BOTH probes and engine
runtime. The remainder live in `venue_overrides.py` (single source of
truth) and `receipt_resolver.py`; both are imported by `engine_probes.py`
AND `execution.py` so probes and runtime see the same overrides.

### Operator-action queue (1 venue walked away)

#### phemex — walked away from v1 catalog

Operator regenerated the phemex API key with explicit Contract/Futures
permissions enabled, IP-whitelisted the VPS, and confirmed USDT in the
Contract Trade Account specifically. Re-ran the probes 2026-05-09 07:16
— the venue continues to return `39999 Error in place order` with
`data: null` and no actionable diagnostic.

The CCXT-verbose request body is well-formed (captured 2026-05-09):

```json
{"symbol":"BTCUSDT","side":"Buy","ordType":"Limit","clOrdID":"CCXT…",
 "posSide":"Merged","orderQtyRq":0.001,"priceRp":"…","timeInForce":"IOC"}
```

Probed variations (`posSide: 'Merged'`, `marginMode: 'cross'`, custom
`clientOrderId`, SELL side, post-`set_margin_mode`) all hit the same
opaque rejection. No CCXT-side bypass is possible from probe data
alone.

**Decision**: phemex is excluded from the v1 verified catalog.
12 verified venues is a sufficient empirical baseline to build the
primitives. Re-add phemex when an operator-side root cause is
identified — likely via a phemex support ticket correlating the
`clOrdID` to a venue-side rejection reason.

`venue_overrides.VENUE_R_MODE['phemex']` is set to `None` (unverified);
the resolver's `requires_fetch_order()` defaults unknown venues to
True, so if phemex ever returns a placement, the receipt resolver will
treat it conservatively (always fetch_order).

### Per-venue set_leverage overrides — Table D

Discovered 2026-05-09 during the cross_venue_smoketest XRP runs (Test 2:
`mexc × bitget`; Test 3: `htx × bitmart`). Every venue listed here
raised `ArgumentsRequired` from CCXT's client-side validator on a stock
`set_leverage(leverage, symbol)` call. The map below is the single
source of truth; `engine.py::_set_leverage_for_leg` consumes it.

Verified 2026-05-10 by surveying all 13 venues with stock
`set_leverage(1, 'XRP/USDT:USDT')` — no params. The `Stock outcome`
column captures what CCXT did with no override; the `Engine override`
column records what we ship.

| Venue           | Stock outcome                                                                  | Engine override (`VENUE_SET_LEVERAGE_PARAMS`)                                                                                                                                                                                          |
|-----------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mexc`          | ArgumentsRequired (openType + positionType)                                    | `[{openType: 2, positionType: 1}, {openType: 2, positionType: 2}]` — two-call fan-out. `openType=2` is cross. `positionType` "ignored when position open in cross" per docs but CCXT validates client-side. Both directions covered.   |
| `bitmart`       | ArgumentsRequired (marginMode)                                                 | `[{marginMode: 'cross'}]`. CCXT bitmart.py:4580 hard-fails without it.                                                                                                                                                                  |
| `bingx`         | ArgumentsRequired (side ∈ {LONG, SHORT, BOTH})                                 | `[{side: 'BOTH'}]`. `BOTH` targets the unified position in one-way mode. If operator switches account to hedge mode, expand to `{LONG}` + `{SHORT}`.                                                                                   |
| `xt`            | ArgumentsRequired (positionSide ∈ {LONG, SHORT})                               | `[{positionSide: 'LONG'}, {positionSide: 'SHORT'}]` — two-call fan-out. Unlike BingX, XT has NO "BOTH" option; per-direction calls always required. Anchor: 2026-05-10 bingx × xt smoketest.                                            |
| `bybit`         | BadRequest 110043 "leverage not modified"                                      | `[{}]` + `is_benign_warmup_error` classifier swallows the idempotency-as-error. (Anchor: 2026-05-10 binance × bybit warmup; classifier in `VENUE_BENIGN_WARMUP_ERROR_SIGNATURES`.)                                                       |
| `binance`       | Success                                                                        | `[{}]` — stock CCXT works.                                                                                                                                                                                                              |
| `bitget`        | Success                                                                        | `[{}]` — stock CCXT works.                                                                                                                                                                                                              |
| `coinex`        | Success                                                                        | `[{}]` — stock CCXT works.                                                                                                                                                                                                              |
| `gate`          | Success                                                                        | `[{}]` — stock CCXT works.                                                                                                                                                                                                              |
| `htx`           | Success                                                                        | `[{}]` — CCXT defaults to cross when marginMode unset (htx.py:7262).                                                                                                                                                                    |
| `kucoinfutures` | Success                                                                        | `[{}]` — routes through `set_contract_leverage` with cross default. (Note: `create_order` DOES need `marginMode: cross` per VENUE_IOC_LIMIT_PARAMS — separate quirk.)                                                                    |
| `okx`           | Success                                                                        | `[{}]` — CCXT defaults to cross when marginMode unset (okx.py:6601). If operator switches to hedge mode, will need `posSide` ∈ {long, short, net} per CCXT okx.py:6609.                                                                  |
| `phemex`        | Success                                                                        | `[{}]` — but venue is on the walked-away list; placeholder for v2 catalog.                                                                                                                                                              |

A `[{}]` row means the venue takes a single empty-dict call — no
override required, but routed through the same fan-out helper for
uniformity. The fan-out shape (list of dicts) accommodates mexc's
positionType requirement without forcing every other venue into the
same shape.

### Cross-leg dispatch-floor discipline — composite per-cycle floor

The slicing loop's "minimum dispatchable size" is a composite of
THREE constraints that must hold simultaneously on both legs:

  1. **Lot floor** — `max(both legs' min_lot_base)`. Static; below
     this, no leg can place any order. CCXT-published from
     `market.limits.amount.min × multiplier × contract_size`.

  2. **Notional floor** — `max(both legs' min_notional_usdt) /
     mid_price_base`. Price-dependent; below this in BASE, the
     venue rejects with InsufficientNotional even though the
     lot-size check passes. CCXT exposes
     `market.limits.cost.min`; 9 of 13 venues return None and we
     fall back to `venue_overrides.DEFAULT_MIN_NOTIONAL_USDT = 5.0`
     (empirically validated against MEXC's venue-side rejection).

  3. **Snap-safe rounding** — composite floor rounded UP to the
     next multiple of `max(both legs' base step)`. Without this, the
     symmetric snap (which rounds DOWN to a multiple of the larger
     step) could drop the dispatched size below the composite floor
     by up to one step, causing venue-side notional rejection.

**Convention** (enforced in `execution._compute_dispatch_floor_base`):

```
lot_floor      = max(lot_long, lot_short)                  # base
notional_floor = max(notional_long_usdt, notional_short_usdt) / mid_price_base
composite      = max(lot_floor, notional_floor)
max_step       = max(step_long, step_short)                # base
dispatch_floor = ceil(composite / max_step) * max_step
```

`dispatch_floor` is computed PER CYCLE (notional component is
price-dependent). Used as both the loop's halt-on-dust threshold
and as `min_dispatch_base` for `project_slice`. The lot-only
floor is preserved in `engine._pair_dust` for the SLICE START
log line and any caller that needs the static value.

**Symmetric dispatch** (enforced in `execution._compute_symmetric_dispatch`):

  Given `dispatch_base` from `project_slice` (always ≥ dispatch_floor),
  iteratively compute the largest base quantity ≤ `dispatch_base`
  such that `amount_to_precision(qty / base_per_native) *
  base_per_native` is the same on both legs. Algorithm:

    current = dispatch_base
    repeat (≤ 3 times):
        nl = ex_long.amount_to_precision(current / long.base_per_native)
        ns = ex_short.amount_to_precision(current / short.base_per_native)
        next = min(nl * long.base_per_native, ns * short.base_per_native)
        if next == current: break
        current = next

  Monotonically non-increasing — converges in ≤ 2 iterations for
  the 12 verified venues (all precision steps are integer multiples
  of one another). The snap-safe rounding in `dispatch_floor`
  guarantees the converged value satisfies the notional gate.
  Rare non-commensurate venue pairings get a `DISPATCH_WARNING`
  log and rely on recovery for any leftover asymmetry.

**Recovery dust convention** (enforced in `execution.recover_imbalance`):

  Dust threshold is the **target leg's COMPOSITE floor**:
  `ceil(max(target_lot, target_notional_usdt / mid_price_base) /
  target_step) * target_step`. NOT the pair-max — the pair-max was
  wrong: an imbalance smaller than the larger leg's min but
  tradeable on the smaller leg got dropped silently (2026-05-09
  KuCoin × OKX anchor). And NOT just lot — recovery's market
  order also has to clear the venue's min-notional and survive
  precision rounding (2026-05-10 mexc × bitget anchor).

  When a residual is below the target leg's composite floor,
  recovery emits a `WARNING`-level log with the residual amount,
  the venue holding the over-fill, the floor breakdown, and an
  explicit "Manual reconciliation required" marker. No silent leaks.

### Per-venue min-notional — Table E

CCXT survey 2026-05-10 across 13 venues × {XRP, BTC} perps. The
`cost.min` column reads `market.limits.cost.min` directly from each
venue's CCXT metadata. **Engine-effective floor** applies the
fallback in `venue_overrides.min_notional_usdt_for`.

| Venue           | CCXT cost.min XRP | CCXT cost.min BTC | Engine-effective USDT floor                                |
|-----------------|-------------------|-------------------|------------------------------------------------------------|
| `binance`       | 5.0               | 50.0              | CCXT-published (per symbol)                                |
| `bingx`         | 2.0               | 2.0               | CCXT-published                                             |
| `bitget`        | 5.0               | 5.0               | CCXT-published                                             |
| `xt`            | 5.0               | 10.0              | CCXT-published                                             |
| `bitmart`       | None              | None              | DEFAULT 5.0                                                |
| `bybit`         | None              | None              | DEFAULT 5.0                                                |
| `coinex`        | None              | None              | DEFAULT 5.0                                                |
| `gate`          | None              | None              | DEFAULT 5.0                                                |
| `htx`           | None              | None              | DEFAULT 5.0                                                |
| `kucoinfutures` | None              | None              | DEFAULT 5.0                                                |
| `mexc`          | None              | None              | DEFAULT 5.0 — venue empirically enforces 5 USDT (anchor)   |
| `okx`           | None              | None              | DEFAULT 5.0                                                |
| `phemex`        | None              | None              | DEFAULT 5.0 (walked-away venue; placeholder)               |

**Why the default exists**: MEXC's CCXT metadata returns
`cost.min = None`, but the venue rejected our 2-XRP slice ($2.84
notional) with "Cannot be less than the minimum order amount 5 USDT".
The default of 5 is the strictest empirically-observed floor on the
venues that don't publish.

**Why per-venue overrides exist** (`VENUE_MIN_NOTIONAL_USDT`):
empty by default — populate when a specific venue's actual floor is
empirically observed to diverge from 5 USDT. Don't speculate; add
based on rejection logs.

### Per-venue WS book-level shape — `[price, amount, ...]` convention

CCXT Pro normalizes the spot path's L2 levels to `[price, amount]`
(2-element lists), but several swap adapters preserve the venue's
native shape, which adds extra fields per level. The engine MUST use
`level[0]` (price) and `level[1]` (amount) by index, never tuple-unpack:

| Venue          | Swap WS level shape       | Source                                              |
|----------------|---------------------------|-----------------------------------------------------|
| `mexc`         | `[price, amount, count]`  | CCXT pro/mexc.py:799-811 (`handle_order_book`)      |
| (others)       | `[price, amount]`         | (default CCXT-Pro normalization)                    |

Anchor: 2026-05-09 22:15:53 mexc × bitget cross_venue_smoketest.
Engine's `vwap_by_depth` did `for price, size in levels:` and crashed
with `too many values to unpack (expected 2)` 1ms after the SLICE
START log. Fix: index-access in `vwap_by_depth`. Tolerates any
level length ≥ 2 without per-venue branching — applies uniformly to
all 13 venues.

### Cycle invariant — symmetric-only commitment

The slicing loop's invariant is: every cycle that returns to the loop
top must have left the venue position state SYMMETRIC across both
legs. Concretely, after `dispatch_ioc_pair` + `recover_imbalance`:

```
cycle_residual_base = abs(cycle_qty_long_base - cycle_qty_short_base)
```

If `cycle_residual_base >= min(both legs' min_lot_base)`, the cycle
LEFT naked exposure on the over-fill venue. Continuing the loop
would compound that exposure on subsequent cycles. The loop
**HALTS** with `halt_reason="asymmetric_residual"` and
`engine.py::handle_entry/handle_exit` fires a Pushover P2 with the
exact per-venue exposure breakdown.

Three failure modes caught:

  1. **Recovery refused** — `recover_imbalance` returned `None` because
     the residual was below the target leg's composite floor. Old behavior:
     WARNING + continue. New behavior: WARNING + halt (residual is
     sub-tradeable on target leg but still real exposure).
  2. **Recovery underfilled** — recovery dispatched but venue partial-filled.
     Old behavior: WARNING + continue. New behavior: WARNING + halt.
  3. **Recovery succeeded fully** — `cycle_qty_long_base ==
     cycle_qty_short_base`. No halt; loop continues.

The cumulative state (`cumulative_qty_long_base`,
`cumulative_qty_short_base`) is updated BEFORE the halt break so the
LoopResult's per-leg quantities accurately reflect the venue state
the operator must reconcile.

**Anchor**: 2026-05-10 02:52 bingx × xt smoketest. XT's IOC SELL
returned `filled=0` (likely positionMode/funding mismatch). Engine
WARNED on the recovery underfill and CONTINUED — adding another 10
XRP naked long on BingX before XT finally errored with
`insufficient_balance` two cycles later. With this halt, the FIRST
asymmetric cycle stops the loop; the operator deals with 10 XRP of
exposure, not 20+.

### Per-venue benign-warmup-error signatures — Table F

Several venues raise an exception when a warmup-time configuration
call is invoked with the value already in effect. The "error" is
the venue punishing the engine for redundant safety; the actual
state matches the engine's intent. These are HARMLESS but the
classification has to be venue-specific because there's no
universal CCXT signal for "already-set vs genuine failure."

The architecture: `engine.py::_set_leverage_for_leg` catches the
exception, asks `venue_overrides.is_benign_warmup_error(venue,
exc)`, and either logs+skips (benign) or re-raises (genuine).
`venue_overrides.VENUE_BENIGN_WARMUP_ERROR_SIGNATURES` is the
single source of truth — one entry per venue × signature, each
keyed on a stable substring of `str(exception)`. Substring
matching avoids JSON-parsing the exception body (CCXT version
churn) while remaining language-agnostic (numeric error codes
don't localize).

| Venue           | Signature substring         | Description                          | Anchor                                      |
|-----------------|-----------------------------|--------------------------------------|---------------------------------------------|
| `bybit`         | `"retCode":110043`          | leverage not modified                | 2026-05-10 binance × bybit warmup           |
| (others)        | TBD — populate empirically  | —                                    | —                                           |

**Discovery path**: when the operator hits a `WARMUP_ERROR` on a
leverage / margin-mode call, capture the exception string. If it
indicates idempotency (e.g., "not modified", "no change", "already
set"), add the entry to `VENUE_BENIGN_WARMUP_ERROR_SIGNATURES` and
re-warm. Add ONLY signatures verified to be idempotency-related —
NEVER add signatures for genuine failures (insufficient margin,
account frozen, etc) just to make warmup pass.

**Roadmap probe**: `set_leverage_idempotent` (Class 2 stub,
engine_probes.py:1216) — when implemented, will systematically
surface the idempotency-as-error pattern across all 13 venues
and populate this table empirically.

### Per-venue fetch_order eventual-consistency timings — Table H

Empirical data from the 2026-05-10 `fill_resolution` Class-3 sweep
across all 8 sync-null venues. Each measurement: place a small SELL
IOC at the BID (will fill), poll fetch_order at 100ms intervals,
record the timestamp at which fetch_order first returned an
authoritative (non-stale, non-OrderNotFound) response.

| Venue           | First authoritative fetch_order | Pre-resolution signature                 | Engine config                    |
|-----------------|---------------------------------|------------------------------------------|----------------------------------|
| `bybit`         | +54 ms                          | (none — first call worked)               | DEFAULT 1.0s cushion             |
| `okx`           | +55 ms                          | (none — first call worked)               | DEFAULT 1.0s cushion             |
| `htx`           | +93 ms                          | (none — first call worked)               | DEFAULT 1.0s cushion             |
| `bitget`        | +96 ms                          | (none — first call worked)               | DEFAULT 1.0s cushion             |
| `bitmart`       | +148 ms                         | (none — first call worked)               | DEFAULT 1.0s cushion             |
| `mexc`          | +232 ms                         | (none — first call worked)               | DEFAULT 1.0s cushion             |
| `kucoinfutures` | +786 ms (after 3× OrderNotFound)| `OrderNotFound` raised                   | EXPLICIT 3.0s                    |
| `xt`            | +817 ms                         | all-None response (filled=None, status=None) | EXPLICIT 3.0s                |

Two distinct eventual-consistency signatures observed:

  1. **OrderNotFound raised** — KuCoin pattern. Order id is registered
     server-side but the order-by-id endpoint hasn't yet caught up.
     CCXT raises the typed exception. Resolver catches and retries.

  2. **All-None response** — XT pattern. fetch_order returns
     successfully but every fill-state field is None
     (`filled=None`, `status=None`, `info.executedQty=None`,
     `info.state=None`). The response shape is INDISTINGUISHABLE from
     the placement receipt. Resolver detects via the strict AND of
     `filled is None and status is None` (false positives avoided —
     `filled=0.0` + `status='canceled'` is a legitimate auto-cancel).

Both signatures are handled in `receipt_resolver._fetch_order_resilient`
under one bounded retry loop, controlled per-venue by
`venue_overrides.VENUE_FETCH_ORDER_INDEXING_LAG_S`.

The 1.0s default cushion catches transient spikes for the fast
cluster; explicit 3.0s for KuCoin and XT covers the known-slow
cluster with ~3.7× margin over the measured lag. On deadline
expiry both signatures escalate (CRITICAL log + RuntimeError) — the
resolver never silently treats a non-authoritative response as
filled=0, which would silently misclassify real fills.

### Per-venue fetch_order quirks beyond the R-Mode catalog

The R-Mode catalog (Table B) covers placement-receipt resolution mode.
This section captures secondary quirks of the `fetch_order(id)` call
itself — failure modes that look structural but are actually transient.

**kucoinfutures `OrderNotFound` (100001) on fresh IOC** — eventual
consistency on the order-by-id index.

  * **Anchor**: 2026-05-09 20:39:23 cross_venue_smoketest XRP run on
    `kucoinfutures × okx`. IOC dispatched at 20:39:23.220; resolver's
    `fetch_order(id)` raised `OrderNotFound('kucoinfutures
    {"msg":"error.getOrder.orderNotExist","code":"100001"}')` 245 ms
    later. The order had partially filled (10 XRP visible in venue UI) —
    KuCoin had matched it but its order-by-id index was lagging.
  * **Mechanism**: CCXT routes `kucoinfutures.fetch_order` through
    `futuresPrivateGetOrdersOrderId` (kucoin.py:5591) → KuCoin endpoint
    `GET /api/v1/orders/{orderId}`. Per docs this serves both active and
    historical orders, so it's NOT a wrong-endpoint issue. KuCoin's
    backend index that this endpoint reads from is eventually consistent
    with the order-placement index — typical lag 50–500ms, observed at
    245ms in the anchor incident.
  * **Fix**: `receipt_resolver._fetch_order_resilient` Phase 1 — catch
    `OrderNotFound` and retry with exponential backoff for up to
    `fetch_order_indexing_lag_s_for(venue)` seconds. Per
    `venue_overrides.VENUE_FETCH_ORDER_INDEXING_LAG_S['kucoinfutures'] =
    3.0`, retry waits are 100, 150, 225, 337, 506, 759 ms — covers the
    observed lag with several quick attempts before slowing.
  * **What if the order genuinely doesn't exist?** After 3 s of retries
    the OrderNotFound propagates and the engine surfaces CRITICAL +
    Pushover P2. Bounded retry never silently masks a real missing-order
    failure.
  * **Other venues**: no other R-Mode-classified venue exhibits this
    behavior on the existing probe runs. The `VENUE_FETCH_ORDER_INDEXING_LAG_S`
    map's default is `0.0` — no retry — so OrderNotFound on a venue
    without an entry there propagates instantly.

### Catalog status: 12/13 venues, primitives green-lit

Tables A and B reflect 12 verified rows. Steps 3 (consolidate venue
overrides into `venue_overrides.py`) and 4 (build `FillReceipt` /
`BookSnapshot` / `receipt_resolver.py`) proceed against this empirical
baseline.

### Re-run cadence

Once per CCXT bump and once per .env or account-state change. The
JSONL logs accumulate one file per run; the latest per probe-name is
authoritative.

---

## Anchor incidents — the historical reference

A small dossier of real production failures, each tied to a probe
that would have caught it. Lives at the bottom of this document so
future-you can grep for the signature.

### 2026-05-07 — Bybit silent-fill on BTC/USDT:USDT IOC

**Signature**: `transaction.log` cycle dispatched cleanly, but every
fill field in the `bybit` receipt is `None`. The engine logs
`filled 0/0 (IOCs rejected entirely)` while the venue side actually
filled.

**Probe that would have caught it**: `receipt_shape` (Class 2). Place
a single far-from-spread IOC; capture the receipt; `bybit` would
return the same all-None shape, and the probe would log
`R-Mode: sync-null, fetch-order required` BEFORE the engine ever ran
a live trade.

**Refactor that fixes it**: `FillReceipt` primitive (R1).

**Lesson**: CCXT's "unification" is sometimes only structural — the
shape of the dict is consistent, but populating it is the venue's
responsibility, and venues differ. Unification is no substitute for
verification.

### 2026-05-09 — KuCoin Futures OrderNotFound on fresh IOC

**Signature**: `cross_venue_smoketest` XRP run on `kucoinfutures × okx`.
IOC pair dispatched at 20:39:23.220 with both legs returning order ids
from `create_order`. The receipt resolver's immediate `fetch_order(id)`
on the KuCoin leg raised:

```
OrderNotFound('kucoinfutures {"msg":"error.getOrder.orderNotExist","code":"100001"}')
```

at 20:39:23.465 (245 ms after dispatch). The engine raised
`RuntimeError` ("Receipt resolution failed") and halted the entry,
firing Pushover P2.

**The trap**: this LOOKS like a structural failure but isn't. The
KuCoin order had matched and partially filled (10 XRP visible in venue
UI). The OKX leg had filled fully (19 XRP). The asymmetric fill — 10
XRP long on KuCoin vs 19 XRP short on OKX — was an exchange-side
delta the engine never recovered because its halt fired BEFORE the
recovery path ran.

**Mechanism**: CCXT routes `kucoinfutures.fetch_order` through
`futuresPrivateGetOrdersOrderId` (kucoin.py:5591) which hits KuCoin's
`GET /api/v1/orders/{orderId}` endpoint. That endpoint serves both
active and historical orders by id — but its backing index is
eventually consistent with the order-placement index. The 245-ms gap
caught us in that consistency window.

**Probe that would have caught it (in retrospect)**: `min_lot_live`
(Class 3, roadmap stub) on kucoinfutures alone — single venue, real
fill, expose-the-receipt-resolution-path probe. The
`cross_venue_smoketest` itself caught it; the lesson is that
single-venue Class-3 verification before pair-trading on a new venue
would have surfaced this without the asymmetric-fill cleanup cost.

**Refactor that fixes it**: `_fetch_order_resilient` Phase 1 in
`receipt_resolver.py` — catch `OrderNotFound` and retry with
exponential backoff for up to `fetch_order_indexing_lag_s_for(venue)`
seconds. KuCoin Futures is configured for 3 s of retries (covers the
observed 245-ms lag with margin for transient spikes). Other venues
default to no retry, so genuine missing-order failures still surface
immediately on venues without this quirk.

**Lesson**: CCXT exception types tell you what the wire returned, not
what the venue's distributed-systems internal state actually is. A
fresh order id raising OrderNotFound is not the same as a fabricated
id raising OrderNotFound — the same exception class spans two
qualitatively different conditions, and the resolver must distinguish
via per-venue retry policy.

### 2026-05-09 — Silent 9-XRP residual on KuCoin × OKX (asymmetric dispatch)

**Signature**: `cross_venue_smoketest` XRP run on `kucoinfutures × okx`.
Smoketest reported `SMOKETEST PASS` and `position: cleared (in-memory=True,
on-disk=True)`. The operator's UI inspection found a 9 XRP naked short on
OKX that the engine never saw.

**The slicing-loop log evidence**:

```
[SLICE] IOC slice dispatch_base=19.00180693 ... limits long_native=1.42134 short_native=1.42 projected_basis=-9.43bps
[SLICE] filled long=10.00000000@1.4213400000 short=19.00000000@1.4200000000 realized_basis=-9.43bps recovered=False
```

Both IOCs filled fully against their own limits. But the long leg's
fill was 10 base, the short leg's was 19 base. `recovered=False`.

**Mechanism**:

  * Engine intended to dispatch 19.00 base on both legs.
  * KuCoin XRP perp: contract_size=10, native precision=1 (integer).
    `19 / 10 = 1.9 contracts`; `amount_to_precision(1.9)` truncated
    to `1.0` → 10 base dispatched.
  * OKX XRP perp: contract_size=100, native precision=0.01.
    `19 / 100 = 0.19 contracts`; `amount_to_precision(0.19)` kept as
    `0.19` → 19 base dispatched.
  * Both IOCs filled at their dispatched quantities. Net position:
    +10 long on KuCoin, -19 short on OKX. Asymmetric residual: 9 XRP
    naked short on OKX.
  * Recovery's pair-dust threshold was `max(KuCoin min=10, OKX min=1) =
    10`. Imbalance |9| < 10 → `recover_imbalance` returned `None`
    silently (no log; the function quietly no-op'd).
  * Position state's symmetric accounting `cycle_filled = min(qty_long,
    qty_short) = 10`. Engine recorded a 10-XRP delta-neutral entry.
    The 9-XRP asymmetric short was never reflected in `positions.json`.

**Probe that would have caught it (in retrospect)**: a venue-pair-aware
precision-audit probe that runs `_compute_symmetric_dispatch` against
small target sizes on a candidate pair and surfaces any base-quantity
discrepancy ≥ either leg's min-lot. None of the existing probes test
the cross-leg dispatch path; the smoketest WAS the catch — but only
caught it via UI inspection, not via engine state. Class-3 single-venue
`min_lot_live` probes would also miss this (no cross-leg interaction).

**Refactor that fixes it**:

  1. `execution._compute_symmetric_dispatch` — at slice dispatch,
     iteratively snap `dispatch_base` to the largest value such that
     post-precision base is identical on both legs. Eliminates the
     asymmetric-fill source.
  2. `execution.recover_imbalance` — dust threshold is now the
     target-leg's min-lot (not pair-dust), so partial-fill residuals
     that are tradeable on the smaller leg are recovered.
  3. When recovery is genuinely impossible (residual < target leg
     min-lot), `recover_imbalance` emits a `WARNING` log instead of
     returning silently — operator sees the leak signal at runtime.

**Lesson**: cross-leg invariants (lot precision, min-notional, fee
schedule) must be enforced AT THE DISPATCH SITE, not relied upon to
hold by accident. A "symmetric" intent expressed only in base units
becomes asymmetric the moment each leg applies its own precision
floor. The fix is to make the symmetric base survive both precisions
BEFORE the IOCs go on the wire.

### 2026-05-09 — MEXC swap `vwap_by_depth` 3-element level unpack

**Signature**: `cross_venue_smoketest` XRP run on `mexc × bitget`.
Slicing loop START fired cleanly; failure 1ms later:

```
[SLICE] Slicing loop START side=entry pair=XRP long=mexc:XRP/USDT:USDT ...
[CRITICAL] Structural failure during entry on XRP: too many values to unpack (expected 2)
```

**Mechanism**: MEXC's swap WS L2 stream ships `[price, amount, count]`
3-element levels (CCXT pro/mexc.py:799-811 `handle_order_book` docstring
shows `asks: [[39146.5, 11264, 1]]`). Engine's `vwap_by_depth` looped
`for price, size in levels:` — 2-tuple unpack on a 3-element list raises
`ValueError: too many values to unpack (expected 2)`. The exception
propagated up through `project_slice` → slicing loop → `handle_entry`
→ CRITICAL log + Pushover P2.

**Probe that would have caught it (in retrospect)**: an `orderbook_liveness`
extension that captures `len(book.bids[0])` per venue and surfaces any
length ≠ 2. Future probe target.

**Refactor that fixes it**: `vwap_by_depth` uses `level[0]` /
`level[1]` index access instead of tuple-unpack. Tolerates any level
length ≥ 2 — applies uniformly to all 13 venues without per-venue
branching.

**Lesson**: CCXT-Pro's normalization isn't venue-uniform on the swap
path. Treat L2 level lists as opaque sequences; access by index, never
by tuple-unpack. A single `for price, size in levels:` is a venue-
specific assumption masquerading as a universal pattern.

### 2026-05-10 — MEXC × Bitget min-notional rejection on residual slice

**Signature**: `cross_venue_smoketest` XRP run on `mexc × bitget`. Entry
filled three full-size slices then attempted a residual; both venues
rejected the same dispatch with InsufficientNotional / equivalent:

```
[SLICE] IOC slice dispatch_base=2.51240135 safe_ceiling_base=5.02480271 (haircut=×0.50)
[SLICE] Symmetric snap: 2.51240135 → 2.00000000 base
[CRITICAL] IOC dispatch failure | long=ERR: ExchangeError('mexc {"message":"Cannot be less than the minimum order amount 5 USDT"}') | short=ERR: InvalidOrder('bitget {"msg":"less than the minimum amount 5 USDT"}')
```

**Mechanism**:

  * Engine's `min_dispatch_base` was lot-only: `max(mexc_lot=1,
    bitget_lot=1) = 1` base. Lot check passed for the 2-XRP slice
    (2 ≥ 1).
  * Both venues enforce a 5 USDT min-notional, but neither publishes
    `market.limits.cost.min` — CCXT metadata returns `None` for both.
  * 2 XRP × $1.42 = $2.84 — well below 5 USDT. Both venues rejected
    venue-side. The structural-failure path fired
    Pushover P2 + halted entry.

**Probe that would have caught it (in retrospect)**: a `notional_floor`
introspection probe enumerating CCXT-published `limits.cost.min` per
venue and flagging None values for empirical follow-up. None of the
existing probes survey min-notional; the smoketest WAS the catch.

**Refactor that fixes it**: composite per-cycle dispatch floor in
`execution._compute_dispatch_floor_base` — combines lot floor +
notional floor (price-dependent) + snap-safe step rounding. Wired
into both the slicing-loop's halt-on-dust threshold AND
`project_slice`'s `min_dispatch_base` arg AND `recover_imbalance`'s
target-leg dust check. Per-venue notional resolution lives in
`venue_overrides.min_notional_usdt_for` with a 5-USDT default for
the 9 venues that don't publish.

**Why the snap-safe rounding matters**: a naïve composite floor of
3.52 base ($5/$1.42) on a step-0.1 venue (e.g., bybit) would let
`project_slice` return `lo_base=3.52`; the symmetric snap rounds DOWN
to 3.5 = $4.97 → still below floor → venue-side rejection. Ceiling
to 3.6 (next step) gives one snap-safe boundary above the floor.

**Lesson**: CCXT's "unification" of market limits is incomplete on
the swap path. `limits.cost.min` is published by ~30% of venues we
trade. The engine MUST carry a fallback default and enforce it
client-side — venue-side enforcement happens AFTER an order goes on
the wire and the rejection cascades into a structural-failure halt.
Never rely on CCXT-published constraints alone; validate against
empirical venue rejections and pin overrides in `venue_overrides.py`.

### 2026-05-10 — Bybit `set_leverage` 110043 idempotency-as-error

**Signature**: `cross_venue_smoketest` `binance × bybit` warmup. Bybit
threw on a leverage-already-set call:

```
[WARMUP] Setting leverage to 1x on 2 legs ...
[WARMUP_ERROR] Leverage setup failed for bybit:XRP/USDT:USDT:
  bybit {"retCode":110043,"retMsg":"leverage not modified",...}
SMOKETEST FAIL (warmup): status=400 ...
```

The operator's bybit account was already at 1x leverage on XRP
(from a prior smoketest). The engine's intent and the venue's state
matched perfectly — yet the venue raised an exception, and the
engine's strict "any leverage exception fails warmup" policy
blocked entry.

**Mechanism**: bybit's API (and several others — binance margin-mode
returns `-4046`, BingX has equivalents) treats redundant
configuration calls as errors. CCXT surfaces the venue payload
verbatim through `ExchangeError`. Without venue-aware classification,
every benign-idempotent error blocks warmup.

**Refactor that fixes it**:
  1. `venue_overrides.VENUE_BENIGN_WARMUP_ERROR_SIGNATURES` —
     per-venue list of `(substring, description)` tuples that match
     known idempotency-as-error payloads.
  2. `venue_overrides.is_benign_warmup_error(venue, exc)` —
     classifier; returns `(True, description)` for matches.
  3. `engine.py::_set_leverage_for_leg` — catches per call,
     consults the classifier, logs+skips on benign, re-raises on
     genuine failure.

The architecture keeps `engine.py` free of venue-specific string
parsing. Adding a new venue's signature is a single-line addition
to the override map.

**Why substring matching, not structured parsing**:
  * The numeric error code (`110043`) is the most stable signal —
    language-agnostic, doesn't shift across CCXT versions.
  * The full venue payload format DOES shift (CCXT 4.x has had
    several rewrites of `handle_errors` for bybit). JSON-parsing
    the exception body would couple us to a specific shape.
  * Substring matching on `"retCode":110043` is stable across both
    formats.

**Why classifier (not blanket "swallow all set_leverage errors")**:
genuine failures (insufficient margin, account frozen, KYC required,
leverage out of range) MUST surface to the operator. The classifier
explicitly enumerates known-benign cases — anything else propagates
loud. Default-to-fail is the safe direction.

**Lesson**: venues conflate "operation rejected" with "operation
unnecessary." The engine must distinguish via per-venue signature
catalog, not via heuristic string scanning. When in doubt, fail
loud — silently swallowing an unrecognized exception is how
capital gets quietly misaligned.

### 2026-05-10 — XT `set_leverage` requires positionSide fan-out

**Signature**: `cross_venue_smoketest` `bingx × xt` warmup. XT raised
on a stock-CCXT `set_leverage(1, 'XRP/USDT:USDT')` call:

```
[WARMUP_ERROR] Leverage setup failed for xt:XRP/USDT:USDT:
  xt setLeverage() requires a positionSide argument, one of (LONG, SHORT)
SMOKETEST FAIL (warmup): status=400 ...
```

**Mechanism**: CCXT xt.py:3934 calls `self.check_required_argument(
'setLeverage', positionSide, 'positionSide', ['LONG', 'SHORT'])`
client-side. XT's API expects per-direction calls regardless of
account mode (one-way or hedge). Unlike BingX which accepts a
"BOTH" option for one-way, XT has NO unified-direction option.

**Survey-driven scope check**: BEFORE patching XT in isolation,
ran a stock-CCXT `set_leverage(1, ...)` survey against all 13
venues. Results:

  * 8 venues succeed stock: binance, bitget, coinex, gate, htx,
    kucoinfutures, okx, phemex.
  * 4 venues raise ArgumentsRequired (already in override map):
    bingx, bitmart, mexc — and the new finding: **xt**.
  * 1 venue raises idempotency BadRequest (handled by classifier):
    bybit `110043`.

XT was the only venue with a missing override. After this turn's
fix, the `VENUE_SET_LEVERAGE_PARAMS` map covers every venue that
requires structural params client-side (Table D).

**Refactor that fixes it**: single-line addition to
`VENUE_SET_LEVERAGE_PARAMS["xt"]` — two-call fan-out
`[{positionSide: 'LONG'}, {positionSide: 'SHORT'}]`. The existing
`engine.py::_set_leverage_for_leg` consumes the list verbatim;
no engine-side code changes.

**Lesson**: when a venue surfaces ANY structural-params requirement,
proactively survey all 13 venues for the SAME call shape. The
survey is cheap (one CCXT round-trip per venue, no capital impact)
and reveals the remaining quirks in a single batch instead of one
per smoketest pair. Saved at minimum the next two pair-runs from
hitting an XT-style block. Make stock-CCXT surveys part of the
incident-response playbook for any "ArgumentsRequired" failure.

### 2026-05-10 — BingX × XT silent asymmetric accumulation

**Signature**: `cross_venue_smoketest` `bingx × xt` entry. Both warmup
calls succeeded (XT's positionSide fan-out worked). Slicing loop
fired the first cycle:

```
[SLICE] Symmetric snap: 19.07 → 10.00 base (long_native=10.0, short_native=1.0)
[RECOVERY] Recovery: sell 1.0 XRP/USDT:USDT on xt ... (filled_base=0, delta_base=10)
[WARNING] Recovery UNDERFILL on xt: requested 10 got 0. Asymmetric residual 10 base.
[SLICE] filled 0/0 (IOCs rejected entirely) cumulative=0/19.07
```

**The misleading log**: "filled 0/0 (IOCs rejected entirely)" was
WRONG — BingX's BUY filled 10 XRP. The else-branch of the
fill-summary logger fired whenever EITHER leg was zero, treating
single-leg fills as if both had been rejected. Operator reading the
log thought "OK, no fill, retry next cycle" while a 10-XRP naked
long was sitting on BingX.

**Cycle 2** then dispatched ANOTHER symmetric pair. BingX BUY filled
ANOTHER 10 XRP (now 20 total naked long). XT SELL again returned
filled=0. THIS time, the recovery's market sell on XT errored with
`insufficient_balance` (likely XT's USDT margin was depleted after
some venue-side accounting, OR the operator's XT futures wallet was
under-funded). The exception propagated → CRITICAL → Pushover →
halt.

**Two distinct bugs**:

  1. **Engine: silent asymmetric accumulation across cycles.** The
     loop continued after recovery underfilled. Each cycle compounded
     the naked exposure. Old code only halted on dispatch exceptions,
     not on persistent asymmetry.
  2. **XT fetch_order returns wrong fill data.** Operator-side UI
     verification (post-incident) revealed: **XT actually filled all
     three SELL orders (10 XRP each, totaling 30 XRP short)**. The
     bug was NOT "XT didn't fill" — it was "XT's `fetch_order(id)`
     returns `executedQty: 0` for IOCs that filled server-side." Our
     engine trusted the broken fetch_order result and treated the
     fills as zeros.

**Refactor that fixes bug #1**: cycle-invariant halt in
`execution.run_slicing_loop`:

```python
cycle_residual_base = abs(cycle_qty_long_base - cycle_qty_short_base)
residual_threshold = min(long_min_lot, short_min_lot)
if cycle_residual_base >= residual_threshold:
    # CRITICAL log + accumulate state + halt with halt_reason='asymmetric_residual'
    break
```

`engine.py::handle_entry / handle_exit` detect `halt_reason ==
"asymmetric_residual"` and fire Pushover P2 with the exact per-venue
exposure breakdown.

**Refactor that fixes bug #2** (probe-confirmed 2026-05-10 03:52):
XT's `fetch_order` is NOT broken — it's eventually consistent with
~800ms typical lag. The signature is "all-None response" (different
from KuCoin's `OrderNotFound` exception, but architecturally
identical: a transient non-authoritative response while the venue's
order index processes the placement).

Probe data (XT fill_resolution):
  * placement returns `id`, all fields None.
  * fetch_order returns `{filled=None, status=None, info.executedQty=None,
    info.state=None}` on EVERY poll from +0ms to +705ms.
  * AT +817ms, atomic transition to
    `{filled=10.0, status='closed', info.executedQty=1, info.state='FILLED'}`.
  * fetch_my_trades surfaces the trade at +1500ms (cross-check: 10
    XRP @ 1.4153 = $14.15, matches the placement).

The smoketest hit this lag exactly: the engine called fetch_order
~200ms after dispatch (well within XT's 800ms indexing window) and
saw all-None — which it treated as filled=0. Cycle 2 doubled the
asymmetry, then recovery hit insufficient_balance.

The architectural fix is a one-line addition to
`venue_overrides.VENUE_FETCH_ORDER_INDEXING_LAG_S['xt'] = 3.0`,
combined with a generalization of `_fetch_order_resilient` to
detect the all-None signature in addition to OrderNotFound. Both
signatures share one bounded retry loop. See Table H for the
8-venue empirical timing data.

**Methodology gap surfaced** (and the bigger lesson): the original
`receipt_shape` Class-2 probe used a 1%-below-bid IOC that
AUTO-CANCELS without filling. This verified the receipt-resolution
PATH worked but never tested whether the resolution returned
correct fill state for filling orders. XT's fetch_order returned
all-None in BOTH cases (non-filling and filling, in the lag window).
The `fill_resolution` Class-3 probe closes this gap by using a
real fill, and the 2026-05-10 sweep characterized all 8 sync-null
venues in 90 seconds at ~$0.50 of fees. **Lean toward real-fill
probes for venue verification going forward** — see
"Methodology note — lean toward real-fill probes" earlier in this
document.

### 2026-05-10 — BingX silent-residual on second smoketest run (sync-zero misclassification)

**Signature**: Re-run of `cross_venue_smoketest bingx × xt` after
shipping the XT lag fix. Cycle 1:

```
[SLICE] Symmetric snap: 19.09 → 10.00 base (long_native=10.0, short_native=1.0)
[CRITICAL] Recovery FAILED on bingx:XRP/USDT:USDT: bingx
  {"code":101253,"msg":"Insufficient margin"}
```

Operator verified post-incident: 10 XRP long on BingX + 10 XRP
short on XT. So both legs ACTUALLY filled symmetrically. But the
engine's recovery target was BingX (long leg) — meaning the engine
saw `delta = filled_long - filled_short < 0` (short ahead of long).
Since XT actually filled 10 (and XT's lag fix made fetch_order
surface that correctly), the engine must have read BingX placement
as filled=0 when it actually filled 10.

**Mechanism**: BingX was classified `sync-zero` in the R-Mode
catalog (`venue_overrides.VENUE_R_MODE`) based on `receipt_shape`
probe results: a 1%-below-bid IOC SELL returned `status='canceled'
filled=0.0`. The engine therefore TRUSTED BingX's placement receipt
without calling fetch_order. For an actually-filling IOC, BingX's
placement returned all-None (or filled=None), the engine read this
as filled=0, and the recovery dispatch hit `Insufficient margin`
because BingX already had the unread 10-XRP long.

**Same pattern as XT, different surface**: XT was sync-null and
`fetch_order` returned all-None (handled by the resolver's
indexing-lag retry). BingX was classified sync-zero so fetch_order
was never called — the all-None placement was trusted as
authoritative.

**Refactor that fixes it**: defensive sync-zero fallback in
`receipt_resolver.resolve_receipt`:

```python
if not requires_fetch_order(venue):
    if raw_create_response.get("filled") is not None:
        # genuine sync-zero: placement is authoritative
        authoritative = raw_create_response
        resolution_path = "placement"
    else:
        # placement is all-None — venue may behave sync-null on
        # filling IOCs. Fall back to fetch_order with retry.
        log("R-Mode 'sync-zero' venue X returned all-None placement ...",
            "RESOLVER_WARNING")
        authoritative = await _fetch_order_resilient(...)
        resolution_path = "fetch_order_fallback"
```

The check `filled is not None` correctly distinguishes:
  * `filled=5.6` (true fill) → trust placement.
  * `filled=0.0` + `status='canceled'` (true auto-cancel) → trust
    placement.
  * `filled=None` + `status=None` (uninformative all-None) → fall
    back.

The `RESOLVER_WARNING` log on every fallback is the feedback loop
that surfaces catalog drift to the operator. Run `fill_resolution`
on the warned venue to update its R-Mode classification.

**Empirical R-Mode catalog state after 2026-05-10 sweep**:

  * **True sync-zero** (placement.filled populated on filling
    IOCs): binance, coinex, gate.
  * **Apparent sync-zero, actually sync-null on filling**: bingx
    (anchor: this incident).
  * **Sync-null with eventual consistency**: bitget, bitmart,
    bybit, htx, kucoinfutures, mexc, okx, xt — all surface fills
    via fetch_order within 50-820ms; engine handles via
    `VENUE_FETCH_ORDER_INDEXING_LAG_S`.

The defensive fallback means even mistaken classifications are
handled transparently — the operator just sees a `RESOLVER_WARNING`
on every fallback fire, and can update the catalog at leisure.

**Lesson**: the safest classification is the one that doesn't matter.
By making the sync-zero classification a HINT (trusted only when
placement is itself authoritative), we removed the catalog's
ability to silently break the engine. The catalog is now a
performance optimization (skip fetch_order when placement is
known-authoritative) rather than a correctness dependency.

### 2026-05-10 — XT `receipt['filled']` is in BASE units (CCXT inconsistency)

**Signature**: After shipping the XT lag fix + defensive sync-zero
fallback, re-ran bingx × xt smoketest. Same failure:

```
[CRITICAL] Recovery FAILED on bingx:XRP/USDT:USDT: bingx
  {"code":101253,"msg":"Insufficient margin"}
```

Operator's UI confirmed both legs DID fill symmetrically — BingX 10
XRP long, XT 1 contract (= 10 XRP) short. So the engine's read of
the fills must have been wrong despite the symmetric reality.

**The discovery** — direct invocation of `resolve_receipt` on the
exact concurrent BingX BUY + XT SELL pattern returned:

```
BINGX FillReceipt: filled_native=10.0  filled_base=10.0     ✓
XT    FillReceipt: filled_native=10.0  filled_base=100.0    ✗ 10× over-read

delta_base = 10.0 - 100.0 = -90.0  ← phantom imbalance
```

**Root cause**: CCXT's XT class (xt.py:3469) is the only venue in
our catalog that **pre-multiplies `executedQty × contract_size` in
parse_order for swap markets**:

```python
filled = filledQuantity if (marketType == 'spot') else Precise.string_mul(
    self.number_to_string(filledQuantity),
    self.number_to_string(market['contractSize']))
```

For XT XRP perp: 1 contract × contract_size 10 → `filled=10` (BASE).
The engine's `leg.to_base_qty(filled)` then multiplies by
contract_size AGAIN: `10 × 10 = 100` base. Double-multiply.

Every other venue we trade (KuCoin, OKX, HTX, BingX, Bitmart, MEXC,
Bitget, Coinex, Gate, etc) leaves `receipt['filled']` in NATIVE
contract units — the engine's standard `leg.to_base_qty(filled)`
multiplies once by contract_size to get base. Correct convention.

XT alone breaks the pattern. CCXT's XT translation is arguably MORE
unified (filled in base IS the documented unified semantic), but
it's the inconsistent one across venues.

**The previous diagnoses on this incident were wrong** — XT was
filling correctly, fetch_order was returning correctly, the resolver
was reading the right fields — but the unit translation was wrong.
Each layer behaved correctly in isolation; the bug was in the
boundary between CCXT and the engine's unit conventions.

**Refactor that fixes it**:

  1. `venue_overrides.VENUE_RECEIPT_FILLED_IN_BASE = {"xt"}` — set
     of venues whose CCXT `parse_order` pre-multiplies filled by
     contract_size.
  2. `venue_overrides.receipt_filled_is_base(venue)` helper.
  3. `receipt_resolver.resolve_receipt` checks this and skips the
     `leg.to_base_qty` multiplication for XT-style venues:

```python
ccxt_filled = float(authoritative.get("filled") or 0)
if receipt_filled_is_base(venue):
    filled_base = ccxt_filled
    filled_native = ccxt_filled / leg.base_per_native
else:
    filled_native = ccxt_filled
    filled_base = leg.to_base_qty(ccxt_filled)
```

**Lesson**: CCXT's "unified" order schema isn't fully unified across
the 13 venues. The `filled` field's unit semantics differ between
XT (base) and everyone else (native). When in doubt, run the
`fill_resolution` Class-3 probe and **compare `receipt['filled']`
against the dispatched native quantity**. If `filled` ≈ dispatched
native → CCXT returns native. If `filled` ≈ dispatched native ×
contract_size → CCXT returns base; add the venue to
VENUE_RECEIPT_FILLED_IN_BASE.

This is the third instance of the same meta-lesson: **CCXT's
unification only documents intent, not actual behavior**. Verify
empirically per venue, per direction, per call.

**Lesson**: the cycle invariant ("after each cycle, position state
is symmetric") was implicit in the design but NOT enforced in code.
Implicit invariants drift; explicit invariants halt. Equally
important: **the R-Mode catalog is only as good as the probes that
built it.** A non-filling probe verifies the resolution path's
PLUMBING but says nothing about the path's CORRECTNESS on real
fills. Always probe with the actual production traffic shape
(filling IOCs, not safe-by-construction non-filling IOCs) before
trusting a venue's resolution behavior.

---

*End of file. This document gets updated whenever a probe runs and
returns a value that contradicts a row above. Always commit the new
finding alongside the probe-log JSONL that proves it.*
