"""
Execution-side primitives.

Holds the (exchange, symbol, multiplier, contract_size) bundle that resolves
the "Asymmetric Symbols & Multiplier Prefixes" reality of cross-venue perps.
The same underlying coin lists under different multiplier prefixes per venue
(`CHEEMS` / `1000CHEEMS` / `1MCHEEMS` / `1000000CHEEMS` are all the same coin)
and prices/sizes scale by `multiplier * contract_size`. This module is the
single carrier of that conversion knowledge — engine and execution code
depend ONLY on these methods and never re-derive the math inline.

All public surface operates in TRUE 1x BASE TOKENS at the boundary; native
units appear only inside leg conversions and at the IOC dispatch site.
"""
import re
from dataclasses import dataclass


# Multiplier-prefix parser. Same regex as the Scanner — verified safe against
# `1INCH`-style digits-then-letters false positives (the `(?=[A-Za-z])` lookahead
# requires the prefix to be followed by a letter, and `1INCH` has no zero so it
# doesn't match `1[KM]|10{2,7}`). Strips:
#   100 / 1000 / 10000 / 100000 / 1000000 / 10000000 / 1K / 1M
# Case-insensitive to tolerate any venue casing drift.
_MULTIPLIER_PREFIX = re.compile(r"^(?:1[KM]|10{2,7})(?=[A-Za-z])", re.IGNORECASE)


def parse_base(raw_base: str) -> tuple[str, int]:
    """Split a CCXT market.base into (base_coin, multiplier).

    'CHEEMS'        -> ('CHEEMS', 1)
    '1000CHEEMS'    -> ('CHEEMS', 1000)
    '1MCHEEMS'      -> ('CHEEMS', 1_000_000)
    '1000000CHEEMS' -> ('CHEEMS', 1_000_000)
    '1INCH'         -> ('1INCH', 1)   # protected by the lookahead
    """
    m = _MULTIPLIER_PREFIX.match(raw_base)
    if not m:
        return raw_base, 1
    prefix = m.group(0).upper()
    base_coin = _MULTIPLIER_PREFIX.sub("", raw_base)
    if prefix == "1K":
        return base_coin, 1_000
    if prefix == "1M":
        return base_coin, 1_000_000
    return base_coin, int(prefix)


@dataclass(frozen=True)
class ExecutionLeg:
    """One trading leg. Owns ALL base<->native conversion knowledge.

    The wire-units factor is `multiplier * contract_size` base tokens per
    native contract. Two scaling sources are collapsed here so downstream
    code never has to know which one came from where:
      - `multiplier`     comes from the symbol prefix (CHEEMS/1000CHEEMS/...)
      - `contract_size`  comes from CCXT `market.contractSize`
    Both are multiplied together; the rest of the engine only sees the result.
    """
    exchange: str          # canonical CCXT id, lowercased
    symbol: str            # CCXT-unified, exchange-native ('1000CHEEMS/USDT:USDT')
    base_coin: str         # multiplier-stripped ('CHEEMS')
    multiplier: int        # prefix integer; 1 when no prefix
    contract_size: float   # CCXT market.contractSize; 1.0 when missing/None

    @property
    def base_per_native(self) -> float:
        """How many true 1x base tokens one native contract represents."""
        return self.multiplier * self.contract_size

    def to_native_qty(self, base_qty: float) -> float:
        """1x base tokens -> exchange-native qty (PRE-precision rounding)."""
        return base_qty / (self.multiplier * self.contract_size)

    def to_base_qty(self, native_qty) -> float:
        """Exchange-native qty (e.g. CCXT receipt['filled']) -> 1x base tokens."""
        return float(native_qty or 0) * self.multiplier * self.contract_size

    def to_base_price(self, native_price: float) -> float:
        """Venue-native price -> per-1x-base price.

        Used wherever cross-leg arithmetic happens (basis math, log lines).
        Two venues showing `CHEEMS@$0.0006` and `1000CHEEMS@$0.6` are
        publishing the same per-1x price; this method makes them comparable.
        """
        return native_price / self.multiplier

    @classmethod
    def from_market(cls, exchange_id: str, market: dict) -> "ExecutionLeg":
        """Build from a CCXT market dict. The boundary normalization site —
        called once at warmup and at pre_warm reconstruction. Trusts the live
        CCXT values; persisted values are advisory (see engine.pre_warm)."""
        base_coin, multiplier = parse_base(market.get("base", ""))
        return cls(
            exchange=exchange_id.lower(),
            symbol=market["symbol"],
            base_coin=base_coin,
            multiplier=multiplier,
            contract_size=float(market.get("contractSize") or 1.0),
        )


@dataclass(frozen=True)
class ExecutionPair:
    """Two legs guaranteed to reference the same underlying base coin.

    The base-coin invariant is enforced at construction. Any caller that
    assembles an asymmetric pair on different underlyings fails fast at the
    boundary, so position-state keying and basis math can both rely on
    `pair.key == pair.long.base_coin == pair.short.base_coin`.
    """
    long: ExecutionLeg
    short: ExecutionLeg

    def __post_init__(self):
        if self.long.base_coin != self.short.base_coin:
            raise ValueError(
                f"Pair base mismatch: long={self.long.base_coin} "
                f"short={self.short.base_coin}. Different coins cannot be hedged."
            )

    @property
    def key(self) -> str:
        """Position-state key. base_coin is unique per simultaneous routing."""
        return self.long.base_coin


@dataclass(frozen=True)
class FillReceipt:
    """Fill-resolved trade outcome — the engine's authoritative view of
    a single create_order's settled state.

    Constructed exclusively by `receipt_resolver.resolve_receipt()`,
    which honors the per-venue R-Mode catalog in `venue_overrides.py`:
    sync-zero / sync-final venues are resolved from placement directly;
    sync-null / eventual venues are resolved via mandatory `fetch_order(id)`
    follow-up. The `resolution_path` field surfaces which route was taken
    so logs and audits can attribute drift to the right layer.

    Replaces `receipt.get('filled')` + `_fill_vwap()` reads scattered
    across `execution.py`. Every fill-state read after the refactor
    flows through this object — there is one place that knows how to
    convert a CCXT receipt into engine-trustable numbers.

    Anchored in the May 7 bybit incident (`transaction.log` 21:05:39):
    the engine was reading `receipt['filled']` directly, the venue was
    returning all-None, and 11 cycles of phantom zero-fills accumulated
    real long exposure on binance against unknown bybit state. With
    this primitive, that bug is impossible — sync-null venues are
    classified at boundary, and the resolver enforces the fetch_order
    follow-up before any fill state is read.
    """
    leg: ExecutionLeg
    order_id: str
    filled_native: float           # post-resolution; native units
    filled_base: float             # post-resolution; 1× base tokens
    vwap_native: float             # 0.0 only if filled_native == 0
    vwap_base: float               # per-1×-base; 0.0 only if filled_base == 0
    status: str | None             # 'closed' | 'canceled' | 'expired' | 'rejected'
                                   # | 'open' (eventual not yet terminal) | None (coinex)
    r_mode: str                    # placement classification at construction time
                                   # ('sync-zero' | 'sync-null' | 'sync-final' |
                                   # 'eventual' | 'unknown')
    resolution_path: str           # 'placement' | 'fetch_order'
                                   # — which call surfaced the resolved state
    raw_create_response: dict      # original CCXT placement receipt (forensic)
    raw_resolve_response: dict | None  # follow-up fetch_order response, if any

    @property
    def is_terminal(self) -> bool:
        """True iff the venue reports a terminal lifecycle state.

        Note: coinex returns `status=None` even on a terminal IOC; the
        engine should NOT gate control flow on `is_terminal` alone.
        Use `filled_base` for the actual quantity decision; `is_terminal`
        is for logging and post-hoc auditing."""
        return self.status in ("closed", "canceled", "expired", "rejected", "filled")

    @property
    def is_zero_fill(self) -> bool:
        """True iff resolved filled quantity is zero — i.e., the IOC
        auto-canceled with no match. Used by the slicing loop's recovery
        path to decide whether to fire imbalance-recovery on the lagging leg."""
        return self.filled_base == 0.0


@dataclass(frozen=True)
class BookSnapshot:
    """Time-stamped order book — replaces the bare-dict cache that
    `engine.order_books[(ex_id, symbol)]` currently holds.

    Adds the metadata the slicing loop needs to gate "is this book
    safe to trade against right now": liveness (was the cache updated
    recently), sufficiency (does it have a top-of-book), and well-formedness
    (is it crossed). Empirically motivated:

      - 1.2% of bitmart yields had crossed books on a calm BTC sample
        (verified 2026-05-09; ENGINE_FIELD_NOTES.md Table C). The slicing
        loop's basis math reads a crossed book as a giant negative
        spread and would trivially "pass" the basis floor at zero size.

      - coinex p95 max-Δ is 2.98 s — 50× the median cluster of the other
        12 venues. Without an explicit freshness gate, the engine cannot
        tell a wedged stream from a quiet pair.

    All cross-leg arithmetic that consumes a BookSnapshot must:
      1. assert `is_fresh(MAX_BOOK_AGE_MS, now_ms)` — both legs.
      2. assert `has_top_of_book()` — both legs.
      3. assert `not is_crossed()` — both legs.
    Failure of any of the three is treated identically to a basis-floor
    miss: skip the cycle and retry next tick.
    """
    bids: list                     # list[list[float]] — [[price, size], ...] descending
    asks: list                     # list[list[float]] — [[price, size], ...] ascending
    venue_ts_ms: int | None        # exchange-side timestamp (None: htx/okx don't expose)
    received_ts_ms: int            # local monotonic-ms when this delta hit our process —
                                   # AUTHORITATIVE for staleness; never depends on venue clock
    delta_count: int               # incremented per watch_order_book yield. Lets a watchdog
                                   # detect "cache wedged on stale snapshot" patterns even
                                   # when venue_ts_ms isn't exposed.
    sequence: int | None = None    # venue-side sequence number, when exposed

    def is_fresh(self, max_age_ms: int, now_ms: int) -> bool:
        """True iff this snapshot is younger than `max_age_ms` against
        the caller-supplied `now_ms` (typically `time.monotonic() * 1000`).

        `now_ms` is a parameter (not implicit) so tests can pass a fixed
        time and so the engine can use a single monotonic clock reading
        across multiple legs in the same cycle (avoiding sub-ms drift
        between leg-A.is_fresh() and leg-B.is_fresh())."""
        return (now_ms - self.received_ts_ms) <= max_age_ms

    def has_top_of_book(self) -> bool:
        """True iff both sides expose at least one level."""
        return bool(self.bids) and bool(self.asks)

    def is_crossed(self) -> bool:
        """True iff best-bid >= best-ask. Empirically observed on
        bitmart (~1.2% of frames). Always indicates venue-side matching
        glitch, never a real arbitrage opportunity at the prices we trade."""
        if not self.has_top_of_book():
            return False
        return float(self.bids[0][0]) >= float(self.asks[0][0])
