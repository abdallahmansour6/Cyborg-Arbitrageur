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
