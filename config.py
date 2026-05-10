import os
from dotenv import load_dotenv

from ccxt_patches import get_ccxt_class

load_dotenv()


# Per-venue post-instantiation option overrides. Each entry is a callable
# that mutates `client.options` to bypass a known CCXT default that would
# otherwise route us to a deprecated endpoint or a wrong account type.
# Each override has a repro in `ccxt_bug_repro.py` and a row in
# ENGINE_FIELD_NOTES.md.
def _htx_route_to_v3_unified(client) -> None:
    """HTX migrated to v3 unified-account-info; CCXT 4.5.51 still defaults
    to the deprecated v1 cross/isolated path. Setting `uta=True` flips
    fetch_balance to `linear-swap-api/v3/unified_account_info`."""
    fb = client.options.setdefault("fetchBalance", {})
    fb["uta"] = True


_VENUE_OPTION_PATCHES = {
    "htx": _htx_route_to_v3_unified,
}


# Per-venue credential remaps. Each entry: {ccxt_creds_field: env_suffix}.
# After the standard `{API_KEY, SECRET, PASSWORD, UID}` env reads, these
# remaps overwrite specific creds-dict slots with values from a different
# env-var. Used to bridge cases where CCXT's internal credential semantics
# diverge from the operator's intuitive `.env` naming.
#
# Empirically discovered; documented in ENGINE_FIELD_NOTES.md per venue.
_VENUE_CREDENTIAL_REMAP: dict[str, dict[str, str]] = {
    # Bitmart's `sign()` (ccxt/async_support/bitmart.py:5437) builds the
    # auth string as `timestamp + '#' + self.uid + '#' + queryString` —
    # consuming what the bitmart UI calls "Memo" via CCXT's `uid` slot.
    # Operator convention puts the Memo in BITMART_PASSWORD (matching the
    # passphrase semantics of OKX/KuCoin/Bitget). Bridge here so the
    # operator's `.env` stays semantically logical.
    "bitmart": {"uid": "PASSWORD"},
}


def get_exchange(exchange_id):
    """Instantiate a CCXT Pro client for the Engine.

    Resolves the venue class through `ccxt_patches.get_ccxt_class` so any
    monkey-patched subclasses are applied (currently bitmart). Then
    applies post-instantiation option overrides registered in
    `_VENUE_OPTION_PATCHES` (currently htx)."""
    creds = {
        "apiKey": os.getenv(f"{exchange_id.upper()}_API_KEY"),
        "secret": os.getenv(f"{exchange_id.upper()}_SECRET"),
        "enableRateLimit": False,  # In this context, we will not let the library throttle us, we blast the network instantly; if we hit a limit, we let the exchange reject us.
        "options": {"defaultType": "swap"},  # Strictly perps
        "newUpdates": True,  # WebSocket order book deltas for max speed and minimal bandwidth
    }

    # Optional credentials beyond apiKey/secret. CCXT venue classes vary
    # in what they require:
    #   PASSWORD — API passphrase (OKX, KuCoin, Bitget, etc.)
    #   UID      — account UID (BITMART)
    # Add new credential names here as venues introduce them; keep the env
    # var convention `{EXCHANGE_ID}_{CREDNAME}` consistent.
    for cred in ("PASSWORD", "UID"):
        v = os.getenv(f"{exchange_id.upper()}_{cred}")
        if v:
            creds[cred.lower()] = v

    # Per-venue credential remap — overwrites specific creds slots with
    # values pulled from a different env-var. See _VENUE_CREDENTIAL_REMAP
    # comment for the rationale per venue.
    canonical = exchange_id.lower()
    for ccxt_field, env_suffix in _VENUE_CREDENTIAL_REMAP.get(canonical, {}).items():
        v = os.getenv(f"{exchange_id.upper()}_{env_suffix}")
        if v:
            creds[ccxt_field] = v

    exchange_class = get_ccxt_class(canonical)
    client = exchange_class(creds)

    patcher = _VENUE_OPTION_PATCHES.get(canonical)
    if patcher:
        patcher(client)

    return client
