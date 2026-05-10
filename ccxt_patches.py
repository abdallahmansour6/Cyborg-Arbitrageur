"""
CCXT venue-class patches — minimal-diff workarounds for bugs in
CCXT 4.5.51 venue classes that the engine and probes must work around
until upstream catches up.

Every patch here:
  - Is a subclass of the upstream class with the smallest possible
    method override.
  - Has a corresponding repro in `ccxt_bug_repro.py` proving both that
    the original is buggy AND that the patch fixes it.
  - Is documented in `ENGINE_FIELD_NOTES.md` under the venue's row.

Promote a patch back to upstream CCXT (and remove it here) once the fix
lands. Keeping patches local makes CCXT version bumps easy: re-run
`ccxt_bug_repro.py` after each bump and delete patches whose tests
now pass without them.
"""
import ccxt.pro as ccxtpro
from ccxt.base.errors import ExchangeError


class PatchedBitmart(ccxtpro.bitmart):
    """Guards `handle_errors` against a None `message` field.

    CCXT 4.5.51 `ccxt/async_support/bitmart.py:5459` calls
    `safe_string(response, 'message').lower()` unconditionally. When the
    venue response omits the `message` key (observed empirically on
    contract create-order error paths), the unguarded `.lower()` raises
    `AttributeError: 'NoneType' object has no attribute 'lower'` — and
    the underlying venue error (signature mismatch, auth failure, etc.)
    is hidden behind the AttributeError.

    This patch guards the `.lower()` call but otherwise preserves the
    original logic 1:1 — including the `errorCode` path that fires
    even when `message` is None.

    Repro: `python3 ccxt_bug_repro.py bitmart`.
    """

    def handle_errors(self, code, reason, url, method, headers, body, response, requestHeaders, requestBody):
        if response is None:
            return None
        message = self.safe_string(response, 'message')
        messageLower = message.lower() if message is not None else ''
        isErrorMessage = (
            (message is not None)
            and (messageLower != 'ok')
            and (messageLower != 'success')
        )
        errorCode = self.safe_string(response, 'code')
        isErrorCode = (errorCode is not None) and (errorCode != '1000')
        if isErrorCode or isErrorMessage:
            feedback = self.id + ' ' + (body or '')
            self.throw_exactly_matched_exception(self.exceptions['exact'], message, feedback)
            self.throw_broadly_matched_exception(self.exceptions['broad'], message, feedback)
            self.throw_exactly_matched_exception(self.exceptions['exact'], errorCode, feedback)
            self.throw_broadly_matched_exception(self.exceptions['broad'], errorCode, feedback)
            raise ExchangeError(feedback)
        return None


# Canonical venue id → patched class. Falls through to `ccxtpro.<id>`
# for venues without patches.
_PATCHED_CLASSES: dict = {
    "bitmart": PatchedBitmart,
}


def get_ccxt_class(canonical_id: str):
    """Return the (possibly patched) CCXT Pro class for a canonical
    venue id. The engine and probes both call through this so a single
    place owns the patch policy."""
    return _PATCHED_CLASSES.get(canonical_id, getattr(ccxtpro, canonical_id))
