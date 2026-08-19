"""Drift detection against the server's own error codes.

Same idea as `test_signing.py`'s golden values: rather than importing
`trading-gateway/proxy/errors.py` live (which would make this package's own
test suite depend on a sibling directory existing on disk — fragile, and
wrong for something meant to be `pip install`-able and testable standalone),
this pins a snapshot of the server's known codes as plain constants below.

`_SERVER_CODES` was read directly off `trading-gateway/proxy/errors.py`'s
`HTTP_STATUS` keys plus `EXCHANGE_ERROR` (the one code deliberately absent
from `HTTP_STATUS`, since §9 says its status "varies"). If the server adds,
removes, or renames a code in `proxy/errors.py`, update the set below by
hand to match — that's the whole maintenance cost, and it's a one-line diff
that shows up in code review.
"""

from __future__ import annotations

from proxy_client.errors import _BY_CODE

# Pinned against trading-gateway/proxy/errors.py. Update by hand when that
# file's code table changes.
_SERVER_CODES: frozenset[str] = frozenset(
    {
        "AUTH_FAILED",
        "CREDENTIAL_EXPIRED",
        "ACTION_NOT_PERMITTED",
        "EXCHANGE_ERROR",
        "RISK_LIMIT_EXCEEDED",
        "KILL_SWITCH_ACTIVE",
        "MALFORMED_REQUEST",
        "IDEMPOTENCY_KEY_REQUIRED",
        "IDEMPOTENCY_KEY_REUSED",
        "IDEMPOTENCY_IN_FLIGHT",
        "LOG_UNAVAILABLE",
        "STREAM_PARAMS_CONFLICT",
        "RATE_LIMITED",
    }
)


def test_sdk_has_a_typed_exception_for_every_known_server_error_code():
    missing = _SERVER_CODES - set(_BY_CODE)
    assert not missing, (
        f"pinned server codes {sorted(missing)} have no typed exception in "
        "proxy_client.errors — add one to errors.py's _BY_CODE."
    )


def test_sdk_has_no_codes_the_pinned_server_snapshot_does_not():
    """The inverse check: an SDK code not in the pinned snapshot is either a
    typo in a `code = "..."` class attribute, or a code the server retired —
    both worth catching rather than silently keeping a dead mapping. If the
    server genuinely added a new code, update `_SERVER_CODES` above."""
    extra = set(_BY_CODE) - _SERVER_CODES
    assert not extra, (
        f"proxy_client.errors maps {sorted(extra)}, which isn't in the "
        "pinned server snapshot — check for a typo, or update _SERVER_CODES "
        "above if trading-gateway/proxy/errors.py genuinely added it."
    )
