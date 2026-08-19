"""Typed errors mirroring `trading-gateway/proxy/errors.py`'s §9 code table.

The Proxy hands back `{"error": {"code": ..., "message": ..., "detail": ...,
"request_id": ...}}`. Every caller of the Proxy needs to turn that `code`
string into a decision: retry with the same idempotency key, retry with a new
one, or stop because it's a bug. That decision table is currently tribal
knowledge encoded as comments in the server's `pipeline.py` — this module
makes it part of the SDK's tested, public contract instead.

`retryable` is deliberately conservative: `None` means "cannot be decided
from the code alone, inspect the exception" (today, only `ExchangeError`,
via `.outcome_unknown`). Getting this wrong in the optimistic direction is
exactly how a retry duplicates a live order.
"""

from __future__ import annotations

from typing import Any


class ProxyError(RuntimeError):
    """Base class for everything this SDK raises from a Proxy call.

    `retryable = False` by default — safe retries are the exception, not the
    rule, for a client talking to a trading endpoint.
    """

    code = "UNKNOWN"
    retryable: bool | None = False

    def __init__(
        self,
        message: str,
        *,
        detail: Any = None,
        request_id: str = "",
        code: str | None = None,
    ) -> None:
        super().__init__(f"{code or self.code}: {message}")
        self.message = message
        self.detail = detail
        self.request_id = request_id
        if code is not None:
            self.code = code


# -- transport-level, raised by this SDK, never sent by the Proxy -----------


class ProxyUnreachableError(ProxyError):
    """The Proxy could not be reached, or replied with a non-JSON body.

    Nothing is known about whether the request was even received, so this is
    the same "outcome unknown" situation as `ExchangeError.outcome_unknown` —
    safe to retry only if the caller is prepared to hit
    `IdempotencyInFlightError` on that retry and back off again.
    """

    code = "PROXY_UNREACHABLE"
    retryable = None


# -- §9, verbatim -------------------------------------------------------


class AuthFailedError(ProxyError):
    """Bad secret, unknown key, wrong operator, or clock skew — the Proxy
    will not say which (see `errors.py`'s module docstring: this ambiguity
    is intentional, so don't build UI that tries to guess further)."""

    code = "AUTH_FAILED"
    retryable = False


class CredentialExpiredError(ProxyError):
    code = "CREDENTIAL_EXPIRED"
    retryable = False


class ActionNotPermittedError(ProxyError):
    code = "ACTION_NOT_PERMITTED"
    retryable = False


class ExchangeError(ProxyError):
    """The exchange rejected or failed the request.

    Check `outcome_unknown` before deciding whether to retry:

    - `False` (or absent): the exchange answered definitively. Retrying
      with the *same* idempotency key just replays that same answer — safe,
      but rarely useful.
    - `True`: the request was forwarded but the outcome was never learned
      (timeout, transport error mid-flight). The server leaves the
      idempotency claim in-flight rather than release it, specifically so a
      naive retry cannot place a duplicate order — a retry with the same key
      will get `IdempotencyInFlightError`, not a second placement. This is
      the "a human looks" case; don't loop on it automatically.
    """

    code = "EXCHANGE_ERROR"
    retryable = None

    @property
    def outcome_unknown(self) -> bool:
        return bool(isinstance(self.detail, dict) and self.detail.get("outcome_unknown"))


class MalformedRequestError(ProxyError):
    """The envelope itself didn't parse. Almost always an SDK bug, not a
    transient condition — if you see this, file it against this package."""

    code = "MALFORMED_REQUEST"
    retryable = False


class IdempotencyKeyRequiredError(ProxyError):
    """This action mutates state and needs `idempotency_key`; none was
    passed to `ProxyClient.call`. An SDK-usage bug, not a retry case."""

    code = "IDEMPOTENCY_KEY_REQUIRED"
    retryable = False


class IdempotencyKeyReusedError(ProxyError):
    """The same idempotency key was already used for a *different* request
    body. This is always a caller bug — reusing a key across two distinct
    order intents — never something to retry."""

    code = "IDEMPOTENCY_KEY_REUSED"
    retryable = False


class IdempotencyInFlightError(ProxyError):
    """An identical request (same key, same body) is still being processed.
    Safe to retry the same call (same key, same payload) after a short
    backoff."""

    code = "IDEMPOTENCY_IN_FLIGHT"
    retryable = True


class LogUnavailableError(ProxyError):
    """The Proxy is fail-closed on audit logging and refused the request
    before forwarding it anywhere. Nothing was sent to the exchange, so the
    same idempotency key is still safe to retry."""

    code = "LOG_UNAVAILABLE"
    retryable = True


class RateLimitedError(ProxyError):
    """Over budget. `retry_after` comes from the Proxy's `Retry-After`
    header when present; retry the same call (same key) after that many
    seconds."""

    code = "RATE_LIMITED"
    retryable = True

    def __init__(self, *args: Any, retry_after: float | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after = retry_after


class StreamParamsConflictError(ProxyError):
    """WebSocket-only; reserved here for forward compatibility with a future
    WS fast-follow. Not reachable through this SDK's v1 (REST-only,
    operator-credential) surface."""

    code = "STREAM_PARAMS_CONFLICT"
    retryable = False


# -- reserved server-side (Milestone 5); included so an unrecognised code
#    never falls through silently once the server starts sending them -----


class RiskLimitExceededError(ProxyError):
    code = "RISK_LIMIT_EXCEEDED"
    retryable = False


class KillSwitchActiveError(ProxyError):
    code = "KILL_SWITCH_ACTIVE"
    retryable = None


_BY_CODE: dict[str, type[ProxyError]] = {
    cls.code: cls
    for cls in (
        AuthFailedError,
        CredentialExpiredError,
        ActionNotPermittedError,
        ExchangeError,
        MalformedRequestError,
        IdempotencyKeyRequiredError,
        IdempotencyKeyReusedError,
        IdempotencyInFlightError,
        LogUnavailableError,
        RateLimitedError,
        StreamParamsConflictError,
        RiskLimitExceededError,
        KillSwitchActiveError,
    )
}


def from_response(
    code: str,
    message: str,
    *,
    detail: Any = None,
    request_id: str = "",
    retry_after: float | None = None,
) -> ProxyError:
    """Build the typed exception for a Proxy error `code`.

    Falls back to the base `ProxyError` (retryable=False) for a code this
    SDK doesn't recognise yet — a new server-side code should fail closed on
    the client, not get treated as retryable by accident.
    """
    cls = _BY_CODE.get(code)
    if cls is None:
        return ProxyError(message, detail=detail, request_id=request_id, code=code)
    if cls is RateLimitedError:
        return RateLimitedError(
            message, detail=detail, request_id=request_id, retry_after=retry_after
        )
    return cls(message, detail=detail, request_id=request_id)
