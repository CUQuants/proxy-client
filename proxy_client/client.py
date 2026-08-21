
"""ProxyClient — the REST call() surface.

v1 scope (see SDK_WRITEUP.md and the follow-up decision to ship this first):

- REST only. WebSocket is a fast-follow.
- Both operator (human) and system (bot) credentials are supported —
  `ProxyClient.for_operator(...)` / `ProxyClient.for_system(...)`. See
  `envelope.py`'s docstring for how the two stay mutually exclusive on the
  wire without either constructor trusting the caller to get it right.
- No automatic retries. `call()`/`acall()` raise a typed `ProxyError`
  subclass (see `errors.py`) and leave the retry decision to the caller,
  matching the pattern in SDK_WRITEUP.md's own example:

      try:
          result = client.call(exchange="okx", action="place_order", payload={...})
      except RateLimitedError as e:
          time.sleep(e.retry_after or 1.0)
          result = client.call(..., idempotency_key=key)  # same key
      except IdempotencyKeyReusedError:
          raise  # a bug, not a transient condition

  Auto-retry was considered and deliberately deferred: a retry policy baked
  into the SDK hides the one decision (same key, how many attempts, what
  backoff) that's genuinely call-site-specific, and every retryable error
  here already carries what a caller needs to implement their own loop.

`call()` and `acall()` are both real transport calls on `httpx` (a sync
`httpx.Client` and an `httpx.AsyncClient`), not one wrapping the other via a
thread executor — a caller on an asyncio event loop gets true async I/O, not
a thread-per-call approximation of it.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx

from .envelope import build_body
from .errors import ProxyUnreachableError, from_response
from .idempotency import new_idempotency_key
from .signing import compute_signature

__all__ = ["ProxyClient", "ProxyResult", "new_idempotency_key"]


class ProxyResult(dict):
    """The `data` field of the Proxy's response envelope, exactly as returned.

    A `dict` subclass, not a wrapper: indexing, iteration, and `==` against a
    plain dict all behave exactly as if `call()`/`acall()` returned a bare
    dict, so existing parsing code needs no changes.

    The one addition is `idempotent_replay`: `True` when this response is not
    a fresh execution but the cached result of an identical earlier request
    (Proxy spec §3.3 — same idempotency key, same body — surfaced via the
    `X-Idempotent-Replay` response header). Most callers can ignore it; it
    matters when you want to log or alert on "this retried request replayed
    my original order" versus "this placed a new one."
    """

    def __new__(cls, data: dict[str, Any], *, idempotent_replay: bool) -> "ProxyResult":
        return super().__new__(cls, data)

    def __init__(self, data: dict[str, Any], *, idempotent_replay: bool) -> None:
        super().__init__(data)
        self.idempotent_replay = idempotent_replay


class ProxyClient:
    """One caller's authenticated session against the Proxy.

    Holds `secret` in memory only, for the life of this object. Never log
    or serialize a `ProxyClient` instance — `__repr__` is overridden so an
    accidental `print(client)` or exception traceback can't leak it.

    Owns both a sync (`httpx.Client`) and an async (`httpx.AsyncClient`)
    transport so `call()` and `acall()` are both real, independent HTTP
    clients rather than one built on the other. Call `close()` / `aclose()`
    (or use as a context manager) to release their connection pools when
    done; a `ProxyClient` used for the lifetime of a process doesn't need to
    bother.

    Construct via `ProxyClient.for_operator(...)` or `.for_system(...)`
    (equivalent to calling `ProxyClient(...)` directly for the operator
    case) rather than passing `system_name` as a loose kwarg here — there
    isn't one. That's deliberate: an operator credential must never carry
    `system_name` on the wire and a system credential always must
    (`trading-gateway/proxy/auth.py`), and the server's rejection for
    getting it wrong is the same uninformative `AUTH_FAILED` for either
    mistake. Making `__init__` operator-only means there's no path to
    building an operator-looking client that accidentally carries one.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        secret: str,
        operator_id: str,
        operator_name: str,
        *,
        timeout: float = 15.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._api_key = api_key
        self._secret = secret.encode("utf-8")
        self.operator_id = operator_id
        self.operator_name = operator_name
        self._system_name: str | None = None
        self._client = httpx.Client(timeout=timeout)
        self._aclient = httpx.AsyncClient(timeout=timeout)

    @classmethod
    def for_operator(
        cls,
        base_url: str,
        api_key: str,
        secret: str,
        operator_id: str,
        operator_name: str,
        *,
        timeout: float = 15.0,
    ) -> "ProxyClient":
        """Identical to `ProxyClient(...)` — exists so a call site reads
        symmetrically next to `for_system()`."""
        return cls(base_url, api_key, secret, operator_id, operator_name, timeout=timeout)

    @classmethod
    def for_system(
        cls,
        base_url: str,
        api_key: str,
        secret: str,
        operator_id: str,
        operator_name: str,
        system_name: str,
        *,
        timeout: float = 15.0,
    ) -> "ProxyClient":
        """Build a client authenticating as a system (bot) credential,
        still attributed to the given responsible operator — both are
        required on every request regardless of credential type
        (`trading-gateway/proxy/auth.py`'s `_attribute`).

        `system_name` has no other entry point onto a `ProxyClient`: this
        is the only constructor that sets it, so there is no way to end up
        with an operator-built client that carries one by accident.
        """
        if not system_name:
            raise ValueError("system_name is required for a system credential")
        client = cls(base_url, api_key, secret, operator_id, operator_name, timeout=timeout)
        client._system_name = system_name
        return client

    @property
    def system_name(self) -> str | None:
        """`None` for an operator client; the bot's registered name for a
        system client (set only via `for_system()`)."""
        return self._system_name

    def __repr__(self) -> str:
        who = self._system_name or self.operator_id
        return f"<ProxyClient {who!r} via {self._base!r}>"

    def close(self) -> None:
        self._client.close()

    async def aclose(self) -> None:
        await self._aclient.aclose()

    def __enter__(self) -> "ProxyClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    async def __aenter__(self) -> "ProxyClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    # -- request/response, shared by call() and acall() ---------------------

    def _prepare(
        self, exchange: str, action: str, payload: dict[str, Any], idempotency_key: str
    ) -> tuple[str, bytes, dict[str, str]]:
        path = f"/v1/{exchange}"
        body = build_body(
            exchange=exchange,
            action=action,
            operator_id=self.operator_id,
            operator_name=self.operator_name,
            system_name=self._system_name,
            payload=payload,
        )

        timestamp = str(int(time.time()))
        nonce = uuid.uuid4().hex
        headers = {
            "X-Api-Key": self._api_key,
            "X-Timestamp": timestamp,
            "X-Nonce": nonce,
            "X-Signature": compute_signature(
                self._secret, "POST", path, timestamp, nonce, body
            ),
            "Content-Type": "application/json",
        }
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key
        return path, body, headers

    def _handle_response(self, response: httpx.Response) -> ProxyResult:
        try:
            parsed = response.json()
        except ValueError:
            raise ProxyUnreachableError(
                f"HTTP {response.status_code} with a non-JSON body"
            ) from None

        if not isinstance(parsed, dict):
            # The Proxy's envelope is always a JSON object; anything else
            # (e.g. a bare list or string) means something other than the
            # Proxy answered — a misconfigured intermediary, not a Proxy
            # response. Fail the same way a non-JSON body does, rather than
            # let `.get` below raise an unrelated AttributeError.
            raise ProxyUnreachableError(
                f"HTTP {response.status_code} with an unexpected JSON shape "
                f"({type(parsed).__name__}, expected an object)"
            )

        if response.status_code >= 400 or "error" in parsed:
            err = parsed.get("error", {})
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            raise from_response(
                err.get("code", f"HTTP_{response.status_code}"),
                err.get("message", "the Proxy rejected the request"),
                detail=err.get("detail"),
                request_id=err.get("request_id", ""),
                retry_after=retry_after,
            )

        return ProxyResult(
            parsed.get("data", {}),
            idempotent_replay=response.headers.get("X-Idempotent-Replay") == "1",
        )

    # -- the two network calls -----------------------------------------------

    def call(
        self,
        exchange: str,
        action: str,
        payload: dict[str, Any],
        *,
        idempotency_key: str = "",
    ) -> ProxyResult:
        """Run one action through the Proxy and return the exchange's body.

        Returns the `data` field of the Proxy's envelope as a `ProxyResult`
        (a `dict` subclass) — exactly what the venue replied, unreshaped, so
        callers keep whatever parsing they already have for that exchange's
        response shape. Check `.idempotent_replay` if you need to know
        whether this was a fresh execution or the cached result of an
        earlier identical request.

        Raises a `ProxyError` subclass from `errors.py` on any rejection —
        by the Proxy, by the exchange, or by the transport (unreachable /
        non-JSON response, raised as `ProxyUnreachableError`).

        `exchange` and `action` are not validated against the Proxy's
        allowlist here — that allowlist (`proxy/actions.py`) is a reviewed
        security boundary, and a client-side copy would drift from it in
        whichever direction is worse (silently blocking a newly-permitted
        action, or giving false confidence about one the server has since
        pulled). An invalid action still fails fast: the server rejects it
        at the cheapest step in its pipeline, before any exchange call.
        """
        path, body, headers = self._prepare(exchange, action, payload, idempotency_key)
        try:
            # `content=body`, never `data=` (httpx's `data=` form-encodes a
            # dict) and never `json=`: either would re-serialize and the
            # bytes on the wire would stop matching the bytes that were
            # signed.
            response = self._client.post(
                f"{self._base}{path}", content=body, headers=headers
            )
        except httpx.RequestError as exc:
            raise ProxyUnreachableError(str(exc)) from exc
        return self._handle_response(response)

    async def acall(
        self,
        exchange: str,
        action: str,
        payload: dict[str, Any],
        *,
        idempotency_key: str = "",
    ) -> ProxyResult:
        """`call()`'s async counterpart, on `httpx.AsyncClient` — real async
        I/O, not `call()` run in a thread."""
        path, body, headers = self._prepare(exchange, action, payload, idempotency_key)
        try:
            response = await self._aclient.post(
                f"{self._base}{path}", content=body, headers=headers
            )
        except httpx.RequestError as exc:
            raise ProxyUnreachableError(str(exc)) from exc
        return self._handle_response(response)


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None
