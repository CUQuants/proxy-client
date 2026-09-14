"""ReportingClient — the audit-reporting API's call() surface.

A second, smaller client alongside `ProxyClient`, not a mode of it. It talks
to `proxy_admin.reporting` (`trading-gateway`'s read-only audit-log API,
`/v1/admin/audit/*`), which is a **different service** from the Proxy
(`/v1/{exchange}`) — separate process, separate default port (`:8090`), and
by design no reverse-proxy route the way the Proxy has one, so `base_url`
here is very unlikely to be the same value a `ProxyClient` in the same
process was built with.

It reuses exactly two things from the rest of this package, unchanged:
`signing.compute_signature` (identical HMAC — the reporting API's
`Authenticator` is the Proxy's own, reused server-side) and every exception
in `errors.py` (the reporting API raises a subset of the same `proxy.errors`
codes: `AUTH_FAILED`, `CREDENTIAL_EXPIRED`, `ACTION_NOT_PERMITTED`,
`MALFORMED_REQUEST` — all already modeled there). Everything else is
deliberately its own, smaller implementation rather than a `ProxyClient`
subclass or a new mode on it:

- **The envelope has no `exchange`/`action`.** `envelope.build_body`
  requires both; the reporting API's body is just `{operator_id,
  operator_name}` plus an optional `payload` (filters, `/logs` only) — see
  `_body()` below.
- **The response isn't wrapped in `"data"`.** The Proxy answers
  `{"data": <result>}` (`trading-gateway/proxy/pipeline.py`); every
  reporting route answers the result directly at the top level (e.g.
  `{"unresolved_orders": [...]}`). Reusing `ProxyClient._handle_response`
  here would silently return `{}` on every call — it only ever looks for a
  `"data"` key that doesn't exist. `_handle_response` below is the same
  shape (still classifies `{"error": {...}}` via `errors.from_response`,
  still guards non-JSON / non-object bodies) with that one line changed.
- **No idempotency keys.** Every endpoint here is `POST` but none of them
  mutate anything (`app.py`'s module docstring on the server: POST only
  because attribution travels in the JSON body, not because these write
  data) — there's no `X-Idempotency-Key` concept to carry.
- **Operator credentials only — no `for_system()`.** The server's
  `require_auditor` check technically doesn't care which credential type
  authenticated (it only inspects the attributed `operator_id`'s
  `admin_role`), but "may view the audit log" is a property of a person,
  not a bot, so this class doesn't offer a system-credential constructor
  even though the wire protocol would technically allow one through it.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime
from typing import Any

import httpx

from .errors import ProxyUnreachableError, from_response
from .signing import compute_signature

__all__ = ["ReportingClient"]

_ENDPOINTS = {
    "unresolved": "/v1/admin/audit/unresolved",
    "health": "/v1/admin/audit/health",
    "auth_failures": "/v1/admin/audit/auth-failures",
    "logs": "/v1/admin/audit/logs",
}

#: Keyword arguments `query_logs()`/`aquery_logs()` accept, in the order
#: `LogFilters` declares them (`trading-gateway/proxy_admin/reporting/db.py`)
#: — kept in one place so the two methods (sync/async) can't drift apart on
#: which filters they support.
_LOG_FILTER_KEYS = (
    "since",
    "until",
    "operator_id",
    "operator_name",
    "exchange",
    "action",
    "response_status",
    "error_code",
    "api_key_id",
    "source_ip",
    "request_id",
    "limit",
    "cursor",
)


def _log_filters_payload(**kwargs: Any) -> dict[str, Any]:
    """Build the `/logs` `payload` object from `query_logs()`'s keyword
    arguments: `datetime` values become ISO-8601 strings, and anything left
    `None` is dropped rather than sent as `null` — the server's own
    `parse_log_filters` treats an absent key as "no filter on this column,"
    so there's no difference in outcome, only in payload size."""
    payload: dict[str, Any] = {}
    for key in _LOG_FILTER_KEYS:
        value = kwargs[key]
        if value is None:
            continue
        if isinstance(value, datetime):
            value = value.isoformat()
        payload[key] = value
    return payload


class ReportingClient:
    """One auditor's authenticated session against the audit-reporting API.

    Holds `secret` in memory only, for the life of this object — same
    posture as `ProxyClient`, including the scrubbed `__repr__` (never log
    or print a `ReportingClient`). Owns both a sync (`httpx.Client`) and an
    async (`httpx.AsyncClient`) transport; call `close()` / `aclose()` (or
    use as a context manager) to release their connection pools.

    Construct via `ReportingClient.for_operator(...)` (equivalent to calling
    `ReportingClient(...)` directly — the classmethod exists only so a call
    site reads the same way a `ProxyClient.for_operator(...)` one does).
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
    ) -> "ReportingClient":
        """Identical to `ReportingClient(...)` — see the class docstring for
        why there is no `for_system()` counterpart."""
        return cls(base_url, api_key, secret, operator_id, operator_name, timeout=timeout)

    def __repr__(self) -> str:
        return f"<ReportingClient {self.operator_id!r} via {self._base!r}>"

    def close(self) -> None:
        self._client.close()

    async def aclose(self) -> None:
        await self._aclient.aclose()

    def __enter__(self) -> "ReportingClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    async def __aenter__(self) -> "ReportingClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    # -- request/response, shared by every endpoint below --------------------

    def _body(self, payload: dict[str, Any] | None) -> bytes:
        """Serialize once — this exact byte string is both what's sent and
        what's signed, same discipline as `envelope.build_body`.

        `payload` is omitted from the body entirely when `None` (the three
        fixed insight endpoints never read it) rather than sent as `{}` —
        smallest body that says what it means, matching `build_body`'s own
        `system_name`-when-absent convention.
        """
        body: dict[str, Any] = {
            "operator_id": self.operator_id,
            "operator_name": self.operator_name,
        }
        if payload is not None:
            body["payload"] = payload
        return json.dumps(body).encode("utf-8")

    def _prepare(
        self, endpoint: str, payload: dict[str, Any] | None
    ) -> tuple[str, bytes, dict[str, str]]:
        path = _ENDPOINTS[endpoint]
        body = self._body(payload)

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
        return path, body, headers

    def _handle_response(self, response: httpx.Response) -> dict[str, Any]:
        """`ProxyClient._handle_response`, minus the `data` unwrap — see the
        module docstring for why that's the one thing that has to differ."""
        try:
            parsed = response.json()
        except ValueError:
            raise ProxyUnreachableError(
                f"HTTP {response.status_code} with a non-JSON body"
            ) from None

        if not isinstance(parsed, dict):
            raise ProxyUnreachableError(
                f"HTTP {response.status_code} with an unexpected JSON shape "
                f"({type(parsed).__name__}, expected an object)"
            )

        if response.status_code >= 400 or "error" in parsed:
            err = parsed.get("error", {})
            raise from_response(
                err.get("code", f"HTTP_{response.status_code}"),
                err.get("message", "the reporting API rejected the request"),
                detail=err.get("detail"),
                request_id=err.get("request_id", ""),
            )

        return parsed

    def _call(self, endpoint: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        path, body, headers = self._prepare(endpoint, payload)
        try:
            response = self._client.post(f"{self._base}{path}", content=body, headers=headers)
        except httpx.RequestError as exc:
            raise ProxyUnreachableError(str(exc)) from exc
        return self._handle_response(response)

    async def _acall(
        self, endpoint: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        path, body, headers = self._prepare(endpoint, payload)
        try:
            response = await self._aclient.post(
                f"{self._base}{path}", content=body, headers=headers
            )
        except httpx.RequestError as exc:
            raise ProxyUnreachableError(str(exc)) from exc
        return self._handle_response(response)

    # -- the four endpoints, sync + async ------------------------------------

    def unresolved_orders(self) -> list[dict[str, Any]]:
        """Orders forwarded to an exchange whose outcome was never recorded.

        Returns the `unresolved_orders` list — each entry has `request_id`,
        `timestamp`, `operator_id`, `operator_name`, `system_name`,
        `exchange`, `action`, `source_ip`, `unresolved_for`.
        """
        return self._call("unresolved")["unresolved_orders"]

    async def aunresolved_orders(self) -> list[dict[str, Any]]:
        return (await self._acall("unresolved"))["unresolved_orders"]

    def log_health(self) -> dict[str, Any]:
        """Partition runway, clock skew, default-partition drift — one
        object: `months_of_runway`, `default_partition_rows`,
        `unprotected_partitions`, `rows_last_24h`, `newest_row`,
        `max_clock_skew_sec`."""
        return self._call("health")

    async def alog_health(self) -> dict[str, Any]:
        return await self._acall("health")

    def auth_failures(self) -> list[dict[str, Any]]:
        """Authentication failures in the last 7 days, by key and origin.

        Returns the `auth_failures` list — each entry has `api_key_id`,
        `source_ip`, `failures`, `latest`.
        """
        return self._call("auth_failures")["auth_failures"]

    async def aauth_failures(self) -> list[dict[str, Any]]:
        return (await self._acall("auth_failures"))["auth_failures"]

    def query_logs(
        self,
        *,
        since: str | datetime | None = None,
        until: str | datetime | None = None,
        operator_id: str | None = None,
        operator_name: str | None = None,
        exchange: str | None = None,
        action: str | None = None,
        response_status: str | None = None,
        error_code: str | None = None,
        api_key_id: str | None = None,
        source_ip: str | None = None,
        request_id: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Filtered, paginated browse over `request_log`.

        Every argument is optional and maps to one `LogFilters` field
        server-side (`trading-gateway/proxy_admin/reporting/db.py`); leaving
        one `None` means "no filter on this column," not "match null."
        `since`/`until` accept a `datetime` directly (encoded with
        `.isoformat()`) as well as a preformatted ISO-8601 string.

        Returns `{"logs": [...], "next_cursor": str | None}` — pass a
        non-`None` `next_cursor` back as `cursor` to fetch the next page.
        `limit` is capped at 200 server-side; leaving it `None` uses the
        server's configured default.
        """
        payload = _log_filters_payload(
            since=since,
            until=until,
            operator_id=operator_id,
            operator_name=operator_name,
            exchange=exchange,
            action=action,
            response_status=response_status,
            error_code=error_code,
            api_key_id=api_key_id,
            source_ip=source_ip,
            request_id=request_id,
            limit=limit,
            cursor=cursor,
        )
        return self._call("logs", payload)

    async def aquery_logs(
        self,
        *,
        since: str | datetime | None = None,
        until: str | datetime | None = None,
        operator_id: str | None = None,
        operator_name: str | None = None,
        exchange: str | None = None,
        action: str | None = None,
        response_status: str | None = None,
        error_code: str | None = None,
        api_key_id: str | None = None,
        source_ip: str | None = None,
        request_id: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """`query_logs()`'s async counterpart — see it for the argument and
        return shape."""
        payload = _log_filters_payload(
            since=since,
            until=until,
            operator_id=operator_id,
            operator_name=operator_name,
            exchange=exchange,
            action=action,
            response_status=response_status,
            error_code=error_code,
            api_key_id=api_key_id,
            source_ip=source_ip,
            request_id=request_id,
            limit=limit,
            cursor=cursor,
        )
        return await self._acall("logs", payload)
