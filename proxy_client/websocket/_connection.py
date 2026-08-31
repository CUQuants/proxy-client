"""One authenticated connection's lifetime — and nothing else.

`_Connection` opens the socket, performs the §5.2 signed handshake, and
then just relays: `send()` a dict, iterate inbound raw frames, `close()`.
It owns no reconnect logic, no request correlation, no channel handlers —
those belong to the supervisor in ``client.py``. A dropped connection is
discarded and a fresh `_Connection` built for the next attempt, so
`connected_at` is always this connection's, never a stale value.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Callable

import websockets

from ..errors import ProxyUnreachableError, from_response
from ..signing import compute_signature

__all__ = ["_Connection", "_build_ws_url", "_build_auth_frame"]

logger = logging.getLogger(__name__)


def _build_ws_url(base_url: str, ws_url_override: str, exchange: str) -> str:
    base = (ws_url_override or base_url).rstrip("/")
    base = base.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    return f"{base}/v1/{exchange}/ws"


def _build_auth_frame(
    api_key: str,
    secret: bytes,
    operator_id: str,
    operator_name: str,
    system_name: str | None,
    exchange: str,
) -> str:
    """The §5.2 handshake, signed exactly like a REST request.

    The signed string is transported **verbatim** as `body` — a frame that
    contains its own signature can't be signed as-is, so `body` is a
    string and whatever string was signed is the string that arrives.
    `method` is the literal `"WS"` so a captured REST signature can't be
    replayed as a handshake, or the reverse.

    Called fresh for every connect (including reconnects): `timestamp` and
    `nonce` must be current or the Proxy rejects the frame as stale, so an
    old signature can never be replayed onto a new socket.
    """
    path = f"/v1/{exchange}/ws"
    timestamp = str(int(time.time()))
    nonce = uuid.uuid4().hex
    inner: dict[str, Any] = {
        "api_key": api_key,
        "timestamp": timestamp,
        "nonce": nonce,
        "operator_id": operator_id,
        "operator_name": operator_name,
    }
    if system_name:
        inner["system_name"] = system_name
    body = json.dumps(inner)
    signature = compute_signature(secret, "WS", path, timestamp, nonce, body.encode())
    return json.dumps({"op": "auth", "body": body, "signature": signature})


class _Connection:
    """A single live socket to the Proxy's `/v1/{exchange}/ws` route.

    :param url: the fully built ``wss://.../v1/{exchange}/ws`` endpoint.
    :param auth_frame_factory: called once per :meth:`open` to produce the
        signed handshake string — a factory, not a value, because every
        connect needs a fresh timestamp/nonce (see :func:`_build_auth_frame`).
    :param timeout: seconds to wait for the handshake ack.
    """

    def __init__(
        self,
        url: str,
        auth_frame_factory: Callable[[], str],
        *,
        timeout: float,
    ) -> None:
        self._url = url
        self._auth_frame_factory = auth_frame_factory
        self._timeout = timeout
        self._ws: Any = None
        self.connected_at: float | None = None

    @property
    def is_open(self) -> bool:
        """True while the socket is usable. Absorbs the `websockets` API
        difference: <13 exposes a `.closed` bool, >=13 exposes
        `.close_code` (None = still open)."""
        if self._ws is None:
            return False
        closed_attr = getattr(self._ws, "closed", None)
        if closed_attr is not None:
            return not bool(closed_attr)
        return getattr(self._ws, "close_code", None) is None

    async def open(self) -> None:
        """Connect and complete the handshake. Raises a typed `ProxyError`
        on a rejected handshake, or `ProxyUnreachableError` /
        `WebSocketException` / `OSError` on a transport failure."""
        logger.info("Connecting to %s ...", self._url)
        self._ws = await websockets.connect(
            self._url,
            ping_interval=None,   # heartbeat is driven by the client's loop
            ping_timeout=None,
            close_timeout=5,
            max_size=2**23,       # 8 MB - adequate for deep book snapshots
        )
        await self._authenticate()
        self.connected_at = time.monotonic()
        logger.info("Connected and authenticated.")

    async def _authenticate(self) -> None:
        """Send the signed handshake and wait for the Proxy to accept it.

        Read synchronously, before the client's receive loop starts, so
        the ack can't race the dispatcher. §9 returns a uniform
        `AUTH_FAILED` for a bad key, bad secret, an operator id that
        doesn't belong to the key, and a clock more than 5s out — the
        Proxy won't say which.
        """
        await self._ws.send(self._auth_frame_factory())

        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=self._timeout)
        except asyncio.TimeoutError:
            raise ProxyUnreachableError(
                f"The Proxy did not answer the auth handshake within "
                f"{self._timeout:.0f}s ({self._url})."
            ) from None

        try:
            reply = json.loads(raw)
        except json.JSONDecodeError:
            raise ProxyUnreachableError(
                f"Non-JSON reply to the auth handshake: {raw[:200]}"
            ) from None

        if reply.get("event") != "auth":
            raise from_response(
                reply.get("code", "AUTH_FAILED"),
                reply.get("message", "the Proxy refused the WebSocket handshake"),
                request_id=reply.get("request_id", ""),
            )

        logger.info("Authenticated to the Proxy as %s.", reply.get("operator_id", "?"))

    async def send(self, message: dict) -> None:
        payload = json.dumps(message)
        logger.debug("WS <- %s", payload)
        await self._ws.send(payload)

    async def close(self) -> None:
        if self.is_open:
            await self._ws.close()
        self._ws = None

    def __aiter__(self):
        """Iterate inbound raw frames. Raises `ConnectionClosed` /
        `WebSocketException` when the socket ends, exactly as iterating the
        underlying `websockets` connection does."""
        if self._ws is None:
            raise RuntimeError("Connection is not open.")
        return self._ws.__aiter__()
