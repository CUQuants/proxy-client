"""ProxyWebSocketClient — the WebSocket counterpart to `ProxyClient`.

Reaches trading-gateway's `/v1/{exchange}/ws` route: one signed handshake
(`compute_signature`, same as REST — see `signing.py`), then Kraken's own
`method`/`params`/`req_id` vocabulary travels verbatim (§4.3's pass-through
principle), plus two Proxy-only control frames (`op:"ping"` /
`event:"pong"`, and the `event:"auth"` handshake reply).

Both `xlrts` and `speedbyte` currently hand-roll this handshake in a local
`ProxySession`-style class (`ws_url()` / `ws_auth_frame()`), each importing
`compute_signature` directly. This module is the one shared implementation
those should eventually delegate to instead.

## Scope — mechanics vs. policy

A WebSocket connection dropping isn't a business decision the way retrying
a mutating REST call is (`ProxyClient.call()` deliberately never
auto-retries — see its module docstring). Keeping the socket alive has no
business meaning, so this class *does* own the reconnect loop and the
heartbeat loop — but exposes their timing as plain parameters
(`reconnect`, `heartbeat_interval`/`heartbeat_timeout`), not baked-in
constants, since *how aggressive* is a per-deployment judgment call.

Routing an inbound frame to whichever handler is registered for its
channel is the same kind of mechanical, error-prone-to-hand-roll plumbing
as the handshake — so `add_handler()` / message dispatch stays here too.
What a registered handler *does* with a message is pure application logic
and none of this module's business; when a handler raises, `on_error` (an
injected callable, default: log and keep the connection alive) is the
seam, so one bad handler can't take down the read loop.

A *different* kind of error — the Proxy or the exchange rejecting a
request outright (bad auth, a rejected order, `RATE_LIMITED`) — is not a
callback matter; `request()` raises a typed exception synchronously, exact
parity with how `ProxyClient.call()` already works.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Callable

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "ProxyWebSocketClient requires the 'websockets' package. "
        "Install with: pip install cuq-proxy-client[websocket]"
    ) from _exc

from .errors import ProxyUnreachableError, from_response
from .idempotency import new_idempotency_key
from .signing import compute_signature

__all__ = [
    "ProxyWebSocketClient",
    "ReconnectBackoff",
    "default_backoff",
    "WebSocketRequestError",
]

logger = logging.getLogger(__name__)

# A channel handler receives the full parsed message dict; may be sync or async.
MessageHandler = Callable[[dict], Any]

# (attempt, seconds the connection that just ended stayed up, the error that
# ended it or None for a clean close) -> seconds to wait before reconnecting,
# or None to stop reconnecting for good.
ReconnectBackoff = Callable[[int, float, "BaseException | None"], "float | None"]

# Methods that place, change or cancel an order. The Proxy requires an
# idempotency key on place/cancel; sent on amend too since a retried amend
# is as capable of doubling up as a retried cancel.
ORDER_METHODS: frozenset[str] = frozenset(
    {"add_order", "amend_order", "edit_order", "cancel_order",
     "batch_add", "batch_cancel"}
)

# Distinguishes "the caller didn't pass reconnect=" from "the caller
# explicitly passed reconnect=None to disable it" — a constructed
# `default_backoff()` instance can't be the literal parameter default
# because it holds its own mutable streak state (see `default_backoff`
# below) and a default *expression* is evaluated once and shared across
# every instance that doesn't override it.
_UNSET = object()


def default_backoff(
    initial: float = 0.5,
    factor: float = 2.0,
    max_delay: float = 30.0,
    reset_after: float = 5.0,
) -> ReconnectBackoff:
    """Exponential backoff, capped, that forgives a connection that was
    actually healthy for a while before dropping.

    Mirrors `trading-gateway`'s own upstream reconnect policy
    (`BaseUpstream._supervise`: "backoff 0.5s -> x2 -> capped 30s, reset
    only if the connection stayed up >= 5s") so a caller who doesn't
    override anything gets behavior consistent with what the server side
    already does to its own upstreams.

    Holds a small mutable streak counter in its closure — call this once
    per `ProxyWebSocketClient` (the constructor does this for you when
    `reconnect` is left unset) rather than sharing one instance across
    clients, or their backoff streaks will bleed into each other.
    """
    streak = 0

    def _backoff(_attempt: int, connected_duration: float, _last_error: BaseException | None) -> float:
        # `_attempt`/`_last_error` are part of the `ReconnectBackoff`
        # signature for callers who want them; this policy tracks its own
        # streak from `connected_duration` instead (see docstring).
        nonlocal streak
        streak = 1 if connected_duration >= reset_after else streak + 1
        return min(initial * (factor ** (streak - 1)), max_delay)

    return _backoff


class WebSocketRequestError(RuntimeError):
    """A Kraken-native WS rejection (`{"success": false, "error": ...}`).

    Deliberately not a `ProxyError` subclass: that hierarchy mirrors the
    Proxy's own §9 error-code table (see `errors.py`), and a venue-level
    subscribe/method rejection isn't one of those codes — it's Kraken's
    own vocabulary, relayed verbatim per §4.3's pass-through principle.
    """

    def __init__(self, message: str, *, response: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.response = response


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


class ProxyWebSocketClient:
    """One caller's authenticated WebSocket connection to the Proxy.

    Construct via `.for_operator(...)` or `.for_system(...)` (equivalent
    to calling `ProxyWebSocketClient(...)` directly for the operator case)
    rather than passing `system_name` as a loose kwarg — there isn't one,
    for the same reason `ProxyClient` doesn't have one: an operator
    credential must never carry `system_name` on the wire and a system
    credential always must, and the server's rejection for getting it
    wrong is the same uninformative `AUTH_FAILED` either way.

    Usage:

        client = ProxyWebSocketClient.for_operator(
            base_url, api_key, secret, operator_id, operator_name,
        )

        async def on_book(msg: dict) -> None:
            for level in msg.get("data", []):
                ...

        client.add_handler("book", on_book)
        await client.start()
        await client.subscribe("book", {"symbol": ["BTC/USD"], "depth": 10})
        await client.run_forever()

    Subclassing and overriding `on_message()` works too, for a caller that
    wants everything in one place instead of per-channel handlers.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        secret: str,
        operator_id: str,
        operator_name: str,
        *,
        exchange: str = "kraken",
        ws_url: str = "",
        timeout: float = 15.0,
        reconnect: ReconnectBackoff | None = _UNSET,  # type: ignore[assignment]
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float = 10.0,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._exchange = exchange
        self._url = _build_ws_url(base_url, ws_url, exchange)
        self._api_key = api_key
        self._secret = secret.encode("utf-8")
        self.operator_id = operator_id
        self.operator_name = operator_name
        self._system_name: str | None = None
        self._timeout = timeout

        # See `_UNSET`'s docstring: a fresh `default_backoff()` per
        # instance, never one constructed at signature-definition time.
        self._reconnect: ReconnectBackoff | None = (
            default_backoff() if reconnect is _UNSET else reconnect
        )
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._on_error = on_error

        self._ws: Any = None
        self._connected_at: float | None = None

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._recv_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None

        self._req_id_counter = 0
        self._pending_requests: dict[int, asyncio.Future] = {}
        self._pong_waiter: asyncio.Future | None = None

        self._handlers: dict[str, list[MessageHandler]] = {}

        # (channel, params) recorded via subscribe(); replayed in order
        # after every reconnect.
        self._subscriptions: list[tuple[str, dict]] = []

    @classmethod
    def for_operator(
        cls,
        base_url: str,
        api_key: str,
        secret: str,
        operator_id: str,
        operator_name: str,
        *,
        exchange: str = "kraken",
        ws_url: str = "",
        timeout: float = 15.0,
        reconnect: ReconnectBackoff | None = _UNSET,  # type: ignore[assignment]
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float = 10.0,
        on_error: Callable[[Exception], None] | None = None,
    ) -> "ProxyWebSocketClient":
        """Identical to `ProxyWebSocketClient(...)` — exists so a call site
        reads symmetrically next to `for_system()`."""
        return cls(
            base_url, api_key, secret, operator_id, operator_name,
            exchange=exchange, ws_url=ws_url, timeout=timeout, reconnect=reconnect,
            heartbeat_interval=heartbeat_interval, heartbeat_timeout=heartbeat_timeout,
            on_error=on_error,
        )

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
        exchange: str = "kraken",
        ws_url: str = "",
        timeout: float = 15.0,
        reconnect: ReconnectBackoff | None = _UNSET,  # type: ignore[assignment]
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float = 10.0,
        on_error: Callable[[Exception], None] | None = None,
    ) -> "ProxyWebSocketClient":
        """Build a client authenticating as a system (bot) credential.

        `system_name` has no other entry point onto this class: this is
        the only constructor that sets it, so there's no way to end up
        with an operator-looking client that carries one by accident.
        """
        if not system_name:
            raise ValueError("system_name is required for a system credential")
        client = cls(
            base_url, api_key, secret, operator_id, operator_name,
            exchange=exchange, ws_url=ws_url, timeout=timeout, reconnect=reconnect,
            heartbeat_interval=heartbeat_interval, heartbeat_timeout=heartbeat_timeout,
            on_error=on_error,
        )
        client._system_name = system_name
        return client

    @property
    def system_name(self) -> str | None:
        """`None` for an operator client; the bot's registered name for a
        system client (set only via `for_system()`)."""
        return self._system_name

    @property
    def is_connected(self) -> bool:
        return self._ws_is_open()

    def __repr__(self) -> str:  # never let the secret reach a traceback or log
        who = self._system_name or self.operator_id
        return f"<ProxyWebSocketClient {who!r} via {self._url!r}>"

    # -- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Connect, complete the handshake, and start the background
        receive/heartbeat loops. Does not block — call `run_forever()` for
        that.

        The *first* connect is not retried: it either raises (a typed
        `ProxyError` on a rejected handshake, `ProxyUnreachableError` /
        `OSError` / `WebSocketException` on a transport failure) or
        succeeds. Only a connection that drops *after* a successful start
        goes through `reconnect`.
        """
        if self._running:
            raise RuntimeError("Client is already running.")
        self._running = True
        self._shutdown_event.clear()
        await self._connect()
        self._recv_task = asyncio.create_task(self._recv_loop(), name="proxy-ws-recv")
        if self._heartbeat_interval is not None:
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(), name="proxy-ws-heartbeat"
            )
        logger.info("ProxyWebSocketClient started - %s", self._url)

    async def stop(self) -> None:
        """Gracefully shut down. Safe to call multiple times."""
        if not self._running:
            return
        self._running = False
        self._shutdown_event.set()

        for task in (self._recv_task, self._heartbeat_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._ws_is_open():
            await self._ws.close()
        self._ws = None

        for fut in self._pending_requests.values():
            if not fut.done():
                fut.cancel()
        self._pending_requests.clear()
        logger.info("ProxyWebSocketClient stopped.")

    async def run_forever(self) -> None:
        """Block until `stop()` is called, or the connection ends for good
        (reconnect disabled, or the `reconnect` callable returned `None`)."""
        await self._shutdown_event.wait()

    async def __aenter__(self) -> "ProxyWebSocketClient":
        await self.start()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.stop()

    # -- messaging --------------------------------------------------------

    async def send(self, message: dict) -> None:
        if not self._ws_is_open():
            raise RuntimeError("WebSocket is not connected. Call start() first.")
        payload = json.dumps(message)
        logger.debug("WS <- %s", payload)
        await self._ws.send(payload)

    async def request(
        self,
        method: str,
        params: dict | None = None,
        timeout: float = 10.0,
        *,
        idempotency_key: str | None = None,
    ) -> dict:
        """Send a method request and await the correlated response.

        Order methods (`ORDER_METHODS`) get an idempotency key
        auto-generated via `idempotency.new_idempotency_key()` when none
        is supplied — **a retry must pass the key its first attempt
        used**; a fresh key per attempt defeats the mechanism.

        Raises a typed `ProxyError` subclass (`errors.from_response`) if
        the Proxy refused the frame itself, `WebSocketRequestError` if
        Kraken rejected it (`success: false`), or `asyncio.TimeoutError`
        if nothing came back within `timeout`.
        """
        req_id = self._next_req_id()
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_requests[req_id] = fut

        msg: dict[str, Any] = {"method": method, "req_id": req_id}
        if params:
            msg["params"] = dict(params)

        if method in ORDER_METHODS:
            # `req_id` is Kraken's, echoed on a subscribe-shaped ack; `id`
            # is the Proxy's own, because an order's venue-side req_id is
            # the Proxy's allocation, not the caller's.
            msg["id"] = req_id
            msg["idempotency_key"] = idempotency_key or new_idempotency_key()
        elif idempotency_key:
            msg["idempotency_key"] = idempotency_key

        try:
            await self.send(msg)
            response = await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            raise asyncio.TimeoutError(
                f"No response for '{method}' req_id={req_id} within {timeout}s"
            ) from None
        finally:
            self._pending_requests.pop(req_id, None)

        self._raise_for_error_response(method, response)
        return response

    @staticmethod
    def _raise_for_error_response(method: str, response: dict) -> None:
        """Two failure shapes reach here. The Proxy's own is
        `event: "error"` (it refused the frame itself, or relays a Proxy
        §9 code the exchange returned) — raised as a typed `ProxyError`
        subclass. Kraken's own is `success: false` on a relayed ack — not
        a Proxy code, so raised as `WebSocketRequestError` instead.
        """
        if response.get("event") == "error":
            data = response.get("data")
            err = data.get("error") if isinstance(data, dict) else None
            if isinstance(err, dict):
                code = err.get("code", "UNKNOWN")
                message = err.get("message", "the Proxy rejected the request")
                detail = err.get("detail")
            else:
                code = str(response.get("code", "UNKNOWN"))
                message = response.get("message", "the Proxy rejected the request")
                detail = data
            raise from_response(
                code, message, detail=detail, request_id=response.get("request_id", "")
            )

        if not response.get("success", True):
            error = response.get("error", "Unknown error")
            raise WebSocketRequestError(
                f"WS '{method}' failed: {error}", response=response
            )

    async def ping(self) -> float:
        """Send an application-level ping and return the round-trip
        latency in milliseconds. `{"op": "ping"}` is the Proxy's own
        control frame — it answers on every route with `{"event":
        "pong"}` and never forwards it upstream."""
        t0 = time.monotonic()
        await self._ping_once(timeout=self._heartbeat_timeout)
        return (time.monotonic() - t0) * 1000

    async def _ping_once(self, timeout: float) -> None:
        loop = asyncio.get_event_loop()
        waiter: asyncio.Future = loop.create_future()
        self._pong_waiter = waiter
        try:
            await self.send({"op": "ping"})
            await asyncio.wait_for(waiter, timeout=timeout)
        finally:
            if self._pong_waiter is waiter:
                self._pong_waiter = None

    # -- subscribe / unsubscribe ------------------------------------------

    async def subscribe(self, channel: str, params: dict | None = None, *, persist: bool = True) -> dict:
        """Subscribe to a channel. Recorded and replayed after a reconnect
        unless `persist=False`."""
        params = dict(params or {})
        params["channel"] = channel
        response = await self.request("subscribe", params=params)
        if persist:
            clean = {k: v for k, v in params.items() if k != "channel"}
            entry = (channel, clean)
            if entry not in self._subscriptions:
                self._subscriptions.append(entry)
        return response

    async def unsubscribe(self, channel: str, params: dict | None = None) -> dict:
        params = dict(params or {})
        params["channel"] = channel
        response = await self.request("unsubscribe", params=params)
        clean = {k: v for k, v in params.items() if k != "channel"}
        try:
            self._subscriptions.remove((channel, clean))
        except ValueError:
            pass
        return response

    # -- handler registration ----------------------------------------------

    def add_handler(self, channel: str, handler: MessageHandler) -> None:
        """Register a callback for messages on `channel`. Use `"*"` for a
        wildcard (every message). Multiple handlers per channel are called
        in registration order; an exception from one goes to `on_error`
        rather than stopping the others or the read loop."""
        self._handlers.setdefault(channel, []).append(handler)

    def remove_handler(self, channel: str, handler: MessageHandler) -> None:
        handlers = self._handlers.get(channel, [])
        try:
            handlers.remove(handler)
        except ValueError:
            pass

    def clear_handlers(self, channel: str | None = None) -> None:
        if channel is None:
            self._handlers.clear()
        else:
            self._handlers.pop(channel, None)

    async def on_message(self, _message: dict) -> None:
        """Called for every inbound message before channel handlers.
        Override in a subclass; default is a no-op."""

    # -- connection state helper -------------------------------------------

    def _ws_is_open(self) -> bool:
        """True if the WebSocket connection is currently open. Handles the
        API difference between `websockets` versions: <13 exposes a
        `.closed` bool, >=13 exposes `.close_code` (None = still open)."""
        if self._ws is None:
            return False
        closed_attr = getattr(self._ws, "closed", None)
        if closed_attr is not None:
            return not bool(closed_attr)
        return getattr(self._ws, "close_code", None) is None

    # -- connect / authenticate ---------------------------------------------

    async def _connect(self) -> None:
        logger.info("Connecting to %s ...", self._url)
        self._ws = await websockets.connect(
            self._url,
            ping_interval=None,   # heartbeat is driven by _heartbeat_loop
            ping_timeout=None,
            close_timeout=5,
            max_size=2**23,       # 8 MB - adequate for deep book snapshots
        )
        await self._authenticate()
        self._connected_at = time.monotonic()
        logger.info("Connected and authenticated.")

    async def _authenticate(self) -> None:
        """Send the signed handshake and wait for the Proxy to accept it.

        Read synchronously, before the receive loop starts, so the ack
        can't race the dispatcher. §9 returns a uniform `AUTH_FAILED` for
        a bad key, bad secret, an operator id that doesn't belong to the
        key, and a clock more than 5s out — the Proxy won't say which.
        """
        await self._ws.send(_build_auth_frame(
            self._api_key, self._secret, self.operator_id, self.operator_name,
            self._system_name, self._exchange,
        ))

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

    # -- receive loop / reconnect -------------------------------------------

    async def _recv_loop(self) -> None:
        attempt = 0
        while self._running:
            last_error: BaseException | None = None
            try:
                async for raw in self._ws:
                    if not self._running:
                        return
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("Non-JSON message received: %s", raw[:200])
                        continue
                    logger.debug("WS -> %s", raw[:400])
                    await self._dispatch(msg)
                if self._running:
                    logger.warning("Server closed the WebSocket connection.")
            except ConnectionClosed as exc:
                last_error = exc
                if not self._running:
                    return
                logger.warning("Connection closed: %s", exc)
            except WebSocketException as exc:
                last_error = exc
                if not self._running:
                    return
                logger.error("WebSocket error: %s", exc)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                last_error = exc
                if not self._running:
                    return
                logger.exception("Unexpected error in recv loop")

            if not self._running:
                return

            connected_duration = (
                time.monotonic() - self._connected_at if self._connected_at is not None else 0.0
            )

            if self._reconnect is None:
                logger.info("Connection ended; reconnect is disabled.")
                self._running = False
                self._shutdown_event.set()
                return

            attempt += 1
            delay = self._reconnect(attempt, connected_duration, last_error)
            if delay is None:
                logger.info("reconnect() stopped retrying after %d attempt(s).", attempt)
                self._running = False
                self._shutdown_event.set()
                return

            if delay > 0:
                logger.info("Reconnecting in %.1fs (attempt %d) ...", delay, attempt)
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return

            if not self._running:
                return

            try:
                await self._connect()
                # `attempt` is intentionally *not* reset here on a bare
                # successful connect — a connection that authenticates and
                # then drops again immediately shouldn't get a cheap
                # backoff reset just because it briefly connected. Reset
                # is `reconnect`'s call to make from `connected_duration`
                # on the *next* drop (see `default_backoff`, which resets
                # its own internal streak only once a connection stayed
                # up past `reset_after`).
                await self._replay_subscriptions()
            except Exception as exc:
                logger.error("Reconnect attempt failed: %s", exc)
                # Loop again: the next `async for raw in self._ws` fails
                # fast on the dead/absent connection, which funnels back
                # through this same reconnect computation.

    async def _replay_subscriptions(self) -> None:
        if not self._subscriptions:
            return
        logger.info("Replaying %d subscription(s) after reconnect ...", len(self._subscriptions))
        for channel, params in list(self._subscriptions):
            try:
                await self.subscribe(channel, params, persist=False)
            except Exception as exc:
                logger.error("Failed to replay '%s': %s", channel, exc)
            await asyncio.sleep(0.1)  # small gap between resubscribes

    # -- heartbeat ------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        # Only ever scheduled from start() when heartbeat_interval is not
        # None (see start()); asserted here so the type checker knows it
        # too, since the attribute itself is typed `float | None`.
        interval = self._heartbeat_interval
        assert interval is not None
        while self._running:
            try:
                await asyncio.sleep(interval)
                if not self._running:
                    return
                try:
                    await self._ping_once(timeout=self._heartbeat_timeout)
                except RuntimeError:
                    # Not connected right now; _recv_loop owns reconnecting.
                    await asyncio.sleep(1)
                    continue
                except asyncio.TimeoutError:
                    logger.warning(
                        "Heartbeat timed out after %ds - closing to trigger reconnect",
                        self._heartbeat_timeout,
                    )
                    if self._ws_is_open():
                        await self._ws.close()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Unexpected error in heartbeat loop")
                await asyncio.sleep(1)

    # -- dispatch ---------------------------------------------------------

    async def _dispatch(self, msg: dict) -> None:
        try:
            result = self.on_message(msg)
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:
            self._handle_callback_error(exc, msg)

        event = msg.get("event")

        if event == "pong":
            waiter = self._pong_waiter
            if waiter is not None and not waiter.done():
                waiter.set_result(msg)
            return

        if event in ("result", "error"):
            ident = msg.get("id")
            if ident is not None and ident in self._pending_requests:
                fut = self._pending_requests.get(ident)
                if fut and not fut.done():
                    fut.set_result(msg)
                return
            if event == "error" and msg.get("req_id") is None:
                logger.error("Proxy error with no correlation id: %s", msg)
                return

        req_id = msg.get("req_id")
        if req_id is not None and req_id in self._pending_requests:
            fut = self._pending_requests.get(req_id)
            if fut and not fut.done():
                fut.set_result(msg)
            return

        # A method response with no req_id (e.g. a malformed-frame error)
        # routes to the sole pending future so its caller doesn't just
        # time out.
        if "method" in msg and req_id is None and self._pending_requests:
            if len(self._pending_requests) == 1:
                _, fut = next(iter(self._pending_requests.items()))
                if not fut.done():
                    fut.set_result(msg)
                return
            logger.warning(
                "Method response without req_id with %d pending requests - cannot route: %s",
                len(self._pending_requests), msg,
            )

        channel = msg.get("channel")

        if channel == "heartbeat" and "heartbeat" not in self._handlers:
            return

        if channel and channel in self._handlers:
            for handler in list(self._handlers[channel]):
                try:
                    result = handler(msg)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    self._handle_callback_error(exc, msg)

        for handler in list(self._handlers.get("*", [])):
            try:
                result = handler(msg)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                self._handle_callback_error(exc, msg)

    def _handle_callback_error(self, exc: Exception, msg: dict) -> None:
        if self._on_error is not None:
            try:
                self._on_error(exc)
            except Exception:
                logger.exception("on_error callback itself raised")
        else:
            logger.exception("Handler raised an unhandled exception for message: %s", msg)

    # -- utilities --------------------------------------------------------

    def _next_req_id(self) -> int:
        self._req_id_counter += 1
        return self._req_id_counter
