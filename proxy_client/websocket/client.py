""":class:`ProxyWebSocketClient` — reconnect supervisor + pub/sub dispatch.

The transport (:class:`._connection._Connection`), the request/response
correlation (:class:`._rpc._PendingRequests`) and the reconnect timing
(:mod:`._backoff`) each live in their own module; this one wires them
together and owns what's left: deciding *when* to reconnect, the
heartbeat loop, the subscription registry that gets replayed after a
reconnect, the channel-handler registry, and the public API.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable

from websockets.exceptions import ConnectionClosed, WebSocketException

from ..idempotency import new_idempotency_key
from ._backoff import ReconnectBackoff, default_backoff
from ._connection import _build_auth_frame, _build_ws_url, _Connection
from ._dialect import Dialect, WebSocketRequestError, dialect_for_exchange
from ._rpc import _PendingRequests

__all__ = ["ProxyWebSocketClient", "WebSocketRequestError"]

logger = logging.getLogger(__name__)

# A channel handler receives the full parsed message dict; may be sync or async.
MessageHandler = Callable[[dict], Any]

# A connection that stays up at least this long before dropping resets the
# reconnect attempt counter, so its next backoff starts from the bottom.
# Matches the old `default_backoff(reset_after=5.0)`.
_RECONNECT_RESET_AFTER = 5.0

# Methods that place, change or cancel an order. The Proxy requires an
# idempotency key on place/cancel; sent on amend too since a retried amend
# is as capable of doubling up as a retried cancel.
ORDER_METHODS: frozenset[str] = frozenset(
    {"add_order", "amend_order", "edit_order", "cancel_order",
     "batch_add", "batch_cancel"}
)


def _as_connection_error(exc: BaseException | None) -> ConnectionError:
    """Normalise whatever ended a connection into a single `ConnectionError`
    so an in-flight `request()` caller has one predictable type to catch.
    The original (a `websockets` `ConnectionClosed`, an `OSError`, ...) is
    chained as `__cause__`."""
    if isinstance(exc, ConnectionError):
        return exc
    err = ConnectionError(
        "WebSocket connection ended"
        if exc is None
        else f"WebSocket connection lost: {exc}"
    )
    err.__cause__ = exc
    return err


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
        reconnect: ReconnectBackoff | None = default_backoff,
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float = 10.0,
        on_error: Callable[[Exception], None] | None = None,
        dialect: Dialect | None = None,
    ) -> None:
        self._exchange = exchange
        # The wire vocabulary for this venue. Defaults from `exchange`;
        # pass `dialect=` for one the SDK has no built-in for.
        self._dialect = dialect if dialect is not None else dialect_for_exchange(exchange)
        self._url = _build_ws_url(base_url, ws_url, exchange)
        self._api_key = api_key
        self._secret = secret.encode("utf-8")
        self.operator_id = operator_id
        self.operator_name = operator_name
        self._system_name: str | None = None
        self._timeout = timeout

        self._reconnect = reconnect
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._on_error = on_error

        self._conn: _Connection | None = None

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._supervise_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None

        self._pending = _PendingRequests()

        self._handlers: dict[str, list[MessageHandler]] = {}

        # (channel, params) recorded via subscribe(); replayed in order
        # after every reconnect.
        self._subscriptions: list[tuple[str, dict]] = []

    def _auth_frame(self) -> str:
        # Read `self._system_name` at call time, not construction time:
        # `for_system()` sets it after `__init__` returns.
        return _build_auth_frame(
            self._api_key, self._secret, self.operator_id, self.operator_name,
            self._system_name, self._exchange,
        )

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
        reconnect: ReconnectBackoff | None = default_backoff,
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float = 10.0,
        on_error: Callable[[Exception], None] | None = None,
        dialect: Dialect | None = None,
    ) -> "ProxyWebSocketClient":
        """Identical to `ProxyWebSocketClient(...)` — exists so a call site
        reads symmetrically next to `for_system()`."""
        return cls(
            base_url, api_key, secret, operator_id, operator_name,
            exchange=exchange, ws_url=ws_url, timeout=timeout, reconnect=reconnect,
            heartbeat_interval=heartbeat_interval, heartbeat_timeout=heartbeat_timeout,
            on_error=on_error, dialect=dialect,
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
        reconnect: ReconnectBackoff | None = default_backoff,
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float = 10.0,
        on_error: Callable[[Exception], None] | None = None,
        dialect: Dialect | None = None,
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
            on_error=on_error, dialect=dialect,
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
        return self._conn is not None and self._conn.is_open

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

        self._conn = _Connection(self._url, self._auth_frame, timeout=self._timeout)
        await self._conn.open()

        self._supervise_task = asyncio.create_task(
            self._supervise(), name="proxy-ws-supervise"
        )
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

        for task in (self._supervise_task, self._heartbeat_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._conn is not None:
            await self._conn.close()
        self._conn = None

        self._pending.cancel_all()
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
        # Bind the connection once: a reconnect can swap `self._conn` across
        # the await below, and the caller is promised `RuntimeError` on a
        # closed socket, not an `AttributeError` on a `None` that appeared
        # mid-call.
        conn = self._conn
        if conn is None or not conn.is_open:
            raise RuntimeError("WebSocket is not connected. Call start() first.")
        await conn.send(message)

    async def request(
        self,
        method: str,
        params: dict | None = None,
        timeout: float = 10.0,
        *,
        idempotency_key: str | None = None,
    ) -> dict:
        """Send a method request and await the correlated response.

        `method` is `"subscribe"` / `"unsubscribe"` or an order method
        (`ORDER_METHODS`). The active dialect (see `_dialect`) frames it on
        the wire; the OKX dialect raises `NotImplementedError` for an
        order method, since OKX order placement over WS isn't wired up yet.

        Order methods get an idempotency key auto-generated via
        `idempotency.new_idempotency_key()` when none is supplied — **a
        retry must pass the key its first attempt used**; a fresh key per
        attempt defeats the mechanism.

        Raises a typed `ProxyError` subclass if the Proxy refused the
        frame itself, `WebSocketRequestError` if Kraken rejected it
        (`success: false`), `asyncio.TimeoutError` if nothing came back
        within `timeout`, or `ConnectionError` (with the underlying
        transport error chained as `__cause__`) if the connection dropped
        while the request was in flight.
        """
        is_order = method in ORDER_METHODS
        if is_order:
            idempotency_key = idempotency_key or new_idempotency_key()

        req_id = self._pending.next_id()
        frame, key = self._dialect.request_frame(
            method, params, req_id, idempotency_key, is_order=is_order
        )
        fut = self._pending.create(key)

        try:
            await self.send(frame)
            response = await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(
                f"No response for '{method}' req_id={req_id} within {timeout}s"
            ) from None
        finally:
            self._pending.discard(key)

        exc = self._dialect.error_of(response)
        if exc is not None:
            raise exc
        return response

    async def ping(self) -> float:
        """Send an application-level ping and return the round-trip
        latency in milliseconds. `{"op": "ping"}` is the Proxy's own
        control frame — it answers on every route with `{"event":
        "pong"}` and never forwards it upstream."""
        t0 = time.monotonic()
        await self._ping_once(timeout=self._heartbeat_timeout)
        return (time.monotonic() - t0) * 1000

    async def _ping_once(self, timeout: float) -> None:
        fut = self._pending.expect_pong()
        try:
            await self.send({"op": "ping"})
            await asyncio.wait_for(fut, timeout=timeout)
        finally:
            # Pass our own future: if `ping()` and the heartbeat overlap, the
            # loser must not evict the winner's slot on its way out.
            self._pending.discard_pong(fut)

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

    # -- receive loop / reconnect -----------------------------------------

    async def _supervise(self) -> None:
        """Own the connection across its whole lifetime: pump inbound
        frames until it ends, then reconnect (or shut down) per the
        `reconnect` policy. The pumping, the decision, and the reconnect
        each live in their own method below."""
        attempt = 0
        while self._running:
            last_error = await self._pump()
            if not self._running:
                return

            # The socket - and every server-side req_id with it - is gone.
            # Fail in-flight request()s now rather than let their callers
            # wait out a timeout that can no longer succeed.
            self._pending.fail_all(_as_connection_error(last_error))

            decision = self._decide_reconnect(attempt, last_error)
            if decision is None:
                self._running = False
                self._shutdown_event.set()
                return
            attempt, delay = decision

            if delay > 0:
                logger.info("Reconnecting in %.1fs (attempt %d) ...", delay, attempt)
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return
            if not self._running:
                return

            await self._reconnect_once()

    async def _pump(self) -> BaseException | None:
        """Read and dispatch frames until the connection ends. Returns the
        exception that ended it, or ``None`` for a clean server-side
        close (or for shutdown mid-pump)."""
        try:
            assert self._conn is not None
            async for raw in self._conn:
                if not self._running:
                    return None
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("Non-JSON message received: %s", raw[:200])
                    continue
                logger.debug("WS -> %s", raw[:400])
                await self._dispatch(msg)
            if self._running:
                logger.warning("Server closed the WebSocket connection.")
            return None
        except asyncio.CancelledError:
            # stop() has already cleared _running; let the outer loop exit.
            return None
        except ConnectionClosed as exc:
            if self._running:
                logger.warning("Connection closed: %s", exc)
            return exc
        except WebSocketException as exc:
            if self._running:
                logger.error("WebSocket error: %s", exc)
            return exc
        except Exception as exc:  # noqa: BLE001 - funnel everything to reconnect
            if self._running:
                logger.exception("Unexpected error in recv loop")
            return exc

    def _decide_reconnect(
        self, attempt: int, last_error: BaseException | None
    ) -> tuple[int, float] | None:
        """Given the current attempt count and how the last connection
        ended, return ``(new_attempt, delay_seconds)`` — or ``None`` to
        stop reconnecting for good."""
        if self._reconnect is None:
            logger.info("Connection ended; reconnect is disabled.")
            return None

        connected_duration = (
            time.monotonic() - self._conn.connected_at
            if self._conn is not None and self._conn.connected_at is not None
            else 0.0
        )
        # A connection that stayed healthy for a while earns a fresh
        # backoff streak; one that dropped almost immediately does not.
        attempt = 0 if connected_duration >= _RECONNECT_RESET_AFTER else attempt + 1

        delay = self._reconnect(attempt)
        if delay is None:
            logger.info("reconnect() stopped retrying after %d attempt(s).", attempt)
            return None
        return attempt, delay

    async def _reconnect_once(self) -> None:
        """Build a fresh connection and replay subscriptions onto it. A
        failure here is logged and swallowed: the supervisor loops, the
        next `_pump` raises immediately on the dead connection, and we
        land back in `_decide_reconnect` with `attempt` advanced."""
        try:
            self._conn = _Connection(self._url, self._auth_frame, timeout=self._timeout)
            await self._conn.open()
            await self._replay_subscriptions()
        except Exception as exc:  # noqa: BLE001
            logger.error("Reconnect attempt failed: %s", exc)

    async def _replay_subscriptions(self) -> None:
        if not self._subscriptions:
            return
        logger.info("Replaying %d subscription(s) after reconnect ...", len(self._subscriptions))
        for channel, params in list(self._subscriptions):
            try:
                await self.subscribe(channel, params, persist=False)
            except Exception as exc:  # noqa: BLE001
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
                # Bind the connection we're about to ping: on a timeout we
                # close *that* socket to force a reconnect, and a reconnect
                # that lands during `_ping_once` must not make us close the
                # fresh connection instead.
                conn = self._conn
                try:
                    await self._ping_once(timeout=self._heartbeat_timeout)
                except RuntimeError:
                    # Not connected right now; _supervise owns reconnecting.
                    await asyncio.sleep(1)
                    continue
                except asyncio.TimeoutError:
                    logger.warning(
                        "Heartbeat timed out after %ds - closing to trigger reconnect",
                        self._heartbeat_timeout,
                    )
                    if conn is not None and conn is self._conn and conn.is_open:
                        await conn.close()
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("Unexpected error in heartbeat loop")
                await asyncio.sleep(1)

    # -- dispatch ---------------------------------------------------------

    async def _dispatch(self, msg: dict) -> None:
        try:
            result = self.on_message(msg)
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:  # noqa: BLE001
            self._handle_callback_error(exc, msg)

        # Correlated replies (method responses, pongs, id-less Proxy
        # errors) are consumed here and never reach channel handlers. The
        # dialect knows which field carries the correlation key for this
        # venue (`req_id`/`id` for Kraken, `id` for OKX).
        if self._pending.resolve(msg, self._dialect.response_key(msg)):
            return

        await self._deliver(msg)

    async def _deliver(self, msg: dict) -> None:
        channel = self._dialect.channel_of(msg)

        if channel == "heartbeat" and "heartbeat" not in self._handlers:
            return

        if channel and channel in self._handlers:
            for handler in list(self._handlers[channel]):
                await self._call_handler(handler, msg)

        for handler in list(self._handlers.get("*", [])):
            await self._call_handler(handler, msg)

    async def _call_handler(self, handler: MessageHandler, msg: dict) -> None:
        try:
            result = handler(msg)
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:  # noqa: BLE001
            self._handle_callback_error(exc, msg)

    def _handle_callback_error(self, exc: Exception, msg: dict) -> None:
        if self._on_error is not None:
            try:
                self._on_error(exc)
            except Exception:  # noqa: BLE001
                logger.exception("on_error callback itself raised")
        else:
            logger.exception("Handler raised an unhandled exception for message: %s", msg)
