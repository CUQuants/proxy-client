"""`TradingStream` — normalized subscriptions over `ProxyWebSocketClient`.

`TradingClient`'s counterpart for long-lived feeds. Same adapter, same models,
different shape: a subscription is an async iterator, not a call::

    stream = TradingStream(adapter, ws)        # ws: a started ProxyWebSocketClient
    async for event in stream.executions():    # OrderEvent | Fill
        ...
    async for update in stream.book(["BTC-USD"], depth=10):
        ...

The socket is the caller's: start it, stop it, share it with a
`TradingClient`. This class only registers a handler, subscribes, and turns
messages into events. Reconnects are the socket's job too — it replays every
subscription, so after a drop the iterator simply receives a fresh snapshot
(`is_snapshot=True`). A snapshot always means "replace your state"; anything
that happened while disconnected (fills especially) has to be reconciled over
REST with `TradingClient`.

Failures reach the consumer
---------------------------
`ProxyWebSocketClient` catches handler exceptions and routes them to
`on_error`, so a parse error raised inside a handler would be logged and the
event lost — a fill silently gone. The handler here therefore never raises:
a parse error is queued and **re-raised from the iterator**, and so is
`StreamOverflow` when the consumer falls more than `max_queue` events behind.
Dropping book updates or fills to keep up would leave the consumer with
wrong state and no sign of it; ending the stream loudly does not.

One subscription per feed
-------------------------
A second subscriber to the same feed would miss its snapshot (the venue sends
one per subscribe, and the venue rejects a duplicate subscribe), so a second
`executions()` while one is running, or a `book()` overlapping a running
`book()`'s symbols, raises instead. Fan out from one iterator if two
consumers need the same feed.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import aclosing
from typing import Any, AsyncIterator, Callable

from .base import StreamAdapter, StreamEvent
from .models import BookUpdate, Fill, OrderEvent

__all__ = ["TradingStream", "StreamOverflow"]

logger = logging.getLogger(__name__)


class StreamOverflow(RuntimeError):
    """The consumer fell behind by more than `max_queue` events."""


class TradingStream:
    def __init__(self, venue: StreamAdapter, ws: Any, *, max_queue: int = 10_000) -> None:
        """`ws` is a `ProxyWebSocketClient` for `venue`'s exchange. Typed
        `Any` so importing this module never needs the `websocket` extra."""
        self.venue = venue
        self._ws = ws
        self._max_queue = max_queue
        self._executions_active = False
        self._book_symbols: set[str] = set()

    async def executions(self) -> AsyncIterator[OrderEvent | Fill]:
        """Every order change and fill on the account.

        On the Proxy's shared vaulted key that means *every member's* orders,
        not just the caller's — filter on `client_order_id` if you only want
        your own.
        """
        if self._executions_active:
            raise RuntimeError("executions() is already streaming on this TradingStream")
        self._executions_active = True
        try:
            channel, params = self.venue.ws_executions_subscription()
            # `aclosing`: closing this generator must close `_run` *now*, so
            # its unsubscribe runs here rather than whenever GC finalizes it.
            async with aclosing(
                self._run(channel, params, lambda e: isinstance(e, (OrderEvent, Fill)))
            ) as events:
                async for event in events:
                    yield event
        finally:
            self._executions_active = False

    async def book(self, symbols: list[str], *, depth: int = 10) -> AsyncIterator[BookUpdate]:
        """Book snapshots and updates for canonical `symbols`."""
        wanted = {s.upper() for s in symbols}
        overlap = wanted & self._book_symbols
        if overlap:
            raise RuntimeError(f"book() is already streaming {sorted(overlap)} on this TradingStream")
        self._book_symbols |= wanted
        try:
            channel, params = self.venue.ws_book_subscription(sorted(wanted), depth)
            # Every book() iterator's handler sees every `book` message on the
            # socket, so each keeps only its own symbols.
            async with aclosing(
                self._run(
                    channel, params, lambda e: isinstance(e, BookUpdate) and e.symbol in wanted
                )
            ) as events:
                async for event in events:
                    yield event
        finally:
            self._book_symbols -= wanted

    async def _run(
        self, channel: str, params: dict, accept: Callable[[StreamEvent], bool]
    ) -> AsyncIterator[Any]:
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._max_queue)
        overflowed = False

        def offer(item: Any) -> None:
            nonlocal overflowed
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                overflowed = True

        def handler(msg: dict) -> None:
            try:
                events = self.venue.parse_ws_message(msg)
            except Exception as exc:  # noqa: BLE001 — re-raised from the iterator
                offer(exc)
                return
            for event in events:
                if accept(event):
                    offer(event)

        # Handler first, subscribe second: the snapshot can arrive before
        # `subscribe()` returns, and it must not be missed.
        self._ws.add_handler(channel, handler)
        try:
            await self._ws.subscribe(channel, params)
            try:
                while True:
                    item = await queue.get()
                    if overflowed:
                        raise StreamOverflow(
                            f"{self.venue.name} {channel}: consumer fell more than "
                            f"{self._max_queue} events behind"
                        )
                    if isinstance(item, Exception):
                        raise item
                    yield item
            finally:
                try:
                    await self._ws.unsubscribe(channel, params)
                except Exception:  # noqa: BLE001 — never mask why the stream ended
                    logger.exception("unsubscribe from %s failed", channel)
        finally:
            self._ws.remove_handler(channel, handler)
