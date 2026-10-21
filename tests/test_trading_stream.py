"""`TradingStream` mechanics against a fake socket: handler-before-subscribe
ordering, cleanup, symbol filtering, and failures surfacing from the
iterator instead of disappearing into `on_error`."""

from __future__ import annotations

import asyncio

import pytest

from proxy_client.venues import (
    BookUpdate,
    Fill,
    KrakenSpot,
    OrderEvent,
    StreamGap,
    StreamOverflow,
    TradingStream,
    UnmappedVenueValue,
)
from proxy_client.websocket import ConnectionEvent


class FakeWS:
    """Records calls in order and lets a test push messages to handlers."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.handlers: dict[str, list] = {}
        self.listeners: list = []

    def add_connection_listener(self, listener):
        self.listeners.append(listener)

    def remove_connection_listener(self, listener):
        self.listeners.remove(listener)

    def emit(self, kind: str, error: BaseException | None = None) -> None:
        for listener in list(self.listeners):
            listener(ConnectionEvent(kind, error))

    def add_handler(self, channel, handler):
        self.calls.append(("add_handler", channel))
        self.handlers.setdefault(channel, []).append(handler)

    def remove_handler(self, channel, handler):
        self.calls.append(("remove_handler", channel))
        self.handlers[channel].remove(handler)

    async def subscribe(self, channel, params=None):
        self.calls.append(("subscribe", channel, params))
        return {"success": True}

    async def unsubscribe(self, channel, params=None):
        self.calls.append(("unsubscribe", channel, params))
        return {"success": True}

    def push(self, msg: dict) -> None:
        for h in list(self.handlers.get(msg.get("channel"), [])):
            h(msg)


def _execution(order_id: str, status: str = "new") -> dict:
    return {"channel": "executions", "type": "update",
            "data": [{"order_id": order_id, "order_status": status}]}


def _book(symbol: str) -> dict:
    return {"channel": "book", "type": "update",
            "data": [{"symbol": symbol, "bids": [], "asks": [], "checksum": 0}]}


async def _started(gen):
    """Advance an async generator to its first wait, so it has subscribed."""
    task = asyncio.ensure_future(gen.__anext__())
    await asyncio.sleep(0)
    return task


def test_executions_registers_handler_before_subscribing_and_cleans_up():
    async def run():
        ws = FakeWS()
        stream = TradingStream(KrakenSpot(), ws)
        gen = stream.executions()
        first = await _started(gen)
        assert ws.calls == [("add_handler", "executions"), ("subscribe", "executions", {})]

        ws.push(_execution("O1"))
        event = await first
        assert isinstance(event, OrderEvent) and event.order_id == "O1"

        await gen.aclose()
        assert ws.calls[-2:] == [("unsubscribe", "executions", {}), ("remove_handler", "executions")]
        assert ws.handlers["executions"] == []

    asyncio.run(run())


def test_parse_error_is_raised_from_the_iterator_not_swallowed():
    async def run():
        ws = FakeWS()
        gen = TradingStream(KrakenSpot(), ws).executions()
        first = await _started(gen)
        ws.push(_execution("O1", status="zombie"))
        with pytest.raises(UnmappedVenueValue):
            await first
        assert ("unsubscribe", "executions", {}) in ws.calls

    asyncio.run(run())


def test_overflow_ends_the_stream_loudly():
    async def run():
        ws = FakeWS()
        gen = TradingStream(KrakenSpot(), ws, max_queue=2).executions()
        first = await _started(gen)
        ws.push(_execution("O1"))       # consumed by the waiting __anext__
        await first
        for i in range(3):              # third one does not fit
            ws.push(_execution(f"O{i + 2}"))
        with pytest.raises(StreamOverflow):
            await gen.__anext__()

    asyncio.run(run())


def test_book_filters_to_its_own_symbols():
    async def run():
        ws = FakeWS()
        stream = TradingStream(KrakenSpot(), ws)
        btc = stream.book(["BTC-USD"])
        eth = stream.book(["ETH-USD"], depth=25)
        btc_next = await _started(btc)
        eth_next = await _started(eth)
        assert ("subscribe", "book", {"symbol": ["BTC/USD"], "depth": 10}) in ws.calls
        assert ("subscribe", "book", {"symbol": ["ETH/USD"], "depth": 25}) in ws.calls

        ws.push(_book("ETH/USD"))
        ws.push(_book("BTC/USD"))
        b, e = await btc_next, await eth_next
        assert isinstance(b, BookUpdate) and b.symbol == "BTC-USD"
        assert e.symbol == "ETH-USD"
        await btc.aclose()
        await eth.aclose()

    asyncio.run(run())


def test_duplicate_feeds_are_refused():
    async def run():
        ws = FakeWS()
        stream = TradingStream(KrakenSpot(), ws)
        ex = stream.executions()
        bk = stream.book(["BTC-USD", "ETH-USD"])
        ex_next, bk_next = await _started(ex), await _started(bk)

        with pytest.raises(RuntimeError):
            await stream.executions().__anext__()
        with pytest.raises(RuntimeError):
            await stream.book(["ETH-USD"]).__anext__()

        for task in (ex_next, bk_next):
            task.cancel()
        await asyncio.gather(ex_next, bk_next, return_exceptions=True)
        # Free again once the first iterators have ended.
        again = await _started(stream.executions())
        again.cancel()
        await asyncio.gather(again, return_exceptions=True)

    asyncio.run(run())


def test_fills_and_order_events_both_come_through_executions():
    async def run():
        ws = FakeWS()
        gen = TradingStream(KrakenSpot(), ws).executions()
        first = await _started(gen)
        ws.push({"channel": "executions", "type": "update", "data": [{
            "order_id": "O1", "exec_id": "T1", "exec_type": "trade", "symbol": "BTC/USD",
            "side": "sell", "last_qty": 1.0, "last_price": 100.0, "order_status": "filled",
            "timestamp": "2023-09-22T10:33:05Z", "fees": [{"asset": "USD", "qty": 0.26}],
        }]})
        assert isinstance(await first, Fill)
        assert isinstance(await gen.__anext__(), OrderEvent)
        await gen.aclose()

    asyncio.run(run())


# -- disconnects ---------------------------------------------------------------


def test_drop_yields_a_gap_in_order_with_surrounding_events():
    async def run():
        ws = FakeWS()
        gen = TradingStream(KrakenSpot(), ws).executions()
        first = await _started(gen)
        drop = ConnectionResetError("peer reset")
        ws.push(_execution("O1"))
        ws.emit("disconnected", drop)
        ws.push(_execution("O2"))
        assert (await first).order_id == "O1"
        gap = await gen.__anext__()
        assert isinstance(gap, StreamGap) and gap.error is drop
        assert (await gen.__anext__()).order_id == "O2"
        await gen.aclose()

    asyncio.run(run())


def test_stop_ends_the_iterator_cleanly_without_unsubscribing():
    async def run():
        ws = FakeWS()
        gen = TradingStream(KrakenSpot(), ws).executions()
        first = await _started(gen)
        ws.emit("stopped")
        with pytest.raises(StopAsyncIteration):
            await first
        assert not any(c[0] == "unsubscribe" for c in ws.calls)
        assert ws.listeners == [] and ws.handlers["executions"] == []

    asyncio.run(run())


def test_closed_for_good_raises_after_delivering_queued_events():
    async def run():
        ws = FakeWS()
        gen = TradingStream(KrakenSpot(), ws).book(["BTC-USD"])
        first = await _started(gen)
        cause = RuntimeError("reconnect gave up")
        ws.push(_book("BTC/USD"))
        ws.emit("closed", cause)
        assert isinstance(await first, BookUpdate)
        with pytest.raises(ConnectionError) as exc:
            await gen.__anext__()
        assert exc.value.__cause__ is cause

    asyncio.run(run())


def test_a_parked_consumer_is_woken_by_close():
    """The original bug: nothing queued, socket gone, consumer waits forever."""
    async def run():
        ws = FakeWS()
        gen = TradingStream(KrakenSpot(), ws).executions()
        first = await _started(gen)
        ws.emit("closed")
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(first, timeout=1)

    asyncio.run(run())
