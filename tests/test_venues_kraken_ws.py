"""`KrakenSpot`'s WS v2 half against pinned message fixtures.

Shapes follow Kraken's WS v2 docs for the `executions` and `book` channels,
as relayed unreshaped by the Proxy — numbers already parsed to `float`, as
`ProxyWebSocketClient` delivers them. Update by hand from a real message if
Kraken changes a shape.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from proxy_client.venues import (
    BookLevel,
    BookUpdate,
    Fill,
    KrakenSpot,
    OrderEvent,
    OrderStatus,
    Side,
    UnmappedVenueValue,
)

TRADE_ROW = {
    "order_id": "OK4GJX-KSTLS-7DZZO5",
    "cl_ord_id": "strat1-042",
    "exec_id": "TZB4EH-OMWO4-JWBUD5",
    "exec_type": "trade",
    "trade_id": 365573,
    "symbol": "BTC/USD",
    "side": "buy",
    "last_qty": 0.25,
    "last_price": 30010.1,
    "liquidity_ind": "m",
    "cost": 7502.525,
    "order_status": "partially_filled",
    "order_type": "limit",
    "order_qty": 1.0,
    "cum_qty": 0.25,
    "avg_price": 30010.1,
    "timestamp": "2023-09-22T10:33:05.709993Z",
    "fees": [{"asset": "USD", "qty": 0.0}],
}


@pytest.fixture
def kraken() -> KrakenSpot:
    # No AssetPairs table: the WS side must not need one.
    return KrakenSpot()


def test_subscriptions_use_ws_v2_spelling(kraken):
    assert kraken.ws_executions_subscription() == ("executions", {})
    assert kraken.ws_book_subscription(["BTC-USD", "doge-usd"], 25) == (
        "book",
        {"symbol": ["BTC/USD", "DOGE/USD"], "depth": 25},
    )


def test_trade_row_yields_a_fill_then_the_order_event(kraken):
    fill, event = kraken.parse_ws_message(
        {"channel": "executions", "type": "update", "data": [TRADE_ROW]}
    )
    assert isinstance(fill, Fill)
    assert fill.trade_id == "TZB4EH-OMWO4-JWBUD5"
    assert fill.order_id == "OK4GJX-KSTLS-7DZZO5"
    assert fill.symbol == "BTC-USD"
    assert fill.side is Side.BUY
    assert fill.price == Decimal("30010.1")  # not 30010.099999999998...
    assert fill.size == Decimal("0.25")
    assert fill.fee_asset == "USD"           # v2 names it; nothing assumed
    assert fill.is_maker is True
    assert fill.timestamp == datetime(2023, 9, 22, 10, 33, 5, 709993, tzinfo=timezone.utc)

    assert isinstance(event, OrderEvent)
    assert event.status is OrderStatus.PARTIALLY_FILLED
    assert event.filled_size == Decimal("0.25")
    assert event.client_order_id == "strat1-042"
    assert event.is_snapshot is False


def test_sparse_cancel_update_leaves_unknowns_as_none(kraken):
    [event] = kraken.parse_ws_message(
        {
            "channel": "executions",
            "type": "update",
            "data": [{
                "order_id": "O1", "exec_type": "canceled", "order_status": "canceled",
                "cancel_reason": "User requested", "timestamp": "2023-09-22T10:40:00.000000Z",
            }],
        }
    )
    assert event.status is OrderStatus.CANCELED
    assert event.symbol is None and event.side is None and event.size is None


def test_snapshot_flag_is_carried(kraken):
    row = {"order_id": "O1", "order_status": "new", "symbol": "ETH/USD", "side": "sell",
           "order_qty": 2.0, "cum_qty": 0.0}
    [event] = kraken.parse_ws_message({"channel": "executions", "type": "snapshot", "data": [row]})
    assert event.is_snapshot is True
    assert event.status is OrderStatus.OPEN
    assert event.symbol == "ETH-USD"


def test_unknown_order_status_raises(kraken):
    with pytest.raises(UnmappedVenueValue):
        kraken.parse_ws_message(
            {"channel": "executions", "data": [{"order_id": "O1", "order_status": "zombie"}]}
        )


@pytest.mark.parametrize("fees", [[], [{"asset": "USD", "qty": 0.1}, {"asset": "BTC", "qty": 0.0}]])
def test_fill_without_exactly_one_fee_raises(kraken, fees):
    with pytest.raises(UnmappedVenueValue):
        kraken.parse_ws_message({"channel": "executions", "data": [{**TRADE_ROW, "fees": fees}]})


def test_book_snapshot_and_update(kraken):
    [snap] = kraken.parse_ws_message({
        "channel": "book", "type": "snapshot",
        "data": [{"symbol": "BTC/USD",
                  "bids": [{"price": 30000.1, "qty": 1.5}],
                  "asks": [{"price": 30000.2, "qty": 0.75}],
                  "checksum": 2439117997}],
    })
    assert isinstance(snap, BookUpdate)
    assert snap.is_snapshot and snap.symbol == "BTC-USD"
    assert snap.bids == (BookLevel(Decimal("30000.1"), Decimal("1.5")),)
    assert snap.checksum == 2439117997

    [upd] = kraken.parse_ws_message({
        "channel": "book", "type": "update",
        "data": [{"symbol": "BTC/USD", "bids": [{"price": 30000.1, "qty": 0.0}], "asks": [],
                  "checksum": 1, "timestamp": "2023-10-06T17:35:55.440295Z"}],
    })
    assert not upd.is_snapshot
    assert upd.bids[0].size == 0  # remove the level
    assert upd.asks == ()


def test_other_channels_yield_nothing(kraken):
    assert kraken.parse_ws_message({"channel": "heartbeat"}) == []
