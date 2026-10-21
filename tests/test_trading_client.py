"""`TradingClient` wiring: adapter builds, `ProxyClient.acall` carries,
adapter parses. The transport is mocked; `test_venues_kraken_spot.py` covers
the parsing itself."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from proxy_client import ExchangeError
from proxy_client.venues import KrakenPrime, KrakenSpot, OrderStatus, TradingClient


def _rest(return_value=None, side_effect=None) -> MagicMock:
    rest = MagicMock()
    rest.acall = AsyncMock(return_value=return_value, side_effect=side_effect)
    return rest


def test_place_order_builds_sends_with_key_and_parses_ack():
    rest = _rest({"error": [], "result": {"descr": {}, "txid": ["O1"]}})
    tc = TradingClient(KrakenSpot(), rest)

    ack = asyncio.run(
        tc.place_order(
            {"symbol": "BTC-USD", "side": "sell", "order_type": "market", "size": "0.5"},
            idempotency_key="intent-1",
        )
    )

    rest.acall.assert_awaited_once_with(
        "kraken",
        "place_order",
        {"pair": "XBTUSD", "type": "sell", "ordertype": "market", "volume": "0.5"},
        idempotency_key="intent-1",
    )
    assert ack.order_ids == ("O1",)


def test_idempotency_key_is_required_for_order_entry():
    tc = TradingClient(KrakenSpot(), _rest())
    with pytest.raises(TypeError):
        asyncio.run(tc.place_order({"symbol": "BTC-USD", "side": "buy",
                                    "order_type": "market", "size": "1"}))


def test_cancel_order_passes_key_and_returns_raw_reply():
    reply = {"error": [], "result": {"count": 1}}
    rest = _rest(reply)
    tc = TradingClient(KrakenSpot(), rest)

    out = asyncio.run(tc.cancel_order(order_id="O1", symbol="BTC-USD", idempotency_key="k"))

    rest.acall.assert_awaited_once_with("kraken", "cancel_order", {"txid": "O1"}, idempotency_key="k")
    assert out is reply


def test_reads_route_to_the_gateway_action_names():
    open_orders = {"error": [], "result": {"open": {"O1": {
        "status": "open", "vol": "1", "vol_exec": "0", "price": "0",
        "descr": {"pair": "XBTUSD", "type": "buy", "ordertype": "limit", "price": "1"},
    }}}}
    rest = _rest(open_orders)
    tc = TradingClient(KrakenSpot({"XBTUSD": "BTC-USD"}), rest)

    [order] = asyncio.run(tc.get_open_orders())
    assert order.status is OrderStatus.OPEN
    rest.acall.assert_awaited_once_with("kraken", "get_pending_orders", {}, idempotency_key="")

    rest.acall.reset_mock()
    rest.acall.return_value = {"error": [], "result": {"ZUSD": {"balance": "5", "hold_trade": "1"}}}
    [bal] = asyncio.run(tc.get_balances())
    assert bal.available == Decimal("4")
    rest.acall.assert_awaited_once_with("kraken", "get_balance", {}, idempotency_key="")


def test_proxy_errors_propagate_unnormalized():
    err = ExchangeError("boom", detail={"outcome_unknown": True})
    tc = TradingClient(KrakenSpot(), _rest(side_effect=err))
    with pytest.raises(ExchangeError) as exc:
        asyncio.run(tc.place_order({"symbol": "BTC-USD", "side": "buy",
                                    "order_type": "market", "size": "1"},
                                   idempotency_key="k"))
    assert exc.value is err


def test_ws_order_venue_never_falls_back_to_rest():
    rest = _rest()
    tc = TradingClient(KrakenPrime(), rest)
    with pytest.raises(NotImplementedError):
        asyncio.run(tc.place_order({"symbol": "BTC-USD", "side": "buy",
                                    "order_type": "market", "size": "1"},
                                   idempotency_key="k"))
    rest.acall.assert_not_awaited()
