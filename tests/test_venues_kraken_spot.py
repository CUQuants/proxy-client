"""`KrakenSpot` against pinned response fixtures.

The fixtures are Kraken REST v0 bodies as the Proxy returns them —
`{"error": [], "result": ...}`, passed through whole — trimmed to the fields
the parsers read plus a few they must ignore. Shapes follow Kraken's
published examples for AssetPairs, AddOrder, QueryOrders, OpenOrders,
TradesHistory and BalanceEx. If Kraken changes a shape, update the fixture
here by hand from a real response.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from proxy_client.venues import (
    KrakenSpot,
    OrderStatus,
    OrderType,
    Side,
    UnmappedVenueValue,
    normalize_asset,
)

ASSET_PAIRS = {
    "error": [],
    "result": {
        "XXBTZUSD": {"altname": "XBTUSD", "wsname": "XBT/USD", "base": "XXBT", "quote": "ZUSD"},
        "XETHZUSD": {"altname": "ETHUSD", "wsname": "ETH/USD", "base": "XETH", "quote": "ZUSD"},
        "USDTZUSD": {"altname": "USDTUSD", "wsname": "USDT/USD", "base": "USDT", "quote": "ZUSD"},
        "XRPUSD": {"altname": "XRPUSD", "wsname": "XRP/USD", "base": "XXRP", "quote": "ZUSD"},
    },
}


@pytest.fixture
def kraken() -> KrakenSpot:
    return KrakenSpot.from_asset_pairs(ASSET_PAIRS)


# -- assets and symbols -------------------------------------------------------


@pytest.mark.parametrize(
    "code, expected",
    [
        ("XXBT", "BTC"), ("XBT", "BTC"), ("ZUSD", "USD"), ("XETH", "ETH"),
        ("XXDG", "DOGE"), ("USDT", "USDT"), ("SOL", "SOL"),
        ("XBT.F", "BTC.F"), ("ETH2.S", "ETH2.S"),
    ],
)
def test_normalize_asset(code, expected):
    assert normalize_asset(code) == expected


def test_every_kraken_spelling_of_a_pair_maps_to_one_canonical_symbol(kraken):
    for spelling in ("XXBTZUSD", "XBTUSD", "XBT/USD"):
        assert kraken.canonical_symbol(spelling) == "BTC-USD"
    assert kraken.canonical_symbol("XRPUSD") == "XRP-USD"


def test_unknown_inbound_pair_raises_rather_than_guessing(kraken):
    with pytest.raises(UnmappedVenueValue) as exc:
        kraken.canonical_symbol("SOLUSD")
    assert exc.value.field_name == "pair"


def test_without_asset_pairs_inbound_symbols_raise():
    with pytest.raises(UnmappedVenueValue):
        KrakenSpot().canonical_symbol("XBTUSD")


def test_outbound_symbol_prefers_altname_then_derives(kraken):
    assert kraken.venue_symbol("BTC-USD") == "XBTUSD"
    assert kraken.venue_symbol("XRP-USD") == "XRPUSD"
    # Not in the table: derived, with Kraken's REST renames applied.
    assert KrakenSpot().venue_symbol("DOGE-USD") == "XDGUSD"
    assert KrakenSpot().venue_symbol("usdc-usd") == "USDCUSD"


def test_outbound_symbol_rejects_non_canonical_input():
    with pytest.raises(ValueError):
        KrakenSpot().venue_symbol("XBTUSD")


# -- outbound payloads --------------------------------------------------------


def test_place_order_maps_to_addorder_fields(kraken):
    payload = kraken.place_order(
        {
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "size": "0.01",
            "price": "60000.5",
            "client_order_id": "strat1-042",
        }
    )
    assert payload == {
        "pair": "XBTUSD",
        "type": "buy",
        "ordertype": "limit",
        "volume": "0.01",
        "price": "60000.5",
        "cl_ord_id": "strat1-042",
    }


def test_place_order_rejects_unknown_side_before_sending(kraken):
    with pytest.raises(ValueError):
        kraken.place_order(
            {"symbol": "BTC-USD", "side": "long", "order_type": "market", "size": "1"}
        )


def test_cancel_order_is_keyed_by_txid_alone(kraken):
    # The Proxy's cancel route is symbol_policy="absent".
    assert kraken.cancel_order(order_id="OABC-123", symbol="BTC-USD") == {"txid": "OABC-123"}


def test_query_orders_joins_txids(kraken):
    assert kraken.query_orders(["O1", "O2"]) == {"txid": "O1,O2"}


# -- inbound: orders ----------------------------------------------------------


def test_parse_order_ack():
    raw = {
        "error": [],
        "result": {"descr": {"order": "buy 0.01 XBTUSD @ limit 60000.5"}, "txid": ["OUF4EM-FRGI2-MQMWZD"]},
    }
    ack = KrakenSpot().parse_order_ack(raw)
    assert ack.order_ids == ("OUF4EM-FRGI2-MQMWZD",)
    assert ack.raw is raw


def _kraken_order(**overrides) -> dict:
    order = {
        "status": "open",
        "opentm": 1688666559.8974,
        "descr": {"pair": "XBTUSD", "type": "buy", "ordertype": "limit", "price": "30010.0"},
        "vol": "1.25000000",
        "vol_exec": "0.00000000",
        "cost": "0.00000",
        "fee": "0.00000",
        "price": "0.00000",
        "misc": "",
        "oflags": "fciq",
    }
    order.update(overrides)
    return order


def test_parse_open_orders_unwraps_open_and_keeps_raw(kraken):
    o = _kraken_order(cl_ord_id="strat1-042")
    [order] = kraken.parse_orders({"error": [], "result": {"open": {"OQCLML-BW3P3-BUCMWZ": o}}})
    assert order.order_id == "OQCLML-BW3P3-BUCMWZ"
    assert order.symbol == "BTC-USD"
    assert order.side is Side.BUY
    assert order.order_type is OrderType.LIMIT
    assert order.status is OrderStatus.OPEN
    assert order.size == Decimal("1.25")
    assert order.price == Decimal("30010.0")
    assert order.avg_fill_price is None  # Kraken's "0.00000" is not a price
    assert order.client_order_id == "strat1-042"
    assert order.created_at == datetime.fromtimestamp(1688666559.8974, tz=timezone.utc)
    assert order.raw is o


def test_open_with_partial_execution_is_partially_filled(kraken):
    o = _kraken_order(vol_exec="0.25000000", price="30009.5")
    [order] = kraken.parse_orders({"error": [], "result": {"O1": o}})
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.filled_size == Decimal("0.25")
    assert order.avg_fill_price == Decimal("30009.5")


def test_closed_maps_to_filled_and_closed_orders_wrapper_is_unwrapped(kraken):
    o = _kraken_order(status="closed", vol_exec="1.25000000", price="30010.0")
    [order] = kraken.parse_orders({"error": [], "result": {"closed": {"O1": o}, "count": 1}})
    assert order.status is OrderStatus.FILLED


def test_market_order_has_no_limit_price(kraken):
    o = _kraken_order(descr={"pair": "XBTUSD", "type": "sell", "ordertype": "market", "price": "0"})
    [order] = kraken.parse_orders({"error": [], "result": {"O1": o}})
    assert order.order_type is OrderType.MARKET
    assert order.price is None


@pytest.mark.parametrize(
    "overrides, field_name",
    [
        ({"status": "someday"}, "status"),
        ({"descr": {"pair": "XBTUSD", "type": "buy", "ordertype": "stop-loss", "price": "1"}}, "ordertype"),
    ],
)
def test_unknown_order_values_raise(kraken, overrides, field_name):
    with pytest.raises(UnmappedVenueValue) as exc:
        kraken.parse_orders({"error": [], "result": {"O1": _kraken_order(**overrides)}})
    assert exc.value.field_name == field_name


# -- inbound: fills -----------------------------------------------------------

TRADES_HISTORY = {
    "error": [],
    "result": {
        "trades": {
            "THVRQM-33VKH-UCI7BS": {
                "ordertxid": "OQCLML-BW3P3-BUCMWZ",
                "postxid": "TKH2SE-M7IF5-CFI7LT",
                "pair": "XXBTZUSD",
                "time": 1688667796.8802,
                "type": "buy",
                "ordertype": "limit",
                "price": "30010.00000",
                "cost": "600.20000",
                "fee": "0.00000",
                "vol": "0.02000000",
                "margin": "0.00000",
                "misc": "",
                "trade_id": 40274859,
                "maker": True,
            }
        },
        "count": 1,
    },
}


def test_parse_fills_uses_trade_id_not_order_id(kraken):
    [fill] = kraken.parse_fills(TRADES_HISTORY)
    assert fill.trade_id == "THVRQM-33VKH-UCI7BS"
    assert fill.order_id == "OQCLML-BW3P3-BUCMWZ"
    assert fill.symbol == "BTC-USD"
    assert fill.side is Side.BUY
    assert fill.price == Decimal("30010")
    assert fill.size == Decimal("0.02")
    assert fill.fee_asset == "USD"  # quote currency, per Kraken's docs
    assert fill.is_maker is True
    assert fill.timestamp.tzinfo is timezone.utc


def test_parse_fills_accepts_querytrades_shape(kraken):
    trades = TRADES_HISTORY["result"]["trades"]
    [fill] = kraken.parse_fills({"error": [], "result": trades})
    assert fill.trade_id == "THVRQM-33VKH-UCI7BS"


# -- inbound: balances --------------------------------------------------------


def test_parse_balanceex():
    raw = {
        "error": [],
        "result": {
            "ZUSD": {"balance": "25435.21", "hold_trade": "8249.76"},
            "XXBT": {"balance": "1.2435", "hold_trade": "0.8423"},
            "USDT": {"balance": "100.0"},
        },
    }
    balances = {b.asset: b for b in KrakenSpot().parse_balances(raw)}
    assert set(balances) == {"USD", "BTC", "USDT"}
    assert balances["USD"].available == Decimal("17185.45")
    assert balances["BTC"].held == Decimal("0.8423")
    assert balances["USDT"].held == Decimal("0")


def test_plain_balance_shape_raises_rather_than_assuming_nothing_held():
    with pytest.raises(UnmappedVenueValue):
        KrakenSpot().parse_balances({"error": [], "result": {"ZUSD": "100.0"}})
