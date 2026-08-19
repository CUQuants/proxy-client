from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

from proxy_client import (
    NormalizedOrder,
    OkxOrderBuilder,
    ProxyClient,
    okx_acancel_order,
    okx_aplace_order,
    okx_cancel_order,
    okx_place_order,
)


def _client() -> ProxyClient:
    return ProxyClient(
        base_url="https://proxy.example.com",
        api_key="cuq_op_test",
        secret="s3cr3t",
        operator_id="cuq-014",
        operator_name="J. Rivera",
    )


def _fake_response(status_code: int, body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.headers = {}
    return resp


# -- OkxOrderBuilder: pure payload shape, no network -------------------------


def test_okx_place_order_maps_normalized_fields_to_okx_wire_names():
    order: NormalizedOrder = {
        "symbol": "BTC-USDT",
        "side": "buy",
        "order_type": "limit",
        "price": "50000.5",
        "size": "0.01",
        "client_order_id": "strat1-042",
    }
    payload = OkxOrderBuilder().place_order(order)
    assert payload == {
        "instId": "BTC-USDT",
        "tdMode": "cash",
        "side": "buy",
        "ordType": "limit",
        "sz": "0.01",
        "px": "50000.5",
        "clOrdId": "strat1-042",
    }


def test_okx_place_order_omits_px_for_market_orders_without_a_price():
    order: NormalizedOrder = {
        "symbol": "BTC-USDT",
        "side": "sell",
        "order_type": "market",
        "size": "0.01",
    }
    payload = OkxOrderBuilder().place_order(order)
    assert "px" not in payload
    assert "clOrdId" not in payload


def test_okx_cancel_order_maps_to_instid_and_ordid():
    payload = OkxOrderBuilder().cancel_order(order_id="1234", symbol="BTC-USDT")
    assert payload == {"instId": "BTC-USDT", "ordId": "1234"}


# -- okx_place_order / okx_aplace_order: wired through ProxyClient.call ------


def test_okx_place_order_sends_okx_field_names_over_the_wire():
    client = _client()
    ok = _fake_response(200, {"data": {"ordId": "999"}})
    order: NormalizedOrder = {
        "symbol": "BTC-USDT",
        "side": "buy",
        "order_type": "limit",
        "price": "50000",
        "size": "0.01",
    }

    with patch.object(client._client, "post", return_value=ok) as post:
        result = okx_place_order(client, order, idempotency_key="key-1")

    _, kwargs = post.call_args
    sent = json.loads(kwargs["content"])
    assert sent["exchange"] == "okx"
    assert sent["action"] == "place_order"
    assert sent["payload"] == {
        "instId": "BTC-USDT",
        "tdMode": "cash",
        "side": "buy",
        "ordType": "limit",
        "sz": "0.01",
        "px": "50000",
    }
    assert kwargs["headers"]["X-Idempotency-Key"] == "key-1"
    assert result == {"ordId": "999"}


def test_okx_aplace_order_uses_the_async_transport():
    client = _client()
    ok = _fake_response(200, {"data": {"ordId": "999"}})
    order: NormalizedOrder = {
        "symbol": "ETH-USDT",
        "side": "sell",
        "order_type": "market",
        "size": "1",
    }

    with patch.object(client._aclient, "post", new=AsyncMock(return_value=ok)) as post:
        result = asyncio.run(okx_aplace_order(client, order))

    _, kwargs = post.call_args
    sent = json.loads(kwargs["content"])
    assert sent["payload"] == {
        "instId": "ETH-USDT",
        "tdMode": "cash",
        "side": "sell",
        "ordType": "market",
        "sz": "1",
    }
    assert result == {"ordId": "999"}


def test_okx_cancel_order_sends_instid_and_ordid():
    client = _client()
    ok = _fake_response(200, {"data": {"ordId": "999"}})

    with patch.object(client._client, "post", return_value=ok) as post:
        okx_cancel_order(client, order_id="999", symbol="BTC-USDT", idempotency_key="key-2")

    _, kwargs = post.call_args
    sent = json.loads(kwargs["content"])
    assert sent["action"] == "cancel_order"
    assert sent["payload"] == {"instId": "BTC-USDT", "ordId": "999"}
    assert kwargs["headers"]["X-Idempotency-Key"] == "key-2"


def test_okx_acancel_order_uses_the_async_transport():
    client = _client()
    ok = _fake_response(200, {"data": {"ordId": "999"}})

    with patch.object(client._aclient, "post", new=AsyncMock(return_value=ok)) as post:
        asyncio.run(okx_acancel_order(client, order_id="999", symbol="BTC-USDT"))

    _, kwargs = post.call_args
    sent = json.loads(kwargs["content"])
    assert sent["payload"] == {"instId": "BTC-USDT", "ordId": "999"}
