"""Pure unit tests for the WebSocket dialects — no socket, no event loop.

The OKX frame shapes are pinned against ``trading-gateway/proxy/ws/okx.py``
(``OKXDialect.parse_client_frame`` / ``ack_frame`` / ``error_envelope``)
and ``proxy/ws/session.py``'s ``_send_error``. If that server contract
changes, the golden values below change with it — same approach as
``tests/test_signing.py``.
"""

from __future__ import annotations

import pytest

from proxy_client.errors import ActionNotPermittedError, RateLimitedError
from proxy_client.websocket import (
    Dialect,
    KrakenDialect,
    OKXDialect,
    WebSocketRequestError,
)
from proxy_client.websocket._dialect import dialect_for_exchange


# -- dialect_for_exchange ---------------------------------------------------


def test_dialect_for_exchange_known():
    assert isinstance(dialect_for_exchange("kraken"), KrakenDialect)
    assert isinstance(dialect_for_exchange("okx"), OKXDialect)


def test_dialect_for_exchange_unknown_raises_valueerror():
    with pytest.raises(ValueError):
        dialect_for_exchange("gemini")


def test_dialects_are_dialect_instances():
    assert isinstance(KrakenDialect(), Dialect)
    assert isinstance(OKXDialect(), Dialect)


# -- Kraken ---------------------------------------------------------------


def test_kraken_subscribe_frame_and_key():
    frame, key = KrakenDialect().request_frame(
        "subscribe", {"channel": "book", "symbol": ["BTC/USD"]}, 7, None, is_order=False
    )
    assert frame == {
        "method": "subscribe",
        "req_id": 7,
        "params": {"channel": "book", "symbol": ["BTC/USD"]},
    }
    assert key == 7


def test_kraken_order_frame_dual_tags_id_and_req_id():
    frame, key = KrakenDialect().request_frame(
        "add_order", {"symbol": "BTC/USD"}, 3, "idem-1", is_order=True
    )
    assert frame["method"] == "add_order"
    assert frame["req_id"] == 3 and frame["id"] == 3
    assert frame["idempotency_key"] == "idem-1"
    assert key == 3


def test_kraken_non_order_carries_explicit_idempotency_key():
    frame, _ = KrakenDialect().request_frame(
        "cancel_all", None, 1, "k", is_order=False
    )
    assert frame == {"method": "cancel_all", "req_id": 1, "idempotency_key": "k"}


def test_kraken_response_key_prefers_id_then_req_id():
    d = KrakenDialect()
    assert d.response_key({"id": 9, "req_id": 4}) == 9
    assert d.response_key({"req_id": 4}) == 4
    assert d.response_key({"channel": "book"}) is None


def test_kraken_channel_of():
    assert KrakenDialect().channel_of({"channel": "book", "data": []}) == "book"
    assert KrakenDialect().channel_of({"data": []}) is None


def test_kraken_error_of_proxy_code_nested_and_toplevel():
    d = KrakenDialect()
    nested = d.error_of(
        {
            "event": "error",
            "request_id": "r1",
            "data": {"error": {"code": "RATE_LIMITED", "message": "slow"}},
        }
    )
    assert isinstance(nested, RateLimitedError) and nested.request_id == "r1"

    toplevel = d.error_of(
        {"event": "error", "code": "RATE_LIMITED", "message": "slow", "request_id": "r2"}
    )
    assert isinstance(toplevel, RateLimitedError) and toplevel.request_id == "r2"


def test_kraken_error_of_native_rejection():
    exc = KrakenDialect().error_of(
        {"success": False, "error": "EGeneral:Invalid arguments"}
    )
    assert isinstance(exc, WebSocketRequestError)


def test_kraken_error_of_none_for_ok_frames():
    d = KrakenDialect()
    assert d.error_of({"success": True, "req_id": 1}) is None
    assert d.error_of({"event": "result", "data": {}}) is None


# -- OKX ----------------------------------------------------------------


def test_okx_subscribe_frame_matches_gateway_shape():
    frame, key = OKXDialect().request_frame(
        "subscribe", {"channel": "orders", "instType": "SPOT"}, 1, None, is_order=False
    )
    assert frame == {
        "op": "subscribe",
        "args": [{"channel": "orders", "instType": "SPOT"}],
        "id": "1",
    }
    # str, so it matches the `id` the Proxy echoes back on the ack
    assert key == "1"


def test_okx_unsubscribe_frame():
    frame, key = OKXDialect().request_frame(
        "unsubscribe", {"channel": "orders", "instType": "SPOT"}, 2, None, is_order=False
    )
    assert frame["op"] == "unsubscribe"
    assert frame["args"] == [{"channel": "orders", "instType": "SPOT"}]
    assert key == "2"


def test_okx_subscribe_frame_drops_none_valued_params():
    frame, _ = OKXDialect().request_frame(
        "subscribe",
        {"channel": "orders", "instType": "SPOT", "instId": None},
        1,
        None,
        is_order=False,
    )
    assert "instId" not in frame["args"][0]


def test_okx_order_over_ws_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        OKXDialect().request_frame("add_order", {}, 1, "k", is_order=True)


def test_okx_unknown_method_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        OKXDialect().request_frame("cancel_all", {}, 1, None, is_order=False)


def test_okx_response_key_is_id():
    d = OKXDialect()
    assert (
        d.response_key({"event": "subscribe", "arg": {"channel": "orders"}, "id": "1"})
        == "1"
    )
    assert d.response_key({"event": "error", "id": "1", "code": "X"}) == "1"
    # a data push carries no id
    assert d.response_key({"arg": {"channel": "orders"}, "data": [{}]}) is None


def test_okx_channel_of_reads_nested_arg():
    d = OKXDialect()
    assert d.channel_of({"arg": {"channel": "orders"}, "data": [{}]}) == "orders"
    assert d.channel_of({"data": [{}]}) is None
    assert d.channel_of({"arg": "not-a-dict"}) is None


def test_okx_error_of_maps_proxy_code():
    exc = OKXDialect().error_of(
        {
            "id": "1",
            "event": "error",
            "code": "ACTION_NOT_PERMITTED",
            "message": "channel 'orders' is not on the allowlist",
            "request_id": "r9",
        }
    )
    assert isinstance(exc, ActionNotPermittedError)
    assert exc.request_id == "r9"


def test_okx_error_of_none_for_ack_and_data_frames():
    d = OKXDialect()
    assert d.error_of({"arg": {"channel": "orders"}, "data": [{}]}) is None
    assert (
        d.error_of({"event": "subscribe", "arg": {"channel": "orders"}, "id": "1"})
        is None
    )
