"""`ProxyWebSocketClient` against an in-process fake Proxy speaking the OKX
dialect.

Companion to ``test_websocket.py`` (Kraken): same hand-built fake-server
approach, no ``trading-gateway`` checkout on disk (this repo's testing
philosophy — see ``CLAUDE.md``). The frames the fake sends are shaped after
``trading-gateway/proxy/ws/okx.py`` (``OKXDialect.ack_frame`` /
``route_frame`` / ``error_envelope``) and ``proxy/ws/session.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json

import pytest
import websockets

from proxy_client.errors import ActionNotPermittedError
from proxy_client.websocket import ProxyWebSocketClient

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


@contextlib.asynccontextmanager
async def _fake_proxy(handler):
    server = await websockets.serve(handler, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        yield f"http://{host}:{port}"
    finally:
        server.close()
        await server.wait_closed()


async def _accept_auth(ws) -> dict:
    raw = await ws.recv()
    await ws.send(json.dumps({"event": "auth", "code": "0", "operator_id": "cuq-014"}))
    return json.loads(raw)


def _client(base_url: str, **kw) -> ProxyWebSocketClient:
    kw.setdefault("reconnect", None)
    kw.setdefault("heartbeat_interval", None)
    return ProxyWebSocketClient.for_operator(
        base_url=base_url,
        api_key="cuq_op_test",
        secret="s3cr3t",
        operator_id="cuq-014",
        operator_name="J. Rivera",
        exchange="okx",
        **kw,
    )


# -- handshake ----------------------------------------------------------


def test_handshake_signs_the_okx_ws_path():
    from proxy_client.signing import compute_signature

    seen = {}

    async def handler(ws):
        seen["frame"] = await _accept_auth(ws)
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            await client.stop()

    asyncio.run(run())

    frame = seen["frame"]
    assert frame["op"] == "auth"
    body = json.loads(frame["body"])
    assert frame["signature"] == compute_signature(
        b"s3cr3t", "WS", "/v1/okx/ws", body["timestamp"], body["nonce"],
        frame["body"].encode(),
    )


# -- subscribe / data push --------------------------------------------


def test_okx_subscribe_ack_resolves_and_data_push_routes_to_handler():
    received = []
    seen = {}

    async def handler(ws):
        await _accept_auth(ws)
        req = json.loads(await ws.recv())
        seen["req"] = req
        # proxy/ws/okx.py OKXDialect.ack_frame: {"event": kind, "arg": ...,
        # "id": <echoed>}
        await ws.send(json.dumps({
            "event": "subscribe", "arg": req["args"][0], "id": req["id"],
        }))
        # an OKX private order data frame, relayed verbatim (has `arg` + `data`)
        await ws.send(json.dumps({
            "arg": {"channel": "orders", "instType": "SPOT"},
            "data": [{"ordId": "O1", "state": "live", "instId": "BTC-USDT"}],
        }))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            client.add_handler("orders", lambda m: received.append(m))
            await client.start()
            ack = await client.subscribe("orders", {"instType": "SPOT"})
            assert ack["event"] == "subscribe"
            await asyncio.sleep(0.05)  # let the pushed frame dispatch
            await client.stop()

    asyncio.run(run())

    assert seen["req"] == {
        "op": "subscribe",
        "args": [{"channel": "orders", "instType": "SPOT"}],
        "id": "1",
    }
    assert len(received) == 1
    assert received[0]["data"][0]["ordId"] == "O1"


def test_okx_error_frame_raises_typed_proxy_error():
    async def handler(ws):
        await _accept_auth(ws)
        req = json.loads(await ws.recv())
        # proxy/ws/session.py _send_error + OKXDialect.error_envelope
        await ws.send(json.dumps({
            "id": req["id"],
            "event": "error",
            "code": "ACTION_NOT_PERMITTED",
            "message": "channel 'nonsense' is not on the §5.2 allowlist",
            "request_id": "r7",
        }))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            with pytest.raises(ActionNotPermittedError) as excinfo:
                await client.subscribe("nonsense", {"instType": "SPOT"})
            assert excinfo.value.request_id == "r7"
            await client.stop()

    asyncio.run(run())


def test_okx_order_over_ws_is_rejected_before_send():
    async def handler(ws):
        await _accept_auth(ws)
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            with pytest.raises(NotImplementedError):
                await client.request("add_order", params={"instId": "BTC-USDT"})
            await client.stop()

    asyncio.run(run())


def test_okx_subscription_replayed_after_reconnect():
    connects = 0
    subs: list[dict] = []

    async def handler(ws):
        nonlocal connects
        connects += 1
        await _accept_auth(ws)
        req = json.loads(await ws.recv())
        subs.append(req)
        await ws.send(json.dumps({
            "event": "subscribe", "arg": req["args"][0], "id": req["id"],
        }))
        if connects == 1:
            await ws.close()  # drop right after acking, forcing a reconnect
        else:
            await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=lambda attempt: 0.01)
            await client.start()
            await client.subscribe("orders", {"instType": "SPOT"}, persist=True)
            await asyncio.sleep(0.3)  # let drop + reconnect + replay happen
            await client.stop()

    asyncio.run(run())

    assert connects >= 2
    assert sum(s["args"][0]["channel"] == "orders" for s in subs) >= 2
