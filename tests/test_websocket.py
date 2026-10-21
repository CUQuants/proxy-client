from __future__ import annotations

import asyncio
import contextlib
import json

import pytest
import websockets

from proxy_client.errors import AuthFailedError, RateLimitedError
from proxy_client.websocket import ConnectionEvent, ProxyWebSocketClient, WebSocketRequestError

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _client(base_url: str, **kw) -> ProxyWebSocketClient:
    kw.setdefault("reconnect", None)  # most tests want a single, predictable connection
    kw.setdefault("heartbeat_interval", None)  # no background ping unless a test wants one
    return ProxyWebSocketClient.for_operator(
        base_url=base_url,
        api_key="cuq_op_test",
        secret="s3cr3t",
        operator_id="cuq-014",
        operator_name="J. Rivera",
        **kw,
    )


@contextlib.asynccontextmanager
async def _fake_proxy(handler):
    """A minimal in-process stand-in for the Proxy's WS route.

    Not `trading-gateway` under test — a hand-built fake, per this repo's
    testing philosophy (`CLAUDE.md`'s Testing philosophy section): no
    filesystem coupling to a sibling checkout.
    """
    # Bind explicitly to the IPv4 loopback — "localhost" can resolve to
    # "::1" on this machine, and an unbracketed IPv6 host breaks URI
    # parsing downstream (`ws://::1:PORT/...` isn't a valid authority).
    server = await websockets.serve(handler, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        yield f"http://{host}:{port}"
    finally:
        server.close()
        await server.wait_closed()


async def _accept_auth(ws) -> dict:
    raw = await ws.recv()
    frame = json.loads(raw)
    await ws.send(json.dumps({"event": "auth", "code": "0", "operator_id": "cuq-014"}))
    return frame


# -- handshake ----------------------------------------------------------


def test_handshake_success_lets_start_return():
    async def handler(ws):
        await _accept_auth(ws)
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            try:
                assert client.is_connected
            finally:
                await client.stop()

    asyncio.run(run())


def test_handshake_rejection_raises_typed_error():
    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({
            "event": "error", "code": "AUTH_FAILED",
            "message": "Authentication failed.", "request_id": "r1",
        }))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            with pytest.raises(AuthFailedError) as excinfo:
                await client.start()
            assert excinfo.value.request_id == "r1"

    asyncio.run(run())


def test_auth_frame_is_signed_like_rest():
    from proxy_client.signing import compute_signature

    seen = {}

    async def handler(ws):
        frame = await _accept_auth(ws)
        seen["frame"] = frame
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
    assert body["operator_id"] == "cuq-014"
    assert "system_name" not in body  # operator credential

    expected_sig = compute_signature(
        b"s3cr3t", "WS", "/v1/kraken/ws", body["timestamp"], body["nonce"],
        frame["body"].encode(),
    )
    assert frame["signature"] == expected_sig


def test_system_credential_carries_system_name():
    seen = {}

    async def handler(ws):
        frame = await _accept_auth(ws)
        seen["frame"] = frame
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = ProxyWebSocketClient.for_system(
                base_url=base_url, api_key="cuq_sys_test", secret="s3cr3t",
                operator_id="cuq-014", operator_name="J. Rivera", system_name="speedbyte",
                reconnect=None, heartbeat_interval=None,
            )
            await client.start()
            await client.stop()

    asyncio.run(run())

    body = json.loads(seen["frame"]["body"])
    assert body["system_name"] == "speedbyte"


# -- subscribe / request -------------------------------------------------


def test_subscribe_ack_resolves_and_pushes_route_to_handler():
    received = []

    async def handler(ws):
        await _accept_auth(ws)
        raw = await ws.recv()
        req = json.loads(raw)
        await ws.send(json.dumps({
            "method": "subscribe", "result": {"channel": "book"},
            "success": True, "req_id": req["req_id"],
        }))
        await ws.send(json.dumps({
            "channel": "book", "type": "snapshot", "data": [{"asks": [], "bids": []}],
        }))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            client.add_handler("book", lambda msg: received.append(msg))
            await client.start()
            ack = await client.subscribe("book", {"symbol": ["BTC/USD"], "depth": 10})
            assert ack["success"] is True
            await asyncio.sleep(0.05)  # let the pushed frame dispatch
            await client.stop()

    asyncio.run(run())
    assert len(received) == 1
    assert received[0]["type"] == "snapshot"


def test_kraken_subscribe_rejection_raises_websocket_request_error():
    async def handler(ws):
        await _accept_auth(ws)
        raw = await ws.recv()
        req = json.loads(raw)
        await ws.send(json.dumps({
            "method": "subscribe", "success": False,
            "error": "EGeneral:Invalid arguments", "req_id": req["req_id"],
        }))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            with pytest.raises(WebSocketRequestError):
                await client.subscribe("book", {"symbol": ["nonsense"]})
            await client.stop()

    asyncio.run(run())


def test_order_request_gets_idempotency_key_and_correlates_on_id():
    seen = {}

    async def handler(ws):
        await _accept_auth(ws)
        raw = await ws.recv()
        req = json.loads(raw)
        seen["req"] = req
        await ws.send(json.dumps({
            "event": "result", "op": "add_order", "code": 200,
            "request_id": "r2", "data": {"order_id": "O123"}, "id": req["id"],
        }))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            response = await client.request(
                "add_order", params={"symbol": "BTC/USD", "side": "buy"},
            )
            await client.stop()
            return response

    response = asyncio.run(run())
    assert response["data"]["order_id"] == "O123"
    req = seen["req"]
    assert req["id"] == req["req_id"]  # order frames dual-tag req_id and id
    assert "idempotency_key" in req and req["idempotency_key"]


def test_order_request_reuses_supplied_idempotency_key():
    seen = {}

    async def handler(ws):
        await _accept_auth(ws)
        raw = await ws.recv()
        req = json.loads(raw)
        seen["req"] = req
        await ws.send(json.dumps({"event": "result", "id": req["id"], "data": {}}))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            await client.request("add_order", params={}, idempotency_key="fixed-key")
            await client.stop()

    asyncio.run(run())
    assert seen["req"]["idempotency_key"] == "fixed-key"


def test_proxy_error_frame_raises_typed_proxy_error():
    async def handler(ws):
        await _accept_auth(ws)
        raw = await ws.recv()
        req = json.loads(raw)
        await ws.send(json.dumps({
            "event": "error", "id": req["id"], "request_id": "r3",
            "data": {"error": {"code": "RATE_LIMITED", "message": "slow down"}},
        }))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            with pytest.raises(RateLimitedError) as excinfo:
                await client.request("add_order", params={})
            await client.stop()
            assert excinfo.value.request_id == "r3"

    asyncio.run(run())


# -- ping / heartbeat -----------------------------------------------------


def test_ping_returns_latency_from_pong():
    async def handler(ws):
        await _accept_auth(ws)
        raw = await ws.recv()
        assert json.loads(raw) == {"op": "ping"}
        await ws.send(json.dumps({"event": "pong"}))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url)
            await client.start()
            latency_ms = await client.ping()
            await client.stop()
            return latency_ms

    latency_ms = asyncio.run(run())
    assert latency_ms >= 0


def test_heartbeat_timeout_forces_close_and_reconnect_fires():
    connect_count = 0
    reconnected = asyncio.Event()

    async def handler(ws):
        nonlocal connect_count
        connect_count += 1
        await _accept_auth(ws)
        if connect_count == 1:
            # Never answer the ping; let the heartbeat timeout close us.
            await ws.wait_closed()
        else:
            reconnected.set()
            await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(
                base_url,
                reconnect=lambda attempt: 0.01,
                heartbeat_interval=0.05,
                heartbeat_timeout=0.1,
            )
            await client.start()
            await asyncio.wait_for(reconnected.wait(), timeout=5)
            await client.stop()

    asyncio.run(run())
    assert connect_count >= 2


# -- reconnect + subscription replay --------------------------------------


def test_default_backoff_reconnects_and_replays_subscriptions():
    connect_count = 0
    subscribed_channels: list[str] = []

    async def handler(ws):
        nonlocal connect_count
        connect_count += 1
        await _accept_auth(ws)
        raw = await ws.recv()
        req = json.loads(raw)
        subscribed_channels.append(req["params"]["channel"])
        await ws.send(json.dumps({
            "method": "subscribe", "success": True, "req_id": req["req_id"],
        }))
        if connect_count == 1:
            await ws.close()  # drop right after acking, to force a reconnect
        else:
            await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=lambda attempt: 0.01)
            await client.start()
            # Persisted, so it's replayed once connection 2 comes up.
            await client.subscribe("ticker", {"symbol": ["BTC/USD"]}, persist=True)
            await asyncio.sleep(0.3)  # let the drop + reconnect + replay happen
            await client.stop()

    asyncio.run(run())
    assert connect_count >= 2
    # Once live on connection 1, once replayed on connection 2.
    assert subscribed_channels.count("ticker") >= 2


def test_reconnect_none_ends_the_client_on_drop():
    async def handler(ws):
        await _accept_auth(ws)
        await ws.close()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=None)
            await client.start()
            await asyncio.wait_for(client.run_forever(), timeout=5)
            assert not client.is_connected

    asyncio.run(run())


def test_reconnect_policy_receives_attempt_and_can_give_up():
    calls: list[int] = []

    async def handler(ws):
        await _accept_auth(ws)
        await ws.close()

    def backoff(attempt):
        calls.append(attempt)
        return None if attempt >= 2 else 0.01

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=backoff)
            await client.start()
            await asyncio.wait_for(client.run_forever(), timeout=5)

    asyncio.run(run())
    assert calls == [1, 2]


# -- on_error ---------------------------------------------------------


def test_on_error_fires_for_a_raising_handler_and_loop_survives():
    errors: list[Exception] = []
    delivered = []

    async def handler(ws):
        await _accept_auth(ws)
        await ws.send(json.dumps({"channel": "ticker", "type": "update", "data": [{"bad": 1}]}))
        await ws.send(json.dumps({"channel": "ticker", "type": "update", "data": [{"bad": 2}]}))
        await ws.wait_closed()

    def bad_handler(msg):
        if len(delivered) == 0:
            delivered.append(msg)
            raise ValueError("boom")
        delivered.append(msg)

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, on_error=lambda exc: errors.append(exc))
            client.add_handler("ticker", bad_handler)
            await client.start()
            await asyncio.sleep(0.1)
            await client.stop()

    asyncio.run(run())
    assert len(delivered) == 2  # the second message still got through
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)


# -- request correlation internals -------------------------------------


def test_pending_requests_routing_and_fail_all():
    from proxy_client.websocket._rpc import _PendingRequests

    async def run():
        p = _PendingRequests()

        # the client passes the dialect's correlation key alongside the frame
        assert p.next_id() == 1
        f1 = p.create(1)
        assert p.resolve({"req_id": 1, "ok": True}, 1) is True
        assert f1.result() == {"req_id": 1, "ok": True}

        # order frames correlate on `id` even with no req_id present
        f2 = p.create(2)
        assert p.resolve({"event": "result", "id": 2}, 2) is True
        assert f2.result()["id"] == 2

        # the pong occupies a reserved slot, not a numeric id
        fp = p.expect_pong()
        assert p.resolve({"event": "pong"}, None) is True and fp.done()
        p.discard_pong()

        # a server-initiated push is not consumed - it goes to handlers
        assert p.resolve({"channel": "book", "data": []}, None) is False

        # an id-less Proxy error is swallowed here, never handed to handlers
        assert p.resolve({"event": "error", "data": {"error": {"code": "X"}}}, None) is True

        # fail_all propagates to every waiter and empties the map
        f3 = p.create(3)
        p.fail_all(ConnectionError("drop"))
        with pytest.raises(ConnectionError):
            f3.result()
        assert p.resolve({"req_id": 3}, 3) is False

    asyncio.run(run())


def test_in_flight_request_fails_fast_when_connection_drops():
    """A request awaiting a reply when the socket dies raises immediately
    (chained ConnectionError), rather than blocking until its timeout."""
    async def handler(ws):
        await _accept_auth(ws)
        await ws.recv()   # take the request frame ...
        await ws.close()  # ... then drop instead of answering it

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=None)
            await client.start()
            with pytest.raises(ConnectionError):
                await client.request("cancel_all", timeout=30)
            await client.stop()

    asyncio.run(run())


# -- reconnect hardening -------------------------------------------------------


def _ack_subscribe(req: dict) -> str:
    return json.dumps({"method": "subscribe", "success": True, "req_id": req["req_id"]})


def test_replay_runs_alongside_the_pump_so_messages_are_not_stalled():
    """Replay used to run before the read loop restarted, so each replayed
    subscribe waited out its full 10s timeout with no messages delivered."""
    connects = 0

    async def handler(ws):
        nonlocal connects
        connects += 1
        await _accept_auth(ws)
        await ws.send(_ack_subscribe(json.loads(await ws.recv())))
        if connects == 1:
            await ws.close()
            return
        await ws.send(json.dumps({"channel": "ticker", "data": [{"n": connects}]}))
        await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=lambda attempt: 0.01)
            got = asyncio.Event()
            events: list[str] = []
            client.add_handler("ticker", lambda msg: got.set())
            client.add_connection_listener(lambda ev: events.append(ev.kind))
            await client.start()
            await client.subscribe("ticker", {"symbol": ["BTC/USD"]})
            await asyncio.wait_for(got.wait(), timeout=2)  # was ~10s
            for _ in range(50):  # `reconnected` lands once the replay ack is read
                if "reconnected" in events:
                    break
                await asyncio.sleep(0.01)
            await client.stop()
            return events

    events = asyncio.run(run())
    assert events == ["disconnected", "reconnected", "stopped"]


def test_auth_refused_on_reconnect_stops_and_is_raised_from_run_forever():
    connects = 0

    async def handler(ws):
        nonlocal connects
        connects += 1
        await ws.recv()
        if connects == 1:
            await ws.send(json.dumps({"event": "auth", "code": "0"}))
            await ws.close()
        else:
            await ws.send(json.dumps({"event": "error", "code": "AUTH_FAILED",
                                      "message": "authentication failed"}))

    async def run():
        async with _fake_proxy(handler) as base_url:
            # A policy that never gives up: only the fatal classification
            # stops it.
            client = _client(base_url, reconnect=lambda attempt: 0.01)
            events: list[ConnectionEvent] = []
            client.add_connection_listener(events.append)
            await client.start()
            with pytest.raises(AuthFailedError):
                await asyncio.wait_for(client.run_forever(), timeout=5)
            return events

    events = asyncio.run(run())
    assert connects == 2  # refused once, never retried
    assert [e.kind for e in events] == ["disconnected", "closed"]
    assert isinstance(events[-1].error, AuthFailedError)


def test_transient_reconnect_failure_is_still_retried():
    connects = 0

    async def handler(ws):
        nonlocal connects
        connects += 1
        if connects == 2:
            await ws.close()  # transport-level failure mid-handshake
            return
        await _accept_auth(ws)
        if connects == 1:
            await ws.close()
        else:
            await ws.wait_closed()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=lambda attempt: 0.01)
            reconnected = asyncio.Event()
            client.add_connection_listener(
                lambda ev: reconnected.set() if ev.kind == "reconnected" else None
            )
            await client.start()
            await asyncio.wait_for(reconnected.wait(), timeout=5)
            await client.stop()

    asyncio.run(run())
    assert connects == 3


def test_reconnect_disabled_emits_closed():
    async def handler(ws):
        await _accept_auth(ws)
        await ws.close()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=None)
            kinds: list[str] = []
            client.add_connection_listener(lambda ev: kinds.append(ev.kind))
            await client.start()
            await asyncio.wait_for(client.run_forever(), timeout=5)  # returns, does not raise
            return kinds

    assert asyncio.run(run()) == ["disconnected", "closed"]


def test_unsubscribe_while_disconnected_still_forgets_the_subscription():
    async def handler(ws):
        await _accept_auth(ws)
        await ws.send(_ack_subscribe(json.loads(await ws.recv())))
        await ws.close()

    async def run():
        async with _fake_proxy(handler) as base_url:
            client = _client(base_url, reconnect=None)
            await client.start()
            await client.subscribe("ticker", {"symbol": ["BTC/USD"]})
            await asyncio.wait_for(client.run_forever(), timeout=5)
            with pytest.raises(RuntimeError):
                await client.unsubscribe("ticker", {"symbol": ["BTC/USD"]})
            return client._subscriptions

    # Left behind, it would be resubscribed on the next connection with no
    # handler listening.
    assert asyncio.run(run()) == []
