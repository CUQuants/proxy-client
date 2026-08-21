from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from proxy_client import ProxyClient, ProxyResult
from proxy_client.errors import AuthFailedError, ProxyUnreachableError, RateLimitedError
from proxy_client.signing import compute_signature


def _client() -> ProxyClient:
    return ProxyClient(
        base_url="https://proxy.example.com",
        api_key="cuq_op_test",
        secret="s3cr3t",
        operator_id="cuq-014",
        operator_name="J. Rivera",
    )


def _fake_response(status_code: int, body: dict, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.headers = headers or {}
    return resp


def test_call_signs_the_exact_bytes_it_sends():
    """`httpx.Client.post` must receive `content=body`, never `data=` or
    `json=`, and the signature header must verify against those exact
    bytes."""
    client = _client()
    ok = _fake_response(200, {"data": {"balance": "100"}})

    with patch.object(client._client, "post", return_value=ok) as post:
        client.call("okx", "get_balance", {"ccy": "USDT"})

    _, kwargs = post.call_args
    sent_body = kwargs["content"]
    headers = kwargs["headers"]

    expected_sig = compute_signature(
        b"s3cr3t", "POST", "/v1/okx", headers["X-Timestamp"], headers["X-Nonce"], sent_body
    )
    assert headers["X-Signature"] == expected_sig
    assert json.loads(sent_body)["operator_id"] == "cuq-014"
    assert "system_name" not in json.loads(sent_body)  # v1: operator-only


def test_call_returns_the_data_field():
    client = _client()
    ok = _fake_response(200, {"data": {"balance": "100"}})
    with patch.object(client._client, "post", return_value=ok):
        result = client.call("okx", "get_balance", {"ccy": "USDT"})
    assert result == {"balance": "100"}


def test_idempotency_key_is_only_sent_when_given():
    client = _client()
    ok = _fake_response(200, {"data": {}})
    with patch.object(client._client, "post", return_value=ok) as post:
        client.call("okx", "get_balance", {})
    assert "X-Idempotency-Key" not in post.call_args.kwargs["headers"]

    with patch.object(client._client, "post", return_value=ok) as post:
        client.call("okx", "place_order", {}, idempotency_key="abc-123")
    assert post.call_args.kwargs["headers"]["X-Idempotency-Key"] == "abc-123"


def test_error_response_raises_the_typed_exception():
    client = _client()
    err = _fake_response(
        401,
        {"error": {"code": "AUTH_FAILED", "message": "Authentication failed.", "request_id": "r1"}},
    )
    with patch.object(client._client, "post", return_value=err):
        with pytest.raises(AuthFailedError) as exc_info:
            client.call("okx", "get_balance", {})
    assert exc_info.value.request_id == "r1"


def test_rate_limited_carries_retry_after_header():
    client = _client()
    err = _fake_response(
        429,
        {"error": {"code": "RATE_LIMITED", "message": "slow down"}},
        headers={"Retry-After": "3"},
    )
    with patch.object(client._client, "post", return_value=err):
        with pytest.raises(RateLimitedError) as exc_info:
            client.call("okx", "get_balance", {})
    assert exc_info.value.retry_after == 3.0


def test_transport_failure_raises_proxy_unreachable():
    client = _client()
    with patch.object(
        client._client, "post", side_effect=httpx.ConnectError("no route")
    ):
        with pytest.raises(ProxyUnreachableError):
            client.call("okx", "get_balance", {})


def test_acall_uses_the_async_client_not_a_thread_wrapper_of_call():
    """acall() must go through `_aclient`, and never touch the sync
    `_client` at all — that's the whole point of not being a thin
    executor wrapper."""
    client = _client()
    ok = _fake_response(200, {"data": {"balance": "100"}})

    with patch.object(client._aclient, "post", new=AsyncMock(return_value=ok)) as post, \
         patch.object(client._client, "post") as sync_post:
        result = asyncio.run(client.acall("okx", "get_balance", {"ccy": "USDT"}))

    assert result == {"balance": "100"}
    post.assert_awaited_once()
    sync_post.assert_not_called()


def test_acall_signs_the_exact_bytes_it_sends():
    client = _client()
    ok = _fake_response(200, {"data": {}})

    with patch.object(client._aclient, "post", new=AsyncMock(return_value=ok)) as post:
        asyncio.run(client.acall("okx", "place_order", {"sz": "1"}, idempotency_key="k1"))

    _, kwargs = post.call_args
    sent_body = kwargs["content"]
    headers = kwargs["headers"]
    expected_sig = compute_signature(
        b"s3cr3t", "POST", "/v1/okx", headers["X-Timestamp"], headers["X-Nonce"], sent_body
    )
    assert headers["X-Signature"] == expected_sig
    assert headers["X-Idempotency-Key"] == "k1"


# -- system (bot) credentials -------------------------------------------------


def _system_client() -> ProxyClient:
    return ProxyClient.for_system(
        base_url="https://proxy.example.com",
        api_key="cuq_sys_test",
        secret="s3cr3t",
        operator_id="cuq-001",
        operator_name="Automated - speedbyte",
        system_name="speedbyte",
    )


def test_for_system_call_includes_system_name_in_signed_body():
    client = _system_client()
    ok = _fake_response(200, {"data": {}})

    with patch.object(client._client, "post", return_value=ok) as post:
        client.call("kraken", "get_balance", {})

    _, kwargs = post.call_args
    sent_body = kwargs["content"]
    headers = kwargs["headers"]

    expected_sig = compute_signature(
        b"s3cr3t", "POST", "/v1/kraken", headers["X-Timestamp"], headers["X-Nonce"], sent_body
    )
    assert headers["X-Signature"] == expected_sig
    parsed = json.loads(sent_body)
    assert parsed["system_name"] == "speedbyte"
    assert parsed["operator_id"] == "cuq-001"


def test_for_system_requires_system_name():
    with pytest.raises(ValueError):
        ProxyClient.for_system(
            base_url="https://proxy.example.com",
            api_key="cuq_sys_test",
            secret="s3cr3t",
            operator_id="cuq-001",
            operator_name="Automated - speedbyte",
            system_name="",
        )


def test_for_operator_is_equivalent_to_init():
    via_init = _client()
    via_classmethod = ProxyClient.for_operator(
        base_url="https://proxy.example.com",
        api_key="cuq_op_test",
        secret="s3cr3t",
        operator_id="cuq-014",
        operator_name="J. Rivera",
    )
    ok = _fake_response(200, {"data": {}})

    bodies = []
    for client in (via_init, via_classmethod):
        with patch.object(client._client, "post", return_value=ok) as post:
            client.call("okx", "get_balance", {"ccy": "USDT"})
        bodies.append(post.call_args.kwargs["content"])

    assert bodies[0] == bodies[1]
    assert "system_name" not in json.loads(bodies[1])
    assert via_classmethod.system_name is None


def test_acall_transport_failure_raises_proxy_unreachable():
    client = _client()
    with patch.object(
        client._aclient, "post", new=AsyncMock(side_effect=httpx.ConnectError("no route"))
    ):
        with pytest.raises(ProxyUnreachableError):
            asyncio.run(client.acall("okx", "get_balance", {}))


def test_call_returns_a_proxy_result_that_behaves_like_a_plain_dict():
    client = _client()
    ok = _fake_response(200, {"data": {"balance": "100"}})
    with patch.object(client._client, "post", return_value=ok):
        result = client.call("okx", "get_balance", {"ccy": "USDT"})
    assert isinstance(result, ProxyResult)
    assert isinstance(result, dict)
    assert result == {"balance": "100"}
    assert result["balance"] == "100"
    assert dict(result) == {"balance": "100"}


def test_call_reports_idempotent_replay_when_the_header_is_set():
    client = _client()
    replayed = _fake_response(
        200, {"data": {"order_id": "1"}}, headers={"X-Idempotent-Replay": "1"}
    )
    with patch.object(client._client, "post", return_value=replayed):
        result = client.call("okx", "place_order", {}, idempotency_key="k1")
    assert result.idempotent_replay is True


def test_call_reports_no_replay_when_the_header_is_absent():
    client = _client()
    fresh = _fake_response(200, {"data": {"order_id": "1"}})
    with patch.object(client._client, "post", return_value=fresh):
        result = client.call("okx", "place_order", {}, idempotency_key="k1")
    assert result.idempotent_replay is False


def test_non_json_body_raises_proxy_unreachable_not_a_raw_json_error():
    client = _client()
    bad = MagicMock()
    bad.status_code = 502
    bad.json.side_effect = ValueError("not json")
    bad.headers = {}
    with patch.object(client._client, "post", return_value=bad):
        with pytest.raises(ProxyUnreachableError):
            client.call("okx", "get_balance", {})


def test_non_dict_json_body_raises_proxy_unreachable_not_attributeerror():
    """A misconfigured intermediary could return valid JSON that isn't the
    Proxy's envelope shape at all (e.g. a bare list). That must fail as a
    clean ProxyUnreachableError, not an unrelated AttributeError out of
    `.get()`."""
    client = _client()
    weird = MagicMock()
    weird.status_code = 200
    weird.json.return_value = ["not", "an", "envelope"]
    weird.headers = {}
    with patch.object(client._client, "post", return_value=weird):
        with pytest.raises(ProxyUnreachableError):
            client.call("okx", "get_balance", {})


def test_acall_non_json_body_raises_proxy_unreachable():
    client = _client()
    bad = MagicMock()
    bad.status_code = 502
    bad.json.side_effect = ValueError("not json")
    bad.headers = {}
    with patch.object(client._aclient, "post", new=AsyncMock(return_value=bad)):
        with pytest.raises(ProxyUnreachableError):
            asyncio.run(client.acall("okx", "get_balance", {}))


def test_acall_reports_idempotent_replay():
    client = _client()
    replayed = _fake_response(
        200, {"data": {"order_id": "1"}}, headers={"X-Idempotent-Replay": "1"}
    )
    with patch.object(client._aclient, "post", new=AsyncMock(return_value=replayed)):
        result = asyncio.run(client.acall("okx", "place_order", {}, idempotency_key="k1"))
    assert result.idempotent_replay is True


def test_context_managers_close_both_clients():
    with _client() as client:
        pass
    assert client._client.is_closed

    async def _run() -> None:
        async with _client() as client:
            assert not client._aclient.is_closed
        assert client._aclient.is_closed

    asyncio.run(_run())
