from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from proxy_client import ReportingClient
from proxy_client.errors import (
    ActionNotPermittedError,
    AuthFailedError,
    ProxyUnreachableError,
)
from proxy_client.signing import compute_signature


def _client() -> ReportingClient:
    return ReportingClient(
        base_url="https://reporting.example.com",
        api_key="cuq_op_test",
        secret="s3cr3t",
        operator_id="cuq-002",
        operator_name="Local Dev Operator",
    )


def _fake_response(status_code: int, body: dict, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.headers = headers or {}
    return resp


# -- signing / envelope shape -------------------------------------------------


def test_unresolved_orders_signs_the_exact_bytes_it_sends():
    client = _client()
    ok = _fake_response(200, {"unresolved_orders": []})

    with patch.object(client._client, "post", return_value=ok) as post:
        client.unresolved_orders()

    _, kwargs = post.call_args
    sent_body = kwargs["content"]
    headers = kwargs["headers"]

    expected_sig = compute_signature(
        b"s3cr3t",
        "POST",
        "/v1/admin/audit/unresolved",
        headers["X-Timestamp"],
        headers["X-Nonce"],
        sent_body,
    )
    assert headers["X-Signature"] == expected_sig
    parsed = json.loads(sent_body)
    assert parsed == {"operator_id": "cuq-002", "operator_name": "Local Dev Operator"}
    assert "payload" not in parsed  # only /logs sends one
    assert "exchange" not in parsed and "action" not in parsed  # not a trading envelope


def test_no_idempotency_key_header_is_ever_sent():
    """Nothing here mutates, so there's no idempotency concept to carry —
    unlike ProxyClient, this class has no idempotency_key parameter at all."""
    client = _client()
    ok = _fake_response(200, {"unresolved_orders": []})
    with patch.object(client._client, "post", return_value=ok) as post:
        client.unresolved_orders()
    assert "X-Idempotency-Key" not in post.call_args.kwargs["headers"]


# -- response shape: NOT unwrapped under "data" -------------------------------


def test_responses_are_not_unwrapped_under_data():
    """The reporting API answers at the top level, unlike the Proxy's
    {"data": ...} envelope — this is the one thing that must not be reused
    from ProxyClient._handle_response unchanged."""
    client = _client()
    ok = _fake_response(200, {"months_of_runway": 12, "rows_last_24h": 3})
    with patch.object(client._client, "post", return_value=ok):
        result = client.log_health()
    assert result == {"months_of_runway": 12, "rows_last_24h": 3}


def test_unresolved_orders_returns_the_list_directly():
    client = _client()
    ok = _fake_response(200, {"unresolved_orders": [{"request_id": "r1"}]})
    with patch.object(client._client, "post", return_value=ok):
        result = client.unresolved_orders()
    assert result == [{"request_id": "r1"}]


def test_auth_failures_returns_the_list_directly():
    client = _client()
    ok = _fake_response(200, {"auth_failures": [{"api_key_id": "k1", "failures": 5}]})
    with patch.object(client._client, "post", return_value=ok):
        result = client.auth_failures()
    assert result == [{"api_key_id": "k1", "failures": 5}]


# -- errors --------------------------------------------------------------


def test_error_response_raises_the_typed_exception():
    client = _client()
    err = _fake_response(
        401,
        {"error": {"code": "AUTH_FAILED", "message": "Authentication failed.", "request_id": "r1"}},
    )
    with patch.object(client._client, "post", return_value=err):
        with pytest.raises(AuthFailedError) as exc_info:
            client.log_health()
    assert exc_info.value.request_id == "r1"


def test_authenticated_but_not_authorized_raises_action_not_permitted():
    """The one check this API adds beyond what the Proxy itself does
    (operators.admin_role null) — still just ACTION_NOT_PERMITTED, already
    modeled in errors.py, so no new exception type was needed."""
    client = _client()
    err = _fake_response(
        403,
        {"error": {"code": "ACTION_NOT_PERMITTED", "message": "not permitted", "request_id": "r2"}},
    )
    with patch.object(client._client, "post", return_value=err):
        with pytest.raises(ActionNotPermittedError):
            client.unresolved_orders()


def test_transport_failure_raises_proxy_unreachable():
    client = _client()
    with patch.object(
        client._client, "post", side_effect=httpx.ConnectError("no route")
    ):
        with pytest.raises(ProxyUnreachableError):
            client.log_health()


# -- async ---------------------------------------------------------------


def test_async_methods_use_the_async_client_not_a_thread_wrapper():
    client = _client()
    ok = _fake_response(200, {"unresolved_orders": []})

    with patch.object(client._aclient, "post", new=AsyncMock(return_value=ok)) as post, \
         patch.object(client._client, "post") as sync_post:
        result = asyncio.run(client.aunresolved_orders())

    assert result == []
    post.assert_awaited_once()
    sync_post.assert_not_called()


# -- query_logs filter building -------------------------------------------


def test_query_logs_omits_unset_filters_entirely():
    client = _client()
    ok = _fake_response(200, {"logs": [], "next_cursor": None})
    with patch.object(client._client, "post", return_value=ok) as post:
        client.query_logs(limit=5)

    payload = json.loads(post.call_args.kwargs["content"])["payload"]
    assert payload == {"limit": 5}


def test_query_logs_encodes_datetime_filters_as_isoformat():
    client = _client()
    ok = _fake_response(200, {"logs": [], "next_cursor": None})
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with patch.object(client._client, "post", return_value=ok) as post:
        client.query_logs(since=when)

    payload = json.loads(post.call_args.kwargs["content"])["payload"]
    assert payload == {"since": when.isoformat()}


def test_query_logs_with_no_filters_sends_an_empty_payload_object():
    """Distinct from the three fixed endpoints: /logs always sends a
    payload key (even {}), since query_logs() is always this endpoint,
    unlike the fixed endpoints which never send one at all."""
    client = _client()
    ok = _fake_response(200, {"logs": [], "next_cursor": None})
    with patch.object(client._client, "post", return_value=ok) as post:
        client.query_logs()

    parsed = json.loads(post.call_args.kwargs["content"])
    assert parsed["payload"] == {}


def test_query_logs_returns_the_page_directly():
    client = _client()
    ok = _fake_response(200, {"logs": [{"request_id": "r1"}], "next_cursor": "abc"})
    with patch.object(client._client, "post", return_value=ok):
        result = client.query_logs(limit=1)
    assert result == {"logs": [{"request_id": "r1"}], "next_cursor": "abc"}


# -- repr scrubbing --------------------------------------------------------


def test_repr_never_leaks_the_secret():
    client = _client()
    assert "s3cr3t" not in repr(client)
    assert "cuq-002" in repr(client)
