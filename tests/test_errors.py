from __future__ import annotations

import pytest

from proxy_client.errors import (
    ActionNotPermittedError,
    AuthFailedError,
    CredentialExpiredError,
    ExchangeError,
    IdempotencyInFlightError,
    IdempotencyKeyRequiredError,
    IdempotencyKeyReusedError,
    KillSwitchActiveError,
    LogUnavailableError,
    MalformedRequestError,
    ProxyError,
    RateLimitedError,
    RiskLimitExceededError,
    StreamParamsConflictError,
    from_response,
)

# Every code the server can send, and the class + retryable value it must map
# to. One row per code in `trading-gateway/proxy/errors.py`'s §9 table (plus
# the two Milestone-5 reserved codes) — see test_error_parity.py for a check
# that this set itself stays complete against the server's actual code list.
CASES: list[tuple[str, type[ProxyError], bool | None]] = [
    ("AUTH_FAILED", AuthFailedError, False),
    ("CREDENTIAL_EXPIRED", CredentialExpiredError, False),
    ("ACTION_NOT_PERMITTED", ActionNotPermittedError, False),
    ("EXCHANGE_ERROR", ExchangeError, None),
    ("MALFORMED_REQUEST", MalformedRequestError, False),
    ("IDEMPOTENCY_KEY_REQUIRED", IdempotencyKeyRequiredError, False),
    ("IDEMPOTENCY_KEY_REUSED", IdempotencyKeyReusedError, False),
    ("IDEMPOTENCY_IN_FLIGHT", IdempotencyInFlightError, True),
    ("LOG_UNAVAILABLE", LogUnavailableError, True),
    ("RATE_LIMITED", RateLimitedError, True),
    ("STREAM_PARAMS_CONFLICT", StreamParamsConflictError, False),
    ("RISK_LIMIT_EXCEEDED", RiskLimitExceededError, False),
    ("KILL_SWITCH_ACTIVE", KillSwitchActiveError, None),
]


@pytest.mark.parametrize("code,expected_cls,expected_retryable", CASES)
def test_every_known_code_maps_to_its_typed_class_and_retryable_value(
    code, expected_cls, expected_retryable
):
    exc = from_response(code, "some message", detail={"x": 1}, request_id="r-1")
    assert type(exc) is expected_cls
    assert exc.code == code
    assert exc.retryable is expected_retryable
    assert exc.detail == {"x": 1}
    assert exc.request_id == "r-1"


def test_every_case_above_covers_a_distinct_code():
    """Guards the test data itself: CASES must not silently drop a code to
    duplication."""
    codes = [code for code, _, _ in CASES]
    assert len(codes) == len(set(codes))


def test_idempotency_key_reused_is_never_retryable():
    exc = from_response("IDEMPOTENCY_KEY_REUSED", "reused")
    assert isinstance(exc, IdempotencyKeyReusedError)
    assert exc.retryable is False


def test_idempotency_in_flight_is_retryable():
    exc = from_response("IDEMPOTENCY_IN_FLIGHT", "in flight")
    assert isinstance(exc, IdempotencyInFlightError)
    assert exc.retryable is True


def test_rate_limited_carries_retry_after():
    exc = from_response("RATE_LIMITED", "slow down", retry_after=2.5)
    assert isinstance(exc, RateLimitedError)
    assert exc.retry_after == 2.5


def test_rate_limited_without_a_retry_after_header_defaults_to_none():
    exc = from_response("RATE_LIMITED", "slow down")
    assert isinstance(exc, RateLimitedError)
    assert exc.retry_after is None


def test_exchange_error_exposes_outcome_unknown():
    known = from_response("EXCHANGE_ERROR", "rejected", detail={"outcome_unknown": False})
    unknown = from_response("EXCHANGE_ERROR", "timeout", detail={"outcome_unknown": True})
    assert isinstance(known, ExchangeError) and known.outcome_unknown is False
    assert isinstance(unknown, ExchangeError) and unknown.outcome_unknown is True


def test_exchange_error_with_no_detail_is_not_outcome_unknown():
    exc = from_response("EXCHANGE_ERROR", "rejected")
    assert isinstance(exc, ExchangeError)
    assert exc.outcome_unknown is False


def test_unrecognised_code_falls_back_to_base_and_is_not_retryable():
    exc = from_response("SOME_FUTURE_CODE", "new thing")
    assert type(exc) is ProxyError
    assert exc.code == "SOME_FUTURE_CODE"
    assert exc.retryable is False


def test_reserved_milestone_5_codes_are_recognised_ahead_of_the_server_sending_them():
    """RISK_LIMIT_EXCEEDED / KILL_SWITCH_ACTIVE aren't raised by the current
    server build, but this SDK should already map them correctly so the day
    Milestone 5 ships, no SDK release is required for a caller to get a typed
    exception instead of the ProxyError fallback."""
    risk = from_response("RISK_LIMIT_EXCEEDED", "over the line")
    kill = from_response("KILL_SWITCH_ACTIVE", "trading halted")
    assert isinstance(risk, RiskLimitExceededError)
    assert isinstance(kill, KillSwitchActiveError)


# -- ExchangeError.venue_error ------------------------------------------------
# Detail shapes pinned against trading-gateway: `exchange_declined` callers in
# proxy/ws/session.py (Kraken WS), proxy/exchanges/kraken.py (Kraken REST) and
# proxy/exchanges/okx.py (OKX REST).


def test_venue_error_kraken_ws_string():
    exc = from_response(
        "EXCHANGE_ERROR",
        "The exchange rejected or failed the request.",
        detail={"error": "EOrder:Unknown order", "exchange": "kraken_prime", "outcome_unknown": False},
    )
    assert exc.venue_error == "EOrder:Unknown order"
    assert "EOrder:Unknown order" in str(exc)
    # `.message` stays the Proxy's own text; only str() gains the venue's.
    assert exc.message == "The exchange rejected or failed the request."


def test_venue_error_kraken_rest_list_deduplicated_against_body():
    exc = from_response(
        "EXCHANGE_ERROR",
        "rejected",
        detail={
            "kraken_http_status": 200,
            "kraken_error": ["EOrder:Unknown order"],
            "body": {"error": ["EOrder:Unknown order"]},
            "exchange": "kraken_prime",
            "outcome_unknown": False,
        },
    )
    assert exc.venue_error == "EOrder:Unknown order"


def test_venue_error_okx_smsg_then_msg():
    per_order = from_response(
        "EXCHANGE_ERROR",
        "rejected",
        detail={"okx_code": "1", "body": {"code": "1", "msg": "", "data": [{"sCode": "51008", "sMsg": "Insufficient balance"}]}},
    )
    assert per_order.venue_error == "Insufficient balance"
    top = from_response(
        "EXCHANGE_ERROR", "rejected", detail={"body": {"code": "50011", "msg": "Too Many Requests", "data": []}}
    )
    assert top.venue_error == "Too Many Requests"


def test_venue_error_none_when_venue_said_nothing():
    timeout = from_response(
        "EXCHANGE_ERROR",
        "The exchange did not answer in time.",
        detail={"reason": "timeout", "exchange": "kraken_prime", "outcome_unknown": True, "message": "The exchange did not answer in time."},
    )
    assert timeout.venue_error is None
    assert str(timeout) == "EXCHANGE_ERROR: The exchange did not answer in time."
    assert from_response("EXCHANGE_ERROR", "rejected").venue_error is None


def test_rate_limited_reads_retry_after_from_detail_when_no_header():
    # WebSocket rejections carry no headers; proxy/ratelimit.py puts the
    # number in detail.retry_after_seconds.
    exc = from_response(
        "RATE_LIMITED", "Too many requests.",
        detail={"scope": "caller", "reason": "rate limit", "retry_after_seconds": 0.569},
    )
    assert isinstance(exc, RateLimitedError) and exc.retry_after == pytest.approx(0.569)


def test_rate_limited_header_wins_over_detail():
    exc = from_response(
        "RATE_LIMITED", "slow", detail={"retry_after_seconds": 0.5}, retry_after=2.0
    )
    assert exc.retry_after == 2.0
