"""Parity tests for `proxy_client.signing`.

The two golden values below were computed directly against
`trading-gateway/proxy/signing.py` (not reimplemented independently) and
pasted in here as fixed expected output, so this test catches the SDK's
signing drifting from the server's — the one failure mode that matters most,
since a one-byte difference here is a silent, informationless 401 for every
caller.

To regenerate after a deliberate signing change on the server side::

    cd trading-gateway && python3 -c "
    from proxy.signing import compute_signature
    print(compute_signature(b'test-secret-do-not-use-in-prod', 'post',
        '/v1/okx', '1735939200', 'fixed-nonce-abc',
        b'{...}'))"
"""

from __future__ import annotations

from proxy_client.signing import canonical_string, compute_signature


def test_canonical_string_matches_the_proxy_exactly():
    result = canonical_string("post", "/v1/okx", "1735939200", "fixed-nonce-abc", b'{"a":1}')
    assert result == 'POST\n/v1/okx\n1735939200\nfixed-nonce-abc\n{"a":1}'


def test_signature_matches_a_value_computed_by_proxy_signing():
    secret = b"test-secret-do-not-use-in-prod"
    body = (
        b'{"exchange":"okx","action":"get_balance","operator_id":"cuq-014",'
        b'"operator_name":"J. Rivera","payload":{"ccy":"USDT"}}'
    )
    sig = compute_signature(secret, "post", "/v1/okx", "1735939200", "fixed-nonce-abc", body)
    assert sig == "zqkmusI4m8tdxnSOpgAI7222txE1QRfvssmH766KbOI="


def test_signature_covers_the_raw_bytes_not_a_reserialisation():
    secret = b"any-secret"
    one = b'{"a":1,"b":2}'
    two = b'{"b":2,"a":1}'
    sig_one = compute_signature(secret, "POST", "/v1/okx", "1", "n", one)
    sig_two = compute_signature(secret, "POST", "/v1/okx", "1", "n", two)
    assert sig_one != sig_two


def test_timestamp_is_used_as_sent_not_renormalised():
    secret = b"any-secret"
    a = compute_signature(secret, "POST", "/p", "1735939200", "n", b"")
    b = compute_signature(secret, "POST", "/p", "1735939200.0", "n", b"")
    assert a != b


def test_method_is_uppercased_before_signing():
    secret = b"any-secret"
    lower = compute_signature(secret, "post", "/v1/okx", "1", "n", b"{}")
    upper = compute_signature(secret, "POST", "/v1/okx", "1", "n", b"{}")
    assert lower == upper
