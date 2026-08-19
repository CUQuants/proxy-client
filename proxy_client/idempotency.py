"""Idempotency key lifecycle.

The rule (Proxy spec §3.3, and the trap called out in SDK_WRITEUP.md): a
mutating action needs a fresh key per *intent* — generated once when the
caller decides "place this order" — and that exact key must be reused,
unchanged, on every retry of that same intent. The timestamp and nonce, by
contrast, must be regenerated on every attempt including retries (they're
per-request, not per-intent; `ProxyClient.call` does this automatically).

This SDK does not retry automatically (see `client.py`), so the lifecycle
rule in practice is: call `new_idempotency_key()` once per order, hold onto
the value, and pass that same value to every `ProxyClient.call(...,
idempotency_key=key)` for that order — including retries after a
`RateLimitedError` or `IdempotencyInFlightError`. Generating a new key on
retry defeats the protection; the whole point is that a dropped connection
and a resend of the *same* key cannot place a second order.
"""

from __future__ import annotations

import uuid


def new_idempotency_key() -> str:
    """A fresh value per user/strategy action — not per HTTP attempt."""
    return uuid.uuid4().hex
