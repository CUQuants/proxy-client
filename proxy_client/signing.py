"""HMAC-SHA256 request signing.

Ported byte-for-byte from `trading-gateway/proxy/signing.py::canonical_string`
and `::compute_signature`. This is the one place in the codebase where "close
enough" is a security bug: the Proxy recomputes the same canonical string over
the raw bytes it received and rejects anything that doesn't match exactly, so
this module must stay a verbatim port, not a reimplementation from the spec.

If `trading-gateway/proxy/signing.py` ever changes, this file needs the same
change on the same day. `tests/test_signing.py` pins a golden value cross
-checked against the server's own implementation to catch drift.
"""

from __future__ import annotations

import base64
import hashlib
import hmac


def canonical_string(
    method: str,
    path: str,
    timestamp: str,
    nonce: str,
    body: bytes,
) -> str:
    """Build the exact string the Proxy signs and verifies.

    `surrogateescape`, not `strict`: a body that happens not to be valid UTF-8
    must still produce a deterministic string here (so it fails signature
    verification cleanly on the server) rather than raise out of this
    function.
    """
    decoded = body.decode("utf-8", errors="surrogateescape")
    return f"{method.upper()}\n{path}\n{timestamp}\n{nonce}\n{decoded}"


def compute_signature(
    secret: bytes,
    method: str,
    path: str,
    timestamp: str,
    nonce: str,
    body: bytes,
) -> str:
    """Return the base64 HMAC-SHA256 signature for these request parts.

    `secret` is the credential's plaintext signing secret exactly as issued —
    the UTF-8 bytes of the base64 string handed out at credential creation.
    Do not base64-decode it first; the Proxy doesn't either.
    """
    canonical = canonical_string(method, path, timestamp, nonce, body)
    digest = hmac.new(
        secret, canonical.encode("utf-8", errors="surrogateescape"), hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode("ascii")
