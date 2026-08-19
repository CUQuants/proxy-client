"""Builds the request body the Proxy expects (§3.1/§4.3).

v1 is scoped to operator (human) credentials only — see SDK_WRITEUP.md's
"next step" and the follow-up scoping decision to ship the proven,
human-facing half first. `system_name` (required for a bot/system
credential, and rejected by the server if an operator credential sends it —
`trading-gateway/proxy/auth.py`) is deliberately not a parameter here yet.
Adding it needs the operator/system exclusivity rule handled correctly, not
just a new kwarg threaded through — that's the fast-follow, not a v1 detail.
"""

from __future__ import annotations

import json
from typing import Any


def build_body(
    *,
    exchange: str,
    action: str,
    operator_id: str,
    operator_name: str,
    payload: dict[str, Any],
) -> bytes:
    """Serialize once. This exact byte string is both what's sent and what's
    signed — never re-serialized, per the re-serialization trap the whole
    SDK exists to avoid (SDK_WRITEUP.md's friction list, item 1)."""
    return json.dumps(
        {
            "exchange": exchange,
            "action": action,
            "operator_id": operator_id,
            "operator_name": operator_name,
            "payload": payload,
        }
    ).encode("utf-8")
