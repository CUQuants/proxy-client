"""Builds the request body the Proxy expects (§3.1/§4.3).

Supports both credential types the server does (`trading-gateway/proxy/auth.py`):
operator (human) requests, and system (bot) requests carrying `system_name`.
The server rejects an operator credential that carries `system_name` and a
system credential that omits it — both `AUTH_FAILED` — so this module stays
a dumb serializer with no opinion on which shape is valid; that exclusivity
rule is enforced one level up, by which `ProxyClient` constructor a caller
used (`ProxyClient.for_operator` vs `ProxyClient.for_system`), not here.
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
    system_name: str | None = None,
    payload: dict[str, Any],
) -> bytes:
    """Serialize once. This exact byte string is both what's sent and what's
    signed — never re-serialized, per the re-serialization trap the whole
    SDK exists to avoid (SDK_WRITEUP.md's friction list, item 1).

    `system_name` is omitted from the body entirely when absent — direct
    (operator) requests omit it on the wire — matching
    `trading-gateway/proxy/envelope.py`'s `_clean()`, which treats a missing
    key and an explicit `null` the same way, so there is no reason to send
    the key at all for an operator request.
    """
    body: dict[str, Any] = {
        "exchange": exchange,
        "action": action,
        "operator_id": operator_id,
        "operator_name": operator_name,
    }
    if system_name:
        body["system_name"] = system_name
    body["payload"] = payload
    return json.dumps(body).encode("utf-8")
