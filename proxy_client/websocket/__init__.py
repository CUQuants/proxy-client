"""ProxyWebSocketClient — the WebSocket counterpart to `ProxyClient`.

Reaches trading-gateway's `/v1/{exchange}/ws` route: one signed handshake
(`compute_signature`, same as REST — see `signing.py`), then the venue's
own subscribe/data vocabulary travels verbatim (§4.3's pass-through
principle), plus two Proxy-only control frames (`op:"ping"` /
`event:"pong"`, and the `event:"auth"` handshake reply).

**Kraken and OKX** are both supported, via a pluggable :class:`Dialect`
(see ``_dialect``) — Kraken speaks `method`/`params`/`req_id`, OKX speaks
`op`/`args`/`id`. The default is chosen from the ``exchange`` argument;
pass ``dialect=`` for a venue the SDK has no built-in for. OKX order
*placement* over the socket is not wired up yet (no consumer needs it —
`ProxyClient.call` covers OKX orders); the OKX dialect raises
``NotImplementedError`` for an order method rather than send an
uncorrelatable frame.

Both `xlrts` and `speedbyte` currently hand-roll this handshake in a local
`ProxySession`-style class (`ws_url()` / `ws_auth_frame()`), each importing
`compute_signature` directly. This package is the one shared implementation
those should eventually delegate to instead.

## Layout

This is a package, not a single module, because a socket that can place
orders carries five separable concerns:

- ``_backoff``     — the reconnect timing policy (a plain, stateless callable).
- ``_connection``  — one connection's lifetime: connect, sign-and-handshake,
                      send, iterate, close. Knows nothing about reconnects,
                      request correlation, or channel handlers.
- ``_dialect``     — the per-venue wire vocabulary: how a request is framed,
                      which field correlates a reply, which field names a
                      channel, how a rejection becomes a typed exception.
                      Composition, not subclass hooks — mirrors
                      ``trading-gateway/proxy/ws/dialect.py``.
- ``_rpc``         — request/response correlation: req_id allocation, the
                      pending-future map, and routing a reply (or a pong)
                      back to its awaiting caller, given the dialect's key.
- ``client``       — :class:`ProxyWebSocketClient` itself: the reconnect
                      supervisor, the heartbeat loop, the subscription
                      registry + replay, the channel-handler registry, and
                      the public API.

## Scope — mechanics vs. policy

A WebSocket connection dropping isn't a business decision the way retrying
a mutating REST call is (`ProxyClient.call()` deliberately never
auto-retries — see its module docstring). Keeping the socket alive has no
business meaning, so the client *does* own the reconnect loop and the
heartbeat loop — but exposes their timing as plain parameters
(`reconnect`, `heartbeat_interval`/`heartbeat_timeout`), not baked-in
constants, since *how aggressive* is a per-deployment judgment call.

Routing an inbound frame to whichever handler is registered for its
channel is the same kind of mechanical, error-prone-to-hand-roll plumbing
as the handshake — so `add_handler()` / message dispatch stays here too.
What a registered handler *does* with a message is pure application logic
and none of this package's business; when a handler raises, `on_error` (an
injected callable, default: log and keep the connection alive) is the
seam, so one bad handler can't take down the read loop.

A *different* kind of error — the Proxy or the exchange rejecting a
request outright (bad auth, a rejected order, `RATE_LIMITED`) — is not a
callback matter; `request()` raises a typed exception synchronously, exact
parity with how `ProxyClient.call()` already works.
"""

from __future__ import annotations

try:
    import websockets as _websockets  # noqa: F401
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "ProxyWebSocketClient requires the 'websockets' package. "
        "Install with: pip install cuq-proxy-client[websocket]"
    ) from _exc

from ._backoff import ReconnectBackoff, default_backoff
from ._dialect import Dialect, KrakenDialect, OKXDialect, WebSocketRequestError
from .client import ProxyWebSocketClient

__all__ = [
    "ProxyWebSocketClient",
    "ReconnectBackoff",
    "default_backoff",
    "WebSocketRequestError",
    "Dialect",
    "KrakenDialect",
    "OKXDialect",
]
