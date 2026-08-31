"""Per-venue WebSocket wire vocabulary, behind one contract.

:class:`~proxy_client.websocket.client.ProxyWebSocketClient` owns
everything that is the same for every exchange — the reconnect supervisor,
the heartbeat loop, the signed handshake (``_connection``),
request/response correlation (``_rpc``), the subscription registry and its
replay, the channel-handler registry. A :class:`Dialect` supplies the four
things that are *not* the same:

1. **how a request is framed** — Kraken uses
   ``{"method": …, "params": …, "req_id": N}``; OKX uses
   ``{"op": …, "args": [ … ], "id": "N"}``;
2. **which field correlates a reply** to a waiting caller — Kraken's
   ``id`` / ``req_id`` vs OKX's ``id``;
3. **which field names the channel** of an inbound data frame — Kraken's
   top-level ``channel`` vs OKX's nested ``arg.channel``;
4. **how a rejection frame becomes a typed exception**.

This mirrors, deliberately, ``trading-gateway/proxy/ws/dialect.py``: the
server abstracts the same two venues over the same ``op:`` (Proxy control
frame) vs ``method:`` / ``args:`` (venue request) split. Composition, not
subclass hooks, so the supervisor has no venue branches to get wrong and
each dialect is unit-testable on its own with no socket — see
``tests/test_ws_dialect.py``.

Frame shapes here are pinned against ``trading-gateway/proxy/ws/okx.py``
(``OKXDialect.parse_client_frame`` / ``ack_frame`` / ``error_envelope``)
and ``proxy/ws/session.py``'s ``_send_error``. If that server contract
changes, the golden values in ``tests/test_ws_dialect.py`` change with it
— same approach as ``tests/test_signing.py``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Hashable

from ..errors import from_response

__all__ = [
    "Dialect",
    "KrakenDialect",
    "OKXDialect",
    "WebSocketRequestError",
    "dialect_for_exchange",
]


class WebSocketRequestError(RuntimeError):
    """A Kraken-native WS rejection (``{"success": false, "error": …}``).

    Deliberately not a ``ProxyError`` subclass: that hierarchy mirrors the
    Proxy's own §9 error-code table (see ``errors.py``), and a venue-level
    subscribe/method rejection isn't one of those codes — it's Kraken's
    own vocabulary, relayed verbatim per §4.3's pass-through principle.
    """

    def __init__(self, message: str, *, response: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.response = response


def _drop_none(params: dict | None) -> dict:
    return {k: v for k, v in (params or {}).items() if v is not None}


class Dialect(ABC):
    """One venue's WebSocket wire vocabulary. Stateless — safe to share."""

    #: The ``/v1/{exchange}/ws`` path segment, and the string the SDK maps
    #: from when no explicit dialect is passed.
    exchange: str

    @abstractmethod
    def request_frame(
        self,
        method: str,
        params: dict | None,
        req_id: int,
        idempotency_key: str | None,
        *,
        is_order: bool,
    ) -> tuple[dict, Hashable]:
        """Build the outbound frame for a correlated request.

        ``method`` is ``"subscribe"``, ``"unsubscribe"``, or an order
        method (then ``is_order`` is ``True`` and ``idempotency_key`` is
        already resolved to a non-``None`` value by the caller).

        Returns ``(frame, key)``. ``key`` is what ``_PendingRequests``
        registers the pending future under, and **must** equal what
        :meth:`response_key` later extracts from the reply.
        """

    @abstractmethod
    def response_key(self, msg: dict) -> Hashable | None:
        """The correlation key of an inbound frame, or ``None`` if it
        carries none (a server-initiated push)."""

    @abstractmethod
    def channel_of(self, msg: dict) -> str | None:
        """The channel an inbound data frame belongs to, for handler
        dispatch — or ``None``."""

    @abstractmethod
    def error_of(self, msg: dict) -> Exception | None:
        """Turn a reply into a typed exception if it is a rejection, else
        ``None``. Called on every reply :meth:`ProxyWebSocketClient.request`
        receives."""


class KrakenDialect(Dialect):
    """Kraken WS v2 — ``method`` / ``params`` / ``req_id``.

    The `xlrts` / `speedbyte` clients already speak this verbatim; the
    Proxy relays Kraken's own frames unreshaped (§4.3).
    """

    exchange = "kraken"

    def request_frame(
        self,
        method: str,
        params: dict | None,
        req_id: int,
        idempotency_key: str | None,
        *,
        is_order: bool,
    ) -> tuple[dict, Hashable]:
        frame: dict[str, Any] = {"method": method, "req_id": req_id}
        if params:
            frame["params"] = dict(params)
        if is_order:
            # `req_id` is Kraken's, echoed on a subscribe-shaped ack; `id`
            # is the Proxy's own allocation for the order's venue-side id.
            frame["id"] = req_id
        if idempotency_key:
            frame["idempotency_key"] = idempotency_key
        return frame, req_id

    def response_key(self, msg: dict) -> Hashable | None:
        ident = msg.get("id")
        if ident is not None:
            return ident
        return msg.get("req_id")

    def channel_of(self, msg: dict) -> str | None:
        return msg.get("channel")

    def error_of(self, msg: dict) -> Exception | None:
        # The Proxy's own rejection: `event: "error"`, carrying a §9 code
        # either nested under `data.error` or at the top level.
        if msg.get("event") == "error":
            data = msg.get("data")
            err = data.get("error") if isinstance(data, dict) else None
            if isinstance(err, dict):
                code = err.get("code", "UNKNOWN")
                message = err.get("message", "the Proxy rejected the request")
                detail = err.get("detail")
            else:
                code = str(msg.get("code", "UNKNOWN"))
                message = msg.get("message", "the Proxy rejected the request")
                detail = data
            return from_response(
                code, message, detail=detail, request_id=msg.get("request_id", "")
            )
        # Kraken's own rejection on a relayed ack — not a §9 code.
        if not msg.get("success", True):
            return WebSocketRequestError(
                f"WS request failed: {msg.get('error', 'Unknown error')}",
                response=msg,
            )
        return None


class OKXDialect(Dialect):
    """OKX WS v5 — ``op`` / ``args`` / ``id``, spot only.

    Covers the subscribe/unsubscribe path (the private ``orders`` /
    ``fills`` / ``account`` streams and the public market-data channels).
    Placing orders over the socket (OKX's ``op: "order"``) is not wired up
    here yet — no consumer needs it, and REST ``ProxyClient.call`` already
    covers OKX order placement. :meth:`request_frame` raises
    ``NotImplementedError`` for it rather than sending a frame the rest of
    this class can't correlate.
    """

    exchange = "okx"

    _SUBSCRIBE_OPS = ("subscribe", "unsubscribe")

    def request_frame(
        self,
        method: str,
        params: dict | None,
        req_id: int,
        idempotency_key: str | None,
        *,
        is_order: bool,
    ) -> tuple[dict, Hashable]:
        if is_order:
            raise NotImplementedError(
                "OKX order placement over WebSocket is not supported by this "
                "SDK yet — use ProxyClient.call(exchange='okx', action=...) "
                "for OKX orders. See proxy_client/websocket/_dialect.py."
            )
        if method not in self._SUBSCRIBE_OPS:
            raise NotImplementedError(
                f"OKX WebSocket dialect has no frame for {method!r}; only "
                "subscribe / unsubscribe are supported."
            )
        # OKX carries `id` as a string, and the Proxy echoes it verbatim on
        # both the ack and any error frame — so the pending future is keyed
        # on the string, not the int req_id.
        wire_id = str(req_id)
        frame = {"op": method, "args": [_drop_none(params)], "id": wire_id}
        return frame, wire_id

    def response_key(self, msg: dict) -> Hashable | None:
        return msg.get("id")

    def channel_of(self, msg: dict) -> str | None:
        arg = msg.get("arg")
        if isinstance(arg, dict):
            return arg.get("channel")
        return None

    def error_of(self, msg: dict) -> Exception | None:
        # proxy/ws/session.py `_send_error` + OKXDialect.error_envelope:
        # {"id": <echoed>, "event": "error", "code": <§9>, "message": …,
        #  "request_id": …, "detail": <optional>}. OKX has no `success`
        # field, so there's no Kraken-style native-rejection branch here.
        if msg.get("event") == "error":
            return from_response(
                str(msg.get("code", "UNKNOWN")),
                msg.get("message", "the Proxy rejected the request"),
                detail=msg.get("detail"),
                request_id=msg.get("request_id", ""),
            )
        return None


_BUILTIN: dict[str, type[Dialect]] = {
    "kraken": KrakenDialect,
    "okx": OKXDialect,
}


def dialect_for_exchange(exchange: str) -> Dialect:
    """The built-in dialect for ``exchange``.

    Raises ``ValueError`` for an exchange with no built-in dialect —
    pass ``dialect=`` to :class:`ProxyWebSocketClient` explicitly in that
    case rather than have the client guess a wire vocabulary.
    """
    try:
        return _BUILTIN[exchange]()
    except KeyError:
        raise ValueError(
            f"no built-in WebSocket dialect for exchange {exchange!r} "
            f"(known: {sorted(_BUILTIN)}); pass dialect=... explicitly."
        ) from None
