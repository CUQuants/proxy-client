"""The contract every venue adapter satisfies.

An adapter is the *only* place venue knowledge lives: how a canonical symbol
is spelled, what the payload fields are called, how a response is parsed,
and which transport an operation travels on. `TradingClient` holds no venue
knowledge at all, so adding a venue is one new adapter class — never an edit
to the client, the models, or a consumer.

Why a `Protocol`, as with `orders.OrderBuilder`: adapters share no behaviour,
only a shape. Each owns its field mapping completely.

Adapters are **pure** — no I/O, no client reference. That keeps every one of
them testable from a pinned response fixture, and keeps transport concerns
(signing, idempotency, retries) where they already live, in `ProxyClient` and
`ProxyWebSocketClient`.
"""

from __future__ import annotations

from typing import Any, Literal, Protocol

from .models import Balance, Fill, NormalizedOrder, Order, OrderAck

__all__ = ["OrderTransport", "VenueAdapter"]

#: Where order entry travels. Kraken Prime has no REST order entry at all, so
#: this is a property of the venue, not a caller's choice.
OrderTransport = Literal["rest", "ws"]


class VenueAdapter(Protocol):
    name: str                         # the Proxy's `exchange` key, e.g. "kraken"
    order_transport: OrderTransport

    # -- outbound: canonical → venue ------------------------------------
    def venue_symbol(self, symbol: str) -> str: ...
    def place_order(self, order: NormalizedOrder) -> dict: ...
    def cancel_order(self, *, order_id: str, symbol: str) -> dict: ...
    def query_orders(self, order_ids: list[str]) -> dict: ...

    # -- inbound: venue → canonical -------------------------------------
    def parse_order_ack(self, raw: Any) -> OrderAck: ...
    def parse_orders(self, raw: Any) -> list[Order]: ...
    def parse_fills(self, raw: Any) -> list[Fill]: ...
    def parse_balances(self, raw: Any) -> list[Balance]: ...
