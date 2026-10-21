"""OKX spot adapter — outbound only, for now.

Outbound reuses `orders.OkxOrderBuilder` unchanged; OKX's `instId` is already
`BASE-QUOTE`, so the canonical symbol passes straight through. Inbound parsers
are not written yet: add them from pinned OKX response fixtures, the same way
`kraken_spot` was, when the first consumer moves its OKX parsing here.
"""

from __future__ import annotations

from typing import Any

from ..orders import OkxOrderBuilder
from .models import Balance, Fill, NormalizedOrder, Order, OrderAck

__all__ = ["Okx"]

_TODO = "OKX inbound parsing is not written yet — see proxy_client/venues/okx.py"


class Okx:
    name = "okx"
    order_transport = "rest"

    def __init__(self) -> None:
        self._builder = OkxOrderBuilder()

    def venue_symbol(self, symbol: str) -> str:
        return symbol.upper()

    def place_order(self, order: NormalizedOrder) -> dict:
        return self._builder.place_order(
            {**order, "symbol": self.venue_symbol(order["symbol"])}
        )

    def cancel_order(self, *, order_id: str, symbol: str) -> dict:
        return self._builder.cancel_order(
            order_id=order_id, symbol=self.venue_symbol(symbol)
        )

    def query_orders(self, order_ids: list[str]) -> dict:
        # OKX's get_order takes one ordId plus its instId, not a list.
        raise NotImplementedError(_TODO)

    def parse_order_ack(self, raw: Any) -> OrderAck:
        raise NotImplementedError(_TODO)

    def parse_orders(self, raw: Any) -> list[Order]:
        raise NotImplementedError(_TODO)

    def parse_fills(self, raw: Any) -> list[Fill]:
        raise NotImplementedError(_TODO)

    def parse_balances(self, raw: Any) -> list[Balance]:
        raise NotImplementedError(_TODO)
