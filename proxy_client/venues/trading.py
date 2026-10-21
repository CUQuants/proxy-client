"""`TradingClient` — one venue-agnostic surface over the existing transports.

It holds no venue knowledge. Every call is the same three steps: the adapter
builds the venue payload, `ProxyClient` (or the WebSocket client) carries it
through the Proxy, and the adapter parses the reply into `models`. Signing,
idempotency headers and error classification are untouched — a rejection
still raises the same typed `ProxyError` it always did, and is never
normalized, because `ExchangeError.outcome_unknown` is what decides whether a
failed order may be retried.

Idempotency keys are **required** arguments on place/cancel, not generated
here. The rule is one key per order *intent*, reused unchanged on retry; a
client that minted its own key per call would hand every retry a fresh one
and duplicate the order. Only the caller knows where an intent starts.

Async only: every intended consumer (speedbyte, the RMU, the WebSocket
client) is async already.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import VenueAdapter
from .models import Balance, Fill, NormalizedOrder, Order, OrderAck

if TYPE_CHECKING:
    from ..client import ProxyClient, ProxyResult

__all__ = ["TradingClient"]


class TradingClient:
    def __init__(
        self,
        venue: VenueAdapter,
        rest: "ProxyClient",
        ws: Any | None = None,
    ) -> None:
        """`ws` is a started `ProxyWebSocketClient`, needed only for a venue
        whose `order_transport` is `"ws"`. Typed `Any` so a REST-only install
        never imports the `websocket` extra."""
        self.venue = venue
        self._rest = rest
        self._ws = ws

    async def _call(self, action: str, payload: dict, idempotency_key: str = "") -> "ProxyResult":
        return await self._rest.acall(
            self.venue.name, action, payload, idempotency_key=idempotency_key
        )

    # -- orders -------------------------------------------------------------

    async def place_order(self, order: NormalizedOrder, *, idempotency_key: str) -> OrderAck:
        payload = self.venue.place_order(order)
        if self.venue.order_transport == "ws":
            # Lands with the first WS-order venue (Kraken Prime): needs its
            # gateway dialect and order-method name before it can be sent.
            raise NotImplementedError(
                f"{self.venue.name}: WebSocket order entry is not wired up yet"
            )
        raw = await self._call("place_order", payload, idempotency_key)
        return self.venue.parse_order_ack(raw)

    async def cancel_order(
        self, *, order_id: str, symbol: str, idempotency_key: str
    ) -> "ProxyResult":
        """Returns the venue's reply unparsed: venues disagree on what a
        cancel even returns (Kraken: a count; OKX: the order id), so there is
        no honest common shape yet."""
        if self.venue.order_transport == "ws":
            raise NotImplementedError(
                f"{self.venue.name}: WebSocket order entry is not wired up yet"
            )
        payload = self.venue.cancel_order(order_id=order_id, symbol=symbol)
        return await self._call("cancel_order", payload, idempotency_key)

    async def get_orders(self, order_ids: list[str]) -> list[Order]:
        raw = await self._call("get_order", self.venue.query_orders(order_ids))
        return self.venue.parse_orders(raw)

    async def get_open_orders(self) -> list[Order]:
        return self.venue.parse_orders(await self._call("get_pending_orders", {}))

    # -- fills and balances -------------------------------------------------

    async def get_fills(self, params: dict | None = None) -> list[Fill]:
        """`params` are venue-native filters (Kraken: `start`/`end`/`ofs`),
        passed through as-is until there is a second venue to unify with."""
        return self.venue.parse_fills(await self._call("get_fills", params or {}))

    async def get_balances(self) -> list[Balance]:
        return self.venue.parse_balances(await self._call("get_balance", {}))
