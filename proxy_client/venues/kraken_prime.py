"""Kraken Prime (institutional) adapter — placeholder.

Exists to pin down the one structural fact already known: **Prime has no REST
order entry.** Orders go over Prime WebSocket (or FIX); Prime REST is API-user
and key management plus settlement. Hence `order_transport = "ws"`, which
routes `TradingClient.place_order` to the socket.

Everything else waits on two things, deliberately not guessed at here:

1. **A sandbox session** (`wss://wss.sandbox.prime.kraken.com/ws/v1`) to
   capture real message shapes and symbol spelling as pinned fixtures. The
   public docs do not show them, and disagree with themselves on standard vs
   URL-safe base64 for the connect signature.
2. **Gateway support** — a Prime upstream and dialect in `trading-gateway`
   (`proxy/ws/`), and a matching `Dialect` in `proxy_client/websocket/`.
   Until the Proxy has an exchange key for Prime, `name` below is a
   placeholder the server will reject.
"""

from __future__ import annotations

from typing import Any

from .models import Balance, Fill, NormalizedOrder, Order, OrderAck

__all__ = ["KrakenPrime"]

_TODO = "Kraken Prime is not wired up yet — see proxy_client/venues/kraken_prime.py"


class KrakenPrime:
    name = "kraken_prime"
    order_transport = "ws"

    def venue_symbol(self, symbol: str) -> str:
        raise NotImplementedError(_TODO)

    def place_order(self, order: NormalizedOrder) -> dict:
        raise NotImplementedError(_TODO)

    def cancel_order(self, *, order_id: str, symbol: str) -> dict:
        raise NotImplementedError(_TODO)

    def query_orders(self, order_ids: list[str]) -> dict:
        raise NotImplementedError(_TODO)

    def parse_order_ack(self, raw: Any) -> OrderAck:
        raise NotImplementedError(_TODO)

    def parse_orders(self, raw: Any) -> list[Order]:
        raise NotImplementedError(_TODO)

    def parse_fills(self, raw: Any) -> list[Fill]:
        raise NotImplementedError(_TODO)

    def parse_balances(self, raw: Any) -> list[Balance]:
        raise NotImplementedError(_TODO)
