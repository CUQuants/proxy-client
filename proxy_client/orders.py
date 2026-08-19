"""Normalizes order payloads so callers don't hand-roll exchange wire formats.

v1 scope: **OKX only**, `place_order` / `cancel_order`. Kraken and the other
order actions (`amend_order`, the `_batch_orders` pair) are deliberately
deferred, not forgotten — add them as their own `OrderBuilder`
implementations / functions when a caller needs them, rather than guessing
their shape now.

Why a `Protocol` and not shared inheritance: there is nothing behavioural to
share between exchanges yet, only a field mapping each one owns completely.
A second exchange is a new class implementing `OrderBuilder`, not a change to
`OkxOrderBuilder`. Contrast this with `trading-gateway/proxy/exchanges/base.py`'s
`RouteTableAdapter`, which *is* an `ABC` — that class has real shared
machinery (the fail-closed route check) worth inheriting.

Why no async twin: building the payload dict is pure data transformation, no
I/O. `ProxyClient` already owns the real sync/async transport split
(`call()` / `acall()`); `okx_place_order` / `okx_aplace_order` below just
call the same synchronous builder before handing off to one or the other.

The Proxy still forwards whatever dict it receives byte-for-byte (see
`client.py`, `trading-gateway/proxy/pipeline.py`) — this module is the one
place a `price`/`size` name becomes OKX's `px`/`sz` before that happens.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypedDict

if TYPE_CHECKING:
    from .client import ProxyClient, ProxyResult

__all__ = [
    "NormalizedOrder",
    "OrderBuilder",
    "OkxOrderBuilder",
    "okx_place_order",
    "okx_aplace_order",
    "okx_cancel_order",
    "okx_acancel_order",
]


class _RequiredOrderFields(TypedDict):
    symbol: str              # OKX spelling for now, e.g. "BTC-USDT" (instId)
    side: str                # "buy" | "sell"
    order_type: str          # "limit" | "market"
    size: str


class NormalizedOrder(_RequiredOrderFields, total=False):
    """Venue-agnostic order intent. `price` is required in practice for
    `"limit"` orders and omitted for `"market"` ones — not enforced here,
    the venue will reject a malformed combination.

    `price` / `size` are strings, not floats: venues are picky about decimal
    formatting, and forcing the caller to pick the string representation
    avoids this layer silently reformatting a number wrong.
    """

    price: str
    client_order_id: str


class OrderBuilder(Protocol):
    """What each exchange's order payload builder implements."""

    def place_order(self, order: NormalizedOrder) -> dict: ...
    def cancel_order(self, *, order_id: str, symbol: str) -> dict: ...


class OkxOrderBuilder:
    """Builds payloads for OKX's `/api/v5/trade/order` and
    `/api/v5/trade/cancel-order` (`trading-gateway`'s `place_order` /
    `cancel_order` routes).

    Spot only: `tdMode` is hardcoded to `"cash"`. Margin/swap trading needs
    its own `tdMode` ("cross" / "isolated") decision and isn't handled here —
    a caller who needs it should get `ValueError`-loud silence, not a
    quietly-wrong spot order, which is why `NormalizedOrder` has no
    `margin_mode` field yet rather than one that's ignored.
    """

    def place_order(self, order: NormalizedOrder) -> dict:
        payload: dict = {
            "instId": order["symbol"],
            "tdMode": "cash",
            "side": order["side"],
            "ordType": order["order_type"],
            "sz": order["size"],
        }
        if "price" in order:
            payload["px"] = order["price"]
        if "client_order_id" in order:
            payload["clOrdId"] = order["client_order_id"]
        return payload

    def cancel_order(self, *, order_id: str, symbol: str) -> dict:
        return {"instId": symbol, "ordId": order_id}


_okx = OkxOrderBuilder()


def okx_place_order(
    client: "ProxyClient", order: NormalizedOrder, *, idempotency_key: str = ""
) -> "ProxyResult":
    return client.call(
        "okx", "place_order", _okx.place_order(order), idempotency_key=idempotency_key
    )


async def okx_aplace_order(
    client: "ProxyClient", order: NormalizedOrder, *, idempotency_key: str = ""
) -> "ProxyResult":
    return await client.acall(
        "okx", "place_order", _okx.place_order(order), idempotency_key=idempotency_key
    )


def okx_cancel_order(
    client: "ProxyClient", *, order_id: str, symbol: str, idempotency_key: str = ""
) -> "ProxyResult":
    return client.call(
        "okx",
        "cancel_order",
        _okx.cancel_order(order_id=order_id, symbol=symbol),
        idempotency_key=idempotency_key,
    )


async def okx_acancel_order(
    client: "ProxyClient", *, order_id: str, symbol: str, idempotency_key: str = ""
) -> "ProxyResult":
    return await client.acall(
        "okx",
        "cancel_order",
        _okx.cancel_order(order_id=order_id, symbol=symbol),
        idempotency_key=idempotency_key,
    )
