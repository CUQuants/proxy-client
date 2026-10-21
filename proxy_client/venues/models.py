"""Canonical, venue-agnostic shapes that adapters parse exchange responses into.

Three rules hold for every model here, and each exists because of a bug a
hand-rolled parser in a consumer has already shipped:

1. **`Decimal`, never `float`.** Venues send quantities as strings. Parsing
   them through `float` and back (`f"{float(a) - float(b):.10f}"`) is how a
   balance ends up off in the last place and an order gets rejected for
   precision.
2. **`raw` is always kept.** The Proxy deliberately passes venue bodies
   through unreshaped so a human can read what the exchange actually said.
   Normalizing must not throw that away — when a parse looks wrong, `raw` is
   the evidence.
3. **Unknown values raise, never default.** `UnmappedVenueValue` is raised for
   a status/side/type an adapter does not recognise. A fill silently read as
   `OPEN`, or a fee silently labelled `USD`, corrupts a position or a P&L
   with nothing to show it happened.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, TypedDict

__all__ = [
    "UnmappedVenueValue",
    "Side",
    "OrderType",
    "OrderStatus",
    "NormalizedOrder",
    "OrderAck",
    "Order",
    "Fill",
    "Balance",
]


class UnmappedVenueValue(ValueError):
    """A venue sent a value this adapter has no mapping for.

    Raised instead of guessing. Carries the venue, the field and the value so
    the fix (a new row in the adapter's table) is obvious from the traceback.
    """

    def __init__(self, venue: str, field_name: str, value: Any) -> None:
        super().__init__(f"{venue}: no mapping for {field_name}={value!r}")
        self.venue = venue
        self.field_name = field_name
        self.value = value


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


class OrderStatus(str, Enum):
    PENDING = "pending"                    # accepted, not yet working
    OPEN = "open"                          # working, nothing filled
    PARTIALLY_FILLED = "partially_filled"  # working, some filled
    FILLED = "filled"
    CANCELED = "canceled"                  # may still have `filled_size` > 0
    EXPIRED = "expired"


# -- outbound ---------------------------------------------------------------


class _RequiredOrderFields(TypedDict):
    symbol: str              # canonical "BASE-QUOTE", e.g. "BTC-USD"
    side: str                # "buy" | "sell"
    order_type: str          # "limit" | "market"
    size: str


class NormalizedOrder(_RequiredOrderFields, total=False):
    """Venue-agnostic order intent. Same shape as `proxy_client.orders`'s,
    except `symbol` is canonical here — the adapter translates it.

    `price` / `size` are strings, not floats: forcing the caller to choose
    the decimal representation stops this layer reformatting it wrong.
    """

    price: str
    client_order_id: str


# -- inbound ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OrderAck:
    """What a venue says when it accepts an order — usually just its id.

    Deliberately not an `Order`: most venues' place/cancel responses do not
    carry status or fills, and inventing them would be the defaulting rule 3
    forbids. Query the order if you need its state.
    """

    venue: str
    order_ids: tuple[str, ...]
    raw: Any = field(repr=False)


@dataclass(frozen=True, slots=True)
class Order:
    venue: str
    order_id: str
    symbol: str
    side: Side
    order_type: OrderType
    status: OrderStatus
    size: Decimal
    filled_size: Decimal
    price: Decimal | None            # limit price; None for market
    avg_fill_price: Decimal | None   # None until something fills
    created_at: datetime | None
    client_order_id: str | None
    raw: Any = field(repr=False)


@dataclass(frozen=True, slots=True)
class Fill:
    venue: str
    trade_id: str
    order_id: str
    symbol: str
    side: Side
    price: Decimal
    size: Decimal
    fee: Decimal
    fee_asset: str | None   # None when the venue does not say — never assumed
    is_maker: bool | None
    timestamp: datetime
    raw: Any = field(repr=False)


@dataclass(frozen=True, slots=True)
class Balance:
    venue: str
    asset: str
    total: Decimal
    held: Decimal           # reserved by open orders
    raw: Any = field(repr=False)

    @property
    def available(self) -> Decimal:
        return self.total - self.held
