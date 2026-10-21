"""Venue-normalization layer: canonical models, per-venue adapters, and a
`TradingClient` facade that runs them over the existing transports.

Optional and additive. `ProxyClient.call()` still returns the venue's raw
body; nothing here changes the request path or the error contract. A
consumer can move over one call site at a time.

    adapter = KrakenSpot.from_asset_pairs(
        await rest.acall("kraken", "get_instruments", {})
    )
    tc = TradingClient(adapter, rest)
    fills = await tc.get_fills()          # list[Fill], Decimals, UTC datetimes

Layout:
    models.py        canonical dataclasses and enums
    base.py          the `VenueAdapter` protocol
    kraken_spot.py   Kraken spot REST — outbound and inbound
    okx.py           OKX — outbound only (inbound TODO)
    kraken_prime.py  Kraken Prime — placeholder; orders travel over WS
    trading.py       `TradingClient`
"""

from .base import OrderTransport, VenueAdapter
from .kraken_prime import KrakenPrime
from .kraken_spot import KrakenSpot, normalize_asset
from .models import (
    Balance,
    Fill,
    NormalizedOrder,
    Order,
    OrderAck,
    OrderStatus,
    OrderType,
    Side,
    UnmappedVenueValue,
)
from .okx import Okx
from .trading import TradingClient

__all__ = [
    "VenueAdapter",
    "OrderTransport",
    "TradingClient",
    "KrakenSpot",
    "KrakenPrime",
    "Okx",
    "normalize_asset",
    "NormalizedOrder",
    "OrderAck",
    "Order",
    "Fill",
    "Balance",
    "Side",
    "OrderType",
    "OrderStatus",
    "UnmappedVenueValue",
]
