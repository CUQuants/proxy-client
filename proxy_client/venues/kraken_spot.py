"""Kraken spot (REST v0) adapter.

Maps to and from the payloads `trading-gateway` forwards for exchange
`"kraken"` — its route table sends `place_order` to `/0/private/AddOrder`,
`get_order` to `QueryOrders`, `get_pending_orders` to `OpenOrders`,
`get_fills` to `TradesHistory`, `get_balance` to `BalanceEx` and
`get_instruments` to `AssetPairs`. The Proxy returns Kraken's body whole —
`{"error": [], "result": {...}}` — so every parser unwraps `result` first.

Symbols
-------
Kraken spells one market three ways: `XXBTZUSD` (the AssetPairs key, and what
TradesHistory reports), `XBTUSD` (the altname, what order `descr.pair`
reports) and `XBT/USD` (wsname). Outbound has a deterministic answer
(canonical → altname). Inbound does not — `XXBTZUSD` cannot be split without
knowing the asset codes — so it needs the AssetPairs table::

    pairs = await rest.acall("kraken", "get_instruments", {})
    adapter = KrakenSpot.from_asset_pairs(pairs)

Without that table an inbound pair raises `UnmappedVenueValue` rather than
being guessed at.

WebSocket v2
------------
The streaming half (`ws_*`, `parse_ws_message`) speaks Kraken WS v2, which the
Proxy relays unreshaped. WS v2 renamed XBT → BTC and **rejects** the REST
spelling (see `xlrts/src/exchange/symbols.py`), but in exchange its symbols
are already canonical apart from the separator — `BTC/USD` ↔ `BTC-USD` — so
the WS side needs no AssetPairs table and never touches `venue_symbol`.

WS v2 sends numbers as JSON numbers, which the transport has already parsed
to `float` by the time a message arrives here. `Decimal(str(x))` recovers
the shortest round-trip text, which matches the wire for the prices and
quantities Kraken sends; it is not a substitute for parsing with
`parse_float=Decimal`, and anything that must match Kraken's book checksum
digit-for-digit should not rely on it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from .base import StreamEvent
from .models import (
    Balance,
    BookLevel,
    BookUpdate,
    Fill,
    NormalizedOrder,
    Order,
    OrderAck,
    OrderEvent,
    OrderStatus,
    OrderType,
    Side,
    UnmappedVenueValue,
)

__all__ = ["KrakenSpot", "normalize_asset"]

VENUE = "kraken"

#: Kraken's legacy X/Z-prefixed asset codes and its renamed tickers. Assets
#: listed since ~2018 use their plain ticker (USDT, SOL, ...) and pass through.
_LEGACY_ASSETS: Mapping[str, str] = {
    "XXBT": "BTC", "XBT": "BTC",
    "XXDG": "DOGE", "XDG": "DOGE",
    "XETH": "ETH", "XETC": "ETC", "XLTC": "LTC", "XXRP": "XRP",
    "XXLM": "XLM", "XXMR": "XMR", "XZEC": "ZEC", "XMLN": "MLN", "XREP": "REP",
    "ZUSD": "USD", "ZEUR": "EUR", "ZGBP": "GBP", "ZCAD": "CAD",
    "ZJPY": "JPY", "ZAUD": "AUD",
}

#: Canonical → REST altname spelling, for outbound pairs.
_REST_ASSETS: Mapping[str, str] = {"BTC": "XBT", "DOGE": "XDG"}

_SIDES: Mapping[str, Side] = {"buy": Side.BUY, "sell": Side.SELL}
_ORDER_TYPES: Mapping[str, OrderType] = {
    "limit": OrderType.LIMIT,
    "market": OrderType.MARKET,
}
_STATUSES: Mapping[str, OrderStatus] = {
    "pending": OrderStatus.PENDING,
    "open": OrderStatus.OPEN,
    "closed": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELED,
    "expired": OrderStatus.EXPIRED,
}


#: WS v2 `order_status`. Unlike REST, v2 reports partial fills itself.
_WS_STATUSES: Mapping[str, OrderStatus] = {
    "pending_new": OrderStatus.PENDING,
    "new": OrderStatus.OPEN,
    "partially_filled": OrderStatus.PARTIALLY_FILLED,
    "filled": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELED,
    "expired": OrderStatus.EXPIRED,
}
_LIQUIDITY: Mapping[str, bool] = {"m": True, "t": False}


def normalize_asset(code: str) -> str:
    """`XXBT` → `BTC`, `ZUSD` → `USD`, `XBT.F` → `BTC.F`, `USDT` → `USDT`.

    A `.F`/`.S`/`.M`/`.B` suffix marks earn/staking balances, which are not
    spendable spot balances — the suffix is kept so they cannot be summed in
    with the spot asset by accident.
    """
    head, dot, suffix = code.partition(".")
    return _LEGACY_ASSETS.get(head, head) + dot + suffix


def _result(raw: Any) -> Any:
    if isinstance(raw, Mapping) and "result" in raw:
        return raw["result"]
    return raw


def _lookup(table: Mapping[str, Any], field_name: str, value: Any) -> Any:
    try:
        return table[value]
    except (KeyError, TypeError):
        raise UnmappedVenueValue(VENUE, field_name, value) from None


def _ts(seconds: Any) -> datetime:
    return datetime.fromtimestamp(float(seconds), tz=timezone.utc)


def _iso(value: Any) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _dec(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _ws_canonical(symbol: str) -> str:
    return symbol.replace("/", "-")


class KrakenSpot:
    name = VENUE
    order_transport = "rest"

    def __init__(
        self,
        pairs: Mapping[str, str] | None = None,
        altnames: Mapping[str, str] | None = None,
    ) -> None:
        """`pairs` maps every Kraken spelling of a pair to its canonical
        symbol; `altnames` maps canonical → the altname to send. Build both
        with `from_asset_pairs` rather than by hand."""
        self._to_canonical: dict[str, str] = dict(pairs or {})
        self._to_altname: dict[str, str] = dict(altnames or {})

    @classmethod
    def from_asset_pairs(cls, raw: Any) -> "KrakenSpot":
        """Build from a `get_instruments` (AssetPairs) response."""
        pairs: dict[str, str] = {}
        altnames: dict[str, str] = {}
        for key, info in _result(raw).items():
            canonical = f"{normalize_asset(info['base'])}-{normalize_asset(info['quote'])}"
            for spelling in (key, info.get("altname"), info.get("wsname")):
                if spelling:
                    pairs[spelling] = canonical
            if info.get("altname"):
                altnames[canonical] = info["altname"]
        return cls(pairs, altnames)

    def canonical_symbol(self, venue_pair: str) -> str:
        """Kraken pair (any spelling) → canonical `BASE-QUOTE`."""
        return _lookup(self._to_canonical, "pair", venue_pair)

    # -- outbound ---------------------------------------------------------

    def venue_symbol(self, symbol: str) -> str:
        if symbol in self._to_altname:
            return self._to_altname[symbol]
        base, sep, quote = symbol.upper().partition("-")
        if not sep or not base or not quote:
            raise ValueError(f"canonical symbol must be BASE-QUOTE, got {symbol!r}")
        return _REST_ASSETS.get(base, base) + _REST_ASSETS.get(quote, quote)

    def place_order(self, order: NormalizedOrder) -> dict:
        payload: dict = {
            "pair": self.venue_symbol(order["symbol"]),
            "type": Side(order["side"]).value,
            "ordertype": OrderType(order["order_type"]).value,
            "volume": order["size"],
        }
        if "price" in order:
            payload["price"] = order["price"]
        if "client_order_id" in order:
            payload["cl_ord_id"] = order["client_order_id"]
        return payload

    def cancel_order(self, *, order_id: str, symbol: str) -> dict:
        # Keyed by txid alone: the Proxy's Kraken route for cancel_order is
        # `symbol_policy="absent"`, so `symbol` is accepted for the protocol
        # and not sent.
        return {"txid": order_id}

    def query_orders(self, order_ids: list[str]) -> dict:
        return {"txid": ",".join(order_ids)}

    # -- inbound ----------------------------------------------------------

    def parse_order_ack(self, raw: Any) -> OrderAck:
        """AddOrder: `{"descr": {...}, "txid": ["O..."]}`."""
        txids = _result(raw).get("txid") or []
        return OrderAck(venue=VENUE, order_ids=tuple(txids), raw=raw)

    def parse_orders(self, raw: Any) -> list[Order]:
        """QueryOrders (`{txid: order}`), OpenOrders (`{"open": {...}}`) or
        ClosedOrders (`{"closed": {...}, "count": n}`)."""
        result = _result(raw)
        for wrapper in ("open", "closed"):
            if isinstance(result.get(wrapper), Mapping):
                result = result[wrapper]
                break
        return [self._parse_order(txid, o) for txid, o in result.items()]

    def _parse_order(self, txid: str, o: Mapping[str, Any]) -> Order:
        descr = o.get("descr", {})
        size = Decimal(o["vol"])
        filled = Decimal(o.get("vol_exec", "0"))
        status = _lookup(_STATUSES, "status", o.get("status"))
        if status is OrderStatus.OPEN and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        order_type = _lookup(_ORDER_TYPES, "ordertype", descr.get("ordertype"))
        return Order(
            venue=VENUE,
            order_id=txid,
            symbol=self.canonical_symbol(descr["pair"]),
            side=_lookup(_SIDES, "type", descr.get("type")),
            order_type=order_type,
            status=status,
            size=size,
            filled_size=filled,
            price=Decimal(descr["price"]) if order_type is OrderType.LIMIT else None,
            # Kraken reports "0.00000" average price for an unfilled order.
            avg_fill_price=Decimal(o["price"]) if filled > 0 else None,
            created_at=_ts(o["opentm"]) if "opentm" in o else None,
            client_order_id=o.get("cl_ord_id"),
            raw=o,
        )

    def parse_fills(self, raw: Any) -> list[Fill]:
        """TradesHistory (`{"trades": {...}, "count": n}`) or QueryTrades
        (`{trade_txid: trade}`)."""
        result = _result(raw)
        trades = result["trades"] if isinstance(result.get("trades"), Mapping) else result
        return [self._parse_fill(tid, t) for tid, t in trades.items()]

    def _parse_fill(self, trade_id: str, t: Mapping[str, Any]) -> Fill:
        symbol = self.canonical_symbol(t["pair"])
        return Fill(
            venue=VENUE,
            trade_id=trade_id,
            order_id=t["ordertxid"],
            symbol=symbol,
            side=_lookup(_SIDES, "type", t.get("type")),
            price=Decimal(t["price"]),
            size=Decimal(t["vol"]),
            fee=Decimal(t["fee"]),
            # Kraken documents spot `fee` as "total fee (quote currency)".
            fee_asset=symbol.partition("-")[2],
            is_maker=t.get("maker"),
            timestamp=_ts(t["time"]),
            raw=t,
        )

    def parse_balances(self, raw: Any) -> list[Balance]:
        """BalanceEx: `{asset: {"balance": "...", "hold_trade": "..."}}`."""
        out = []
        for code, entry in _result(raw).items():
            if not isinstance(entry, Mapping):
                # Plain `Balance` returns a bare string with no hold figure;
                # reporting held=0 for it would be a guess.
                raise UnmappedVenueValue(VENUE, "balance entry", entry)
            out.append(
                Balance(
                    venue=VENUE,
                    asset=normalize_asset(code),
                    total=Decimal(entry["balance"]),
                    held=Decimal(entry.get("hold_trade", "0")),
                    raw=entry,
                )
            )
        return out

    # -- WebSocket v2 -----------------------------------------------------

    def ws_executions_subscription(self) -> tuple[str, dict]:
        # Kraken's defaults: snapshot of open orders, no trade snapshot.
        return "executions", {}

    def ws_book_subscription(self, symbols: list[str], depth: int) -> tuple[str, dict]:
        return "book", {"symbol": [s.upper().replace("-", "/") for s in symbols], "depth": depth}

    def parse_ws_message(self, raw: dict) -> list[StreamEvent]:
        """`executions` → `OrderEvent`s and `Fill`s; `book` → `BookUpdate`s.

        Other channels (heartbeat, status, ...) yield nothing. Anything the
        caller subscribed to through `TradingStream` is one of the two.
        """
        channel = raw.get("channel")
        snapshot = raw.get("type") == "snapshot"
        rows = raw.get("data") or []
        if channel == "executions":
            out: list[StreamEvent] = []
            for row in rows:
                if row.get("exec_type") == "trade":
                    out.append(self._parse_ws_fill(row))
                out.append(self._parse_ws_order_event(row, snapshot))
            return out
        if channel == "book":
            return [self._parse_ws_book(row, snapshot) for row in rows]
        return []

    def _parse_ws_order_event(self, row: Mapping[str, Any], snapshot: bool) -> OrderEvent:
        status = row.get("order_status")
        side = row.get("side")
        return OrderEvent(
            venue=VENUE,
            order_id=row["order_id"],
            status=None if status is None else _lookup(_WS_STATUSES, "order_status", status),
            symbol=_ws_canonical(row["symbol"]) if "symbol" in row else None,
            side=None if side is None else _lookup(_SIDES, "side", side),
            size=_dec(row.get("order_qty")),
            filled_size=_dec(row.get("cum_qty")),
            avg_fill_price=_dec(row.get("avg_price")),
            client_order_id=row.get("cl_ord_id"),
            timestamp=_iso(row.get("timestamp")),
            is_snapshot=snapshot,
            raw=row,
        )

    def _parse_ws_fill(self, row: Mapping[str, Any]) -> Fill:
        # Unlike REST, v2 names the fee asset. One entry is the only shape
        # with an honest single `fee`; anything else is a new case to model.
        fees = row.get("fees") or []
        if len(fees) != 1:
            raise UnmappedVenueValue(VENUE, "fees", fees)
        liquidity = row.get("liquidity_ind")
        return Fill(
            venue=VENUE,
            trade_id=str(row["exec_id"]),
            order_id=row["order_id"],
            symbol=_ws_canonical(row["symbol"]),
            side=_lookup(_SIDES, "side", row.get("side")),
            price=Decimal(str(row["last_price"])),
            size=Decimal(str(row["last_qty"])),
            fee=Decimal(str(fees[0]["qty"])),
            fee_asset=fees[0]["asset"],
            is_maker=None if liquidity is None else _lookup(_LIQUIDITY, "liquidity_ind", liquidity),
            timestamp=datetime.fromisoformat(row["timestamp"]),
            raw=row,
        )

    def _parse_ws_book(self, row: Mapping[str, Any], snapshot: bool) -> BookUpdate:
        def levels(side: str) -> tuple[BookLevel, ...]:
            return tuple(
                BookLevel(Decimal(str(lv["price"])), Decimal(str(lv["qty"])))
                for lv in row.get(side) or []
            )

        return BookUpdate(
            venue=VENUE,
            symbol=_ws_canonical(row["symbol"]),
            is_snapshot=snapshot,
            bids=levels("bids"),
            asks=levels("asks"),
            checksum=row.get("checksum"),
            timestamp=_iso(row.get("timestamp")),
            raw=row,
        )
