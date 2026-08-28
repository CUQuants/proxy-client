"""Request/response correlation for :class:`ProxyWebSocketClient`.

`request()` allocates a `req_id`, registers a future, sends the frame, and
awaits. Every inbound frame is offered to :meth:`_PendingRequests.resolve`
first; if it carries a correlation id (`id` for order frames, `req_id`
for everything else) or is a `pong`, it settles the matching future and
is considered consumed. Anything `resolve` doesn't consume is a
server-initiated push and falls through to the channel handlers.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

__all__ = ["_PendingRequests"]

logger = logging.getLogger(__name__)

# Reserved key for the single in-flight application-level ping. A pong
# carries no correlation id of its own, so it gets a fixed slot rather
# than a numeric req_id.
_PONG_KEY = "__pong__"


class _PendingRequests:
    """The pending-future map plus the req_id counter.

    Lives across reconnects (it's the client's, not the connection's), but
    :meth:`fail_all` is called on every drop: an in-flight request can't
    be answered once the socket — and with it the server-side req_id — is
    gone, so its caller is failed immediately rather than left to time
    out.
    """

    def __init__(self) -> None:
        self._counter = 0
        self._futures: dict[Any, asyncio.Future] = {}

    def next_id(self) -> int:
        self._counter += 1
        return self._counter

    def create(self, key: Any) -> asyncio.Future:
        """Register and return a future for `key` (a req_id). Always
        paired with an ``await`` on the returned future and a
        :meth:`discard` in a ``finally``."""
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._futures[key] = fut
        return fut

    def discard(self, key: Any) -> None:
        self._futures.pop(key, None)

    def expect_pong(self) -> asyncio.Future:
        return self.create(_PONG_KEY)

    def discard_pong(self) -> None:
        self.discard(_PONG_KEY)

    def resolve(self, msg: dict) -> bool:
        """Try to route `msg` to a waiting caller.

        Returns ``True`` if `msg` was a correlated reply (or a pong, or a
        Proxy error with no id that no caller could ever match) and should
        not be delivered to channel handlers; ``False`` if it's a
        server-initiated push.
        """
        if msg.get("event") == "pong":
            return self._settle(_PONG_KEY, msg)

        # Order frames are dual-tagged: `id` is the Proxy's own allocation,
        # `req_id` is Kraken's echo. Prefer `id`.
        ident = msg.get("id")
        if ident is not None and ident in self._futures:
            return self._settle(ident, msg)

        req_id = msg.get("req_id")
        if req_id is not None and req_id in self._futures:
            return self._settle(req_id, msg)

        # A Proxy error with no correlation id at all (e.g. a
        # malformed-frame rejection) can't reach a specific caller. Log it
        # and swallow it — it is not a channel push.
        if msg.get("event") == "error" and ident is None and req_id is None:
            logger.error("Proxy error frame with no correlation id: %s", msg)
            return True

        return False

    def fail_all(self, exc: BaseException) -> None:
        for fut in list(self._futures.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._futures.clear()

    def cancel_all(self) -> None:
        for fut in list(self._futures.values()):
            if not fut.done():
                fut.cancel()
        self._futures.clear()

    def _settle(self, key: Any, msg: dict) -> bool:
        fut = self._futures.get(key)
        if fut is None:
            return False
        if not fut.done():
            fut.set_result(msg)
        return True
