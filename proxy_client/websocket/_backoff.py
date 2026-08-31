"""Reconnect timing policy for :class:`ProxyWebSocketClient`.

A ``ReconnectBackoff`` is just ``(attempt: int) -> float | None``: given
the attempt number (1 for the first reconnect after a drop, 2 for the
next, ...), return how many seconds to wait, or ``None`` to stop
reconnecting for good.

It is deliberately *stateless*. "Forgive a connection that stayed healthy
for a while" is not the policy's job — the supervisor in ``client.py``
resets ``attempt`` back to 0 once a connection has been up past its
reset threshold, so a long-lived connection that finally drops starts
its next backoff from the bottom.
"""

from __future__ import annotations

from typing import Callable

__all__ = ["ReconnectBackoff", "default_backoff"]

# (attempt number, 1-based) -> seconds to wait before reconnecting, or
# None to stop reconnecting for good.
ReconnectBackoff = Callable[[int], "float | None"]

_INITIAL = 0.5
_FACTOR = 2.0
_MAX_DELAY = 30.0


def default_backoff(attempt: int) -> float | None:
    """Exponential backoff: 0.5s, 1s, 2s, 4s, ... capped at 30s.

    Never gives up (never returns ``None``). Mirrors `trading-gateway`'s
    own upstream reconnect policy (`BaseUpstream._supervise`: "backoff
    0.5s -> x2 -> capped 30s, reset only if the connection stayed up long
    enough") so a caller who doesn't override anything gets behavior
    consistent with what the server side already does to its own
    upstreams. The "reset if healthy" half of that lives in the
    supervisor (see ``client._RECONNECT_RESET_AFTER``).
    """
    return min(_INITIAL * (_FACTOR ** max(attempt - 1, 0)), _MAX_DELAY)
