# cuq-proxy-client

A Python SDK for the CU Quants Exchange Access Proxy. It handles request
signing, idempotency-key bookkeeping, and typed error classification, so a
caller gets `client.call(exchange, action, payload)` instead of hand-rolling
an HMAC-signed HTTP client.

See [`SDK_WRITEUP.md`](../SDK_WRITEUP.md) for the full problem statement.
Short version: every one of these is a footgun a hand-rolled client has to
get right on its own, and this package is the one place that gets them right
once —

- **Signing.** The Proxy verifies an HMAC over the exact raw bytes it
  received. `requests.post(url, json=payload)` or `httpx`'s `data=` would
  re-serialize the body and silently break the signature — a bare,
  uninformative `401`.
- **Idempotency keys.** Generated once per order intent, reused (not
  regenerated) on retry — while the timestamp and nonce *do* need to be
  regenerated on that same retry.
- **Error classification.** `RATE_LIMITED` means back off and retry with the
  same key. `IDEMPOTENCY_KEY_REUSED` means stop, that's a bug. `EXCHANGE_ERROR`
  may or may not be safe to retry depending on a `detail.outcome_unknown`
  flag. Getting this table wrong either duplicates a live order or gives up
  on a request that should have been retried.

## Scope

- **REST and WebSocket (Kraken).** `ProxyClient` for `/v1/{exchange}` REST
  calls, `ProxyWebSocketClient` for the `/v1/{exchange}/ws` route — see
  [WebSocket](#websocket) below. WS is Kraken-only for now; OKX rides a
  different Proxy dialect that no consumer currently reaches through this
  SDK.
- **Both operator and system credentials**, on both transports.
  `.for_operator(...)` for a credential tied to a person, `.for_system(...)`
  for a bot/unattended consumer (like `speedbyte`) carrying `system_name`.
  The two constructors exist because the Proxy enforces strict, opposite
  rules for the two credential types (an operator credential must *not*
  send `system_name`; a system credential *must*) — see
  [System (bot) credentials](#system-bot-credentials).
- **No automatic retries on REST.** `call()`/`acall()` raise a typed
  exception and leave the retry decision to the caller — see
  [Errors and retries](#errors-and-retries). WebSocket reconnection is
  different: a dropped connection isn't a business decision the way
  resending a mutating request is, so `ProxyWebSocketClient` *does* own
  reconnection — its aggressiveness is a parameter, not a caller-built loop.

## Install

Not yet published to a package index. Build and install the wheel directly:

```bash
cd proxy-client
make build
pip install dist/cuq_proxy_client-0.2.0-py3-none-any.whl
# add [websocket] if you need ProxyWebSocketClient:
pip install "dist/cuq_proxy_client-0.2.0-py3-none-any.whl[websocket]"
```

or, for local development against a consumer (editable install so changes
here show up immediately):

```bash
pip install -e /path/to/proxy-client
```

## Quickstart

```python
from proxy_client import ProxyClient, RateLimitedError, IdempotencyKeyReusedError

client = ProxyClient.for_operator(
    base_url="https://your-proxy-host.example",
    api_key="cuq_op_...",
    secret="...",           # never store this; read it at process startup only
    operator_id="cuq-014",
    operator_name="J. Rivera",
)
# ProxyClient(...) directly is identical — for_operator() just reads
# symmetrically next to for_system(), below.

try:
    result = client.call(
        exchange="okx",
        action="get_balance",
        payload={"ccy": "USDT"},
    )
except RateLimitedError as e:
    ...
```

`result` is a `ProxyResult` — a `dict` subclass holding exactly the `data`
field of the Proxy's response envelope, the venue's reply unreshaped, so it
behaves like a plain dict everywhere you'd use one. It carries one extra
attribute, `result.idempotent_replay`: `True` when this response wasn't a
fresh execution but the cached result of an identical earlier request (same
idempotency key, same body). Most callers can ignore it; it matters if you
want to distinguish "my retry replayed the original order" from "this placed
a new one" for logging or alerting. Signing, timestamp, and nonce are
handled internally on every call.

### Async

```python
result = await client.acall(exchange="okx", action="get_balance", payload={"ccy": "USDT"})
```

`call()` and `acall()` are both real transport calls (`httpx.Client` and
`httpx.AsyncClient` respectively) — `acall()` is not `call()` run in a
thread, so it's safe to use from a busy event loop without stalling it.

Close the client's connection pools when you're done with it (a client used
for the lifetime of a process doesn't need to bother):

```python
client.close()               # or: with ProxyClient(...) as client: ...
await client.aclose()        # or: async with ProxyClient(...) as client: ...
```

### Placing an order (idempotency keys)

Any mutating action needs an idempotency key — generated **once per order
intent**, and reused unchanged across every retry of that same intent:

```python
from proxy_client import new_idempotency_key, RateLimitedError, IdempotencyInFlightError
import time

key = new_idempotency_key()  # once, when you decide to place this order

for attempt in range(3):
    try:
        result = client.call(
            exchange="okx",
            action="place_order",
            payload={"instId": "BTC-USDT", "tdMode": "cash", "side": "buy",
                     "ordType": "market", "sz": "10"},
            idempotency_key=key,  # same key every attempt
        )
        break
    except (RateLimitedError, IdempotencyInFlightError) as e:
        time.sleep(getattr(e, "retry_after", None) or 1.0)
```

Generating a new key on retry defeats the protection: the whole point is
that resending the *same* key after a dropped connection cannot place a
second order.

### System (bot) credentials

An unattended consumer (a bot, a market-making engine) authenticates with a
system credential rather than a person's — `ProxyClient.for_system(...)`
instead of `.for_operator(...)`:

```python
client = ProxyClient.for_system(
    base_url="https://your-proxy-host.example",
    api_key="cuq_sys_...",
    secret="...",
    operator_id="cuq-014",           # still required: the human responsible for this run
    operator_name="J. Rivera",
    system_name="speedbyte",
)
```

`operator_id`/`operator_name` are required either way — the Proxy always
wants a responsible human on record, bot or not — but a system credential
isn't tied to *that* operator specifically the way an operator credential is
to its own owner. `call()`/`acall()` work identically after construction;
the only difference is what's in the signed envelope. There's no
`system_name` kwarg on `ProxyClient(...)` itself or on `.for_operator(...)`,
so there's no way to end up with an operator-looking client that
accidentally carries one, or a system client that's missing it — both are
the same uninformative `AUTH_FAILED` from the server if you get them
backwards.

### Normalized orders (OKX only, for now)

`call()`'s `payload` is forwarded to the exchange byte-for-byte — you're
writing that venue's own field names (`px`, `sz`, `instId`, ...). For OKX,
`proxy_client.orders` gives you a venue-agnostic alternative:

```python
from proxy_client import okx_place_order, okx_cancel_order, new_idempotency_key

result = okx_place_order(
    client,
    {"symbol": "BTC-USDT", "side": "buy", "order_type": "limit",
     "price": "50000", "size": "0.01"},
    idempotency_key=new_idempotency_key(),
)

okx_cancel_order(client, order_id=result["ordId"], symbol="BTC-USDT")
```

Async equivalents are `okx_aplace_order` / `okx_acancel_order`, both real
`await`s on `client.acall()` — the normalization itself is pure and needs no
async form. Other exchanges and order actions (`amend_order`, batch orders)
aren't covered yet; see `orders.py`'s module docstring before adding one.

## WebSocket

`ProxyWebSocketClient` reaches trading-gateway's `/v1/{exchange}/ws` route
(Kraken only for now). Requires the `websocket` extra, since it's the only
part of this package that needs the `websockets` library:

```bash
pip install "cuq-proxy-client[websocket]"
```

```python
from proxy_client.websocket import ProxyWebSocketClient

client = ProxyWebSocketClient.for_operator(
    base_url="https://your-proxy-host.example",
    api_key="cuq_op_...",
    secret="...",
    operator_id="cuq-014",
    operator_name="J. Rivera",
)

async def on_book(msg: dict) -> None:
    for level in msg.get("data", []):
        ...

client.add_handler("book", on_book)   # or subclass and override on_message()

await client.start()
await client.subscribe("book", {"symbol": ["BTC/USD"], "depth": 10})
await client.run_forever()            # blocks until stop() or the connection gives up for good
```

Kraken's own `method`/`params`/`req_id` vocabulary travels verbatim — you're
writing the same subscribe/order payloads you would against Kraken WS v2
directly. Two things are handled for you: the signed handshake (identical
mechanism to REST's `compute_signature`, just over `WS`), and everything
below.

### Reconnect and heartbeat are parameters, not something you build

A dropped connection isn't a business decision — nobody wants a market-data
stream to just go quiet, the way `ProxyClient.call()`'s "no auto-retry"
stance correctly leaves a *mutating* retry to you. So `ProxyWebSocketClient`
owns the reconnect loop and the heartbeat loop; you control their timing:

```python
client = ProxyWebSocketClient.for_operator(
    ..., 
    reconnect=None,              # disable: a dropped connection just ends
    heartbeat_interval=None,     # disable: no application-level ping loop
)
```

`reconnect` defaults to `default_backoff()` — exponential, capped at 30s,
forgiving of a connection that was healthy for a while before dropping
(mirrors `trading-gateway`'s own upstream reconnect policy). Swap in your
own:

```python
def my_backoff(attempt: int, connected_duration: float, last_error: Exception | None) -> float | None:
    if attempt > 10:
        return None          # stop reconnecting for good
    return min(1.0 * attempt, 15.0)

client = ProxyWebSocketClient.for_operator(..., reconnect=my_backoff)
```

Active subscriptions are replayed automatically after every reconnect.
`heartbeat_interval`/`heartbeat_timeout` (default 30s/10s) are plain numbers
— pure keep-alive wire mechanics, nothing to swap in besides the timing.

### Handlers are the app-level seam

Routing an inbound frame to whichever handler is registered for its channel
is the SDK's job (`add_handler(channel, callback)`, or subclass and
override `on_message()`); what the callback does with that message is
yours. If a handler raises, the read loop doesn't die — the exception goes
to `on_error` instead:

```python
def log_bad_handler(exc: Exception) -> None:
    logging.exception("WS handler failed", exc_info=exc)

client = ProxyWebSocketClient.for_operator(..., on_error=log_bad_handler)
```

Default (`on_error=None`) is log-and-continue.

### Orders over WebSocket

Kraken executes orders on the same socket (its WS and REST order
vocabularies differ — `order_qty`/`symbol` vs `volume`/`pair` — so an order
placed over WS isn't relayed through REST). `request()` handles req_id/`id`
correlation and idempotency-key injection for you:

```python
response = await client.request(
    "add_order",
    params={"symbol": "BTC/USD", "side": "buy", "order_type": "market", "order_qty": "0.01"},
    # idempotency_key="...": omit it and one is generated for you; if you
    # pass one, reuse it unchanged across retries of this same order.
)
```

Raises a typed `ProxyError` subclass (same hierarchy as REST, via
`errors.from_response`) if the Proxy refused the frame, or
`WebSocketRequestError` if Kraken itself rejected it (`success: false`) —
kept as a separate exception type since that's Kraken's own vocabulary, not
one of the Proxy's §9 codes.

## Errors and retries

Every exception is a `ProxyError` subclass, importable from `proxy_client`.
Each carries `.code`, `.message`, `.detail`, `.request_id`, and a class-level
`.retryable` (`True`, `False`, or `None` — "depends, inspect the exception").

| Exception                     | Code                     | Retryable (same key)                          |
|--------------------------------|---------------------------|------------------------------------------------|
| `AuthFailedError`              | `AUTH_FAILED`             | No — bad secret, unknown key, or clock skew    |
| `CredentialExpiredError`       | `CREDENTIAL_EXPIRED`      | No                                              |
| `ActionNotPermittedError`      | `ACTION_NOT_PERMITTED`    | No                                              |
| `MalformedRequestError`        | `MALFORMED_REQUEST`       | No — an SDK bug if you see this                |
| `IdempotencyKeyRequiredError`  | `IDEMPOTENCY_KEY_REQUIRED`| No — a caller bug (forgot the key)             |
| `IdempotencyKeyReusedError`    | `IDEMPOTENCY_KEY_REUSED`  | **Never** — same key, different body: a real bug|
| `IdempotencyInFlightError`     | `IDEMPOTENCY_IN_FLIGHT`   | Yes, after a short backoff                     |
| `LogUnavailableError`          | `LOG_UNAVAILABLE`         | Yes — nothing was forwarded yet                |
| `RateLimitedError`             | `RATE_LIMITED`            | Yes, after `.retry_after` seconds              |
| `ExchangeError`                | `EXCHANGE_ERROR`          | Depends — see `.outcome_unknown` below         |
| `ProxyUnreachableError`        | *(transport failure)*     | Depends — same as `outcome_unknown`            |

`ExchangeError.outcome_unknown` is the one case this table can't answer
statically:

```python
except ExchangeError as e:
    if e.outcome_unknown:
        # Forwarded to the exchange, but the outcome was never learned
        # (timeout, connection drop mid-flight). The Proxy leaves the
        # idempotency claim in-flight rather than release it, specifically
        # so a naive retry can't duplicate the order — a retry with the
        # same key gets IdempotencyInFlightError, not a second placement.
        # This is a "get a human to look" case, not a tight retry loop.
        alert_a_human(e)
    else:
        # The exchange answered definitively (e.g. rejected the order).
        # Retrying with the same key just replays that same answer.
        raise
```

An unrecognised code (e.g. a new one the server starts sending before this
SDK knows about it) falls back to the base `ProxyError` with
`retryable = False` — fails closed rather than guessing.

## Development

```bash
make install   # create .venv, install with dev dependencies
make test      # run the test suite
make build     # build the sdist and wheel into dist/
make clean     # remove .venv, build artifacts, and caches
```

`tests/test_websocket.py` covers `ProxyWebSocketClient` against a small
in-process fake Proxy server (`websockets.serve`), not a real
`trading-gateway` checkout — same filesystem-independence rule as the rest
of this suite.

`tests/test_signing.py` pins golden values computed directly against
`trading-gateway/proxy/signing.py`, and `tests/test_error_parity.py` pins a
snapshot of `trading-gateway/proxy/errors.py`'s code table — both are meant
to catch the server's protocol drifting out from under this SDK. Both are
plain pinned constants, not a live import: this package's test suite doesn't
depend on `trading-gateway` being checked out anywhere. If the server's
signing logic or error codes change, update the pinned values by hand — see
each file's module docstring for exactly what to re-run.

## Roadmap

- `xlrts` and `speedbyte` each still hand-roll their own `ws_url()` /
  `ws_auth_frame()` in a local `ProxySession`-style class rather than using
  `ProxyWebSocketClient` — migrating them is separate follow-up work, not
  done as part of adding this class.
- OKX over WebSocket: no consumer currently reaches OKX through the Proxy's
  WS route, so `ProxyWebSocketClient` stays Kraken-only until one does.

This package shipped the REST surface first
(`xlrts/src/proxy_session.py` was the original hand-rolled implementation
this SDK generalizes, and now delegates to it) — WebSocket followed once a
second consumer (`speedbyte`) had independently hand-rolled the same
handshake, making the duplication worth extracting.
