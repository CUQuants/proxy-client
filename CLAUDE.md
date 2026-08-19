# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`cuq-proxy-client` (import name `proxy_client`) is the SDK for the CU Quants
Exchange Access Proxy (`trading-gateway`, a sibling repo — private, at
`../trading-gateway` in local checkouts but not a dependency of this
package). It exists so `xlrts`, `speedbyte`, and `trading-terminal` don't
each hand-roll HMAC signing, idempotency-key bookkeeping, and error
classification against the Proxy's raw wire protocol. See `README.md` for
the full usage-facing documentation (quickstart, error/retry table,
idempotency pattern) — this file is architecture and workflow, not usage.

## Commands

```bash
make install   # create .venv (python3.11) and install with dev dependencies
make test      # run the full test suite
make build     # build sdist + wheel into dist/
make clean     # remove .venv, dist/, build/, caches
```

Requires Python **3.11+** specifically (`pyproject.toml`'s
`requires-python`). The system `python3` may resolve to an older version
(e.g. 3.10) — if `make install` fails on the version check, confirm
`python3.11` exists (`which python3.11`) before debugging further; the
`Makefile`'s `PYTHON := python3.11` assumes it's on `PATH`.

Run a single test:

```bash
.venv/bin/python -m pytest tests/test_client.py::test_call_signs_the_exact_bytes_it_sends -v
```

CI (`.github/workflows/ci.yml`) runs `pip install -e ".[dev]"` +
`pytest` on push to `main` and on every PR targeting `main`. It only checks
out this repo — the test suite has **zero dependency on `trading-gateway`
being present on disk** (see Testing philosophy below), so there's nothing
else to wire up.

## Architecture

Request flow through the modules, for one `client.call(exchange, action, payload)`:

1. **`envelope.py`** (`build_body`) serializes the request body once, as raw
   bytes — `{exchange, action, operator_id, operator_name, payload}`.
2. **`signing.py`** (`compute_signature`) HMAC-SHA256s the canonical string
   `METHOD\npath\ntimestamp\nnonce\nbody` over those *exact* bytes.
3. **`client.py`** (`ProxyClient._prepare` / `_handle_response`) wires
   headers, sends via `httpx`, and turns a `{"error": {...}}` envelope into a
   typed exception via `errors.from_response`.
4. **`errors.py`** classifies every Proxy error code into a `ProxyError`
   subclass with a `.retryable` flag (`True` / `False` / `None` = "inspect
   the exception further" — currently only `ExchangeError.outcome_unknown`).
5. **`idempotency.py`** is just `new_idempotency_key()` — a UUID4 generator.
   The lifecycle rule (generate once per intent, reuse unchanged across
   retries) is documented there but enforced by the caller, not this SDK.

**The one invariant everything else depends on:** the bytes that get signed
must be the *exact* bytes that get sent — never re-serialized in between.
That's why `client.py` calls `self._client.post(..., content=body, ...)`,
never `data=` (httpx form-encodes dicts under `data=`) or `json=` (would
re-serialize). If you touch the request-sending path, preserve this.

**Sync and async are both real transports, not one wrapping the other.**
`ProxyClient` owns both an `httpx.Client` and an `httpx.AsyncClient`; `call()`
and `acall()` each hit their own transport directly. `acall()` was
deliberately *not* implemented as `call()` run in a thread executor (that
was the first draft, corrected during review) — don't reintroduce that
pattern without a reason.

**`ProxyResult`** (in `client.py`) is a `dict` subclass returned by both
`call()` and `acall()` — indexing/iteration/`==` behave exactly like a plain
dict (backward compatible with "just returns the exchange's data"), plus one
extra attribute, `.idempotent_replay`, sourced from the real
`X-Idempotent-Replay` response header.

### Deliberately out of v1 scope — don't add without discussion

These were each considered and explicitly deferred; if a task seems to call
for one, that's worth flagging rather than just implementing:

- **System (bot) credentials.** `envelope.py`/`client.py` only support
  operator (human) credentials — no `system_name` field. The Proxy enforces
  strict, opposite rules for the two credential types (`trading-gateway/proxy/auth.py`),
  which needs its own design pass, not a bolted-on kwarg.
- **WebSocket support.** REST only.
- **Automatic retries.** `call()`/`acall()` always raise; retry policy is
  the caller's decision. See `README.md`'s error/retry table for why.
- **Client-side action/exchange allowlist validation.** The Proxy's
  allowlist (`trading-gateway/proxy/actions.py`) is a reviewed security
  boundary; a client-side copy would drift from it in whichever direction is
  worse. An invalid action still fails fast server-side, before any exchange
  call. See the docstring on `ProxyClient.call` for the full reasoning.

### Testing philosophy — no filesystem coupling to `trading-gateway`

`tests/test_signing.py` and `tests/test_error_parity.py` guard against this
SDK silently drifting from the server's actual protocol (canonical signing
string, error code table). Both do this via **pinned golden constants**
computed once against `trading-gateway/proxy/signing.py` /
`proxy/errors.py` and hardcoded in the test file — not a live import of
`trading-gateway`. An earlier draft of the error-parity test *did*
`sys.path`-import `trading-gateway` directly; that was deliberately reverted
because a package meant to be `pip install`-able and CI-able standalone
shouldn't have its test suite depend on a sibling directory existing on
disk. If the server's signing logic or error codes change, update the
pinned values in these two files by hand — each file's module docstring
says exactly what to regenerate.

### Security note on examples/docs

Don't put a real Proxy hostname, IP, or credential in `README.md` or
anywhere else in this repo, even as an "example" — `base_url` in the
quickstart is `https://your-proxy-host.example` on purpose. A real one
(`178-105-55-5.sslip.io`) was accidentally copied in from `SDK_WRITEUP.md`
and had to be scrubbed; that host runs a real trading-gateway holding
vaulted, real-money exchange credentials, and this repo may become public.
