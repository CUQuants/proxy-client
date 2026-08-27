from .client import ProxyClient, ProxyResult, new_idempotency_key
from .orders import (
    NormalizedOrder,
    OkxOrderBuilder,
    okx_aplace_order,
    okx_acancel_order,
    okx_cancel_order,
    okx_place_order,
)
from .errors import (
    ActionNotPermittedError,
    AuthFailedError,
    CredentialExpiredError,
    ExchangeError,
    IdempotencyInFlightError,
    IdempotencyKeyRequiredError,
    IdempotencyKeyReusedError,
    KillSwitchActiveError,
    LogUnavailableError,
    MalformedRequestError,
    ProxyError,
    ProxyUnreachableError,
    RateLimitedError,
    RiskLimitExceededError,
    StreamParamsConflictError,
)

__all__ = [
    "ProxyClient",
    "ProxyResult",
    "new_idempotency_key",
    "ProxyError",
    "ProxyUnreachableError",
    "AuthFailedError",
    "CredentialExpiredError",
    "ActionNotPermittedError",
    "ExchangeError",
    "MalformedRequestError",
    "IdempotencyKeyRequiredError",
    "IdempotencyKeyReusedError",
    "IdempotencyInFlightError",
    "LogUnavailableError",
    "RateLimitedError",
    "StreamParamsConflictError",
    "RiskLimitExceededError",
    "KillSwitchActiveError",
    "NormalizedOrder",
    "OkxOrderBuilder",
    "okx_place_order",
    "okx_aplace_order",
    "okx_cancel_order",
    "okx_acancel_order",
]

# WebSocket support requires the optional `websocket` extra
# (`pip install cuq-proxy-client[websocket]`) since it's the only part of
# this package that needs the `websockets` library. Imported lazily via
# `proxy_client.websocket` rather than re-exported here, so a REST-only
# install never has to resolve that import at all.
