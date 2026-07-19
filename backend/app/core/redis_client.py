"""Redis client singletons.

Strategies run as sync code (matching the sync SQLAlchemy/psycopg2 stack), so
they use the sync client. The WebSocket router and queue worker are async and
use the async client. Both point at the same Redis.
"""

import asyncio
from weakref import WeakKeyDictionary

import redis
import redis.asyncio as aredis

from app.config import get_settings

_sync_client: redis.Redis | None = None
# Async connections are bound to the event loop they were created on; reusing
# them from another loop (fresh loop per test, uvicorn reload, ...) raises
# "Event loop is closed". Cache one client per living loop instead.
_async_clients: "WeakKeyDictionary[asyncio.AbstractEventLoop, aredis.Redis]" = (
    WeakKeyDictionary()
)


def get_redis() -> redis.Redis:
    global _sync_client
    if _sync_client is None:
        # Flash-sale traffic means hundreds of threads hitting Redis at the
        # same instant (the lock strategy spins on SET NX) — the default
        # pool cap is far too small for that burst.
        _sync_client = redis.Redis.from_url(
            get_settings().redis_url, decode_responses=True, max_connections=1000
        )
    return _sync_client


def get_async_redis() -> aredis.Redis:
    loop = asyncio.get_running_loop()
    client = _async_clients.get(loop)
    if client is None:
        client = aredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
        _async_clients[loop] = client
    return client
