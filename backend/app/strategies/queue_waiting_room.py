"""Strategy C: queue-based waiting room.

Buyers are not served directly: each request lands in a per-event Redis list
(RPUSH tail, worker LPOPs head -> strict FIFO) and gets back "queued" with a
position. A worker admits requests at a controlled rate and runs the actual
purchase, storing the outcome under the request's idempotency key for the
client to pick up (via WebSocket push or the status endpoint).

This file: queue primitives. The admission worker builds on these.
"""

import asyncio
import json
import uuid

from app.core.redis_client import get_redis
from app.strategies import optimistic_locking
from app.strategies.base import PurchaseStatus, ReservationResult

RESULT_TTL_SECONDS = 600  # outcomes are pickup-once-soon data, not records
PAYLOAD_TTL_SECONDS = 3600
ADMISSIONS_PER_SECOND = 50  # worker throttle: the knob that shields Postgres
IDLE_POLL_SECONDS = 0.05


def _queue_key(event_id: uuid.UUID) -> str:
    return f"event:{event_id}:queue"


def _payload_key(event_id: uuid.UUID) -> str:
    return f"event:{event_id}:queue:payloads"


def _result_key(idempotency_key: str) -> str:
    return f"purchase:result:{idempotency_key}"


def _served_channel(event_id: uuid.UUID) -> str:
    return f"event:{event_id}:served"


def enqueue(event_id: uuid.UUID, user_id: uuid.UUID, idempotency_key: str) -> int:
    """Join the waiting room. Returns 1-based queue position (1 = next up).

    Re-joining with the same idempotency key does not queue twice; you keep
    your original spot.
    """
    r = get_redis()
    existing = r.lpos(_queue_key(event_id), idempotency_key)
    if existing is not None:
        return existing + 1

    r.hset(
        _payload_key(event_id),
        idempotency_key,
        json.dumps({"user_id": str(user_id)}),
    )
    r.expire(_payload_key(event_id), PAYLOAD_TTL_SECONDS)
    # RPUSH returns the new length; we are the tail, so that IS our position.
    return r.rpush(_queue_key(event_id), idempotency_key)


def queue_position(event_id: uuid.UUID, idempotency_key: str) -> int | None:
    """1-based position, or None once dequeued (or never queued)."""
    index = get_redis().lpos(_queue_key(event_id), idempotency_key)
    return None if index is None else index + 1


def queue_length(event_id: uuid.UUID) -> int:
    return get_redis().llen(_queue_key(event_id))


def store_result(idempotency_key: str, result: ReservationResult) -> None:
    get_redis().set(
        _result_key(idempotency_key),
        json.dumps(
            {
                "status": result.status.value,
                "reservation_id": str(result.reservation_id or ""),
                "ticket_id": str(result.ticket_id or ""),
                "message": result.message,
            }
        ),
        ex=RESULT_TTL_SECONDS,
    )


def fetch_result(idempotency_key: str) -> ReservationResult | None:
    raw = get_redis().get(_result_key(idempotency_key))
    if raw is None:
        return None
    data = json.loads(raw)
    return ReservationResult(
        status=PurchaseStatus(data["status"]),
        reservation_id=uuid.UUID(data["reservation_id"]) if data["reservation_id"] else None,
        ticket_id=uuid.UUID(data["ticket_id"]) if data["ticket_id"] else None,
        message=data["message"],
    )


def process_one(event_id: uuid.UUID) -> str | None:
    """Admit the head of the queue and run its purchase.

    Returns the idempotency key served, or None if the queue was empty.
    The purchase itself is delegated to the optimistic strategy — the queue
    has already spaced requests out, so contention there is minimal.
    """
    r = get_redis()
    key = r.lpop(_queue_key(event_id))
    if key is None:
        return None

    raw = r.hget(_payload_key(event_id), key)
    r.hdel(_payload_key(event_id), key)
    if raw is None:
        # Payload hash expired while they waited; don't leave them hanging.
        store_result(
            key,
            ReservationResult(
                status=PurchaseStatus.retry_exhausted,
                message="Queue entry expired; please retry.",
            ),
        )
        return key

    user_id = uuid.UUID(json.loads(raw)["user_id"])
    result = optimistic_locking.attempt_purchase(event_id, user_id, key)
    store_result(key, result)
    # Wake WebSocket handlers: someone was served, positions changed.
    r.publish(_served_channel(event_id), key)
    return key


def drain(event_id: uuid.UUID) -> int:
    """Serve the whole queue at full speed (tests/benchmarks). Returns count."""
    served = 0
    while process_one(event_id) is not None:
        served += 1
    return served


def attempt_purchase(
    event_id: uuid.UUID, user_id: uuid.UUID, idempotency_key: str
) -> ReservationResult:
    """Waiting-room semantics: never blocks on the actual purchase.

    First call queues you and returns `queued` + position; repeat calls with
    the same key return your current position while waiting, and the final
    outcome once the worker has served you.
    """
    done = fetch_result(idempotency_key)
    if done is not None:
        return done

    position = enqueue(event_id, user_id, idempotency_key)
    return ReservationResult(
        status=PurchaseStatus.queued,
        queue_position=position,
        message=f"You are number {position} in line.",
    )


async def worker_loop(event_id: uuid.UUID, stop: asyncio.Event) -> None:
    """Admit at ADMISSIONS_PER_SECOND until stop is set. Runs as an asyncio
    task; the sync Redis/DB work is pushed to a thread so the loop (and the
    WebSocket handlers sharing it) never blocks."""
    interval = 1 / ADMISSIONS_PER_SECOND
    while not stop.is_set():
        served = await asyncio.to_thread(process_one, event_id)
        await asyncio.sleep(interval if served else IDLE_POLL_SECONDS)


def _serve_all_queues_once() -> bool:
    """One admission pass over every live event queue. True if anyone was
    served. (Pattern matches list keys only: payload hashes end differently.)"""
    r = get_redis()
    served_any = False
    for key in r.scan_iter("event:*:queue"):
        event_id = uuid.UUID(key.split(":")[1])
        if process_one(event_id) is not None:
            served_any = True
    return served_any


async def worker_loop_all(stop: asyncio.Event) -> None:
    """App-lifespan worker: one loop serving every event's queue."""
    interval = 1 / ADMISSIONS_PER_SECOND
    while not stop.is_set():
        served = await asyncio.to_thread(_serve_all_queues_once)
        await asyncio.sleep(interval if served else IDLE_POLL_SECONDS)
