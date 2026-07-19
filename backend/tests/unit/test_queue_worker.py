"""Tests for Strategy C's admission worker."""

import asyncio
import uuid

import pytest

from app.core.redis_client import get_redis
from app.db.session import SessionLocal
from app.models import Event
from app.strategies import queue_waiting_room as qwr
from app.strategies.base import PurchaseStatus


def test_process_one_on_empty_queue_returns_none():
    assert qwr.process_one(uuid.uuid4()) is None


def test_process_one_serves_head_and_stores_result(make_event, buyer):
    event_id = make_event(1)
    user_id, key = buyer()
    qwr.enqueue(event_id, user_id, key)

    served = qwr.process_one(event_id)

    assert served == key
    result = qwr.fetch_result(key)
    assert result.status is PurchaseStatus.success
    assert qwr.queue_length(event_id) == 0
    assert get_redis().hget(qwr._payload_key(event_id), key) is None
    with SessionLocal() as db:
        assert db.get(Event, event_id).tickets_remaining == 0


def test_drain_serves_fifo_and_losers_get_sold_out(make_event, buyer):
    event_id = make_event(2)
    keys = []
    for _ in range(4):
        user_id, key = buyer()
        qwr.enqueue(event_id, user_id, key)
        keys.append(key)

    assert qwr.drain(event_id) == 4

    outcomes = [qwr.fetch_result(k).status for k in keys]
    # FIFO fairness: the first two in line won, the last two did not.
    assert outcomes == [
        PurchaseStatus.success,
        PurchaseStatus.success,
        PurchaseStatus.sold_out,
        PurchaseStatus.sold_out,
    ]


def test_lost_payload_still_resolves_the_request(make_event, buyer):
    event_id = make_event(1)
    user_id, key = buyer()
    qwr.enqueue(event_id, user_id, key)
    get_redis().hdel(qwr._payload_key(event_id), key)

    assert qwr.process_one(event_id) == key

    result = qwr.fetch_result(key)
    assert result.status is PurchaseStatus.retry_exhausted
    with SessionLocal() as db:  # no ticket was consumed
        assert db.get(Event, event_id).tickets_remaining == 1


@pytest.mark.asyncio
async def test_worker_loop_serves_queue_until_stopped(make_event, buyer):
    event_id = make_event(1)
    user_id, key = buyer()
    qwr.enqueue(event_id, user_id, key)

    stop = asyncio.Event()
    task = asyncio.create_task(qwr.worker_loop(event_id, stop))
    try:
        async with asyncio.timeout(5):
            while qwr.fetch_result(key) is None:
                await asyncio.sleep(0.02)
    finally:
        stop.set()
        await task

    assert qwr.fetch_result(key).status is PurchaseStatus.success
