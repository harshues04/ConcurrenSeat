"""Duplicate purchase requests must return the original reservation."""

import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import func, select

from app.db.session import SessionLocal
from app.models import Event, Reservation
from app.strategies import optimistic_locking, redis_distributed_lock
from app.strategies.base import PurchaseStatus

STRATEGIES = [optimistic_locking, redis_distributed_lock]


def _reservation_count(key: str) -> int:
    with SessionLocal() as db:
        return db.scalar(
            select(func.count())
            .select_from(Reservation)
            .where(Reservation.idempotency_key == key)
        )


@pytest.mark.parametrize("strategy", STRATEGIES, ids=["optimistic", "redis_lock"])
def test_sequential_duplicate_returns_original(strategy, make_event, buyer):
    event_id = make_event(5)
    user_id, key = buyer()

    first = strategy.attempt_purchase(event_id, user_id, key)
    second = strategy.attempt_purchase(event_id, user_id, key)

    assert first.status is PurchaseStatus.success
    assert second.status is PurchaseStatus.success
    assert second.reservation_id == first.reservation_id
    assert second.ticket_id == first.ticket_id
    assert _reservation_count(key) == 1
    with SessionLocal() as db:  # only one ticket consumed
        assert db.get(Event, event_id).tickets_remaining == 4


@pytest.mark.parametrize("strategy", STRATEGIES, ids=["optimistic", "redis_lock"])
def test_concurrent_duplicates_create_one_reservation(strategy, make_event, buyer):
    event_id = make_event(10)
    user_id, key = buyer()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda _: strategy.attempt_purchase(event_id, user_id, key),
                range(8),
            )
        )

    assert all(r.status is PurchaseStatus.success for r in results)
    assert len({r.reservation_id for r in results}) == 1
    assert _reservation_count(key) == 1
    with SessionLocal() as db:
        assert db.get(Event, event_id).tickets_remaining == 9


def test_queue_strategy_dedupes_via_waiting_room(make_event, buyer):
    from app.core.redis_client import get_redis
    from app.strategies import queue_waiting_room as qwr

    event_id = make_event(5)
    user_id, key = buyer()
    try:
        qwr.attempt_purchase(event_id, user_id, key)
        qwr.attempt_purchase(event_id, user_id, key)
        assert qwr.queue_length(event_id) == 1

        qwr.drain(event_id)
        first = qwr.attempt_purchase(event_id, user_id, key)
        second = qwr.attempt_purchase(event_id, user_id, key)

        assert first.reservation_id == second.reservation_id
        assert _reservation_count(key) == 1
    finally:
        r = get_redis()
        keys = r.keys("event:*:queue") + r.keys("event:*:queue:payloads") + r.keys(
            "purchase:result:*"
        )
        if keys:
            r.delete(*keys)
