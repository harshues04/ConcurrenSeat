"""Tests for Strategy C's queue primitives (no worker involved yet)."""

import uuid

from app.core.redis_client import get_redis
from app.strategies import queue_waiting_room as qwr
from app.strategies.base import PurchaseStatus, ReservationResult


def test_enqueue_assigns_fifo_positions():
    event_id = uuid.uuid4()

    assert qwr.enqueue(event_id, uuid.uuid4(), "first") == 1
    assert qwr.enqueue(event_id, uuid.uuid4(), "second") == 2
    assert qwr.enqueue(event_id, uuid.uuid4(), "third") == 3
    assert qwr.queue_length(event_id) == 3


def test_rejoining_keeps_original_position():
    event_id = uuid.uuid4()
    qwr.enqueue(event_id, uuid.uuid4(), "first")
    user = uuid.uuid4()
    assert qwr.enqueue(event_id, user, "mine") == 2
    qwr.enqueue(event_id, uuid.uuid4(), "third")

    # Double-click / network retry: same key joins again.
    assert qwr.enqueue(event_id, user, "mine") == 2
    assert qwr.queue_length(event_id) == 3


def test_position_reflects_queue_state_and_none_when_absent():
    event_id = uuid.uuid4()
    assert qwr.queue_position(event_id, "ghost") is None

    qwr.enqueue(event_id, uuid.uuid4(), "a")
    qwr.enqueue(event_id, uuid.uuid4(), "b")
    assert qwr.queue_position(event_id, "b") == 2

    # Head is served: everyone behind moves up.
    get_redis().lpop(qwr._queue_key(event_id))
    assert qwr.queue_position(event_id, "b") == 1


def test_result_roundtrip_preserves_fields():
    key = f"res-{uuid.uuid4()}"
    original = ReservationResult(
        status=PurchaseStatus.success,
        reservation_id=uuid.uuid4(),
        ticket_id=uuid.uuid4(),
        message="won",
    )

    qwr.store_result(key, original)
    fetched = qwr.fetch_result(key)

    assert fetched == original
    assert get_redis().ttl(qwr._result_key(key)) > 0


def test_result_roundtrip_with_empty_ids():
    key = f"res-{uuid.uuid4()}"
    qwr.store_result(key, ReservationResult(status=PurchaseStatus.sold_out))

    fetched = qwr.fetch_result(key)

    assert fetched.status is PurchaseStatus.sold_out
    assert fetched.reservation_id is None and fetched.ticket_id is None


def test_fetch_result_missing_returns_none():
    assert qwr.fetch_result(f"never-{uuid.uuid4()}") is None
