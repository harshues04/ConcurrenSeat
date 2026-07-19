"""Tests for Strategy C's attempt_purchase facade."""

import uuid

from app.db.session import SessionLocal
from app.models import Event
from app.strategies import queue_waiting_room as qwr
from app.strategies.base import PurchaseStatus


def test_first_call_queues_without_touching_inventory(make_event, buyer):
    event_id = make_event(5)

    result = qwr.attempt_purchase(event_id, *buyer())

    assert result.status is PurchaseStatus.queued
    assert result.queue_position == 1
    assert result.reservation_id is None
    with SessionLocal() as db:
        assert db.get(Event, event_id).tickets_remaining == 5


def test_later_buyers_queue_behind(make_event, buyer):
    event_id = make_event(5)
    qwr.attempt_purchase(event_id, *buyer())

    assert qwr.attempt_purchase(event_id, *buyer()).queue_position == 2
    assert qwr.attempt_purchase(event_id, *buyer()).queue_position == 3


def test_duplicate_call_keeps_position_and_queue_size(make_event, buyer):
    event_id = make_event(5)
    qwr.attempt_purchase(event_id, *buyer())
    user_id, key = buyer()
    first = qwr.attempt_purchase(event_id, user_id, key)

    again = qwr.attempt_purchase(event_id, user_id, key)

    assert again.status is PurchaseStatus.queued
    assert again.queue_position == first.queue_position == 2
    assert qwr.queue_length(event_id) == 2


def test_resolved_key_returns_final_result_not_queued(make_event, buyer):
    event_id = make_event(1)
    user_id, key = buyer()
    qwr.attempt_purchase(event_id, user_id, key)
    qwr.drain(event_id)

    result = qwr.attempt_purchase(event_id, user_id, key)

    assert result.status is PurchaseStatus.success
    assert result.reservation_id is not None
    assert qwr.queue_length(event_id) == 0  # did not re-queue
