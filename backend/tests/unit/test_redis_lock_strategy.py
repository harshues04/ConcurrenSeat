"""Tests for Strategy B's lock handling and full purchase path."""

import uuid

import pytest

from app.core.redis_client import get_redis
from app.db.session import SessionLocal
from app.models import Event, Ticket, TicketStatus
from app.strategies import redis_distributed_lock as rdl
from app.strategies.base import PurchaseStatus


def test_lock_is_mutually_exclusive_and_reacquirable_after_release(monkeypatch):
    event_id = uuid.uuid4()

    token = rdl._acquire_lock(event_id)
    assert token is not None

    # Second acquire must fail while held (shrink the wait so the test is fast).
    monkeypatch.setattr(rdl, "LOCK_ACQUIRE_TIMEOUT_SECONDS", 0.05)
    assert rdl._acquire_lock(event_id) is None

    rdl._release_lock(event_id, token)
    token2 = rdl._acquire_lock(event_id)
    assert token2 is not None
    rdl._release_lock(event_id, token2)


def test_release_with_wrong_token_keeps_lock():
    event_id = uuid.uuid4()
    token = rdl._acquire_lock(event_id)

    rdl._release_lock(event_id, "not-the-owner-token")
    assert get_redis().exists(rdl._lock_key(event_id)) == 1

    rdl._release_lock(event_id, token)
    assert get_redis().exists(rdl._lock_key(event_id)) == 0


def test_purchase_success_updates_postgres_and_redis(make_event, buyer):
    event_id = make_event(2)
    user_id, key = buyer()

    result = rdl.attempt_purchase(event_id, user_id, key)

    assert result.status is PurchaseStatus.success
    with SessionLocal() as db:
        assert db.get(Ticket, result.ticket_id).status is TicketStatus.reserved
        assert db.get(Event, event_id).tickets_remaining == 1
    assert get_redis().get(rdl._counter_key(event_id)) == "1"
    assert get_redis().exists(rdl._lock_key(event_id)) == 0  # lock released


def test_purchase_sold_out(make_event, buyer):
    event_id = make_event(1)
    assert rdl.attempt_purchase(event_id, *buyer()).succeeded

    result = rdl.attempt_purchase(event_id, *buyer())

    assert result.status is PurchaseStatus.sold_out


def test_lock_timeout_returns_unit_and_fails_gracefully(make_event, buyer, monkeypatch):
    event_id = make_event(1)
    # Someone else holds the lock for the whole attempt.
    blocker = rdl._acquire_lock(event_id)
    monkeypatch.setattr(rdl, "LOCK_ACQUIRE_TIMEOUT_SECONDS", 0.05)

    result = rdl.attempt_purchase(event_id, *buyer())

    assert result.status is PurchaseStatus.retry_exhausted
    # The gate unit was compensated back, so a later buyer can still win.
    assert get_redis().get(rdl._counter_key(event_id)) == "1"
    rdl._release_lock(event_id, blocker)


def test_postgres_failure_compensates_redis_and_reraises(make_event, buyer, monkeypatch):
    event_id = make_event(3)
    rdl._ensure_counter(event_id)  # init first; _ensure_counter needs the real DB

    def explode():
        raise RuntimeError("pg down")

    monkeypatch.setattr(rdl, "SessionLocal", explode)

    with pytest.raises(RuntimeError, match="pg down"):
        rdl.attempt_purchase(event_id, *buyer())

    assert get_redis().get(rdl._counter_key(event_id)) == "3"
    assert get_redis().exists(rdl._lock_key(event_id)) == 0  # lock still released
