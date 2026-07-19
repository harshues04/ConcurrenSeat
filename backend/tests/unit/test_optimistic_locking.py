import uuid

from sqlalchemy import select

from app.db.session import SessionLocal
from app.models import Event, Reservation, Ticket, TicketStatus
from app.strategies import optimistic_locking
from app.strategies.base import PurchaseStatus, ReservationResult


def test_purchase_succeeds_and_updates_all_state(make_event, buyer):
    event_id = make_event(1)
    user_id, key = buyer()

    result = optimistic_locking.attempt_purchase(event_id, user_id, key)

    assert result.status is PurchaseStatus.success
    assert result.reservation_id is not None
    with SessionLocal() as db:
        ticket = db.get(Ticket, result.ticket_id)
        assert ticket.status is TicketStatus.reserved
        assert ticket.version == 1
        assert db.get(Event, event_id).tickets_remaining == 0
        reservation = db.get(Reservation, result.reservation_id)
        assert reservation.ticket_id == result.ticket_id
        assert reservation.user_id == user_id
        assert reservation.idempotency_key == key


def test_sold_out_when_no_tickets_available(make_event, buyer):
    event_id = make_event(1)
    assert optimistic_locking.attempt_purchase(event_id, *buyer()).succeeded

    result = optimistic_locking.attempt_purchase(event_id, *buyer())

    assert result.status is PurchaseStatus.sold_out
    assert result.reservation_id is None


def test_conflict_is_retried_with_backoff_then_gives_up(monkeypatch, buyer):
    conflict = ReservationResult(status=PurchaseStatus.retry_exhausted)
    attempts = []
    sleeps = []
    monkeypatch.setattr(
        optimistic_locking, "_try_once", lambda *a: attempts.append(a) or conflict
    )
    monkeypatch.setattr(optimistic_locking.time, "sleep", sleeps.append)

    result = optimistic_locking.attempt_purchase(uuid.uuid4(), *buyer())

    assert result.status is PurchaseStatus.retry_exhausted
    assert len(attempts) == 1 + optimistic_locking.MAX_RETRIES
    assert len(sleeps) == optimistic_locking.MAX_RETRIES
    # Backoff windows double each round; sampled values must stay inside them.
    for i, slept in enumerate(sleeps):
        assert 0 <= slept <= optimistic_locking.BACKOFF_BASE_SECONDS * (2**i)


def test_conflict_then_success_stops_retrying(monkeypatch, buyer):
    conflict = ReservationResult(status=PurchaseStatus.retry_exhausted)
    win = ReservationResult(status=PurchaseStatus.success)
    outcomes = iter([conflict, win])
    attempts = []
    monkeypatch.setattr(
        optimistic_locking,
        "_try_once",
        lambda *a: attempts.append(a) or next(outcomes),
    )
    monkeypatch.setattr(optimistic_locking.time, "sleep", lambda s: None)

    result = optimistic_locking.attempt_purchase(uuid.uuid4(), *buyer())

    assert result.status is PurchaseStatus.success
    assert len(attempts) == 2


def test_sold_out_is_not_retried(monkeypatch, buyer):
    sold_out = ReservationResult(status=PurchaseStatus.sold_out)
    attempts = []
    monkeypatch.setattr(
        optimistic_locking, "_try_once", lambda *a: attempts.append(a) or sold_out
    )

    result = optimistic_locking.attempt_purchase(uuid.uuid4(), *buyer())

    assert result.status is PurchaseStatus.sold_out
    assert len(attempts) == 1
