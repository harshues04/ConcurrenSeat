import uuid
from datetime import datetime, timezone
from typing import Callable

import pytest
from sqlalchemy import delete, select

from app.core.redis_client import get_redis
from app.db.session import SessionLocal
from app.models import Event, Reservation, Ticket


@pytest.fixture(autouse=True)
def clean_redis_keys():
    """Drop per-event Redis state (counters, locks, queues, results) after
    each test."""
    yield
    r = get_redis()
    keys = r.keys("event:*") + r.keys("purchase:result:*")
    if keys:
        r.delete(*keys)


@pytest.fixture
def make_event() -> Callable[[int], uuid.UUID]:
    """Create a throwaway event with N available tickets; cleaned up after."""
    created: list[uuid.UUID] = []

    def _make(ticket_count: int) -> uuid.UUID:
        with SessionLocal() as db:
            event = Event(
                name=f"test-{uuid.uuid4()}",
                total_tickets=ticket_count,
                tickets_remaining=ticket_count,
                sale_start_time=datetime.now(timezone.utc),
            )
            db.add(event)
            db.flush()
            db.add_all(Ticket(event_id=event.id) for _ in range(ticket_count))
            db.commit()
            created.append(event.id)
            return event.id

    yield _make

    with SessionLocal() as db:
        for event_id in created:
            ticket_ids = db.scalars(
                select(Ticket.id).where(Ticket.event_id == event_id)
            ).all()
            if ticket_ids:
                db.execute(
                    delete(Reservation).where(Reservation.ticket_id.in_(ticket_ids))
                )
            db.execute(delete(Ticket).where(Ticket.event_id == event_id))
            db.execute(delete(Event).where(Event.id == event_id))
        db.commit()


@pytest.fixture
def buyer() -> Callable[[], tuple[uuid.UUID, str]]:
    """Fresh (user_id, idempotency_key) pair per call."""

    def _buyer() -> tuple[uuid.UUID, str]:
        return uuid.uuid4(), f"idem-{uuid.uuid4()}"

    return _buyer
