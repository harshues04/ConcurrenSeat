"""Seed the database with a demo event and its tickets.

Idempotent: re-running finds the existing event by name and leaves it alone.

Usage (from backend/): python -m app.db.seed
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.db.session import SessionLocal
from app.models import Event, Ticket

DEMO_EVENT_NAME = "ConcurrenSeat Launch Concert"
DEMO_TICKET_COUNT = 100


def seed() -> Event:
    with SessionLocal() as db:
        event = db.scalar(select(Event).where(Event.name == DEMO_EVENT_NAME))
        if event is not None:
            print(f"Event already seeded: {event.id} ({event.tickets_remaining} remaining)")
            return event

        event = Event(
            name=DEMO_EVENT_NAME,
            total_tickets=DEMO_TICKET_COUNT,
            tickets_remaining=DEMO_TICKET_COUNT,
            sale_start_time=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        db.add(event)
        db.flush()  # assigns event.id before the tickets reference it

        db.add_all(Ticket(event_id=event.id) for _ in range(DEMO_TICKET_COUNT))
        db.commit()
        db.refresh(event)
        print(f"Seeded event {event.id} with {DEMO_TICKET_COUNT} tickets")
        return event


if __name__ == "__main__":
    seed()
