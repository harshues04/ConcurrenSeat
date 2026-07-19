"""Strategy A: optimistic locking via a version column.

No locks are held while deciding. We read an available ticket (and the version
we saw), then issue a conditional UPDATE that only succeeds if the version is
unchanged. A concurrent writer bumps the version first -> our rowcount is 0 ->
we know we lost the race without ever blocking.
"""

import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from app.core.idempotency import find_existing
from app.db.session import SessionLocal
from app.models import Event, Reservation, Ticket, TicketStatus
from app.strategies.base import PurchaseStatus, ReservationResult

RESERVATION_TTL = timedelta(minutes=5)
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 0.02


def _try_once(
    event_id: uuid.UUID, user_id: uuid.UUID, idempotency_key: str
) -> ReservationResult:
    with SessionLocal() as db:
        # Random pick spreads concurrent buyers across rows; if everyone
        # grabbed the first available ticket, every racer but one would
        # conflict on the same row each round.
        candidate = db.execute(
            select(Ticket.id, Ticket.version)
            .where(
                Ticket.event_id == event_id,
                Ticket.status == TicketStatus.available,
            )
            .order_by(func.random())
            .limit(1)
        ).first()

        if candidate is None:
            return ReservationResult(
                status=PurchaseStatus.sold_out, message="No tickets remaining."
            )

        claimed = db.execute(
            update(Ticket)
            .where(Ticket.id == candidate.id, Ticket.version == candidate.version)
            .values(status=TicketStatus.reserved, version=Ticket.version + 1)
        )
        if claimed.rowcount == 0:
            # Someone else claimed this ticket between our read and write.
            db.rollback()
            return ReservationResult(
                status=PurchaseStatus.retry_exhausted,
                message="Lost a write race on the selected ticket.",
            )

        db.execute(
            update(Event)
            .where(Event.id == event_id)
            .values(tickets_remaining=Event.tickets_remaining - 1)
        )
        reservation = Reservation(
            ticket_id=candidate.id,
            user_id=user_id,
            idempotency_key=idempotency_key,
            expires_at=datetime.now(timezone.utc) + RESERVATION_TTL,
        )
        db.add(reservation)
        try:
            db.commit()
        except IntegrityError:
            # Duplicate idempotency_key raced past the pre-check. The rollback
            # also undoes our ticket claim and counter decrement (same txn),
            # so the original reservation is the only inventory consumed.
            db.rollback()
            return find_existing(idempotency_key) or ReservationResult(
                status=PurchaseStatus.retry_exhausted,
                message="Duplicate request raced and neither side won; retry.",
            )
        return ReservationResult(
            status=PurchaseStatus.success,
            reservation_id=reservation.id,
            ticket_id=candidate.id,
        )


def attempt_purchase(
    event_id: uuid.UUID, user_id: uuid.UUID, idempotency_key: str
) -> ReservationResult:
    existing = find_existing(idempotency_key)
    if existing is not None:
        return existing

    result = _try_once(event_id, user_id, idempotency_key)
    for attempt in range(MAX_RETRIES):
        if result.status is not PurchaseStatus.retry_exhausted:
            return result
        # Full jitter: sleep a random slice of the doubling window so retriers
        # that conflicted together don't retry in lockstep and conflict again.
        time.sleep(random.uniform(0, BACKOFF_BASE_SECONDS * (2**attempt)))
        result = _try_once(event_id, user_id, idempotency_key)
    return result
