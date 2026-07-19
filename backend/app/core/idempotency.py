"""Idempotency for purchase requests.

Two layers:
1. A cheap pre-check: if a reservation with this key already exists, return
   it without attempting a purchase.
2. The race-proof backstop: reservations.idempotency_key is UNIQUE, so if two
   requests with the same key pass the pre-check simultaneously, Postgres
   rejects the second insert with IntegrityError and we hand back the first
   one's reservation. The losing attempt must undo any inventory it claimed.
"""

from sqlalchemy import select

from app.db.session import SessionLocal
from app.models import Reservation
from app.strategies.base import PurchaseStatus, ReservationResult


def find_existing(idempotency_key: str) -> ReservationResult | None:
    """Return the original result for a key we've already served, else None."""
    with SessionLocal() as db:
        reservation = db.scalar(
            select(Reservation).where(Reservation.idempotency_key == idempotency_key)
        )
        if reservation is None:
            return None
        return ReservationResult(
            status=PurchaseStatus.success,
            reservation_id=reservation.id,
            ticket_id=reservation.ticket_id,
            message="Duplicate request; returning the original reservation.",
        )
