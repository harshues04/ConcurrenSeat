import uuid

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.session import SessionLocal, get_db
from app.models import Event, Reservation
from app.schemas import PurchaseIn, PurchaseOut, ReservationStatusOut
from app.strategies import optimistic_locking, queue_waiting_room, redis_distributed_lock
from app.strategies.base import PurchaseStatus

router = APIRouter(tags=["bookings"])

STRATEGIES = {
    "optimistic": optimistic_locking.attempt_purchase,
    "redis_lock": redis_distributed_lock.attempt_purchase,
    "queue": queue_waiting_room.attempt_purchase,
}

# One request, one HTTP code: the body's status field always tells the full
# story, the code just makes curl/monitoring legible.
STATUS_CODES = {
    PurchaseStatus.success: 201,
    PurchaseStatus.queued: 202,
    PurchaseStatus.sold_out: 409,
    PurchaseStatus.retry_exhausted: 503,
}


@router.post("/events/{event_id}/purchase", response_model=PurchaseOut)
def purchase(
    event_id: uuid.UUID,
    body: PurchaseIn,
    response: Response,
) -> PurchaseOut:
    strategy_name = body.strategy or get_settings().default_strategy
    attempt = STRATEGIES.get(strategy_name)
    if attempt is None:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown strategy {strategy_name!r}; expected one of {sorted(STRATEGIES)}",
        )
    # Deliberately NOT a request-scoped Depends(get_db) session: the strategy
    # opens its own sessions, and holding one across that call means N
    # concurrent requests hold N connections while waiting for N more —
    # a pool deadlock under burst load. Check-and-release instead.
    with SessionLocal() as db:
        event_exists = db.get(Event, event_id) is not None
    if not event_exists:
        raise HTTPException(status_code=404, detail="Event not found")

    result = attempt(event_id, body.user_id, body.idempotency_key)
    response.status_code = STATUS_CODES[result.status]
    return PurchaseOut(
        status=result.status,
        reservation_id=result.reservation_id,
        ticket_id=result.ticket_id,
        queue_position=result.queue_position,
        message=result.message,
    )


@router.get("/reservations/{reservation_id}/status", response_model=ReservationStatusOut)
def reservation_status(
    reservation_id: uuid.UUID, db: Session = Depends(get_db)
) -> Reservation:
    reservation = db.get(Reservation, reservation_id)
    if reservation is None:
        raise HTTPException(status_code=404, detail="Reservation not found")
    return reservation
