"""Strategy B: Redis-gated inventory with a distributed lock.

Correctness rests on the Lua script: Redis executes scripts atomically, so
"check remaining > 0, then decrement" cannot interleave with another buyer's
check. Overselling is therefore impossible at the gate, before Postgres is
ever touched. (The per-event lock and Postgres assignment come next.)

The counter key is initialized lazily from Postgres with SET NX, so two
requests racing to initialize cannot overwrite each other.
"""

import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from app.core.idempotency import find_existing
from app.core.redis_client import get_redis
from app.db.session import SessionLocal
from app.models import Event, Reservation, Ticket, TicketStatus
from app.strategies.base import PurchaseStatus, ReservationResult

SOLD_OUT = -1
UNINITIALIZED = -2

RESERVATION_TTL = timedelta(minutes=5)
LOCK_TTL_MS = 2000  # safety net: a crashed holder's lock self-expires
LOCK_ACQUIRE_TIMEOUT_SECONDS = 2.0

# KEYS[1] = counter key. Returns remaining-after-decrement, or a sentinel.
_CHECK_AND_DECREMENT = """
local remaining = redis.call('GET', KEYS[1])
if remaining == false then return -2 end
remaining = tonumber(remaining)
if remaining <= 0 then return -1 end
return redis.call('DECR', KEYS[1])
"""


def _counter_key(event_id: uuid.UUID) -> str:
    return f"event:{event_id}:remaining"


def _ensure_counter(event_id: uuid.UUID) -> None:
    r = get_redis()
    key = _counter_key(event_id)
    if r.exists(key):
        return
    with SessionLocal() as db:
        available = db.scalar(
            select(func.count())
            .select_from(Ticket)
            .where(
                Ticket.event_id == event_id,
                Ticket.status == TicketStatus.available,
            )
        )
    # nx: if a concurrent request initialized meanwhile, keep theirs.
    r.set(key, available, nx=True)


def try_decrement(event_id: uuid.UUID) -> int:
    """Atomically take one unit of inventory. Returns remaining after the
    take, or SOLD_OUT. Initializes the counter from Postgres on first use."""
    r = get_redis()
    key = _counter_key(event_id)
    result = r.eval(_CHECK_AND_DECREMENT, 1, key)
    if result == UNINITIALIZED:
        _ensure_counter(event_id)
        result = r.eval(_CHECK_AND_DECREMENT, 1, key)
        if result == UNINITIALIZED:  # no such event / zero tickets seeded
            return SOLD_OUT
    return result


def give_back(event_id: uuid.UUID) -> None:
    """Compensation: return a unit taken by try_decrement after a downstream
    failure, so Redis inventory doesn't leak."""
    get_redis().incr(_counter_key(event_id))


# Token check ensures we only ever delete a lock we still own — if our TTL
# expired and someone else acquired it, a blind DEL would free THEIR lock.
_RELEASE_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


def _lock_key(event_id: uuid.UUID) -> str:
    return f"event:{event_id}:lock"


def _acquire_lock(event_id: uuid.UUID) -> str | None:
    """Spin on SET NX PX until acquired or timeout. Returns the owner token."""
    r = get_redis()
    token = uuid.uuid4().hex
    deadline = time.monotonic() + LOCK_ACQUIRE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if r.set(_lock_key(event_id), token, nx=True, px=LOCK_TTL_MS):
            return token
        time.sleep(random.uniform(0.001, 0.005))
    return None


def _release_lock(event_id: uuid.UUID, token: str) -> None:
    get_redis().eval(_RELEASE_LOCK, 1, _lock_key(event_id), token)


def attempt_purchase(
    event_id: uuid.UUID, user_id: uuid.UUID, idempotency_key: str
) -> ReservationResult:
    existing = find_existing(idempotency_key)
    if existing is not None:
        return existing

    # Gate first: losing here costs one Redis round-trip, no lock, no Postgres.
    if try_decrement(event_id) == SOLD_OUT:
        return ReservationResult(
            status=PurchaseStatus.sold_out, message="No tickets remaining."
        )

    token = _acquire_lock(event_id)
    if token is None:
        give_back(event_id)
        return ReservationResult(
            status=PurchaseStatus.retry_exhausted,
            message="Timed out waiting for the event lock; please retry.",
        )

    try:
        with SessionLocal() as db:
            # Serialized by the lock, so plain "first available" is safe here —
            # no other writer can pick the same row while we hold it.
            candidate = db.scalar(
                select(Ticket.id)
                .where(
                    Ticket.event_id == event_id,
                    Ticket.status == TicketStatus.available,
                )
                .limit(1)
            )
            if candidate is None:
                # Redis and Postgres disagree (e.g. counter re-initialized);
                # trust Postgres and repair the counter.
                give_back(event_id)
                return ReservationResult(
                    status=PurchaseStatus.sold_out, message="No tickets remaining."
                )

            db.execute(
                update(Ticket)
                .where(Ticket.id == candidate)
                .values(status=TicketStatus.reserved, version=Ticket.version + 1)
            )
            db.execute(
                update(Event)
                .where(Event.id == event_id)
                .values(tickets_remaining=Event.tickets_remaining - 1)
            )
            reservation = Reservation(
                ticket_id=candidate,
                user_id=user_id,
                idempotency_key=idempotency_key,
                expires_at=datetime.now(timezone.utc) + RESERVATION_TTL,
            )
            db.add(reservation)
            try:
                db.commit()
            except IntegrityError:
                # Duplicate key raced past the pre-check. The rollback undoes
                # the Postgres claim, but the Redis gate unit was ours alone —
                # hand it back before returning the original reservation.
                db.rollback()
                give_back(event_id)
                return find_existing(idempotency_key) or ReservationResult(
                    status=PurchaseStatus.retry_exhausted,
                    message="Duplicate request raced and neither side won; retry.",
                )
            return ReservationResult(
                status=PurchaseStatus.success,
                reservation_id=reservation.id,
                ticket_id=candidate,
            )
    except Exception:
        give_back(event_id)  # Postgres write failed; hand the unit back
        raise
    finally:
        _release_lock(event_id, token)
