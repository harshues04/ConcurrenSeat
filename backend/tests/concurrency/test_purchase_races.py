"""The tests this project exists for: N concurrent buyers, T tickets, T < N.

Every strategy must yield EXACTLY T successful reservations - never more
(overselling), never fewer (lost inventory) - with no ticket double-assigned,
and duplicate idempotency keys must never consume extra inventory, even when
the duplicates race each other.
"""

import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import func, select

from app.core.redis_client import get_redis
from app.db.session import SessionLocal
from app.models import Event, Reservation, Ticket, TicketStatus
from app.strategies import optimistic_locking, queue_waiting_room, redis_distributed_lock

TICKETS = 100
BUYERS = 500


def _race(fn, n=BUYERS):
    """Run fn(i) for i in range(n) with maximum simultaneity."""
    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(fn, range(n)))


def _assert_sold_out_exactly(event_id, successes):
    """The shared postcondition: DB agrees exactly TICKETS were sold, once each."""
    assert len(successes) == TICKETS, f"expected {TICKETS} winners, got {len(successes)}"
    assert len({r.ticket_id for r in successes}) == TICKETS, "ticket double-assigned!"
    assert len({r.reservation_id for r in successes}) == TICKETS

    with SessionLocal() as db:
        assert db.get(Event, event_id).tickets_remaining == 0
        available = db.scalar(
            select(func.count())
            .select_from(Ticket)
            .where(Ticket.event_id == event_id, Ticket.status == TicketStatus.available)
        )
        assert available == 0, "tickets left available despite sold_out responses"
        reservations = db.scalar(
            select(func.count())
            .select_from(Reservation)
            .join(Ticket, Reservation.ticket_id == Ticket.id)
            .where(Ticket.event_id == event_id)
        )
        assert reservations == TICKETS, "reservation rows != tickets sold"


@pytest.mark.parametrize(
    "strategy",
    [optimistic_locking, redis_distributed_lock],
    ids=["optimistic", "redis_lock"],
)
def test_500_buyers_100_tickets_exactly_100_win(strategy, make_event, monkeypatch):
    # Correctness test, not a latency test: with 100 winners serializing on
    # the per-event lock, the last one legitimately waits longer than the
    # production 2s acquire timeout - don't let that fail the race.
    monkeypatch.setattr(redis_distributed_lock, "LOCK_ACQUIRE_TIMEOUT_SECONDS", 30.0)
    event_id = make_event(TICKETS)

    results = _race(
        lambda i: strategy.attempt_purchase(event_id, uuid.uuid4(), f"race-{uuid.uuid4()}")
    )

    successes = [r for r in results if r.succeeded]
    _assert_sold_out_exactly(event_id, successes)
    assert len(results) == BUYERS  # every buyer got a definitive answer


@pytest.mark.parametrize(
    "strategy",
    [optimistic_locking, redis_distributed_lock],
    ids=["optimistic", "redis_lock"],
)
def test_racing_duplicate_keys_consume_one_ticket_each(strategy, make_event, monkeypatch):
    """250 buyers double-click: 500 racing calls, 250 distinct keys, 100 tickets.
    Duplicates must map to ONE reservation per key, never two."""
    monkeypatch.setattr(redis_distributed_lock, "LOCK_ACQUIRE_TIMEOUT_SECONDS", 30.0)
    event_id = make_event(TICKETS)
    buyers = [(uuid.uuid4(), f"dup-{uuid.uuid4()}") for _ in range(BUYERS // 2)]

    # i and i + 250 are the same buyer firing the same request twice.
    results = _race(
        lambda i: strategy.attempt_purchase(event_id, *buyers[i % len(buyers)])
    )

    by_key = {}
    for i, result in enumerate(results):
        by_key.setdefault(buyers[i % len(buyers)][1], []).append(result)

    winners = set()
    for key, outcomes in by_key.items():
        reservation_ids = {r.reservation_id for r in outcomes if r.succeeded}
        assert len(reservation_ids) <= 1, f"key {key} produced two reservations"
        if reservation_ids:
            winners.add(reservation_ids.pop())
    assert len(winners) == TICKETS

    with SessionLocal() as db:  # DB-level proof: one row per winning key
        ticket_ids = select(Ticket.id).where(Ticket.event_id == event_id)
        assert (
            db.scalar(
                select(func.count())
                .select_from(Reservation)
                .where(Reservation.ticket_id.in_(ticket_ids))
            )
            == TICKETS
        )


def test_queue_strategy_serves_exactly_100_in_fifo_order(make_event):
    event_id = make_event(TICKETS)
    buyers = [(uuid.uuid4(), f"q-{uuid.uuid4()}") for _ in range(BUYERS)]

    queued = _race(lambda i: queue_waiting_room.attempt_purchase(event_id, *buyers[i]))
    assert all(r.status.value == "queued" for r in queued)

    # Arrival order = the queue's truth, whatever thread interleaving produced.
    arrival = get_redis().lrange(queue_waiting_room._queue_key(event_id), 0, -1)
    assert len(arrival) == BUYERS

    assert queue_waiting_room.drain(event_id) == BUYERS

    outcomes = [queue_waiting_room.fetch_result(key) for key in arrival]
    assert all(o is not None for o in outcomes), "someone never got an answer"
    statuses = [o.status.value for o in outcomes]
    # FIFO fairness: the first 100 in line won, everyone later did not -
    # a success after a sold_out would mean the queue jumped someone.
    assert statuses == ["success"] * TICKETS + ["sold_out"] * (BUYERS - TICKETS)

    successes = [o for o in outcomes if o.succeeded]
    _assert_sold_out_exactly(event_id, successes)
