"""Tests for Strategy B's Redis inventory gate (Lua check-and-decrement)."""

import uuid
from concurrent.futures import ThreadPoolExecutor

from app.core.redis_client import get_redis
from app.strategies import redis_distributed_lock as rdl


def test_counter_initializes_from_postgres(make_event):
    event_id = make_event(5)

    assert rdl.try_decrement(event_id) == 4
    assert get_redis().get(rdl._counter_key(event_id)) == "4"


def test_decrements_to_sold_out_and_stays_sold_out(make_event):
    event_id = make_event(2)

    assert rdl.try_decrement(event_id) == 1
    assert rdl.try_decrement(event_id) == 0
    assert rdl.try_decrement(event_id) == rdl.SOLD_OUT
    assert rdl.try_decrement(event_id) == rdl.SOLD_OUT


def test_unknown_event_is_sold_out():
    assert rdl.try_decrement(uuid.uuid4()) == rdl.SOLD_OUT


def test_give_back_restores_inventory(make_event):
    event_id = make_event(1)

    assert rdl.try_decrement(event_id) == 0
    assert rdl.try_decrement(event_id) == rdl.SOLD_OUT
    rdl.give_back(event_id)
    assert rdl.try_decrement(event_id) == 0


def test_gate_never_oversells_under_thread_race(make_event):
    tickets, buyers = 10, 80
    event_id = make_event(tickets)

    with ThreadPoolExecutor(max_workers=buyers) as pool:
        results = list(pool.map(lambda _: rdl.try_decrement(event_id), range(buyers)))

    wins = [r for r in results if r >= 0]
    assert len(wins) == tickets
    assert results.count(rdl.SOLD_OUT) == buyers - tickets
    # Every successful decrement saw a distinct remaining-count: proof that
    # no two buyers consumed the same unit.
    assert sorted(wins) == list(range(tickets))
