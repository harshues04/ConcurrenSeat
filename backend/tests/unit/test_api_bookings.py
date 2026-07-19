import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.strategies import queue_waiting_room as qwr

client = TestClient(app)


def _purchase(event_id, strategy=None, key=None, user_id=None):
    return client.post(
        f"/events/{event_id}/purchase",
        json={
            "user_id": str(user_id or uuid.uuid4()),
            "idempotency_key": key or f"api-{uuid.uuid4()}",
            "strategy": strategy,
        },
    )


@pytest.mark.parametrize("strategy", ["optimistic", "redis_lock"])
def test_purchase_success_201(strategy, make_event):
    resp = _purchase(make_event(3), strategy)

    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "success"
    assert body["reservation_id"]


def test_purchase_queue_strategy_202_with_position(make_event):
    resp = _purchase(make_event(3), "queue")

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert body["queue_position"] == 1


def test_purchase_sold_out_409(make_event):
    event_id = make_event(1)
    assert _purchase(event_id, "optimistic").status_code == 201

    resp = _purchase(event_id, "optimistic")

    assert resp.status_code == 409
    assert resp.json()["status"] == "sold_out"


def test_unknown_strategy_422(make_event):
    resp = _purchase(make_event(1), "pessimistic")

    assert resp.status_code == 422
    assert "pessimistic" in resp.json()["detail"]


def test_purchase_missing_event_404():
    assert _purchase(uuid.uuid4(), "optimistic").status_code == 404


def test_default_strategy_used_when_omitted(make_event):
    resp = _purchase(make_event(1))  # config default is "optimistic"

    assert resp.status_code == 201


def test_duplicate_purchase_returns_same_reservation(make_event):
    event_id = make_event(5)
    key = f"api-dup-{uuid.uuid4()}"
    user_id = uuid.uuid4()

    first = _purchase(event_id, "optimistic", key, user_id)
    second = _purchase(event_id, "optimistic", key, user_id)

    assert first.json()["reservation_id"] == second.json()["reservation_id"]


def test_reservation_status_roundtrip(make_event):
    event_id = make_event(1)
    reservation_id = _purchase(event_id, "optimistic").json()["reservation_id"]

    resp = client.get(f"/reservations/{reservation_id}/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == reservation_id
    assert body["status"] == "pending"
    assert body["expires_at"] is not None


def test_reservation_status_missing_404():
    assert client.get(f"/reservations/{uuid.uuid4()}/status").status_code == 404


def test_queued_request_resolves_after_worker_serves(make_event):
    event_id = make_event(2)
    key = f"api-q-{uuid.uuid4()}"
    user_id = uuid.uuid4()
    assert _purchase(event_id, "queue", key, user_id).status_code == 202

    qwr.drain(event_id)
    resp = _purchase(event_id, "queue", key, user_id)

    assert resp.status_code == 201
    assert resp.json()["status"] == "success"
