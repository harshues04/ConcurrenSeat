import uuid

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health():
    assert client.get("/health").json() == {"status": "ok"}


def test_get_event_returns_details(make_event):
    event_id = make_event(7)

    resp = client.get(f"/events/{event_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == str(event_id)
    assert body["total_tickets"] == 7
    assert body["tickets_remaining"] == 7


def test_get_missing_event_404s():
    assert client.get(f"/events/{uuid.uuid4()}").status_code == 404
