"""WebSocket queue-position tests. The lifespan worker is not running here
(TestClient without a `with` block skips lifespan), so the tests drive the
queue by hand and the WS must react."""

from fastapi.testclient import TestClient

from app.main import app
from app.strategies import queue_waiting_room as qwr

client = TestClient(app)


def _ws(event_id, key):
    return client.websocket_connect(f"/ws/queue/{event_id}?idempotency_key={key}")


def test_connect_pushes_current_position_then_resolution(make_event, buyer):
    event_id = make_event(1)
    user_id, key = buyer()
    qwr.enqueue(event_id, user_id, key)

    with _ws(event_id, key) as websocket:
        assert websocket.receive_json() == {"type": "position", "position": 1}

        qwr.drain(event_id)

        resolved = websocket.receive_json()
        assert resolved["type"] == "resolved"
        assert resolved["status"] == "success"
        assert resolved["reservation_id"]


def test_position_updates_as_queue_ahead_drains(make_event, buyer):
    event_id = make_event(1)
    qwr.enqueue(event_id, *buyer())  # someone ahead
    user_id, key = buyer()
    qwr.enqueue(event_id, user_id, key)

    with _ws(event_id, key) as websocket:
        assert websocket.receive_json() == {"type": "position", "position": 2}

        qwr.process_one(event_id)  # the head is served; we move up
        assert websocket.receive_json() == {"type": "position", "position": 1}

        qwr.process_one(event_id)  # we are served; only ticket already gone
        resolved = websocket.receive_json()
        assert resolved["type"] == "resolved"
        assert resolved["status"] == "sold_out"


def test_already_resolved_key_gets_resolved_immediately(make_event, buyer):
    event_id = make_event(1)
    user_id, key = buyer()
    qwr.enqueue(event_id, user_id, key)
    qwr.drain(event_id)

    with _ws(event_id, key) as websocket:
        assert websocket.receive_json()["type"] == "resolved"
