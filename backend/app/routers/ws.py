"""Live queue position over WebSocket — replaces client-side polling.

Protocol (JSON messages, server -> client):
  {"type": "position", "position": <int>}   sent on connect and whenever
                                            your position improves
  {"type": "resolved", "status": ..., "reservation_id": ..., "ticket_id": ...,
   "message": ...}                          terminal; connection closes after

The handler subscribes to the event's "served" pub/sub channel, so pushes are
event-driven (the worker publishes after each admission). A 1s timeout on the
subscription doubles as a fallback tick in case a publish is missed.
"""

import json
import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.redis_client import get_async_redis
from app.strategies import queue_waiting_room as qwr

router = APIRouter()


@router.websocket("/ws/queue/{event_id}")
async def queue_position_ws(
    websocket: WebSocket, event_id: uuid.UUID, idempotency_key: str
) -> None:
    await websocket.accept()
    r = get_async_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(qwr._served_channel(event_id))
    last_sent: int | None = None
    try:
        while True:
            raw = await r.get(qwr._result_key(idempotency_key))
            if raw is not None:
                data = json.loads(raw)
                await websocket.send_json({"type": "resolved", **data})
                break

            index = await r.lpos(qwr._queue_key(event_id), idempotency_key)
            if index is not None and index + 1 != last_sent:
                last_sent = index + 1
                await websocket.send_json({"type": "position", "position": last_sent})

            # Wait for the next "someone was served" signal (or fallback tick).
            await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.aclose()
