import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import assistant, bookings, events, ws
from app.strategies import queue_waiting_room as qwr


@asynccontextmanager
async def lifespan(app: FastAPI):
    # The waiting-room admission worker lives and dies with the app.
    stop = asyncio.Event()
    worker = asyncio.create_task(qwr.worker_loop_all(stop))
    yield
    stop.set()
    await worker


app = FastAPI(title="ConcurrenSeat", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in get_settings().cors_origins.split(",") if o.strip()],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(events.router)
app.include_router(bookings.router)
app.include_router(ws.router)
app.include_router(assistant.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
