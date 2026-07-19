import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from app.strategies.base import PurchaseStatus


class EventOut(BaseModel):
    id: uuid.UUID
    name: str
    total_tickets: int
    tickets_remaining: int
    sale_start_time: datetime

    model_config = {"from_attributes": True}


class PurchaseIn(BaseModel):
    user_id: uuid.UUID
    idempotency_key: str = Field(min_length=1, max_length=255)
    strategy: str | None = None  # optimistic | redis_lock | queue; None = default


class PurchaseOut(BaseModel):
    status: PurchaseStatus
    reservation_id: uuid.UUID | None = None
    ticket_id: uuid.UUID | None = None
    queue_position: int | None = None
    message: str = ""


class ReservationStatusOut(BaseModel):
    id: uuid.UUID
    ticket_id: uuid.UUID
    status: str
    expires_at: datetime | None

    model_config = {"from_attributes": True}


class AskIn(BaseModel):
    question: str = Field(min_length=1, max_length=500)


class AskOut(BaseModel):
    answer: str
