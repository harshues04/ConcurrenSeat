import enum
import uuid
from dataclasses import dataclass
from typing import Protocol


class PurchaseStatus(str, enum.Enum):
    success = "success"
    sold_out = "sold_out"
    retry_exhausted = "retry_exhausted"  # lost the race too many times; client may retry
    queued = "queued"  # waiting room admitted the request into the queue, not yet decided


@dataclass(frozen=True)
class ReservationResult:
    status: PurchaseStatus
    reservation_id: uuid.UUID | None = None
    ticket_id: uuid.UUID | None = None
    queue_position: int | None = None
    message: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status is PurchaseStatus.success


class PurchaseStrategy(Protocol):
    """Common interface: every strategy is swappable behind this call."""

    def attempt_purchase(
        self, event_id: uuid.UUID, user_id: uuid.UUID, idempotency_key: str
    ) -> ReservationResult: ...
