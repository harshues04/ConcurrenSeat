import uuid

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.ai import faq_assistant
from app.config import get_settings
from app.db.session import get_db
from app.models import Event
from app.schemas import AskIn, AskOut

router = APIRouter(tags=["assistant"])


@router.post("/events/{event_id}/ask", response_model=AskOut)
def ask_assistant(
    event_id: uuid.UUID, body: AskIn, db: Session = Depends(get_db)
) -> AskOut:
    event = db.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    if not get_settings().anthropic_api_key:
        raise HTTPException(
            status_code=503, detail="FAQ assistant is not configured (no API key)."
        )

    try:
        answer = faq_assistant.ask(
            event_id,
            body.question,
            event_name=event.name,
            sale_start_time=event.sale_start_time,
            tickets_remaining=event.tickets_remaining,
            total_tickets=event.total_tickets,
        )
    except anthropic.RateLimitError:
        raise HTTPException(
            status_code=503, detail="Assistant is busy right now; try again shortly."
        )
    except anthropic.APIConnectionError:
        raise HTTPException(
            status_code=503, detail="Assistant is unreachable; try again shortly."
        )
    except anthropic.APIStatusError:
        raise HTTPException(status_code=502, detail="Assistant request failed.")

    return AskOut(answer=answer)
