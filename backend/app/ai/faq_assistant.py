"""LLM-powered FAQ assistant for event pages.

Scoped narrowly: a tight system prompt plus a small per-request context block
(event name, sale time, live ticket count, how the queue works). The system
prompt is static and comes first; the volatile context rides in the user turn
so it can never poison a cached prefix.
"""

import uuid
from datetime import datetime

import anthropic

from app.config import get_settings

SYSTEM_PROMPT = """You are the FAQ assistant for ConcurrenSeat, a ticket sale platform.
Answer visitor questions about the event and ticket sale using ONLY the context
provided in each message. Keep answers to 2-4 friendly sentences.

If asked something unrelated to the event, tickets, or how the sale works,
politely say you can only help with questions about this ticket sale.
Never invent details (prices, seat numbers, refund policies) that are not in
the context."""

HOW_IT_WORKS = """How the sale works: tickets are limited and sold first-come,
first-served. When traffic is very high, buyers may be placed in a fair FIFO
waiting-room queue; their position updates live and they are admitted
automatically - no need to refresh. Buying is safe against double-charges:
retrying a purchase never creates a second reservation. A reservation holds
the ticket for 5 minutes."""

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=get_settings().anthropic_api_key)
    return _client


def build_context(
    event_name: str, sale_start_time: datetime, tickets_remaining: int, total_tickets: int
) -> str:
    return (
        f"Event: {event_name}\n"
        f"Sale start time: {sale_start_time.isoformat()}\n"
        f"Tickets remaining right now: {tickets_remaining} of {total_tickets}\n\n"
        f"{HOW_IT_WORKS}"
    )


def ask(
    event_id: uuid.UUID,
    question: str,
    *,
    event_name: str,
    sale_start_time: datetime,
    tickets_remaining: int,
    total_tickets: int,
) -> str:
    context = build_context(event_name, sale_start_time, tickets_remaining, total_tickets)
    response = _get_client().messages.create(
        model=get_settings().anthropic_model,
        max_tokens=512,  # deliberately short: this is a widget, not a chatbot
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"<context>\n{context}\n</context>\n\nVisitor question: {question}",
            }
        ],
    )
    return next((b.text for b in response.content if b.type == "text"), "")
