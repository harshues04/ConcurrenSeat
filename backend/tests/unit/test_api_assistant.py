import uuid
from types import SimpleNamespace

import anthropic
import httpx
import pytest
from fastapi.testclient import TestClient

from app.ai import faq_assistant
from app.config import get_settings
from app.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def fake_api_key(monkeypatch):
    monkeypatch.setattr(get_settings(), "anthropic_api_key", "sk-ant-test")


class FakeAnthropicClient:
    def __init__(self, answer="The sale starts soon!"):
        self.calls = []
        self.answer = answer
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        block = SimpleNamespace(type="text", text=self.answer)
        return SimpleNamespace(content=[block])


def test_ask_returns_answer_with_live_context(make_event, monkeypatch):
    event_id = make_event(42)
    fake = FakeAnthropicClient()
    monkeypatch.setattr(faq_assistant, "_get_client", lambda: fake)

    resp = client.post(
        f"/events/{event_id}/ask", json={"question": "When does the sale start?"}
    )

    assert resp.status_code == 200
    assert resp.json() == {"answer": "The sale starts soon!"}
    call = fake.calls[0]
    user_content = call["messages"][0]["content"]
    assert "Tickets remaining right now: 42 of 42" in user_content
    assert "When does the sale start?" in user_content
    assert call["system"] == faq_assistant.SYSTEM_PROMPT


def test_ask_unknown_event_404s():
    resp = client.post(f"/events/{uuid.uuid4()}/ask", json={"question": "Hi?"})
    assert resp.status_code == 404


def test_ask_without_api_key_503s(make_event, monkeypatch):
    monkeypatch.setattr(get_settings(), "anthropic_api_key", "")
    resp = client.post(f"/events/{make_event(1)}/ask", json={"question": "Hi?"})
    assert resp.status_code == 503


def test_ask_empty_question_422s(make_event):
    resp = client.post(f"/events/{make_event(1)}/ask", json={"question": ""})
    assert resp.status_code == 422


def test_anthropic_outage_maps_to_503(make_event, monkeypatch):
    def explode():
        raise anthropic.APIConnectionError(request=httpx.Request("POST", "http://x"))

    fake = FakeAnthropicClient()
    fake.messages = SimpleNamespace(create=lambda **kw: explode())
    monkeypatch.setattr(faq_assistant, "_get_client", lambda: fake)

    resp = client.post(f"/events/{make_event(1)}/ask", json={"question": "Hi?"})

    assert resp.status_code == 503
