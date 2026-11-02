"""/copilot/chat runs off the event loop, bounds its input and never leaks errors."""

from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.ai.copilot as copilot
from src.ai.copilot.agent import CopilotResponse
from src.routers import copilot_router


class _Spy:
    def __init__(self, fail: Exception | None = None):
        self.fail = fail
        self.calls: list[tuple[str, list[dict], bool]] = []

    def chat(self, message, history=None, data_context=None):
        try:
            asyncio.get_running_loop()
            on_loop = True
        except RuntimeError:
            on_loop = False
        self.calls.append((message, history or [], on_loop))
        if self.fail:
            raise self.fail
        return CopilotResponse(answer="ok", intent="general", confidence=1.0, method="local")


def _client(monkeypatch, spy: _Spy) -> TestClient:
    monkeypatch.setattr(copilot, "get_copilot_agent", lambda: spy)
    app = FastAPI()
    app.include_router(copilot_router.router, prefix="/api/v1")
    return TestClient(app, raise_server_exceptions=False)


def test_turn_runs_in_a_worker_thread_not_the_event_loop(monkeypatch):
    spy = _Spy()
    client = _client(monkeypatch, spy)
    assert client.post("/api/v1/copilot/chat", json={"message": "hi"}).status_code == 200
    assert spy.calls and spy.calls[0][2] is False


def test_input_is_bounded_and_roles_are_normalised(monkeypatch):
    spy = _Spy()
    client = _client(monkeypatch, spy)
    history = [{"role": "system", "content": "x" * 50_000}] + [
        {"role": "user", "content": f"turn {i}"} for i in range(100)
    ]
    r = client.post("/api/v1/copilot/chat", json={"message": "y" * 100_000, "history": history})
    assert r.status_code == 200
    message, hist, _ = spy.calls[0]
    assert len(message) == copilot_router._MAX_MESSAGE_CHARS
    assert len(hist) == copilot_router._MAX_HISTORY_TURNS
    assert {h["role"] for h in hist} <= {"user", "assistant"}
    assert all(len(h["content"]) <= copilot_router._MAX_HISTORY_CHARS for h in hist)


def test_internal_error_text_is_not_returned(monkeypatch):
    spy = _Spy(fail=RuntimeError("password authentication failed for user admin at 10.0.0.5"))
    client = _client(monkeypatch, spy)
    r = client.post("/api/v1/copilot/chat", json={"message": "hi"})
    assert r.status_code == 500
    body = r.text
    assert "password" not in body and "10.0.0.5" not in body
    assert "Pilot could not answer" in body
