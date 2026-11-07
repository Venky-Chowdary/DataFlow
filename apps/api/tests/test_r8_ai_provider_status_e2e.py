"""Settings -> AI Models: Ollama status reflects reachability (QA enterprise #42).

QA (production 557f556c): with no Ollama server running,
``GET /workspace/ai-providers`` said ``configured: true, key_state: ready`` and
``POST /workspace/ai-providers/ollama/test`` answered "ollama has no cloud key
to test". Root cause: ``integrations_store.provider_key_state`` returned
``"ready"`` for Ollama unconditionally, ``configured`` was ``... or provider ==
"ollama"``, and the test route only knew cloud keys.
"""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from r8_ent_harness import ent  # noqa: F401


class _Tags(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        body = json.dumps({"models": [{"name": "llama3.2:latest"}]}).encode()
        self.send_response(200 if self.path == "/api/tags" else 404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        return


@pytest.fixture
def fake_ollama():
    server = HTTPServer(("127.0.0.1", 0), _Tags)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()


def _closed_port_url() -> str:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}"


@pytest.fixture(autouse=True)
def _no_env_ollama(monkeypatch):
    from services import integrations_store

    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    monkeypatch.delenv("OLLAMA_MODEL", raising=False)
    integrations_store._HYDRATED_ENV.discard("OLLAMA_BASE_URL")
    integrations_store._HYDRATED_ENV.discard("OLLAMA_MODEL")
    getattr(integrations_store, "_OLLAMA_PROBE", {}).clear()
    yield


def _ollama(ent) -> dict:
    res = ent.admin.get("/api/v1/workspace/ai-providers")
    assert res.status_code == 200, res.text
    return res.json()["providers"]["ollama"]


def test_offline_ollama_is_not_ready_and_test_says_why(ent):
    dead = _closed_port_url()
    assert ent.admin.patch("/api/v1/workspace/ai-providers/ollama", json={"base_url": dead}).status_code == 200
    row = _ollama(ent)
    assert row["configured"] is False
    assert row["key_state"] == "unreachable"

    tested = ent.admin.post("/api/v1/workspace/ai-providers/ollama/test")
    assert tested.status_code == 200, tested.text
    assert tested.json()["ok"] is False
    assert "not reachable" in tested.json()["error"]


def test_reachable_ollama_is_ready_and_test_checks_the_model(ent, fake_ollama):
    patched = ent.admin.patch(
        "/api/v1/workspace/ai-providers/ollama", json={"base_url": fake_ollama, "model": "llama3.2"}
    )
    assert patched.status_code == 200, patched.text
    row = _ollama(ent)
    assert (row["configured"], row["key_state"]) == (True, "ready")

    ok = ent.admin.post("/api/v1/workspace/ai-providers/ollama/test").json()
    assert ok["ok"] is True, ok
    assert "llama3.2:latest" in ok["models"]

    ent.admin.patch("/api/v1/workspace/ai-providers/ollama", json={"model": "mistral"})
    missing = ent.admin.post("/api/v1/workspace/ai-providers/ollama/test").json()
    assert missing["ok"] is False
    assert "not pulled" in missing["error"]

    # A base URL changed after the first save takes effect (it used to stick in env).
    ent.admin.patch("/api/v1/workspace/ai-providers/ollama", json={"base_url": _closed_port_url()})
    assert _ollama(ent)["key_state"] == "unreachable"
