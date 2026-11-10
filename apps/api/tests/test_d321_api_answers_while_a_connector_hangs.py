"""D321: one silent connector host must not take the whole /api/v1 backend down.

Production (543c995cbcba, Nov 10 10:32-10:49 ET): every /api/v1 route
(health, version, connectors, MCP list_jobs) timed out for 15 minutes while
the web service's static /health stayed green; only a process restart ended it.

Reproduced here against a black hole (a TCP listener that accepts and never
answers) with python-oracledb, which has no handshake timeout:

* ``POST /api/v1/connectors/test`` was ``async def`` and called the blocking
  probe on the event loop. The loop stopped, so ``/api/v1/health`` (and every
  other route) waited until the far end closed.
* MCP ``create_connector`` (``test_first`` is the default) probed the new host
  with no deadline on the MCP request thread (the loop's default executor).
  A few of them held every thread for good and every MCP call, list_jobs
  included, queued behind them; the connector pool and /health never saw them.

The black hole is closed after ``RELEASE_AFTER`` seconds so the pre-fix code
fails on latency instead of hanging the suite.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import socket
import threading
import time

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

pytest.importorskip("oracledb")
httpx = pytest.importorskip("httpx")

#: Lowered for a quick test, never raised.
DEADLINE = 2
#: When the black hole lets go: the pre-fix loop stays blocked until then.
RELEASE_AFTER = 7.0
#: An answer the operator would call "the API is up".
FAST = 1.0
#: The loop's default executor (MCP request threads), kept small and fixed.
MCP_THREADS = 3


class _BlackHole:
    """Accepts TCP connections and never sends a byte; ``close`` lets go."""

    def __init__(self) -> None:
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(64)
        self.server.settimeout(0.1)
        self.port = self.server.getsockname()[1]
        self.held: list[socket.socket] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self.server.accept()
                self.held.append(conn)
            except OSError:
                continue

    def close(self) -> None:
        self._stop.set()
        self._thread.join(2)
        for conn in self.held:
            conn.close()
        self.server.close()


@pytest.fixture
def silent_host(monkeypatch):
    from services import connector_executor

    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.delenv("DATAFLOW_CONNECTOR_POOL_WORKERS", raising=False)
    monkeypatch.delenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", raising=False)
    connector_executor.reset_for_tests()
    hole = _BlackHole()
    timer = threading.Timer(RELEASE_AFTER, hole.close)
    timer.start()
    try:
        yield hole
    finally:
        timer.cancel()
        hole.close()
        # Nothing outlives the test: every connector worker ends once the
        # far end is gone.
        end = time.monotonic() + 20
        while time.monotonic() < end and connector_executor.snapshot()["in_flight"]:
            time.sleep(0.1)
        left = connector_executor.snapshot()["in_flight"]
        connector_executor.reset_for_tests()
        assert left == 0, f"{left} connector workers still running after the test"


def _oracle(port: int) -> dict:
    return {
        "type": "oracle",
        "host": "127.0.0.1",
        "port": port,
        "database": "ORCL",
        "username": "u",
        "password": "p",
    }


def _mcp(name: str, arguments: dict) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }


async def _timed(coro) -> float:
    started = time.monotonic()
    response = await coro
    assert response.status_code < 500, response.text
    return time.monotonic() - started


async def _api_latencies(client) -> dict[str, float]:
    return {
        "health": await _timed(client.get("/api/v1/health")),
        "connectors": await _timed(client.get("/api/v1/connectors/saved")),
        "list_jobs": await _timed(client.post("/api/v1/mcp", json=_mcp("list_jobs", {}))),
    }


def _run(main) -> object:
    async def wrapper():
        loop = asyncio.get_running_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=MCP_THREADS)
        loop.set_default_executor(executor)
        try:
            return await main()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    return asyncio.run(wrapper())


def test_api_answers_while_a_connector_test_waits_on_a_silent_host(silent_host):
    from src.main import app

    async def main():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://api", timeout=60) as client:
            hung = asyncio.create_task(
                client.post("/api/v1/connectors/test", json=_oracle(silent_host.port))
            )
            issued = time.monotonic()
            await asyncio.sleep(0.5)
            # A blocked event loop shows first as a late wake-up.
            late = time.monotonic() - issued - 0.5
            during = {"event_loop": late, **await _api_latencies(client)}
            answer = await hung
            return during, answer

    during, answer = _run(main)
    assert max(during.values()) < FAST, during
    body = answer.json()
    assert body["success"] is False
    # The deadline answer names the silent endpoint.
    assert f"127.0.0.1:{silent_host.port}" in body["message"], body


def test_mcp_keeps_answering_while_create_connector_probes_a_silent_host(silent_host):
    from src.main import app

    async def main():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://api", timeout=60) as client:
            creates = [
                asyncio.create_task(
                    client.post(
                        "/api/v1/mcp",
                        json=_mcp(
                            "create_connector",
                            {"name": f"d321-silent-{i}", "test_first": True, **_oracle(silent_host.port)},
                        ),
                    )
                )
                for i in range(MCP_THREADS)
            ]
            # Every MCP request thread is taken by a probe; past their deadline
            # the threads must be free again.
            issued = time.monotonic()
            await asyncio.sleep(DEADLINE + 1.5)
            late = time.monotonic() - issued - (DEADLINE + 1.5)
            after = {"event_loop": late, **await _api_latencies(client)}
            answers = await asyncio.gather(*creates)
            return after, answers

    after, answers = _run(main)
    assert max(after.values()) < FAST, after
    for response in answers:
        result = response.json()["result"]
        assert result["isError"] is True
        assert f"127.0.0.1:{silent_host.port}" in result["content"][0]["text"], result
