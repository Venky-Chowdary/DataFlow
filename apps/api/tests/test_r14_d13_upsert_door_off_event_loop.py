"""D13 review 2: the upsert key door never blocks the event loop.

00f246a6c called assert_upsert_key_resolvable — which may read the source
catalog for up to PROBE_SECONDS (8 s) — synchronously inside the async
POST/PATCH /schedules and /transfer/execute handlers, so every other request
on that worker waited behind it. The door now runs in a worker thread.
A cheap request fired while a 1.5 s door is running must finish before it.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from tests import r4_route_harness as h


_DOOR: dict = {}


@pytest.fixture
def app(tmp_path, monkeypatch):
    import threading

    h.isolate(tmp_path, monkeypatch)
    import services.upsert_key_door as door

    _DOOR.clear()
    _DOOR["entered"] = threading.Event()

    def _slow(**_kw):
        _DOOR["entered"].set()
        time.sleep(1.5)
        _DOOR["ended_at"] = time.monotonic()

    monkeypatch.setattr(door, "assert_upsert_key_resolvable", _slow)
    from src.main import app as fastapi_app

    return fastapi_app


def _quick_done_before_the_door_ends(app, method: str, path: str, body: dict) -> bool:
    """Fire a cheap request once the slow one is inside the door. On a blocked
    loop this coroutine cannot even observe the entry until the door returns,
    so the cheap request finishes after it; off the loop it finishes during it."""
    import httpx

    async def _run() -> bool:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            slow = asyncio.create_task(client.request(method, path, json=body))
            deadline = time.monotonic() + 10
            while not _DOOR["entered"].is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert _DOOR["entered"].is_set(), "the request never reached the door"
            quick = await client.get("/api/v1/schedules/intervals")
            quick_done = time.monotonic()
            assert quick.status_code == 200, quick.text
            await slow
            return quick_done < _DOOR["ended_at"]

    return asyncio.run(_run())


def test_schedule_create_door_runs_off_the_event_loop(app):
    from services import connector_store

    # Real connectors: a schedule's connector ids are ownership-checked (review 3).
    src, dst = (connector_store.create_connector({"name": n, "type": "postgresql", "host": "h", "database": "d"}).id
                for n in ("door-s", "door-d"))
    assert _quick_done_before_the_door_ends(app, "POST", "/api/v1/schedules/", {
        "name": "door", "source_connector_id": src, "source_table": "t",
        "dest_connector_id": dst, "dest_table": "o", "sync_mode": "upsert",
        "mappings": [{"source": "id", "target": "id", "confidence": 0.99}], "enabled": False,
    })


def test_transfer_execute_door_runs_off_the_event_loop(app):
    assert _quick_done_before_the_door_ends(app, "POST", "/api/v1/transfer/execute", {
        "source": {"kind": "database", "format": "postgresql", "host": "127.0.0.1", "table": "t"},
        "destination": {"kind": "database", "format": "postgresql", "host": "127.0.0.1", "table": "o"},
        "sync_mode": "upsert",
    })
