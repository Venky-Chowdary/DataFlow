"""D21 — Confirm did not re-check the source schema it had staged against.

A transfer staged against ``orders(id, name, region)`` and confirmed after
``region`` was dropped ran with the stale mapping and died mid-run on a raw
driver error. Confirm now re-reads the source columns and refuses a stale
approval with a message that says what changed and what to do.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from fastapi import HTTPException


def _make_source(path: Path, *, with_region: bool) -> None:
    conn = sqlite3.connect(str(path))
    cols = "id INTEGER PRIMARY KEY, name TEXT" + (", region TEXT" if with_region else "")
    conn.execute(f"CREATE TABLE orders ({cols})")
    conn.commit()
    conn.close()


@pytest.fixture
def saved_source(tmp_path, monkeypatch):
    import services.connector_store as connectors

    monkeypatch.setattr(connectors, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connectors, "_use_mongo", lambda: False)
    monkeypatch.setattr(connectors, "_resolve_backend", lambda: "file")
    monkeypatch.setattr(connectors, "_backend_choice", "file")
    db = tmp_path / "src.sqlite"
    conn = connectors.create_connector(
        {"name": "d21-src", "type": "sqlite", "database": str(db)}
    )
    return db, conn.id


def _payload(connector_id: str) -> dict:
    return {
        "source": {"kind": "database", "format": "sqlite",
                   "connector_id": connector_id, "table": "orders"},
        "destination": {"connector_id": "dst-1", "table": "orders"},
        "sync_mode": "full_refresh_append",
        "mappings": [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
            {"source": "region", "target": "region"},
        ],
    }


@pytest.fixture
def started(monkeypatch):
    from src.transfer import background, engine

    seen: dict = {}

    class _Engine:
        def _create_pending_job(self, request_obj):
            seen["created"] = True
            return "job-d21"

    monkeypatch.setattr(engine, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr(background, "run_transfer_async", lambda job_id, req: None)
    return seen


def test_confirm_refuses_when_a_mapped_source_column_was_dropped(saved_source, started):
    from src.routers.copilot_router import _start_confirmed_transfer

    db, connector_id = saved_source
    _make_source(db, with_region=False)
    with pytest.raises(HTTPException) as refused:
        asyncio.run(_start_confirmed_transfer(_payload(connector_id)))
    assert refused.value.status_code == 409
    detail = str(refused.value.detail)
    assert "changed since this transfer was staged" in detail
    assert "region" in detail
    assert "Re-stage" in detail
    assert "created" not in started  # no job was created


def test_confirm_proceeds_when_the_source_still_matches(saved_source, started):
    from src.routers.copilot_router import _start_confirmed_transfer

    db, connector_id = saved_source
    _make_source(db, with_region=True)
    out = asyncio.run(_start_confirmed_transfer(_payload(connector_id)))
    assert out["job_id"] == "job-d21"
