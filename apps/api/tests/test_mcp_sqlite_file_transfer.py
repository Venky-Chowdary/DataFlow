"""MCP loads a real CSV into a SQLite file and counts the rows back.

Named fixture: ``tests/fixtures/sample_payments.csv`` (10 rows). The destination
is a SQLite file under ``DATAFLOW_SQLITE_ROOT``. The call is the streamable MCP
endpoint. Preflight, mapping, and the transfer engine are the real ones.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import time
from pathlib import Path

import pytest

from services.platform_config import data_dir


def _digest(path: Path) -> bytes | None:
    if not path.exists():
        return None
    return path.read_bytes()


def _mcp(client, name: str, arguments: dict) -> dict:
    response = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "error" not in body, body
    result = body["result"]
    text = result["content"][0]["text"]
    assert result["isError"] is False, text
    parsed = json.loads(text)
    assert isinstance(parsed, dict)
    return parsed


def _wait_job(job_id: str, timeout_s: float = 90.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    last: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        last = job
        status = str(job.get("status") or "")
        if status in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return last


@pytest.fixture
def mcp_client(tmp_path, monkeypatch):
    real = data_dir()
    before = {name: _digest(real / name) for name in ("connectors.json", "schedules.json", "pilot_acks.json")}
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    shutil.copy(Path(__file__).resolve().parent / "fixtures" / "sample_payments.csv", uploads / "sample_payments.csv")
    sqlite_root = tmp_path / "sqlite"
    sqlite_root.mkdir()

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(uploads))
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(sqlite_root))
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.schedule_store as schedule_store
    import src.ai.copilot.ack_ledger as ack_mod
    import src.ai.copilot.data_analyst as data_analyst
    from services.mongodb_service import MemoryMongoDBService
    from src.ai.training.universal_data_feeder import UniversalDataFeeder
    from src.main import app
    from fastapi.testclient import TestClient

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    analyst = data_analyst.CopilotDataAnalyst()
    analyst.feeder = UniversalDataFeeder([str(uploads)])
    monkeypatch.setattr(data_analyst, "_analyst", analyst)

    with TestClient(app) as client:
        yield client, sqlite_root / "payments.db"

    after = {name: _digest(real / name) for name in ("connectors.json", "schedules.json", "pilot_acks.json")}
    assert after == before


def test_mcp_loads_sample_payments_into_sqlite(mcp_client):
    client, db_path = mcp_client
    staged = _mcp(
        client,
        "create_connector",
        {
            "name": "qe-mcp-sqlite",
            "type": "sqlite",
            "database": str(db_path),
            "test_first": True,
        },
    )
    assert staged["requires_confirm"] is True
    assert staged["ack_id"]
    saved = _mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "qe"})
    assert saved["ok"] is True, saved
    assert saved["idempotent"] is False
    assert saved["name"] == "qe-mcp-sqlite"
    assert db_path.is_file()

    transfer = _mcp(
        client,
        "start_dataset_transfer",
        {
            "dataset_name": "sample_payments",
            "dest_connector_name": "qe-mcp-sqlite",
            "dest_table": "payments",
            "sync_mode": "full_refresh_append",
        },
    )
    assert transfer["requires_confirm"] is True, transfer
    assert transfer["preview"]["rows"] == 10
    assert transfer["preview"]["file"] == "sample_payments.csv"

    confirmed = _mcp(client, "confirm_action", {"ack_id": transfer["ack_id"], "reason": "qe"})
    assert confirmed["ok"] is True, confirmed
    assert confirmed["idempotent"] is False
    job_id = confirmed["job_id"]
    assert confirmed["source"] == "sample_payments.csv"

    job = _wait_job(job_id)
    assert str(job.get("status") or "") == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
        "phase": job.get("phase"),
    }

    with sqlite3.connect(db_path) as conn:
        count = conn.execute('SELECT COUNT(*) FROM "payments"').fetchone()[0]
        cust = conn.execute('SELECT "CUST_ID" FROM "payments" ORDER BY "CUST_ID"').fetchone()[0]
    assert count == 10
    assert str(cust) in {"1001", "1001.0"}

    replay = _mcp(client, "confirm_action", {"ack_id": transfer["ack_id"], "reason": "qe"})
    assert replay["idempotent"] is True
    assert replay["job_id"] == job_id
    with sqlite3.connect(db_path) as conn:
        assert conn.execute('SELECT COUNT(*) FROM "payments"').fetchone()[0] == 10

    warehouse = db_path.with_name("warehouse.db")
    staged_dest = _mcp(
        client,
        "create_connector",
        {
            "name": "qe-mcp-sqlite-warehouse",
            "type": "sqlite",
            "database": str(warehouse),
            "test_first": True,
        },
    )
    saved_dest = _mcp(client, "confirm_action", {"ack_id": staged_dest["ack_id"], "reason": "qe"})
    assert saved_dest["ok"] is True, saved_dest

    scheduled = _mcp(
        client,
        "create_schedule",
        {
            "name": "qe-mcp-payments-nightly",
            "source_connector_name": "qe-mcp-sqlite",
            "source_table": "payments",
            "dest_connector_name": "qe-mcp-sqlite-warehouse",
            "dest_table": "payments_wh",
            "sync_mode": "full_refresh_append",
            "cadence": "daily at 02:00 UTC",
        },
    )
    assert scheduled["requires_confirm"] is True, scheduled
    created = _mcp(client, "confirm_action", {"ack_id": scheduled["ack_id"], "reason": "qe"})
    assert created["ok"] is True, created
    assert created["enabled"] is True
    assert created["name"] == "qe-mcp-payments-nightly"

    run = _mcp(client, "run_schedule_now", {"name": "qe-mcp-payments-nightly"})
    assert run["requires_confirm"] is True, run
    started = _mcp(client, "confirm_action", {"ack_id": run["ack_id"], "reason": "qe"})
    assert started["ok"] is True, started
    sched_job = _wait_job(started["job_id"])
    assert str(sched_job.get("status") or "") == "completed", {
        "status": sched_job.get("status"),
        "error": sched_job.get("error") or sched_job.get("message"),
        "phase": sched_job.get("phase"),
    }
    with sqlite3.connect(warehouse) as conn:
        assert conn.execute('SELECT COUNT(*) FROM "payments_wh"').fetchone()[0] == 10

    replay_run = _mcp(client, "confirm_action", {"ack_id": run["ack_id"], "reason": "qe"})
    assert replay_run["idempotent"] is True
    assert replay_run["job_id"] == started["job_id"]
    with sqlite3.connect(warehouse) as conn:
        assert conn.execute('SELECT COUNT(*) FROM "payments_wh"').fetchone()[0] == 10
