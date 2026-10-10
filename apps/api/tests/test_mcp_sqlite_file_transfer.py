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


def _call(client, name: str, arguments: dict) -> tuple[bool, Any]:
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
    if result["isError"]:
        return True, text
    parsed = json.loads(text)
    assert isinstance(parsed, dict)
    return False, parsed


def _mcp(client, name: str, arguments: dict) -> dict:
    failed, payload = _call(client, name, arguments)
    assert failed is False, payload
    return payload


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
    fixtures = Path(__file__).resolve().parent / "fixtures"
    for name in (
        "sample_payments.csv",
        "sample_payments.tsv",
        "sample_hr.json",
        "sample_mixed_types.jsonl",
    ):
        shutil.copy(fixtures / name, uploads / name)
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

    from services.schedule_store import get_schedule

    current = _mcp(client, "get_schedule", {"name": "qe-mcp-payments-nightly"})
    assert current["cron"] == "0 2 * * *"
    schedule_id = current["id"]
    stored = get_schedule(schedule_id)
    assert stored is not None
    mappings_before = list(stored.mappings or [])
    assert mappings_before

    same, same_reason = _call(
        client,
        "update_schedule",
        {"name": "qe-mcp-payments-nightly", "cadence": "daily at 02:00 UTC"},
    )
    assert same is True, same_reason
    assert "already" in same_reason.lower()

    unresolved, question = _call(
        client,
        "update_schedule",
        {"name": "qe-mcp-payments-nightly", "cadence": "whenever"},
    )
    assert unresolved is True, question

    staged_clock = _mcp(
        client,
        "update_schedule",
        {"name": "qe-mcp-payments-nightly", "cadence": "daily at 03:00 UTC"},
    )
    assert staged_clock["requires_confirm"] is True, staged_clock
    assert staged_clock["preview"]["cron_after"] == "0 3 * * *"
    assert "mappings" not in staged_clock["preview"]
    moved = _mcp(client, "confirm_action", {"ack_id": staged_clock["ack_id"], "reason": "qe"})
    assert moved["ok"] is True, moved
    assert moved["idempotent"] is False
    assert moved["schedule_id"] == schedule_id
    assert moved["cron"] == "0 3 * * *"
    assert moved["enabled"] is True
    assert moved["name"] == "qe-mcp-payments-nightly"
    assert moved["next_run_at"] != (stored.next_run_at or "")

    after = get_schedule(schedule_id)
    assert after is not None
    assert after.cron == "0 3 * * *"
    assert after.interval == "daily"
    assert after.mappings == mappings_before
    assert after.source_connector_id == stored.source_connector_id
    assert after.dest_table == stored.dest_table

    replay_clock = _mcp(client, "confirm_action", {"ack_id": staged_clock["ack_id"], "reason": "qe"})
    assert replay_clock["idempotent"] is True
    assert replay_clock["cron"] == "0 3 * * *"
    assert get_schedule(schedule_id).cron == "0 3 * * *"

    staged_hourly = _mcp(
        client,
        "update_schedule",
        {"name": "qe-mcp-payments-nightly", "cadence": "hourly"},
    )
    assert staged_hourly["preview"]["cron_after"] == ""
    hourly = _mcp(client, "confirm_action", {"ack_id": staged_hourly["ack_id"], "reason": "qe"})
    assert hourly["ok"] is True, hourly
    assert hourly["interval"] == "hourly"
    assert hourly["cron"] == ""
    assert hourly["schedule_id"] == schedule_id
    cleared = get_schedule(schedule_id)
    assert cleared is not None
    assert cleared.cron == ""
    assert cleared.interval == "hourly"
    assert cleared.mappings == mappings_before


def _load(client, dataset: str, table: str, dest: str = "qe-mcp-files", **extra) -> str:
    staged = _mcp(
        client,
        "start_dataset_transfer",
        {
            "dataset_name": dataset,
            "dest_connector_name": dest,
            "dest_table": table,
            "sync_mode": "full_refresh_append",
            **extra,
        },
    )
    assert staged["requires_confirm"] is True, staged
    confirmed = _mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "qe"})
    assert confirmed["ok"] is True, confirmed
    job = _wait_job(confirmed["job_id"])
    assert str(job.get("status") or "") == "completed", {
        "dataset": dataset,
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
        "phase": job.get("phase"),
        "rejected": job.get("rejected_rows"),
    }
    return confirmed["job_id"]


def test_mcp_loads_tsv_json_and_jsonl_then_pauses_and_deletes_the_schedule(mcp_client):
    """TSV, JSON, and JSONL through the same MCP door, then pause and delete.

    The TSV is 3 payment rows, the JSON array is 5 employees, and the JSONL
    file is 3 mixed-type rows including a nested object. Pause and delete are
    confirm_action calls against the schedule that was just created.
    """
    client, db_path = mcp_client
    staged = _mcp(
        client,
        "create_connector",
        {
            "name": "qe-mcp-files",
            "type": "sqlite",
            "database": str(db_path),
            "test_first": True,
        },
    )
    saved = _mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "qe"})
    assert saved["ok"] is True, saved

    _load(client, "sample_payments.tsv", "payments_tsv")
    _load(client, "sample_hr", "hr")

    refused, reason = _call(
        client,
        "start_dataset_transfer",
        {
            "dataset_name": "sample_mixed_types",
            "dest_connector_name": "qe-mcp-files",
            "dest_table": "mixed",
            "sync_mode": "full_refresh_append",
        },
    )
    assert refused is True, reason
    assert "created_at" in reason
    assert "naive wall-clock" in reason
    assert "source_timezone" in reason
    unknown, unknown_reason = _call(
        client,
        "start_dataset_transfer",
        {
            "dataset_name": "sample_mixed_types",
            "dest_connector_name": "qe-mcp-files",
            "dest_table": "mixed",
            "sync_mode": "full_refresh_append",
            "source_timezone": "Not/AZone",
        },
    )
    assert unknown is True, unknown_reason
    assert "Not/AZone" in unknown_reason
    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "mixed" not in tables

    _load(client, "sample_mixed_types", "mixed", source_timezone="UTC")

    with sqlite3.connect(db_path) as conn:
        assert conn.execute('SELECT COUNT(*) FROM "payments_tsv"').fetchone()[0] == 3
        assert str(conn.execute('SELECT "CUST_ID" FROM "payments_tsv" ORDER BY "CUST_ID"').fetchone()[0]) == "2001"
        assert conn.execute('SELECT COUNT(*) FROM "hr"').fetchone()[0] == 5
        assert conn.execute('SELECT "emp_id" FROM "hr" ORDER BY "emp_id"').fetchone()[0] == "E001"
        mixed_count = conn.execute('SELECT COUNT(*) FROM "mixed"').fetchone()[0]
        nested = conn.execute('SELECT "metadata_json" FROM "mixed" ORDER BY "row_id"').fetchone()[0]
        declared = {
            row[1]: str(row[2] or "")
            for row in conn.execute('PRAGMA table_info("mixed")')
        }
        stamps = [
            str(row[0])
            for row in conn.execute('SELECT "created_at" FROM "mixed" ORDER BY "row_id"')
        ]
    assert mixed_count == 3
    assert "us-east" in str(nested)
    # SQLite keeps the TIMESTAMPTZ token so the offset round-trips. BINARY
    # lands in BLOB. Anonymous TEXT for either column is a fidelity collapse.
    assert "TIMESTAMPTZ" in declared["created_at"].upper()
    assert declared["payload_b64"].upper() == "BLOB"
    # SQLite stores RFC 3339 UTC with an explicit offset (MXD07). The
    # operator declared UTC, so the wall-clock digits are that instant. The Z
    # on the middle row is the same instant and is not shifted.
    assert stamps == [
        "2024-06-01T12:00:00+00:00",
        "2024-06-02T15:30:00+00:00",
        "2024-06-03T08:00:00+00:00",
    ]

    warehouse = db_path.with_name("hr_warehouse.db")
    dest = _mcp(
        client,
        "create_connector",
        {
            "name": "qe-mcp-hr-warehouse",
            "type": "sqlite",
            "database": str(warehouse),
            "test_first": True,
        },
    )
    assert _mcp(client, "confirm_action", {"ack_id": dest["ack_id"], "reason": "qe"})["ok"] is True

    scheduled = _mcp(
        client,
        "create_schedule",
        {
            "name": "qe-mcp-hr-nightly",
            "source_connector_name": "qe-mcp-files",
            "source_table": "hr",
            "dest_connector_name": "qe-mcp-hr-warehouse",
            "dest_table": "hr_wh",
            "sync_mode": "full_refresh_append",
            "cadence": "daily at 02:00 UTC",
        },
    )
    created = _mcp(client, "confirm_action", {"ack_id": scheduled["ack_id"], "reason": "qe"})
    assert created["ok"] is True, created
    assert created["enabled"] is True

    paused = _mcp(
        client,
        "set_schedule_enabled",
        {"name": "qe-mcp-hr-nightly", "enabled": False},
    )
    assert paused["requires_confirm"] is True, paused
    pause_done = _mcp(client, "confirm_action", {"ack_id": paused["ack_id"], "reason": "qe"})
    assert pause_done["ok"] is True, pause_done
    assert pause_done["enabled"] is False

    from services.schedule_store import get_schedule, list_schedules

    stored = get_schedule(created["schedule_id"])
    assert stored is not None
    assert stored.enabled is False

    removed = _mcp(client, "delete_schedule", {"name": "qe-mcp-hr-nightly"})
    assert removed["requires_confirm"] is True, removed
    deleted = _mcp(client, "confirm_action", {"ack_id": removed["ack_id"], "reason": "qe"})
    assert deleted["ok"] is True, deleted
    assert get_schedule(created["schedule_id"]) is None
    assert list_schedules() == []

    listed = _mcp(client, "list_schedules", {})
    assert listed.get("count") == 0


def test_mcp_file_load_into_existing_sqlite_writes_by_column_name(mcp_client):
    """Dest-exists: reversed column order still lands CUST_ID in CUST_ID.

    The CSV header is CUST_ID, AMT, TXN_DT, ACCT_NO, CCY, REF_NO, STS, DESC.
    The table is created with those columns reversed, and with the carriers
    Map reads from the file, before the load. A positional write would put
    the description in CUST_ID.
    """
    client, db_path = mcp_client
    staged = _mcp(
        client,
        "create_connector",
        {
            "name": "qe-mcp-landed",
            "type": "sqlite",
            "database": str(db_path),
            "test_first": True,
        },
    )
    saved = _mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "qe"})
    assert saved["ok"] is True, saved

    # Declared types match the carriers Map reads from the file (integer,
    # decimal, date, text). The order is the reverse of the CSV header. A
    # TEXT sink for CUST_ID is a lossy coercion and stays blocked; this table
    # is the same shape in a different physical order.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE "landed" (
                "DESC" TEXT,
                "STS" TEXT,
                "REF_NO" TEXT,
                "CCY" TEXT,
                "ACCT_NO" TEXT,
                "TXN_DT" DATE,
                "AMT" DECIMAL(8,2),
                "CUST_ID" BIGINT
            )
            """
        )

    _load(client, "sample_payments", "landed", dest="qe-mcp-landed")

    with sqlite3.connect(db_path) as conn:
        order = [row[1] for row in conn.execute('PRAGMA table_info("landed")')]
        row = conn.execute(
            'SELECT "CUST_ID", "DESC", "AMT" FROM "landed" ORDER BY "CUST_ID"'
        ).fetchone()
        count = conn.execute('SELECT COUNT(*) FROM "landed"').fetchone()[0]
    assert order[0] == "DESC"
    assert order[-1] == "CUST_ID"
    assert count == 10
    assert row is not None
    assert str(row[0]) in {"1001", "1001.0"}
    assert "Wire" in str(row[1])
    assert str(row[2]) in {"1500", "1500.0", "1500.00"}
