"""QA check 6 — in-file duplicate keys must not hard-block an upsert.

``QA_R16/dups/dups.csv`` repeats ids 1-10 and has 50 unique keys. The file
scan used to mint ``rc-duplicate-identity`` on ``g9_data_integrity`` before
any job existed, so the writer never got to collapse the copies.

This goes through ``start_transfer`` (preflight, then confirm, then the job).
Append/overwrite on the same file still refuses the run.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

PG = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "dataflow",
    "password": "dataflow",
    "dbname": "dataflow",
}


def _require_postgres():
    try:
        import psycopg2
    except ImportError as exc:
        pytest.fail(f"Postgres driver is required for the upsert duplicate e2e: {exc}")
    try:
        conn = psycopg2.connect(**PG)
    except Exception as exc:
        pytest.fail(f"Postgres is required for the upsert duplicate e2e: {exc}")
    conn.autocommit = True
    return conn


def _csv_text() -> str:
    lines = ["id,name"]
    for i in range(1, 51):
        lines.append(f"{i},a{i}")
    for i in range(1, 11):
        lines.append(f"{i},b{i}")
    return "\n".join(lines) + "\n"


def _stage_file(server) -> None:
    if server is None:
        pytest.fail("In-process SFTP is required to stage the duplicate file")
    path = Path(server.local_path("/QA_R16/dups/dups.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_csv_text(), encoding="utf-8")


def _advisory(preflight: dict) -> dict:
    for item in preflight.get("warnings") or []:
        if not isinstance(item, dict):
            continue
        text = str(item.get("message") or "")
        if "counted once" in text.lower() and "collapsed" in text.lower():
            return item
    pytest.fail(f"upsert preflight has no collapse advisory: {preflight.get('warnings')}")


def _wait_job(job_id: str, timeout_s: float = 120.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    last: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        last = job
        if str(job.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return last


@pytest.fixture
def transfer_client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)

    with TestClient(app) as client:
        yield client


def _connectors(server) -> tuple[str, str]:
    from services.connector_store import create_connector

    cfg = server.endpoint_config("/QA_R16/dups/dups.csv")
    token = uuid.uuid4().hex[:8]
    source = create_connector(
        {
            "name": f"qa-r16-dups-sftp-{token}",
            "type": "sftp",
            "role": "source",
            "host": cfg["host"],
            "port": cfg["port"],
            "database": cfg["database"],
            "username": cfg["username"],
            "password": cfg["password"],
            "host_key": server.host_key,
            "ssl": False,
        }
    )
    dest = create_connector(
        {
            "name": f"qa-r16-dups-pg-{token}",
            "type": "postgresql",
            "role": "destination",
            "host": PG["host"],
            "port": PG["port"],
            "database": PG["dbname"],
            "username": PG["user"],
            "password": PG["password"],
            "schema": "public",
            "ssl": False,
        }
    )
    return source.id, dest.id


def test_start_transfer_upsert_collapses_infile_duplicates(
    transfer_client, local_sftp
) -> None:
    _stage_file(local_sftp)
    pg = _require_postgres()
    try:
        with pg.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS public.qa_r16_dups2')
    finally:
        pg.close()

    from src.ai.copilot.transfer_tools import start_transfer

    source_id, dest_id = _connectors(local_sftp)
    staged = start_transfer(
        source_connector_id=source_id,
        source_table="dups.csv",
        dest_connector_id=dest_id,
        dest_table="qa_r16_dups2",
        sync_mode="incremental_upsert",
        primary_key="id",
        schema_policy="propagate_columns",
        validation_mode="strict",
    )
    assert staged.success, staged.error
    preflight = (staged.output.get("plan") or {}).get("preflight") or {}
    assert preflight.get("passed") is True, staged.error or preflight
    blockers = preflight.get("blockers") or []
    blob = " ".join(str(item.get("id") or "") + " " + str(item.get("message") or "") for item in blockers)
    assert "rc-duplicate-identity" not in blob
    advisory = _advisory(preflight)
    assert advisory["blocks_transfer"] is False
    assert advisory["gate"] == "g9_data_integrity"
    message = str(advisory.get("message") or "")
    assert "5 extra" not in message, message
    assert "10 extra" in message, message
    assert "50 unique" in message, message
    assert "60 scanned" in message, message

    confirmed = transfer_client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "qa-r16-dups"},
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    job_id = str(body.get("job_id") or "")
    assert job_id

    job = _wait_job(job_id)
    assert str(job.get("status") or "") == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
        "phase": job.get("phase"),
    }

    pg = _require_postgres()
    try:
        with pg.cursor() as cur:
            cur.execute('SELECT id, name FROM public.qa_r16_dups2 ORDER BY id')
            rows = [(int(row[0]), str(row[1])) for row in cur.fetchall()]
    finally:
        pg.close()
    assert len(rows) == 50
    assert [row[0] for row in rows] == list(range(1, 51))
    assert dict(rows[:10]) == {i: f"b{i}" for i in range(1, 11)}


def test_start_transfer_overwrite_still_blocks_infile_duplicates(
    transfer_client, local_sftp
) -> None:
    _stage_file(local_sftp)
    _require_postgres().close()

    from src.ai.copilot.transfer_tools import start_transfer

    source_id, dest_id = _connectors(local_sftp)
    staged = start_transfer(
        source_connector_id=source_id,
        source_table="dups.csv",
        dest_connector_id=dest_id,
        dest_table="qa_r16_dups2_overwrite",
        sync_mode="full_refresh_overwrite",
        primary_key="id",
        schema_policy="propagate_columns",
        validation_mode="strict",
    )
    assert staged.success is False, staged.output
    assert "rc-duplicate-identity" in (staged.error or "")
    assert not (staged.output or {}).get("ack_id")
