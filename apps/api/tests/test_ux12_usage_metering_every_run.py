"""UX-12: "Rows written · today" read 0 after 15,000 rows.

The usage meter sat in one success branch of the engine and its failures
were logged at debug, so finished runs went unmetered and /usage/summary
answered zeros. The meter is now one call where every tracked run ends:
a successful run records the rows it wrote, a failed run records nothing,
and a metering failure is a warning that never fails the transfer.
Live PostgreSQL -> PostgreSQL through ``execute_tracked``.
"""

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace

import pytest

from services import usage_metering as um


@pytest.fixture
def meter(tmp_path, monkeypatch):
    monkeypatch.setattr(um, "STORE_PATH", tmp_path / "usage_metering.json")
    monkeypatch.setattr(um, "_mongo", lambda: None)
    return tmp_path / "usage_metering.json"


def _pg(table: str, schema: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT") or 5432),
        database=os.environ.get("PGDATABASE", "dataflow"),
        username=os.environ.get("PGUSER", "dataflow"),
        password=os.environ.get("PGPASSWORD", "dataflow"),
        schema=schema,
        table=table,
    )


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT") or 5432),
            dbname=os.environ.get("PGDATABASE", "dataflow"),
            user=os.environ.get("PGUSER", "dataflow"),
            password=os.environ.get("PGPASSWORD", "dataflow"),
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


def test_a_finished_run_is_metered_for_its_workspace(meter):
    from src.transfer.adapters import write_destination_database
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest
    from tests.sync_mode_probe import COLUMNS, MAPPINGS, RECORDS, SCHEMA, stream_contract

    conn = _pg_conn()
    schema = f"w4ux_um_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
    try:
        source = _pg("src", schema)
        write_destination_database(source, RECORDS, COLUMNS, SCHEMA, MAPPINGS)
        workspace = f"ws-{uuid.uuid4().hex[:6]}"
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=source,
                destination=_pg("dst", schema),
                sync_mode="full_refresh_append",
                skip_preflight=False,
                validation_mode="balanced",
                stream_contracts=[stream_contract("src", "full_refresh_append")],
                mappings=list(MAPPINGS),
                workspace_id=workspace,
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success, result.error
        summary = um.summarize_usage(workspace, days=1)
        assert summary["rows_written"] == len(RECORDS), summary
        assert summary["event_count"] == 1
        assert summary["daily"][-1]["rows_written"] == len(RECORDS)
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_a_failed_run_is_not_metered(meter):
    request = SimpleNamespace(workspace_id="w", source=SimpleNamespace(format="postgresql"), destination=SimpleNamespace(format="mysql"))
    assert um.record_job_usage(request, SimpleNamespace(success=False, job_id="j", records_transferred=9)) == ""
    assert um.summarize_usage("w")["event_count"] == 0


def test_a_metering_failure_is_a_warning_not_a_failed_transfer(meter, monkeypatch, caplog):
    def broken(**_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(um, "record_transfer_usage", broken)
    request = SimpleNamespace(workspace_id="w", source=SimpleNamespace(format="x"), destination=SimpleNamespace(format="y"))
    with caplog.at_level("WARNING", logger=um.__name__):
        assert um.record_job_usage(request, SimpleNamespace(success=True, job_id="j1", records_transferred=3)) == ""
    assert any("Usage metering failed for job j1" in r.getMessage() for r in caplog.records)


def test_the_engine_meters_once_where_every_run_ends():
    from pathlib import Path

    engine = (Path(__file__).resolve().parents[1] / "src" / "transfer" / "engine.py").read_text(encoding="utf-8")
    assert engine.count("record_job_usage(request, result)") == 1
    assert "record_transfer_usage(" not in engine
