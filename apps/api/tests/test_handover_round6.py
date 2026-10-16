"""Round-6 handover: drift before the run, spill lifetime, schema create, today's file, update counts."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from services.drift_approval import (
    SOURCE_DRIFT_CODE,
    is_source_drift_code,
    message_is_unmapped_column_drift,
)
from services.failure_retry_policy import DETERMINISTIC, classify_failure
from services.file_pickup import RemoteFile, plan_pickup
from services.file_pickup_load import apply_pickup_job_outcome, pickup_job_outcome
from services.row_conservation import DEST_READBACK, KeyCensus, account_population
from services.schedule_approvals import ApprovalRequired


def _postgres_or_fail():
    try:
        import psycopg2
    except ImportError:
        pytest.fail("psycopg2 is required to prove the missing-schema path")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
    except Exception as exc:
        pytest.fail(f"Postgres is required for the missing-schema proof: {exc}")
    conn.close()


class _Sched:
    def __init__(self, schema: dict[str, str] | None, policy: str = "manual_review"):
        self.id = "sched-r6"
        self.source_schema = dict(schema or {})
        self.source_schema_fingerprint = ""
        self.schema_policy = policy
        self.source_primary_key = []
        self.cursor_column = ""


class _Endpoint:
    format = "sftp"


class _Request:
    def __init__(self, mappings):
        self.source = _Endpoint()
        self.destination = _Endpoint()
        self.mappings = mappings


def test_unmapped_sftp_column_opens_source_drift_before_the_run(monkeypatch):
    import services.schedule_runner as runner

    recorded: list = []
    monkeypatch.setattr(
        runner,
        "_remember_source_schema",
        lambda *a, **k: recorded.append(True),
    )
    base = {"order_id": "string", "amount": "string"}
    drifted = {**base, "currency": "string"}
    # Nested file types are what an SFTP sample returns when the type is an object.
    monkeypatch.setattr(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        lambda _endpoint: {
            "schema": {name: {"type": kind} for name, kind in drifted.items()},
            "columns": list(drifted),
        },
        raising=False,
    )
    with pytest.raises(ApprovalRequired) as excinfo:
        runner._guard_source_schema_drift(
            _Sched(base),
            _Request(
                [
                    {"source": "order_id", "target": "order_id"},
                    {"source": "amount", "target": "amount"},
                ]
            ),
        )
    assert excinfo.value.code == SOURCE_DRIFT_CODE
    assert excinfo.value.kind == "source_drift"
    assert "currency" in str(excinfo.value)
    assert "neither mapped nor declared omitted" in str(excinfo.value)
    assert recorded == []


def test_propagate_policy_keeps_an_unmapped_column_moving(monkeypatch):
    import services.schedule_runner as runner

    recorded: list = []
    monkeypatch.setattr(
        runner,
        "_remember_source_schema",
        lambda *a, **k: recorded.append(True),
    )
    monkeypatch.setattr(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        lambda _endpoint: {
            "schema": {"order_id": "string", "currency": "string"},
            "columns": ["order_id", "currency"],
        },
        raising=False,
    )
    runner._guard_source_schema_drift(
        _Sched({"order_id": "string"}, policy="propagate_columns"),
        _Request([{"source": "order_id", "target": "order_id"}]),
    )
    assert recorded


def test_field_reduction_sentence_parks_source_drift_without_a_gate_id(tmp_path, monkeypatch):
    import services.schedule_runner as runner
    import services.schedule_store as store

    path = tmp_path / "schedules.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    sched = store.create_schedule({
        "name": "orders",
        "source_connector_id": "src",
        "source_table": "orders_*.csv",
        "dest_connector_id": "dst",
        "dest_table": "orders",
        "interval": "daily",
        "mappings": [{"source": "order_id", "target": "order_id"}],
    })
    message = (
        "currency: neither mapped nor declared omitted — an unanswered "
        "question, not a decision (see G13)"
    )
    assert message_is_unmapped_column_drift(message)
    assert is_source_drift_code("no_disposition")
    assert classify_failure(error=message, phase="failed", rows_written=0).kind == DETERMINISTIC
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda _jid: {
            "status": "failed",
            "phase": "failed",
            "error": message,
            "records_processed": 0,
        },
    )
    runner._finalize_run(sched.id, "job-manual", attempt=0, started_at=datetime.now(timezone.utc))
    approval = store.get_schedule(sched.id).approval_request
    assert approval["code"] == SOURCE_DRIFT_CODE
    assert approval["kind"] == "source_drift"


def test_spill_ttl_outlives_the_lease_and_a_second_read_does_not_download(tmp_path, monkeypatch):
    from services.object_streaming import clear_spill_cache, download_object, spill_ttl_seconds

    monkeypatch.setenv("DATAFLOW_SPILL_TTL", "300")
    assert spill_ttl_seconds() >= 60 * 60
    clear_spill_cache()
    calls = {"n": 0}

    def _write(path: Path) -> None:
        calls["n"] += 1
        path.write_bytes(b"orders")

    key = f"sftp:r6:{uuid.uuid4().hex}"
    first = download_object(key, _write)
    second = download_object(key, _write)
    assert first == second
    assert calls["n"] == 1
    assert first.read_bytes() == b"orders"


def test_loading_an_older_dated_file_warns_that_today_is_absent():
    today = date(2026, 10, 16)
    message = "expected file for 2026-10-16 not arrived"
    plan = plan_pickup(
        pattern="orders_*.csv",
        files=[RemoteFile(name="orders_2026-10-13.csv", size=10, checksum="a")],
        manifest=[],
        today=today,
    )
    assert plan.action == "load"
    assert [item.name for item in plan.files] == ["orders_2026-10-13.csv"]
    assert plan.message == message
    outcome = pickup_job_outcome(
        {"file_pickup": "glob", "warnings": [message]},
        20,
    )
    assert outcome is not None
    assert outcome["status"] == "completed_with_warnings"
    assert outcome["message"] == message
    recon, status, job_message = apply_pickup_job_outcome(
        {"passed": False, "message": "checksum mismatch"},
        {"file_pickup": "glob", "warnings": [message]},
        20,
    )
    assert status is None
    assert recon["passed"] is False
    assert job_message is None


def test_update_only_resend_counts_the_updated_rows():
    census = KeyCensus(unique_batch_keys=20, dest_preexisting=20)
    ledger = account_population(
        rows_read=20,
        dest_count=80,
        dest_count_source=DEST_READBACK,
        dest_count_before=80,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=20,
        sync_mode="incremental_deduped",
        census=census,
    )
    assert ledger.inserts == 0
    assert ledger.updates == 20
    assert ledger.dest_delta == 0
    assert ledger.rows_written == 20
    assert ledger.unaccounted == 0
    assert ledger.balanced is True


def test_missing_destination_schema_is_created_when_create_is_granted():
    _postgres_or_fail()
    import psycopg2

    from services.destination_privilege_probe import probe_destination_privileges
    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig

    schema = f"qa_r6_{uuid.uuid4().hex[:8]}"
    table = "orders"
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    cur.close()
    conn.close()
    try:
        probe = probe_destination_privileges(
            "postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            schema=schema,
            table=table,
            username="dataflow",
            password="dataflow",
            table_exists=False,
        )
        assert probe.status == "ok", probe.detail
        assert probe.can_create_table is True
        assert "Privilege catalog unavailable" not in (probe.detail or "")
        assert "does not exist" not in (probe.detail or "").lower() or "create" in (probe.detail or "").lower()
        written, _ddl, summary, _columns = stream_file_to_database(
            b"order_id,amount\n1,10\n",
            "orders.csv",
            EndpointConfig(
                kind="database",
                format="postgresql",
                host="127.0.0.1",
                port=5432,
                database="dataflow",
                schema=schema,
                username="dataflow",
                password="dataflow",
                table=table,
            ),
            [{"source": "order_id", "target": "order_id", "confidence": 1}],
            {"order_id": "string", "amount": "string"},
            sync_mode="full_refresh_append",
            skip_preflight=True,
            validation_mode="balanced",
        )
        assert int(written) == 1
        assert int(summary.get("rows_written") or 0) == 1
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        assert int(cur.fetchone()[0]) == 1
        cur.close()
        conn.close()
    finally:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        cur.close()
        conn.close()


def test_missing_schema_without_create_names_the_grant():
    from services.destination_privilege_probe import _missing_postgres_schema

    class _Cur:
        def __init__(self):
            self.calls = 0

        def execute(self, _sql, _params=None):
            self.calls += 1

        def fetchone(self):
            # First call: schema does not exist. Second: CREATE is not granted.
            return (False,)

    signals: dict = {}
    result = _missing_postgres_schema(
        _Cur(),
        engine="postgresql",
        schema="qa_r6_seq",
        table="orders",
        username="loader",
        need_update=False,
        signals=signals,
    )
    assert result is not None
    assert result.status == "denied"
    assert 'schema "qa_r6_seq" does not exist' in result.detail
    assert "cannot create it" in result.detail
    assert "Privilege catalog unavailable" not in result.detail
    assert signals["database_create"] is False
