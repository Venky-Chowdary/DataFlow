"""Round-5 handover: one row bucket, drift by gate code, bounded uniqueness."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

import services.schedule_runner as runner
import services.schedule_store as store
from services.drift_approval import (
    SOURCE_DRIFT_CODE,
    build_source_drift_approval,
    is_source_drift_code,
)
from services.row_conservation import (
    PopulationConservationError,
    assert_population_conservation_closed,
)


def _csv_60() -> str:
    lines = ["order_id,amount"]
    for i in range(1, 41):
        lines.append(f"{i},{i}.00")
    for i in range(41, 51):
        lines.append(f"{i},{i}.10")
        lines.append(f"{i},{i}.95")
    return "\n".join(lines) + "\n"


def _postgres_or_fail():
    try:
        import psycopg2
    except ImportError:
        pytest.fail("psycopg2 is required to prove the postgres duplicate accounting")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
    except Exception as exc:
        pytest.fail(f"Postgres is required for the duplicate-accounting proof: {exc}")
    conn.close()


def _load_postgres(csv: str, *, mode: str, policy: str) -> tuple[int, dict, int]:
    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig

    import psycopg2

    table = f"r5_{uuid.uuid4().hex[:10]}"
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.close()
    conn.close()
    try:
        written, _ddl, summary, _columns = stream_file_to_database(
            csv.encode(),
            "orders.csv",
            EndpointConfig(
                kind="database",
                format="postgresql",
                host="127.0.0.1",
                port=5432,
                database="dataflow",
                username="dataflow",
                password="dataflow",
                table=table,
            ),
            [
                {"source": "order_id", "target": "order_id", "confidence": 1},
                {"source": "amount", "target": "amount", "confidence": 1},
            ],
            {"order_id": "string", "amount": "string"},
            sync_mode=mode,
            stream_contracts=[{"name": "orders", "primary_key": ["order_id"], "selected": True}],
            duplicate_policy=policy,
            skip_preflight=True,
            validation_mode="balanced",
        )
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        dest = int(cur.fetchone()[0])
        cur.close()
        conn.close()
        return int(written), summary, dest
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
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.close()
        conn.close()


def _conservation_closes(summary: dict, *, mode: str, dest: int, written: int) -> None:
    """The engine fails the job when this ledger does not balance."""
    read = int(summary.get("source_row_count") or 0)
    rejected = int(summary.get("rejected_rows") or 0)
    skipped = int(summary.get("rows_skipped") or 0)
    job = {
        "sync_mode": mode,
        "records_processed": written,
        "rejected_rows": rejected,
        "destination_summary": {
            "rows_skipped": skipped,
            "sync_mode": mode,
            "target_rows_before": 0,
        },
        "reconciliation": {
            "source_rows": read,
            "target_rows": dest,
            "target_checksum": "measured",
            "rejected_rows": rejected,
            "rows_skipped": skipped,
            "target_rows_before": 0,
        },
    }
    try:
        ledger = assert_population_conservation_closed(job, validation_mode="balanced")
    except PopulationConservationError as exc:
        pytest.fail(f"committed load would be marked failed: {exc}")
    assert ledger.balanced
    assert ledger.unaccounted == 0


@pytest.fixture(scope="module", autouse=True)
def _require_postgres():
    _postgres_or_fail()


@pytest.mark.parametrize("mode", ["full_refresh_append", "incremental_append"])
@pytest.mark.parametrize("policy", ["upsert", "skip_duplicates", "append"])
def test_each_source_row_is_counted_once(mode: str, policy: str):
    written, summary, dest = _load_postgres(_csv_60(), mode=mode, policy=policy)
    read = int(summary.get("source_row_count") or 0)
    rejected = int(summary.get("rejected_rows") or 0)
    skipped = int(summary.get("rows_skipped") or 0)
    assert read == 60
    assert read == dest + rejected + skipped
    assert read == written + rejected + skipped
    if policy == "append":
        assert dest == 60
        assert skipped == 0
        assert rejected == 0
    else:
        assert dest == 50
        assert skipped == 10
        assert rejected == 0
    _conservation_closes(summary, mode=mode, dest=dest, written=written)


def test_drift_is_the_gate_code_not_the_sentence():
    message = "1 source column(s) are neither mapped nor declared omitted: currency"
    assert is_source_drift_code("g13_source_coverage")
    assert is_source_drift_code("schema_drift")
    assert is_source_drift_code("g13_source_coverage.unaccounted")
    assert not is_source_drift_code(message)
    approval = build_source_drift_approval(
        message=message,
        codes=["g13_source_coverage", "g13_source_coverage.unaccounted"],
        job_id="job-manual",
    )
    assert approval is not None
    assert approval["code"] == SOURCE_DRIFT_CODE
    assert approval["kind"] == "source_drift"
    assert "replay_schema_drift_ack" in approval["requested_scopes"]
    assert build_source_drift_approval(message=message, codes=["g9_data_integrity"]) is None


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    path = tmp_path / "schedules.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield path


def test_schedule_coverage_gate_opens_source_drift(temp_store, monkeypatch):
    sched = store.create_schedule({
        "name": "orders",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders",
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
        "max_retries": 2,
    })
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    message = "1 source column(s) are neither mapped nor declared omitted: currency"
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda _jid: {
            "status": "failed",
            "phase": "failed",
            "error": message,
            "records_processed": 0,
            "preflight": {
                "blockers": [
                    {
                        "id": "g13_source_coverage",
                        "message": message,
                        "details": {"rule_id": "g13_source_coverage.unaccounted"},
                    }
                ],
            },
        },
    )
    runner._finalize_run(sched.id, "job-coverage", attempt=0, started_at=datetime.now(timezone.utc))
    approval = store.get_schedule(sched.id).approval_request
    assert approval["code"] == SOURCE_DRIFT_CODE
    assert approval["kind"] == "source_drift"
    assert "currency" in approval["finding"]


def test_manual_preflight_opens_source_drift_on_the_job_and_schedule(temp_store):
    from src.transfer.job_failure import _fail_job_preflight

    sched = store.create_schedule({
        "name": "orders",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders",
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })
    message = "1 source column(s) are neither mapped nor declared omitted: currency"
    saved: dict = {}

    class _Mongo:
        def get_job(self, _job_id):
            return {
                "transfer_request": {
                    "source": {"extra": {"schedule_id": sched.id}},
                }
            }

        def update_job_status(self, _job_id, status, **kwargs):
            saved["status"] = status
            saved.update(kwargs)
            return True

    class _Lineage:
        def emit_preflight_completed(self, **_kwargs):
            return None

        def emit_run_failed(self, **_kwargs):
            return None

    _fail_job_preflight(
        _Mongo(),
        "job-manual",
        {
            "blockers": [
                {
                    "id": "g13_source_coverage",
                    "message": message,
                    "details": {"rule_id": "g13_source_coverage.unaccounted"},
                }
            ],
            "gates": [
                {
                    "id": "g13_source_coverage",
                    "status": "block",
                    "message": message,
                    "details": {"rule_id": "g13_source_coverage.unaccounted"},
                }
            ],
            "proof_bundle": {},
        },
        lineage=_Lineage(),
        rows_read=0,
        sync_mode="full_refresh_append",
    )
    assert saved["approval_request"]["code"] == SOURCE_DRIFT_CODE
    parked = store.get_schedule(sched.id).approval_request
    assert parked["code"] == SOURCE_DRIFT_CODE
    assert parked["job_id"] == "job-manual"


def test_two_million_row_uniqueness_is_one_pass(tmp_path: Path):
    from services.identity_uniqueness import scan_identity_file
    from services.source_duplicate_probe import (
        _finding_value,
        _identity_cell,
        _normalize_key_cell,
    )

    path = tmp_path / "orders.csv"
    with path.open("w", encoding="utf-8") as handle:
        handle.write("id\n")
        for i in range(2_000_000):
            handle.write(f"{i}\n")
    started = time.perf_counter()
    findings, scanned, complete = scan_identity_file(
        path,
        "orders.csv",
        ["id"],
        limit=5,
        normalize_key=_normalize_key_cell,
        identity_cell=_identity_cell,
        finding_value=_finding_value,
    )
    elapsed = time.perf_counter() - started
    assert complete is True
    assert scanned == 2_000_000
    assert findings == []
    assert elapsed < 120, f"2M-row uniqueness took {elapsed:.1f}s"


def test_ten_million_keys_stay_in_bounded_partitions():
    from services.identity_uniqueness import PartitionedUniqueness
    from services.source_duplicate_probe import _finding_value, _identity_cell

    scanner = PartitionedUniqueness()
    try:
        for i in range(10_000_000):
            scanner.add((str(i % 9_999_999),))
        findings = scanner.findings(
            ["id"],
            5,
            identity_cell=_identity_cell,
            finding_value=_finding_value,
        )
    finally:
        scanner.close()
    assert scanner.rows == 10_000_000
    assert findings
    assert findings[0]["count"] >= 2


def test_sftp_probe_uses_the_spill_not_the_row_cap(monkeypatch, tmp_path: Path):
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    path = tmp_path / "orders.csv"
    path.write_text("id\n1\n1\n2\n", encoding="utf-8")
    monkeypatch.setattr(
        "connectors.sftp_reader.materialize_spill",
        lambda **_kwargs: (path, "orders.csv"),
    )
    result = probe_source_duplicate_keys_result(
        source_config={
            "type": "sftp",
            "host": "files.example",
            "database": "/inbound",
            "table": "orders.csv",
        },
        source_table="orders.csv",
        primary_key_columns=["id"],
    )
    assert result.status == "ran"
    assert result.findings
    assert result.findings[0]["count"] == 2
