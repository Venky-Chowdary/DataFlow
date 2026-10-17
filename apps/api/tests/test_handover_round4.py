"""Round-4 handover: keep-last gates, policy sample, one row count, warnings, SFTP, append, drift."""

from __future__ import annotations

import sqlite3
import stat
from datetime import datetime, timezone
from pathlib import Path

import pytest

import services.schedule_runner as runner
import services.schedule_store as store
from services.blocker_titles import collapse_operator_message
from services.data_integrity import run_integrity_audit
from services.failure_retry_policy import DETERMINISTIC, classify_failure
from services.file_pickup_load import pickup_job_outcome


def _amount(sample: list[dict]) -> str:
    row = next(item for item in sample if str(item.get("order_id")) == "1")
    return str(row.get("amount"))


def _load(tmp_path: Path, csv: str, *, mode: str, policy: str, validation: str = "balanced"):
    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig

    db = tmp_path / "dest.db"
    written, _ddl, summary, _columns = stream_file_to_database(
        csv.encode(),
        "orders.csv",
        EndpointConfig(kind="database", format="sqlite", database=str(db), table="orders"),
        [
            {"source": "order_id", "target": "order_id", "confidence": 1},
            {"source": "amount", "target": "amount", "confidence": 1},
        ],
        {"order_id": "string", "amount": "string"},
        sync_mode=mode,
        stream_contracts=[{"name": "orders", "primary_key": ["order_id"], "selected": True}],
        duplicate_policy=policy,
        skip_preflight=True,
        validation_mode=validation,
    )
    with sqlite3.connect(db) as conn:
        landed = conn.execute("select order_id, amount from orders order by order_id").fetchall()
    return written, summary, landed


@pytest.mark.parametrize("mode", ["full_refresh_append", "incremental_append"])
@pytest.mark.parametrize(
    ("policy", "amount"),
    [("upsert", "184.95"), ("skip_duplicates", "183.95")],
)
def test_verification_sample_follows_the_duplicate_policy(tmp_path: Path, mode: str, policy: str, amount: str):
    _written, summary, landed = _load(
        tmp_path,
        "order_id,amount\n1,183.95\n1,184.95\n",
        mode=mode,
        policy=policy,
    )
    assert _amount(summary["reconcile_sample"]) == amount
    assert landed == [("1", amount)]
    assert int(summary.get("rows_skipped") or 0) >= 1
    assert int(summary.get("source_row_count") or 0) == 2


def test_keep_last_unique_expectation_does_not_block():
    rows = [
        {"order_id": "1", "amount": "183.95"},
        {"order_id": "1", "amount": "184.95"},
    ]
    blocked = run_integrity_audit(
        mappings=[{"source": "order_id", "target": "order_id", "confidence": 1}],
        source_columns=["order_id", "amount"],
        source_schemas=[
            {"name": "order_id", "inferred_type": "VARCHAR"},
            {"name": "amount", "inferred_type": "VARCHAR"},
        ],
        sample_rows=rows,
        validation_mode="strict",
        destination_db_type="postgresql",
        sync_mode="upsert",
        contract_primary_key="order_id",
        destination_pk_columns=["order_id"],
    )
    suite = next(item for item in blocked["checks"] if item["check"] == "expectations_suite")
    dup = next(item for item in blocked["checks"] if item["check"] == "duplicate_keys")
    assert suite["blocks_transfer"] is False
    assert dup["blocks_transfer"] is False
    assert "Counted once" in " ".join(dup.get("warnings") or [])

    kept = run_integrity_audit(
        mappings=[{"source": "order_id", "target": "order_id", "confidence": 1}],
        source_columns=["order_id", "amount"],
        source_schemas=[
            {"name": "order_id", "inferred_type": "VARCHAR"},
            {"name": "amount", "inferred_type": "VARCHAR"},
        ],
        sample_rows=rows,
        validation_mode="strict",
        destination_db_type="postgresql",
        sync_mode="upsert",
        contract_primary_key="order_id",
        destination_pk_columns=["order_id"],
        in_file_dedupe="keep_last",
    )
    suite = next(item for item in kept["checks"] if item["check"] == "expectations_suite")
    assert suite["blocks_transfer"] is False


def test_append_overwrite_allows_duplicate_identity_keys():
    findings = [{"value": "1", "count": 2}]
    blocked = run_integrity_audit(
        mappings=[{"source": "order_id", "target": "order_id", "confidence": 1}],
        source_columns=["order_id"],
        source_schemas=[{"name": "order_id", "inferred_type": "VARCHAR"}],
        sample_rows=[{"order_id": "1"}, {"order_id": "1"}],
        validation_mode="strict",
        destination_db_type="postgresql",
        sync_mode="full_refresh_overwrite",
        contract_primary_key="order_id",
        destination_pk_columns=["order_id"],
        source_duplicate_findings=findings,
        source_duplicate_probe_status="ran",
        source_duplicate_probe_expected=True,
    )
    dupes = next(item for item in blocked["checks"] if item["check"] == "duplicate_keys")
    assert dupes["blocks_transfer"] is True

    allowed = run_integrity_audit(
        mappings=[{"source": "order_id", "target": "order_id", "confidence": 1}],
        source_columns=["order_id"],
        source_schemas=[{"name": "order_id", "inferred_type": "VARCHAR"}],
        sample_rows=[{"order_id": "1"}, {"order_id": "1"}],
        validation_mode="strict",
        destination_db_type="postgresql",
        sync_mode="full_refresh_overwrite",
        contract_primary_key="order_id",
        destination_pk_columns=["order_id"],
        duplicate_policy="append",
        source_duplicate_findings=findings,
        source_duplicate_probe_status="ran",
        source_duplicate_probe_expected=True,
    )
    dupes = next(item for item in allowed["checks"] if item["check"] == "duplicate_keys")
    assert dupes["blocks_transfer"] is False


def test_null_identity_is_quarantined_once(tmp_path: Path):
    _written, summary, landed = _load(
        tmp_path,
        "order_id,amount\n1,10\n,20\n2,30\n",
        mode="full_refresh_append",
        policy="upsert",
        validation="lenient",
    )
    read = int(summary.get("source_row_count") or 0)
    rejected = int(summary.get("rejected_rows") or 0)
    skipped = int(summary.get("rows_skipped") or 0)
    assert read == 3
    assert rejected == 1
    assert skipped == 0
    assert len(landed) == 2
    assert read == len(landed) + rejected + skipped


def test_todays_file_warning_is_a_job_outcome():
    message = "expected file for 2026-10-16 not arrived"
    outcome = pickup_job_outcome(
        {"file_pickup": "warn", "warnings": [message], "rows_written": 0},
        0,
    )
    assert outcome is not None
    assert outcome["status"] == "completed_with_warnings"
    assert outcome["message"] == message
    assert pickup_job_outcome(
        {"file_pickup": "unchanged", "warnings": ["no new or changed files"]},
        0,
    ) is None


def test_sftp_glob_stat_downloads_a_large_file_once(monkeypatch, tmp_path: Path):
    from connectors import sftp_reader

    downloads = {"n": 0}
    stated: list[str] = []

    class _Entry:
        filename = "big_orders.csv"
        st_mode = stat.S_IFREG
        st_size = 66
        st_mtime = 100

    class _Sftp:
        def listdir_attr(self, _directory):
            return [_Entry()]

        def stat(self, path):
            stated.append(str(path))
            if "*" in str(path):
                raise OSError("glob is not a file")
            return _Entry()

        def close(self):
            return None

    class _Transport:
        def close(self):
            return None

    def fake_connect(_cfg):
        return _Transport(), _Sftp()

    def fake_download(_kind, path, _cfg, _bucket, _key):
        downloads["n"] += 1
        payload = b"id\n" + (b"1\n" * 2_000_000)
        Path(path).write_bytes(payload)

    monkeypatch.setattr(sftp_reader, "connect_sftp", fake_connect)
    monkeypatch.setattr(sftp_reader, "download_for_object_store", fake_download)
    cfg = {
        "host": f"cache-{tmp_path.name}",
        "port": 2222,
        "username": "dataflow",
        "password": "dataflow",
        "database": f"/inbound/{tmp_path.name}",
        "table": "big_*.csv",
    }
    first = sftp_reader.read_object(cfg=cfg, offset=0, limit=1)
    second = sftp_reader.read_object(cfg=cfg, offset=1, limit=1)
    assert downloads["n"] == 1
    assert first.total_rows == 2_000_000
    assert second.total_rows == 2_000_000
    assert stated
    assert all("*" not in path for path in stated)
    assert any(path.endswith("big_orders.csv") for path in stated)


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    path = tmp_path / "schedules.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield path


def test_schema_change_opens_one_drift_approval(temp_store, monkeypatch):
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
    error = (
        "Schema change detected — policy 'manual_review' requires review. "
        "Schema drift: type change narrowing on amount. "
        "Mapping confidence below floor on notes"
    )
    classified = classify_failure(error=error, phase="failed", rows_written=0)
    assert classified.kind == DETERMINISTIC
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda _jid: {
            "status": "failed",
            "phase": "failed",
            "error": error,
            "records_processed": 0,
        },
    )
    runner._finalize_run(sched.id, "job-drift", attempt=0, started_at=datetime.now(timezone.utc))
    reloaded = store.get_schedule(sched.id)
    approval = reloaded.approval_request
    assert approval["code"] == "SOURCE_DRIFT"
    assert approval["kind"] == "source_drift"
    finding = approval["finding"]
    assert finding.startswith("Schema change detected")
    assert "Schema drift:" not in finding
    assert "Mapping confidence below floor" in finding
    assert collapse_operator_message(error) == finding or "Schema drift:" not in collapse_operator_message(error)
