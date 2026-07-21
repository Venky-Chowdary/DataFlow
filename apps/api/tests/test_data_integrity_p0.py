"""P0 data-integrity guarantees: no silent NULL, honest bad-data accounting.

These tests prove the four audit fixes end-to-end:

  1. Strict/maximum FAIL-FAST on a non-coercible value on the STREAMING path
     (previously it silently substituted NULL).
  2. Postgres/MySQL/Mongo/BigQuery writers emit structured ``rejected_details``
     (row/column/target/value/reason/policy) — proven directly against real
     local services when available, and via the shared builder they all call.
  3. A kept-but-altered row is counted as ``coerced_null_rows`` so reconciliation
     can NOT report "100% fidelity" when a cell was forced to NULL.
  4. Such a run is reported with the terminal status ``completed_with_quarantine``
     rather than a clean ``completed``.

SQLite is used for the deterministic end-to-end proofs because it is a real
writer that exercises the identical ``_write_batch`` -> writer ->
``build_mapped_rows_with_details`` path as the networked destinations.
"""

from __future__ import annotations

import socket
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.checkpoint_service import CheckpointService  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.stream import stream_database_transfer  # noqa: E402


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str):
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


def _sqlite_source_with_bad_value(tmp_path: Path) -> Path:
    """One row has a value that cannot be cast to the DECIMAL target."""
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?, ?)",
        [(1, "10.50"), (2, "not-a-number"), (3, "20.00")],
    )
    conn.commit()
    conn.close()
    return db


_MAPPINGS = [
    {"source": "id", "target": "id"},
    {"source": "amount", "target": "amount"},
]
_SCHEMA = {"id": "integer", "amount": "decimal"}


def _stream(src_db: Path, dst_db: Path, mode: str):
    return stream_database_transfer(
        EndpointConfig(kind="database", format="sqlite", database=str(src_db), table="t"),
        EndpointConfig(kind="database", format="sqlite", database=str(dst_db), table="out"),
        _MAPPINGS,
        _SCHEMA,
        job_id="0" * 24,
        checkpoint_service=CheckpointService(_FakeMongo()),
        validation_mode=mode,
    )


# --- Task 1: strict/maximum FAIL-FAST on the streaming path -----------------

@pytest.mark.parametrize("mode", ["strict", "maximum"])
def test_stream_strict_fails_instead_of_silent_null(mode):
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = _sqlite_source_with_bad_value(tmp_path)
        dst = tmp_path / f"dst_{mode}.db"
        with pytest.raises(Exception) as exc:
            _stream(src, dst, mode)
        # It must fail because of the coercion, not silently write a NULL.
        assert "amount" in str(exc.value) or "decimal" in str(exc.value).lower()


# --- Tasks 2 + 3: balanced keeps the row, records the coercion --------------

def test_stream_balanced_keeps_row_and_records_coercion():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = _sqlite_source_with_bad_value(tmp_path)
        dst = tmp_path / "dst.db"
        rows_written, _ddl, summary, _cols = _stream(src, dst, "balanced")

        # All three rows are kept (no data dropped).
        assert rows_written == 3
        # The altered row is counted as a data-alteration statistic.
        assert summary["coerced_null_rows"] == 1
        assert summary["rejected_rows"] >= 1
        assert summary["error_policy"] == "quarantine"

        # Structured drill-down detail is present for the destination.
        details = summary["rejected_details"]
        assert details, "rejected_details must be populated"
        bad = details[0]
        for key in ("row", "column", "target", "value", "reason", "policy"):
            assert key in bad, f"missing {key} in rejected_details"
        assert bad["value"] == "not-a-number"
        assert bad["policy"] == "quarantine"

        # The kept row really landed with a NULL amount (not dropped).
        conn = sqlite3.connect(dst)
        got = dict(conn.execute('SELECT "id", "amount" FROM "out"').fetchall())
        conn.close()
        assert got[2] is None
        assert len(got) == 3


# --- Task 3: reconciliation honesty -----------------------------------------

def test_reconcile_not_100_percent_when_coerced():
    from services.reconciliation import reconcile

    # Row counts and checksums match (the same coercion is applied when the
    # source checksum is recomputed) — but a value was altered to NULL.
    report = reconcile(
        source_rows=3,
        target_rows=3,
        source_checksum="abc",
        target_checksum="abc",
        rejected_rows=1,
        coerced_null_rows=1,
        strict_checksum=True,
    )
    assert report.passed is True  # rows were not lost
    assert report.coerced_null_rows == 1
    assert "100%" not in report.message
    assert "NOT full fidelity" in report.message


def test_reconcile_clean_still_reports_full_fidelity():
    from services.reconciliation import reconcile

    report = reconcile(
        source_rows=3,
        target_rows=3,
        source_checksum="abc",
        target_checksum="abc",
        strict_checksum=True,
    )
    assert report.passed is True
    assert report.coerced_null_rows == 0
    assert "100% row fidelity" in report.message


def test_reconcile_dropped_rows_do_not_confuse_row_count():
    """Fail policy: dropped rows lower expected count; coerced==0."""
    from services.reconciliation import reconcile

    report = reconcile(
        source_rows=3,
        target_rows=2,
        source_checksum="abc",
        target_checksum="abc",
        rejected_rows=1,
        coerced_null_rows=0,
        strict_checksum=True,
    )
    assert report.passed is True
    assert "1 rejected" in report.message


# --- Task 4: terminal status ------------------------------------------------

def test_terminal_status_helper():
    from services.job_status import (
        COMPLETED,
        COMPLETED_WITH_QUARANTINE,
        is_completed,
        is_terminal,
        terminal_status_for,
    )

    assert terminal_status_for(0, 0) == COMPLETED
    assert terminal_status_for(1, 0) == COMPLETED_WITH_QUARANTINE  # dropped
    assert terminal_status_for(0, 2) == COMPLETED_WITH_QUARANTINE  # coerced
    # completed_with_quarantine is a SUCCESS terminal status.
    assert is_completed(COMPLETED_WITH_QUARANTINE) is True
    assert is_terminal(COMPLETED_WITH_QUARANTINE) is True
    assert is_completed("running") is False


# --- Task 4 end-to-end: engine sets completed_with_quarantine ---------------

def test_engine_file_to_sqlite_sets_completed_with_quarantine():
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    csv = b"id,age\n1,30\n2,not-a-number\n3,42\n"
    with tempfile.TemporaryDirectory() as tmp:
        dest_path = Path(tmp) / f"p0_{uuid.uuid4().hex[:8]}.db"
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(
                kind="database", format="sqlite", table="users",
                connection_string=f"sqlite:///{dest_path}",
            ),
            source_filename="users.csv",
            source_content=csv,
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
            ],
        )
        engine = UniversalTransferEngine()
        result = engine.execute(request)

        assert result.success is True
        assert result.destination_summary.get("coerced_null_rows", 0) >= 1
        # Reconciliation must not claim 100% fidelity.
        assert "100% row fidelity" not in (result.reconciliation.get("message") or "")
        assert result.reconciliation.get("coerced_null_rows", 0) >= 1

        # The persisted terminal status reflects the alteration.
        job = get_mongodb_service().get_job(result.job_id)
        assert job is not None
        assert job.get("status") == "completed_with_quarantine"


# --- Task 2: structured detail shape shared by all writers ------------------

def test_shared_builder_detail_shape_and_counts():
    """Every native writer (PG/MySQL/Mongo/BigQuery/SQLite/Snowflake) routes
    rejects through this shared builder, so this pins the emitted contract."""
    from connectors.writer_common import (
        _coerced_null_row_count,
        _rejected_row_count,
        build_mapped_rows_with_details,
    )

    headers = ["id", "age"]
    data_rows = [["1", "30"], ["2", "nope"], ["3", "40"]]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "age", "target": "age", "target_type": "integer"},
    ]
    kwargs = dict(
        headers=headers, data_rows=data_rows, mappings=mappings,
        target_cols=["id", "age"], column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
    )

    # Quarantine: row kept, detail recorded, counted as coerced.
    mapped_q, _errs_q, details_q = build_mapped_rows_with_details(error_policy="quarantine", **kwargs)
    assert len(mapped_q) == 3
    assert _coerced_null_row_count(details_q, "quarantine") == 1
    assert _rejected_row_count(data_rows, mapped_q, details_q, "quarantine") == 1
    d = next(x for x in details_q if x["value"] == "nope")
    assert d["row"] == 2
    assert d["column"] == "age"
    assert d["target"] == "age"
    assert d["value"] == "nope"
    assert d["policy"] == "quarantine"
    assert "integer" in d["reason"].lower()
    # Full source row context for quarantine review UI
    assert d.get("values", {}).get("age") == "nope"

    # Fail: row dropped, not coerced.
    mapped_f, _errs_f, details_f = build_mapped_rows_with_details(error_policy="fail", **kwargs)
    assert len(mapped_f) == 2
    assert _coerced_null_row_count(details_f, "fail") == 0
    assert _rejected_row_count(data_rows, mapped_f, details_f, "fail") == 1


# --- Task 2: real-service proofs (skip cleanly when unavailable) ------------

def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _skip_if_not_connected(result) -> None:
    """Real-service creds/config vary by environment; skip (don't fail) when the
    writer could not connect or authenticate."""
    if result.ok:
        return
    err = (result.error or "").lower()
    if any(s in err for s in ("access denied", "authentication", "could not connect",
                              "connection refused", "connect", "role", "does not exist",
                              "timed out", "no such host", "driver")):
        pytest.skip(f"service not usable in this environment: {result.error}")


def _bad_row_kwargs(table: str) -> dict:
    return dict(
        table_name=table,
        headers=["id", "age"],
        data_rows=[["1", "30"], ["2", "not-a-number"], ["3", "40"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "age", "target": "age", "target_type": "INTEGER"},
        ],
        column_types={"id": "INTEGER", "age": "INTEGER"},
        create_table=True,
    )


def test_postgres_writer_emits_details_and_coercion():
    if not _reachable("localhost", 5432):
        pytest.skip("Postgres not reachable")
    from connectors.postgresql_writer import write_mapped_rows

    table = "p0_pg_" + uuid.uuid4().hex[:8]
    common = dict(host="localhost", port=5432, database="dataflow",
                  username="dataflow", password="dataflow", schema="public",
                  connection_string="", ssl=False)
    # Quarantine keeps rows and records details.
    res = write_mapped_rows(**common, error_policy="quarantine", **_bad_row_kwargs(table))
    _skip_if_not_connected(res)
    assert res.rows_written == 3
    assert res.coerced_null_rows == 1
    assert any(d["value"] == "not-a-number" for d in res.rejected_details)
    # Fail-fast rejects the whole write.
    res2 = write_mapped_rows(**common, error_policy="fail", **_bad_row_kwargs(table + "_f"))
    assert res2.ok is False
    assert res2.rejected_details


def test_mysql_writer_emits_details_and_coercion():
    if not _reachable("localhost", 3306):
        pytest.skip("MySQL not reachable")
    from connectors.mysql_writer import write_mapped_rows

    table = "p0_my_" + uuid.uuid4().hex[:8]
    common = dict(host="localhost", port=3306, database="dataflow",
                  username="root", password="dataflow", schema="",
                  connection_string="", ssl=False)
    res = write_mapped_rows(**common, error_policy="quarantine", **_bad_row_kwargs(table))
    _skip_if_not_connected(res)
    assert res.coerced_null_rows == 1
    assert any(d["value"] == "not-a-number" for d in res.rejected_details)
    res2 = write_mapped_rows(**common, error_policy="fail", **_bad_row_kwargs(table + "_f"))
    assert res2.ok is False


def test_mongodb_writer_emits_details_and_coercion():
    if not _reachable("localhost", 27017):
        pytest.skip("MongoDB not reachable")
    from connectors.mongodb_writer import write_mapped_rows

    coll = "p0_mg_" + uuid.uuid4().hex[:8]
    common = dict(host="localhost", port=27017, database="dataflow",
                  username="", password="", schema="db",
                  connection_string="", ssl=False)
    try:
        res = write_mapped_rows(**common, error_policy="quarantine", **_bad_row_kwargs(coll))
        _skip_if_not_connected(res)
        assert res.coerced_null_rows == 1
        assert any(d["value"] == "not-a-number" for d in res.rejected_details)
    finally:
        try:
            from pymongo import MongoClient

            c = MongoClient("localhost", 27017, serverSelectionTimeoutMS=2000)
            c["dataflow"][coll].drop()
            c.close()
        except Exception:
            pass
