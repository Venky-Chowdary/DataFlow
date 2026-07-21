"""Destination DLQ table — real SQLite write + promote stamp proofs."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_dlq_table_name_stable():
    from services.dest_quarantine import dlq_table_name

    assert dlq_table_name("users") == "users_df_quarantine"
    assert dlq_table_name("public.orders") == "public_orders_df_quarantine"


def test_write_dest_quarantine_sqlite_and_promote(tmp_path: Path):
    from services.dest_quarantine import (
        count_open_dlq_rows,
        mark_dlq_promoted,
        write_dest_quarantine,
    )
    from src.transfer.models import EndpointConfig

    dest_path = tmp_path / "dlq.db"
    conn = f"sqlite:///{dest_path}"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        table="users",
        connection_string=conn,
        database=str(dest_path),
    )
    details = [
        {
            "row": 2,
            "column": "age",
            "target": "age",
            "value": "not-a-number",
            "reason": "invalid integer",
            "policy": "quarantine",
            "values": {"id": "2", "age": "not-a-number"},
        }
    ]
    result = write_dest_quarantine(dest, details, job_id="job-dlq-1")
    assert result["ok"] is True
    assert result["skipped"] is False
    assert result["rows_written"] >= 1
    assert result["table"] == "users_df_quarantine"
    assert details[0].get("_df_qid")

    # Table exists with metadata columns
    with sqlite3.connect(dest_path) as db:
        cols = [r[1] for r in db.execute("PRAGMA table_info(users_df_quarantine)").fetchall()]
        assert "_df_qid" in cols
        assert "_df_payload" in cols
        row = db.execute(
            "SELECT _df_job_id, _df_column, _df_value, _df_promoted_at FROM users_df_quarantine"
        ).fetchone()
        assert row[0] == "job-dlq-1"
        assert row[1] == "age"
        assert row[2] == "not-a-number"
        assert not row[3]

    open_info = count_open_dlq_rows(dest, job_id="job-dlq-1")
    assert open_info["supported"] is True
    assert open_info["open_rows"] >= 1

    promoted = mark_dlq_promoted(dest, qids=[details[0]["_df_qid"]], job_id="job-dlq-1")
    assert promoted["updated"] >= 1

    open_after = count_open_dlq_rows(dest, job_id="job-dlq-1")
    assert open_after["open_rows"] == 0


def test_engine_persists_dest_quarantine_on_transfer(tmp_path: Path):
    """Full transfer path: rejected cells land in users_df_quarantine."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest_path = tmp_path / "engine_dlq.db"
    conn = f"sqlite:///{dest_path}"
    csv = b"id,age\n1,30\n2,not-a-number\n"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            table="users",
            connection_string=conn,
            database=str(dest_path),
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
        column_types={"id": "string", "age": "string"},
    )
    engine = UniversalTransferEngine()
    result = engine.execute(request)
    assert result.success is True
    assert int(result.destination_summary.get("rejected_rows") or 0) >= 1
    dq = result.destination_summary.get("dest_quarantine") or {}
    assert dq.get("ok") is True
    assert dq.get("table") == "users_df_quarantine"
    assert int(dq.get("rows_written") or 0) >= 1

    with sqlite3.connect(dest_path) as db:
        n = db.execute("SELECT COUNT(*) FROM users_df_quarantine").fetchone()[0]
        assert n >= 1
