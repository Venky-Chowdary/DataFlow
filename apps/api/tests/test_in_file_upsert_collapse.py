"""In-file duplicate keys on upsert collapse once and do not block the job.

``rc-duplicate-identity`` used to refuse the run before dedup. Upsert and
incremental_deduped keep the last row, warn, and succeed. Append that did
not ask to collapse still blocks when the destination key is enforced.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

from services.data_integrity import _check_duplicate_keys
from services.preflight_service import run_file_preflight
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _rows() -> list[dict]:
    rows = [{"id": str(i), "name": f"a{i}"} for i in range(1, 11)]
    rows.extend({"id": str(i), "name": f"b{i}"} for i in range(1, 11))
    return rows


def test_incremental_upsert_collapses_in_file_duplicates_without_blocking() -> None:
    report = _check_duplicate_keys(
        [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        _rows(),
        "strict",
        dest_kind="postgresql",
        primary_key="id",
        sync_mode="incremental_upsert",
        identity_declared=True,
    )
    assert report["blocks_transfer"] is False
    assert report["passed"] is True
    warning = " ".join(report["warnings"])
    assert "10 extra row" in warning
    assert "Counted once" in warning
    assert "quarantined" in warning

    preflight = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "INTEGER", "name": "VARCHAR"},
        row_count=20,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1, "transform": "none"},
            {"source": "name", "target": "name", "confidence": 1, "transform": "none"},
        ],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_table_exists=True,
        destination_pk_columns=["id"],
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="incremental_upsert",
        sample_rows=_rows(),
        destination_db_type="postgresql",
        validation_mode="strict",
        schema_policy="propagate_columns",
        contract_primary_key="id",
        stream_contracts=[{
            "name": "orders",
            "sync_mode": "incremental_upsert",
            "primary_key": "id",
            "selected": True,
        }],
    )
    blob = str(preflight.get("blockers")) + str(preflight.get("gates"))
    assert "rc-duplicate-identity" not in blob
    assert "duplicate_identity" not in blob
    assert preflight["passed"] is True


def test_append_with_enforced_key_still_blocks_in_file_duplicates() -> None:
    report = _check_duplicate_keys(
        [{"source": "id", "target": "id"}],
        [{"id": "1", "name": "a"}, {"id": "1", "name": "b"}],
        "strict",
        dest_kind="postgresql",
        primary_key="id",
        sync_mode="full_refresh_append",
        destination_pk_columns=["id"],
        identity_declared=True,
    )
    assert report["blocks_transfer"] is True


def test_csv_upsert_job_keeps_ten_rows(tmp_path: Path, monkeypatch) -> None:
    import io

    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")

    dst = tmp_path / "dst.db"
    buf = io.StringIO()
    buf.write("seq,id,name\n")
    for i in range(1, 11):
        buf.write(f"{i},{i},a{i}\n")
    for i in range(1, 11):
        buf.write(f"{10 + i},{i},b{i}\n")
    engine = UniversalTransferEngine()
    result = engine.execute_tracked(
        TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="orders.csv",
            source_content=buf.getvalue().encode(),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table="orders"
            ),
            sync_mode="incremental_upsert",
            mappings=[
                {"source": "seq", "target": "seq", "confidence": 1},
                {"source": "id", "target": "id", "confidence": 1},
                {"source": "name", "target": "name", "confidence": 1},
            ],
            stream_contracts=[{
                "name": "orders",
                "sync_mode": "incremental_upsert",
                "primary_key": "id",
                "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "selected": True,
            }],
            skip_preflight=False,
        ),
        uuid.uuid4().hex[:24],
    )
    assert result.success, result.error
    conn = sqlite3.connect(dst)
    rows = list(conn.execute("SELECT id, name FROM orders ORDER BY id"))
    conn.close()
    assert rows == [(i, f"b{i}") for i in range(1, 11)]
    summary = result.destination_summary or {}
    rejected = int(summary.get("rejected_rows") or 0)
    skipped = int(summary.get("rows_skipped") or 0)
    assert rejected == 0
    assert skipped == 10
