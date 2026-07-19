"""Verify file → database resume skips already-committed chunks."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest


def _csv_bytes(rows: int = 6) -> bytes:
    lines = ["id,name"]
    for i in range(rows):
        lines.append(f"{i},row-{i}")
    return "\n".join(lines).encode()


def test_stream_file_resume_skips_committed_chunks(monkeypatch):
    from services.checkpoint_service import Checkpoint, CheckpointService
    from transfer.file_stream import stream_file_to_database
    from transfer.models import EndpointConfig

    calls: list[dict] = []

    def fake_write_batch(
        dest_type, destination, dest_cfg, dest_table,
        headers, data_rows, mappings, column_types,
        create_table=False, on_checkpoint=None, chunk_idx=0, total_chunks=0,
        rows_so_far=0, write_mode="insert", conflict_columns=None, backfill_new_fields=False,
        error_policy=None,
        **_kwargs,
    ):
        calls.append({
            "chunk_idx": chunk_idx,
            "create_table": create_table,
            "rows": len(data_rows),
            "first_id": data_rows[0][0] if data_rows else None,
        })
        return len(data_rows), "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("transfer.file_stream._write_batch", fake_write_batch)
    monkeypatch.setattr("transfer.file_stream.CHUNK_SIZE", 2)

    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string="sqlite:///:memory:",
        table="import",
    )
    content = _csv_bytes(6)
    checkpoint = Checkpoint(
        job_id="resume-test",
        source_type="file",
        dest_type="sqlite",
        chunk_index=1,
        chunk_total=3,
        rows_processed=2,
        write_mode="insert",
    )

    written, _, _, _ = stream_file_to_database(
        content=content,
        filename="rows.csv",
        destination=destination,
        mappings=[],
        schema={},
        job_id="resume-test",
        checkpoint=checkpoint,
        checkpoint_service=CheckpointService(),
    )

    # With CHUNK_SIZE patched to 2, a 6-row CSV has 3 chunks. The checkpoint says chunk 1
    # (rows 0-1) is already committed, so only original chunks 1 and 2 should run.
    assert written == 6  # 2 previously committed + 4 processed now
    assert len(calls) == 2
    assert calls[0]["chunk_idx"] == 2
    assert calls[0]["create_table"] is True
    assert calls[0]["first_id"] == "2"
    assert calls[1]["chunk_idx"] == 3
    assert calls[1]["first_id"] == "4"


def test_stream_file_resume_recomputes_full_checksum(monkeypatch):
    """A resumed transfer must still fingerprint the whole source for reconciliation."""
    from services.checkpoint_service import Checkpoint, CheckpointService
    from transfer.file_stream import stream_file_to_database
    from transfer.models import EndpointConfig

    calls: list[dict] = []

    def fake_write_batch(*args, **kwargs):
        data_rows = kwargs.get("data_rows") or args[5]
        calls.append({"rows": len(data_rows)})
        return len(data_rows), "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("transfer.file_stream._write_batch", fake_write_batch)
    monkeypatch.setattr("transfer.file_stream.CHUNK_SIZE", 2)

    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string="sqlite:///:memory:",
        table="import",
    )
    content = _csv_bytes(10)
    checkpoint = Checkpoint(job_id="resume-test", chunk_index=1, rows_processed=2)

    _, _, dest_summary, _ = stream_file_to_database(
        content=content,
        filename="rows.csv",
        destination=destination,
        mappings=[],
        schema={},
        job_id="resume-test",
        checkpoint=checkpoint,
        checkpoint_service=CheckpointService(),
    )

    # 10 rows / chunk size 2 = 5 chunks; skip first chunk, write 4 => 8 rows
    assert sum(c["rows"] for c in calls) == 8
    assert dest_summary.get("checksum") is not None
