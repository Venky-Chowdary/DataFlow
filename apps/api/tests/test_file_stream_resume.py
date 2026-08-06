"""Verify file → database resume skips already-committed chunks."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.checkpoint_service import Checkpoint  # noqa: E402


class _MemoryCheckpointService:
    """In-memory checkpoint store for resume tests (no Mongo)."""

    def __init__(self) -> None:
        self._jobs: dict[str, dict] = {}

    def save(self, checkpoint: Checkpoint) -> bool:
        self._jobs[checkpoint.job_id] = {"checkpoint": checkpoint.to_dict()}
        return True

    def require_save(self, checkpoint: Checkpoint) -> None:
        if not self.save(checkpoint):
            from services.checkpoint_service import (
                CHECKPOINT_PERSISTENCE_FAILED,
                CheckpointPersistenceError,
            )

            raise CheckpointPersistenceError(CHECKPOINT_PERSISTENCE_FAILED)

    def load(self, job_id: str) -> Checkpoint | None:
        data = self._jobs.get(job_id, {}).get("checkpoint")
        return Checkpoint.from_dict(data) if data else None


def _csv_bytes(rows: int = 6) -> bytes:
    lines = ["id,name"]
    for i in range(rows):
        lines.append(f"{i},row-{i}")
    return "\n".join(lines).encode()


def test_stream_file_resume_skips_committed_chunks(monkeypatch):
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
            "write_mode": write_mode,
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
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        job_id="resume-test",
        checkpoint=checkpoint,
        checkpoint_service=_MemoryCheckpointService(),
        # Resume requires an identity so write_mode can upgrade insert→upsert
        # (parity with db stream — refuse silent re-append of committed chunks).
        stream_contracts=[{"selected": True, "primary_key": ["id"], "sync_mode": "full_refresh_append"}],
        sync_mode="full_refresh_append",
    )

    # With CHUNK_SIZE patched to 2, a 6-row CSV has 3 chunks. The checkpoint says chunk 1
    # (rows 0-1) is already committed, so only original chunks 1 and 2 should run.
    assert written == 6  # 2 previously committed + 4 processed now
    assert len(calls) == 2
    assert calls[0]["chunk_idx"] == 2
    assert calls[0]["create_table"] is True
    assert calls[0]["first_id"] == "2"
    assert calls[0]["write_mode"] == "upsert"
    assert calls[1]["chunk_idx"] == 3
    assert calls[1]["first_id"] == "4"


def test_stream_file_resume_without_pk_refuses_silent_reappend(monkeypatch):
    """Resume of insert without primary_key must fail closed — not re-append."""
    from transfer.file_stream import stream_file_to_database
    from transfer.models import EndpointConfig

    monkeypatch.setattr(
        "transfer.file_stream._write_batch",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not write")),
    )
    monkeypatch.setattr("transfer.file_stream.CHUNK_SIZE", 2)
    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string="sqlite:///:memory:",
        table="import",
    )
    checkpoint = Checkpoint(job_id="resume-no-pk", chunk_index=1, rows_processed=2)
    try:
        stream_file_to_database(
            content=_csv_bytes(6),
            filename="rows.csv",
            destination=destination,
            mappings=[],
            schema={},
            job_id="resume-no-pk",
            checkpoint=checkpoint,
            checkpoint_service=_MemoryCheckpointService(),
            sync_mode="full_refresh_append",
        )
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "primary key" in str(exc).lower()


def test_stream_file_resume_recomputes_full_checksum(monkeypatch):
    """A resumed transfer must still fingerprint the whole source for reconciliation."""
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
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        job_id="resume-test",
        checkpoint=checkpoint,
        checkpoint_service=_MemoryCheckpointService(),
        stream_contracts=[{"selected": True, "primary_key": ["id"], "sync_mode": "full_refresh_append"}],
        sync_mode="full_refresh_append",
    )

    # 10 rows / chunk size 2 = 5 chunks; skip first chunk, write 4 => 8 rows
    assert sum(c["rows"] for c in calls) == 8
    assert dest_summary.get("checksum") is not None
