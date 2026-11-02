"""Verify file → database resume skips already-committed chunks."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

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


def _stream_write_view(args, kwargs) -> tuple[int, Any]:
    """Row count + first cell after file-stream skipped the retained matrix."""
    data_rows = kwargs.get("data_rows")
    if data_rows is None and len(args) > 5:
        data_rows = args[5]
    if data_rows:
        return len(data_rows), data_rows[0][0] if data_rows[0] else None
    spool = kwargs.get("source_spool")
    if spool is not None:
        first = None
        for _start, bundle in spool.iter_bundles(1):
            if bundle and bundle[0]:
                first = bundle[0][0]
            break
        return int(getattr(spool, "row_count", 0) or 0), first
    return 0, None


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
        rows, first_id = _stream_write_view(
            (dest_type, destination, dest_cfg, dest_table, headers, data_rows),
            {
                "data_rows": data_rows,
                "source_spool": _kwargs.get("source_spool"),
            },
        )
        calls.append({
            "chunk_idx": chunk_idx,
            "create_table": create_table,
            "rows": rows,
            "first_id": first_id,
            "write_mode": write_mode,
        })
        return rows, "checksum", {"rejected_rows": 0}

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
        rows, _first = _stream_write_view(args, kwargs)
        calls.append({"rows": rows})
        return rows, "checksum", {"rejected_rows": 0}

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
    # Gate-8 conservation: the reader-side source count must be the FULL source
    # population (10), not the resumed tail (8) — comparing a tail count against a
    # full-table read-back would mis-account an otherwise correct resumed load.
    assert dest_summary.get("source_row_count") == 10
    assert dest_summary.get("source_row_count_source") == "full_rescan_rows"


def test_stream_file_resume_full_rescan_respects_source_filter(monkeypatch):
    """A filtered resume must count/hash the FILTERED population — the full-file
    re-scan applies source_filter exactly like the main write path, or Gate-8
    overstates source_row_count and fails conservation on a correct load."""
    from transfer.file_stream import stream_file_to_database
    from transfer.models import EndpointConfig

    def fake_write_batch(*args, **kwargs):
        rows, _first = _stream_write_view(args, kwargs)
        return rows, "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("transfer.file_stream._write_batch", fake_write_batch)
    monkeypatch.setattr("transfer.file_stream.CHUNK_SIZE", 2)

    destination = EndpointConfig(
        kind="database", format="sqlite",
        connection_string="sqlite:///:memory:", table="import",
    )
    content = _csv_bytes(10)  # ids 0..9
    checkpoint = Checkpoint(job_id="resume-filter", chunk_index=1, rows_processed=2)

    _, _, dest_summary, _ = stream_file_to_database(
        content=content,
        filename="rows.csv",
        destination=destination,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        job_id="resume-filter",
        checkpoint=checkpoint,
        checkpoint_service=_MemoryCheckpointService(),
        stream_contracts=[{"selected": True, "primary_key": ["id"], "sync_mode": "full_refresh_append"}],
        sync_mode="full_refresh_append",
        source_filter={"column": "id", "operator": "lt", "value": 6},  # keep 0..5 => 6 rows
    )

    # Full population after the filter is 6, not the unfiltered 10.
    assert dest_summary.get("source_row_count") == 6
    assert dest_summary.get("source_row_count_source") == "full_rescan_rows"


def test_checkpoint_roundtrips_cumulative_quarantine_counts():
    """rejected_rows AND coerced_null_rows must survive persist/reload so a
    resume can restore cumulative quarantine for Gate-8 conservation."""
    cp = Checkpoint(job_id="j", chunk_index=2, rows_processed=5,
                    rejected_rows=7, coerced_null_rows=3)
    reloaded = Checkpoint.from_dict(cp.to_dict())
    assert reloaded.rejected_rows == 7
    assert reloaded.coerced_null_rows == 3


def test_resume_restores_first_pass_quarantine_for_conservation(monkeypatch):
    """Gate-8 hole: a resumed pass that starts rejected/coerced at 0 forgets the
    rows the first pass quarantined, so source - dropped over-expects delivery and
    a correct load fails. The counts must be restored from the checkpoint."""
    from transfer.file_stream import stream_file_to_database
    from transfer.models import EndpointConfig

    def fake_write_batch(*args, **kwargs):
        rows, _first = _stream_write_view(args, kwargs)
        return rows, "checksum", {"rejected_rows": 0}  # this pass is clean

    monkeypatch.setattr("transfer.file_stream._write_batch", fake_write_batch)
    monkeypatch.setattr("transfer.file_stream.CHUNK_SIZE", 2)

    destination = EndpointConfig(
        kind="database", format="sqlite",
        connection_string="sqlite:///:memory:", table="import",
    )
    content = _csv_bytes(10)
    # First pass committed chunk 0 (2 rows) and quarantined 3, coerced 1.
    checkpoint = Checkpoint(
        job_id="resume-q", chunk_index=1, rows_processed=2,
        rejected_rows=3, coerced_null_rows=1,
    )

    _, _, dest_summary, _ = stream_file_to_database(
        content=content,
        filename="rows.csv",
        destination=destination,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        job_id="resume-q",
        checkpoint=checkpoint,
        checkpoint_service=_MemoryCheckpointService(),
        stream_contracts=[{"selected": True, "primary_key": ["id"], "sync_mode": "full_refresh_append"}],
        sync_mode="full_refresh_append",
    )

    # Cumulative quarantine from the first pass is restored, not reset to 0.
    assert dest_summary.get("rejected_rows") == 3
    assert dest_summary.get("coerced_null_rows") == 1
