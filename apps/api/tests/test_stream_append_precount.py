"""Streaming writers must stamp the pre-write destination count.

The buffered writer captures it in ``write_destination_database``, but files and
database sources go through the streaming paths, which call the batch writer
directly. Without the stamp Gate-8 cannot tell "appended 20 rows" from "table
already held 20 rows", so every streamed append reported an unproven delta.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from services.checkpoint_service import CheckpointService
from services.dest_precount import PRECOUNT_KEY
from src.transfer.file_stream import stream_file_to_database
from src.transfer.models import EndpointConfig
from src.transfer.stream import stream_database_transfer


class _FakeMongo:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs: object) -> bool:
        self.jobs.setdefault(job_id, {}).update(kwargs, status=status)
        return True


ROWS = "id,name\n1,alice\n2,bob\n"
SCHEMA = {"id": "integer", "name": "string"}
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.99},
    {"source": "name", "target": "name", "confidence": 0.99},
]


def _sqlite_dest(path: Path, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="sqlite", database=str(path), table=table
    )


def _seed(path: Path, table: str, rows: list[tuple[int, str]]) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER, name TEXT)")
        conn.executemany(f"INSERT INTO {table} VALUES (?, ?)", rows)


def test_file_stream_stamps_precount_of_existing_rows(tmp_path: Path) -> None:
    db = tmp_path / "dest.db"
    _seed(db, "landing", [(9, "seed"), (10, "seed")])

    _, _, summary, _ = stream_file_to_database(
        ROWS.encode(),
        "rows.csv",
        _sqlite_dest(db, "landing"),
        MAPPINGS,
        SCHEMA,
        sync_mode="full_refresh_append",
    )

    assert summary[PRECOUNT_KEY] == 2


def test_file_stream_strict_append_gate8_closes_on_precount_delta(tmp_path: Path) -> None:
    """CSV Full Append into existing rows must not fail Gate-8 on whole-table digest.

    Dest held 2; batch wrote 2; dest=4. Strict checksum compared those
    incomparable populations and marked a healthy write Failed. Dest-before
    delta is the identity.
    """
    from src.transfer.reconcile_step import run_reconciliation

    db = tmp_path / "dest.db"
    _seed(db, "landing", [(9, "seed"), (10, "seed")])
    dest = _sqlite_dest(db, "landing")
    written, _warnings, summary, _ddl = stream_file_to_database(
        ROWS.encode(),
        "rows.csv",
        dest,
        MAPPINGS,
        SCHEMA,
        sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    assert written == 2
    assert summary[PRECOUNT_KEY] == 2
    report = run_reconciliation(
        endpoint=dest,
        records=[
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ],
        columns=["id", "name"],
        rows_written=written,
        writer_checksum="writer-ack-not-dest",
        dest_summary={**summary, "sync_mode": "full_refresh_append"},
        mappings=MAPPINGS,
        source_schema=SCHEMA,
        validation_mode="strict",
    )
    assert report["passed"] is True, report.get("message")
    assert "checksum mismatch" not in str(report.get("message") or "").lower()
    assert report.get("migration_proven") is not True
    msg = str(report.get("message") or "").lower()
    scoped = str(report.get("checksum_scope") or "")
    # Either dest-before delta (incomparable whole-table hashes) or keyed-batch
    # cell proof — both close Gate-8 without failing the healthy append.
    assert scoped in {"whole_table_not_comparable", "written_batch_keys"} or (
        "append delta" in msg or "this run wrote" in msg
    )


def test_file_stream_precount_is_zero_for_create_new(tmp_path: Path) -> None:
    # A table that does not exist yet is a known-empty destination — that is a
    # proof of the "before" cardinality, not an unknown.
    db = tmp_path / "dest.db"
    _, _, summary, _ = stream_file_to_database(
        ROWS.encode(),
        "rows.csv",
        _sqlite_dest(db, "fresh"),
        MAPPINGS,
        SCHEMA,
        sync_mode="full_refresh_append",
    )

    assert summary[PRECOUNT_KEY] == 0


def test_database_stream_stamps_precount(tmp_path: Path) -> None:
    src_db = tmp_path / "src.db"
    dst_db = tmp_path / "dst.db"
    _seed(src_db, "people", [(1, "alice"), (2, "bob")])
    _seed(dst_db, "people", [(7, "seed")])

    source = EndpointConfig(
        kind="database", format="sqlite", database=str(src_db), table="people"
    )
    _, _, summary, _ = stream_database_transfer(
        source,
        _sqlite_dest(dst_db, "people"),
        MAPPINGS,
        SCHEMA,
        sync_mode="full_refresh_append",
        job_id="0" * 24,
        checkpoint_service=CheckpointService(_FakeMongo()),
    )

    assert summary[PRECOUNT_KEY] == 1
