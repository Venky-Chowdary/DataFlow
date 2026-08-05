"""The chunk ledger must skip committed work and report what really landed.

Two distinct failures live here.

The first is duplication: an insert-mode write that is interrupted after the
destination committed but before the client saw the acknowledgement. Replaying
it appends a second copy of every row while the job still reports success. The
ledger exists to make that replay a no-op.

The second is quieter and was live in the Postgres and MySQL writers. Those
readers returned a *boolean* — "was this chunk committed?" — so the retry
credited ``len(batch)``. A chunk that quarantined rows commits fewer rows than
it holds, so the replay over-reported the transfer and reconcile then compared
an inflated count against the real table. The ledger stores the count for
exactly this reason, and the reader must return it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from connectors import sqlite_writer
from connectors.write_resilience import (
    LEDGER_TABLE,
    ensure_raw_write_ledger,
    mark_raw_chunk_committed,
    raw_chunk_rows_written,
)

HEADERS = ["id", "name"]
MAPPINGS = [
    {"source": "id", "target": "id", "type": "integer"},
    {"source": "name", "target": "name", "type": "string"},
]
COLUMN_TYPES = {"id": "integer", "name": "string"}


def _rows(n: int) -> list[list[str]]:
    return [[str(i), f"row-{i}"] for i in range(1, n + 1)]


def _write(db: Path, rows: list[list[str]], **kwargs):
    return sqlite_writer.write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="people",
        headers=HEADERS,
        data_rows=rows,
        mappings=MAPPINGS,
        column_types=COLUMN_TYPES,
        **kwargs,
    )


def _count(db: Path, table: str = "people") -> int:
    conn = sqlite3.connect(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # nosec B608
    finally:
        conn.close()


class TestRawLedgerRoundTrip:
    """The dialect-parameterised ledger must behave identically everywhere."""

    def test_unrecorded_chunk_reads_as_none(self, tmp_path: Path) -> None:
        conn = sqlite3.connect(tmp_path / "l.db")
        try:
            cur = conn.cursor()
            ensure_raw_write_ledger(cur, dialect="sqlite")
            assert (
                raw_chunk_rows_written(
                    cur, dialect="sqlite", job_id="j", batch_key="b", chunk_idx=0
                )
                is None
            )
        finally:
            conn.close()

    def test_recorded_chunk_returns_its_row_count(self, tmp_path: Path) -> None:
        conn = sqlite3.connect(tmp_path / "l.db")
        try:
            cur = conn.cursor()
            ensure_raw_write_ledger(cur, dialect="sqlite")
            mark_raw_chunk_committed(
                cur,
                dialect="sqlite",
                job_id="j",
                batch_key="b",
                chunk_idx=0,
                rows_written=97,
            )
            assert (
                raw_chunk_rows_written(
                    cur, dialect="sqlite", job_id="j", batch_key="b", chunk_idx=0
                )
                == 97
            )
        finally:
            conn.close()

    def test_zero_rows_is_distinguishable_from_never_committed(
        self, tmp_path: Path
    ) -> None:
        """A chunk that quarantined every row still committed.

        Returning ``0`` rather than ``None`` is what stops the retry re-sending
        a batch the destination already rejected in full.
        """
        conn = sqlite3.connect(tmp_path / "l.db")
        try:
            cur = conn.cursor()
            ensure_raw_write_ledger(cur, dialect="sqlite")
            mark_raw_chunk_committed(
                cur,
                dialect="sqlite",
                job_id="j",
                batch_key="b",
                chunk_idx=4,
                rows_written=0,
            )
            recorded = raw_chunk_rows_written(
                cur, dialect="sqlite", job_id="j", batch_key="b", chunk_idx=4
            )
            assert recorded == 0
            assert recorded is not None
        finally:
            conn.close()

    def test_remarking_a_chunk_is_idempotent(self, tmp_path: Path) -> None:
        """Two workers racing the same chunk must not raise or double-count."""
        conn = sqlite3.connect(tmp_path / "l.db")
        try:
            cur = conn.cursor()
            ensure_raw_write_ledger(cur, dialect="sqlite")
            for _ in range(3):
                mark_raw_chunk_committed(
                    cur,
                    dialect="sqlite",
                    job_id="j",
                    batch_key="b",
                    chunk_idx=0,
                    rows_written=10,
                )
            assert cur.execute(f"SELECT COUNT(*) FROM {LEDGER_TABLE}").fetchone()[0] == 1  # nosec B608
        finally:
            conn.close()

    def test_ensure_is_safe_to_call_repeatedly(self, tmp_path: Path) -> None:
        conn = sqlite3.connect(tmp_path / "l.db")
        try:
            cur = conn.cursor()
            ensure_raw_write_ledger(cur, dialect="sqlite")
            mark_raw_chunk_committed(
                cur,
                dialect="sqlite",
                job_id="j",
                batch_key="b",
                chunk_idx=0,
                rows_written=5,
            )
            ensure_raw_write_ledger(cur, dialect="sqlite")
            assert (
                raw_chunk_rows_written(
                    cur, dialect="sqlite", job_id="j", batch_key="b", chunk_idx=0
                )
                == 5
            )
        finally:
            conn.close()

    def test_keys_are_scoped_so_jobs_do_not_collide(self, tmp_path: Path) -> None:
        conn = sqlite3.connect(tmp_path / "l.db")
        try:
            cur = conn.cursor()
            ensure_raw_write_ledger(cur, dialect="sqlite")
            mark_raw_chunk_committed(
                cur,
                dialect="sqlite",
                job_id="job-a",
                batch_key="t:0",
                chunk_idx=0,
                rows_written=10,
            )
            for job, key, idx in (
                ("job-b", "t:0", 0),
                ("job-a", "t:1", 0),
                ("job-a", "t:0", 1),
            ):
                assert (
                    raw_chunk_rows_written(
                        cur,
                        dialect="sqlite",
                        job_id=job,
                        batch_key=key,
                        chunk_idx=idx,
                    )
                    is None
                ), f"{job}/{key}/{idx} collided with an unrelated chunk"
        finally:
            conn.close()

    @pytest.mark.parametrize("dialect", ["postgresql", "mysql", "sqlite"])
    def test_every_dialect_emits_a_primary_keyed_ledger(self, dialect: str) -> None:
        """The composite key is what makes the lookup exact — never drop it."""
        statements: list[str] = []

        class RecordingCursor:
            def execute(self, sql, params=None):  # noqa: ANN001, ARG002
                statements.append(sql)

            def fetchone(self):
                return None

        ensure_raw_write_ledger(RecordingCursor(), dialect=dialect, schema=None)
        assert len(statements) == 1
        ddl = statements[0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "PRIMARY KEY (job_id, batch_key, chunk_idx)" in ddl
        assert LEDGER_TABLE in ddl

    def test_unknown_dialect_is_refused_rather_than_guessed(self) -> None:
        with pytest.raises(ValueError, match="No raw chunk ledger"):
            ensure_raw_write_ledger(object(), dialect="cassandra")

    def test_schema_qualified_reference_is_quoted(self) -> None:
        statements: list[str] = []

        class RecordingCursor:
            def execute(self, sql, params=None):  # noqa: ANN001, ARG002
                statements.append(sql)

        ensure_raw_write_ledger(
            RecordingCursor(), dialect="postgresql", schema='we"ird'
        )
        # The embedded quote must be doubled, not left to break out of the name.
        assert '"we""ird"' in statements[0]


class TestSqliteWriterSkipsCommittedChunks:
    """End-to-end: the writer must not duplicate rows a replay already landed."""

    def test_replaying_the_same_batch_does_not_duplicate_rows(
        self, tmp_path: Path
    ) -> None:
        db = tmp_path / "t.db"
        first = _write(
            db, _rows(5), job_id="job-1", write_batch_key="people:0"
        )
        assert first.ok and first.rows_written == 5
        assert _count(db) == 5

        second = _write(
            db, _rows(5), job_id="job-1", write_batch_key="people:0"
        )
        assert second.ok
        assert _count(db) == 5, "replay duplicated rows despite the ledger"
        assert second.rows_written == 5, "replay must still report what landed"
        assert any("already committed" in w for w in (second.warnings or [])), (
            "the operator has to be told the replay was skipped"
        )

    def test_a_write_without_a_job_id_has_no_ledger_to_consult(
        self, tmp_path: Path
    ) -> None:
        """Documents the honest limit that classify_replay_safety reports."""
        db = tmp_path / "t.db"
        _write(db, _rows(5))
        _write(db, _rows(5))
        assert _count(db) == 10

    def test_a_different_job_writing_the_same_rows_is_not_skipped(
        self, tmp_path: Path
    ) -> None:
        """The ledger dedupes retries, not distinct transfers."""
        db = tmp_path / "t.db"
        _write(db, _rows(5), job_id="job-1", write_batch_key="people:0")
        _write(db, _rows(5), job_id="job-2", write_batch_key="people:0")
        assert _count(db) == 10

    def test_upsert_mode_is_left_to_its_conflict_key(self, tmp_path: Path) -> None:
        """Upserts already converge, so the ledger must not suppress them.

        Skipping a replayed upsert would drop a legitimate value update.
        """
        db = tmp_path / "t.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()
        conn.close()

        _write(
            db,
            [["1", "before"]],
            job_id="job-1",
            write_batch_key="people:0",
            write_mode="upsert",
            conflict_columns=["id"],
            create_table=False,
        )
        _write(
            db,
            [["1", "after"]],
            job_id="job-1",
            write_batch_key="people:0",
            write_mode="upsert",
            conflict_columns=["id"],
            create_table=False,
        )

        conn = sqlite3.connect(db)
        try:
            name = conn.execute("SELECT name FROM people WHERE id = 1").fetchone()[0]
        finally:
            conn.close()
        assert name == "after", "ledger wrongly suppressed an upsert update"
        assert _count(db) == 1

    def test_ledger_lives_in_its_own_table_not_the_target(
        self, tmp_path: Path
    ) -> None:
        db = tmp_path / "t.db"
        _write(db, _rows(3), job_id="job-1", write_batch_key="people:0")
        conn = sqlite3.connect(db)
        try:
            tables = {
                r[0]
                for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        finally:
            conn.close()
        assert {"people", LEDGER_TABLE} <= tables
