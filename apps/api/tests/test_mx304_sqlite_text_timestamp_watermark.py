"""QA MX3-04 — a SQLite TEXT timestamp cursor compared lexically.

The SQLite source holds ISO timestamps as TEXT. Rows written as
``2025-01-01T00:01:00`` (run 1) set the watermark; rows later written as
``2025-01-01 03:21:00`` are newer in time but sort *below* the ``T`` form
(space < ``T``), so ``cursor > watermark`` skipped them. Live SQLite→PG.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from tests.test_rt02_incremental_append_l1_dest_count import (  # noqa: F401
    _count,
    _endpoint,
    _pg,
    isolated,
    pytestmark,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


@pytest.mark.parametrize("dest_kind", ["postgresql", "sqlite"])
def test_space_form_rows_after_a_t_form_watermark_are_read(isolated, tmp_path, dest_kind):  # noqa: F811
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"QA_Mx304_S_{suffix}", f"qa_mx304_d_{suffix}"
    db = tmp_path / "src.sqlite"
    with sqlite3.connect(db) as c:
        c.execute(f'CREATE TABLE "{src}" (id INTEGER PRIMARY KEY, updated_at TIMESTAMP)')
        c.executemany(
            f'INSERT INTO "{src}" VALUES (?, ?)',
            [(i, f"2025-01-01T{i // 60:02d}:{i % 60:02d}:00") for i in range(1, 101)],
        )
    source = EndpointConfig(
        kind="database", format="sqlite", connection_string=f"sqlite:///{db}", table=src
    )
    dest_db = tmp_path / "dst.sqlite"
    dest = (
        _endpoint(dst)
        if dest_kind == "postgresql"
        else EndpointConfig(
            kind="database", format="sqlite",
            connection_string=f"sqlite:///{dest_db}", table=dst,
        )
    )
    conn = _pg()

    def dest_count() -> int:
        if dest_kind == "postgresql":
            return _count(conn, dst)
        with sqlite3.connect(dest_db) as c:
            return c.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0]

    def run():
        job_id = "mx304" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=source,
                destination=dest,
                sync_mode="incremental_append",
                stream_contracts=[{
                    "name": src, "cursor_field": "updated_at", "primary_key": ["id"],
                    "sync_mode": "incremental_append", "selected": True,
                }],
                skip_preflight=True,
                validation_mode="strict",
            ),
            job_id,
        )

    try:
        first = run()
        assert first.success, first.error
        assert dest_count() == 100
        with sqlite3.connect(db) as c:
            # Same day, later in time (03:21 > 01:40), space separator.
            c.executemany(
                f'INSERT INTO "{src}" VALUES (?, ?)',
                [(100 + i, f"2025-01-01 03:{20 + i:02d}:00") for i in range(1, 11)],
            )
        second = run()
        assert second.success, second.error
        assert dest_count() == 110, "space-form rows newer than the watermark were skipped"
        third = run()  # quiet: the watermark must now sit on 03:30, not 01:40
        assert third.success, third.error
        assert dest_count() == 110
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        conn.close()


def test_sqlite_filtered_scan_reads_space_form_rows_past_a_t_form_watermark(tmp_path):
    """Stream path: the filtered scan bound compares by time, integers untouched."""
    from connectors.sqlite_reader import read_table_scan_batch

    db = tmp_path / "scan.sqlite"
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, updated_at TIMESTAMP, seq INTEGER)")
        c.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [
                (1, "2025-01-01T00:30:00", 9),
                (2, "2025-01-01T01:40:00", 10),
                (3, "2025-01-01 03:21:00", 11),
                (4, "2025-01-01 01:00:00", 12),
            ],
        )

    def scan(column: str, after: str) -> list[str]:
        batch = read_table_scan_batch(
            host="", port=0, database=str(db), username="", password="", schema="",
            connection_string="", ssl=False, table="t", scan_state={},
            filter_column=column, filter_after=after,
        )
        return sorted(row[0] for row in batch.rows)

    assert scan("updated_at", "2025-01-01T01:40:00") == ["3"]
    assert scan("seq", "10") == ["3", "4"]
