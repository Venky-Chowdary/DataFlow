"""Clearing a destination for an overwrite forgets the job's write-ledger rows.

The ledger lets a retry skip batches already committed. A cancelled overwrite
restarted with the same job id cleared the table, then skipped the batches
the ledger still listed, and loaded 25 of 30 rows (route certification
cancel/resume matrix, sqlite/mysql destinations).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from connectors.write_resilience import LEDGER_TABLE
from src.transfer.engine_destination import _drop_destination_table
from src.transfer.models import EndpointConfig


def _destination(db: Path) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        table="t_dst",
        database=str(db),
        connection_string=f"sqlite:///{db}",
    )


def _seed(db: Path) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t_dst (id INTEGER PRIMARY KEY, qty INTEGER)")
        conn.executemany("INSERT INTO t_dst VALUES (?, ?)", [(i, i * 10) for i in range(1, 6)])
        conn.execute(
            f"CREATE TABLE {LEDGER_TABLE} (job_id TEXT, batch_key TEXT, chunk_idx INTEGER, "
            "rows_written INTEGER, row_start INTEGER, row_end INTEGER, attempt INTEGER, "
            "written_at TEXT, PRIMARY KEY (job_id, batch_key))"
        )
        conn.executemany(
            f"INSERT INTO {LEDGER_TABLE} VALUES (?, ?, 0, 5, 0, 4, 1, '2026-01-01')",
            [("job-a", "t_dst:0"), ("job-b", "t_dst:0")],
        )


def test_overwrite_clear_drops_only_this_jobs_ledger_rows(tmp_path: Path) -> None:
    db = tmp_path / "dst.db"
    _seed(db)

    assert _drop_destination_table(_destination(db), job_id="job-a")

    with sqlite3.connect(db) as conn:
        jobs = [r[0] for r in conn.execute(f"SELECT job_id FROM {LEDGER_TABLE}")]
    assert jobs == ["job-b"]
