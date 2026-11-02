"""A resumed pass must reconcile against the whole population, not its tail.

Resume slices the source past the rows an earlier attempt already committed, so
this pass reads (and digests) only the remainder while the destination read-back
is always full-table. Comparing those two scopes reported a checksum mismatch —
and a short row count — on a destination that was byte-for-byte correct.

Two honest outcomes are required:

* buffered resume re-supplies the full source, so full population proof still
  applies (``full_checksum``);
* streaming resume cannot re-read the committed prefix, so cardinality is
  proven and population fidelity is explicitly *not* claimed.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconciliation import canonical_checksum_from_iter  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.reconcile_step import run_reconciliation  # noqa: E402

ROWS = 1_000
SKIPPED = 600  # committed by the attempt that died
COLUMNS = ["id", "name"]

MAPPINGS = [{"source": c, "target": c} for c in COLUMNS]


def _rows() -> list[dict]:
    return [{"id": i, "name": f"row-{i}"} for i in range(1, ROWS + 1)]


def _seed(tmp_path: Path) -> Path:
    db = tmp_path / "dest.db"
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?)",
            [(i, f"row-{i}") for i in range(1, ROWS + 1)],
        )
    conn.close()
    return db


def _endpoint(db: Path) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="sqlite", database=str(db), table="orders"
    )


def _tail_checksum() -> str:
    """Digest of only the rows the resumed pass wrote — the scope-bug value."""
    return canonical_checksum_from_iter(
        [[i, f"row-{i}"] for i in range(SKIPPED + 1, ROWS + 1)],
        COLUMNS,
        dest_db_type="sqlite",
    )


def test_buffered_resume_proves_the_full_population(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    report = run_reconciliation(
        endpoint=_endpoint(db),
        records=_rows(),  # engine re-supplies the whole population on resume
        columns=COLUMNS,
        rows_written=ROWS - SKIPPED,
        writer_checksum=_tail_checksum(),
        dest_summary={
            "table": "orders",
            "sync_mode": "incremental_upsert",
            "source_row_count": ROWS - SKIPPED,
            "resumed_from": SKIPPED,
            "resume_full_source_rows": ROWS,
        },
        mappings=MAPPINGS,
        validation_mode="strict",
    )
    assert report["source_rows"] == ROWS, report
    assert report["target_rows"] == ROWS, report
    assert report["checksum_match"] is True, report
    assert report["assurance_level"] == "full_checksum", report
    assert report["passed"] is True, report


def test_streaming_resume_proves_rows_and_declines_population_fidelity(
    tmp_path: Path,
) -> None:
    db = _seed(tmp_path)
    report = run_reconciliation(
        endpoint=_endpoint(db),
        records=[],  # a stream cannot re-read the committed prefix
        columns=COLUMNS,
        rows_written=ROWS - SKIPPED,
        writer_checksum=_tail_checksum(),
        dest_summary={
            "table": "orders",
            "streaming": True,
            "sync_mode": "incremental_upsert",
            "source_row_count": ROWS - SKIPPED,
            "resumed_from": SKIPPED,
        },
        mappings=MAPPINGS,
        validation_mode="strict",
    )
    assert report["source_rows"] == ROWS, report
    assert report["target_rows"] == ROWS, report
    assert report["passed"] is True, report
    assert report["checksum_match"] is False, report
    assert report["migration_proven"] is False, report
    assert report["coverage"] == "row_count", report
    assert "Resumed after 600" in report["message"], report


def test_streaming_resume_still_fails_a_short_destination(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("DELETE FROM orders WHERE id > 950")
    conn.close()
    report = run_reconciliation(
        endpoint=_endpoint(db),
        records=[],
        columns=COLUMNS,
        rows_written=ROWS - SKIPPED,
        writer_checksum=_tail_checksum(),
        dest_summary={
            "table": "orders",
            "streaming": True,
            "sync_mode": "full_refresh_overwrite",
            "source_row_count": ROWS - SKIPPED,
            "resumed_from": SKIPPED,
        },
        mappings=MAPPINGS,
        validation_mode="strict",
    )
    assert report["passed"] is False, report
    assert report["target_rows"] == 950, report
    assert "Row count mismatch after resume" in report["message"], report
