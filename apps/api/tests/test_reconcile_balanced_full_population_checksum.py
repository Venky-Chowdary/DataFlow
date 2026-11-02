"""Balanced mode must digest the whole destination, not a 5000-row prefix.

The target checksum used to be capped at 5000 rows in ``balanced`` validation
mode while the source checksum always covered the full population, so any
balanced transfer above 5000 rows reported a checksum mismatch on byte-identical
data. Validation mode governs fail-closed severity, never comparison scope.
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

ROWS = 6_000
COLUMNS = ["id", "name"]


def _seed(tmp_path: Path) -> tuple[Path, str]:
    db = tmp_path / "dest.db"
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?)",
            [(i, f"row-{i}") for i in range(1, ROWS + 1)],
        )
    conn.close()
    checksum = canonical_checksum_from_iter(
        [[i, f"row-{i}"] for i in range(1, ROWS + 1)],
        COLUMNS,
        dest_db_type="sqlite",
    )
    return db, checksum


def _reconcile(db: Path, source_checksum: str, mode: str) -> dict:
    endpoint = EndpointConfig(
        kind="database", format="sqlite", database=str(db), table="orders"
    )
    return run_reconciliation(
        endpoint=endpoint,
        records=[],
        columns=COLUMNS,
        rows_written=ROWS,
        writer_checksum=source_checksum,
        dest_summary={
            "table": "orders",
            "source_row_count": ROWS,
            "sync_mode": "full_refresh_overwrite",
        },
        mappings=[{"source": c, "target": c} for c in COLUMNS],
        validation_mode=mode,
    )


def test_balanced_matches_an_identical_destination_beyond_5000_rows(
    tmp_path: Path,
) -> None:
    db, source_checksum = _seed(tmp_path)
    report = _reconcile(db, source_checksum, "balanced")
    assert report["target_rows"] == ROWS, report
    assert report["target_checksum"] == source_checksum, report
    assert report["checksum_match"] is True, report
    assert report["passed"] is True, report


def test_balanced_still_detects_a_real_mismatch(tmp_path: Path) -> None:
    db, source_checksum = _seed(tmp_path)
    # Corrupt one row past the old 5000-row cap: a prefix digest would miss it.
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("UPDATE orders SET name = 'tampered' WHERE id = 5999")
    conn.close()
    report = _reconcile(db, source_checksum, "balanced")
    assert report["checksum_match"] is False, report
    assert report["passed"] is False, report


def test_strict_and_balanced_agree_on_scope(tmp_path: Path) -> None:
    db, source_checksum = _seed(tmp_path)
    balanced = _reconcile(db, source_checksum, "balanced")
    strict = _reconcile(db, source_checksum, "strict")
    assert balanced["target_checksum"] == strict["target_checksum"], (
        balanced["target_checksum"],
        strict["target_checksum"],
    )
