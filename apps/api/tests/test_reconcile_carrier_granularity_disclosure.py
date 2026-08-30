"""A Gate-8 pass on a narrowed instant column must say what it could not prove.

Fingerprinting at the destination carrier's granularity is what stops a declared
narrowing (Snowflake ``TIMESTAMP_NTZ`` → MySQL ``DATETIME``) from reporting as a
checksum mismatch. That comparison proves every cell the destination *can* hold,
so the report names the columns whose fractional seconds were dropped rather
than presenting an unqualified green.
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

COLUMNS = ["id", "order_ts"]
ROWS = [(i, f"2024-01-0{i} 00:00:00.6543{i}1") for i in range(1, 6)]


def _seed(tmp_path: Path) -> Path:
    db = tmp_path / "dest.db"
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, order_ts TEXT)")
        conn.executemany("INSERT INTO orders VALUES (?, ?)", ROWS)
    conn.close()
    return db


def _reconcile(db: Path, target_ts_type: str) -> dict:
    mappings = [
        {"source": "id", "target": "id", "source_type": "BIGINT", "target_type": "BIGINT"},
        {
            "source": "order_ts",
            "target": "order_ts",
            "source_type": "TIMESTAMP_NTZ",
            "target_type": target_ts_type,
        },
    ]
    dest_types = {"id": "BIGINT", "order_ts": target_ts_type}
    source_checksum = canonical_checksum_from_iter(
        [list(r) for r in ROWS], COLUMNS, dest_db_type="sqlite", dest_types=dest_types
    )
    return run_reconciliation(
        endpoint=EndpointConfig(
            kind="database", format="sqlite", database=str(db), table="orders"
        ),
        records=[],
        columns=COLUMNS,
        rows_written=len(ROWS),
        writer_checksum=source_checksum,
        dest_summary={
            "table": "orders",
            "source_row_count": len(ROWS),
            "sync_mode": "full_refresh_overwrite",
            "column_types": dest_types,
        },
        mappings=mappings,
        source_schema={"id": "BIGINT", "order_ts": "TIMESTAMP_NTZ"},
        validation_mode="strict",
    )


def test_narrowed_instant_column_passes_and_is_named(tmp_path: Path) -> None:
    report = _reconcile(_seed(tmp_path), "DATETIME")
    assert report["passed"] is True, report
    rounded = report.get("carrier_rounded_columns")
    assert [c["column"] for c in rounded or []] == ["order_ts"], report
    assert "declared narrowing" in str(report.get("message") or ""), report
    assert "order_ts TIMESTAMP_NTZ → DATETIME" in str(report.get("message") or ""), report


def test_carrier_that_keeps_the_precision_claims_nothing_extra(tmp_path: Path) -> None:
    report = _reconcile(_seed(tmp_path), "DATETIME(6)")
    assert report["passed"] is True, report
    assert not report.get("carrier_rounded_columns"), report
    assert "declared narrowing" not in str(report.get("message") or ""), report
