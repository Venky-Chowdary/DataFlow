"""End-to-end data integrity test: messy CSV → SQLite with no silent loss."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from services.mapping_pipeline import run_mapping_pipeline  # noqa: E402
from src.transfer.adapters import parse_file_content, write_destination_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


CSV_MESSY_TEXT = """order_id,amount,created_at,active,notes
1,"$1,234.56","2024-06-01T12:00:00Z",yes,"Large payment"
2,"€2.000,00","2024-06-02 08:30:00",true,
3,"N/A","2024-06-03","false","bad amount"
4,"0","2024-06-04",1,
5,"1,000.00","2024-06-05",no,"thousand comma"
6,"1.000.000,89","2024-06-06",TRUE,
7,"(100.00)","2024-06-07",FALSE,
"""
CSV_MESSY = CSV_MESSY_TEXT.encode("utf-8")


def _records_to_samples(records, columns):
    samples: dict[str, list[str]] = {c: [] for c in columns}
    for row in records:
        for c in columns:
            samples[c].append(str(row.get(c, "")))
    return samples


def test_messy_csv_to_sqlite_preserves_values():
    records, columns, schema = parse_file_content(CSV_MESSY, "messy_orders.csv")
    samples = _records_to_samples(records, columns)

    plan = run_mapping_pipeline(
        source_columns=columns,
        target_columns=[],
        source_schemas=[{"name": c, "inferred_type": schema.get(c, "VARCHAR"), "samples": samples.get(c, [])} for c in columns],
        source_samples=samples,
        validation_mode="balanced",
        use_llm=False,
        destination_db_type="sqlite",
    )

    mappings = plan["mappings"]
    for m in mappings:
        if m["source"] == "amount":
            m["transform"] = "decimal"
        if m["source"] == "active":
            m["transform"] = "boolean"
        if m["source"] == "created_at":
            m["transform"] = "datetime"

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "messy.db"
        dest = EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table="messy_orders",
        )
        rows_written, ddl_log, meta = write_destination_database(
            dest,
            records,
            columns,
            schema,
            mappings,
            validation_mode="balanced",
        )

        assert rows_written >= 4, f"Expected at least 4 rows written, got {rows_written}: {meta}"
        assert meta.get("rejected_rows", 0) <= 2

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute('SELECT order_id, amount FROM "messy_orders" ORDER BY order_id')
        rows = cur.fetchall()
        conn.close()

        by_id = {r[0]: r[1] for r in rows if r[0] is not None}
        assert "$1,234.56" not in by_id.values()
        assert "1,234.56" not in by_id.values()
        assert "€2.000,00" not in by_id.values()
        assert "2.000,00" not in by_id.values()
        assert "1.000.000,89" not in by_id.values()

        assert 1 in by_id
        assert by_id[1] == 1234.56 or by_id[1] == "1234.56" or by_id[1] == "1234.5600000000"
        assert 2 in by_id
        assert by_id[2] == 2000.00 or by_id[2] == "2000.00" or by_id[2] == "2000.0000000000"
        assert 6 in by_id
        assert by_id[6] == 1000000.89 or by_id[6] == "1000000.89" or by_id[6] == "1000000.8900000000"
