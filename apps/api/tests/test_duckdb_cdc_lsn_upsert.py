"""Integration: older CDC LSN must not overwrite a newer DuckDB row."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.generic_sql import write_mapped_rows  # noqa: E402
from connectors.writer_common import DF_LSN_COL  # noqa: E402


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def test_duckdb_upsert_rejects_older_lsn():
    duckdb = pytest.importorskip("duckdb")
    path = f"/tmp/duckdb_cdc_lsn_{uuid.uuid4().hex[:8]}.duck"
    table_name = f"cdc_lsn_{uuid.uuid4().hex[:8]}"

    common = {
        "type": "duckdb",
        "host": "",
        "port": 0,
        "database": path,
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": "",
        "ssl": False,
        "table_name": table_name,
        "headers": ["id", "amount", DF_LSN_COL],
        "mappings": [
            _mapping("id", "id"),
            _mapping("amount", "amount"),
            _mapping(DF_LSN_COL, DF_LSN_COL),
        ],
        "column_types": {"id": "INTEGER", "amount": "TEXT", DF_LSN_COL: "TEXT"},
    }

    Path(path).unlink(missing_ok=True)

    r1 = write_mapped_rows(
        **common,
        data_rows=[["1", "new", "0/16B3748"]],
        create_table=True,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r1.ok, r1.error

    r2 = write_mapped_rows(
        **common,
        data_rows=[["1", "stale", "0/16B3700"]],
        create_table=False,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r2.ok, r2.error

    conn = duckdb.connect(path)
    try:
        row = conn.execute(
            f'SELECT amount, "{DF_LSN_COL}" FROM "{table_name}" WHERE id = 1'
        ).fetchone()
        conn.execute(f'DROP TABLE "{table_name}"')
    finally:
        conn.close()
    Path(path).unlink(missing_ok=True)

    assert row is not None
    assert str(row[0]) == "new"
    assert str(row[1]) == "0/16B3748"
