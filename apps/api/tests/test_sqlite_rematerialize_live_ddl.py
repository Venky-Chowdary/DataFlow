"""SQLite rematerialize when PRAGMA affinity differs from Map stamps."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


def test_sqlite_rematerialize_when_pragma_real_vs_map_varchar(tmp_path: Path):
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "live_ddl.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE amounts (amount REAL)")
    conn.commit()
    conn.close()

    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="main",
        connection_string="",
        ssl=False,
        table_name="amounts",
        headers=["amount"],
        data_rows=[["12.50"], ["not-a-number"]],
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "target_type": "VARCHAR",
                "source_type": "VARCHAR",
            }
        ],
        column_types={"amount": "VARCHAR"},
        create_table=False,
        error_policy="quarantine",
        write_mode="insert",
    )
    assert result.ok is True, result.error
    # Fit numeric row lands; unfit text must quarantine (not silent TEXT insert).
    assert result.rows_written == 1
    assert result.rejected_details
    assert any(
        (d.get("column") or "").lower() == "amount" for d in result.rejected_details
    )

    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute("SELECT amount FROM amounts").fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert float(rows[0][0]) == pytest.approx(12.5)


def test_sqlite_create_new_refuses_partial_studio(tmp_path: Path):
    """Create-new SQLite + partial Studio — refuse Map VARCHAR invent."""
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "partial_studio.db"
    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="main",
        connection_string="",
        ssl=False,
        table_name="orders",
        headers=["id", "qty"],
        data_rows=[["1", "7"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
        ],
        column_types={"id": "VARCHAR", "qty": "VARCHAR"},
        create_table=True,
        destination_column_types={"id": "INTEGER"},
    )
    assert result.ok is False
    assert "qty" in (result.error or "").lower()
