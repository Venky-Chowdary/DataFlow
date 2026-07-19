"""CI-friendly zero-loss matrix: bad cells quarantine, good cells land (SQLite)."""

from __future__ import annotations

from pathlib import Path

import pytest

from connectors import sqlite_writer


@pytest.fixture()
def sqlite_db(tmp_path: Path) -> Path:
    return tmp_path / "zero_loss.db"


def test_bad_int_quarantined_good_rows_written(sqlite_db: Path):
    headers = ["id", "amount"]
    rows = [
        ["1", "10"],
        ["2", "not-a-number"],
        ["3", "30"],
    ]
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "amount", "target": "amount", "confidence": 1.0, "target_type": "INTEGER"},
    ]
    result = sqlite_writer.write_mapped_rows(
        host=str(sqlite_db),
        port=0,
        database=str(sqlite_db),
        username="",
        password="",
        schema="",
        connection_string=f"sqlite:///{sqlite_db}",
        ssl=False,
        warehouse="",
        table_name="t_matrix",
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        column_types={"id": "INTEGER", "amount": "INTEGER"},
        error_policy="quarantine",
        create_table=True,
    )
    assert result.ok, result.error
    assert result.rejected_details, "bad int must produce rejected_details"
    assert any("amount" in str(d.get("column", "")).lower() or "not-a-number" in str(d.get("value", ""))
               for d in result.rejected_details)
    # At least the clean rows must land — never silent total loss.
    assert result.rows_written >= 2


def test_iso_datetime_coerced_not_rejected_on_sqlite(sqlite_db: Path):
    headers = ["id", "ts"]
    rows = [["1", "2024-08-09T01:58:42Z"]]
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "ts", "target": "ts", "confidence": 1.0, "target_type": "DATETIME"},
    ]
    result = sqlite_writer.write_mapped_rows(
        host=str(sqlite_db),
        port=0,
        database=str(sqlite_db),
        username="",
        password="",
        schema="",
        connection_string=f"sqlite:///{sqlite_db}",
        ssl=False,
        warehouse="",
        table_name="t_ts",
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        column_types={"id": "INTEGER", "ts": "VARCHAR"},
        error_policy="quarantine",
        create_table=True,
    )
    assert result.ok, result.error
    assert result.rows_written == 1
