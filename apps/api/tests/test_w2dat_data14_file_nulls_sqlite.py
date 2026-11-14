"""DATA-14: SQLite takes NULLs in nullable typed columns from file sources.

Before the fix Parquet date32 / naive timestamp NULLs (and JSON nulls) were
flattened to "" in the Validate sample and refused as EMPTY_VALUE_NOT_NULLABLE
on SQLite, while PostgreSQL accepted them.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import sqlite3

import pytest


def _run(tmp_path, content: bytes, filename: str, fmt: str, table: str):
    from tests import w2dat_live as live
    from src.transfer.models import EndpointConfig

    return live.run(
        EndpointConfig(kind="file", format=fmt), live.ep_sqlite(tmp_path / "d14.db", table),
        "full_refresh_overwrite", key="id",
        extra_req={"source_content": content, "source_filename": filename},
    )


def test_project_row_cells_keeps_a_parser_null_as_sql_null():
    from services.value_serializer import SQL_NULL_SENTINEL, project_row_cells

    assert project_row_cells({"a": None, "b": ""}, ["a", "b"]) == [SQL_NULL_SENTINEL, ""]


@pytest.mark.parametrize("col", ["d", "tsn"])
def test_parquet_null_date_and_naive_timestamp_land_in_sqlite(tmp_path, col):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    arr = (pa.array([dt.date(2024, 1, 1), None], pa.date32()) if col == "d"
           else pa.array([dt.datetime(2024, 1, 1), None], pa.timestamp("us")))
    buf = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array([1, 2], pa.int64()), col: arr}), buf)
    res = _run(tmp_path, buf.getvalue(), f"{col}.parquet", "parquet", col)
    assert res.success, res.error
    with sqlite3.connect(tmp_path / "d14.db") as conn:
        rows = conn.execute(f'SELECT id, "{col}" FROM "{col}" ORDER BY id').fetchall()
    assert rows[1] == (2, None) and rows[0][1] is not None


def test_json_null_lands_in_sqlite(tmp_path):
    payload = json.dumps([{"id": 1, "when": "2024-01-01"}, {"id": 2, "when": None}]).encode()
    res = _run(tmp_path, payload, "rows.json", "json", "jn")
    assert res.success, res.error
    with sqlite3.connect(tmp_path / "d14.db") as conn:
        assert conn.execute('SELECT "when" FROM "jn" WHERE id = 2').fetchone() == (None,)
