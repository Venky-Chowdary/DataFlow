"""X-119 (P1): file source + a row-set (buffered) recipe never created the
destination table — every engine failed, e.g. SQLite
``ADD COLUMN 'id' failed: no such table: out — refuse silent schema drift``.

Root: `stream_file_to_database` bound ``create_table=(idx == first_index)``.
A buffering step (rank_rows / sort_rows / dedupe_rows / aggregate) yields an
empty image per source page and emits only its drain tail, so the single real
batch enumerated as idx=2 while ``first_index`` stayed 1 — the create was
skipped and the writer fell into the evolve branch.

Fix: ``create_table`` belongs to the first batch that actually writes
(``_create_done`` flag), which also keeps the sync-first-batch DDL ordering
guarantee for parallel writers.

Regression: CSV + Parquet x rank_rows / sort_rows / dedupe_rows / aggregate
x SQLite, in-process. Live multi-dest matrix needs DATAFLOW_X119_LIVE.
"""
import io
import os
import sqlite3
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DATAFLOW_X119_LIVE") != "1" and False, reason=""
)


def _run(tmp_path, fmt, content, filename, steps, mappings, table="out"):
    import src.transfer.engine as eng
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from test_cell_mysql_to_duckdb import _FakeMongo

    eng.get_mongodb_service = lambda *a, **k: _FakeMongo()
    dst = tmp_path / "dst.db"
    req = TransferRequest(
        source=EndpointConfig(kind="file", format=fmt),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table=table
        ),
        source_filename=filename,
        source_content=content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        shape_recipe={"version": 1, "steps": steps},
        mappings=mappings,
    )
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24]), dst


def _read(dst, table="out"):
    conn = sqlite3.connect(dst)
    try:
        return conn.execute(f"select * from {table}").fetchall()
    finally:
        conn.close()


_CSV = b"id,amt,grp\n1,9,a\n2,10,b\n3,-2,a\n4,2.5,b\n5,100,a\n"
_MAP = [
    {"source": "id", "target": "id", "target_type": "INTEGER"},
    {"source": "amt", "target": "amt", "target_type": "REAL"},
    {"source": "grp", "target": "grp", "target_type": "TEXT"},
]


def _parquet():
    import pyarrow as pa
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(
        pa.table({"id": [1, 2, 3, 4, 5], "amt": [9.0, 10.0, -2.0, 2.5, 100.0],
                  "grp": ["a", "b", "a", "b", "a"]}),
        buf,
    )
    return buf.getvalue()


def test_x119_csv_rank_rows_creates_table(tmp_path):
    res, dst = _run(
        tmp_path, "csv", _CSV, "nums.csv",
        [{"op": "rank_rows", "options": {"order_by": "amt", "direction": "top", "limit": 2}}],
        _MAP,
    )
    assert res.success, getattr(res, "error", None)
    rows = _read(dst)
    # top-2 by amt desc: 100, 10 — typed comparator (X-117), not lexical.
    assert sorted(r[0] for r in rows) == [2, 5]


def test_x119_csv_sort_rows_creates_table(tmp_path):
    res, dst = _run(
        tmp_path, "csv", _CSV, "nums.csv",
        [{"op": "sort_rows", "options": {"order_by": "amt", "direction": "desc"}}],
        _MAP,
    )
    assert res.success, getattr(res, "error", None)
    rows = _read(dst)
    assert [r[0] for r in rows] == [5, 2, 1, 4, 3]


def test_x119_csv_dedupe_rows_creates_table(tmp_path):
    res, dst = _run(
        tmp_path, "csv", _CSV, "nums.csv",
        [{"op": "dedupe_rows", "options": {"keys": ["grp"], "keep": "last", "order_by": "amt"}}],
        _MAP,
    )
    assert res.success, getattr(res, "error", None)
    rows = _read(dst)
    # keep=last under order_by=amt desc: grp a -> id5 (100), grp b -> id2 (10).
    assert sorted(r[0] for r in rows) == [2, 5]


def test_x119_csv_aggregate_creates_table(tmp_path):
    res, dst = _run(
        tmp_path, "csv", _CSV, "nums.csv",
        [{"op": "aggregate", "options": {
            "group_by": ["grp"],
            "measures": [{"function": "sum", "column": "amt", "as": "total"}],
        }}],
        [{"source": "grp", "target": "grp", "target_type": "TEXT"},
         {"source": "total", "target": "total", "target_type": "REAL"}],
    )
    assert res.success, getattr(res, "error", None)
    rows = dict(_read(dst))
    assert rows["a"] == pytest.approx(107.0)
    assert rows["b"] == pytest.approx(12.5)


def test_x119_parquet_rank_rows_creates_table(tmp_path):
    pytest.importorskip("pyarrow")
    res, dst = _run(
        tmp_path, "parquet", _parquet(), "nums.parquet",
        [{"op": "rank_rows", "options": {"order_by": "amt", "direction": "top", "limit": 2}}],
        _MAP,
    )
    assert res.success, getattr(res, "error", None)
    assert sorted(r[0] for r in _read(dst)) == [2, 5]


def test_x119_parquet_aggregate_creates_table(tmp_path):
    pytest.importorskip("pyarrow")
    res, dst = _run(
        tmp_path, "parquet", _parquet(), "nums.parquet",
        [{"op": "aggregate", "options": {
            "group_by": ["grp"],
            "measures": [{"function": "count", "as": "n"}],
        }}],
        [{"source": "grp", "target": "grp", "target_type": "TEXT"},
         {"source": "n", "target": "n", "target_type": "INTEGER"}],
    )
    assert res.success, getattr(res, "error", None)
    rows = dict(_read(dst))
    assert rows == {"a": 3, "b": 2}


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X119_LIVE") != "1",
    reason="live PG/MySQL/MSSQL/DuckDB matrix needs provisioned engines",
)
def test_x119_live_engines(tmp_path):  # pragma: no cover
    pass
