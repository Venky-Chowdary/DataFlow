"""Snowflake COPY threshold, batch MERGE, and warehouse stream chunk sizing."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_writer import (  # noqa: E402
    COPY_THRESHOLD,
    _merge_batch_via_temp,
    write_mapped_rows,
)
from services.resilience import adaptive_chunk_size  # noqa: E402


def test_snowflake_copy_threshold_prefers_modest_batches():
    """Stream chunks often land ~hundreds–thousands of rows; 200 clears COPY."""
    assert COPY_THRESHOLD == 200 or COPY_THRESHOLD <= 200
    assert COPY_THRESHOLD >= 100


def test_warehouse_stream_chunk_clears_copy_threshold_for_wide_rows():
    """64MB warehouse budget keeps chunk_size ≥ COPY_THRESHOLD for ~2KB Mongo docs."""
    avg_row_size = 2000
    chunk = adaptive_chunk_size(
        20_000,
        avg_row_size,
        max_size=20_000,
        target_memory_bytes=64 * 1024 * 1024,
    )
    assert chunk >= COPY_THRESHOLD


def test_merge_batch_issues_single_merge_sql():
    cur = MagicMock()
    conn = MagicMock()
    conn.__class__.__name__ = "FakeSnowflakeConnection"  # force INSERT stage path
    rows = [
        ("k1", "a"),
        ("k2", "b"),
        ("k3", "c"),
    ]
    written = _merge_batch_via_temp(
        cur,
        "DEST",
        ["id", "val"],
        ["VARCHAR", "VARCHAR"],
        rows,
        ["id"],
        prefer_copy=True,
        conn=conn,
    )
    assert written == 3
    merge_calls = [
        c for c in cur.execute.call_args_list
        if c.args and isinstance(c.args[0], str) and c.args[0].lstrip().upper().startswith("MERGE")
    ]
    assert len(merge_calls) == 1
    sql = merge_calls[0].args[0]
    assert 'MERGE INTO "DEST"' in sql
    assert "USING" in sql.upper()


def test_upsert_uses_merge_batch_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "0")

    class FakeCursor:
        def __init__(self):
            self.statements: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            self.statements.append(str(sql))

        def executemany(self, sql, params=None):
            self.statements.append(str(sql))

        def fetchall(self):
            return []

    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor()

        def cursor(self):
            return self.cur

        def close(self):
            pass

    fake = FakeConn()
    monkeypatch.setattr(
        "connectors.snowflake_writer.get_connection",
        lambda **kwargs: fake,
    )
    # Avoid widen / information_schema paths failing on empty mock
    monkeypatch.setattr(
        "connectors.snowflake_writer._widen_existing_number_columns",
        lambda *a, **k: None,
    )

    result = write_mapped_rows(
        host="xy12345.us-east-1",
        port=443,
        database="ANALYTICS",
        username="user",
        password="pass",
        schema="PUBLIC",
        connection_string="",
        ssl=True,
        warehouse="COMPUTE_WH",
        table_name="df_upsert_batch",
        headers=["ID", "NAME"],
        data_rows=[["1", "a"], ["2", "b"], ["1", "a2"]],
        mappings=[
            {"source": "ID", "target": "id"},
            {"source": "NAME", "target": "name"},
        ],
        column_types={"ID": "TEXT", "NAME": "TEXT"},
        write_mode="upsert",
        conflict_columns=["id"],
        create_table=True,
    )
    assert result.ok, result.error
    assert result.load_method == "merge_batch"
    merges = [s for s in fake.cur.statements if s.lstrip().upper().startswith("MERGE")]
    assert len(merges) == 1


def test_benchmarks_page_labels_csv_sqlite_workload():
    """Proofs UI must not be readable as warehouse (Mongo→Snowflake) throughput."""
    page = Path(__file__).resolve().parents[2] / "web" / "src" / "pages" / "BenchmarksPage.tsx"
    text = page.read_text(encoding="utf-8")
    assert "CSV → SQLite" in text
    assert "synthetic CSV" in text
    assert "MongoDB→Snowflake" in text or "Mongo→Snowflake" in text
