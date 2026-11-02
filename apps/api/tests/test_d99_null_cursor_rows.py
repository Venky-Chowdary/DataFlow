"""D99 — NULL cursor values are silently skipped forever.

An incremental run bounds the delta with ``cursor > watermark`` — and
``NULL > x`` is never true, so NULL-cursor rows are absent from every delta
while the job reports verified. Validate must warn (block under strict) with
the live count, and the job must report how many rows it skipped.
"""
from __future__ import annotations

import sqlite3

import pytest

from services.preflight_service import run_file_preflight


def _sqlite_src(tmp_path, rows):
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = str(tmp_path / "d99_src.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE e (id INTEGER PRIMARY KEY, v INTEGER, name TEXT)")
    for r in rows:
        conn.execute("INSERT INTO e (id, v, name) VALUES (?, ?, ?)", r)
    conn.commit()
    conn.close()
    return path


def _gate(result, gate_id):
    return next((g for g in result["gates"] if g["id"] == gate_id), None)


def _preflight(tmp_path, *, sync_mode="incremental_append", validation_mode="strict",
               rows=None, column_nullability=None):
    src = _sqlite_src(
        tmp_path,
        rows if rows is not None else [(1, 10, "a"), (2, None, "b"), (3, 11, "c")],
    )
    return run_file_preflight(
        columns=["id", "v", "name"],
        column_types={"id": "INTEGER", "v": "INTEGER", "name": "VARCHAR(50)"},
        column_nullability=column_nullability or {"id": False, "v": True, "name": True},
        row_count=None,
        mappings=[
            {"source": c, "target": c, "confidence": 1.0}
            for c in ["id", "v", "name"]
        ],
        sample_rows=[{"id": 1, "v": 10, "name": "a"}],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="sqlite",
        source_config={"type": "sqlite", "database": src},
        source_table="e",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="sqlite",
        destination_table="e_copy",
        sync_mode=sync_mode,
        stream_contracts=[
            {"selected": True, "cursor_column": "v", "primary_key": "id"}
        ],
        validation_mode=validation_mode,
    )


def test_preflight_warns_nullable_cursor_with_nulls(tmp_path):
    # strict validation: NULL-cursor rows block with the live count
    result = _preflight(tmp_path, validation_mode="strict")
    gate = _gate(result, "g_nullable_cursor")
    assert gate is not None, [g["id"] for g in result["gates"]]
    assert gate["status"] == "block"
    assert "1" in gate["message"]
    assert gate["details"]["null_count"] == 1
    assert gate["details"]["cursor_column"] == "v"
    assert result["passed"] is False
    assert any(b["id"] == "g_nullable_cursor" for b in result["blockers"])

    # lenient validation: same evidence, warn not block
    result2 = _preflight(tmp_path / "lenient", validation_mode="lenient")
    gate2 = _gate(result2, "g_nullable_cursor")
    assert gate2 is not None
    assert gate2["status"] == "warn"
    assert gate2["details"]["null_count"] == 1


def test_preflight_nullable_cursor_clean_passes(tmp_path):
    result = _preflight(tmp_path, rows=[(1, 10, "a"), (2, 20, "b")])
    gate = _gate(result, "g_nullable_cursor")
    # nullable column, zero NULLs → honest warning about the future risk
    assert gate is not None
    assert gate["status"] == "warn"
    assert "nullable" in gate["message"].lower()


def test_null_cursor_probe_counts_sql(tmp_path):
    from services.null_cursor_probe import probe_null_cursor_count

    src = _sqlite_src(
        tmp_path, [(1, 10, "a"), (2, None, "b"), (3, None, "c"), (4, 11, "d")]
    )
    res = probe_null_cursor_count(
        source_config={"type": "sqlite", "database": src},
        source_table="e",
        cursor_column="v",
    )
    assert res.ran and res.status == "ran"
    assert res.null_count == 2

    res_missing = probe_null_cursor_count(
        source_config={"type": "sqlite", "database": src},
        source_table="e",
        cursor_column="nonexistent_col",
    )
    # An unmeasurable probe reports itself — never invents a count.
    assert res_missing.status in {"error", "skipped_unsupported"}
    assert not res_missing.ran


@pytest.mark.parametrize("sync_mode", ["incremental_upsert", "incremental_append"])
def test_incremental_reports_skipped_null_cursor_rows(tmp_path, sync_mode):
    """The job surface carries the skipped-NULL count and names the cursor.

    The stream-side stamp lives on dest_summary as ``skipped_null_cursor_rows``
    + ``null_cursor_note``; the count itself comes from the same probe the
    gate uses, so job and Validate never disagree.
    """
    result = _preflight(tmp_path, sync_mode=sync_mode)
    gate = _gate(result, "g_nullable_cursor")
    assert gate is not None
    assert gate["status"] == "block"
    assert gate["details"]["null_count"] == 1

    # Job-side: the stamp the run writes onto dest_summary reports the same
    # count and states that only a full refresh reaches those rows.
    from services.null_cursor_probe import stamp_skipped_null_cursor_rows

    summary: dict = {}
    stamp_skipped_null_cursor_rows(summary, 1, "v")
    assert summary["skipped_null_cursor_rows"] == 1
    assert "full refresh" in summary["null_cursor_note"]
    assert "'v'" in summary["null_cursor_note"]
