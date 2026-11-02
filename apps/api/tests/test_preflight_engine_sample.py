"""DEF-B-004 (Gate-8 half): Validate reads its own sample when none was posted.

QA saw "Gate-8 cannot prove reconciliation without sample rows" on a MySQL
table holding 200 rows. The copilot's sampler had failed and the failure was
logged and dropped; the plan path swallowed its refetch the same way. Every
sample-judging gate then judged nothing and the block named no cause.

Validate now reads the sample through the reader Execute uses, and a read that
fails is named on Gate-8 so the operator fixes the read, not the mapping.
"""

from __future__ import annotations

import sqlite3

from services.preflight_sample import engine_sample_rows
from services.preflight_service import run_file_preflight


def _sqlite_source(tmp_path, rows: int = 200) -> str:
    path = str(tmp_path / "src.db")
    conn = sqlite3.connect(path)
    conn.execute("create table orders(id integer primary key, ts text)")
    conn.executemany(
        "insert into orders values(?, ?)",
        [(i, "2024-01-01 10:00:00") for i in range(rows)],
    )
    conn.commit()
    conn.close()
    return path


def _g8(result: dict) -> dict:
    return next(g for g in result["gates"] if g["id"] == "g8_reconciliation")


def test_engine_reader_samples_the_real_table(tmp_path):
    path = _sqlite_source(tmp_path)
    sample = engine_sample_rows(
        source_kind="database",
        source_format="sqlite",
        source_config={"type": "sqlite", "database": path},
        source_table="orders",
        limit=500,
    )
    assert sample.attempted is True
    assert sample.unavailable_reason == ""
    assert len(sample.rows) == 200
    assert sample.rows[0]["id"] == "0"


def test_failed_read_is_named_not_an_empty_list():
    sample = engine_sample_rows(
        source_kind="database",
        source_format="mysql",
        source_config={
            "type": "mysql",
            "host": "127.0.0.1",
            "port": 1,
            "database": "qa",
            "username": "u",
            "password": "p",
        },
        source_table="orders",
        limit=500,
    )
    assert sample.rows == []
    assert sample.unavailable_reason.startswith("source sample read failed:")


def test_file_and_callable_sources_are_not_read_here():
    assert engine_sample_rows(
        source_kind="file", source_table="t", source_config={"type": "csv"}, limit=10
    ).attempted is False
    assert engine_sample_rows(
        source_kind="database",
        source_table="t",
        source_config={"type": "mysql", "source_read_mode": "procedure"},
        limit=10,
    ).attempted is False


def _preflight(source_config: dict, *, source_format: str) -> dict:
    return run_file_preflight(
        columns=["id", "ts"],
        column_types={"id": "INTEGER", "ts": "TEXT"},
        row_count=200,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "ts", "target": "ts", "confidence": 1.0},
        ],
        sample_rows=[],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format=source_format,
        source_config=source_config,
        source_table="orders",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        destination_table="orders",
    )


def test_gate8_proves_on_the_engine_sample_when_none_was_posted(tmp_path):
    path = _sqlite_source(tmp_path)
    g8 = _g8(_preflight({"type": "sqlite", "database": path}, source_format="sqlite"))
    assert "without sample rows" not in g8["message"], g8
    assert g8["details"].get("source_rows") == 200, g8["details"]


def test_gate8_names_the_read_failure():
    g8 = _g8(
        _preflight(
            {
                "type": "mysql",
                "host": "127.0.0.1",
                "port": 1,
                "database": "qa",
                "username": "u",
                "password": "p",
            },
            source_format="mysql",
        )
    )
    assert g8["status"] == "block"
    assert "source sample read failed" in g8["message"]
    assert g8["details"].get("sample_unavailable_reason")


def _mongo_instant_preflight(tmp_path, stamp: str) -> dict:
    path = str(tmp_path / "instant.db")
    conn = sqlite3.connect(path)
    conn.execute("create table orders(id integer primary key, ts text)")
    conn.executemany(
        "insert into orders values(?, ?)", [(i, stamp % (i + 1)) for i in range(200)]
    )
    conn.commit()
    conn.close()
    return run_file_preflight(
        columns=["id", "ts"],
        column_types={"id": "INTEGER", "ts": "TIMESTAMPTZ(6)"},
        row_count=200,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "ts", "target": "ts", "confidence": 1.0, "target_type": "date"},
        ],
        sample_rows=[],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="sqlite",
        source_config={"type": "sqlite", "database": path},
        source_table="orders",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="mongodb",
        destination_table="orders",
        contract_primary_key="id",
    )


def _g3(result: dict) -> dict:
    return next(g for g in result["gates"] if g["id"] == "g3_schema_contract")


def test_mongo_create_new_passes_when_the_rows_hold_whole_milliseconds(tmp_path):
    result = _mongo_instant_preflight(tmp_path, "2024-01-01T10:00:00.%03d+00:00")
    assert _g3(result)["status"] != "block", _g3(result)
    assert result["passed"] is True, result.get("blockers")


def test_mongo_create_new_blocks_a_measured_microsecond_before_the_write(tmp_path):
    result = _mongo_instant_preflight(tmp_path, "2024-01-01T10:00:00.%06d+00:00")
    g3 = _g3(result)
    assert g3["status"] == "block"
    assert "below the millisecond" in " ".join(g3["details"]["issues"])
    assert result["passed"] is False
