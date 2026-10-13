"""DEF-C-022 / DEF-A-018: a source measured empty validates as a 0-row run.

QA: an empty PostgreSQL table into MySQL, and a header-only orders file on a
scheduled route, were both blocked — Gate-5/8/9 "no sample rows", Gate-1
"file not parseable", and for the file a fidelity collapse and a paused
``type_change`` judged on ``string`` placeholders inferred from no values.

Empty is only granted when it was measured: the engine read the table and got
no row, or the whole population (stored upload, Execute's batch) is empty. A
read that failed, or a preview with no population behind it, still blocks.
"""

from __future__ import annotations

import sqlite3

import pytest

from services.preflight_sample import engine_sample_rows, peek_population_empty
from services.preflight_service import run_file_preflight
from services.source_schema_authority import empty_source_column_types

_COLS = ["order_id", "order_date", "quantity", "unit_price", "amount"]
_PREV = {
    "order_id": "INTEGER",
    "order_date": "DATE",
    "quantity": "INTEGER",
    "unit_price": "DECIMAL(10,2)",
    "amount": "DECIMAL(12,2)",
}
_DEST = {
    "order_id": "INTEGER",
    "order_date": "DATE",
    "quantity": "INTEGER",
    "unit_price": "NUMERIC(10,2)",
    "amount": "NUMERIC(12,2)",
}


def _gate(result: dict, gate_id: str) -> dict:
    return next(g for g in result["gates"] if g["id"] == gate_id)


def _blocks(result: dict) -> list[str]:
    return [g["id"] for g in result["gates"] if g["status"] == "block"]


def _sqlite(tmp_path, rows: int) -> str:
    path = str(tmp_path / f"src{rows}.db")
    conn = sqlite3.connect(path)
    conn.execute("create table e(id integer primary key, name text, amt numeric, ts text)")
    conn.executemany(
        "insert into e values(?,?,?,?)",
        [(i, f"n{i}", "1.25", "2024-01-01 10:00:00") for i in range(rows)],
    )
    conn.commit()
    conn.close()
    return path


def _table_preflight(source_config: dict, *, dest: str, source_format: str = "sqlite") -> dict:
    cols = ["id", "name", "amt", "ts"]
    return run_file_preflight(
        columns=cols,
        column_types={
            "id": "INTEGER",
            "name": "VARCHAR(50)",
            "amt": "NUMERIC(10,2)",
            "ts": "TIMESTAMPTZ",
        },
        row_count=0,
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in cols],
        sample_rows=[],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format=source_format,
        source_config=source_config,
        source_table="e",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type=dest,
        destination_table="e_copy",
        sync_mode="full_refresh_overwrite",
    )


def test_engine_read_of_an_empty_table_is_measured_empty(tmp_path):
    sample = engine_sample_rows(
        source_kind="database",
        source_format="sqlite",
        source_config={"type": "sqlite", "database": _sqlite(tmp_path, 0)},
        source_table="e",
        limit=500,
    )
    assert sample.attempted and sample.measured_empty and sample.rows == []
    assert sample.unavailable_reason == ""


def test_a_filter_that_matches_no_sampled_row_is_not_an_empty_source(tmp_path):
    sample = engine_sample_rows(
        source_kind="database",
        source_format="sqlite",
        source_config={"type": "sqlite", "database": _sqlite(tmp_path, 5)},
        source_table="e",
        limit=500,
        source_filter={"column": "name", "op": "eq", "value": "nobody"},
    )
    assert sample.rows == []
    assert sample.measured_empty is False
    assert "match the row filter" in sample.unavailable_reason


@pytest.mark.parametrize("dest", ["mysql", "postgresql", "sqlserver"])
def test_empty_table_passes_and_names_the_zero_row_proof(tmp_path, dest):
    result = _table_preflight(
        {"type": "sqlite", "database": _sqlite(tmp_path, 0)}, dest=dest
    )
    assert result["passed"] is True, _blocks(result)
    g8 = _gate(result, "g8_reconciliation")
    assert g8["details"]["source_measured_empty"] is True
    assert g8["details"]["source_rows"] == 0
    assert "0 rows" in _gate(result, "g5_dry_run")["message"]


def test_failed_read_of_a_table_still_blocks():
    result = _table_preflight(
        {
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 1,
            "database": "qa",
            "username": "u",
            "password": "p",
        },
        dest="mysql",
        source_format="postgresql",
    )
    assert {"g5_dry_run", "g8_reconciliation", "g9_data_integrity"} <= set(_blocks(result))
    assert "source sample read failed" in _gate(result, "g8_reconciliation")["message"]


def _header_only_file(population) -> dict:
    return run_file_preflight(
        columns=_COLS,
        column_types={c: "string" for c in _COLS},
        row_count=0,
        mappings=[
            {"source": c, "target": c, "confidence": 1.0, "target_type": _DEST[c]}
            for c in _COLS
        ],
        sample_rows=[],
        destination_connected=True,
        source_kind="file",
        source_format="csv",
        source_filename="orders.csv",
        destination_table_exists=True,
        destination_can_write=True,
        destination_column_types=_DEST,
        destination_db_type="postgresql",
        destination_table="orders",
        sync_mode="full_refresh_append",
        previous_source_columns=_COLS,
        previous_source_schema=_PREV,
        population_rows=population,
        rows_are_population=population is not None,
    )


@pytest.mark.parametrize("population", [[], iter(())], ids=["batch", "stream"])
def test_header_only_file_is_a_zero_row_run_not_a_type_change(population):
    result = _header_only_file(population)
    assert result["passed"] is True, [
        (g["id"], g["message"][:160]) for g in result["gates"] if g["status"] == "block"
    ]
    assert _gate(result, "g1_source")["status"] == "pass"
    assert "type_change" not in str(result.get("blockers"))


def test_header_only_preview_without_a_population_is_still_unproven():
    result = _header_only_file(None)
    assert result["passed"] is False
    assert "g8_reconciliation" in _blocks(result)


def test_a_new_header_on_an_empty_file_is_still_drift():
    types = empty_source_column_types(
        {c: "string" for c in [*_COLS, "coupon"]},
        [{"source": c, "target": c} for c in [*_COLS, "coupon"]],
        previous=_PREV,
        destination=_DEST,
    )
    assert types["order_id"] == "INTEGER"
    assert types["unit_price"] == "DECIMAL(10,2)"
    assert types["coupon"] == "string"


def test_population_peek_does_not_consume_rows():
    rows, empty = peek_population_empty(iter([{"a": 1}, {"a": 2}]))
    assert empty is False
    assert list(rows) == [{"a": 1}, {"a": 2}]
    assert peek_population_empty(None) == (None, False)
    assert peek_population_empty([])[1] is True
