"""X-87/X-88: one empty-CSV-cell rule on every engine.

X-87: an empty CSV field in a non-text column is NULL on every engine —
was FAIL_JOB 'Empty value cannot coerce to decimal' on SQL Server/DuckDB
while MySQL/SQLite/PG landed NULL.
X-88: an empty CSV field in a text column was '' on SQL Server/DuckDB but
NULL elsewhere. Documented rule now: '' → NULL by default; a per-stream
``empty_cell='keep'`` keeps '' only on text-family carriers. The run
summary names the rule and counts the cells.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

from services.read_options import ReadOptions, ReadOptionsError
from transfer.file_stream import file_empty_cell_bind


def test_default_empty_field_is_null_everywhere():
    for carrier in ("DECIMAL(12,4)", "INTEGER", "VARCHAR(50)", "TEXT", "string", ""):
        assert file_empty_cell_bind("", carrier) is None
    # Non-empty values are untouched.
    assert file_empty_cell_bind("0", "INTEGER") == "0"
    assert file_empty_cell_bind(None, "TEXT") is None


def test_keep_option_holds_empty_text_only():
    for carrier in ("VARCHAR(50)", "TEXT", "string", "LONGTEXT"):
        assert file_empty_cell_bind("", carrier, empty_cell="keep") == ""
    # '' is not a number — numeric carriers are NULL even under keep (X-87).
    for carrier in ("DECIMAL(12,4)", "INTEGER", "REAL", "DATE"):
        assert file_empty_cell_bind("", carrier, empty_cell="keep") is None


def test_read_options_validates_empty_cell():
    assert ReadOptions(empty_cell="keep").empty_cell == "keep"
    assert ReadOptions().empty_cell == "null"
    with pytest.raises(ReadOptionsError):
        ReadOptions(empty_cell="drop")


def test_stream_batches_apply_rule_with_schema():
    from transfer.file_stream import _apply_empty_cell_rule

    schema = {"amt": "decimal(12,4)", "name": "string", "qty": "integer"}
    counter = [0]
    rows = [{"amt": "", "name": "", "qty": "5"}, {"amt": "1.5", "name": "x", "qty": ""}]
    out = _apply_empty_cell_rule(rows, schema, "null", counter)
    assert isinstance(out, list)
    assert out[0] == {"amt": None, "name": None, "qty": "5"}
    assert out[1]["qty"] is None and out[1]["amt"] == "1.5"
    assert counter[0] == 3
    out2 = _apply_empty_cell_rule(
        [{"amt": "", "name": "", "qty": "5"}], schema, "keep", [0]
    )
    assert out2[0]["name"] == "" and out2[0]["amt"] is None


def test_recipe_leg_csv_empty_numeric_sqlite(tmp_path):
    """CSV '' numeric under a recipe → SQLite lands NULL (was 'Empty value
    cannot coerce to decimal' through the recipe path)."""
    import src.transfer.engine as eng
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from test_cell_mysql_to_duckdb import _FakeMongo

    eng.get_mongodb_service = lambda *a, **k: _FakeMongo()
    dst = tmp_path / "dst.db"
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        source_filename="t.csv",
        source_content=b"id,name,amt\n1,ann,\n2,bob,4.5\n3,cid,\n",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        shape_recipe={"version": 1, "steps": [
            {"op": "derive_column", "options": {"to": "nm", "expression": "upper(name)"}},
        ]},
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "nm", "target": "nm", "target_type": "TEXT"},
            {"source": "amt", "target": "amt", "target_type": "REAL"},
        ],
    )
    res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert res.success, getattr(res, "error", None)
    conn = sqlite3.connect(tmp_path / "dst.db")
    try:
        rows = conn.execute("select id, amt from out order by id").fetchall()
    finally:
        conn.close()
    assert rows == [(1, None), (2, 4.5), (3, None)]


@pytest.mark.skipif(not os.environ.get("DATAFLOW_X87_LIVE"), reason="live engines — user-run")
def test_x87_live_5engine_csv_matrix():
    pytest.fail("harness: '' in numeric + text CSV cols x 5 dests, clean + tamper")
