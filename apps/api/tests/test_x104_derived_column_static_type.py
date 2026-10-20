"""X-104 — derived columns are typed by the expression, not the sample.

At 616df100 the first batch's observed values sized the produced column:
round(amt*qty,2) over five small totals declared DECIMAL(4,2), so run 2
refused a legitimate 9999999.9999 × 1000 on every DECIMAL engine while
SQLite landed it — the engines disagreed. Static typing bounds the column
by the AST (round(NUMERIC(12,4)*INT,2) → DECIMAL(31,2); division → the
declared wide default DECIMAL(38,10)), same rule on every engine, and the
plan lists each derived column's declared type via shape_derived_columns.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

from services.shape_apply import derived_static_types
from services.shape_models import ShapeRecipe

_SRC_COLS = ["id", "email", "amt", "qty", "d", "name", "nick"]
_SRC_TYPES = {
    "id": "INTEGER",
    "email": "TEXT",
    "amt": "NUMERIC(12,4)",
    "qty": "INTEGER",
    "d": "DATE",
    "name": "TEXT",
    "nick": "TEXT",
}


def _recipe() -> dict:
    """The live_616d_rexpr recipe's numeric + produced columns."""
    return {
        "steps": [
            {
                "op": "derive_column",
                "options": {
                    "to": "total",
                    "expression": "round(amt * qty, 2)",
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "unit",
                    "expression": "if(qty = 0, null, round(amt / qty, 4))",
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "tier",
                    "expression": (
                        "case when amt >= 100 then 'gold' "
                        "when amt >= 10 then 'silver' else 'bronze' end"
                    ),
                },
            },
        ]
    }


def test_static_types_from_operand_precision_not_sample():
    r = ShapeRecipe.parse(_recipe(), source_columns=_SRC_COLS)
    types = derived_static_types(r, _SRC_TYPES)
    # round(NUMERIC(12,4) * INTEGER(19,0), 2): product (32,4), int digits
    # 28 + scale 2 + 1 carry headroom → fits 9,999,999,999.90 and far more.
    assert types["total"] == "DECIMAL(31,2)"
    # division takes the declared wide default, then round(...,4) binds it.
    assert types["unit"] == "DECIMAL(33,4)"
    assert types["tier"] == "TEXT"
    # Every declared int-digit budget covers the live run-2 values.
    for col in ("total", "unit"):
        precision, scale = [
            int(x)
            for x in types[col][8:-1].split(",")
        ]
        assert precision - scale >= 10, (col, types[col])


def _req(src: str, dst: str, sync_mode: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    recipe = _recipe()
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=src, table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=dst, table="out"
        ),
        sync_mode=sync_mode,
        validation_mode="strict",
        skip_preflight=True,
        stream_contracts=[
            {
                "name": "t",
                "selected": True,
                "sync_mode": sync_mode,
                "primary_key": "id",
            }
        ],
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=_SRC_COLS
        ).recipe_hash,
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "amt", "target": "amt", "target_type": "NUMERIC(12,4)"},
            {"source": "qty", "target": "qty", "target_type": "INTEGER"},
            {"source": "total", "target": "total", "target_type": "DECIMAL(31,2)"},
            {"source": "unit", "target": "unit", "target_type": "DECIMAL(33,4)"},
            {"source": "tier", "target": "tier", "target_type": "TEXT"},
        ],
    )


def _mk_src(path):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, amt NUMERIC(12,4), "
        "qty INTEGER, d TEXT, name TEXT, nick TEXT)"
    )
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?,?,?,?,?)",
        [
            (1, "a@x.com", 37.04, 1, "2024-02-01", "ada", None),
            (2, "b@y.com", 20.5, 4, "2024-02-02", "bob", "B"),
        ],
    )
    conn.commit()
    conn.close()


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    yield


def test_run2_grown_values_land_and_verify(env, tmp_path):
    """Run 1 small values; run 2 upsert with 9999999.9999 × 1000 — the values
    that failed on DECIMAL(4,2) at 616df100 must land and verify."""
    from src.transfer.engine import UniversalTransferEngine

    src = str(tmp_path / "s.db")
    dst = str(tmp_path / "d.db")
    _mk_src(src)
    engine = UniversalTransferEngine()
    r1 = engine.execute_tracked(
        _req(src, dst, "full_refresh_overwrite"), uuid.uuid4().hex[:24]
    )
    assert r1.success, r1.error
    derived = (r1.destination_summary or {}).get("shape_derived_columns") or []
    declared = {d["column"]: d for d in derived}
    assert declared["total"]["declared_type"] == "DECIMAL(31,2)", declared
    assert declared["unit"]["declared_type"] == "DECIMAL(33,4)", declared

    conn = sqlite3.connect(src)
    conn.execute("UPDATE t SET amt = 9999999.9999, qty = 1000 WHERE id = 1")
    conn.execute(
        "INSERT INTO t VALUES (6,'f@z.com',1234567.8912,1,'2024-02-06','fay',NULL)"
    )
    conn.commit()
    conn.close()
    r2 = engine.execute_tracked(_req(src, dst, "upsert"), uuid.uuid4().hex[:24])
    assert r2.success, r2.error
    rows = dict(
        sqlite3.connect(dst).execute("SELECT id, total FROM out").fetchall()
    )
    assert abs(float(rows[1]) - 9999999999.90) < 0.01, rows
    assert abs(float(rows[6]) - 1234567.89) < 0.01, rows


def test_wide_default_is_stated_in_the_summary(env, tmp_path):
    """An expression with no static bound takes DECIMAL(38,10) and the run
    summary flags it wide_default — declared, not measured."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    recipe = {
        "steps": [
            {
                "op": "derive_column",
                "options": {"to": "ratio", "expression": "amt / qty"},
            }
        ]
    }
    src = str(tmp_path / "s2.db")
    dst = str(tmp_path / "d2.db")
    _mk_src(src)
    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=src, table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=dst, table="out"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=_SRC_COLS
        ).recipe_hash,
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "ratio", "target": "ratio", "target_type": "DECIMAL(38,10)"},
        ],
    )
    r = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert r.success, r.error
    derived = (r.destination_summary or {}).get("shape_derived_columns") or []
    assert derived == [
        {
            "column": "ratio",
            "declared_type": "DECIMAL(38,10)",
            "wide_default": True,
        }
    ], derived


@pytest.mark.skipif(
    not os.environ.get("DATAFLOW_X104_LIVE"),
    reason="5-dest live matrix (MySQL/PG/MSSQL/DuckDB/SQLite) run on the host",
)
def test_live_matrix():
    pass
