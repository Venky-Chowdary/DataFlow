"""X-1: database-sourced '' and whitespace-only text are real values.

Cleanse/expression text steps must NOT silently convert '' or whitespace-only
values to NULL on database sources (a spreadsheet reader's empty cell is a
different thing). Oracle destinations are the native exception ('' IS NULL
there) — not covered here.

Live read-back per destination, never checksum-only.
"""

import os
import sqlite3
import uuid

import pytest
import psycopg2

import src.transfer.engine as engine_mod
from src.transfer.models import EndpointConfig, TransferRequest
from services.shape_models import ShapeRecipe

from tests.test_scd2_pg_live import _engine_env, _exec, _pg

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATAFLOW_PG_TEST_DSN"), reason="no live PG"
)

_TEXT_STEPS = {
    "trim": {"op": "trim", "column": "v"},
    "upper": {"op": "case", "column": "v", "options": {"mode": "upper"}},
    "lower": {"op": "case", "column": "v", "options": {"mode": "lower"}},
    "replace": {
        "op": "replace",
        "column": "v",
        "options": {"search": "x", "replacement": "y"},
    },
    "title": {"op": "case", "column": "v", "options": {"mode": "title"}},
}

# Source population: id -> raw v. '' and '   ' must reach the destination
# as real stored values, never NULL.
_SRC_ROWS = {1: "", 2: "   ", 3: "x1", 4: None, 5: "MiXeD"}


def _make_src(cur, tbl):
    cur.execute(f"CREATE TABLE {tbl} (id INT PRIMARY KEY, v TEXT)")
    for i, v in _SRC_ROWS.items():
        cur.execute(
            f"INSERT INTO {tbl} (id, v) VALUES (%s, %s)", (i, v)
        )


def _expect(step_name: str) -> dict[int, object]:
    """Expected destination values per row (database semantics)."""
    if step_name == "trim":
        # trim('   ') -> '' (a real value), '' stays '', NULL stays NULL
        return {1: "", 2: "", 3: "x1", 4: None, 5: "MiXeD"}
    if step_name == "upper":
        return {1: "", 2: "   ", 3: "X1", 4: None, 5: "MIXED"}
    if step_name == "lower":
        return {1: "", 2: "   ", 3: "x1", 4: None, 5: "mixed"}
    if step_name == "replace":
        return {1: "", 2: "   ", 3: "y1", 4: None, 5: "MiXeD"}
    if step_name == "title":
        return {1: "", 2: "   ", 3: "X1", 4: None, 5: "Mixed"}
    raise AssertionError(step_name)


def _ep(parsed, table):
    return EndpointConfig(
        kind="database", format="postgresql",
        host=parsed["host"], port=parsed["port"],
        database=parsed["database"], username=parsed["username"],
        password=parsed["password"], table=table,
    )


@pytest.mark.parametrize("step_name", sorted(_TEXT_STEPS))
def test_x1_blank_text_survives_text_steps_pg_dest(monkeypatch, step_name):
    """PG→PG: '' and '   ' are written as stored values, NULL stays NULL —
    verified by native read-back, not checksum."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    cols = ["id", "v"]
    tbl = "x1_" + uuid.uuid4().hex[:10]
    dst = tbl + "_d"
    try:
        _make_src(cur, tbl)
        conn.commit()
        recipe = {"steps": [_TEXT_STEPS[step_name]]}
        req = TransferRequest(
            source=_ep(parsed, tbl), destination=_ep(parsed, dst),
            sync_mode="full_refresh_overwrite",
            mappings=[
                {"source": c, "target": c, "confidence": 1.0,
                 "user_override": True, "transform": "none"}
                for c in cols
            ],
            skip_preflight=True, validation_mode="strict",
            shape_recipe=recipe,
            approved_shape_recipe_hash=ShapeRecipe.parse(
                recipe, source_columns=cols
            ).recipe_hash,
        )
        res = _exec(fake, req, prefix="x1_")
        assert res.success, res.error
        cur.execute(f"SELECT id, v FROM {dst} ORDER BY id")
        got = {r[0]: r[1] for r in cur.fetchall()}
        for i, want in _expect(step_name).items():
            assert got.get(i) == want, (
                f"{step_name} row {i}: want {want!r} got {got.get(i)!r} — "
                "''/whitespace collapsed or wrongly transformed"
            )
        # NULL stayed NULL, '' stayed '' — the two must be distinct in dest
        assert got.get(4) is None
        if step_name != "trim":
            assert got.get(1) == "" and got.get(2) == "   "
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {tbl}, {dst}")
        conn.close()


def test_x1_blank_text_pg_to_mysql_readback(monkeypatch):
    """PG→MySQL live read-back: '' and '   ' land as stored values."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    cols = ["id", "v"]
    tbl = "x1m_" + uuid.uuid4().hex[:10]
    dst = tbl + "_d"
    mysql_up = False
    try:
        import pymysql  # noqa: F401

        mconn = pymysql.connect(
            host="127.0.0.1", port=3306, user="root",
            password="dataflow", database="dataflow",
        )
        mysql_up = True
    except Exception:
        mconn = None
    if not mysql_up:
        pytest.skip("mysql not reachable")
    mcur = mconn.cursor()
    try:
        _make_src(cur, tbl)
        conn.commit()
        mcur.execute(f"DROP TABLE IF EXISTS {dst}")
        mconn.commit()
        recipe = {"steps": [_TEXT_STEPS["upper"]]}
        dst_ep = EndpointConfig(
            kind="database", format="mysql",
            host="127.0.0.1", port=3306, database="dataflow",
            username="root", password="dataflow", table=dst,
        )
        req = TransferRequest(
            source=_ep(parsed, tbl), destination=dst_ep,
            sync_mode="full_refresh_overwrite",
            mappings=[
                {"source": c, "target": c, "confidence": 1.0,
                 "user_override": True, "transform": "none"}
                for c in cols
            ],
            skip_preflight=True, validation_mode="strict",
            shape_recipe=recipe,
            approved_shape_recipe_hash=ShapeRecipe.parse(
                recipe, source_columns=cols
            ).recipe_hash,
        )
        res = _exec(fake, req, prefix="x1m_")
        assert res.success, res.error
        mcur.execute(f"SELECT id, v FROM {dst} ORDER BY id")
        got = {r[0]: r[1] for r in mcur.fetchall()}
        for i, want in _expect("upper").items():
            assert got.get(i) == want, (
                f"mysql row {i}: want {want!r} got {got.get(i)!r}"
            )
    finally:
        try:
            mcur.execute(f"DROP TABLE IF EXISTS {dst}")
            mconn.commit()
            mconn.close()
        except Exception:
            pass
        cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.close()


def test_x1_blank_text_pg_to_sqlite_readback(monkeypatch, tmp_path):
    """PG→SQLite live read-back: '' and '   ' land as stored values."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    cols = ["id", "v"]
    tbl = "x1s_" + uuid.uuid4().hex[:10]
    dst = "x1dst_" + uuid.uuid4().hex[:8]
    dbfile = str(tmp_path / f"{dst}.sqlite")
    try:
        _make_src(cur, tbl)
        conn.commit()
        recipe = {"steps": [_TEXT_STEPS["trim"]]}
        dst_ep = EndpointConfig(
            kind="database", format="sqlite",
            host="", port=0, database=dbfile,
            username="", password="", table=dst,
        )
        req = TransferRequest(
            source=_ep(parsed, tbl), destination=dst_ep,
            sync_mode="full_refresh_overwrite",
            mappings=[
                {"source": c, "target": c, "confidence": 1.0,
                 "user_override": True, "transform": "none"}
                for c in cols
            ],
            skip_preflight=True, validation_mode="strict",
            shape_recipe=recipe,
            approved_shape_recipe_hash=ShapeRecipe.parse(
                recipe, source_columns=cols
            ).recipe_hash,
        )
        res = _exec(fake, req, prefix="x1s_")
        assert res.success, res.error
        sconn = sqlite3.connect(dbfile)
        got = {
            r[0]: r[1]
            for r in sconn.execute(f"SELECT id, v FROM {dst} ORDER BY id")
        }
        sconn.close()
        for i, want in _expect("trim").items():
            assert got.get(i) == want, (
                f"sqlite row {i}: want {want!r} got {got.get(i)!r}"
            )
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.close()


def test_x1_unshaped_blank_text_passthrough_pg(monkeypatch):
    """Unshaped run: '' and '   ' pass through as stored values."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    cols = ["id", "v"]
    tbl = "x1u_" + uuid.uuid4().hex[:10]
    dst = tbl + "_d"
    try:
        _make_src(cur, tbl)
        conn.commit()
        req = TransferRequest(
            source=_ep(parsed, tbl), destination=_ep(parsed, dst),
            sync_mode="full_refresh_overwrite",
            mappings=[
                {"source": c, "target": c, "confidence": 1.0,
                 "user_override": True, "transform": "none"}
                for c in cols
            ],
            skip_preflight=True, validation_mode="strict",
        )
        res = _exec(fake, req, prefix="x1u_")
        assert res.success, res.error
        cur.execute(f"SELECT id, v FROM {dst} ORDER BY id")
        got = {r[0]: r[1] for r in cur.fetchall()}
        for i, want in _SRC_ROWS.items():
            assert got.get(i) == want, (
                f"row {i}: want {want!r} got {got.get(i)!r}"
            )
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {tbl}, {dst}")
        conn.close()
