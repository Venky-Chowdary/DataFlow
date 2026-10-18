"""B1 SCD2 Type-2 PG-live regressions.

Gated on DATAFLOW_PG_TEST_DSN — SQLite cannot prove the history contract
against a real destination (type fidelity of valid_from/valid_to and the
per-table write transactions differ).
"""
import os
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")
DSN = os.environ.get("DATAFLOW_PG_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="DATAFLOW_PG_TEST_DSN unset")

from services.scd2_engine import (  # noqa: E402
    apply_scd2,
    preview_scd2_merge,
    verify_scd2_history,
)
from src.transfer.models import EndpointConfig  # noqa: E402


def _parse_dsn():
    from urllib.parse import urlparse

    u = urlparse(DSN)
    return {
        "host": u.hostname,
        "port": u.port or 5432,
        "database": (u.path or "/postgres").lstrip("/"),
        "username": u.username or "postgres",
        "password": u.password or "",
    }


def _cfg(endpoint):
    from src.transfer.adapters import resolve_connector_config

    return resolve_connector_config(endpoint)


def _pg(schema="public"):
    parsed = _parse_dsn()
    conn = psycopg2.connect(
        host=parsed["host"], port=parsed["port"], dbname=parsed["database"],
        user=parsed["username"], password=parsed["password"],
    )
    conn.autocommit = True
    return conn, parsed


def _endpoint(table):
    p = _parse_dsn()
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=p["host"],
        port=p["port"],
        database=p["database"],
        username=p["username"],
        password=p["password"],
        table=table,
    )


ROWS = [
    {"id": "1", "name": "A", "price": "10.00", "note": "n1"},
    {"id": "2", "name": "B", "price": "20.00", "note": "n2"},
    {"id": "3", "name": "C", "price": "30.00", "note": "n3"},
]
COLS = ["id", "name", "price", "note"]
SCHEMA = {"id": "string", "name": "string", "price": "decimal", "note": "string"}


def _fetch(cur, table):
    cur.execute(
        f"SELECT id, name, price, note, version, valid_from, valid_to, "
        f"is_current FROM {table} ORDER BY id, version"
    )
    return cur.fetchall()


def test_scd2_full_lifecycle_pg(tmp_path):
    table = f"scd2_t_{uuid.uuid4().hex[:8]}"
    conn, parsed = _pg()
    cur = conn.cursor()
    opts = {
        "version": "version",
        "tracked": ["name", "price"],  # note is untracked
        "type1": True,
        "close_missing": True,
    }
    ep = _endpoint(table)

    # 1. First load — one current version per key, version 1.
    s1 = apply_scd2(
        ep, [dict(r) for r in ROWS], COLS, SCHEMA, None, ["id"],
        scd2_opts=opts,
    )
    assert s1["ok"], s1
    assert s1["rows_written"] == 3 and s1["updated_rows"] == 0
    rows = _fetch(cur, table)
    assert len(rows) == 3
    assert all(r[4] == 1 and r[7] is True and r[6] is None for r in rows)

    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], opts
    )
    assert hist["ok"], hist["violations"]
    assert hist["current_rows"] == 3 and hist["keys"] == 3

    # 2. Unchanged rerun — a complete no-op.
    s2 = apply_scd2(
        ep, [dict(r) for r in ROWS], COLS, SCHEMA, None, ["id"],
        scd2_opts=opts,
    )
    assert s2["ok"] and s2["rows_written"] == 0 and s2["updated_rows"] == 0
    assert len(_fetch(cur, table)) == 3

    # 3. Tracked change (price 10.00 -> 15.00 on id=1): close v1, open v2
    #    in the same transaction — one is_current per key still.
    changed = [dict(r) for r in ROWS]
    changed[0]["price"] = "15.00"
    s3 = apply_scd2(ep, changed, COLS, SCHEMA, None, ["id"], scd2_opts=opts)
    assert s3["ok"] and s3["rows_written"] == 1 and s3["updated_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 4
    v1 = [r for r in rows if r[0] == "1" and r[4] == 1][0]
    v2 = [r for r in rows if r[0] == "1" and r[4] == 2][0]
    assert v1[7] is False and v1[6] is not None
    assert v2[7] is True and v2[6] is None
    assert str(v2[5]) == str(v1[6])  # contiguous, no gap/overlap
    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], opts
    )
    assert hist["ok"], hist["violations"]
    assert hist["current_rows"] == 3 and hist["history_rows"] == 1

    # 4. Untracked change (note on id=2): Type-1 overwrite in place —
    #    no new version, no expire.
    untracked = [dict(r) for r in changed]
    untracked[1]["note"] = "n2-updated"
    s4 = apply_scd2(ep, untracked, COLS, SCHEMA, None, ["id"], scd2_opts=opts)
    assert s4["ok"] and s4["rows_written"] == 0 and s4["updated_rows"] == 0
    assert s4["type1_updated_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 4
    row2 = [r for r in rows if r[0] == "2"][0]
    assert row2[3] == "n2-updated" and row2[4] == 1 and row2[7] is True

    # 5. Key removal (id=3 absent from a full snapshot): soft-close —
    #    the version stays in history, stops being current.
    reduced = [r for r in untracked if r["id"] != "3"]
    s5 = apply_scd2(
        ep, reduced, COLS, SCHEMA, None, ["id"],
        complete_snapshot=True, scd2_opts=opts,
    )
    assert s5["ok"] and s5["closed_missing_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 4
    row3 = [r for r in rows if r[0] == "3"][0]
    assert row3[7] is False and row3[6] is not None

    # 6. Same key changes twice across runs: id=1 price again -> 18.00.
    #    v2 closes, v3 opens; v1 keeps its original close.
    twice = [dict(r) for r in reduced]
    twice[0]["price"] = "18.00"
    s6 = apply_scd2(ep, twice, COLS, SCHEMA, None, ["id"], scd2_opts=opts)
    assert s6["ok"] and s6["rows_written"] == 1 and s6["updated_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 5
    versions_1 = sorted(r[4] for r in rows if r[0] == "1")
    assert versions_1 == [1, 2, 3]
    currents_1 = [r for r in rows if r[0] == "1" and r[7] is True]
    assert len(currents_1) == 1 and currents_1[0][4] == 3

    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], opts
    )
    assert hist["ok"], hist["violations"]
    conn.close()


def test_scd2_custom_stamp_names_pg(tmp_path):
    """Recipe-declared stamp column names are written to the destination."""
    table = f"scd2_c_{uuid.uuid4().hex[:8]}"
    conn, parsed = _pg()
    cur = conn.cursor()
    resolved = {
        "version": "ver",
        "valid_from": "eff_from",
        "valid_to": "eff_to",
        "is_current": "current_flag",
        "row_hash": "attr_hash",
    }
    ep = _endpoint(table)
    s1 = apply_scd2(
        ep, [dict(r) for r in ROWS[:2]], COLS, SCHEMA, None, ["id"],
        scd2_opts=resolved,
    )
    assert s1["ok"], s1
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s",
        (table,),
    )
    cols = {r[0] for r in cur.fetchall()}
    assert {"eff_from", "eff_to", "current_flag", "attr_hash", "ver"} <= cols
    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], resolved
    )
    assert hist["ok"], hist["violations"]
    conn.close()


def test_scd2_per_step_preview_pg(tmp_path):
    """The per-step preview names which rows would open/close — before any write."""
    table = f"scd2_p_{uuid.uuid4().hex[:8]}"
    conn, parsed = _pg()
    ep = _endpoint(table)
    opts = {"version": "version", "tracked": ["name", "price"], "type1": True}
    apply_scd2(
        ep, [dict(r) for r in ROWS], COLS, SCHEMA, None, ["id"],
        scd2_opts=opts,
    )
    # Incoming: id=1 tracked change, id=2 untracked change, id=3 gone, id=4 new.
    incoming = [
        {"id": "1", "name": "A", "price": "99.00", "note": "n1"},
        {"id": "2", "name": "B", "price": "20.00", "note": "n2-new"},
        {"id": "4", "name": "D", "price": "40.00", "note": "n4"},
    ]
    p = preview_scd2_merge(
        ep, incoming, COLS, SCHEMA, None, ["id"],
        complete_snapshot=True, scd2_opts=opts,
    )
    assert p["ok"], p
    assert p["counts"]["open"] == 2      # id=1 new version + id=4 first load
    assert p["counts"]["close"] == 2     # id=1 close + id=3 missing-close
    assert p["counts"]["type1"] == 1     # id=2 untracked Type-1
    assert set(p["opens"]) == {"1", "4"}
    assert set(p["closes"]) == {"1", "3"}
    # Preview wrote nothing — history unchanged.
    hist = verify_scd2_history(_cfg(ep), "public", table, ["id"], opts)
    assert hist["ok"] and hist["history_rows"] == 0
    conn.close()


def test_scd2_recipe_step_parses_contract():
    """The recipe ``scd2`` step declares the contract the merge honors."""
    from services.shape_models import ShapeRecipe

    recipe = ShapeRecipe.parse(
        {
            "steps": [
                {
                    "op": "scd2",
                    "enabled": True,
                    "options": {
                        "keys": ["id"],
                        "tracked": ["name", "price"],
                        "type1": True,
                        "version": "version",
                    },
                }
            ]
        },
        source_columns=COLS,
    )
    names = recipe.scd2_column_names()
    assert names["keys"] == ["id"]
    assert names["tracked"] == ["name", "price"]
    assert names["type1"] is True
    assert names["close_missing"] is True
    assert names["version"] == "version"
