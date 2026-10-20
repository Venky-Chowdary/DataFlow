"""X-111/X-112/X-113/X-114 — absent-object flatten keys + expanding keys.

X-111: flatten_json refused the wire NULL sentinel on a PG jsonb column —
the reader's ``__DF_SQL_NULL__`` slipped past ``raw is None`` and hit
"not a JSON object". One null predicate (`_dedupe_is_null`) now guards it.
X-112: a null/absent object still produces its declared keys as NULL, the
same way a missing key inside '{}' does — the output schema declared them,
so nest_json downstream finds them.
X-113: declaring a produced key member (unnest index_to, surrogate_key,
hash_identity) is valid on every route — the transform guard only refuses
a rewritten/dropped source column, and key columns are only guarded on
routes that resolve rows by them.
X-114: when the recipe expands rows (unnest_json, join fanout, union) the
source PK repeats per emitted row and must not be carried into create-new
DDL; a declared produced key replaces it. Same rule on every engine.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from services.shape_engine import ShapeEngine, _SQL_NULL_SENTINEL
from services.shape_models import ShapeRecipe


def _engine(steps, rows, source_types=None):
    recipe = ShapeRecipe.parse(
        {"steps": steps},
        source_columns=list(rows[0].keys()),
        source_types=source_types,
    )
    engine = ShapeEngine(recipe)
    emitted = engine.apply_batch(rows)
    emitted.extend(engine.drain())
    return emitted


def test_x111_x112_absent_object_produces_declared_null_keys():
    rows = [
        {"id": 1, "doc": _SQL_NULL_SENTINEL},   # reader's SQL NULL wire text (X-111)
        {"id": 2, "doc": "null"},                # JSON null literal
        {"id": 3, "doc": "{}"},                  # present, keys absent
        {"id": 4, "doc": '{"city":"sf","zip":"94110"}'},
    ]
    eng = _engine(
        [
            {
                "op": "flatten_json",
                "column": "doc",
                "options": {"keys": ["city", "zip"]},
            },
            {
                "op": "nest_json",
                "options": {
                    "to": "addr",
                    "fields": [{"from": "city", "to": "c"}, {"from": "zip", "to": "z"}],
                },
            },
        ],
        rows,
    )
    out = {r["id"]: r for r in eng}
    # Absent objects produce the declared keys as NULL — nest_json consumes them.
    for rid in (1, 2, 3):
        assert out[rid]["city"] is None and out[rid]["zip"] is None, out[rid]
        assert out[rid]["addr"] == '{"c":null,"z":null}', out[rid]["addr"]
    assert out[4]["city"] == "sf" and out[4]["zip"] == "94110"


def test_x113_produced_key_member_not_refused_on_overwrite():
    from services.shape_apply import build_shape_runner
    from src.transfer.engine_shape import _shape_stream_refusal

    runner = build_shape_runner(
        {
            "steps": [
                {
                    "op": "unnest_json",
                    "column": "tags",
                    "options": {"to": "tag", "index_to": "pos", "keep_parent": False},
                },
                {"op": "keep_columns", "options": {"columns": ["id", "pos", "tag"]}},
            ]
        },
        source_columns=["id", "tags"],
    )
    # Declared key "id,pos" — pos is produced, not rewritten: valid on a
    # whole-table overwrite route (keys do not resolve rows there).
    refusal = _shape_stream_refusal(
        runner,
        effective_sync="full_refresh_overwrite",
        multi_stream=False,
        cursor_field="",
        key_columns=["id", "pos"],
    )
    assert refusal == "", refusal
    # And on an upsert route a produced key member is valid too.
    refusal = _shape_stream_refusal(
        runner,
        effective_sync="upsert",
        multi_stream=False,
        cursor_field="",
        key_columns=["id", "pos"],
    )
    assert refusal == "", refusal


def test_x113_rewritten_source_key_still_refused():
    from services.shape_apply import build_shape_runner
    from src.transfer.engine_shape import _shape_stream_refusal

    runner = build_shape_runner(
        {"steps": [{"op": "set_value", "column": "id", "options": {"expression": "id + 100"}}]},
        source_columns=["id", "v"],
    )
    refusal = _shape_stream_refusal(
        runner,
        effective_sync="upsert",
        multi_stream=False,
        cursor_field="",
        key_columns=["id"],
    )
    assert "refuses" in refusal


def _mk_src(tmp: Path) -> Path:
    src = tmp / "src.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, tags TEXT, note TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?)",
        [
            (1, '["a","b"]', "n1"),
            (2, '["c"]', "n2"),
        ],
    )
    conn.commit()
    conn.close()
    return src


_UNNEST = {
    "steps": [
        {
            "op": "unnest_json",
            "column": "tags",
            "options": {"to": "tag", "index_to": "pos", "keep_parent": False},
        },
        {
            "op": "keep_columns",
            "options": {"columns": ["id", "pos", "tag", "note"]},
        },
    ]
}


def _req(src: Path, dst: Path, primary_key: str = ""):
    from src.transfer.models import EndpointConfig, TransferRequest

    contract = {
        "name": "t",
        "selected": True,
        "sync_mode": "full_refresh_overwrite",
    }
    if primary_key:
        contract["primary_key"] = primary_key
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        stream_contracts=[contract],
        shape_recipe=_UNNEST,
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "pos", "target": "pos", "target_type": "INTEGER"},
            {"source": "tag", "target": "tag", "target_type": "TEXT"},
            {"source": "note", "target": "note", "target_type": "TEXT"},
        ],
    )


def _run(src: Path, dst: Path, primary_key: str = ""):
    from src.transfer.engine import UniversalTransferEngine

    return UniversalTransferEngine().execute_tracked(
        _req(src, dst, primary_key), uuid.uuid4().hex[:24]
    )


def _dest_ddl(dst: Path, table: str = "out") -> str:
    conn = sqlite3.connect(dst)
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return (row or ("",))[0]
    finally:
        conn.close()


def test_x114_unnest_expansion_carries_no_source_pk(tmp_path, monkeypatch):
    """unnest_json repeats id per element — PRIMARY KEY(id) would reject
    every second row of a parent. No declared key → destination unkeyed."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    result = _run(src, dst)
    assert result.success, getattr(result, "error", None)
    ddl = _dest_ddl(dst).upper()
    assert "PRIMARY KEY" not in ddl, ddl
    conn = sqlite3.connect(dst)
    rows = conn.execute("SELECT id, pos, tag FROM out ORDER BY id, pos").fetchall()
    conn.close()
    assert rows == [(1, 0, "a"), (1, 1, "b"), (2, 0, "c")], rows


def test_x114_declared_produced_key_is_carried(tmp_path, monkeypatch):
    """Declared key (id,pos) — pos is the unnest index — IS the dest key."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    result = _run(src, dst, primary_key="id,pos")
    assert result.success, getattr(result, "error", None)
    ddl = _dest_ddl(dst).upper()
    assert "PRIMARY KEY" in ddl and "POS" in ddl, ddl


@pytest.mark.skipif(
    not __import__("os").environ.get("DATAFLOW_X114_LIVE"),
    reason="live engines — user-run (live_616d_rjson B, 5 dests)",
)
def test_x114_live_rjson_b_all_engines():
    pytest.fail("harness: unnest recipe B with key id,pos on all 5 engines")
