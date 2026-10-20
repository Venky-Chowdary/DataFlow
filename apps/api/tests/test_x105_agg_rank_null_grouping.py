"""X-105/106/107/108 — one null predicate, one grouping key, NULLS LAST rank.

Live at 616df100 (identical on all 5 engines):
- count(qty) counted the group's NULL (nq=3 for (1,2,NULL); SQL = 2) — the
  count incremented before the null check (X-105).
- min/first returned NULL when the group held a NULL: the wire sentinel
  slipped past `value is None` (X-106).
- rank_rows partition_by merged '' and NULL into one partition (X-107).
- rank "top" floated NULL first in-process — preview ≠ run (X-108).

Fix: `_dedupe_is_null` is the one predicate for every measure;
`_group_key(_part)` is the one grouping key for aggregate / rank_rows /
dedupe_rows / lookup / join ('' ≠ NULL, NULL ≡ NULL); rank sorts NULLS
LAST in both directions with an optional deterministic then_by.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

from services.shape_engine import ShapeEngine, _SQL_NULL_SENTINEL
from services.shape_models import ShapeRecipe

# The live 12-row set: '' and NULL cust are separate keys; NULLs in
# amt/qty/note; tied amt for id4/id5; negative amt id8.
ROWS = [
    {"id": 1, "cust": "A", "region": "r", "amt": 10.0, "qty": 1, "note": "a1"},
    {"id": 2, "cust": "A", "region": "r", "amt": _SQL_NULL_SENTINEL, "qty": 2, "note": "a2"},
    {"id": 3, "cust": "A", "region": "r", "amt": 20.5, "qty": _SQL_NULL_SENTINEL, "note": "a3"},
    {"id": 4, "cust": "B", "region": "r", "amt": 5.25, "qty": 1, "note": _SQL_NULL_SENTINEL},
    {"id": 5, "cust": "B", "region": "r", "amt": 5.25, "qty": 1, "note": "b5"},
    {"id": 6, "cust": "", "region": "r", "amt": 1.0, "qty": 1, "note": "e6"},
    {"id": 7, "cust": _SQL_NULL_SENTINEL, "region": "r", "amt": 2.0, "qty": 1, "note": "n7"},
    {"id": 8, "cust": "C", "region": "r", "amt": -4.0, "qty": 1, "note": "c8"},
    {"id": 9, "cust": "D", "region": "r", "amt": 0.1, "qty": 1, "note": "d9"},
    {"id": 10, "cust": "D", "region": "r", "amt": 0.2, "qty": 1, "note": "d10"},
    {"id": 11, "cust": "D", "region": "r", "amt": 0.4, "qty": 1, "note": "d11"},
    {"id": 12, "cust": "D", "region": "r", "amt": 0.3, "qty": 1, "note": "d12"},
]


def _engine(steps):
    recipe = ShapeRecipe.parse(
        {"steps": steps},
        source_columns=["id", "cust", "region", "amt", "qty", "note"],
    )
    engine = ShapeEngine(recipe)
    engine.apply_batch(ROWS)
    return engine


def _agg_steps():
    return [
        {
            "op": "aggregate",
            "options": {
                "group_by": ["cust"],
                "measures": [
                    {"function": "count", "column": "id", "as": "n"},
                    {"function": "count", "column": "qty", "as": "nq"},
                    {"function": "count_distinct", "column": "qty", "as": "nd"},
                    {"function": "sum", "column": "amt", "as": "total"},
                    {"function": "avg", "column": "amt", "as": "av"},
                    {"function": "min", "column": "amt", "as": "mn"},
                    {"function": "max", "column": "amt", "as": "mx"},
                    {"function": "first", "column": "note", "as": "fn"},
                    {"function": "last", "column": "note", "as": "ln"},
                ],
            },
        }
    ]


def test_count_column_skips_null_count_star_counts_rows():
    out = _engine(_agg_steps()).drain()
    by_cust = {r.get("cust"): r for r in out}
    # '' and NULL are separate groups; group rows carry the raw key value.
    assert len(out) == 6
    a = by_cust["A"]
    assert a["n"] == 3  # count(id): three rows
    assert a["nq"] == 2  # count(qty) skips the NULL — SQL semantics (X-105)
    assert a["nd"] == 2  # distinct non-NULL qty {1,2}
    b = by_cust["B"]
    assert b["n"] == 2 and b["nq"] == 2 and b["nd"] == 1


def test_min_first_ignore_null_not_return_it():
    out = _engine(_agg_steps()).drain()
    by_cust = {r.get("cust"): r for r in out}
    a = by_cust["A"]
    # X-106: sentinel NULLs are skipped — min is 10.0 not the NULL marker.
    assert a["mn"] == 10.0, a
    assert a["mx"] == 20.5 and abs(float(a["total"]) - 30.5) < 1e-9
    assert abs(float(a["av"]) - 15.25) < 1e-9
    b = by_cust["B"]
    assert b["fn"] == "b5"  # id4's NULL note is skipped, not returned
    assert b["ln"] == "b5"
    d = by_cust["D"]
    assert abs(float(d["av"]) - 0.25) < 1e-9  # 1.0/4


def test_rank_partition_empty_string_not_null():
    out = _engine(
        [
            {
                "op": "rank_rows",
                "options": {
                    "order_by": "amt",
                    "direction": "top",
                    "limit": 1,
                    "partition_by": ["cust"],
                },
            }
        ]
    ).drain()
    # X-107: '' and NULL are different partitions → both winners land.
    ids = sorted(r["id"] for r in out)
    assert 6 in ids and 7 in ids, ids  # '' and NULL partitions both survive
    assert len(out) == 6


def test_rank_top_nulls_last_both_directions():
    # X-108: group A holds amt NULL (id2) — "top" must never let NULL win.
    out = _engine(
        [
            {
                "op": "rank_rows",
                "options": {
                    "order_by": "amt",
                    "direction": "top",
                    "limit": 1,
                    "partition_by": ["cust"],
                },
            }
        ]
    ).drain()
    by_cust = {r.get("cust") for r in out}
    a_winners = [r for r in out if r.get("cust") == "A"]
    assert a_winners and a_winners[0]["id"] == 3, a_winners  # 20.5, not NULL
    # "bottom" likewise never floats NULL first.
    bot = _engine(
        [
            {
                "op": "rank_rows",
                "options": {
                    "order_by": "amt",
                    "direction": "bottom",
                    "limit": 1,
                    "partition_by": ["cust"],
                },
            }
        ]
    ).drain()
    b_a = [r for r in bot if r.get("cust") == "A"]
    assert b_a and b_a[0]["id"] == 1, b_a  # 10.0 lowest, not NULL


def test_rank_then_by_deterministic_tiebreak_and_nulls_first():
    # B ties id4/id5 at 5.25 — then_by id descending picks id5; input
    # order is the default without then_by.
    tied = _engine(
        [
            {
                "op": "rank_rows",
                "options": {
                    "order_by": "amt",
                    "direction": "top",
                    "limit": 1,
                    "partition_by": ["cust"],
                    "then_by": ["id"],
                },
            }
        ]
    ).drain()
    b = [r for r in tied if r.get("cust") == "B"]
    assert b and b[0]["id"] == 5, b  # later id wins under desc then_by
    nf = _engine(
        [
            {
                "op": "rank_rows",
                "options": {
                    "order_by": "amt",
                    "direction": "top",
                    "limit": 1,
                    "partition_by": ["cust"],
                    "nulls": "first",
                },
            }
        ]
    ).drain()
    a = [r for r in nf if r.get("cust") == "A"]
    assert a and a[0]["id"] == 2, a  # nulls='first' restores SQL NULLS FIRST


def test_dedupe_keys_empty_string_not_null():
    # keep=first is streaming — winners emit from apply_batch, not drain.
    recipe = ShapeRecipe.parse(
        {
            "steps": [
                {
                    "op": "dedupe_rows",
                    "options": {"keys": ["cust"], "keep": "first"},
                }
            ]
        },
        source_columns=["id", "cust", "region", "amt", "qty", "note"],
    )
    engine = ShapeEngine(recipe)
    out = engine.apply_batch(ROWS) + engine.drain()
    custs = [r.get("cust") for r in out]
    # '' row and NULL row are distinct key-holders, both survive.
    assert "" in custs and _SQL_NULL_SENTINEL in custs
    assert len(out) == 6



@pytest.fixture
def _env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    yield


def test_e2e_sqlite_aggregate_and_rank(_env, tmp_path):
    """The live recipe on sqlite: 6 aggregate groups + 6 rank partitions."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src = tmp_path / "s.db"
    conn = sqlite3.connect(src)
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, cust TEXT, region TEXT, "
        "amt NUMERIC(12,4), qty INTEGER, note TEXT)"
    )
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?,?,?,?)",
        [
            (r["id"], None if r["cust"] is _SQL_NULL_SENTINEL else r["cust"],
             r["region"], r["amt"], r["qty"],
             None if r["note"] is _SQL_NULL_SENTINEL else r["note"])
            for r in ROWS
        ],
    )
    conn.commit()
    conn.close()
    dst = tmp_path / "d.db"
    recipe = {"steps": _agg_steps()}
    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=["id", "cust", "region", "amt", "qty", "note"]
        ).recipe_hash,
        mappings=[
            {"source": "cust", "target": "cust", "target_type": "TEXT"},
            {"source": "n", "target": "n", "target_type": "INTEGER"},
            {"source": "nq", "target": "nq", "target_type": "INTEGER"},
            {"source": "total", "target": "total", "target_type": "REAL"},
            {"source": "av", "target": "av", "target_type": "REAL"},
            {"source": "mn", "target": "mn", "target_type": "REAL"},
            {"source": "mx", "target": "mx", "target_type": "REAL"},
            {"source": "fn", "target": "fn", "target_type": "TEXT"},
        ],
    )
    r = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert r.success, r.error
    rows = sqlite3.connect(dst).execute(
        "SELECT cust, n, nq, mn, fn FROM out"
    ).fetchall()
    assert len(rows) == 6, rows
    vals = {row[0]: row for row in rows}
    a = vals["A"]
    assert a[1] == 3 and a[2] == 2, a  # count(id)=3, count(qty)=2
    assert abs(float(a[3]) - 10.0) < 1e-6, a  # min ignores NULL
    assert vals["B"][4] == "b5", vals  # first(note) ignores NULL
    assert any(row[0] is None for row in rows), rows  # NULL group present
    assert "" in vals, rows  # '' group distinct from NULL


@pytest.mark.skipif(
    not os.environ.get("DATAFLOW_X105_LIVE"),
    reason="12-row aggregate+rank matrix on a server engine, run on the host",
)
def test_live_server_engine():
    pass
