"""A2: multi-input join (inner/left) and union (append) on the shape engine."""
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402

from services.connector_store import SavedConnector  # noqa: E402
from services.shape_apply import ShapeRunner  # noqa: E402
from services.shape_models import ShapeError, ShapeRecipe  # noqa: E402


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


_COUNTRIES = [
    {"code": "US", "name": "United States", "risk": "low"},
    {"code": "CN", "name": "China", "risk": "high"},
]


def _join_recipe(**opt):
    options = {
        "type": "inner",
        "on": {"cc": "code"},
        "map": {"name": "country_name", "risk": "risk"},
        "rows": _COUNTRIES,
        **opt,
    }
    return ShapeRecipe.parse(
        {"steps": [{"op": "join", "options": options}]},
        source_columns=["cc", "amt"],
    )


# --- design-time validation -------------------------------------------------


@pytest.mark.parametrize("bad,msg", [
    ({"on": {}}, "non-empty on"),
    ({"on": None, "map": {"a": "b"}}, "needs 'on'"),
    ({"map": {}}, "non-empty map"),
    ({"type": "right"}, "inner or left"),
    ({"fanout": "many"}, "first, refuse or allow"),
    ({"unmatched": "null"}, "drop or divert"),
    ({"rows": [], "reference": None}, "either a reference"),
    ({"rows": [{"code": "X"}], "reference": {"connector_id": "c", "table": "t"}}, "not both"),
    ({"on": {"ghost": "code"}}, "not a source column"),
    ({"on": {"cc": "code"}, "reference": {"connector_id": "c", "table": "t", "columns": ["code", "name"]},
      "map": {"zzz": "out"}, "rows": None}, "not in reference.columns"),
    ({"on": {"cc": "missing_ref"}, "reference": {"connector_id": "c", "table": "t", "columns": ["code", "name"]},
      "map": {"name": "out"}, "rows": None}, "not in reference.columns"),
])
def test_join_option_validation_refuses(bad, msg):
    with pytest.raises(ShapeError, match=msg):
        _join_recipe(**bad)


@pytest.mark.parametrize("bad,msg", [
    ({"rows": [], "reference": None}, "either a reference"),
    ({"rows": [{"cc": "x"}], "reference": {"connector_id": "c", "table": "t"}}, "not both"),
    ({"extra": "skip"}, "refuse or drop"),
    ({"map": "notadict"}, "must be an object"),
])
def test_union_option_validation_refuses(bad, msg):
    options = {"rows": [{"cc": "ZZ"}], **bad}
    with pytest.raises(ShapeError, match=msg):
        ShapeRecipe.parse(
            {"steps": [{"op": "union", "options": options}]},
            source_columns=["cc"],
        )


def test_union_reference_columns_unmapped_extra_refuses_at_parse():
    """R5: reference.columns declaring a column the union cannot place
    (not an output column, not renamed by map) must refuse at design time
    under extra=refuse — not at write time under the capacity wrapper."""
    with pytest.raises(ShapeError, match="reference.columns declares mystery"):
        ShapeRecipe.parse(
            {"steps": [{"op": "union", "options": {
                "reference": {
                    "connector_id": "c", "table": "t",
                    "columns": ["cc", "mystery"],
                },
                "extra": "refuse",
            }}]},
            source_columns=["cc"],
        )
    # Renaming it via map, or dropping it, stays legal.
    ShapeRecipe.parse(
        {"steps": [{"op": "union", "options": {
            "reference": {
                "connector_id": "c", "table": "t",
                "columns": ["cc", "mystery"],
            },
            "map": {"mystery": "cc"}, "extra": "refuse",
        }}]},
        source_columns=["cc"],
    )


# --- join engine semantics --------------------------------------------------


def test_inner_join_enriches_and_drops_nonmatches():
    r = ShapeRunner(_join_recipe())
    headers, rows = r.matrix(["cc", "amt"], [["US", "10"], ["XX", "20"], ["CN", "30"]])
    out = [dict(zip(headers, r)) for r in rows]
    assert out == [
        {"cc": "US", "amt": "10", "country_name": "United States", "risk": "low"},
        {"cc": "CN", "amt": "30", "country_name": "China", "risk": "high"},
    ]
    step = r.effect.steps[0]
    assert step.rows_removed == 1  # XX dropped


def test_inner_join_diverts_with_reason():
    r = ShapeRunner(_join_recipe(unmatched="divert", reason="unknown country"))
    headers, rows = r.matrix(["cc"], [["US"], ["XX"]])
    assert len(rows) == 1
    diverted = r.take_diverted()
    assert len(diverted) == 1 and "unknown country" in diverted[0].reason


def test_left_join_keeps_row_with_nulls():
    r = ShapeRunner(_join_recipe(type="left"))
    headers, rows = r.matrix(["cc"], [["US"], ["XX"]])
    out = [dict(zip(headers, row)) for row in rows]
    assert out[1]["country_name"] is None and out[1]["risk"] is None
    assert r.effect.steps[0].rows_removed == 0


def test_join_fanout_refuse_fails_loud_on_duplicate_key():
    recipe = _join_recipe(
        rows=[{"code": "US", "name": "A"}, {"code": "US", "name": "B"}],
        map={"name": "country_name"},
    )
    r = ShapeRunner(recipe)
    with pytest.raises(Exception, match="fanout=refuse"):
        r.matrix(["cc"], [["US"]])


def test_join_fanout_allow_expands_and_ledgers():
    recipe = _join_recipe(
        rows=[{"code": "US", "name": "A"}, {"code": "US", "name": "B"}],
        map={"name": "country_name"},
        fanout="allow",
    )
    r = ShapeRunner(recipe)
    headers, rows = r.matrix(["cc", "amt"], [["US", "10"]])
    out = [dict(zip(headers, row)) for row in rows]
    assert [o["country_name"] for o in out] == ["A", "B"]
    assert r.effect.rows_expanded == 1


def test_join_fanout_first_keeps_earliest():
    recipe = _join_recipe(
        rows=[{"code": "US", "name": "A"}, {"code": "US", "name": "B"}],
        map={"name": "country_name"},
        fanout="first",
    )
    r = ShapeRunner(recipe)
    headers, rows = r.matrix(["cc", "amt"], [["US", "10"]])
    out = [dict(zip(headers, row)) for row in rows]
    assert [o["country_name"] for o in out] == ["A"]


def test_join_composite_key():
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "join", "options": {
            "type": "inner",
            "on": {"cc": "code", "region": "reg"},
            "map": {"name": "n"},
            "rows": [{"code": "US", "reg": "W", "name": "West"}],
        }}]},
        source_columns=["cc", "region"],
    )
    r = ShapeRunner(recipe)
    headers, rows = r.matrix(["cc", "region"], [["US", "W"], ["US", "E"]])
    assert rows == [["US", "W", "West"]]


def test_join_reference_resolves_saved_connector(tmp_path, monkeypatch):
    db = tmp_path / "ref.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE countries (code TEXT, name TEXT)")
    conn.executemany("INSERT INTO countries VALUES (?, ?)", [("US", "United States")])
    conn.commit(); conn.close()
    saved = SavedConnector(id="jref", name="ref", type="sqlite", role="source", database=str(db))
    import services.connector_store as cs
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: saved if cid == saved.id else None)

    recipe = _join_recipe(
        rows=None,
        reference={"connector_id": "jref", "table": "countries", "columns": ["code", "name"]},
        map={"name": "country_name"},
    )
    r = ShapeRunner(recipe)
    headers, rows = r.matrix(["cc", "amt"], [["US", "10"], ["XX", "20"]])
    out = [dict(zip(headers, row)) for row in rows]
    assert out == [{"cc": "US", "amt": "10", "country_name": "United States"}]


# --- union engine semantics --------------------------------------------------


def test_union_appends_aligned_rows_and_ledgers_balance():
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "union", "options": {"rows": [{"cc": "ZZ"}, {"cc": "QQ"}]}}]},
        source_columns=["cc"],
    )
    r = ShapeRunner(recipe)
    r.records([{"cc": "US"}])
    tail = r.drain()
    assert tail == [{"cc": "ZZ"}, {"cc": "QQ"}]
    e = r.effect
    # in + expanded = out + held + removed + diverted
    assert e.rows_in + e.rows_expanded == e.rows_out + e.rows_buffered + e.rows_shaped_out + e.rows_diverted


def test_union_reorders_and_maps_columns_by_name():
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "union", "options": {
            "rows": [{"extra": "x", "cc": "ZZ"}],
            "map": {"extra": "cc2"},
        }}]},
        source_columns=["cc"],
    )
    r = ShapeRunner(recipe)
    r.records([{"cc": "US", "cc2": "orig"}])
    assert r.drain() == [{"cc": "ZZ", "cc2": "x"}]


def test_union_extra_column_refuses_fail_closed():
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "union", "options": {"rows": [{"cc": "ZZ", "mystery": "x"}]}}]},
        source_columns=["cc"],
    )
    r = ShapeRunner(recipe)
    r.records([{"cc": "US"}])
    with pytest.raises(ValueError, match="not a stream column"):
        r.drain()


def test_union_extra_column_drops_explicitly():
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "union", "options": {"rows": [{"cc": "ZZ", "mystery": "x"}], "extra": "drop"}}]},
        source_columns=["cc"],
    )
    r = ShapeRunner(recipe)
    r.records([{"cc": "US"}])
    assert r.drain() == [{"cc": "ZZ"}]


def test_union_rows_flow_through_tail_steps():
    recipe = ShapeRecipe.parse(
        {"steps": [
            {"op": "union", "options": {"rows": [{"cc": "zz"}]}},
            {"op": "case", "column": "cc", "options": {"mode": "upper"}},
        ]},
        source_columns=["cc"],
    )
    r = ShapeRunner(recipe)
    r.records([{"cc": "us"}])
    assert r.drain() == [{"cc": "ZZ"}]


# --- end-to-end through the transfer engine ---------------------------------


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            if rows is None:
                conn.execute(sql)
            else:
                conn.executemany(sql, rows)
    finally:
        conn.close()


def test_join_union_end_to_end_destination_readback(tmp_path, isolated):
    """SQLite->SQLite run: inner join enriches, union appends; destination
    read-back proves both and the ledger balances in = out + diverted."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (cc TEXT, amt TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?)", [("US", "10"), ("XX", "20"), ("CN", "30")])

    payload = {"steps": [
        {"op": "join", "options": {
            "type": "inner", "on": {"cc": "code"},
            "map": {"name": "country_name"},
            "rows": _COUNTRIES,
            "unmatched": "divert", "reason": "unknown country",
        }},
        {"op": "union", "options": {
            "rows": [{"cc": "ZZ", "country_name": "Appended"}],
        }},
    ]}
    recipe = ShapeRecipe.parse(payload)
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        shape_recipe=payload,
        approved_shape_recipe_hash=recipe.recipe_hash,
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "joinunion" + uuid.uuid4().hex[:10]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error

    conn = sqlite3.connect(str(dst))
    try:
        got = sorted(conn.execute("SELECT cc, amt, country_name FROM t").fetchall())
    finally:
        conn.close()
    # joined rows enriched; XX diverted; ZZ appended by the union
    assert ("CN", "30", "China") in got
    assert ("US", "10", "United States") in got
    assert ("ZZ", None, "Appended") in got
    assert not any(r[0] == "XX" for r in got)

    # R2: the diverted row is quarantine, not a silent removal — the ledger
    # must charge it to rejected_rows and say diverted, never rows_shaped_out.
    summary = result.destination_summary or {}
    assert int(summary.get("rows_diverted") or 0) == 1, summary
    assert int(summary.get("rejected_rows") or 0) == 1, summary
    assert not summary.get("rows_shaped_out"), (
        "a diverted row sits in the DLQ — removal terms may not swallow it"
    )

    import services.quarantine_dlq as dlq
    quarantined = dlq.canonical_replay_details({"id": job_id})
    assert len(quarantined) == 1 and "unknown country" in quarantined[0]["reason"]
