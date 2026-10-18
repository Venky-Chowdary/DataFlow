"""A3: router — one input to named outputs by condition, per-route tables."""
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402

from services.shape_apply import ShapeRunner  # noqa: E402
from services.shape_models import ROUTE_COLUMN, ShapeError, ShapeRecipe  # noqa: E402


@pytest.fixture()
def isolated(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _recipe(**opt):
    options = {
        "routes": [
            {"name": "high", "condition": "[amt] >= 100"},
            {"name": "mid", "condition": "[amt] >= 50"},
            {"name": "other", "default": True},
        ],
        **opt,
    }
    return ShapeRecipe.parse(
        {"steps": [{"op": "router", "options": options}]},
        source_columns=["id", "amt"],
    )


# --- design-time validation -------------------------------------------------


@pytest.mark.parametrize("bad,msg", [
    ({"routes": []}, "non-empty routes list"),
    ({"routes": "x"}, "non-empty routes list"),
    ({"routes": [{"name": "a", "condition": "[amt] >= 1"}, {"name": "a", "condition": "[amt] >= 2"}]},
     "declared twice"),
    ({"routes": [{"name": "a", "default": True}, {"name": "b", "default": True}]},
     "only one route may be default"),
    ({"routes": [{"name": "a", "default": True, "condition": "[amt] >= 1"}]},
     "takes no condition"),
    ({"routes": [{"name": "a", "condition": ""}]}, "needs a condition or default"),
    ({"routes": [{"name": "a", "condition": "[ghost] >= 1"}]}, "ghost"),
    ({"routes": [{"name": "a", "condition": "[amt] >= 1"}], "unmatched": "drop"},
     "divert or fail"),
    ({"routes": [{"name": "a", "condition": "[amt] >= 1"}]},
     "declare a default route or set unmatched"),
    ({"routes": [{"name": "a", "condition": "[amt] >= 1"}, {"name": "d", "default": True}],
      "unmatched": "divert"}, "no effect when a default route exists"),
    ({"routes": [{"name": "a", "condition": "[amt] >= 1"}, {"name": "d", "default": True}],
      "targets": {"ghost": "t"}}, "unknown route"),
    ({"routes": [{"name": "a", "condition": "[amt] >= 1"}, {"name": "d", "default": True}],
      "targets": "t"}, "must be an object"),
    ({"routes": [{"name": "a", "condition": "[amt] >= 1", "map": {"ghost": "val"}},
       {"name": "d", "default": True}]}, "ghost"),
])
def test_router_option_validation_refuses(bad, msg):
    with pytest.raises(ShapeError, match=msg):
        _recipe(**bad)


def test_two_router_steps_refuse():
    with pytest.raises(ShapeError, match="one router step"):
        ShapeRecipe.parse(
            {"steps": [
                {"op": "router", "options": {"routes": [
                    {"name": "a", "default": True}]}},
                {"op": "router", "options": {"routes": [
                    {"name": "b", "default": True}]}},
            ]},
            source_columns=["amt"],
        )


# --- engine semantics -------------------------------------------------------


def test_router_three_conditions_plus_default_conserves_every_row():
    r = ShapeRunner(_recipe())
    headers, rows = r.matrix(
        ["id", "amt"],
        [["1", "150"], ["2", "75"], ["3", "10"], ["4", "300"], ["5", "60"], ["6", "1"]],
    )
    tag_ix = headers.index(ROUTE_COLUMN)
    tags = [row[tag_ix] for row in rows]
    # every input row lands in exactly one output (conservation)
    assert len(rows) == 6
    assert sorted(tags) == ["high", "high", "mid", "mid", "other", "other"]
    assert sum(r.effect.routes.values()) == 6
    assert r.effect.routes == {"high": 2, "mid": 2, "other": 2}
    # first match wins: amt 150 matches high even though mid also matches
    assert rows[0][tag_ix] == "high" and rows[3][tag_ix] == "high"
    e = r.effect
    assert e.rows_in == e.rows_out + e.rows_diverted


def test_router_unmatched_divert_quarantines_with_reason():
    recipe = _recipe(
        routes=[{"name": "high", "condition": "[amt] >= 100"}],
        unmatched="divert",
    )
    r = ShapeRunner(recipe)
    headers, rows = r.matrix(["id", "amt"], [["1", "150"], ["2", "10"]])
    assert len(rows) == 1
    diverted = r.take_diverted()
    assert len(diverted) == 1 and "no route matched" in diverted[0].reason
    assert diverted[0].op == "router"
    e = r.effect
    assert e.rows_in == e.rows_out + e.rows_diverted


def test_router_unmatched_fail_refuses_loud():
    recipe = _recipe(
        routes=[{"name": "high", "condition": "[amt] >= 100"}],
        unmatched="fail",
    )
    r = ShapeRunner(recipe)
    with pytest.raises(Exception, match="no route matched"):
        r.matrix(["id", "amt"], [["2", "10"]])


def test_router_per_route_map_and_output_columns():
    recipe = _recipe(
        routes=[
            {"name": "high", "condition": "[amt] >= 100", "map": {"amt": "big_amt"}},
            {"name": "other", "default": True},
        ],
    )
    assert recipe.route_output_columns("high") == ["big_amt"]
    assert recipe.route_output_columns("other") == ["id", "amt"]
    r = ShapeRunner(recipe)
    headers, rows = r.matrix(["id", "amt"], [["1", "150"], ["2", "10"]])
    tag_ix = headers.index(ROUTE_COLUMN)
    # The map applies at WRITE time, not in the shared batch — rewriting the
    # row here would mix row shapes and read the mapped population as NULLs
    # in the data-quality audit (RT-3).
    # The output schema may still declare the map target column, but the
    # rows keep the source values — the write emits the rename.
    big_ix = headers.index("big_amt")
    assert rows[0][tag_ix] == "high"
    amt_ix = headers.index("amt")
    assert rows[0][amt_ix] == "150"
    assert rows[0][big_ix] is None
    assert rows[1][tag_ix] == "other" and rows[1][amt_ix] == "10"


def test_router_preview_matrix_shows_tag_column():
    """Per-output preview: the matrix output carries the route tag so a preview
    can group rows by output name."""
    r = ShapeRunner(_recipe())
    headers, rows = r.matrix(["id", "amt"], [["1", "150"], ["2", "10"]])
    assert ROUTE_COLUMN in headers
    by_route = {}
    for row in rows:
        by_route.setdefault(row[headers.index(ROUTE_COLUMN)], []).append(row)
    assert sorted(by_route) == ["high", "other"]


def test_split_routed_batch_groups_and_names_tables():
    from src.transfer.stream import _split_routed_batch

    recipe = _recipe(targets={"high": "customers_vip"})
    r = ShapeRunner(recipe)
    headers, rows = r.matrix(["id", "amt"], [["1", "150"], ["2", "10"], ["3", "200"]])

    class _B:
        def __init__(self, h, rr):
            self.headers = h
            self.rows = rr

    splits = _split_routed_batch(_B(headers, rows), recipe, "dst")
    by_route = {name: (table, hdrs, rr) for name, table, hdrs, rr in splits}
    assert by_route["high"][0] == "customers_vip"
    assert by_route["other"][0] == "dst__other"
    assert len(by_route["high"][2]) == 2 and len(by_route["other"][2]) == 1
    assert ROUTE_COLUMN not in by_route["high"][1]
    assert sum(len(s[3]) for s in splits) == 3  # conservation across splits


def test_materialized_path_refuses_router():
    from src.transfer.engine_shape import _shape_materialized_read

    r = ShapeRunner(_recipe())
    with pytest.raises(ValueError, match="only supported on streaming database sources"):
        _shape_materialized_read(r, [{"id": "1", "amt": "150"}], ["id", "amt"], {})


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


def test_router_end_to_end_separate_destination_tables(tmp_path, isolated):
    """SQLite->SQLite run: 3 conditions + default; every row lands in exactly
    one route table, verified by reading each destination table separately."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, amt TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?)", [
        ("1", "150"), ("2", "75"), ("3", "10"), ("4", "300"), ("5", "60"), ("6", "1"),
    ])

    payload = {"steps": [{"op": "router", "options": {
        "routes": [
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        "targets": {"high": "cust_high", "mid": "cust_mid", "other": "cust_other"},
    }}]}
    recipe = ShapeRecipe.parse(payload)
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="cust"),
        sync_mode="full_refresh_overwrite",
        shape_recipe=payload,
        approved_shape_recipe_hash=recipe.recipe_hash,
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "router" + uuid.uuid4().hex[:10]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    isolated.update_job_fields(job_id, {"id": job_id, "job_id": job_id})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error

    conn = sqlite3.connect(str(dst))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"cust_high", "cust_mid", "cust_other"} <= tables
        high = sorted(conn.execute("SELECT id, amt FROM cust_high").fetchall())
        mid = sorted(conn.execute("SELECT id, amt FROM cust_mid").fetchall())
        other = sorted(conn.execute("SELECT id, amt FROM cust_other").fetchall())
    finally:
        conn.close()
    # destination tables verified separately — conservation: 6 in = 6 landed
    assert high == [("1", "150"), ("4", "300")]
    assert mid == [("2", "75"), ("5", "60")]
    assert other == [("3", "10"), ("6", "1")]
    assert len(high) + len(mid) + len(other) == 6
    # every input row lands in exactly one output (no overlaps between tables)
    ids = [r[0] for r in high + mid + other]
    assert sorted(ids) == ["1", "2", "3", "4", "5", "6"]


def test_router_end_to_end_unmatched_diverts_to_dlq(tmp_path, isolated):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, amt TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?)", [("1", "150"), ("2", "10")])

    payload = {"steps": [{"op": "router", "options": {
        "routes": [{"name": "high", "condition": "to_number([amt]) >= 100"}],
        "unmatched": "divert",
        "targets": {"high": "cust_high"},
    }}]}
    recipe = ShapeRecipe.parse(payload)
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="cust"),
        sync_mode="full_refresh_overwrite",
        shape_recipe=payload,
        approved_shape_recipe_hash=recipe.recipe_hash,
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "routediv" + uuid.uuid4().hex[:10]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    isolated.update_job_fields(job_id, {"id": job_id, "job_id": job_id})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error

    conn = sqlite3.connect(str(dst))
    try:
        high = conn.execute("SELECT id, amt FROM cust_high").fetchall()
    finally:
        conn.close()
    assert high == [("1", "150")]

    import services.quarantine_dlq as dlq
    quarantined = dlq.canonical_replay_details({"id": job_id})
    assert len(quarantined) == 1 and "no route matched" in quarantined[0]["reason"]
