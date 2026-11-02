"""Phase A set-graph contract: preserve, hash, and refuse. Nothing runs.

A join inside ``steps`` is still a hard error. A join inside ``set_graph`` is
kept, excluded from ``recipe_hash``, and refused by validate, preview, and
execute before any row step is applied.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.set_graph import (  # noqa: E402
    POST_LOAD_ONLY_OPERATIONS,
    executable_set_ops,
)
from services.shape_apply import build_shape_runner  # noqa: E402
from services.shape_models import ShapeError, ShapeRecipe  # noqa: E402
from services.shape_preflight import ShapePreflightRefused, shaped_preflight_image  # noqa: E402
from src.routers.shape_router import router  # noqa: E402
from src.routers.transfer_router import _validated_shape_recipe  # noqa: E402

JOIN = {
    "nodes": [
        {
            "id": "join_orders",
            "op": "join",
            "inputs": {"left": "orders", "right": "customers"},
            "outputs": {"main": "main"},
            "join_type": "left",
            "keys": [{"left": "customer_id", "right": "id"}],
            "nulls_equal": False,
            "on_error": "refuse",
        }
    ],
    "main_output": "main",
}

TRIM = {"op": "trim", "column": "name"}


@pytest.fixture(scope="module")
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def test_catalog_publishes_an_empty_set_engine_and_runs_join_in_flight(client):
    body = client.get("/api/v1/shape/catalog").json()
    ops = {op["op"] for op in body["operations"]}
    assert body["set_operations"] == []
    assert executable_set_ops() == frozenset()
    assert body["set_engine"] == {
        "available": False,
        "reference": "duckdb",
        "pushdown_dialects": [],
        "set_hash": "",
    }
    # join graduated from post_load_only to a real in-flight canvas op
    # (shape_join_union e2e executes it); only truly whole-population ops
    # stay in the post-load list.
    assert any(node["op"] == "join" for node in body["canvas_nodes"])
    assert body["post_load_only"]["operations"] == list(POST_LOAD_ONLY_OPERATIONS)
    assert "join" not in body["post_load_only"]["operations"]
    assert "join" in ops
    assert "join" not in body["set_operations"]


def test_recipe_hash_ignores_the_graph_and_an_empty_graph_has_no_set_hash():
    plain = ShapeRecipe.parse({"steps": [TRIM]}, source_columns=["name"])
    with_graph = ShapeRecipe.parse(
        {"steps": [TRIM], "set_graph": JOIN},
        source_columns=["name"],
    )
    empty = ShapeRecipe.parse(
        {"steps": [TRIM], "set_graph": {"nodes": []}},
        source_columns=["name"],
    )
    assert plain.recipe_hash == with_graph.recipe_hash
    assert plain.recipe_hash == empty.recipe_hash
    assert plain.set_hash == ""
    assert empty.set_hash == ""
    assert with_graph.set_hash
    assert with_graph.set_hash != plain.recipe_hash


def test_changing_nulls_equal_changes_set_hash_and_key_order_does_not():
    left = ShapeRecipe.parse({"steps": [], "set_graph": JOIN})
    flipped = {
        "main_output": "main",
        "nodes": [{**JOIN["nodes"][0], "nulls_equal": True}],
    }
    right = ShapeRecipe.parse({"steps": [], "set_graph": flipped})
    reordered = {
        "nodes": [
            {
                "nulls_equal": False,
                "on_error": "refuse",
                "keys": [{"right": "id", "left": "customer_id"}],
                "outputs": {"main": "main"},
                "inputs": {"right": "customers", "left": "orders"},
                "join_type": "left",
                "op": "Join",
                "id": "join_orders",
            }
        ],
        "main_output": "main",
    }
    same = ShapeRecipe.parse({"steps": [], "set_graph": reordered})
    assert left.set_hash != right.set_hash
    assert left.set_hash == same.set_hash
    assert left.recipe_hash == ""
    assert right.recipe_hash == ""


def test_join_inside_steps_is_still_a_shape_error():
    with pytest.raises(ShapeError, match="join"):
        ShapeRecipe.parse(
            {"steps": [{"op": "join", "column": "id"}], "set_graph": JOIN},
            source_columns=["id"],
        )


def test_a_malformed_graph_is_refused_instead_of_dropped():
    with pytest.raises(ShapeError, match="set_graph must be an object"):
        ShapeRecipe.parse({"steps": [TRIM], "set_graph": "join the customers"})
    with pytest.raises(ShapeError, match="nodes must be a list"):
        ShapeRecipe.parse({"steps": [], "set_graph": {"nodes": {"op": "join"}}})


def test_to_wire_preserves_the_graph_and_omits_an_empty_one():
    recipe = ShapeRecipe.parse({"steps": [TRIM], "set_graph": JOIN}, source_columns=["name"])
    wire = recipe.to_wire()
    assert wire["set_graph"]["nodes"][0]["id"] == "join_orders"
    assert wire["set_graph"]["nodes"][0]["nulls_equal"] is False
    again = ShapeRecipe.parse(wire, source_columns=["name"])
    assert again.set_hash == recipe.set_hash
    assert again.recipe_hash == recipe.recipe_hash
    assert "set_graph" not in ShapeRecipe.parse({"steps": [TRIM]}, source_columns=["name"]).to_wire()


def test_validate_returns_set_hash_and_errors_without_dropping_the_document(client):
    body = client.post(
        "/api/v1/shape/validate",
        json={
            "source_columns": ["name"],
            "recipe": {"steps": [TRIM], "set_graph": JOIN},
        },
    ).json()
    assert body["valid"] is False
    assert body["recipe_hash"]
    assert body["set_hash"]
    assert body["set_errors"]
    assert body["set_errors"][0]["node"] == "join_orders"
    assert body["set_errors"][0]["op"] == "join"
    assert "not an executable set operation" in body["set_errors"][0]["message"]
    assert body["set_graph"]["nodes"][0]["op"] == "join"
    plain = client.post(
        "/api/v1/shape/validate",
        json={"source_columns": ["name"], "recipe": {"steps": [TRIM]}},
    ).json()
    assert plain["valid"] is True
    assert plain["set_hash"] == ""
    assert plain["set_errors"] == []
    assert plain["recipe_hash"] == body["recipe_hash"]


def test_preview_does_not_show_trimmed_rows_as_a_successful_join(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"name": " Ada "}],
            "source_columns": ["name"],
            "recipe": {"steps": [TRIM], "set_graph": JOIN},
        },
    ).json()
    assert body["after"] == []
    assert body["before"] == [{"name": " Ada "}]
    assert body["refusal"]["op"] == "join"
    assert "was not applied" in body["refusal"]["message"]
    assert "not shaped" in body["preview_note"]
    assert body["recipe"]["valid"] is False
    assert body["effect"]["cells_changed"] == 0


def test_execute_refuses_a_graph_with_no_row_steps_and_a_graph_beside_trim():
    with pytest.raises(ShapeError, match="was not applied"):
        build_shape_runner({"steps": [], "set_graph": JOIN}, source_columns=["name"])
    with pytest.raises(ShapeError, match="was not applied"):
        build_shape_runner(
            {"steps": [TRIM], "set_graph": JOIN},
            source_columns=["name"],
            approved_set_hash="f" * 16,
        )
    assert build_shape_runner({"steps": []}, source_columns=["name"]) is None
    assert build_shape_runner(
        {"steps": [TRIM], "set_graph": {"nodes": []}},
        source_columns=["name"],
    ) is not None


def test_a_stale_set_approval_refuses_a_recipe_with_no_graph():
    with pytest.raises(ShapeError, match="set graph"):
        build_shape_runner(
            {"steps": []},
            source_columns=["name"],
            approved_set_hash="abc123",
        )


def test_the_execute_door_refuses_a_graph_the_engine_cannot_run():
    with pytest.raises(HTTPException) as caught:
        _validated_shape_recipe({"steps": [TRIM], "set_graph": JOIN})
    assert caught.value.status_code == 400
    assert "was not applied" in caught.value.detail
    wire = _validated_shape_recipe({"steps": [TRIM]})
    assert wire["steps"][0]["op"] == "trim"
    assert wire["steps"][0]["column"] == "name"
    assert "set_graph" not in wire


def test_preflight_does_not_pass_a_graph_through_as_todays_rows():
    with pytest.raises(ShapePreflightRefused, match="was not applied"):
        shaped_preflight_image(
            {"steps": [], "set_graph": JOIN},
            columns=["name"],
            column_types={"name": "text"},
            sample_rows=[{"name": " Ada "}],
        )
    with pytest.raises(ShapePreflightRefused, match="was not applied"):
        shaped_preflight_image(
            {"steps": [TRIM], "set_graph": JOIN},
            columns=["name"],
            column_types={"name": "text"},
            sample_rows=[{"name": " Ada "}],
        )


def test_schedule_round_trip_keeps_the_graph_on_the_recipe():
    from services.schedule_store import PipelineSchedule

    recipe = {"steps": [TRIM], "set_graph": JOIN}
    sched = PipelineSchedule.from_dict({
        "id": "sched-set-1",
        "name": "orders",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders",
        "shape_recipe": recipe,
        "approved_set_hash": "abc123",
    })
    restored = PipelineSchedule.from_dict(sched.to_dict())
    assert restored.shape_recipe["set_graph"]["nodes"][0]["id"] == "join_orders"
    assert restored.approved_set_hash == "abc123"
    assert restored.shape_recipe["steps"][0]["op"] == "trim"


def test_a_legacy_steps_only_recipe_and_schedule_still_run():
    """Documents saved before set_graph have no key and no approved_set_hash.

    Loading one, shaping it, and previewing it must do what the row engine
    did: trim the sample, keep recipe_hash, and leave the schedule stamp alone.
    """
    from services.schedule_store import PipelineSchedule, drop_blank_validate_identity

    recipe = {"steps": [TRIM]}
    parsed = ShapeRecipe.parse(recipe, source_columns=["name"])
    assert parsed.set_graph is None
    assert parsed.set_hash == ""
    assert parsed.set_errors == ()
    assert "set_graph" not in parsed.to_wire()
    runner = build_shape_runner(
        recipe,
        source_columns=["name"],
        approved_hash=parsed.recipe_hash,
    )
    assert runner is not None
    assert runner.records([{"name": " Ada "}]) == [{"name": "Ada"}]
    assert runner.report()["recipe_hash"] == parsed.recipe_hash
    assert runner.report()["set_hash"] == ""

    legacy = {
        "id": "sched-legacy",
        "name": "legacy",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders",
        "shape_recipe": dict(recipe),
        "approved_shape_recipe_hash": parsed.recipe_hash,
    }
    assert "approved_set_hash" not in legacy
    sched = PipelineSchedule.from_dict(legacy)
    assert sched.approved_set_hash == ""
    assert sched.approved_shape_recipe_hash == parsed.recipe_hash
    assert sched.shape_recipe == recipe
    restored = PipelineSchedule.from_dict(sched.to_dict())
    assert restored.approved_set_hash == ""
    assert restored.shape_recipe == recipe
    assert "set_graph" not in restored.shape_recipe

    echoed = drop_blank_validate_identity({
        "name": "legacy",
        "approved_set_hash": "",
        "approved_shape_recipe_hash": "",
    })
    assert "approved_set_hash" not in echoed
    assert "approved_shape_recipe_hash" not in echoed


def test_preview_of_a_steps_only_recipe_still_returns_the_shaped_rows(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"name": " Ada "}],
            "source_columns": ["name"],
            "recipe": {"steps": [TRIM]},
        },
    ).json()
    assert body["after"] == [{"name": "Ada"}]
    assert body["refusal"] is None
    assert body["recipe"]["valid"] is True
    assert body["recipe"]["set_hash"] == ""
    assert body["recipe"]["set_errors"] == []
    assert "set_graph" not in body["recipe"]


def test_a_steps_only_execute_still_opens_the_row_runner():
    from services.execute_shape_route import open_execute_shape

    request = SimpleNamespace(
        shape_recipe={"steps": [TRIM]},
        stream_contracts=[],
        sync_mode="full_refresh_append",
        approved_shape_recipe_hash="",
        operation="transfer",
    )
    plan = open_execute_shape(request, ["name"], job_id="job-legacy")
    assert plan.failure is None
    assert plan.shape_runner is not None
    assert plan.shape_runner.records([{"name": " Ada "}]) == [{"name": "Ada"}]


def test_an_existing_recipe_hash_mismatch_still_raises():
    """Phase A must not turn a row-recipe approval miss into a sync-mode failure."""
    from services.execute_shape_route import open_execute_shape

    request = SimpleNamespace(
        shape_recipe={"steps": [TRIM]},
        stream_contracts=[],
        sync_mode="full_refresh_append",
        approved_shape_recipe_hash="deadbeefdeadbeef",
        operation="transfer",
    )
    with pytest.raises(ShapeError, match="not the one approved"):
        open_execute_shape(request, ["name"], job_id="job-mismatch")


def test_multi_stream_execute_refuses_before_a_steps_only_runner(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise AssertionError("row runner built while the set graph was still on the recipe")

    monkeypatch.setattr("services.shape_apply.build_shape_runner", _boom)

    class _Mongo:
        def update_job_status(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr("services.execute_shape_route._mongo", lambda: _Mongo())
    from services.execute_shape_route import open_execute_shape

    request = SimpleNamespace(
        shape_recipe={
            "steps": [{**TRIM, "source_table": "orders"}],
            "set_graph": JOIN,
        },
        stream_contracts=[
            {"name": "orders", "selected": True},
            {"name": "customers", "selected": True},
        ],
        sync_mode="full_refresh_append",
        approved_shape_recipe_hash="",
        approved_set_hash="",
        operation="transfer",
    )
    plan = open_execute_shape(request, ["name"], job_id="job-set-1")
    assert plan.failure is not None
    assert plan.failure.success is False
    assert "was not applied" in (plan.failure.error or "")
    assert plan.shape_runner is None
    assert plan.design_runner is None
