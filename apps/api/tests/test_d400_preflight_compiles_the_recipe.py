"""D400 / R8: Validate judges the saved library recipe Execute will run.

``POST /preflight/run`` had no ``transform_recipe_id`` field, so the gates
judged the raw rows (long_txt and payload in clear, no derived column) while
Execute compiled the recipe, and a recipe that could not compile was refused
only at Confirm. Validate now compiles the same recipe, or refuses it.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401


def _store(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services import transform_library
    from services.transform_library import FileTransformLibraryStore

    store = FileTransformLibraryStore(tmp_path / "lib.json")
    monkeypatch.setattr(transform_library, "_store", store)
    return store


def _body(**extra):
    from src.routers.preflight_router import PreflightRequest

    return PreflightRequest(
        columns=["id", "name", "long_txt"],
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        **extra,
    )


def test_validate_compiles_the_library_recipe_execute_runs(tmp_path, monkeypatch):
    from services.transform_binding import compile_preflight_recipe, compile_transfer_recipe
    from services.transform_recipe import RecipeStep, TransformRecipe

    store = _store(tmp_path, monkeypatch)
    saved = store.save(TransformRecipe(name="r48", steps=[RecipeStep(column="name", op="trim")]))
    body = _body(transform_recipe_id=saved.id)
    assert not (body.shape_recipe or {}).get("steps")
    compile_preflight_recipe(body)
    assert body.shape_recipe["steps"]

    from types import SimpleNamespace

    execute = SimpleNamespace(
        transform_recipe_id=saved.id, transform_recipe_version=0,
        transform_recipe_version_mode="pin", transform_recipe_params=None,
        mappings=body.mappings, workspace_id="", shape_recipe={}, approved_shape_recipe_hash="",
    )
    compile_transfer_recipe(execute)
    assert execute.shape_recipe == body.shape_recipe


def test_validate_refuses_a_draft_recipe_before_confirm(tmp_path, monkeypatch):
    from services.transform_binding import compile_preflight_recipe
    from services.transform_recipe import RecipeStep, TransformRecipe

    store = _store(tmp_path, monkeypatch)
    draft = store.save(TransformRecipe(name="d", steps=[RecipeStep(column="a", op="bogus")]))
    with pytest.raises(ValueError, match="draft"):
        compile_preflight_recipe(_body(transform_recipe_id=draft.id))


def test_validate_without_a_recipe_id_is_unchanged():
    from services.transform_binding import compile_preflight_recipe

    body = _body(shape_recipe={"steps": [{"column": "name", "op": "trim"}]})
    compile_preflight_recipe(body)
    assert body.shape_recipe == {"steps": [{"column": "name", "op": "trim"}]}


def test_validate_in_another_workspace_gets_the_missing_recipe_answer(tenancy_world):  # noqa: F811
    """M1 / R11 / R8: Validate looked a recipe up with no workspace at all.

    ``PreflightRequest`` carries no workspace, so the library lookup skipped
    its workspace check: workspace B compiled A's recipe (and a draft's 422
    named it) while Execute in B refused it as not found. B now gets exactly
    the answer a recipe id that does not exist gets; A still compiles it.
    """
    from services import transform_library

    world = tenancy_world
    transform_library.reset_transform_library()

    def saved(steps) -> str:
        made = world.a.post("/api/v1/transform-library", json={
            "name": "secret-recipe-of-A", "steps": steps,
        })
        assert made.status_code in {200, 201}, made.text
        return (made.json().get("recipe") or made.json())["id"]

    active = saved([{"column": "name", "op": "trim"}])
    draft = saved([{"column": "name", "op": "bogus"}])

    def validate(client, recipe_id):
        return client.post("/api/v1/preflight/run", json={
            "columns": ["id", "name"],
            "mappings": [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
            "transform_recipe_id": recipe_id,
        })

    missing_id = "tr_" + "0" * 24
    missing = validate(world.b, missing_id)
    assert missing.status_code == 422, missing.text
    for foreign_id in (active, draft):
        foreign = validate(world.b, foreign_id)
        assert foreign.status_code == 422, foreign.text
        assert foreign.json()["detail"].replace(foreign_id, "<id>") == (
            missing.json()["detail"].replace(missing_id, "<id>")
        )
        assert "secret-recipe-of-A" not in foreign.text and "draft" not in foreign.text

    own = validate(world.a, active)
    assert own.status_code == 200, own.text[:400]
    # The shaped image was judged: the plan names the compiled recipe's hash.
    assert re.search(r"Transform recipe [0-9a-f]+ applied before the gates", own.text), own.text[:400]
    own_draft = validate(world.a, draft)
    assert own_draft.status_code == 422 and "draft" in own_draft.json()["detail"], own_draft.text
