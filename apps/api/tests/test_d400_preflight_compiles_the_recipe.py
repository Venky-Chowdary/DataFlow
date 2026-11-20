"""D400 / R8: Validate judges the saved library recipe Execute will run.

``POST /preflight/run`` had no ``transform_recipe_id`` field, so the gates
judged the raw rows (long_txt and payload in clear, no derived column) while
Execute compiled the recipe, and a recipe that could not compile was refused
only at Confirm. Validate now compiles the same recipe, or refuses it.
"""

from __future__ import annotations

import pytest


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
