"""Transform Library: recipe validation, versioning, apply, preview."""

from __future__ import annotations

import pytest

from services.transform_library import FileTransformLibraryStore
from services.transform_recipe import (
    RecipeStep,
    TransformRecipe,
    preview_recipe,
)


def _recipe(**kw) -> TransformRecipe:
    base = dict(
        name="clean customers",
        steps=[
            RecipeStep(column="email", op="trim"),
            RecipeStep(column="email", op="lower"),
            RecipeStep(column="amount", op="decimal"),
        ],
    )
    base.update(kw)
    return TransformRecipe(**base)


def test_recipe_requires_name_and_steps():
    with pytest.raises(ValueError, match="needs a name"):
        TransformRecipe(name="", steps=[RecipeStep(column="a", op="trim")]).validate()
    with pytest.raises(ValueError, match="at least one step"):
        TransformRecipe(name="x", steps=[]).validate()


def test_recipe_rejects_unknown_op_and_bad_on_error():
    with pytest.raises(ValueError, match="unknown transform"):
        _recipe(steps=[RecipeStep(column="a", op="not_a_real_op")]).validate()
    with pytest.raises(ValueError, match="on_error"):
        _recipe(steps=[RecipeStep(column="a", op="trim", on_error="vanish")]).validate()


def test_alias_ops_validate():
    r = _recipe(steps=[RecipeStep(column="a", op="to_decimal")])
    r.validate()


def test_recipe_runs_its_steps_in_order_through_the_shape_engine():
    """One composition model: the preview is the engine the transfer runs."""
    row = {"email": "  Ann@EXAMPLE.COM ", "amount": "12.5", "name": "Ann"}
    result = preview_recipe(_recipe(), [row])
    assert result["compiled"] is True
    out = result["results"][0]
    assert out["errors"] == []
    assert out["after"]["email"] == "ann@example.com"
    assert str(out["after"]["amount"]) == "12.5"
    assert out["after"]["name"] == "Ann"


def test_recipe_fail_step_refuses_the_row():
    recipe = TransformRecipe(
        name="r", steps=[RecipeStep(column="amount", op="decimal")]
    )
    result = preview_recipe(recipe, [{"amount": "NOT_A_NUMBER"}])
    assert result["rows_with_errors"] == 1
    assert "amount" in result["results"][0]["errors"][0]["error"]


def test_keep_original_recipe_cannot_run_so_it_previews_nothing():
    """keep_original never compiles, so no transfer can bind it — the preview
    used to show its output anyway through a second, per-cell engine."""
    recipe = TransformRecipe(
        name="r",
        steps=[RecipeStep(column="amount", op="decimal", on_error="keep_original")],
    )
    result = preview_recipe(recipe, [{"amount": "junk"}])
    assert result["compiled"] is False
    assert result["results"] == []
    assert "keep_original" in result["error"]


def test_preview_counts_error_rows():
    recipe = TransformRecipe(
        name="r", steps=[RecipeStep(column="amount", op="decimal")]
    )
    result = preview_recipe(
        recipe, [{"amount": "1.0"}, {"amount": "bad"}, {"amount": "2"}]
    )
    assert result["rows"] == 3
    assert result["rows_with_errors"] == 1
    assert result["results"][1]["errors"]


def test_file_store_roundtrip_and_versioning(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    store = FileTransformLibraryStore()
    saved = store.save(_recipe())
    assert saved.version == 1
    again = store.get(saved.id)
    assert again is not None and again.name == "clean customers"

    again.name = "clean customers v2"
    saved2 = store.save(again)
    # Metadata-only edits keep the runnable version (a pinned binding is not
    # invalidated by a rename); a steps change is what bumps it.
    assert saved2.version == 1
    saved2.steps = [saved2.steps[0], *saved2.steps[1:]]
    saved2.steps[0].options = {"x": 1}
    saved3 = store.save(saved2)
    assert saved3.version == 2

    listed = store.list()
    assert [r.id for r in listed] == [saved.id]
    assert store.delete(saved.id) is True
    assert store.get(saved.id) is None


def test_file_store_workspace_scoping(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    store = FileTransformLibraryStore()
    a = store.save(_recipe(workspace_id="ws-a"))
    store.save(_recipe(workspace_id="ws-b", name="b"))
    assert [r.name for r in store.list("ws-a")] == ["clean customers"]
    assert store.get(a.id, workspace_id="ws-b") is None
    assert store.get(a.id, workspace_id="ws-a") is not None


def test_save_marks_active_vs_draft(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    store = FileTransformLibraryStore()
    good = store.save(_recipe())
    assert good.status == "active" and good.issues == []
    bad = store.save(
        _recipe(steps=[RecipeStep(column="a", op="not_a_real_op")])
    )
    assert bad.status == "draft" and bad.issues


def test_version_pin_resolves_prior_definition(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    store = FileTransformLibraryStore()
    saved = store.save(_recipe())
    saved.steps = [RecipeStep(column="email", op="upper")]
    store.save(saved)
    assert store.get(saved.id).version == 2
    pinned = store.get(saved.id, version=1)
    assert pinned is not None and pinned.version == 1
    assert [s.op for s in pinned.steps] == ["trim", "lower", "decimal"]
    assert store.get(saved.id, version=99) is None


def test_bind_parameters_defaults_overrides_and_refusals():
    recipe = _recipe(
        steps=[
            RecipeStep(
                column="amount",
                op="default_if_null",
                options={"value": ":default_amount"},
            ),
            RecipeStep(column="amount", op="decimal"),
        ],
        parameters={"default_amount": {"default": "0"}},
    )
    bound = recipe.bind_parameters({"default_amount": "9.99"})
    assert bound.steps[0].options["value"] == "9.99"
    bound2 = recipe.bind_parameters()
    assert bound2.steps[0].options["value"] == "0"
    with pytest.raises(ValueError, match="undeclared parameter"):
        recipe.bind_parameters({"nope": "1"})


def test_bind_parameters_missing_required_is_named():
    recipe = _recipe(
        steps=[RecipeStep(column="a", op="trim", options={"x": ":p"})],
        parameters={"p": {}},
    )
    with pytest.raises(ValueError, match="missing value.*p"):
        recipe.bind_parameters()


def test_compile_recipe_to_shape_payload():
    from services.transform_recipe import compile_recipe

    recipe = _recipe()
    payload, shape = compile_recipe(recipe)
    ops = [s["op"] for s in payload["steps"]]
    assert ops == ["trim", "case", "cast_column"]
    assert payload["steps"][1]["options"]["mode"] == "lower"
    assert payload["steps"][2]["options"]["to_type"] == "decimal"
    assert shape.recipe_hash


def test_compile_refuses_draft_and_uncompilable_op():
    from services.transform_recipe import compile_recipe

    with pytest.raises(ValueError, match="not runnable"):
        compile_recipe(_recipe(steps=[RecipeStep(column="a", op="bogus")]))
    keep = _recipe(
        steps=[RecipeStep(column="a", op="trim", on_error="keep_original")]
    )
    with pytest.raises(ValueError, match="keep_original"):
        compile_recipe(keep)


def test_compile_input_contract_drift_named():
    from services.transform_recipe import compile_recipe

    recipe = _recipe(input_columns=("email", "amount"))
    with pytest.raises(ValueError, match="expects input column"):
        compile_recipe(recipe, source_columns=["email"])
    compile_recipe(recipe, source_columns=["email", "amount", "extra"])


def test_per_step_preview_through_engine():
    recipe = _recipe()
    result = preview_recipe(
        recipe,
        [{"email": "  A@B.COM ", "amount": "5"}],
        per_step=True,
    )
    assert result["compiled"] is True
    steps = result["results"][0]["steps"]
    assert steps[0]["after"]["email"] == "A@B.COM"
    assert steps[1]["after"]["email"] == "a@b.com"


def test_templates_clone_valid_recipe(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services.transform_recipe_templates import clone_template, list_templates

    keys = [t["key"] for t in list_templates()]
    assert "standardise-email-hash-pii" in keys
    store = FileTransformLibraryStore()
    for key in keys:
        recipe = clone_template(key)
        assert recipe is not None
        saved = store.save(recipe)
        assert saved.status == "active", (key, saved.issues)
    assert clone_template("does-not-exist") is None


def test_binding_resolution_pin_follow_and_draft(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services.transform_binding import (
        apply_schedule_binding,
        resolve_recipe_binding,
    )

    store = FileTransformLibraryStore()
    monkeypatch.setattr(
        "services.transform_library._store", None, raising=False
    )
    from services import transform_library

    monkeypatch.setattr(transform_library, "_store", store)

    saved = store.save(_recipe())
    bound = resolve_recipe_binding(saved.id)
    assert bound.bound_version == 1 and bound.payload["steps"]
    draft = store.save(
        _recipe(name="d", steps=[RecipeStep(column="a", op="bogus")])
    )
    with pytest.raises(ValueError, match="draft"):
        resolve_recipe_binding(draft.id)
    payload = apply_schedule_binding(
        {
            "transform_recipe_id": saved.id,
            "transform_recipe_version_mode": "pin",
            "transform_recipe_version": 0,
        }
    )
    assert payload["transform_recipe_version"] == 1
    assert payload["shape_recipe"]["steps"]
    tb_bound = payload
    assert tb_bound["transform_recipe_version_mode"] == "pin"
