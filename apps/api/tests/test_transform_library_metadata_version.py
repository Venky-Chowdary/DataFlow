"""Audit P2-1: metadata-only saves do not bump the runnable version."""
from services.transform_recipe import TransformRecipe, RecipeStep
from services.transform_library import FileTransformLibraryStore


def _recipe(**kw):
    return TransformRecipe(
        name=kw.pop("name", "r"),
        steps=[RecipeStep(column="a", op="trim")],
        **kw,
    )


def test_first_save_is_version_1(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    store = FileTransformLibraryStore()
    saved = store.save(_recipe())
    assert saved.version == 1


def test_metadata_only_save_keeps_version(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    store = FileTransformLibraryStore()
    saved = store.save(_recipe())
    saved.description = "renamed only"
    again = store.save(saved)
    assert again.version == 1
    # and no history entry was created for a non-runnable edit
    entry = store._entry(saved.id)
    assert len(entry.get("_history") or []) == 0


def test_step_change_bumps_version(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    store = FileTransformLibraryStore()
    saved = store.save(_recipe())
    saved.steps = [RecipeStep(column="a", op="upper")]
    bumped = store.save(saved)
    assert bumped.version == 2


def test_dedupe_asc_tie_keeps_first_seen():
    """Audit P2-2: equal order values keep the first-seen winner."""
    from services.shape_apply import ShapeRunner
    from services.shape_models import ShapeRecipe

    recipe = ShapeRecipe.parse(
        {
            "steps": [
                {
                    "op": "dedupe_rows",
                    "options": {
                        "keys": ["k"], "keep": "last",
                        "order_by": "v", "order_dir": "asc",
                    },
                }
            ]
        },
        source_columns=["k", "v", "tag"],
    )
    runner = ShapeRunner(recipe)
    headers, rows = runner.matrix(
        ["k", "v", "tag"],
        [
            ["a", 1, "first"],
            ["a", 1, "second"],
        ],
    )
    tail_headers, tail_rows = runner.drain_matrix()
    winners = tail_rows or rows
    hdrs = tail_headers if tail_rows else headers
    assert len(winners) == 1
    assert dict(zip(hdrs, winners[0]))["tag"] == "first"
