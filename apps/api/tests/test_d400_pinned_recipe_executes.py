"""DW-D400: a pinned library recipe that preview compiled must execute.

Map sees the recipe output (note renamed to memo). Execute used to compile
the saved recipe against those mapping names and refuse with
"column 'note' is not available", after preview and Validate had already
passed. The pinned version is the program preview ran. A recipe that names
a column the source does not have is still refused, at Validate, with that
column named.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

_SOURCE = ["id", "name", "note", "qty", "updated_at"]


def _library(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services import transform_library
    from services.transform_library import FileTransformLibraryStore

    store = FileTransformLibraryStore(tmp_path / "lib.json")
    monkeypatch.setattr(transform_library, "_store", store)
    return store


def _save(store, steps):
    from services.transform_recipe import TransformRecipe

    return store.save(TransformRecipe(name="mahi_qa_20261121_d400_recipe", steps=steps))


def _steps():
    from services.transform_recipe import RecipeStep

    return [
        RecipeStep(column="name", op="trim"),
        RecipeStep(column="note", op="rename_column", output_column="memo"),
        RecipeStep(column="", op="filter_rows", options={"condition": "[id] <> 10", "keep": True}),
    ]


def _shaped_mappings() -> list[dict]:
    """What Map stores after the recipe: memo, not note."""
    return [
        {"source": name, "target": name, "user_override": True, "confidence": 1}
        for name in ("id", "name", "memo", "qty", "updated_at")
    ]


def test_pinned_recipe_compiles_against_the_source_not_the_mapped_image(tmp_path, monkeypatch):
    from services.transform_binding import compile_preflight_recipe, compile_transfer_recipe
    from services.transform_recipe import RecipeStep, compile_recipe
    from src.routers.preflight_router import PreflightRequest
    from types import SimpleNamespace

    store = _library(tmp_path, monkeypatch)
    saved = _save(store, _steps())
    payload, shape = compile_recipe(saved, source_columns=_SOURCE)
    request = SimpleNamespace(
        transform_recipe_id=saved.id,
        transform_recipe_version=saved.version,
        transform_recipe_version_mode="pin",
        transform_recipe_params=None,
        mappings=_shaped_mappings(),
        workspace_id="",
        shape_recipe=payload,
        approved_shape_recipe_hash=shape.recipe_hash,
    )
    assert compile_transfer_recipe(request) is False
    assert request.shape_recipe["steps"][1]["column"] == "note"
    assert request.approved_shape_recipe_hash == shape.recipe_hash

    body = PreflightRequest(
        columns=_SOURCE,
        mappings=_shaped_mappings(),
        shape_recipe=payload,
        transform_recipe_id=saved.id,
        transform_recipe_version=saved.version,
        transform_recipe_version_mode="pin",
    )
    compile_preflight_recipe(body)
    assert body.shape_recipe["steps"][0]["op"] == "trim"

    missing = _save(store, [RecipeStep(column="b", op="trim")])
    refused = PreflightRequest(
        columns=_SOURCE,
        mappings=_shaped_mappings(),
        transform_recipe_id=missing.id,
        transform_recipe_version=missing.version,
        transform_recipe_version_mode="pin",
    )
    with pytest.raises(ValueError, match="column 'b' is not available"):
        compile_preflight_recipe(refused)


def test_pinned_recipe_executes_on_the_engine(tmp_path, monkeypatch):
    """Save, pin, and run through UniversalTransferEngine.execute_tracked."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from services.transform_recipe import compile_recipe
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    store = _library(tmp_path, monkeypatch)
    saved = _save(store, _steps())
    payload, shape = compile_recipe(saved, source_columns=_SOURCE)

    src = tmp_path / "src.sqlite"
    dst = tmp_path / "dst.sqlite"
    conn = sqlite3.connect(src)
    conn.execute(
        "CREATE TABLE mahi_qa_20261121_late ("
        "id INTEGER PRIMARY KEY, name TEXT, note TEXT, qty INTEGER, updated_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO mahi_qa_20261121_late VALUES (?, ?, ?, ?, ?)",
        [(i, f"  n{i}  ", f"note-{i}", i, "2026-11-21") for i in range(1, 11)],
    )
    conn.commit()
    conn.close()
    sqlite3.connect(dst).close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="mahi_qa_20261121_late",
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="mahi_qa_20261121_recipe2_pp",
        ),
        mappings=_shaped_mappings(),
        shape_recipe=payload,
        approved_shape_recipe_hash=shape.recipe_hash,
        transform_recipe_id=saved.id,
        transform_recipe_version=saved.version,
        transform_recipe_version_mode="pin",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="off",
        stream_contracts=[{
            "name": "mahi_qa_20261121_late",
            "primary_key": ["id"],
            "selected": True,
        }],
    )
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    job_id = "d400" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    assert request.transform_recipe_version == saved.version

    with sqlite3.connect(dst) as out:
        cols = [row[1] for row in out.execute("PRAGMA table_info(mahi_qa_20261121_recipe2_pp)")]
        rows = out.execute(
            "SELECT id, name, memo FROM mahi_qa_20261121_recipe2_pp ORDER BY id"
        ).fetchall()
    assert "note" not in cols
    assert "memo" in cols
    assert len(rows) == 9
    assert rows[0] == (1, "n1", "note-1")
    assert all(row[0] != 10 for row in rows)
