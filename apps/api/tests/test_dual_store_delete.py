"""Ghost-delete regression: a delete that removes only the file mirror must
not leave the Mongo copy resurfacing in list() — the "deleted item still
shows" bug in the Library / Transform project stores.

Both stores are file-primary-write + Mongo-mirror merges.  The durable copy
must be removed too, or the next list() merges it straight back.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.transform_library import (
    FileTransformLibraryStore,
    MongoTransformLibraryStore,
)
from services.transform_recipe import RecipeStep, TransformRecipe
from services.transform_store import MongoTransformProjectStore


class _FakeCollection:
    def __init__(self, *, fail_delete: bool = False) -> None:
        self.docs: dict[str, dict] = {}
        self.fail_delete = fail_delete

    def delete_one(self, query):
        if self.fail_delete:
            raise RuntimeError("mongo unreachable")
        removed = self.docs.pop(query["id"], None)
        return SimpleNamespace(deleted_count=int(removed is not None))

    def find(self, query=None):
        docs = [dict(d) for d in self.docs.values()]
        if query and "workspace_id" in query:
            docs = [d for d in docs if d.get("workspace_id") == query["workspace_id"]]
        return docs

    def find_one(self, query):
        doc = self.docs.get(query["id"])
        return dict(doc) if doc else None

    def replace_one(self, query, doc, upsert=False):
        self.docs[query["id"]] = dict(doc)


class _FakeDb:
    def __init__(self, coll: _FakeCollection) -> None:
        self._coll = coll

    def __getitem__(self, _name):
        return self._coll


class _FakeService:
    def __init__(self, coll: _FakeCollection) -> None:
        self._db = _FakeDb(coll)

    def get_database(self):
        return self._db


def _library_store(tmp_path, monkeypatch, *, fail_delete=False):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    coll = _FakeCollection(fail_delete=fail_delete)
    return MongoTransformLibraryStore(_FakeService(coll)), coll


def _project_store(tmp_path, monkeypatch, *, fail_delete=False):
    monkeypatch.setenv("DATAFLOW_TRANSFORMS_PATH", str(tmp_path / "projects.json"))
    coll = _FakeCollection(fail_delete=fail_delete)
    return MongoTransformProjectStore(_FakeService(coll)), coll


def _recipe() -> TransformRecipe:
    return TransformRecipe(
        name="clean", steps=[RecipeStep(column="a", op="trim")]
    )


def test_library_delete_removes_both_copies(tmp_path, monkeypatch):
    store, coll = _library_store(tmp_path, monkeypatch)
    recipe = store.save(_recipe())
    assert recipe.id in coll.docs

    assert store.delete(recipe.id) is True
    assert recipe.id not in coll.docs
    assert all(r.id != recipe.id for r in store.list())


def test_library_delete_mongo_failure_is_honest_not_ghost(
    tmp_path, monkeypatch
):
    """Pre-fix: file copy deleted, mongo delete raised and was swallowed —
    the recipe kept showing in list(). Now: nothing is removed and the
    caller gets an honest failure."""
    store, coll = _library_store(tmp_path, monkeypatch, fail_delete=True)
    recipe = store.save(_recipe())

    with pytest.raises(RuntimeError, match="primary store"):
        store.delete(recipe.id)
    # Both copies intact — the failure is honest, not a ghost.
    assert FileTransformLibraryStore._read(store._file).get(recipe.id)
    assert recipe.id in coll.docs


def test_library_delete_mongo_only_record_still_succeeds(tmp_path, monkeypatch):
    """A recipe that only ever existed in Mongo (file mirror lost) must still
    be deletable — 'not found' while the item shows is the twin bug."""
    store, coll = _library_store(tmp_path, monkeypatch)
    recipe = _recipe()
    coll.docs[recipe.id] = {**recipe.to_dict(), "workspace_id": ""}

    assert store.delete(recipe.id) is True
    assert recipe.id not in coll.docs


def _project() -> "TransformProject":
    from services.transform_store import TransformProject

    return TransformProject(name="p1", destination_connector_id="c1")


def test_project_store_delete_is_dual(tmp_path, monkeypatch):
    store, coll = _project_store(tmp_path, monkeypatch)
    project = store.save(_project())
    assert project.id in coll.docs

    assert store.delete(project.id) is True
    assert project.id not in coll.docs


def test_project_store_delete_mongo_failure_honest(tmp_path, monkeypatch):
    store, coll = _project_store(tmp_path, monkeypatch, fail_delete=True)
    project = store.save(_project())

    with pytest.raises(RuntimeError, match="primary store"):
        store.delete(project.id)
    assert project.id in coll.docs
