"""Deleting a schedule must leave nothing behind — including the last one.

A delete that returns ``{"success": true}`` while the document is still in Mongo
is worse than a failure: the next read resurrects the schedule, and with it any
open Autopilot approval stored on it.
"""

from __future__ import annotations

from typing import Any

import pytest

from services import schedule_store as store


class _FakeColl:
    """The subset of a pymongo collection ``schedule_store`` actually uses."""

    def __init__(self, docs: dict[str, dict[str, Any]]) -> None:
        self.docs = docs

    def find(self, *_a: Any, **_k: Any) -> list[dict[str, Any]]:
        return [dict(d) for d in self.docs.values()]

    def find_one(self, filt: dict[str, Any], *_a: Any, **_k: Any) -> dict[str, Any] | None:
        doc = self.docs.get(filt.get("_id"))
        return dict(doc) if doc else None

    def find_one_and_update(
        self,
        filt: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        return_document: bool = True,
    ) -> dict[str, Any] | None:
        oid = filt["_id"]
        if oid not in self.docs and not upsert:
            return None
        payload = {**dict(update.get("$set") or {}), "_id": oid}
        self.docs[oid] = payload
        return dict(payload)

    def replace_one(self, filt: dict[str, Any], doc: dict[str, Any], upsert: bool = False) -> None:
        self.docs[filt["_id"]] = dict(doc)

    def delete_many(self, filt: dict[str, Any]) -> None:
        clause = filt.get("_id") or {}
        if "$nin" in clause:
            keep = set(clause["$nin"])
            for key in [k for k in self.docs if k not in keep]:
                del self.docs[key]
            return
        if "$in" in clause:
            for key in [k for k in self.docs if k in set(clause["$in"])]:
                del self.docs[key]

    def update_one(
        self, filt: dict[str, Any], update: dict[str, Any], upsert: bool = False
    ) -> None:
        oid = filt.get("_id")
        doc = self.docs.get(oid)
        if doc is None:
            if not upsert:
                return
            doc = {"_id": oid}
            self.docs[oid] = doc
        doc.update(dict(update.get("$set") or {}))


class _FakeDB:
    def __init__(self) -> None:
        self.collections: dict[str, _FakeColl] = {}

    def __getitem__(self, name: str) -> _FakeColl:
        return self.collections.setdefault(name, _FakeColl({}))


class _FakeMongo:
    client = object()

    def __init__(self) -> None:
        self.db = _FakeDB()

    def get_database(self) -> _FakeDB:
        return self.db


@pytest.fixture()
def mongo(monkeypatch: pytest.MonkeyPatch) -> _FakeMongo:
    svc = _FakeMongo()
    monkeypatch.setattr(store, "_mongo_backend", lambda: svc)
    return svc


def _docs(mongo: _FakeMongo) -> dict[str, dict[str, Any]]:
    return mongo.db["pipeline_schedules"].docs


def _make(name: str) -> store.PipelineSchedule:
    return store.create_schedule(
        {
            "name": name,
            "source_connector_id": "src",
            "source_table": "t",
            "dest_connector_id": "dst",
            "dest_table": "u",
            "interval": "daily",
        }
    )


def test_deleting_the_only_schedule_removes_the_document(mongo: _FakeMongo) -> None:
    sched = _make("only")
    assert list(_docs(mongo)) == [sched.id]

    assert store.delete_schedule(sched.id) is True

    assert _docs(mongo) == {}
    assert store.list_schedules() == []
    assert store.get_schedule(sched.id) is None


def test_deleting_one_of_two_keeps_the_other(mongo: _FakeMongo) -> None:
    first = _make("first")
    second = _make("second")

    assert store.delete_schedule(first.id) is True

    assert list(_docs(mongo)) == [second.id]
    assert [s.id for s in store.list_schedules()] == [second.id]


def test_deleting_the_last_of_several_leaves_nothing(mongo: _FakeMongo) -> None:
    ids = [_make(f"s{i}").id for i in range(3)]
    for sid in ids:
        assert store.delete_schedule(sid) is True

    assert _docs(mongo) == {}
    assert store.list_schedules() == []


def test_deleting_an_unknown_id_changes_nothing(mongo: _FakeMongo) -> None:
    sched = _make("keep")

    assert store.delete_schedule("no-such-id") is False

    assert list(_docs(mongo)) == [sched.id]


def test_a_legacy_blob_cannot_resurrect_a_deleted_schedule(mongo: _FakeMongo) -> None:
    """The per-schedule store is authoritative once it has written."""
    sched = _make("only")
    mongo.db["schedule_store"].docs["primary"] = {
        "_id": "primary",
        "schedules": [sched.to_dict()],
    }

    assert store.delete_schedule(sched.id) is True

    assert store.list_schedules() == []
    assert mongo.db["schedule_store"].docs["primary"]["superseded_by"] == "pipeline_schedules"


def test_an_unsuperseded_blob_is_still_read(mongo: _FakeMongo) -> None:
    """Migration path: a blob-only deployment keeps loading until the first write."""
    mongo.db["schedule_store"].docs["primary"] = {
        "_id": "primary",
        "schedules": [
            {
                "id": "legacy-1",
                "name": "legacy",
                "source_connector_id": "src",
                "source_table": "t",
                "dest_connector_id": "dst",
                "dest_table": "u",
                "interval": "daily",
            }
        ],
    }

    assert [s.id for s in store.list_schedules()] == ["legacy-1"]
