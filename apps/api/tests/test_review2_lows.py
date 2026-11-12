"""Security review round 2: the Low findings on the exports migration.

- The migration ran inside the API lifespan, on every boot while unowned
  files remained (2.4-3.1 s for 20k flat files), and its owner lookup was an
  unindexed ``$regex`` scan of ``transfer_jobs``; every download of a flat file
  ran the same root-wide scan to judge one file.
- ``_move`` checked ``target.exists()`` and then ``os.replace``d, overwriting a
  file that appeared in between (an old pod still writing during a rolling
  upgrade).

Now the startup pass runs in a background thread with its own error
handling, ``destination_summary.path`` is indexed through the job store's
index-ensure step, a single-file owner lookup is an exact-path query, and a
move is ``os.link`` + ``unlink``: a file that appears first is never
overwritten and both files are kept.
"""

from __future__ import annotations

import inspect
import os
import threading
from pathlib import Path

import pytest


@pytest.fixture
def exports(tmp_path, monkeypatch):
    monkeypatch.delenv("DATAWRAP_EXPORTS_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    (tmp_path / "exports").mkdir()
    return tmp_path / "exports"


WS = "0b5c3f8e-0000-4000-8000-00000000000a"


def test_move_never_overwrites_a_file_that_appears_between_check_and_move(exports, monkeypatch):
    from services import export_migration
    from services.export_paths import workspace_exports_dir

    (exports / "orders.csv").write_text("OLD-FLAT", encoding="utf-8")
    target = workspace_exports_dir(WS) / "orders.csv"
    real_exists = Path.exists

    def racing_exists(self, *a, **k):
        if self == target and not real_exists(self):
            # The check runs; a new file lands right after it.
            target.write_text("NEW-FROM-OLD-POD", encoding="utf-8")
            return False
        return real_exists(self, *a, **k)

    monkeypatch.setattr(Path, "exists", racing_exists)
    outcome = export_migration._move(exports, "orders.csv", WS)
    monkeypatch.setattr(Path, "exists", real_exists)
    assert outcome == "kept_existing"
    assert target.read_text(encoding="utf-8") == "NEW-FROM-OLD-POD"
    assert (exports / "orders.csv").read_text(encoding="utf-8") == "OLD-FLAT"


def test_move_moves_when_nothing_is_in_the_way(exports):
    from services import export_migration
    from services.export_paths import workspace_exports_dir

    (exports / "nightly").mkdir()
    (exports / "nightly" / "orders.csv").write_text("FLAT", encoding="utf-8")
    assert export_migration._move(exports, "nightly/orders.csv", WS) == "moved"
    assert (workspace_exports_dir(WS) / "nightly" / "orders.csv").read_text(encoding="utf-8") == "FLAT"
    assert not (exports / "nightly" / "orders.csv").exists()
    assert export_migration._move(exports, "nightly/orders.csv", WS) in ("gone", "kept_existing")


def test_startup_migration_runs_off_the_lifespan_critical_path(monkeypatch):
    from services import export_migration
    from src import main

    source = inspect.getsource(main.lifespan)
    assert "await asyncio.to_thread(migrate_flat_exports_at_startup)" not in source
    assert "start_in_background(migrate_flat_exports_at_startup" in source
    release = threading.Event()
    seen: list[str] = []

    def slow():
        release.wait(5)
        seen.append("ran")
        raise RuntimeError("an export volume problem")

    thread = export_migration.start_in_background(slow)
    assert thread.is_alive(), "the caller is not held while the pass runs"
    release.set()
    thread.join(5)
    assert seen == ["ran"] and not thread.is_alive()


def test_export_path_claims_are_indexed_and_a_single_file_is_an_exact_query(monkeypatch):
    from services.mongodb_service import MongoDBService

    class Coll:
        def __init__(self):
            self.indexes: list[str] = []
            self.queries: list[dict] = []

        def create_index(self, key, **_k):
            self.indexes.append(key)

        def find(self, query, _projection=None):
            self.queries.append(query)
            return []

    coll = Coll()
    svc = MongoDBService.__new__(MongoDBService)
    monkeypatch.setattr(svc, "get_database", lambda: {"transfer_jobs": coll}, raising=False)
    svc.list_export_file_claims("/srv/exports")
    svc.list_export_file_claims("/srv/exports", path="/srv/exports/orders.csv")
    assert coll.indexes == ["destination_summary.path"], "ensured once per process"
    assert coll.queries[1] == {"destination_summary.path": "/srv/exports/orders.csv"}


def test_one_files_owner_never_scans_the_whole_root(exports, monkeypatch):
    from services import export_migration

    calls: list[dict] = []

    class Store:
        def list_export_file_claims(self, root, path=None):
            calls.append({"root": root, "path": path})
            return [(str(exports / "orders.csv"), WS)] if path else []

        def get_job(self, _job_id):
            return None

    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: Store())
    assert export_migration.flat_export_owner("orders.csv") == WS
    assert calls == [{"root": str(exports.resolve()), "path": str(exports.resolve() / "orders.csv")}]


def test_memory_store_answers_an_exact_path_query():
    from services.mongodb_service import MemoryMongoDBService

    store = MemoryMongoDBService()
    store.connect()
    store.create_transfer_job({"workspace_id": WS, "destination_summary": {"path": "/srv/exports/a.csv"}})
    store.create_transfer_job({"workspace_id": "other", "destination_summary": {"path": "/srv/exports/ab.csv"}})
    assert store.list_export_file_claims("/srv/exports", path="/srv/exports/a.csv") == [("/srv/exports/a.csv", WS)]
    assert len(store.list_export_file_claims("/srv/exports")) == 2


def test_move_without_hard_links_still_never_overwrites(exports, monkeypatch):
    """A volume with no hard links: the fallback is an ``O_EXCL`` copy."""
    from services import export_migration
    from services.export_paths import workspace_exports_dir

    def no_links(*_a, **_k):
        raise OSError(1, "Operation not permitted")

    monkeypatch.setattr(os, "link", no_links)
    (exports / "a.csv").write_text("FLAT-A", encoding="utf-8")
    assert export_migration._move(exports, "a.csv", WS) == "moved"
    assert (workspace_exports_dir(WS) / "a.csv").read_text(encoding="utf-8") == "FLAT-A"
    (exports / "a.csv").write_text("SECOND", encoding="utf-8")
    real_exists = Path.exists
    monkeypatch.setattr(Path, "exists", lambda self, *a, **k: False if self.name == "a.csv" and "_workspaces" in str(self) else real_exists(self, *a, **k))
    assert export_migration._move(exports, "a.csv", WS) == "kept_existing"
    assert (workspace_exports_dir(WS) / "a.csv").read_text(encoding="utf-8") == "FLAT-A"
    assert (exports / "a.csv").read_text(encoding="utf-8") == "SECOND"
