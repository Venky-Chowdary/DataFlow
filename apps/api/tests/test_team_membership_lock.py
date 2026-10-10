"""Cross-request serialization for the workspace last-admin invariant."""

from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from services import team_store


@pytest.fixture
def file_store(tmp_path, monkeypatch):
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "data_dir", lambda: tmp_path, raising=False)


def _run_last_admin_race(monkeypatch, *, change: str) -> tuple[list[str], int]:
    workspace = team_store.create_workspace(name="Race", created_by="first@example.test")
    team_store.add_workspace_member(
        workspace_id=workspace.id,
        email="second@example.test",
        role="admin",
        added_by="first@example.test",
        actor_is_platform_admin=True,
    )

    original_read = team_store._read_memberships
    actor_reads = threading.Barrier(2, timeout=1)
    admin_reads = threading.Barrier(2, timeout=0.25)
    admins = {"first@example.test", "second@example.test"}

    def synchronized_read(*, workspace_id=None, email=None):
        rows = original_read(workspace_id=workspace_id, email=email)
        if workspace_id == workspace.id and email in admins:
            try:
                actor_reads.wait()
            except threading.BrokenBarrierError:
                pass
        if workspace_id == workspace.id and email is None:
            try:
                admin_reads.wait()
            except threading.BrokenBarrierError:
                pass
        return rows

    monkeypatch.setattr(team_store, "_read_memberships", synchronized_read)
    start = threading.Barrier(3, timeout=2)
    results: list[str] = []

    def update(target: str, actor: str):
        start.wait()
        try:
            if change == "remove":
                team_store.remove_workspace_member(
                    workspace_id=workspace.id,
                    email=target,
                    removed_by=actor,
                    actor_is_platform_admin=True,
                )
            else:
                team_store.add_workspace_member(
                    workspace_id=workspace.id,
                    email=target,
                    role="viewer",
                    added_by=actor,
                    actor_is_platform_admin=True,
                )
        except team_store.LastAdminProtected:
            results.append("protected")
        except Exception as exc:
            results.append(type(exc).__name__)
        else:
            results.append("success")

    threads = [
        threading.Thread(
            target=update,
            args=("first@example.test", "second@example.test"),
            daemon=True,
        ),
        threading.Thread(
            target=update,
            args=("second@example.test", "first@example.test"),
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=3)
    assert all(not thread.is_alive() for thread in threads)
    remaining_admins = sum(
        row["role"] == "admin" for row in team_store.list_workspace_members(workspace.id)
    )
    return results, remaining_admins


@pytest.mark.parametrize("change", ["remove", "demote"])
def test_file_backend_serializes_concurrent_last_admin_changes(
    file_store, monkeypatch, change
):
    results, remaining_admins = _run_last_admin_race(monkeypatch, change=change)
    assert sorted(results) == ["protected", "success"]
    assert remaining_admins >= 1


def test_workspace_lock_is_reentrant_while_adding_a_member(file_store):
    workspace = team_store.create_workspace(name="Reentrant", created_by="admin@example.test")
    errors: list[Exception] = []

    def add_inside_lock():
        try:
            with team_store.workspace_membership_lock(workspace.id):
                team_store.add_workspace_member(
                    workspace_id=workspace.id,
                    email="member@example.test",
                    role="editor",
                    added_by="admin@example.test",
                    actor_is_platform_admin=True,
                )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=add_inside_lock, daemon=True)
    thread.start()
    thread.join(timeout=3)
    assert not thread.is_alive(), "reentrant membership lock deadlocked"
    assert not errors
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email="member@example.test"
    ) == "editor"


LIVE_MONGO_URI = os.getenv("DATAFLOW_LIVE_MONGO_URI", "").strip()


@pytest.mark.skipif(not LIVE_MONGO_URI, reason="DATAFLOW_LIVE_MONGO_URI is not configured")
def test_live_mongo_serializes_concurrent_last_admin_removals(monkeypatch):
    client = MongoClient(LIVE_MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        client.close()
        pytest.skip("DATAFLOW_LIVE_MONGO_URI is unavailable")
    db = client[f"dataflow_team_lock_it_{uuid.uuid4().hex}"]
    monkeypatch.setattr(team_store, "mongo_database", lambda: db)
    monkeypatch.setattr(team_store, "data_dir", lambda: Path("/tmp"), raising=False)
    team_store._PREPARED_DATABASES.discard(db.name)
    try:
        results, remaining_admins = _run_last_admin_race(monkeypatch, change="remove")
        assert sorted(results) == ["protected", "success"]
        assert remaining_admins >= 1
    finally:
        client.drop_database(db.name)
        client.close()
        team_store._PREPARED_DATABASES.discard(db.name)
