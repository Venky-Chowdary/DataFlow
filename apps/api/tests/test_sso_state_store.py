"""Concurrency and replay guarantees for transient SSO state."""

from __future__ import annotations

import hashlib
import multiprocessing
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def _ensure_spawn_can_import_services() -> None:
    """Spawn children import ``services`` by name. Pytest's cwd is on sys.path
    in the parent, but a fresh interpreter only sees PYTHONPATH."""
    root = str(Path(__file__).resolve().parents[1])
    current = os.environ.get("PYTHONPATH", "")
    parts = [part for part in current.split(os.pathsep) if part]
    if root not in parts:
        os.environ["PYTHONPATH"] = os.pathsep.join([root, *parts]) if parts else root


def _run_eight_processes(target, path: Path) -> list[bool]:
    """Eight fresh interpreters, one flocked file, exactly one winner.

    ``spawn`` (vfork + exec) is required. ``fork`` copies this pytest process.
    On a GitHub runner the overcommit heuristic charges that copy and
    ``Process.start`` raises ``OSError: Cannot allocate memory``. The local
    kernel has overcommit enabled, so the same fork succeeds here and hides
    the failure.
    """
    from services.sso_file_race import RACE_WAIT_SECONDS

    _ensure_spawn_can_import_services()
    context = multiprocessing.get_context("spawn")
    assert context.get_start_method() == "spawn"
    queue = context.Queue()
    barrier = context.Barrier(8)
    processes = [
        context.Process(target=target, args=(str(path), queue, barrier))
        for _ in range(8)
    ]
    for process in processes:
        process.start()
    results = [queue.get(timeout=RACE_WAIT_SECONDS) for _ in processes]
    for process in processes:
        process.join(timeout=RACE_WAIT_SECONDS)
        assert process.exitcode == 0
    return results


def test_claim_once_returns_true_only_for_first_claim(tmp_path, monkeypatch):
    from services import sso_state

    monkeypatch.setattr(sso_state, "mongo_database", lambda: None)
    monkeypatch.setattr(sso_state, "data_dir", lambda: tmp_path)
    expires = datetime.now(timezone.utc) + timedelta(minutes=5)

    assert sso_state.claim_once("oidc_nonce", "same-token", expires) is True
    assert sso_state.claim_once("oidc_nonce", "same-token", expires) is False
    assert sso_state.claim_once("saml_assertion", "same-token", expires) is True


def test_claim_once_purges_expired_file_entries(tmp_path, monkeypatch):
    from services import sso_state
    from services.metadata_backend import json_doc_transaction

    monkeypatch.setattr(sso_state, "mongo_database", lambda: None)
    monkeypatch.setattr(sso_state, "data_dir", lambda: tmp_path)
    token_id = "expired-token"
    key = f"oidc_nonce:{hashlib.sha256(token_id.encode()).hexdigest()}"
    path = tmp_path / "sso_replay.json"
    with json_doc_transaction(path, {}) as replay:
        replay[key] = {
            "expires_at": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        }

    expires = datetime.now(timezone.utc) + timedelta(minutes=5)
    assert sso_state.claim_once("oidc_nonce", token_id, expires) is True


def test_claim_once_rejects_empty_token_id(tmp_path, monkeypatch):
    from services import sso_state

    monkeypatch.setattr(sso_state, "mongo_database", lambda: None)
    monkeypatch.setattr(sso_state, "data_dir", lambda: tmp_path)
    with pytest.raises(ValueError):
        sso_state.claim_once("oidc_nonce", "", datetime.now(timezone.utc))


def test_file_claim_once_is_atomic_across_processes(tmp_path):
    from services.sso_file_race import claim_once_child

    results = _run_eight_processes(claim_once_child, tmp_path / "sso_replay.json")
    assert sum(results) == 1


def test_file_get_state_is_atomic_across_processes(tmp_path):
    from services import sso_state
    from services.sso_file_race import get_state_child

    sso_state.mongo_database = lambda: None
    sso_state.STATE_PATH = tmp_path / "sso_state.json"
    sso_state.set_state("shared-state", "oidc")
    results = _run_eight_processes(get_state_child, sso_state.STATE_PATH)
    assert sum(results) == 1


def test_mongo_errors_raise_store_unavailable(monkeypatch):
    from services import sso_state

    class BrokenCollection:
        def replace_one(self, *_args, **_kwargs):
            raise PyMongoError("database unavailable")

    class BrokenDatabase:
        def __getitem__(self, _name):
            return BrokenCollection()

    monkeypatch.setattr(sso_state, "mongo_database", lambda: BrokenDatabase())
    with pytest.raises(sso_state.SsoStoreUnavailable):
        sso_state.set_state("state", "oidc")


LIVE_MONGO_URI = os.getenv("DATAFLOW_LIVE_MONGO_URI", "").strip()


@pytest.mark.skipif(not LIVE_MONGO_URI, reason="DATAFLOW_LIVE_MONGO_URI is not configured")
def test_live_mongo_state_replay_and_ttl_index(monkeypatch):
    from services import sso_state

    client = MongoClient(LIVE_MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    db = client[f"dataflow_sso_it_{uuid.uuid4().hex}"]
    monkeypatch.setattr(sso_state, "mongo_database", lambda: db)
    monkeypatch.setattr(sso_state, "_REPLAY_INDEX_READY", False)

    token_id = uuid.uuid4().hex
    expiry = datetime.now(timezone.utc) + timedelta(minutes=2)
    assert sso_state.claim_once("live_test", token_id, expiry) is True
    assert sso_state.claim_once("live_test", token_id, expiry) is False
    indexes = db["sso_replay"].index_information()
    assert any(
        index.get("key") == [("expires_at", 1)]
        and index.get("expireAfterSeconds") == 0
        for index in indexes.values()
    )

    state = uuid.uuid4().hex
    sso_state.set_state(state, "oidc", {"client_id": "live-test"})
    assert sso_state.get_state(state, "oidc")["extra"]["client_id"] == "live-test"
    assert sso_state.get_state(state, "oidc") is None
    client.drop_database(db.name)
    client.close()
