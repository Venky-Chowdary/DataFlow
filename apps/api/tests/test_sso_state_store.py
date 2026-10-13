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


def _claim_once_in_process(path: str, queue) -> None:
    from services import sso_state

    sso_state.mongo_database = lambda: None
    sso_state.data_dir = lambda: Path(path).parent
    queue.put(
        sso_state.claim_once(
            "oidc_nonce",
            "shared-token",
            datetime.now(timezone.utc) + timedelta(minutes=5),
        )
    )


def _get_state_in_process(path: str, queue) -> None:
    from services import sso_state

    sso_state.mongo_database = lambda: None
    sso_state.STATE_PATH = Path(path)
    queue.put(sso_state.get_state("shared-state", "oidc") is not None)


def _run_eight_processes(target, path: Path) -> list[bool]:
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    processes = [
        context.Process(target=target, args=(str(path), queue))
        for _ in range(8)
    ]
    for process in processes:
        process.start()
    results = [queue.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
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
    results = _run_eight_processes(
        _claim_once_in_process,
        tmp_path / "sso_replay.json",
    )
    assert sum(results) == 1


def test_file_get_state_is_atomic_across_processes(tmp_path):
    from services import sso_state

    sso_state.mongo_database = lambda: None
    sso_state.STATE_PATH = tmp_path / "sso_state.json"
    sso_state.set_state("shared-state", "oidc")
    if hasattr(sso_state, "_load_file"):
        barrier = multiprocessing.get_context("fork").Barrier(8)
        load_file = sso_state._load_file

        def synchronized_load():
            states = load_file()
            barrier.wait(timeout=10)
            return states

        sso_state._load_file = synchronized_load

    results = _run_eight_processes(_get_state_in_process, sso_state.STATE_PATH)
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
