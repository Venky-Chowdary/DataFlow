"""Capture-once journals in shared storage: mirror, cross-replica replay, delete, open."""

from __future__ import annotations

import os
import shutil
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")

import src.transfer.engine as engine_mod  # noqa: E402
from services import object_store  # noqa: E402
from services import transfer_journal as tj  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_transfer_journal import _Mongo, _rows, _seed  # noqa: E402


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    monkeypatch.setenv("DATAFLOW_JOURNAL_DIR", str(tmp_path / "journals"))
    monkeypatch.delenv("DATAFLOW_JOURNAL_CAPTURE", raising=False)
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "0")
    monkeypatch.delenv("DATAWRAP_DISABLE_OBJECT_STORE", raising=False)
    monkeypatch.setenv("DATAFLOW_BLOB_ROOT", str(tmp_path / "shared"))
    monkeypatch.delenv("DATAWRAP_BLOB_ROOT", raising=False)
    yield


def _db_request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t",
                              extra={"journal_capture": True}),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
    )


def _replay_request(journal_id: str, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", extra={"journal_id": journal_id}),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
    )


def _run(request: TransferRequest):
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex)


def _remote_keys(journal_id: str) -> list[str]:
    return [k for k in object_store.list_keys("journals/") if f"/{journal_id}/" in k]


def _capture(tmp_path: Path, n: int = 40) -> dict:
    src = tmp_path / "src.db"
    _seed(src, n)
    result = _run(_db_request(src, tmp_path / "d0.db"))
    assert result.success, result.error
    journal = result.source_summary["journal"]
    assert journal["captured"] is True, journal
    return journal


def test_committed_journal_is_mirrored_streams_then_manifest(tmp_path):
    journal = _capture(tmp_path)
    assert journal["storage"]["durable"] is True
    assert journal["storage"]["backend"] == "blob_root"
    keys = _remote_keys(journal["journal_id"])
    assert any(k.endswith("/" + tj.MANIFEST_NAME) for k in keys)
    assert any(k.endswith(".jsonl") for k in keys)
    _dir, manifest = tj.find_journal(journal["journal_id"])
    assert manifest["storage"]["durable"] is True


def test_another_replica_replays_from_shared_storage(tmp_path):
    """The capturing host's disk is gone: the journal still lists and replays."""
    journal = _capture(tmp_path, 25)
    shutil.rmtree(tmp_path / "journals")
    (tmp_path / "src.db").unlink()

    assert [m["journal_id"] for m in tj.list_journals()] == [journal["journal_id"]]
    dst = tmp_path / "d1.db"
    replay = _run(_replay_request(journal["journal_id"], dst))
    assert replay.success, replay.error
    assert replay.records_transferred == 25
    assert len(_rows(dst)) == 25


def test_tampered_remote_stream_is_refused_after_hydrate(tmp_path):
    journal = _capture(tmp_path)
    shutil.rmtree(tmp_path / "journals")
    stream_key = next(k for k in _remote_keys(journal["journal_id"]) if k.endswith(".jsonl"))
    path = tmp_path / "shared" / stream_key
    path.write_bytes(path.read_bytes().replace(b"name-1", b"name-X", 1))
    with pytest.raises(tj.JournalError, match="integrity"):
        tj.resolve_replay(journal["journal_id"])


def test_delete_removes_local_and_shared_copies(tmp_path):
    journal = _capture(tmp_path)
    tj.delete_journal(journal["journal_id"])
    assert _remote_keys(journal["journal_id"]) == []
    with pytest.raises(tj.JournalError):
        tj.find_journal(journal["journal_id"])


def test_remote_journal_is_invisible_to_another_workspace(tmp_path):
    from tests.test_transfer_journal import tj_bind, tj_unbind

    writer = tj.JournalWriter(job_id="w1", workspace_id="ws-acme", request=_db_request(tmp_path / "s", tmp_path / "d"))
    writer.capture_records("t", ["id"], [{"id": 1}])
    jid = writer.commit(delivered_rows=1)["journal_id"]
    shutil.rmtree(tmp_path / "journals")
    token = tj_bind("ws-mallory")
    try:
        assert tj.list_journals() == []
        with pytest.raises(tj.JournalError):
            tj.resolve_replay(jid)
    finally:
        tj_unbind(token)
    assert not (tmp_path / "journals").exists() or not any((tmp_path / "journals").rglob("*.jsonl"))


def test_no_shared_store_is_reported_host_local(tmp_path, monkeypatch):
    monkeypatch.delenv("DATAFLOW_BLOB_ROOT", raising=False)
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    journal = _capture(tmp_path)
    assert journal["storage"]["durable"] is False
    assert "S3_BUCKET or BLOB_ROOT" in journal["storage"]["reason"]


def test_prune_drops_expired_shared_journals(tmp_path):
    journal = _capture(tmp_path)
    shutil.rmtree(tmp_path / "journals")
    counts = tj.prune_journals(now=10**12)
    assert counts["expired"] == 1
    assert _remote_keys(journal["journal_id"]) == []


def test_open_profiles_the_verified_stream_for_the_studio(tmp_path):
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    from src.routers.journals_router import router

    journal = _capture(tmp_path, 30)
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    client = TestClient(app)
    res = client.post(f"/api/v1/journals/{journal['journal_id']}/open")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["row_count"] == 30
    assert body["columns"] == ["id", "name", "amount"]
    assert body["journal"]["journal_id"] == journal["journal_id"]
    assert body["journal"]["rows"] == 30
    assert body["journal"]["storage"]["durable"] is True
    listed = client.get("/api/v1/journals").json()
    assert listed["total"] == 1
    assert client.post("/api/v1/journals/nope.1/open").status_code == 404
    assert client.delete(f"/api/v1/journals/{journal['journal_id']}").status_code == 200
    assert client.get("/api/v1/journals").json()["total"] == 0
