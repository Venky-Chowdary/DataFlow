"""Process split: API enqueues, scheduler owns cadence, workers claim by workload.

Proof for docs/ENTERPRISE_SERVICE_TOPOLOGY.md steps 1–4. No cluster, no 1M harness.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

from services.process_role import (
    ack_backend,
    api_executes_transfers,
    orphan_resume_enabled,
    process_role,
    schedule_loop_enabled,
    topology_errors,
    workload_for_sync_mode,
)
from services.worker_leases import WorkerLeaseStore


def _honest_api(monkeypatch) -> None:
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    monkeypatch.setenv("DATAFLOW_PROCESS_ROLE", "api")
    monkeypatch.setenv("DATAFLOW_SCHEDULE_LOOP", "0")
    monkeypatch.setenv("DATAFLOW_API_CLAIM_LOOP", "0")
    monkeypatch.setenv("DATAFLOW_ACK_BACKEND", "mongo")
    for key in (
        "DATAWRAP_MULTI_REPLICA",
        "DATAWRAP_WORKER_FLEET",
        "DATAWRAP_PROCESS_ROLE",
        "DATAWRAP_SCHEDULE_LOOP",
        "DATAWRAP_API_CLAIM_LOOP",
        "DATAWRAP_ACK_BACKEND",
        "DATAWRAP_WORKER_MODE",
        "DATAFLOW_WORKER_MODE",
    ):
        monkeypatch.delenv(key, raising=False)


def test_single_process_has_no_topology_errors(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MULTI_REPLICA", raising=False)
    monkeypatch.delenv("DATAWRAP_MULTI_REPLICA", raising=False)
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "0")
    assert topology_errors() == []


def test_multi_replica_local_fleet_is_refused(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "0")
    monkeypatch.delenv("DATAWRAP_WORKER_FLEET", raising=False)
    monkeypatch.setenv("DATAFLOW_ACK_BACKEND", "mongo")
    errors = topology_errors()
    assert any("WORKER_FLEET" in msg or "thread pool" in msg for msg in errors)


def test_multi_replica_api_must_name_the_split_explicitly(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.delenv("DATAWRAP_MULTI_REPLICA", raising=False)
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    monkeypatch.delenv("DATAWRAP_WORKER_FLEET", raising=False)
    monkeypatch.setenv("DATAFLOW_PROCESS_ROLE", "api")
    monkeypatch.delenv("DATAFLOW_SCHEDULE_LOOP", raising=False)
    monkeypatch.delenv("DATAWRAP_SCHEDULE_LOOP", raising=False)
    monkeypatch.delenv("DATAFLOW_API_CLAIM_LOOP", raising=False)
    monkeypatch.delenv("DATAWRAP_API_CLAIM_LOOP", raising=False)
    monkeypatch.delenv("DATAFLOW_ACK_BACKEND", raising=False)
    monkeypatch.delenv("DATAWRAP_ACK_BACKEND", raising=False)

    errors = topology_errors()
    assert any("SCHEDULE_LOOP" in msg for msg in errors)
    assert any("API_CLAIM_LOOP" in msg for msg in errors)
    assert any("ACK_BACKEND" in msg for msg in errors)

    _honest_api(monkeypatch)
    assert topology_errors() == []
    assert process_role() == "api"
    assert schedule_loop_enabled() is False
    assert api_executes_transfers() is False
    assert orphan_resume_enabled() is True
    assert ack_backend() == "mongo"


def test_scheduler_without_a_loop_is_refused(monkeypatch):
    _honest_api(monkeypatch)
    monkeypatch.setenv("DATAFLOW_PROCESS_ROLE", "scheduler")
    monkeypatch.setenv("DATAFLOW_SCHEDULE_LOOP", "0")
    assert any("no process fires" in msg for msg in topology_errors())
    monkeypatch.setenv("DATAFLOW_SCHEDULE_LOOP", "1")
    assert topology_errors() == []
    assert schedule_loop_enabled() is True
    assert api_executes_transfers() is False


def test_worker_cannot_own_cadence(monkeypatch):
    _honest_api(monkeypatch)
    monkeypatch.setenv("DATAFLOW_PROCESS_ROLE", "worker")
    monkeypatch.setenv("DATAFLOW_SCHEDULE_LOOP", "1")
    assert any("SCHEDULE_LOOP" in msg for msg in topology_errors())
    monkeypatch.setenv("DATAFLOW_SCHEDULE_LOOP", "0")
    assert topology_errors() == []
    assert api_executes_transfers() is False


def test_orphan_scan_can_be_disabled(monkeypatch):
    _honest_api(monkeypatch)
    assert orphan_resume_enabled() is True
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    assert orphan_resume_enabled() is False


def test_cdc_alias_is_a_cdc_workload():
    assert workload_for_sync_mode("cdc") == "cdc"
    assert workload_for_sync_mode("cdc_incremental") == "cdc"
    assert workload_for_sync_mode("full_refresh_append") == "batch"
    assert workload_for_sync_mode(None) == "batch"


def test_run_transfer_async_enqueues_and_does_not_execute(monkeypatch):
    _honest_api(monkeypatch)
    from src.transfer.background import run_transfer_async
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql"),
        destination=EndpointConfig(kind="database", format="postgresql"),
        sync_mode="cdc_incremental",
    )
    with mock.patch("services.worker_fleet.enqueue_job", return_value=True) as enqueued:
        with mock.patch("src.transfer.background._submit_transfer") as submit:
            future = run_transfer_async("job-topology-1", req)
            assert future.result() is None
            submit.assert_not_called()
    enqueued.assert_called_once()
    assert enqueued.call_args.args[0] == "job-topology-1"
    assert enqueued.call_args.kwargs["payload"]["workload"] == "cdc"


class _AckColl:
    """In-memory collection covering the ack claim filter."""

    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}

    def insert_one(self, doc: dict) -> None:
        self.docs[doc["_id"]] = dict(doc)

    def find_one(self, filt: dict) -> dict | None:
        for doc in self.docs.values():
            if _match(doc, filt):
                return dict(doc)
        return None

    def find_one_and_update(self, filt, update, return_document=None):  # noqa: ANN001
        for key, doc in self.docs.items():
            if _match(doc, filt):
                if "$set" in update:
                    doc.update(update["$set"])
                return dict(doc)
        return None

    def update_one(self, filt, update, upsert=False):  # noqa: ANN001
        for doc in self.docs.values():
            if _match(doc, filt):
                if "$set" in update:
                    doc.update(update["$set"])
                if "$unset" in update:
                    for field in update["$unset"]:
                        doc.pop(field, None)
                return
        return None

    def delete_one(self, filt: dict) -> None:
        for key, doc in list(self.docs.items()):
            if _match(doc, filt):
                self.docs.pop(key, None)
                return

    def delete_many(self, filt: dict) -> None:
        if not filt:
            self.docs.clear()
            return
        for key, doc in list(self.docs.items()):
            if _match(doc, filt):
                self.docs.pop(key, None)


def _match(doc: dict, filt: dict) -> bool:
    for key, expected in filt.items():
        if key == "$or":
            if not any(_match(doc, sub) for sub in expected):
                return False
            continue
        if isinstance(expected, dict):
            actual = doc.get(key)
            if "$ne" in expected and actual == expected["$ne"]:
                return False
            if "$gt" in expected and not (actual is not None and actual > expected["$gt"]):
                return False
            if "$lt" in expected and not (actual is not None and actual < expected["$lt"]):
                return False
            if "$lte" in expected and not (actual is not None and actual <= expected["$lte"]):
                return False
            if "$exists" in expected:
                present = key in doc and doc.get(key) is not None
                if bool(expected["$exists"]) != present:
                    return False
            if any(op in expected for op in ("$ne", "$gt", "$lt", "$lte", "$exists")):
                continue
        if doc.get(key) != expected:
            return False
    return True


def test_two_api_processes_share_one_ack():
    from src.ai.copilot.ack_ledger import MongoAckLedger

    coll = _AckColl()
    first = MongoAckLedger(collection=coll, ttl_sec=120)
    second = MongoAckLedger(collection=coll, ttl_sec=120)
    ack_id = first.put(kind="start_transfer", payload={"path": "/data/rows.csv", "password": "secret"})
    payload, err = first.claim(ack_id, actor="api-a")
    assert err == ""
    assert payload is not None
    assert payload["path"] == "/data/rows.csv"
    lost, lost_err = second.claim(ack_id, actor="api-b")
    assert lost is None
    assert "already being confirmed" in lost_err
    first.finalize(ack_id, actor="api-a", result={"job_id": "job-ack"})
    replay, replay_err = second.claim(ack_id, actor="api-b")
    assert replay_err == ""
    assert replay is not None
    assert replay["_idempotent"] is True
    assert replay["job_id"] == "job-ack"
    peeked = second.peek(ack_id)
    assert peeked is not None
    assert "password" not in peeked["preview"] or peeked["preview"].get("password") == "***"


def test_blob_root_is_readable_from_another_upload_dir(tmp_path, monkeypatch):
    blob = tmp_path / "blobs"
    upload_a = tmp_path / "upload-a"
    upload_b = tmp_path / "upload-b"
    monkeypatch.setenv("DATAFLOW_BLOB_ROOT", str(blob))
    monkeypatch.delenv("DATAWRAP_BLOB_ROOT", raising=False)
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(upload_a))
    monkeypatch.delenv("DATAWRAP_UPLOAD_DIR", raising=False)
    monkeypatch.delenv("DATAFLOW_DISABLE_OBJECT_STORE", raising=False)
    monkeypatch.delenv("DATAWRAP_DISABLE_OBJECT_STORE", raising=False)

    from services.transfer_file_staging import hydrate_file_source, persist_file_source
    from src.transfer.models import EndpointConfig, TransferRequest

    body = b"id,name\n1,ada\n"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite"),
        source_filename="rows.csv",
        source_content=body,
    )
    persist_file_source(request, job_token="topologyblob")
    assert request.source_object_uri.startswith("s3://local/transfers/")
    staged = Path(request.source_path)
    assert staged.is_file()
    staged.unlink()

    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(upload_b))
    request.source_path = str(upload_a / "missing.csv")
    request.source_content = b""
    hydrate_file_source(request)
    assert Path(request.source_path).is_file()
    assert Path(request.source_path).read_bytes() == body
    assert str(upload_b) in request.source_path


def _restore_process_env(before: dict[str, str | None]) -> None:
    for key, value in before.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_scheduler_process_exits_when_acks_are_not_shared(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.delenv("DATAFLOW_ACK_BACKEND", raising=False)
    monkeypatch.delenv("DATAWRAP_ACK_BACKEND", raising=False)
    keys = (
        "DATAFLOW_PROCESS_ROLE",
        "DATAFLOW_WORKER_FLEET",
        "DATAFLOW_SCHEDULE_LOOP",
        "DATAFLOW_API_CLAIM_LOOP",
    )
    before = {key: os.environ.get(key) for key in keys}
    from src.scheduler_main import main

    try:
        assert main() == 2
    finally:
        _restore_process_env(before)


def test_worker_process_exits_when_acks_are_not_shared(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.delenv("DATAFLOW_ACK_BACKEND", raising=False)
    monkeypatch.delenv("DATAWRAP_ACK_BACKEND", raising=False)
    keys = (
        "DATAFLOW_PROCESS_ROLE",
        "DATAFLOW_WORKER_FLEET",
        "DATAFLOW_SCHEDULE_LOOP",
        "DATAFLOW_API_CLAIM_LOOP",
    )
    before = {key: os.environ.get(key) for key in keys}
    from src.worker_main import main

    try:
        assert main() == 2
    finally:
        _restore_process_env(before)


def test_held_lease_blocks_reclaim_on_the_memory_store(monkeypatch):
    """Same lease store the worker uses: a live CDC fence is not stale."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.delenv("DATAWRAP_JOB_STORE", raising=False)
    monkeypatch.delenv("DATAFLOW_MULTI_REPLICA", raising=False)
    monkeypatch.setattr(WorkerLeaseStore, "_mongo_collection", lambda self: None)
    job_id = "topology-cdc-lease"
    owner = WorkerLeaseStore("cdc-owner-topology")
    assert owner.acquire(job_id, ttl_seconds=300)
    try:
        assert WorkerLeaseStore("batch-reclaim").is_held(job_id) is True
    finally:
        owner.release(job_id)
        assert WorkerLeaseStore("batch-reclaim").is_held(job_id) is False
