"""D289 — resume / retry dial the connector's *current* host, not the one stored at job start.

Prod repro: job on tunnel A cancelled; tunnel A killed; the PG connector
repointed to tunnel C (test_connector OK); resume_job failed "could not
translate host name <tunnel A>". Same without the cancel (job failed when the
tunnel died, repointed, resume dialled the dead host again).

Root cause: the REST route re-resolves the saved connectors in memory (D172)
but, in claim-queue mode, enqueues only the job id; the worker rebuilds the
request from the stored ``transfer_request`` — the host at job start — and
runs that. If the old host still resolved to another database, the resumed
job would read or write the wrong target.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.adapters as adapters  # noqa: E402
import src.transfer.background as background  # noqa: E402
from src.transfer.models import (  # noqa: E402
    EndpointConfig,
    TransferRequest,
    transfer_request_to_dict,
)

OLD, NEW = "odkwh.tunnel.example", "udegi.tunnel.example"


def _saved(host: str, port: int, db: str = "qa") -> dict:
    return {
        "type": "postgresql", "host": host, "port": port, "database": db, "schema": "",
        "username": "qa", "password": "secret", "connection_string": "", "warehouse": "",
        "ssl": False, "extra": {},
    }


class _Mongo:
    def __init__(self, payload: dict):
        self.job = {"_id": "job-6aea290c", "status": "cancelled", "transfer_request": payload,
                    "records_processed": 0}
        self.fields: dict = {}
        self.statuses: list = []

    def is_cancel_requested(self, job_id):
        return False

    def get_job(self, job_id):
        return dict(self.job)

    def update_job_status(self, job_id, status, **kw):
        self.statuses.append((status, kw))
        return True

    def update_job_fields(self, job_id, fields, **kw):
        self.fields.update(fields)
        return True


@pytest.fixture()
def world(monkeypatch):
    """A job recorded against tunnel A; both connectors since repointed to tunnel C."""
    saved = {"c-src": _saved(NEW, 44067), "c-dst": _saved(NEW, 44067, "wh")}
    monkeypatch.setattr(adapters, "_lookup_saved_connector", lambda cid, workspace_id=None: saved.get(cid))
    import services.connector_store as connector_store

    monkeypatch.setattr(connector_store, "get_connector", lambda cid, workspace_id=None: object() if cid in saved else None)
    recorded = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", connector_id="c-src", host=OLD, port=41593,
                              database="qa", username="qa", password="secret", table="orders"),
        destination=EndpointConfig(kind="database", format="postgresql", connector_id="c-dst", host=OLD, port=41593,
                                   database="wh", username="qa", password="secret", table="orders"),
        sync_mode="incremental_append",
    )
    mongo = _Mongo(transfer_request_to_dict(recorded))
    monkeypatch.setattr(background, "get_mongodb_service", lambda: mongo)
    dialled: list = []

    class _Engine:
        def execute_tracked(self, request, job_id, resume=False):
            dialled.append((request.source.host, request.source.port, request.destination.host, resume))

    monkeypatch.setattr(background, "get_transfer_engine", lambda: _Engine())
    events: list = []
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw))
    return saved, mongo, dialled, events


def test_worker_resume_dials_the_connectors_current_host(world):
    saved, mongo, dialled, events = world
    mongo.job["checkpoint"] = {"rows_processed": 10, "offset": 10}
    background.run_fleet_job("job-6aea290c")
    assert dialled, "the engine must run"
    src_host, src_port, dst_host, _ = dialled[0]
    assert (src_host, src_port, dst_host) == (NEW, 44067, NEW)
    assert OLD not in (src_host, dst_host)
    # Audited, and the change is on the job for the operator — hosts only, no secrets.
    assert len(events) == 1 and events[0]["action"] == "job.endpoint_refreshed"
    changes = events[0]["details"]["changes"]
    assert {c["role"] for c in changes} == {"source", "destination"}
    assert all(OLD in c["from"] and NEW in c["to"] for c in changes)
    note = mongo.fields["endpoint_refresh"]
    assert OLD in note["note"] and NEW in note["note"]
    assert "secret" not in str(events) and "secret" not in str(note)


def test_local_resume_with_refreshed_request_is_audited_once(world):
    """The REST route already refreshed the request in memory; the run still
    records that the endpoint moved, exactly once."""
    saved, mongo, dialled, events = world
    from src.transfer.adapters import refresh_saved_connectors
    from src.transfer.models import transfer_request_from_dict

    req = transfer_request_from_dict(mongo.job["transfer_request"])
    refresh_saved_connectors(req)  # what the route does (D172)
    background._run_transfer("job-6aea290c", req, resume=True)
    assert dialled[0][0] == NEW
    assert [e["action"] for e in events] == ["job.endpoint_refreshed"]


def test_unchanged_connector_leaves_no_audit_trail(world):
    saved, mongo, dialled, events = world
    saved["c-src"] = _saved(OLD, 41593)
    saved["c-dst"] = _saved(OLD, 41593, "wh")
    background.run_fleet_job("job-6aea290c")
    assert dialled[0][0] == OLD
    assert events == [] and "endpoint_refresh" not in mongo.fields


def test_deleted_connector_fails_the_job_with_its_name(world):
    saved, mongo, dialled, events = world
    saved.pop("c-dst")
    background.run_fleet_job("job-6aea290c")
    assert not dialled, "a job whose connector is gone must not run against the stored host"
    failed = [kw for status, kw in mongo.statuses if status == "failed"]
    assert failed and "c-dst" in failed[-1]["error"] and "no longer exists" in failed[-1]["error"]
