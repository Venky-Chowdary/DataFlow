"""Security re-check F3: what counts as this object's earlier runs, and when the check runs.

* A job whose destination named its cluster through a connection string, or
  through another connector to the same host, was never attributed to this
  cluster: the projection had no connection string and differing connector
  ids ended the comparison (live MongoDB job store).
* A run whose Map renamed nothing stamped "exact" and masked a pre-upgrade
  job, so the next run that did rename skipped the old-field check. A run now
  stamps only what it decided: exact when it checked or acknowledged the old
  fields for that object.
* An existing collection with no attributable history (purged job history, a
  recreated connector, another origin) is checked anyway, bounded by the
  same cap and timeout; a new Redis prefix that no SCAN key shows within the
  budget is new, not refused.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from tests.test_g11_naming_history_live_mongo import _add, _history, stores  # noqa: F401


def test_another_connector_to_the_same_host_is_the_same_cluster(stores, monkeypatch):  # noqa: F811
    tok = uuid.uuid4().hex[:8]
    obj = f"c_conn_{tok}"
    _add(stores, destination_collection=obj, workspace_id="", dest_connector_id="connA",
         transfer_request={"destination": {"host": "mongo-a", "port": 27017}})
    ep_b = SimpleNamespace(connector_id="connB", host="mongo-a", port=27017,
                           connection_string="")
    live, mem = stores
    assert (_history(live, monkeypatch, obj, ep_b), _history(mem, monkeypatch, obj, ep_b)) == (
        "legacy", "legacy")
    ep_c = SimpleNamespace(connector_id="connC", host="mongo-b", port=27017,
                           connection_string="")
    assert (_history(live, monkeypatch, obj, ep_c), _history(mem, monkeypatch, obj, ep_c)) == (
        "none", "none")


def test_a_connection_string_job_is_attributed_to_its_host(stores, monkeypatch):  # noqa: F811
    tok = uuid.uuid4().hex[:8]
    obj = f"c_uri_{tok}"
    _add(stores, destination_collection=obj, workspace_id="",
         transfer_request={"destination": {"connection_string": "mongodb://mongo-a:27017/app"}})
    live, mem = stores
    assert (_history(live, monkeypatch, obj), _history(mem, monkeypatch, obj)) == (
        "legacy", "legacy")


@pytest.fixture
def jobs():
    from services import mongodb_service

    svc = mongodb_service.MemoryMongoDBService()
    svc.connect()
    with patch.object(mongodb_service, "_mongodb_service", svc):
        yield svc


def _mongo_request(collection, mappings, sync_mode="upsert"):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in mappings],
        source=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        destination=EndpointConfig(kind="database", format="mongodb", host="mongo-a",
                                   port=27017, database="app", table=collection),
        sync_mode=sync_mode)


def _run(svc, req):
    from services.naming_history import naming_job_scope
    from services.source_filter_gate import request_refusal

    job = svc.create_transfer_job({
        "workspace_id": "", "destination_type": req.destination.format,
        "destination_collection": req.destination.table,
        "transfer_request": {"destination": {"host": "mongo-a", "port": 27017}}})
    with naming_job_scope(job, ""):
        refusal = request_refusal(req)
    if not refusal:
        svc.update_job_status(job, "completed", records_processed=3)
    return refusal, svc._jobs[job]


PLAIN = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]
RENAMED = [{"source": "id", "target": "id"}, {"source": "fn", "target": "full name"}]


def test_a_run_that_renamed_nothing_does_not_mask_pre_upgrade_history(jobs):
    jid = "old_" + uuid.uuid4().hex[:6]
    jobs._jobs[jid] = {"_id": jid, "workspace_id": "", "destination_type": "mongodb",
                       "destination_collection": "people", "records_processed": 5,
                       "status": "completed",
                       "transfer_request": {"destination": {"host": "mongo-a", "port": 27017}}}
    with patch("services.target_name_legacy.mongo_fields_present",
               return_value={"full_name"}) as probe:
        refusal, job = _run(jobs, _mongo_request("people", PLAIN))
        assert refusal == "" and not job.get("naming"), job.get("naming")
        refusal, _job = _run(jobs, _mongo_request("people", RENAMED))
    assert probe.call_count == 1
    assert "full_name" in refusal


def test_an_acknowledged_run_stamps_exact_and_later_runs_follow_it(jobs):
    jid = "old_" + uuid.uuid4().hex[:6]
    jobs._jobs[jid] = {"_id": jid, "workspace_id": "", "destination_type": "mongodb",
                       "destination_collection": "people", "records_processed": 5,
                       "status": "completed",
                       "transfer_request": {"destination": {"host": "mongo-a", "port": 27017}}}
    with patch("services.target_name_legacy.mongo_fields_present",
               return_value={"full_name"}) as probe:
        req = _mongo_request("people", RENAMED)
        req.destination.extra = {"legacy_field_policy": "write_new"}
        refusal, job = _run(jobs, req)
        assert refusal == ""
        assert job["naming"] == [{"object": "people", "mode": "exact", "scheme_version": 1}]
        refusal, _job = _run(jobs, _mongo_request("people", RENAMED))
    assert refusal == "" and probe.call_count == 1  # the second run follows the stamp


def test_an_existing_collection_with_purged_history_is_still_checked(jobs):
    with patch("services.target_name_legacy.mongo_fields_present",
               return_value={"full_name"}) as probe:
        refusal, _job = _run(jobs, _mongo_request("people", RENAMED))
    assert probe.call_count == 1 and "full_name" in refusal


def test_a_redis_prefix_no_key_shows_is_new_but_an_unfinished_check_refuses(jobs):
    from services.target_name_legacy import LegacyFieldsUnverified, NoKeysWithinBudget
    from src.transfer.models import EndpointConfig, TransferRequest

    def redis_request():
        return TransferRequest(
            mappings=[dict(m) for m in RENAMED],
            source=EndpointConfig(kind="database", format="postgresql", database="d",
                                  table="t"),
            destination=EndpointConfig(kind="database", format="redis", host="localhost",
                                       port=6379, table="customers"),
            sync_mode="upsert")

    from services.target_name_legacy import legacy_fields_check

    with patch("services.target_name_legacy.redis_fields_present",
               side_effect=NoKeysWithinBudget("no customers:* key within budget")):
        assert legacy_fields_check(redis_request()) == ("", [])
    with patch("services.target_name_legacy.redis_fields_present",
               side_effect=LegacyFieldsUnverified("SCAN budget spent mid-sample")):
        refusal, _ack = legacy_fields_check(redis_request())
    assert "DATAFLOW_LEGACY_FIELD_PROBE_SECONDS" in refusal
