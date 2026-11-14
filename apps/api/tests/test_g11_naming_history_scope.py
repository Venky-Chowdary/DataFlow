"""Security check M1: naming history knows the database.

M1: history was keyed by object name and cluster. A run into scratch.people
stamped "exact"; a later run into app.people on the same server followed that
stamp, skipped the old-field probe and wrote "full name" beside the legacy
full_name. The stamp and the match now carry the destination's namespace
(MongoDB database, Redis db index, SQL database and schema): an "exact"
stamp counts only for the same database.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

RENAMED = [{"source": "id", "target": "id"}, {"source": "fn", "target": "full name"}]


@pytest.fixture
def jobs():
    from services import mongodb_service

    svc = mongodb_service.MemoryMongoDBService()
    svc.connect()
    with patch.object(mongodb_service, "_mongodb_service", svc):
        yield svc


def _mongo(database, connector_id=None):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in RENAMED],
        source=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        destination=EndpointConfig(kind="database", format="mongodb", host="mongo-a",
                                   port=27017, database=database, table="people",
                                   connector_id=connector_id),
        sync_mode="upsert")


def _redis(db_index="0"):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in RENAMED],
        source=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        destination=EndpointConfig(kind="database", format="redis", host="redis-a", port=6379,
                                   database=db_index, table="customers"),
        sync_mode="upsert")


def _run(svc, req):
    """One Execute: the job document as job_submission writes it, then the checks."""
    from services.naming_history import naming_job_scope
    from services.source_filter_gate import request_refusal
    from src.transfer.models import endpoint_to_dict

    job = svc.create_transfer_job({
        "workspace_id": "", "destination_type": req.destination.format,
        "destination_collection": req.destination.table,
        "destination_database": req.destination.database or "",
        "transfer_request": {"destination": endpoint_to_dict(req.destination)}})
    with naming_job_scope(job, ""):
        refusal = request_refusal(req)
    if not refusal:
        svc.update_job_status(job, "completed", records_processed=1)
    return refusal, svc._jobs[job]


def _app_holds_old_field(cfg, table, olds):
    return {"full_name"} if str(cfg.get("database")) == "app" else set()


def test_an_exact_stamp_in_another_database_does_not_mask_this_one(jobs):
    """The reviewer's probe: scratch.people then app.people on the same host."""
    with patch("services.target_name_legacy.mongo_fields_present",
               side_effect=_app_holds_old_field) as probe:
        refusal, _ = _run(jobs, _mongo("app"))
        assert "full_name" in refusal, refusal
        refusal, job = _run(jobs, _mongo("scratch"))
        assert refusal == ""
        assert job["naming"][0]["scope"] == "scratch", job["naming"]
        refusal, _ = _run(jobs, _mongo("app"))
    assert "full_name" in refusal, refusal
    assert probe.call_count == 3


def test_an_exact_stamp_in_the_same_database_is_followed(jobs):
    with patch("services.target_name_legacy.mongo_fields_present",
               return_value=set()) as probe:
        assert _run(jobs, _mongo("app"))[0] == ""
        assert _run(jobs, _mongo("app"))[0] == ""
    assert probe.call_count == 1


def test_another_redis_db_index_is_another_namespace(jobs):
    with patch("services.target_name_legacy.redis_fields_present",
               return_value=set()) as probe:
        assert _run(jobs, _redis("1"))[0] == ""
        refusal, _ = _run(jobs, _redis("0"))
    assert refusal == "" and probe.call_count == 2
