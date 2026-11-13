"""G11 final review B2 / M-a / M-b / M-d: what earlier runs of an object did.

B2: a new Kafka topic wrote the exact names on run 1, then the old names from
run 2 on: ``topic_has_prior_runs`` counted run 1 (a post-G11 exact-name run)
as legacy history. M-a: history was read under ``current_workspace_filter()``,
which a scheduler beat leaves unbound (every workspace). M-b: history was
matched on ``destination_collection`` only. M-d: the MongoDB / Redis old-field
probe ran for every new collection or prefix, so a new Redis prefix on a large
keyspace was refused when the SCAN budget ran out.

Now every job created after the upgrade carries ``naming_scheme`` and each run
stamps its naming decision per destination object (``naming``). Legacy
history means a job to the SAME resolved writer object, in the run's OWN
workspace, on the SAME cluster, that predates the scheme; a stamped run
decides instead (the most recent stamp wins). The same decision gates the
expensive old-field probes: a new collection / prefix is never probed.
"""

from __future__ import annotations

import os
import uuid
from unittest.mock import patch

import pytest

MAPPINGS = [{"source": "fn", "target": "First Name"}, {"source": "id", "target": "id"}]


@pytest.fixture
def jobs():
    from services import mongodb_service

    svc = mongodb_service.MemoryMongoDBService()
    svc.connect()
    with patch.object(mongodb_service, "_mongodb_service", svc):
        yield svc


def _request(fmt="kafka", *, table="orders_events", workspace="", host="broker",
             extra=None, connector_id=None):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in MAPPINGS],
        source=EndpointConfig(kind="database", format="postgresql", database="d", table="orders"),
        destination=EndpointConfig(kind="database", format=fmt, host=host, port=9092,
                                   table=table, connector_id=connector_id,
                                   extra=dict(extra or {})),
        sync_mode="append", workspace_id=workspace,
    )


def _targets(req):
    return [m["target"] for m in req.mappings]


def _run(svc, req):
    """One Execute up to the write, as the engine does it: the job document
    first (create_transfer_job), the refusal checks under the job scope, then
    the job completes with the object the writer wrote."""
    from services.naming_history import naming_job_scope
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import writer_object_name

    job_id = svc.create_transfer_job({
        "workspace_id": req.workspace_id, "destination_type": req.destination.format,
        "destination_collection": req.destination.table or "",
        "dest_connector_id": req.destination.connector_id,
        "transfer_request": {"destination": {"host": req.destination.host,
                                             "port": req.destination.port}},
    })
    with naming_job_scope(job_id, req.workspace_id):
        refusal = request_refusal(req)
    if not refusal:
        svc.update_job_status(job_id, "completed", records_processed=3,
                              destination_collection=writer_object_name(req.destination,
                                                                        req.source))
    return refusal


def _pre_g11_job(svc, *, obj="orders_events", workspace="", fmt="kafka", host="broker",
                 connector_id=None, port=9092):
    """A job document written before the upgrade: no naming_scheme, no stamp."""
    jid = f"old_{uuid.uuid4().hex[:8]}"
    svc._jobs[jid] = {
        "_id": jid, "workspace_id": workspace, "destination_type": fmt,
        "destination_collection": obj, "records_processed": 5, "status": "completed",
        "dest_connector_id": connector_id,
        "transfer_request": {"destination": {"host": host, "port": port}},
    }


def test_a_new_topic_stays_exact_on_every_run(jobs):
    """The reviewer's flip probe: run 2 follows run 1's exact names."""
    for _ in range(3):
        req = _request()
        assert _run(jobs, req) == ""
        assert _targets(req) == ["First Name", "id"]
    stamps = [j.get("naming") for j in jobs._jobs.values()]
    assert all(s == [{"object": "orders_events", "mode": "exact", "scheme_version": 1}]
               for s in stamps), stamps


def test_a_pre_upgrade_topic_keeps_the_old_names_on_every_run(jobs):
    _pre_g11_job(jobs)
    for _ in range(2):
        req = _request()
        assert _run(jobs, req) == ""
        assert _targets(req) == ["First_Name", "id"]


def test_key_format_exact_then_unset_follows_the_latest_stamp(jobs):
    _pre_g11_job(jobs)
    req = _request(extra={"key_format": "exact"})
    assert _run(jobs, req) == ""
    assert _targets(req) == ["First Name", "id"]
    req = _request()  # the option removed again: the latest run wrote exact names
    assert _run(jobs, req) == ""
    assert _targets(req) == ["First Name", "id"]


def test_another_workspace_history_does_not_count(jobs):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter

    _pre_g11_job(jobs, workspace="wsA")
    req = _request(workspace="wsB")  # a scheduler beat: nothing bound
    assert _run(jobs, req) == ""
    assert _targets(req) == ["First Name", "id"]
    req = _request(workspace="wsB")
    with bound_workspace_filter(WorkspaceFilter.everything(), "wsB", "u"):
        assert _run(jobs, req) == ""
    assert _targets(req) == ["First Name", "id"]
    req = _request(workspace="wsA")
    assert _run(jobs, req) == ""
    assert _targets(req) == ["First_Name", "id"]


def test_default_topic_history_is_found_by_the_writer_object(jobs):
    from services.target_name_legacy import writer_object_name

    req = _request(table="")
    topic = writer_object_name(req.destination, req.source)
    assert topic == "dataflow.orders"
    _pre_g11_job(jobs, obj=topic)
    assert _run(jobs, req) == ""
    assert _targets(req) == ["First_Name", "id"]


def test_same_cluster_needs_a_matching_connector_or_host(jobs):
    _pre_g11_job(jobs, host="")  # an inline job with no host: not provably this cluster
    req = _request(host="")
    assert _run(jobs, req) == ""
    assert _targets(req) == ["First Name", "id"]
    jobs._jobs.clear()
    _pre_g11_job(jobs, host="", connector_id="connK")
    req = _request(host="", connector_id="connK")
    req.destination.connector_id = "connK"
    with patch("src.transfer.adapters.resolve_connector_config",
               return_value={"type": "kafka", "connector_id": "connK"}):
        assert _run(jobs, req) == ""
    assert _targets(req) == ["First_Name", "id"]


def test_validate_and_execute_agree_in_a_scheduler_context(jobs):
    from services.naming_history import naming_job_scope
    from services.target_name_legacy import target_name_gates

    _pre_g11_job(jobs, workspace="wsB")
    _pre_g11_job(jobs, obj="other_topic", workspace="wsA")
    for topic, expect in (("orders_events", "First_Name"), ("other_topic", "First Name")):
        with naming_job_scope("job_beat", "wsB"):  # Execute's own preflight on a beat
            gates = target_name_gates(
                "kafka", MAPPINGS, destination_config={"host": "broker", "port": 9092},
                destination_table=topic, sync_mode="append")
        adopted = any("'First_Name'" in g["message"] for g in gates
                      if g["id"] == "g23d_existing_target_columns")
        req = _request(workspace="wsB", table=topic)
        assert _run(jobs, req) == ""
        assert _targets(req)[0] == expect
        assert adopted == (expect == "First_Name"), gates


# -- the old-field probe runs only on legacy history (M-d, M1) ---------------


def _redis_request(prefix, extra=None):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[{"source": "fn", "target": "full name"}, {"source": "id", "target": "id"}],
        source=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        destination=EndpointConfig(kind="database", format="redis", host="localhost",
                                   port=6379, table=prefix, extra=dict(extra or {})),
        sync_mode="upsert",
    )


def test_a_new_prefix_is_not_refused_and_legacy_history_is(jobs):
    """No key of a new prefix within the SCAN budget: new, not refused. With
    pre-upgrade history the same miss is an unverified check (re-check F3:
    the check runs whenever the latest run did not decide; see
    test_g11_naming_history_attribution)."""
    from services.target_name_legacy import NoKeysWithinBudget, legacy_fields_check

    with patch("services.target_name_legacy.redis_fields_present",
               side_effect=NoKeysWithinBudget("no customers:* key within budget")) as probe:
        assert legacy_fields_check(_redis_request("customers")) == ("", [])
        _pre_g11_job(jobs, obj="customers", fmt="redis", host="localhost", port=6379)
        refusal, _ack = legacy_fields_check(_redis_request("customers"))
    assert probe.call_count == 2
    for words in ("LEGACY_FIELD_PROBE_SECONDS", "index", "write_new", "overwrite"):
        assert words in refusal, refusal


def _redis_url() -> str:
    return (os.environ.get("DATAFLOW_REDIS_URL") or os.environ.get("REDIS_URL")
            or "redis://localhost:6379/0")


def test_a_new_prefix_on_a_large_keyspace_is_not_refused_live(jobs):
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(_redis_url(), socket_connect_timeout=2)
    try:
        client.ping()
    except Exception as exc:  # noqa: BLE001 - no server, no test
        pytest.skip(f"Redis not reachable: {exc}")
    tag = "g11h_" + uuid.uuid4().hex[:6]
    pipe = client.pipeline()
    for i in range(120_000):
        pipe.set(f"{tag}:filler:{i}", "1")
    pipe.execute()
    try:
        from services.target_name_legacy import legacy_fields_check

        with patch.dict(os.environ, {"DATAFLOW_LEGACY_FIELD_PROBE_SECONDS": "0.1"}):
            assert legacy_fields_check(_redis_request(f"{tag}_newprefix")) == ("", [])
    finally:
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor, match=f"{tag}:*", count=5000)
            if keys:
                client.delete(*keys)
            if not cursor:
                break
        client.close()
