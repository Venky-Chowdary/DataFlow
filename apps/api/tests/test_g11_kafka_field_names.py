"""G11 re-review M3: Kafka topics keep the field names earlier runs gave them.

N-F1 made the Kafka writer produce each Map target exactly as mapped
(``First Name``, ``address.city``) where it used to sanitize them
(``First_Name``, ``address_city``). On an existing topic that silently changed
the event shape consumers read (and a composite message key) with no Validate
signal; with a Schema Registry subject holding the old names the write failed
with a misleading "physical DDL missing for First Name, address.city".

Now: a registry subject's fields are the topic's existing columns and the old
spelling is adopted (as for SaaS fields), at Validate and Execute; without a
registry, a topic with earlier runs refuses the renamed targets at Validate
(g23e) and Execute unless ``key_format=legacy`` (keep the old names) or
``legacy_field_policy=write_new`` (recorded on the run).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

MAPPINGS = [{"source": "fn", "target": "First Name"},
            {"source": "city", "target": "address.city"},
            {"source": "id", "target": "id"}]
REGISTRY = "http://registry.invalid:8081"


def _request(extra=None, table="customers"):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in MAPPINGS],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format="kafka", host="broker",
                                   port=9092, table=table, extra=dict(extra or {})),
        sync_mode="append",
    )


@pytest.fixture
def jobs():
    """The in-memory job history; records a prior Kafka run when asked."""
    from services import mongodb_service

    svc = mongodb_service.MemoryMongoDBService()
    svc.connect()
    with patch.object(mongodb_service, "_mongodb_service", svc):
        yield svc


def _prior_run(svc, topic="customers", host="broker", rows=5):
    svc._jobs["job_prior"] = {
        "_id": "job_prior", "destination_type": "kafka", "destination_collection": topic,
        "records_processed": rows, "status": "completed", "workspace_id": "",
        "transfer_request": {"destination": {"host": host}},
    }


def _targets(rows):
    return [r["target"] if isinstance(r, dict) else r.target for r in rows]


# -- Schema Registry subject: adopt the old spelling ------------------------


def test_registry_subject_fields_are_adopted_at_execute(jobs):
    from services.source_filter_gate import request_refusal

    req = _request({"schema_registry_url": REGISTRY})
    with patch("services.kafka_field_names.registry_subject_fields",
               return_value=["First_Name", "address_city", "id"]) as fetch:
        assert request_refusal(req) == ""
    assert fetch.call_args.args[:2] == (REGISTRY, "customers")
    assert _targets(req.mappings) == ["First_Name", "address_city", "id"]


def test_registry_subject_fields_are_adopted_at_validate():
    from services.target_name_legacy import adopt_rows_for_run, target_name_gates

    cfg = {"schema_registry_url": REGISTRY}
    with patch("services.kafka_field_names.registry_subject_fields",
               return_value=["First_Name", "address_city", "id"]):
        rows = adopt_rows_for_run("kafka", MAPPINGS, [], "append",
                                  destination_config=cfg, destination_table="customers")
        gates = target_name_gates("kafka", MAPPINGS, destination_config=cfg,
                                  destination_table="customers", sync_mode="append")
    assert _targets(rows) == ["First_Name", "address_city", "id"]
    note = next(g for g in gates if g["id"] == "g23d_existing_target_columns")
    assert "First_Name" in note["message"]
    assert not [g for g in gates if g["status"] == "block"]


def test_registry_failure_fails_closed_at_execute(jobs):
    from services.source_filter_gate import request_refusal

    req = _request({"schema_registry_url": REGISTRY})
    with patch("services.kafka_field_names.registry_subject_fields",
               side_effect=RuntimeError("HTTP 503")):
        refusal = request_refusal(req)
    assert "Could not read" in refusal and "HTTP 503" in refusal


# -- no registry: topic with earlier runs ------------------------------------


def test_topic_with_prior_runs_blocks_renamed_targets(jobs):
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import target_name_gates

    _prior_run(jobs)
    gates = target_name_gates("kafka", MAPPINGS, destination_config={"host": "broker"},
                              destination_table="customers", sync_mode="append",
                              destination_endpoint={"format": "kafka"})
    g23e = next(g for g in gates if g["id"] == "g23e_target_legacy_fields")
    assert g23e["status"] == "block"
    assert "'First_Name'" in g23e["message"] and "key_format='legacy'" in g23e["message"]
    refusal = request_refusal(_request())
    assert "'First_Name'" in refusal and "legacy_field_policy='write_new'" in refusal


def test_key_format_legacy_keeps_the_old_names(jobs):
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import adopt_rows_for_run, target_name_gates

    _prior_run(jobs)
    req = _request({"key_format": "legacy"})
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First_Name", "address_city", "id"]
    cfg = {"host": "broker", "key_format": "legacy"}
    rows = adopt_rows_for_run("kafka", MAPPINGS, [], "append",
                              destination_config=cfg, destination_table="customers")
    assert _targets(rows) == ["First_Name", "address_city", "id"]
    gates = target_name_gates("kafka", MAPPINGS, destination_config=cfg,
                              destination_table="customers", sync_mode="append")
    assert not [g for g in gates if g["status"] == "block"]


def test_write_new_is_recorded_on_the_run(jobs, monkeypatch):
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import LEGACY_FIELD_ACK, target_name_gates

    _prior_run(jobs)
    req = _request({"legacy_field_policy": "write_new"})
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First Name", "address.city", "id"]
    ack = req.destination.extra[LEGACY_FIELD_ACK]
    assert any("First_Name" in a and "First Name" in a for a in ack)
    gates = target_name_gates("kafka", MAPPINGS, destination_config={"host": "broker"},
                              destination_table="customers", sync_mode="append",
                              legacy_field_policy="write_new")
    assert next(g for g in gates if g["id"] == "g23e_target_legacy_fields")["status"] == "warn"

    # The writer records it in the run's warnings.
    from connectors import kafka_writer as kw

    class _Producer:
        def send(self, *a, **k):
            return type("A", (), {"get": lambda self, timeout=None: None})()

        def flush(self, timeout=None):
            pass

        def close(self, timeout=None):
            pass

    monkeypatch.setattr(kw, "_producer", lambda *a, **k: _Producer())
    result = kw.write_mapped_rows(
        host="broker", table_name="customers", headers=["fn"], data_rows=[["Ann"]],
        mappings=[{"source": "fn", "target": "First Name"}], column_types={"fn": "string"},
        write_mode="insert", legacy_field_ack=ack,
    )
    assert result.ok and any("First_Name" in w for w in result.warnings)


def test_new_topic_or_other_cluster_keeps_exact_names(jobs):
    from services.source_filter_gate import request_refusal

    _prior_run(jobs, topic="other_topic")
    req = _request()
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First Name", "address.city", "id"]
    jobs._jobs.clear()
    _prior_run(jobs, host="another-broker")
    req = _request()
    assert request_refusal(req) == ""


def test_history_unreadable_fails_closed(jobs):
    from services.source_filter_gate import request_refusal

    with patch.object(jobs, "destination_job_history", side_effect=RuntimeError("down")):
        refusal = request_refusal(_request())
    assert "Could not verify" in refusal and "down" in refusal
