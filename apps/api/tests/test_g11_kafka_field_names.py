"""G11 re-review M3: Kafka topics keep the field names earlier runs gave them.

N-F1 made the Kafka writer produce each Map target exactly as mapped
(``First Name``, ``address.city``) where it used to sanitize them
(``First_Name``, ``address_city``). On an existing topic that silently changed
the event shape consumers read (and a composite message key) with no Validate
signal; with a Schema Registry subject holding the old names the write failed
with a misleading "physical DDL missing for First Name, address.city".

Now: a registry subject's fields are the topic's existing columns and the old
spelling is adopted (as for SaaS fields), at Validate and Execute. Without a
registry, a topic with earlier runs and no ``key_format`` keeps the old names
by default (``key_format=legacy``, the Redis precedent): an existing or
scheduled job does not change shape or break on upgrade. The run records that
default and Validate names the opt-in (``key_format=exact``). ``key_format=exact``
or ``legacy_field_policy=write_new`` switches to the exact names on purpose,
recorded on the run. A new topic gets the exact names.
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


class _Producer:
    def send(self, *a, **k):
        return type("A", (), {"get": lambda self, timeout=None: None})()

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        pass


def _write(monkeypatch, mappings, ack):
    from connectors import kafka_writer as kw

    monkeypatch.setattr(kw, "_producer", lambda *a, **k: _Producer())
    return kw.write_mapped_rows(
        host="broker", table_name="customers", headers=["fn"], data_rows=[["Ann"]],
        mappings=mappings, column_types={"fn": "string"}, write_mode="insert",
        legacy_field_ack=ack,
    )


def test_scheduled_job_on_a_used_topic_keeps_the_old_names(jobs, monkeypatch):
    """The upgrade default: no key_format, earlier runs -> key_format=legacy."""
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import LEGACY_FIELD_ACK

    _prior_run(jobs)
    req = _request()
    req.triggered_by = "schedule:nightly"
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First_Name", "address_city", "id"]
    ack = req.destination.extra[LEGACY_FIELD_ACK]
    assert any("key_format=legacy by default" in a and "key_format=exact" in a for a in ack)
    result = _write(monkeypatch, [{"source": "fn", "target": "First_Name"}], ack)
    assert result.ok and any("key_format=legacy by default" in w for w in result.warnings)
    assert result.meta["reconcile_sample"] == [{"First_Name": "Ann"}]


def test_validate_names_the_exact_option_on_a_used_topic(jobs):
    from services.target_name_legacy import adopt_rows_for_run, target_name_gates

    _prior_run(jobs)
    cfg = {"host": "broker"}
    rows = adopt_rows_for_run("kafka", MAPPINGS, [], "append",
                              destination_config=cfg, destination_table="customers")
    assert _targets(rows) == ["First_Name", "address_city", "id"]
    gates = target_name_gates("kafka", MAPPINGS, destination_config=cfg,
                              destination_table="customers", sync_mode="append",
                              destination_endpoint={"format": "kafka"})
    assert not [g for g in gates if g["status"] in ("block", "warn")], gates
    note = next(g for g in gates if g["id"] == "g23d_existing_target_columns")
    assert note.get("severity") == "info"
    assert "set key_format=exact to send the mapped names exactly" in note["message"]
    assert "'First_Name'" in note["message"]


def test_new_topic_gets_exact_names(jobs):
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import LEGACY_FIELD_ACK, target_name_gates

    _prior_run(jobs, topic="other_topic")  # another topic's history
    req = _request()
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First Name", "address.city", "id"]
    assert not (req.destination.extra or {}).get(LEGACY_FIELD_ACK)
    gates = target_name_gates("kafka", MAPPINGS, destination_config={"host": "broker"},
                              destination_table="customers", sync_mode="append")
    assert not [g for g in gates if g["id"].startswith(("g23d", "g23e"))], gates
    jobs._jobs.clear()
    _prior_run(jobs, host="another-broker")  # same topic name, another cluster
    req = _request()
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First Name", "address.city", "id"]


@pytest.mark.parametrize("extra", [{"key_format": "exact"},
                                   {"legacy_field_policy": "write_new"}],
                         ids=["key_format_exact", "write_new"])
def test_exact_names_on_a_used_topic_are_chosen_and_recorded(jobs, monkeypatch, extra):
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import LEGACY_FIELD_ACK, target_name_gates

    _prior_run(jobs)
    req = _request(extra)
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First Name", "address.city", "id"]
    ack = req.destination.extra[LEGACY_FIELD_ACK]
    assert any("First_Name" in a and "First Name" in a for a in ack), ack
    gates = target_name_gates(
        "kafka", MAPPINGS, destination_config={"host": "broker", **extra},
        destination_table="customers", sync_mode="append",
        legacy_field_policy=extra.get("legacy_field_policy", ""))
    g23e = next(g for g in gates if g["id"] == "g23e_target_legacy_fields")
    assert g23e["status"] == "warn" and "'First_Name'" in g23e["message"]
    result = _write(monkeypatch, [{"source": "fn", "target": "First Name"}], ack)
    assert result.ok and any("First_Name" in w for w in result.warnings)


def test_key_format_legacy_keeps_the_old_names_without_history(jobs):
    from services.source_filter_gate import request_refusal

    req = _request({"key_format": "legacy"})
    assert request_refusal(req) == ""
    assert _targets(req.mappings) == ["First_Name", "address_city", "id"]


def test_history_unreadable_fails_closed_unless_key_format_is_set(jobs):
    from services.source_filter_gate import request_refusal

    with patch.object(jobs, "destination_job_history", side_effect=RuntimeError("down")):
        refusal = request_refusal(_request())
        assert "down" in refusal and "key_format" in refusal
        assert request_refusal(_request({"key_format": "exact"})) == ""
