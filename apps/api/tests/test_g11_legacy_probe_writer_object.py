"""G11 re-review (unverified lead): the old-field probe reads the object the writer writes.

``legacy_fields_check`` took the collection from ``destination.table`` or
``destination.collection`` only and skipped the check when both were empty.
The writers do not: ``resolve_dest_table`` falls back to ``dt_<source>`` for
MongoDB / Redis / DynamoDB and reads an Elasticsearch index (or a DynamoDB
table) from ``database``. So a run whose destination named its object only
that way wrote ``full name`` beside the stale ``full_name`` with no refusal.

Now one helper (``writer_object_name``, built on ``resolve_dest_table`` with
the stream's own fallback) names the object for the probe, Validate and
Execute alike.
"""

from __future__ import annotations

import pytest

from unittest.mock import patch


@pytest.fixture(autouse=True)
def _written_before_g11(monkeypatch):
    """These collections / indexes / prefixes / tables were written by runs
    before the G11 upgrade, so the old-field check applies to them
    (``naming_history.object_history``; final review M-d)."""
    import services.naming_history as naming_history

    monkeypatch.setattr(naming_history, "object_history",
                        lambda *a, **k: naming_history.LEGACY)

MAPPINGS = [{"source": "a", "target": "full name"}, {"source": "b", "target": "id"}]


def _request(fmt, *, table="", collection="", database="db", source_table="orders"):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in MAPPINGS],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db",
                              table=source_table),
        destination=EndpointConfig(kind="database", format=fmt, host="localhost",
                                   database=database, table=table, collection=collection),
        sync_mode="append",
    )


def test_writer_object_name_matches_resolve_dest_table():
    from services.target_name_legacy import writer_object_name
    from src.transfer.adapters import resolve_dest_table

    for fmt, kw in (("mongodb", {}), ("redis", {}), ("dynamodb", {"database": "items"}),
                    ("elasticsearch", {"database": "logs"}), ("mongodb", {"table": "c1"})):
        req = _request(fmt, **kw)
        assert writer_object_name(req.destination, req.source) == resolve_dest_table(
            fmt, req.destination, "orders")
    assert writer_object_name(_request("mongodb").destination, _request("mongodb").source) == (
        "dt_orders")
    assert writer_object_name(_request("elasticsearch", database="logs").destination) == "logs"


def test_mongo_without_table_probes_the_fallback_collection():
    from services.target_name_legacy import legacy_fields_check

    seen = []

    def present(cfg, table, names):
        seen.append(table)
        return {"full_name"}

    with patch("services.target_name_legacy.mongo_fields_present", side_effect=present):
        refusal, _ack = legacy_fields_check(_request("mongodb"))
    assert seen == ["dt_orders"]
    assert "full_name" in refusal


def test_elasticsearch_index_from_database_is_probed():
    from services.target_name_legacy import legacy_fields_check

    seen = []

    def mapping(cfg, index):
        seen.append(index)
        return {"full_name", "id"}

    with patch("services.target_name_legacy.es_mapping_fields", side_effect=mapping):
        refusal, _ack = legacy_fields_check(_request("elasticsearch", database="logs"))
    assert seen == ["logs"]
    assert "full_name" in refusal


def test_validate_gate_uses_the_same_object():
    from services.preflight_transfer_policy import _legacy_probe_object

    req = _request("elasticsearch", database="logs")
    assert _legacy_probe_object("", req.destination, req.source) == "logs"
    req = _request("mongodb")
    assert _legacy_probe_object("", req.destination, req.source) == "dt_orders"
    assert _legacy_probe_object("given", req.destination, req.source) == "given"
    # Validate passes endpoints as dicts too.
    assert _legacy_probe_object(
        "", {"format": "elasticsearch", "database": "logs"}, {"table": "orders"}) == "logs"
