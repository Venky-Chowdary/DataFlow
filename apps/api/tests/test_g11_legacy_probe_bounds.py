"""G11 re-review M1: the old-field probe is bounded and fails closed.

The MongoDB check ran ``find_one({old: {$exists: true}})`` per renamed target
with no time limit and no index: a full collection scan each, on every Execute,
schedule beat and CDC cycle, twice per stream on multi-stream routes (measured
1.09 s for 400k documents and 8 targets, linear in size). Redis walked the
whole keyspace when the prefix held fewer than 200 keys. A probe failure was
swallowed (``return ""``), so an unchecked store read as "no old fields".

Now: one ``$or`` query per collection with a projection, a document cap and
``max_time_ms``; a Redis SCAN budget; a timeout or failure is refused with a
clear "could not verify" message (or recorded when ``write_new`` is set); a
run asks each store once.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

MAPPINGS = [{"source": "a", "target": "address.city"}, {"source": "b", "target": "full name"},
            {"source": "c", "target": "id"}]


def _request(fmt="mongodb", extra=None, sync_mode="upsert"):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in MAPPINGS],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format=fmt, host="localhost",
                                   database="db", table="customers", extra=dict(extra or {})),
        sync_mode=sync_mode,
    )


class _Cursor:
    def __init__(self, docs, raises=None):
        self.docs, self.raises, self.calls = docs, raises, []

    def limit(self, n):
        self.calls.append(("limit", n))
        return self

    def max_time_ms(self, ms):
        self.calls.append(("max_time_ms", ms))
        return self

    def __iter__(self):
        if self.raises:
            raise self.raises
        return iter(self.docs)


def _mongo(cursor):
    coll = MagicMock()
    coll.find.return_value = cursor
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = coll
    return client, coll


def test_mongo_probe_is_one_bounded_query_per_collection():
    from services.target_name_legacy import mongo_fields_present

    cursor = _Cursor([{"_id": 1, "address_city": "x"}])
    client, coll = _mongo(cursor)
    with patch("connectors.mongodb_common._mongo_client", return_value=client):
        found = mongo_fields_present({"database": "db"}, "customers",
                                     ["address_city", "full_name"])
    assert found == {"address_city"}
    assert coll.find.call_count == 1 and coll.find_one.call_count == 0
    query, projection = coll.find.call_args.args
    assert query == {"$or": [{"address_city": {"$exists": True}},
                             {"full_name": {"$exists": True}}]}
    assert projection == {"address_city": 1, "full_name": 1}
    assert ("limit", 200) in cursor.calls and any(c[0] == "max_time_ms" for c in cursor.calls)


def test_mongo_probe_timeout_fails_closed_or_is_recorded():
    from pymongo.errors import ExecutionTimeout

    from services.source_filter_gate import request_refusal

    client, _coll = _mongo(_Cursor([], raises=ExecutionTimeout("operation exceeded time limit")))
    with patch("connectors.mongodb_common._mongo_client", return_value=client):
        refused = request_refusal(_request())
        acked = _request(extra={"legacy_field_policy": "write_new"})
        assert request_refusal(acked) == ""
        assert request_refusal(_request(sync_mode="full_refresh_overwrite")) == ""
    assert "Could not verify the destination's old field names" in refused, refused
    assert "within" in refused and "legacy_field_policy" in refused
    assert "not verified" in acked.destination.extra["legacy_field_ack"][0]


def test_unreachable_store_is_refused_not_ignored():
    from services.source_filter_gate import request_refusal

    with patch("connectors.mongodb_common._mongo_client", side_effect=OSError("no route")):
        refused = request_refusal(_request())
    assert "Could not verify" in refused and "no route" in refused


def test_redis_scan_is_budgeted():
    from services.target_name_legacy import LegacyFieldsUnverified, redis_fields_present

    client = MagicMock()
    client.scan.return_value = (17, [])  # a huge keyspace without this prefix
    with patch("connectors.redis_reader._redis_client", return_value=client), \
            pytest.raises(LegacyFieldsUnverified):
        redis_fields_present({}, "people", ["First_Name"])
    assert client.scan.call_count <= 50


def test_validate_reports_an_unchecked_store():
    from services.preflight_transfer_policy import run_transfer_policy_gates

    with patch("connectors.mongodb_common._mongo_client", side_effect=OSError("no route")):
        gates = {g["id"]: g for g in run_transfer_policy_gates(
            sync_mode="upsert", dest_type="mongodb", source_type="sqlite", source_kind="database",
            mappings=[dict(m) for m in MAPPINGS], destination_table="customers",
            destination_config={"database": "db"},
        )}
    gate = gates["g23e_target_legacy_fields"]
    assert gate["status"] == "block"
    assert gate["details"]["issues"][0]["code"] == "target_path_legacy_fields_unchecked"


def test_multi_stream_asks_each_store_once():
    import services.target_name_legacy as legacy

    calls: list[str] = []

    def present(cfg, table, names):
        calls.append(table)
        return set()

    request = _request()
    request.stream_contracts = [{"name": "s1", "selected": True}, {"name": "s2", "selected": True}]
    with patch.object(legacy, "mongo_fields_present", side_effect=present), \
            patch("src.transfer.adapters.resolve_connector_config",
                  return_value={"database": "db"}):
        legacy.begin_run(request.destination)
        assert legacy.streams_refusal(request) == ""
        # the stream loop, for each stream, on the run's own destination
        for name in ("s1", "s2"):
            from services.multi_stream_targets import bind_endpoint_to_stream

            bind_endpoint_to_stream(request.destination, name)
            legacy.prepare_stream_targets(request.destination, MAPPINGS, "upsert")
    assert sorted(calls) == ["s1", "s2"], calls
