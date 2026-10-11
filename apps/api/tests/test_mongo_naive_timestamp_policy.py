"""A zoneless timestamp reaching MongoDB's instant carrier needs a decision.

MongoDB has exactly one temporal type: BSON date, a UTC instant. Every other
destination in the matrix offers a zoneless carrier for a zoneless source
(Snowflake TIMESTAMP_NTZ, BigQuery DATETIME, MySQL DATETIME(6)), so PostgreSQL
``timestamp without time zone`` lands unchanged. MongoDB has nothing to land it
on, which makes the pair the one case ``resolve_timezone_policy`` calls
POLICY_UTC_INVENT: the instant is stamped, not carried, and it requires an
operator contract.

Both halves matter. Without a contract the run must refuse rather than pick a
zone quietly. With one it must actually proceed — the writer used to refuse
unconditionally, so a signed contract bought nothing and the route was dead.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from services.timezone_policy import POLICY_UTC_INVENT, resolve_timezone_policy
from services.type_system import (
    document_instant_wire_preserved,
    is_lossy_coercion,
    is_precision_collapse_coercion,
)


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def test_bson_date_keeps_the_time_of_day_for_an_offset_source():
    """The carrier is an instant; the SQL DATE spelling is a name collision."""
    for source in ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"):
        assert document_instant_wire_preserved(
            source, "date", dest_db="mongodb"
        ) is True
        assert is_lossy_coercion(source, "date", dest_db="mongodb") is False
        assert is_precision_collapse_coercion(source, "date", dest_db="mongodb") is False


def test_sub_millisecond_precision_is_not_preserved():
    """BSON date counts milliseconds — microseconds are truncated on write."""
    assert document_instant_wire_preserved(
        "TIMESTAMPTZ(6)", "date", dest_db="mongodb"
    ) is False
    assert document_instant_wire_preserved(
        "TIMESTAMPTZ(3)", "date", dest_db="mongodb"
    ) is True


def test_zoneless_source_is_a_stamped_instant_not_a_carried_one():
    assert document_instant_wire_preserved(
        "TIMESTAMP", "date", dest_db="mongodb"
    ) is False
    assert is_lossy_coercion("TIMESTAMP", "date", dest_db="mongodb") is True

    policy = resolve_timezone_policy("TIMESTAMP", "TIMESTAMPTZ", dest_db="mongodb")
    assert policy is not None
    assert policy.policy == POLICY_UTC_INVENT
    assert policy.requires_contract is True
    assert policy.instant_preserved is False


def _capturing_client(captured: dict):
    class _Result:
        inserted_ids = [1]

    class _Coll:
        def insert_many(self, docs, ordered=False):
            captured["docs"] = list(docs)
            return _Result()

    class _DB:
        def list_collection_names(self, **_kwargs):
            return []

        def __getitem__(self, _name):
            return _Coll()

    class _Client:
        def __getitem__(self, _name):
            return _DB()

    return _Client()


def _write_one_temporal(monkeypatch, *, source_type: str, acknowledged: bool = False):
    from connectors.mongodb_writer import write_mapped_rows

    captured: dict = {}
    monkeypatch.setattr(
        "connectors.mongodb_common._mongo_client",
        lambda *_args, **_kwargs: _capturing_client(captured),
    )
    mapping = {"source": "created_at", "target": "created_at", "source_type": source_type}
    if acknowledged:
        mapping["risk_acknowledged"] = True
    result = write_mapped_rows(
        host="localhost",
        port=27017,
        database="dataflow_test",
        username="",
        password="",
        connection_string="",
        ssl=False,
        schema="dataflow_test",
        table_name="tz_policy",
        headers=["created_at"],
        data_rows=[["2024-03-01 12:00:00"]],
        mappings=[mapping],
        column_types={"created_at": source_type},
        error_policy="fail",
    )
    return result, captured


def test_timestamptz_naive_wire_keeps_the_clock_as_utc(monkeypatch):
    """Driver-stripped tzinfo on a declared instant is UTC, not a refused wall clock."""
    from datetime import timezone

    result, captured = _write_one_temporal(monkeypatch, source_type="TIMESTAMPTZ")
    assert result.ok is True, result.error
    stamped = captured["docs"][0]["created_at"]
    assert stamped.tzinfo is not None
    utc = stamped.astimezone(timezone.utc)
    assert utc.hour == 12
    assert utc.minute == 0


def test_schema_timestamptz_without_a_mapping_stamp_keeps_the_clock(monkeypatch):
    """Live introspect puts TIMESTAMPTZ on the source schema, not always on the mapping."""
    from connectors.mongodb_writer import write_mapped_rows
    from datetime import timezone

    captured: dict = {}
    monkeypatch.setattr(
        "connectors.mongodb_common._mongo_client",
        lambda *_args, **_kwargs: _capturing_client(captured),
    )
    result = write_mapped_rows(
        host="localhost",
        port=27017,
        database="dataflow_test",
        username="",
        password="",
        connection_string="",
        ssl=False,
        schema="dataflow_test",
        table_name="tz_schema",
        headers=["created_at"],
        data_rows=[["2024-03-01 12:00:00"]],
        mappings=[{"source": "created_at", "target": "created_at"}],
        column_types={"created_at": "TIMESTAMPTZ"},
        error_policy="fail",
    )
    assert result.ok is True, result.error
    utc = captured["docs"][0]["created_at"].astimezone(timezone.utc)
    assert utc.hour == 12


def test_declared_target_date_is_calendar_day_for_text_source(monkeypatch):
    from datetime import datetime, timezone

    from connectors.mongodb_writer import write_mapped_rows

    captured: dict = {}
    monkeypatch.setattr(
        "connectors.mongodb_common._mongo_client",
        lambda *_args, **_kwargs: _capturing_client(captured),
    )
    result = write_mapped_rows(
        host="localhost",
        port=27017,
        database="dataflow_test",
        username="",
        password="",
        connection_string="",
        ssl=False,
        schema="dataflow_test",
        table_name="date_target",
        headers=["dob"],
        data_rows=[["03/15/1985"]],
        mappings=[
            {
                "source": "dob",
                "target": "date_of_birth",
                "target_type": "DATE",
            }
        ],
        column_types={"dob": "VARCHAR"},
        error_policy="fail",
    )

    assert result.ok is True, result.error
    assert captured["docs"][0]["date_of_birth"] == datetime(
        1985, 3, 15, tzinfo=timezone.utc
    )


def test_timestamp_ntz_naive_wire_still_needs_a_contract(monkeypatch):
    result, captured = _write_one_temporal(monkeypatch, source_type="TIMESTAMP_NTZ")
    assert result.ok is False
    assert "naive wall-clock" in (result.error or "")
    assert "docs" not in captured


def test_sql_date_target_is_untouched_by_the_document_rule():
    """A real calendar DATE still drops the time of day."""
    assert document_instant_wire_preserved(
        "TIMESTAMP", "DATE", dest_db="postgresql"
    ) is False


def test_mongodb_bson_date_reader_serializes_naive_driver_value_as_utc():
    from datetime import datetime

    from connectors.mongodb_reader import _serialize

    assert _serialize(datetime(2025, 1, 1, 0, 0)) == "2025-01-01T00:00:00+00:00"
    assert is_lossy_coercion("TIMESTAMP", "DATE", dest_db="postgresql") is True


@pytest.mark.skipif(
    not _reachable("localhost", 27017), reason="MongoDB not reachable"
)
def test_writer_refuses_zoneless_without_a_contract_and_accepts_it_with_one():
    from connectors.mongodb_writer import write_mapped_rows

    def _run(*, acknowledged: bool):
        collection = "tznaive_" + uuid.uuid4().hex[:8]
        mapping = {"source": "created_at", "target": "created_at"}
        if acknowledged:
            mapping["risk_acknowledged"] = True
        return collection, write_mapped_rows(
            host="localhost",
            port=27017,
            database="dataflow_test",
            username="",
            password="",
            connection_string="",
            ssl=False,
            schema="dataflow_test",
            table_name=collection,
            headers=["created_at"],
            data_rows=[["2024-01-05 10:30:00"]],
            mappings=[mapping],
            column_types={"created_at": "TIMESTAMP"},
            error_policy="fail",
        )

    _c1, refused = _run(acknowledged=False)
    assert refused.ok is False
    assert "naive wall-clock" in (refused.error or "")

    collection, accepted = _run(acknowledged=True)
    try:
        assert accepted.ok is True, accepted.error
        assert accepted.rows_written == 1

        from pymongo import MongoClient

        client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
        try:
            doc = client["dataflow_test"][collection].find_one()
            assert doc is not None
            # The wall clock is what a zoneless source has; stamping UTC must not
            # move it. A shifted hour here is the silent instant shift the whole
            # policy exists to prevent.
            assert doc["created_at"].hour == 10
            assert doc["created_at"].minute == 30
        finally:
            client["dataflow_test"][collection].drop()
            client.close()
    finally:
        pass
