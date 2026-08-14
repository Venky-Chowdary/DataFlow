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


def test_sql_date_target_is_untouched_by_the_document_rule():
    """A real calendar DATE still drops the time of day."""
    assert document_instant_wire_preserved(
        "TIMESTAMP", "DATE", dest_db="postgresql"
    ) is False
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
