"""Physical placement (tablespace / filegroup / partitioning / clustering).

The certificate used to print "No partitioning on source" from a flag nothing
ever populated. Placement must now be *measured*; when it cannot be measured
the aspect is ``unknown``, never a silent "absent".
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.physical_storage_metadata import (
    PhysicalStorage,
    compare_physical_storage,
    probe_physical_storage,
)
from services.schema_fidelity import (
    build_catalog_from_introspect,
    plan_create_new_fidelity,
)


def _aspect(report: dict, aspect: str) -> dict:
    items = [i for i in report["items"] if i["aspect"] == aspect]
    assert items, f"certificate is silent about {aspect}"
    return items[0]


def _plan_report(storage: dict | None) -> dict:
    catalog = build_catalog_from_introspect(
        dialect="postgresql",
        columns=["id", "created"],
        column_types={"id": "INTEGER", "created": "DATE"},
        nullable={"id": False, "created": True},
        keys={"primary_key_columns": ["id"], "physical_storage": storage},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="postgresql",
        target_columns=["id", "created"],
        target_types=["BIGINT", "DATE"],
    )
    return plan.report.to_dict()


def test_unmeasured_placement_is_unknown_not_absent():
    report = _plan_report(None)
    for aspect in ("partitioning", "tablespace", "clustering"):
        item = _aspect(report, aspect)
        assert item["status"] == "unknown", item
        assert "not absent" in item["reason"] or "not proven absent" in item["reason"]
    assert report["unknown_count"] >= 3


def test_probe_unavailable_keeps_placement_unknown():
    unavailable = PhysicalStorage(
        dialect="postgresql",
        status="unavailable",
        detail="relation not visible for this role",
    )
    report = _plan_report(unavailable.to_dict())
    assert _aspect(report, "partitioning")["status"] == "unknown"
    assert "not visible" in _aspect(report, "partitioning")["reason"]


def test_measured_partitioning_is_certified_unsupported_with_detail():
    measured = PhysicalStorage(
        dialect="postgresql",
        status="measured",
        tablespace="fast_ssd",
        is_default_tablespace=False,
        partitioned=True,
        partition_strategy="range",
        partition_keys=["created"],
        partition_count=4,
        clustering=["id"],
    )
    report = _plan_report(measured.to_dict())
    part = _aspect(report, "partitioning")
    assert part["status"] == "unsupported"
    assert "range on created" in part["source_detail"]
    assert _aspect(report, "tablespace")["status"] == "unsupported"
    assert _aspect(report, "tablespace")["source_detail"] == "fast_ssd"
    assert _aspect(report, "clustering")["status"] == "unsupported"


def test_measured_plain_table_is_skipped_not_unknown():
    measured = PhysicalStorage(
        dialect="postgresql",
        status="measured",
        tablespace="pg_default",
        is_default_tablespace=True,
        partitioned=False,
        partition_keys=[],
        partition_count=0,
        clustering=[],
    )
    report = _plan_report(measured.to_dict())
    for aspect in ("partitioning", "tablespace", "clustering"):
        item = _aspect(report, aspect)
        assert item["status"] == "skipped", item
        assert "measured" in item["reason"]


def test_compare_refuses_carry_claim_when_a_side_is_unmeasured():
    measured = PhysicalStorage(dialect="postgresql", status="measured", partitioned=False)
    blind = PhysicalStorage(dialect="postgresql", status="unavailable")
    result = compare_physical_storage(measured, blind)
    assert result.carried is None
    assert result.status == "unavailable"
    assert compare_physical_storage(measured, measured).carried is True


def test_unknown_dialect_reports_unavailable_not_unpartitioned():
    result = probe_physical_storage("cassandra", object(), "ks", "t")
    assert result.status == "unavailable"
    assert result.partitioned is None


# --------------------------------------------------------------------------
# live PostgreSQL
# --------------------------------------------------------------------------

_PG = {
    "host": os.environ.get("P6_PG_HOST", "127.0.0.1"),
    "port": int(os.environ.get("P6_PG_PORT", "5432")),
    "database": os.environ.get("P6_PG_DB", "postgres"),
    "username": os.environ.get("P6_PG_USER", "postgres"),
    "password": os.environ.get("P6_PG_PASSWORD", "admin"),
}


def _pg_up() -> bool:
    try:
        with socket.create_connection((_PG["host"], _PG["port"]), timeout=0.4):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_up(), reason="live PostgreSQL not reachable")
def test_live_postgres_partitioning_is_measured_from_the_catalog():
    psycopg2 = pytest.importorskip("psycopg2")

    conn = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], dbname=_PG["database"],
        user=_PG["username"], password=_PG["password"], connect_timeout=5,
    )
    conn.autocommit = True
    sfx = uuid.uuid4().hex[:6]
    part = f"psm_part_{sfx}"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{part}" (id INTEGER, created DATE) '
                "PARTITION BY RANGE (created)"
            )
            cur.execute(
                f'CREATE TABLE public."{part}_a" PARTITION OF public."{part}" '
                "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"
            )
            measured = probe_physical_storage("postgresql", cur, "public", part)
            missing = probe_physical_storage("postgresql", cur, "public", f"absent_{sfx}")

        assert measured.status == "measured"
        assert measured.partitioned is True
        assert measured.partition_strategy == "range"
        assert measured.partition_keys == ["created"]

        # An invisible relation must never be certified as "not partitioned".
        assert missing.status == "unavailable"
        assert missing.partitioned is None

        report = _plan_report(measured.to_dict())
        assert _aspect(report, "partitioning")["status"] == "unsupported"
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{part}" CASCADE')
        conn.close()
