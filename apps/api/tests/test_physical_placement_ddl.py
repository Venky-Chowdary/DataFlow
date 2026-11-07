"""Physical placement carry: partitioning / tablespace / clustering.

Measuring placement (``physical_storage_metadata``) only told the operator what
was lost. These tests pin the carry contract: what is reproduced, what is
refused with a reason, and that nothing is certified carried until the
destination catalog is read back.
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.physical_placement_ddl import (
    PlacementDecision,
    plan_physical_placement,
    verify_placement,
)

PG_PARTITIONED = {
    "status": "measured",
    "dialect": "postgresql",
    "tablespace": "pg_default",
    "is_default_tablespace": True,
    "partitioned": True,
    "partition_strategy": "range",
    "partition_keys": ["created"],
    "partition_count": 2,
    "partition_bounds": [
        {"name": "events_2023", "bound": "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')"},
        {"name": "events_2024", "bound": "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"},
    ],
    "clustering": [],
}


def _plan(**over):
    kwargs = {
        "source_storage": PG_PARTITIONED,
        "source_dialect": "postgresql",
        "dest_dialect": "postgresql",
        "dest_schema": "public",
        "dest_table": "events",
        "dest_columns": ["id", "created"],
        "primary_key": ["id", "created"],
        "unique_constraints": [],
        "dest_tablespaces": set(),
    }
    kwargs.update(over)
    return plan_physical_placement(**kwargs)


def _decision(plan, aspect: str) -> PlacementDecision:
    found = [d for d in plan.decisions if d.aspect == aspect]
    assert found, f"placement plan is silent about {aspect}"
    return found[0]


def test_postgres_range_partitioning_is_reproduced_with_its_bounds():
    plan = _plan()
    assert plan.create_suffix == 'PARTITION BY RANGE ("created")'
    assert len(plan.post_create_sql) == 2
    assert 'PARTITION OF "public"."events" FOR VALUES FROM' in plan.post_create_sql[0]
    assert _decision(plan, "partitioning").status == "planned"


def test_partition_key_missing_from_primary_key_is_refused():
    """PostgreSQL would reject the PK; losing the key to keep the scheme is worse."""
    plan = _plan(primary_key=["id"])
    decision = _decision(plan, "partitioning")
    assert decision.status == "unsupported"
    assert "PRIMARY KEY" in decision.reason
    assert plan.create_suffix == ""
    assert plan.post_create_sql == []


def test_partition_scheme_without_bounds_is_refused_not_half_emitted():
    storage = {**PG_PARTITIONED, "partition_bounds": []}
    plan = _plan(source_storage=storage)
    decision = _decision(plan, "partitioning")
    assert decision.status == "unsupported"
    assert "accepts no rows" in decision.reason
    assert plan.create_suffix == ""


def test_unmapped_partition_key_is_refused():
    plan = _plan(dest_columns=["id"])
    assert _decision(plan, "partitioning").status == "unsupported"
    assert "not mapped" in _decision(plan, "partitioning").reason


def test_cross_engine_partitioning_is_not_invented():
    plan = _plan(dest_dialect="mysql", dest_columns=["id", "created"])
    decision = _decision(plan, "partitioning")
    assert decision.status == "unsupported"
    assert "do not translate between engines" in decision.reason
    assert plan.create_suffix == ""


def test_mysql_range_partitioning_is_reproduced_inline():
    storage = {
        **PG_PARTITIONED,
        "dialect": "mysql",
        "partition_strategy": "range",
        "partition_bounds": [
            {"name": "p0", "bound": "2023"},
            {"name": "p1", "bound": "MAXVALUE"},
        ],
    }
    plan = _plan(
        source_storage=storage, source_dialect="mysql", dest_dialect="mysql"
    )
    assert plan.create_suffix == (
        "PARTITION BY RANGE (`created`) "
        "(PARTITION `p0` VALUES LESS THAN (2023), "
        "PARTITION `p1` VALUES LESS THAN (MAXVALUE))"
    )
    assert plan.post_create_sql == []


def test_bound_expression_outside_whitelist_is_refused():
    storage = {
        **PG_PARTITIONED,
        "partition_bounds": [
            {"name": "p0", "bound": "FOR VALUES FROM (1) TO (2); DROP TABLE users"}
        ],
    }
    plan = _plan(source_storage=storage)
    assert _decision(plan, "partitioning").status == "unsupported"
    assert plan.create_suffix == ""


def test_named_tablespace_is_carried_only_when_it_exists_on_destination():
    storage = {
        **PG_PARTITIONED,
        "partitioned": False,
        "partition_bounds": [],
        "tablespace": "fast_ssd",
        "is_default_tablespace": False,
    }
    present = _plan(source_storage=storage, dest_tablespaces={"pg_default", "fast_ssd"})
    assert present.create_suffix == 'TABLESPACE "fast_ssd"'
    assert _decision(present, "tablespace").status == "planned"

    absent = _plan(source_storage=storage, dest_tablespaces={"pg_default"})
    assert absent.create_suffix == ""
    assert _decision(absent, "tablespace").status == "unsupported"
    assert "does not exist on the destination" in _decision(absent, "tablespace").reason


def test_unreadable_destination_tablespace_catalog_is_unknown_not_missing():
    storage = {
        **PG_PARTITIONED,
        "partitioned": False,
        "partition_bounds": [],
        "tablespace": "fast_ssd",
        "is_default_tablespace": False,
    }
    plan = _plan(source_storage=storage, dest_tablespaces=None)
    decision = _decision(plan, "tablespace")
    assert decision.status == "unknown"
    assert "not readable" in decision.reason
    assert plan.create_suffix == ""


def test_unmeasured_source_placement_stays_unknown():
    plan = _plan(source_storage={"status": "unavailable", "detail": "no privilege"})
    for aspect in ("partitioning", "tablespace", "clustering"):
        decision = _decision(plan, aspect)
        assert decision.status == "unknown"
        assert "not absent" in decision.reason
    assert plan.create_suffix == ""


def test_planned_is_only_carried_after_the_destination_proves_it():
    planned = [
        PlacementDecision(aspect="partitioning", status="planned", reason="r"),
        PlacementDecision(aspect="tablespace", status="planned", reason="r"),
    ]
    source = {**PG_PARTITIONED, "tablespace": "fast_ssd"}
    proven = verify_placement(
        decisions=planned,
        source_storage=source,
        dest_storage={
            "status": "measured",
            "partitioned": True,
            "partition_keys": ["created"],
            "partition_count": 2,
            "tablespace": "fast_ssd",
        },
    )
    assert [d.status for d in proven] == ["carried", "carried"]

    ignored = verify_placement(
        decisions=planned,
        source_storage=source,
        dest_storage={
            "status": "measured",
            "partitioned": False,
            "partition_count": 0,
            "tablespace": "pg_default",
        },
    )
    assert [d.status for d in ignored] == ["unsupported", "unsupported"]
    assert "is not partitioned after CREATE" in ignored[0].reason


def test_unreadable_destination_after_create_is_unverified_not_carried():
    planned = [PlacementDecision(aspect="partitioning", status="planned", reason="r")]
    out = verify_placement(
        decisions=planned,
        source_storage=PG_PARTITIONED,
        dest_storage={"status": "unavailable", "detail": "no privilege"},
    )
    assert out[0].status == "unknown"
    assert "unverified" in out[0].reason


def test_child_partitions_are_named_after_the_destination_table():
    """Reusing source child names is a silent no-op in a shared schema.

    ``CREATE TABLE IF NOT EXISTS <source child> PARTITION OF <dest>`` matches
    the source's own child when both live in one database: the destination
    parent then holds no partition and accepts no row.
    """
    storage = {
        **PG_PARTITIONED,
        "table": "events_src",
        "partition_bounds": [
            {"name": "events_src_2023", "bound": "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')"},
            {"name": "events_src_2024", "bound": "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"},
        ],
    }
    plan = _plan(source_storage=storage, dest_table="events_dst")
    assert '"public"."events_dst_2023"' in plan.post_create_sql[0]
    assert '"public"."events_dst_2024"' in plan.post_create_sql[1]
    assert "events_src" not in " ".join(plan.post_create_sql).replace(
        '"public"."events_dst', ""
    )
