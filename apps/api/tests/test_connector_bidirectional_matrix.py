"""Bidirectional route + sample-attach contract for Transfer Studio Validate.

Proves:
1. Core DB/warehouse/NoSQL pairs are live in both directions (validate_transfer).
2. Introspect sample helpers always populate ``data``/``sample_data`` so dry-run
   cannot block on schema-only metadata (the Snowflake→Mongo regression).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.transfer.endpoint_intelligence import (
    _attach_batch_sample_rows,
    _attach_db_sample,
    _attach_sql_sample_rows,
)
from src.transfer.models import EndpointConfig
from src.transfer.registry import validate_transfer

# User-facing "to and fro" engines — must all be live both ways.
CORE_ENGINES = (
    "postgresql",
    "mysql",
    "mongodb",
    "snowflake",
    "bigquery",
    "dynamodb",
    "sqlite",
    "sqlserver",
    "oracle",
    "s3",
    "redshift",
)


@pytest.mark.parametrize("src", CORE_ENGINES)
@pytest.mark.parametrize("dst", CORE_ENGINES)
def test_core_engines_bidirectional_live(src: str, dst: str) -> None:
    ok, msg = validate_transfer("database", src, "database", dst)
    assert ok, f"{src} → {dst}: {msg}"


def test_batch_sample_attach_populates_data() -> None:
    batch = SimpleNamespace(
        headers=["a", "b"],
        rows=[["1", "x"], ["2", "y"], ["3", "z"]],
    )
    out: dict = {"message": "ok"}
    _attach_batch_sample_rows(out, batch, preview=2)
    assert out["data"] == [{"a": "1", "b": "x"}, {"a": "2", "b": "y"}]
    assert out["sample_data"] == out["data"]


@pytest.mark.parametrize(
    "fmt",
    ["snowflake", "postgresql", "mysql", "bigquery", "sqlite", "sqlserver", "oracle", "redshift"],
)
def test_sql_warehouse_sample_attach_for_dry_run(fmt: str) -> None:
    out: dict = {"columns": ["id"], "schema": {"id": "string"}, "message": "schema"}
    ep = EndpointConfig(kind="database", format=fmt, table="t1", database="db")
    records = [{"id": "1"}, {"id": "2"}]
    with patch(
        "src.transfer.adapters.read_source_database",
        return_value=(records, ["id"], {"id": "string"}),
    ):
        _attach_sql_sample_rows(out, ep, {"type": fmt}, fmt, "t1", 50)
    assert len(out["data"]) == 2
    assert out["sample_data"][0]["id"] == "1"


def test_dynamodb_attach_loads_sample_not_schema_only() -> None:
    out: dict = {"message": "DynamoDB connected"}
    ep = EndpointConfig(kind="database", format="dynamodb", database="orders", table="orders")
    cfg = {"type": "dynamodb", "database": "orders", "host": "", "port": 0}

    batch = SimpleNamespace(
        headers=["pk", "sk"],
        rows=[["p1", "s1"], ["p2", "s2"]],
        total_rows=2,
    )

    with (
        patch(
            "connectors.dynamodb_reader.describe_table_schema",
            return_value=(["pk", "sk"], {"pk": "string", "sk": "string"}),
        ),
        patch("connectors.dynamodb_reader.estimate_item_count", return_value=100),
        patch("connectors.dynamodb_reader.read_all_paginated", return_value=batch),
        patch(
            "src.transfer.endpoint_intelligence.resolve_connector_config",
            return_value=cfg,
        ),
    ):
        _attach_db_sample(out, ep, sample_limit=10)

    assert out["columns"] == ["pk", "sk"]
    assert len(out["data"]) == 2
    assert out["sample_data"][0]["pk"] == "p1"


def test_redis_s3_elasticsearch_attach_sample_rows() -> None:
    batch = SimpleNamespace(
        headers=["key", "value"],
        rows=[["k1", "v1"]],
        total_rows=1,
    )
    for fmt, patch_target in (
        ("redis", "connectors.redis_reader.read_keys_batch"),
        ("elasticsearch", "connectors.elasticsearch_reader.read_index_batch"),
    ):
        out: dict = {"message": f"{fmt} ok"}
        ep = EndpointConfig(
            kind="database",
            format=fmt,
            database="idx" if fmt == "elasticsearch" else "0",
            table="*",
        )
        cfg = {"type": fmt, "database": ep.database, "host": "h", "port": 0}
        with (
            patch(patch_target, return_value=batch),
            patch(
                "src.transfer.endpoint_intelligence.resolve_connector_config",
                return_value=cfg,
            ),
        ):
            _attach_db_sample(out, ep, sample_limit=5)
        assert out.get("data"), f"{fmt} must attach sample rows"
        assert out["data"][0]["key"] == "k1"


def test_warehouse_to_mongo_and_mongo_to_warehouse_routes() -> None:
    pairs = [
        ("snowflake", "mongodb"),
        ("mongodb", "snowflake"),
        ("postgresql", "snowflake"),
        ("snowflake", "postgresql"),
        ("mongodb", "dynamodb"),
        ("dynamodb", "mongodb"),
        ("dynamodb", "snowflake"),
        ("snowflake", "dynamodb"),
        ("mysql", "mongodb"),
        ("bigquery", "mongodb"),
    ]
    for src, dst in pairs:
        ok, msg = validate_transfer("database", src, "database", dst)
        assert ok, f"{src}→{dst}: {msg}"
