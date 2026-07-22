"""Canonical sample values for universal logical types.

Used by the bind/wire matrix and typed e2e expansions. One source of truth —
do not invent per-test ISO-Z / DECIMAL / NULL fixtures.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

from services.type_system import (
    LOGICAL_ARRAY,
    LOGICAL_BINARY,
    LOGICAL_BOOLEAN,
    LOGICAL_DATE,
    LOGICAL_DATETIME,
    LOGICAL_DECIMAL,
    LOGICAL_FLOAT,
    LOGICAL_GEOGRAPHY,
    LOGICAL_INTEGER,
    LOGICAL_INTERVAL,
    LOGICAL_JSON,
    LOGICAL_STRING,
    LOGICAL_TEXT,
    LOGICAL_TIME,
    LOGICAL_UUID,
    LOGICAL_VECTOR,
)

ALL_LOGICALS: tuple[str, ...] = (
    LOGICAL_STRING,
    LOGICAL_TEXT,
    LOGICAL_INTEGER,
    LOGICAL_DECIMAL,
    LOGICAL_FLOAT,
    LOGICAL_BOOLEAN,
    LOGICAL_DATE,
    LOGICAL_DATETIME,
    LOGICAL_TIME,
    LOGICAL_UUID,
    LOGICAL_JSON,
    LOGICAL_ARRAY,
    LOGICAL_BINARY,
    LOGICAL_INTERVAL,
    LOGICAL_GEOGRAPHY,
    LOGICAL_VECTOR,
)

# Live Transfer Studio failure class: PG timestamptz → transform ISO-Z → MySQL DATETIME.
ISO_Z_DATETIME = "2026-07-04T06:57:37Z"
ISO_Z_DATETIME_MS = "2024-12-31T23:59:59.123456Z"
ISO_OFFSET_DATETIME = "2024-08-09T03:58:42+02:00"

# Per-logical wire samples: transform-engine shapes operators actually see.
SAMPLES: dict[str, list[Any]] = {
    LOGICAL_STRING: ["hello", ""],
    LOGICAL_TEXT: ["line1\nline2", ""],
    LOGICAL_INTEGER: [42, "42", -7],
    LOGICAL_DECIMAL: [Decimal("10.5000"), "10.5000", Decimal("0.0001")],
    LOGICAL_FLOAT: [1500.0, "1.5e3", 2.5e-2],
    LOGICAL_BOOLEAN: [True, False, "true", 1, 0],
    LOGICAL_DATE: ["2024-12-31", "2024-12-31T23:59:59Z", date(2024, 12, 31)],
    LOGICAL_DATETIME: [
        ISO_Z_DATETIME,
        ISO_Z_DATETIME_MS,
        ISO_OFFSET_DATETIME,
        "2024-12-31 23:59:59",
        datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    ],
    LOGICAL_TIME: ["23:59:59", "23:59:59.123456", time(23, 59, 59)],
    LOGICAL_UUID: ["550e8400-e29b-41d4-a716-446655440000"],
    LOGICAL_JSON: ['{"a":1,"b":null}', {"a": 1}],
    LOGICAL_ARRAY: ["[1,2,3]", [1, 2, 3]],
    LOGICAL_BINARY: [b"\x00\x01\x02", "AAEC"],  # base64 AAEC == \x00\x01\x02
    LOGICAL_INTERVAL: ["P1DT2H", "1 day 02:00:00"],
    LOGICAL_GEOGRAPHY: ["POINT(0 0)", "POINT(1 2)"],
    LOGICAL_VECTOR: ["[0.1,0.2,0.3]", "[1.0,0.0]"],
}

# Top transfer-ready / production-class destinations for bind proof.
# (catalog_label, ddl_key in type_system.DDL_TYPES). Hosted twins share ddl_key.
TOP_CONNECTOR_DESTS: tuple[tuple[str, str], ...] = (
    ("postgresql", "postgresql"),
    ("postgresql_rds", "postgresql"),
    ("postgresql_supabase", "postgresql"),
    ("mysql", "mysql"),
    ("mysql_rds", "mysql"),
    ("mariadb", "mysql"),
    ("sqlserver", "sqlserver"),
    ("oracle", "oracle"),
    ("snowflake", "snowflake"),
    ("snowflake_aws", "snowflake"),
    ("bigquery", "bigquery"),
    ("google_bigquery", "bigquery"),
    ("redshift", "redshift"),
    ("amazon_redshift", "redshift"),
    ("mongodb", "mongodb"),
    ("mongodb_atlas", "mongodb"),
    ("dynamodb", "dynamodb"),
    ("amazon_dynamodb", "dynamodb"),
    ("sqlite", "sqlite"),
    ("generic_sql", "generic_sql"),
    ("duckdb", "duckdb"),
    ("databricks", "databricks"),
    ("iceberg", "iceberg"),
    ("redis", "redis"),
    ("elasticsearch", "elasticsearch"),
    ("opensearch", "elasticsearch"),
    ("clickhouse", "clickhouse"),
    ("trino", "trino"),
    ("presto", "presto"),
    ("synapse", "sqlserver"),  # Synapse uses SQL Server type family
)

assert len(TOP_CONNECTOR_DESTS) >= 30, len(TOP_CONNECTOR_DESTS)
