"""Cross-schema and cross-type accuracy tests for all transfer-live drivers."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from connectors.writer_common import build_mapped_rows  # noqa: E402
from services.mapping_pipeline import run_mapping_pipeline  # noqa: E402
from services.transform_engine import apply_transform, infer_transform_for_mapping  # noqa: E402
from services.type_system import ddl_type, is_lossy_coercion, normalize_logical_type  # noqa: E402
from transfer.connector_capabilities import _DRIVER_CAPS, _FILE_CAPS, get_capabilities, transfer_ready  # noqa: E402
from transfer.registry import LIVE_DEST_DATABASES, LIVE_SOURCE_DATABASES, LIVE_SOURCE_FORMATS, validate_transfer  # noqa: E402

ALL_DB_DRIVERS = sorted(_DRIVER_CAPS.keys())
ALL_FILE_FORMATS = sorted(k for k, v in _FILE_CAPS.items() if transfer_ready(v))
ALL_SOURCES = sorted(set(LIVE_SOURCE_DATABASES))
ALL_DESTINATIONS = sorted(set(LIVE_DEST_DATABASES))


@pytest.mark.parametrize("driver", ALL_DB_DRIVERS)
def test_every_db_driver_has_probe_read_write(driver: str):
    caps = get_capabilities(driver)
    assert caps.get("test"), driver
    if caps.get("source_only"):
        assert caps.get("read"), driver
    else:
        assert transfer_ready(caps), driver

    probes = {
        "postgresql": ("connectors.postgresql", "test_postgresql"),
        "mysql": ("connectors.mysql", "test_mysql"),
        "snowflake": ("connectors.snowflake", "test_snowflake"),
        "bigquery": ("connectors.bigquery", "test_bigquery"),
        "redshift": ("connectors.redshift", "test_redshift"),
        "dynamodb": ("connectors.dynamodb", "test_dynamodb"),
        "s3": ("connectors.s3", "test_s3"),
        "gcs": ("connectors.gcs", "test_gcs"),
        "adls": ("connectors.adls", "test_adls"),
        "redis": ("connectors.redis_kv", "test_redis"),
        "elasticsearch": ("connectors.elasticsearch", "test_elasticsearch"),
        "sqlite": ("connectors.sqlite", "test_sqlite"),
        "sftp": ("connectors.sftp", "test_sftp"),
        "email": ("connectors.email", "test_email"),
        "salesforce": ("connectors.salesforce", "test_salesforce"),
        "hubspot": ("connectors.hubspot", "test_hubspot"),
        "stripe": ("connectors.stripe", "test_stripe"),
        "rest_api": ("connectors.rest_api", "test_connection"),
        "influxdb": ("connectors.influxdb", "test_connection"),
        "neo4j": ("connectors.neo4j", "test_connection"),
        "couchbase": ("connectors.couchbase", "test_connection"),
    }
    if driver == "mongodb":
        import pymongo  # noqa: F401
    else:
        mod_name, fn_name = probes[driver]
        assert callable(getattr(importlib.import_module(mod_name), fn_name))

    readers = {
        "postgresql": "connectors.postgresql_reader",
        "mysql": "connectors.mysql_reader",
        "mongodb": "connectors.mongodb_reader",
        "snowflake": "connectors.snowflake_reader",
        "bigquery": "connectors.bigquery_reader",
        "redshift": "connectors.postgresql_reader",
        "dynamodb": "connectors.dynamodb_reader",
        "s3": "connectors.s3_reader",
        "gcs": "connectors.gcs_reader",
        "adls": "connectors.adls_reader",
        "redis": "connectors.redis_reader",
        "elasticsearch": "connectors.elasticsearch_reader",
        "sqlite": "connectors.sqlite_reader",
        "sftp": "connectors.sftp_reader",
        "salesforce": "connectors.salesforce",
        "hubspot": "connectors.hubspot",
        "stripe": "connectors.stripe",
        "rest_api": "connectors.rest_api",
        "influxdb": "connectors.influxdb",
        "neo4j": "connectors.neo4j",
        "couchbase": "connectors.couchbase",
    }
    writers = {
        "postgresql": "connectors.postgresql_writer",
        "mysql": "connectors.mysql_writer",
        "mongodb": "connectors.mongodb_writer",
        "snowflake": "connectors.snowflake_writer",
        "bigquery": "connectors.bigquery_writer",
        "redshift": "connectors.postgresql_writer",
        "dynamodb": "connectors.dynamodb_writer",
        "s3": "connectors.s3_writer",
        "gcs": "connectors.gcs_writer",
        "adls": "connectors.adls_writer",
        "redis": "connectors.redis_writer",
        "elasticsearch": "connectors.elasticsearch_writer",
        "sqlite": "connectors.sqlite_writer",
        "sftp": "connectors.sftp_writer",
        "email": "connectors.email",
        "salesforce": "connectors.saas_common",
        "hubspot": "connectors.saas_common",
        "stripe": "connectors.saas_common",
        "rest_api": "connectors.saas_common",
        "influxdb": "connectors.saas_common",
        "neo4j": "connectors.saas_common",
        "couchbase": "connectors.saas_common",
    }
    if readers.get(driver):
        assert importlib.import_module(readers[driver])
    writer_mod = importlib.import_module(writers[driver])
    if caps.get("source_only"):
        assert callable(getattr(writer_mod, "write_not_supported"))
    else:
        assert callable(writer_mod.write_mapped_rows)


@pytest.mark.parametrize("src_fmt", ALL_FILE_FORMATS)
@pytest.mark.parametrize("dest", ALL_DESTINATIONS)
def test_file_to_db_route_live(src_fmt: str, dest: str):
    ok, msg = validate_transfer("file", src_fmt, "database", dest)
    assert ok, msg


@pytest.mark.parametrize("src", ALL_SOURCES)
@pytest.mark.parametrize("dest", ALL_DESTINATIONS)
def test_db_to_db_route_live(src: str, dest: str):
    ok, msg = validate_transfer("database", src, "database", dest)
    assert ok, msg


@pytest.mark.parametrize(
    "source_type,target_type,raw,transform,expected",
    [
        ("VARCHAR", "INTEGER", "42", "integer", 42),
        ("VARCHAR", "DECIMAL", "$1,234.50", "decimal", "1234.50"),
        ("VARCHAR", "BOOLEAN", "yes", "boolean", True),
        ("VARCHAR", "DATE", "2024-06-01", "date", "2024-06-01"),
        ("TIMESTAMP", "DATETIME", "2024-06-01T12:00:00Z", "datetime", "2024-06-01T12:00:00Z"),
        ("VARCHAR", "JSON", '{"a":1}', "json", '{"a":1}'),
        ("INTEGER", "VARCHAR", "99", "trim", "99"),
        ("DECIMAL", "INTEGER", "10.0", "integer", 10),
    ],
)
def test_apply_transform_cross_types(source_type, target_type, raw, transform, expected):
    val, err = apply_transform(raw, transform)
    assert err is None, f"{source_type}→{target_type}: {err}"
    assert val == expected


def test_infer_transform_uses_target_type():
    assert infer_transform_for_mapping("amount", "total_cents", "VARCHAR", "INTEGER") == "integer"
    assert infer_transform_for_mapping("payload", "doc", "VARCHAR", "JSON") == "json"
    assert infer_transform_for_mapping("created", "created_at", "VARCHAR", "TIMESTAMP") == "datetime"


def test_mapping_pipeline_passes_source_types():
    result = run_mapping_pipeline(
        ["payment_amount", "txn_date"],
        ["payment_amount", "txn_date"],
        source_schemas=[
            {"name": "payment_amount", "inferred_type": "DECIMAL", "samples": ["10.00"]},
            {"name": "txn_date", "inferred_type": "DATE", "samples": ["2024-01-01"]},
        ],
        target_schemas=[
            {"name": "payment_amount", "inferred_type": "NUMERIC", "samples": []},
            {"name": "txn_date", "inferred_type": "DATE", "samples": []},
        ],
        confidence_threshold=0.5,
    )
    by_source = {m["source"]: m for m in result["mappings"]}
    assert by_source["payment_amount"]["transform"] == "decimal"
    assert by_source["txn_date"]["transform"] == "date"
    assert by_source["payment_amount"]["source_type"] == "DECIMAL"


@pytest.mark.parametrize("dest", ["postgresql", "mysql", "snowflake", "bigquery", "mongodb", "redshift"])
def test_ddl_covers_all_logical_types(dest: str):
    logical_types = ["string", "integer", "decimal", "boolean", "date", "datetime", "json", "binary", "uuid"]
    for lt in logical_types:
        ddl = ddl_type(dest, lt)
        assert isinstance(ddl, str) and len(ddl) > 0, f"{dest}/{lt} -> {ddl!r}"


def test_lossy_coercion_detection():
    assert is_lossy_coercion("VARCHAR", "INTEGER")
    assert is_lossy_coercion("DECIMAL", "INTEGER")
    assert is_lossy_coercion("TIMESTAMP", "DATE")
    assert not is_lossy_coercion("INTEGER", "VARCHAR")
    assert not is_lossy_coercion("DATE", "DATE")


@pytest.mark.parametrize(
    "source_type,target_type,expected",
    [
        # widening / reversible conversions are safe
        ("INTEGER", "DECIMAL", False),
        ("INTEGER", "VARCHAR", False),
        ("DATE", "DATETIME", False),
        ("BOOLEAN", "INTEGER", False),
        ("JSON", "TEXT", False),
        ("BINARY", "TEXT", False),
        ("TEXT", "BINARY", False),
        ("UUID", "VARCHAR", False),
        # narrowing or semantically incompatible conversions are lossy
        ("VARCHAR", "UUID", True),
        ("INTEGER", "UUID", True),
        ("DECIMAL", "UUID", True),
        ("DATETIME", "DATE", True),
        ("DATE", "TIME", True),
        ("VARCHAR", "BOOLEAN", True),
        ("JSON", "INTEGER", True),
        ("ARRAY", "BOOLEAN", True),
        ("BINARY", "INTEGER", True),
    ],
)
def test_is_lossy_coercion_matrix(source_type, target_type, expected):
    assert is_lossy_coercion(source_type, target_type) is expected


def test_build_mapped_rows_typed_matrix():
    headers = ["id", "amount", "active", "payload"]
    rows = [["1", "10.50", "true", '{"k":"v"}']]
    mappings = [
        {"source": "id", "target": "id", "target_type": "INTEGER"},
        {"source": "amount", "target": "amount", "target_type": "DECIMAL"},
        {"source": "active", "target": "active", "target_type": "BOOLEAN"},
        {"source": "payload", "target": "payload", "target_type": "JSON"},
    ]
    column_types = {
        "id": "VARCHAR", "amount": "VARCHAR", "active": "VARCHAR", "payload": "VARCHAR",
        "INTEGER": "INTEGER", "DECIMAL": "DECIMAL", "BOOLEAN": "BOOLEAN", "JSON": "JSON",
    }
    mapped, errors = build_mapped_rows(
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        target_cols=["id", "amount", "active", "payload"],
        column_types=column_types,
    )
    assert not errors, errors
    assert mapped == [(1, "10.50", True, '{"k":"v"}')]


@pytest.mark.parametrize("logical", ["string", "integer", "decimal", "datetime", "json"])
def test_normalize_roundtrip(logical: str):
    assert normalize_logical_type(logical.upper()) == logical


# ─── CSV → Snowflake: realistic heterogeneous schema ───

CSV_SNOWFLAKE_COLUMNS = [
    {"name": "order_id", "inferred_type": "VARCHAR", "samples": ["ORD-1001", "ORD-1002"]},
    {"name": "customer_email", "inferred_type": "STRING", "samples": ["a@example.com", "b@corp.io"]},
    {"name": "order_total", "inferred_type": "DECIMAL", "samples": ["1299.99", "42.50"]},
    {"name": "quantity", "inferred_type": "INTEGER", "samples": ["3", "12"]},
    {"name": "is_gift", "inferred_type": "BOOLEAN", "samples": ["true", "false"]},
    {"name": "order_date", "inferred_type": "DATE", "samples": ["2024-03-15", "2024-06-01"]},
    {"name": "shipped_at", "inferred_type": "TIMESTAMP", "samples": ["2024-03-16T10:30:00Z", "2024-06-02T08:00:00"]},
    {"name": "metadata", "inferred_type": "JSON", "samples": ['{"tier":"gold"}', '{"tier":"silver"}']},
    {"name": "notes", "inferred_type": "TEXT", "samples": ["Rush delivery", ""]},
]

CSV_SNOWFLAKE_DDL_EXPECTED = {
    "order_id": "VARCHAR",
    "customer_email": "VARCHAR",
    "order_total": "NUMBER(38,10)",
    "quantity": "NUMBER(38,0)",
    "is_gift": "BOOLEAN",
    "order_date": "DATE",
    "shipped_at": "TIMESTAMP_TZ",
    "metadata": "VARIANT",
    "notes": "VARCHAR",
}


def test_csv_to_snowflake_route_valid():
    ok, msg = validate_transfer("file", "csv", "database", "snowflake")
    assert ok, msg


@pytest.mark.parametrize("col,expected_ddl", list(CSV_SNOWFLAKE_DDL_EXPECTED.items()))
def test_csv_columns_map_to_snowflake_ddl(col: str, expected_ddl: str):
    src = next(c for c in CSV_SNOWFLAKE_COLUMNS if c["name"] == col)
    assert ddl_type("snowflake", src["inferred_type"]) == expected_ddl


def test_csv_to_snowflake_mapping_pipeline_transforms():
    source_names = [c["name"] for c in CSV_SNOWFLAKE_COLUMNS]
    target_names = [c.upper() for c in source_names]
    result = run_mapping_pipeline(
        source_names,
        target_names,
        source_schemas=CSV_SNOWFLAKE_COLUMNS,
        target_schemas=[{"name": t, "inferred_type": "VARCHAR", "samples": []} for t in target_names],
        confidence_threshold=0.5,
    )
    by_source = {m["source"]: m for m in result["mappings"]}
    assert by_source["order_total"]["transform"] == "decimal"
    assert by_source["quantity"]["transform"] == "integer"
    assert by_source["is_gift"]["transform"] == "boolean"
    assert by_source["order_date"]["transform"] == "date"
    assert by_source["shipped_at"]["transform"] == "datetime"
    assert by_source["metadata"]["transform"] == "json"
    assert by_source["order_id"]["source_type"] == "VARCHAR"


def test_csv_rows_map_to_snowflake_typed_values():
    headers = [c["name"] for c in CSV_SNOWFLAKE_COLUMNS]
    row = [
        "ORD-9001",
        "user@test.com",
        "$2,499.00",
        "5",
        "yes",
        "2024-11-20",
        "2024-11-21T14:00:00Z",
        '{"promo":true}',
        "Handle with care",
    ]
    mappings = [
        {
            "source": c["name"],
            "target": c["name"].upper(),
            "target_type": ddl_type("snowflake", c["inferred_type"]),
            "transform": infer_transform_for_mapping(
                c["name"], c["name"].upper(), c["inferred_type"],
                ddl_type("snowflake", c["inferred_type"]),
            ),
        }
        for c in CSV_SNOWFLAKE_COLUMNS
    ]
    column_types = {c["name"]: c["inferred_type"] for c in CSV_SNOWFLAKE_COLUMNS}
    for m in mappings:
        column_types[m["target_type"]] = m["target_type"]

    mapped, errors = build_mapped_rows(
        headers=headers,
        data_rows=[row],
        mappings=mappings,
        target_cols=[m["target"] for m in mappings],
        column_types=column_types,
    )
    assert not errors, errors
    assert mapped[0][0] == "ORD-9001"
    assert mapped[0][2] == "2499.00"
    assert mapped[0][3] == 5
    assert mapped[0][4] is True
    assert mapped[0][5] == "2024-11-20"
    assert mapped[0][7] == '{"promo":true}'


def test_csv_to_snowflake_lossy_coercions_flagged():
    """String and decimal sources coerced to numeric targets are lossy."""
    assert is_lossy_coercion("VARCHAR", "INTEGER")
    assert is_lossy_coercion("DECIMAL", "INTEGER")
    assert not is_lossy_coercion("DATE", "DATE")


@pytest.mark.parametrize(
    "raw,transform,expected,expect_error",
    [
        # booleans with common spellings
        ("true", "boolean", True, False),
        ("FALSE", "boolean", False, False),
        ("yes", "boolean", True, False),
        ("0", "boolean", False, False),
        ("n", "boolean", False, False),
        ("maybe", "boolean", None, True),
        # decimals preserve precision
        ("12345678901234567890.12345", "decimal", "12345678901234567890.12345", False),
        ("1.5e-10", "decimal", "0.00000000015", False),
        # integers reject fractional values
        ("42.0", "integer", 42, False),
        ("999999999999999999999999999999", "integer", 999999999999999999999999999999, False),
        ("3.14", "integer", None, True),
        # timestamps with timezones
        ("2024-06-01T12:00:00+05:30", "datetime", "2024-06-01T06:30:00Z", False),
        # json and arrays
        ('{"a": [1, 2]}', "json", '{"a":[1,2]}', False),
        ("[1, 2, 3]", "json", "[1,2,3]", False),
        ("not-json", "json", None, True),
        # uuids
        ("550e8400-e29b-41d4-a716-446655440000", "uuid", "550e8400-e29b-41d4-a716-446655440000", False),
        # null sentinels collapse to None for typed transforms
        ("NULL", "integer", None, False),
        ("N/A", "decimal", None, False),
        ("", "boolean", None, False),
        # time values
        ("12:30:45", "time", "12:30:45", False),
        ("14:30", "time", "14:30:00", False),
        ("02:30:45 PM", "time", "14:30:45", False),
        ("not-a-time", "time", None, True),
        # binary values
        ("SGVsbG8=", "binary", "SGVsbG8=", False),
        ("hello", "binary", "aGVsbG8=", False),
        # semantic transforms
        ("+1-555-0199", "phone", "+1-555-0199", False),
        ("Test@Example.COM", "email", "test@example.com", False),
        ("HTTPS://Example.COM/Path", "url", "https://Example.COM/Path", False),
        ("GB82 west 1234 5698 7654 32", "iban", "GB82WEST12345698765432", False),
        ("k1a 0b1", "postal", "K1A0B1", False),
        ("$1,234.56", "currency", "1234.56", False),
        ("50%", "percentage", "50", False),
    ],
)
def test_apply_transform_edge_values(raw, transform, expected, expect_error):
    val, err = apply_transform(raw, transform)
    if expect_error:
        assert err is not None
        assert val is None
    else:
        assert err is None, f"{raw} -> {transform}: {err}"
        assert val == expected
