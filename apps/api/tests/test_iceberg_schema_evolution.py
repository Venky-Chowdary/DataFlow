from __future__ import annotations

import os
import socket
from dataclasses import FrozenInstanceError
from decimal import Decimal
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

try:
    from connectors.iceberg_schema_evolution import (
        IcebergSchemaEvolutionError,
        SchemaChange,
        SchemaPlan,
        apply_schema_plan,
        plan_schema_change,
    )
except ImportError:
    IcebergSchemaEvolutionError = None
    SchemaChange = None
    SchemaPlan = None
    apply_schema_plan = None
    plan_schema_change = None


REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181")
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_WAREHOUSE", "s3://warehouse/"
)
S3_ENDPOINT = os.environ.get(
    "DATAFLOW_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000"
)


def _schema(name: str, field_type: object, *, required: bool = False) -> Schema:
    return Schema(NestedField(1, name, field_type, required=required))


def _rest_reachable() -> bool:
    try:
        for url, default_port in ((REST_URI, 8181), (S3_ENDPOINT, 9000)):
            host = url.split("://", 1)[-1].split("/", 1)[0]
            hostname, _, port_s = host.partition(":")
            with socket.create_connection(
                (hostname, int(port_s or str(default_port))), timeout=1.5
            ):
                pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as response:
            return int(getattr(response, "status", 0) or 0) == 200
    except (OSError, URLError, ValueError):
        return False


requires_rest = pytest.mark.skipif(
    not _rest_reachable(),
    reason=f"Iceberg REST or S3 endpoint is not reachable: {REST_URI}, {S3_ENDPOINT}",
)


def _sql_catalog(tmp_path, table_name: str):
    from connectors.iceberg_catalog import load_catalog
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    warehouse = str(tmp_path / "warehouse")
    uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    endpoint = {
        "connection_string": uri,
        "warehouse": warehouse,
        "table": f"default.{table_name}",
        "extra": {"catalog_type": "sql"},
    }
    catalog = load_catalog(endpoint)
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass
    return catalog, warehouse, uri, table_name


def _create_sql_table(
    tmp_path,
    table_name: str,
    fields: list[NestedField],
    row: dict,
    arrow_fields: list[pa.Field],
):
    catalog, warehouse, uri, table_name = _sql_catalog(tmp_path, table_name)
    catalog.create_table(("default", table_name), schema=Schema(*fields))
    catalog.load_table(("default", table_name)).append(
        pa.Table.from_pylist([row], schema=pa.schema(arrow_fields))
    )
    return catalog, warehouse, uri, table_name


def _writer_args(
    uri: str,
    warehouse: str,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
) -> dict:
    return {
        "connection_string": uri,
        "warehouse": warehouse,
        "table_name": f"default.{table_name}",
        "headers": headers,
        "data_rows": data_rows,
        "mappings": mappings,
        "column_types": {
            str(mapping["target"]): str(mapping.get("target_type") or "string")
            for mapping in mappings
        },
        "write_mode": "append",
        "create_table": True,
    }


def _rest_endpoint(table_name: str) -> dict:
    return {
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "schema": "default",
        "table": table_name,
        "extra": {
            "catalog_type": "rest",
            "warehouse": REST_WAREHOUSE,
        },
    }


def _install_rest_catalog_loader(monkeypatch: pytest.MonkeyPatch):
    import connectors.iceberg_catalog as catalog_module
    from connectors.iceberg_catalog import parse_iceberg_catalog_config
    from pyiceberg.catalog.rest import RestCatalog

    def load_catalog(endpoint):
        config = parse_iceberg_catalog_config(endpoint)
        properties = dict(config["properties"])
        properties.update(
            {
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": os.environ.get("MINIO_ROOT_USER", "admin"),
                "s3.secret-access-key": os.environ.get(
                    "MINIO_ROOT_PASSWORD", "password"
                ),
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
            }
        )
        return RestCatalog(config["catalog_name"], **properties)

    monkeypatch.setattr(catalog_module, "load_catalog", load_catalog)
    return load_catalog


def test_schema_change_types_are_frozen() -> None:
    change = SchemaChange("add", "new", None, StringType())
    plan = SchemaPlan((change,), ())
    with pytest.raises(FrozenInstanceError):
        change.column = "changed"
    with pytest.raises(FrozenInstanceError):
        plan.refused = ("reason",)


def test_plan_adds_new_column_as_optional() -> None:
    plan = plan_schema_change(
        _schema("value", IntegerType()),
        pa.schema(
            [
                pa.field("value", pa.int32()),
                pa.field("added", pa.string()),
            ]
        ),
    )
    assert not plan.refused
    assert [(change.kind, change.column) for change in plan.changes] == [
        ("add", "added")
    ]
    assert plan.changes[0].from_type is None
    assert plan.changes[0].to_type == StringType()


@pytest.mark.parametrize(
    ("existing_type", "incoming_type", "arrow_type"),
    [
        (IntegerType(), LongType(), pa.int64()),
        (FloatType(), DoubleType(), pa.float64()),
        (DecimalType(10, 2), DecimalType(12, 2), pa.decimal128(12, 2)),
    ],
    ids=["int-to-long", "float-to-double", "decimal-precision"],
)
def test_plan_allows_spec_type_widening(
    existing_type, incoming_type, arrow_type
) -> None:
    plan = plan_schema_change(_schema("value", existing_type), pa.schema([("value", arrow_type)]))
    assert not plan.refused
    assert len(plan.changes) == 1
    assert plan.changes[0].kind == "widen"
    assert plan.changes[0].from_type == existing_type
    assert plan.changes[0].to_type == incoming_type


@pytest.mark.parametrize(
    ("existing_type", "incoming_type", "allow_widening"),
    [
        (LongType(), IntegerType(), True),
        (IntegerType(), StringType(), True),
        (DecimalType(10, 2), DecimalType(12, 3), True),
        (IntegerType(), LongType(), False),
    ],
    ids=["narrowing", "cross-family", "decimal-scale", "widening-disabled"],
)
def test_plan_refuses_unsupported_type_changes(
    existing_type, incoming_type, allow_widening
) -> None:
    plan = plan_schema_change(
        _schema("value", existing_type),
        Schema(NestedField(1, "value", incoming_type)),
        allow_widening=allow_widening,
    )
    assert not plan.changes
    assert plan.refused


def test_plan_rename_is_only_explicit_and_uses_existing_field_id() -> None:
    existing = _schema("old", IntegerType())
    incoming = pa.schema([("new", pa.int32())])

    inferred = plan_schema_change(existing, incoming)
    assert [(change.kind, change.column) for change in inferred.changes] == [
        ("add", "new")
    ]

    explicit = plan_schema_change(
        existing, incoming, rename_columns={"old": "new"}
    )
    assert not explicit.refused
    assert len(explicit.changes) == 1
    assert explicit.changes[0].kind == "rename"
    assert explicit.changes[0].column == "old"
    assert explicit.changes[0].new_name == "new"


@pytest.mark.parametrize(
    ("renames", "reason"),
    [
        ({"missing": "new"}, "source"),
        ({"old": "present"}, "target"),
    ],
    ids=["missing-source", "existing-target"],
)
def test_plan_refuses_invalid_rename_hints(renames, reason) -> None:
    existing = Schema(
        NestedField(1, "old", StringType()),
        NestedField(2, "present", StringType()),
    )
    plan = plan_schema_change(
        existing,
        pa.schema([("old", pa.string()), ("present", pa.string())]),
        rename_columns=renames,
    )
    assert not plan.changes
    assert any(reason in refusal for refusal in plan.refused)


def test_plan_preserves_existing_requiredness_and_refuses_nullability_changes() -> None:
    required = _schema("value", IntegerType(), required=True)
    unchanged = plan_schema_change(
        required,
        pa.schema([pa.field("value", pa.int32(), nullable=False)]),
    )
    assert not unchanged.changes
    assert not unchanged.refused

    became_optional = plan_schema_change(
        required,
        pa.schema([pa.field("value", pa.int32(), nullable=True)]),
    )
    assert any("required to optional" in refusal for refusal in became_optional.refused)

    became_required = plan_schema_change(
        _schema("value", IntegerType()),
        pa.schema([pa.field("value", pa.int32(), nullable=False)]),
    )
    assert any("optional to required" in refusal for refusal in became_required.refused)


def test_plan_refuses_adding_required_column() -> None:
    plan = plan_schema_change(
        _schema("value", IntegerType()),
        pa.schema(
            [
                pa.field("value", pa.int32()),
                pa.field("required_new", pa.string(), nullable=False),
            ]
        ),
    )
    assert not plan.changes
    assert any("cannot add required column" in refusal for refusal in plan.refused)


def test_plan_validates_rename_mapping_values() -> None:
    with pytest.raises(ValueError, match="rename_columns"):
        plan_schema_change(
            _schema("value", StringType()),
            pa.schema([("value", pa.string())]),
            rename_columns={1: "new"},
        )


def test_apply_schema_plan_raises_for_refused_changes() -> None:
    plan = plan_schema_change(
        _schema("value", LongType()),
        pa.schema([("value", pa.int32())]),
    )
    with pytest.raises(IcebergSchemaEvolutionError):
        apply_schema_plan(None, plan)


@pytest.mark.parametrize(
    "rename_options",
    [
        {"rename_columns": [("old", "new")]},
        {"extra": {"rename_columns": [("old", "new")]}},
    ],
    ids=["endpoint", "extra"],
)
def test_writer_rejects_non_dict_rename_columns(tmp_path, rename_options) -> None:
    from connectors.iceberg_writer import write_mapped_rows

    kwargs = dict(rename_options)
    with pytest.raises(ValueError, match="rename_columns"):
        write_mapped_rows(
            **_writer_args(
                f"sqlite:///{tmp_path / 'catalog.db'}",
                str(tmp_path / "warehouse"),
                "invalid_rename",
                ["id"],
                [["1"]],
                [{"source": "id", "target": "id", "target_type": "string"}],
            ),
            **kwargs,
        )


def test_sql_catalog_widens_int_to_long_and_keeps_values(tmp_path) -> None:
    pa_type = pa.int32()
    catalog, warehouse, uri, table_name = _create_sql_table(
        tmp_path,
        "widen_int",
        [
            NestedField(1, "id", StringType()),
            NestedField(2, "value", IntegerType()),
        ],
        {"id": "old", "value": 7},
        [pa.field("id", pa.string()), pa.field("value", pa_type)],
    )
    from connectors.iceberg_writer import write_mapped_rows

    result = write_mapped_rows(
        **_writer_args(
            uri,
            warehouse,
            table_name,
            ["id", "value"],
            [["large", "3000000000"]],
            [
                {"source": "id", "target": "id", "target_type": "string"},
                {"source": "value", "target": "value", "target_type": "long"},
            ],
        )
    )
    assert result.ok, result.error
    table = catalog.load_table(("default", table_name))
    assert table.schema().find_field("value").field_type == LongType()
    rows = table.scan().to_arrow().to_pylist()
    assert {row["id"]: row["value"] for row in rows} == {
        "old": 7,
        "large": 3000000000,
    }


def test_sql_catalog_widens_float_to_double_and_keeps_values(tmp_path) -> None:
    catalog, warehouse, uri, table_name = _create_sql_table(
        tmp_path,
        "widen_float",
        [
            NestedField(1, "id", StringType()),
            NestedField(2, "value", FloatType()),
        ],
        {"id": "old", "value": 1.25},
        [pa.field("id", pa.string()), pa.field("value", pa.float32())],
    )
    from connectors.iceberg_writer import write_mapped_rows

    result = write_mapped_rows(
        **_writer_args(
            uri,
            warehouse,
            table_name,
            ["id", "value"],
            [["new", "3.141592653589793"]],
            [
                {"source": "id", "target": "id", "target_type": "string"},
                {"source": "value", "target": "value", "target_type": "double"},
            ],
        )
    )
    assert result.ok, result.error
    table = catalog.load_table(("default", table_name))
    assert table.schema().find_field("value").field_type == DoubleType()
    rows = table.scan().to_arrow().to_pylist()
    assert {row["id"] for row in rows} == {"old", "new"}
    assert next(row["value"] for row in rows if row["id"] == "new") == pytest.approx(
        3.141592653589793
    )


def test_sql_catalog_widens_decimal_precision_and_keeps_values(tmp_path) -> None:
    catalog, warehouse, uri, table_name = _create_sql_table(
        tmp_path,
        "widen_decimal",
        [
            NestedField(1, "id", StringType()),
            NestedField(2, "amount", DecimalType(10, 2)),
        ],
        {"id": "old", "amount": Decimal("12.34")},
        [
            pa.field("id", pa.string()),
            pa.field("amount", pa.decimal128(10, 2)),
        ],
    )
    from connectors.iceberg_writer import write_mapped_rows

    result = write_mapped_rows(
        **_writer_args(
            uri,
            warehouse,
            table_name,
            ["id", "amount"],
            [["new", "1234567890.12"]],
            [
                {"source": "id", "target": "id", "target_type": "string"},
                {
                    "source": "amount",
                    "target": "amount",
                    "target_type": "DECIMAL(12,2)",
                },
            ],
        )
    )
    assert result.ok, result.error
    table = catalog.load_table(("default", table_name))
    assert table.schema().find_field("amount").field_type == DecimalType(12, 2)
    rows = table.scan().to_arrow().to_pylist()
    assert {row["id"] for row in rows} == {"old", "new"}
    assert next(row["amount"] for row in rows if row["id"] == "new") == Decimal(
        "1234567890.12"
    )


def test_sql_catalog_rename_preserves_field_id_and_old_values(tmp_path) -> None:
    catalog, warehouse, uri, table_name = _create_sql_table(
        tmp_path,
        "rename_column",
        [
            NestedField(1, "id", StringType()),
            NestedField(2, "old_name", StringType()),
        ],
        {"id": "old", "old_name": "legacy"},
        [pa.field("id", pa.string()), pa.field("old_name", pa.string())],
    )
    old_field_id = catalog.load_table(("default", table_name)).schema().find_field(
        "old_name"
    ).field_id
    from connectors.iceberg_writer import write_mapped_rows

    result = write_mapped_rows(
        **_writer_args(
            uri,
            warehouse,
            table_name,
            ["id", "new_name"],
            [["new", "current"]],
            [
                {"source": "id", "target": "id", "target_type": "string"},
                {
                    "source": "new_name",
                    "target": "new_name",
                    "target_type": "string",
                },
            ],
        ),
        extra={"rename_columns": {"old_name": "new_name"}},
    )
    assert result.ok, result.error
    table = catalog.load_table(("default", table_name))
    assert table.schema().find_field("new_name").field_id == old_field_id
    rows = table.scan().to_arrow().to_pylist()
    assert {row["id"]: row["new_name"] for row in rows} == {
        "old": "legacy",
        "new": "current",
    }


def test_sql_catalog_strict_refusal_does_not_mutate_table(tmp_path) -> None:
    catalog, warehouse, uri, table_name = _create_sql_table(
        tmp_path,
        "strict_refusal",
        [
            NestedField(1, "id", StringType()),
            NestedField(2, "value", StringType()),
        ],
        {"id": "old", "value": "original"},
        [pa.field("id", pa.string()), pa.field("value", pa.string())],
    )
    from connectors.iceberg_writer import write_mapped_rows

    before = catalog.load_table(("default", table_name))
    before_schema = before.schema().model_dump_json()
    before_snapshot = before.current_snapshot().snapshot_id
    before_rows = before.scan().to_arrow().to_pylist()
    result = write_mapped_rows(
        **_writer_args(
            uri,
            warehouse,
            table_name,
            ["id", "value"],
            [["new", "42"]],
            [
                {"source": "id", "target": "id", "target_type": "string"},
                {"source": "value", "target": "value", "target_type": "int"},
            ],
        ),
        schema_evolution="strict",
    )
    assert not result.ok
    assert "IcebergSchemaEvolutionError" in (result.error or "")
    after = catalog.load_table(("default", table_name))
    assert after.schema().model_dump_json() == before_schema
    assert after.current_snapshot().snapshot_id == before_snapshot
    assert after.scan().to_arrow().to_pylist() == before_rows


def test_sql_catalog_non_strict_refusal_keeps_type_locked_behavior(tmp_path) -> None:
    catalog, warehouse, uri, table_name = _create_sql_table(
        tmp_path,
        "non_strict_refusal",
        [
            NestedField(1, "id", StringType()),
            NestedField(2, "value", StringType()),
        ],
        {"id": "old", "value": "original"},
        [pa.field("id", pa.string()), pa.field("value", pa.string())],
    )
    from connectors.iceberg_writer import write_mapped_rows

    result = write_mapped_rows(
        **_writer_args(
            uri,
            warehouse,
            table_name,
            ["id", "value"],
            [["new", "42"]],
            [
                {"source": "id", "target": "id", "target_type": "string"},
                {"source": "value", "target": "value", "target_type": "int"},
            ],
        )
    )
    assert result.ok, result.error
    assert any("type_locked" in warning for warning in result.warnings)
    table = catalog.load_table(("default", table_name))
    assert table.schema().find_field("value").field_type == StringType()
    rows = table.scan().to_arrow().to_pylist()
    assert {row["id"]: row["value"] for row in rows} == {
        "old": "original",
        "new": "42",
    }


@requires_rest
def test_live_rest_add_widen_and_rename(monkeypatch, tmp_path) -> None:
    from connectors.iceberg_writer import write_mapped_rows
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    load_catalog = _install_rest_catalog_loader(monkeypatch)
    table_name = f"m3_evolution_{uuid4().hex[:12]}"
    endpoint = _rest_endpoint(table_name)
    catalog = load_catalog(endpoint)
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass
    catalog.create_table(
        ("default", table_name),
        schema=Schema(
            NestedField(1, "id", StringType()),
            NestedField(2, "amount", IntegerType()),
            NestedField(3, "old_name", StringType()),
        ),
    )
    catalog.load_table(("default", table_name)).append(
        pa.Table.from_pylist(
            [{"id": "old", "amount": 7, "old_name": "legacy"}],
            schema=pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("amount", pa.int32()),
                    pa.field("old_name", pa.string()),
                ]
            ),
        )
    )

    result = write_mapped_rows(
        connection_string=REST_URI,
        warehouse=REST_WAREHOUSE,
        table_name=f"default.{table_name}",
        headers=["id", "amount", "new_name", "added"],
        data_rows=[["new", "3000000000", "current", "extra"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "string"},
            {"source": "amount", "target": "amount", "target_type": "long"},
            {"source": "new_name", "target": "new_name", "target_type": "string"},
            {"source": "added", "target": "added", "target_type": "string"},
        ],
        column_types={
            "id": "string",
            "amount": "long",
            "new_name": "string",
            "added": "string",
        },
        write_mode="append",
        rename_columns={"old_name": "new_name"},
    )
    assert result.ok, result.error

    table = catalog.load_table(("default", table_name))
    assert table.schema().find_field("amount").field_type == LongType()
    assert table.schema().find_field("new_name").field_id == 3
    assert table.schema().find_field("added").required is False
    rows = table.scan().to_arrow().to_pylist()
    assert {row["id"]: row for row in rows} == {
        "old": {
            "id": "old",
            "amount": 7,
            "new_name": "legacy",
            "added": None,
        },
        "new": {
            "id": "new",
            "amount": 3000000000,
            "new_name": "current",
            "added": "extra",
        },
    }
    try:
        import duckdb
    except ImportError:
        duckdb = None

    if duckdb is not None:
        duck = duckdb.connect()
        duck.execute("LOAD iceberg")
        duck.execute(
            """
            CREATE SECRET (
                TYPE S3,
                KEY_ID 'admin',
                SECRET 'password',
                REGION 'us-east-1',
                ENDPOINT '127.0.0.1:9000',
                URL_STYLE 'path',
                USE_SSL false
            )
            """
        )
        duck_rows = sorted(
            duck.execute(
                "SELECT id, amount, new_name, added FROM iceberg_scan(?)",
                [table.metadata_location],
            ).fetchall()
        )
        assert duck_rows == [
            ("new", 3000000000, "current", "extra"),
            ("old", 7, "legacy", None),
        ]
        duck.close()
