from __future__ import annotations

import builtins
import os
import socket
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

try:
    from connectors.iceberg_partitioning import (
        IcebergPartitionSpecError,
        parse_partition_spec,
        partition_spec_for_schema,
        partition_spec_matches,
    )
except ImportError:
    IcebergPartitionSpecError = None
    parse_partition_spec = None
    partition_spec_for_schema = None
    partition_spec_matches = None


REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181")
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_WAREHOUSE", "s3://warehouse/"
)
S3_ENDPOINT = os.environ.get(
    "DATAFLOW_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000"
)


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


def _sql_endpoint(tmp_path, table_name: str) -> dict:
    return {
        "connection_string": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": str(tmp_path / "warehouse"),
        "table": f"default.{table_name}",
        "extra": {"catalog_type": "sql"},
    }


def _writer_args(
    endpoint: dict,
    table_name: str,
    rows: list[list[str]],
) -> dict:
    return {
        "connection_string": endpoint["connection_string"],
        "warehouse": endpoint["warehouse"],
        "table_name": f"default.{table_name}",
        "headers": ["id", "event_day", "value"],
        "data_rows": rows,
        "mappings": [
            {"source": "id", "target": "id", "target_type": "integer"},
            {
                "source": "event_day",
                "target": "event_day",
                "target_type": "date",
            },
            {"source": "value", "target": "value", "target_type": "string"},
        ],
        "column_types": {
            "id": "integer",
            "event_day": "date",
            "value": "string",
        },
        "write_mode": "append",
        "create_table": True,
        "extra": dict(endpoint["extra"]),
    }


def _write_partitioned(
    endpoint: dict,
    table_name: str,
    rows: list[list[str]],
    spec: list[dict[str, str]],
    *,
    allow_evolution: bool = False,
    spec_in_extra: bool = False,
):
    from connectors.iceberg_writer import write_mapped_rows

    args = _writer_args(endpoint, table_name, rows)
    if spec_in_extra:
        args["extra"]["partition_spec"] = spec
        if allow_evolution:
            args["extra"]["allow_partition_evolution"] = True
    else:
        args["partition_spec"] = spec
        if allow_evolution:
            args["allow_partition_evolution"] = True
    return write_mapped_rows(**args)


def _block_pyiceberg_core(monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__

    def import_without_core(name, *args, **kwargs):
        if name == "pyiceberg_core" or name.startswith("pyiceberg_core."):
            raise ModuleNotFoundError(
                "No module named 'pyiceberg_core'", name="pyiceberg_core"
            )
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_core)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("identity", "identity"),
        ("bucket[16]", "bucket[16]"),
        ("truncate[8]", "truncate[8]"),
        ("year", "year"),
        ("month", "month"),
        ("day", "day"),
        ("hour", "hour"),
    ],
)
def test_partition_spec_parser_uses_pyiceberg_transforms(
    name: str, expected: str
) -> None:
    assert issubclass(IcebergPartitionSpecError, ValueError)
    parsed = parse_partition_spec([{"column": "event_day", "transform": name}])
    assert len(parsed) == 1
    assert str(parsed[0][1]) == expected


@pytest.mark.parametrize(
    "transform",
    ["bucket[0]", "bucket[-1]", "bucket[x]", "truncate[0]", "truncate[1.5]", "random"],
)
def test_partition_spec_parser_rejects_invalid_transforms(
    transform: str,
) -> None:
    with pytest.raises(ValueError, match="partition"):
        parse_partition_spec([{"column": "id", "transform": transform}])


@pytest.mark.parametrize(
    "spec",
    [
        None,
        {"column": "id", "transform": "identity"},
        [{"column": "id"}],
        [{"column": "id", "transform": "identity", "extra": True}],
        [{"column": "", "transform": "identity"}],
        [{"column": 1, "transform": "identity"}],
    ],
)
def test_partition_spec_parser_rejects_invalid_shapes(spec) -> None:
    with pytest.raises(ValueError, match="partition_spec"):
        parse_partition_spec(spec)


def test_partition_spec_rejects_missing_columns_and_incompatible_transforms() -> None:
    parsed = parse_partition_spec([{"column": "missing", "transform": "identity"}])
    schema = Schema(NestedField(1, "id", StringType()))
    with pytest.raises(ValueError, match="does not exist"):
        partition_spec_for_schema(schema, parsed)

    day_spec = parse_partition_spec([{"column": "id", "transform": "day"}])
    with pytest.raises(ValueError, match="cannot transform"):
        partition_spec_for_schema(schema, day_spec)


def test_writer_rejects_invalid_partition_spec_at_root_and_extra(tmp_path) -> None:
    from connectors.iceberg_writer import write_mapped_rows

    invalid_specs = (
        {"partition_spec": [{"column": "id", "transform": "bucket[0]"}]},
        {
            "extra": {
                "catalog_type": "sql",
                "partition_spec": [{"column": "missing", "transform": "identity"}],
            }
        },
    )
    for index, options in enumerate(invalid_specs):
        endpoint = _sql_endpoint(tmp_path, f"invalid_spec_{index}")
        args = _writer_args(
            endpoint,
            f"invalid_spec_{index}",
            [["1", "2024-01-01", "a"]],
        )
        args.update(options)
        with pytest.raises(ValueError, match="partition"):
            write_mapped_rows(**args)


def test_writer_fails_closed_when_partition_core_is_missing(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_name = "partition_core_missing"
    endpoint = _sql_endpoint(tmp_path, table_name)
    _block_pyiceberg_core(monkeypatch)

    result = _write_partitioned(
        endpoint,
        table_name,
        [["1", "2024-01-02", "first"]],
        [
            {"column": "id", "transform": "bucket[16]"},
            {"column": "event_day", "transform": "day"},
        ],
    )

    assert not result.ok
    assert result.error.startswith("IcebergPartitionSpecError:")
    assert "pyiceberg-core" in result.error


def test_partitioned_table_fails_closed_when_partition_core_is_missing(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from connectors.iceberg_writer import write_mapped_rows

    table_name = "partition_core_existing"
    endpoint = _sql_endpoint(tmp_path, table_name)
    initial = _write_partitioned(
        endpoint,
        table_name,
        [["1", "2024-01-02", "first"]],
        [
            {"column": "id", "transform": "bucket[16]"},
            {"column": "event_day", "transform": "day"},
        ],
    )
    assert initial.ok, initial.error

    _block_pyiceberg_core(monkeypatch)
    result = write_mapped_rows(
        **_writer_args(endpoint, table_name, [["2", "2024-01-03", "second"]])
    )

    assert not result.ok
    assert result.error.startswith("IcebergPartitionSpecError:")
    assert "pyiceberg-core" in result.error


def test_unpartitioned_writer_is_unaffected_when_partition_core_is_missing(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from connectors.iceberg_catalog import load_catalog
    from connectors.iceberg_writer import write_mapped_rows

    table_name = "unpartitioned_without_core"
    endpoint = _sql_endpoint(tmp_path, table_name)
    _block_pyiceberg_core(monkeypatch)

    result = write_mapped_rows(
        **_writer_args(endpoint, table_name, [["1", "2024-01-02", "first"]])
    )

    assert result.ok, result.error
    table = load_catalog(endpoint).load_table(("default", table_name))
    assert table.scan().to_arrow().num_rows == 1


def test_sql_catalog_creates_bucket_and_day_partition_spec(tmp_path) -> None:
    from connectors.iceberg_catalog import load_catalog

    table_name = "partition_create"
    endpoint = _sql_endpoint(tmp_path, table_name)
    result = _write_partitioned(
        endpoint,
        table_name,
        [["1", "2024-01-02", "first"]],
        [
            {"column": "id", "transform": "bucket[16]"},
            {"column": "event_day", "transform": "day"},
        ],
        spec_in_extra=True,
    )
    assert result.ok, result.error

    table = load_catalog(endpoint).load_table(("default", table_name))
    assert {
        (table.schema().find_column_name(field.source_id), str(field.transform))
        for field in table.spec().fields
    } == {("id", "bucket[16]"), ("event_day", "day")}
    assert partition_spec_matches(
        table.schema(),
        table.spec(),
        parse_partition_spec(
            [
                {"column": "id", "transform": "bucket[16]"},
                {"column": "event_day", "transform": "day"},
            ]
        ),
    )


def test_sql_catalog_refuses_partition_drift_without_mutating_table(
    tmp_path,
) -> None:
    from connectors.iceberg_catalog import load_catalog

    table_name = "partition_drift"
    endpoint = _sql_endpoint(tmp_path, table_name)
    initial_spec = [
        {"column": "id", "transform": "bucket[16]"},
        {"column": "event_day", "transform": "day"},
    ]
    assert _write_partitioned(
        endpoint, table_name, [["1", "2024-01-02", "first"]], initial_spec
    ).ok

    catalog = load_catalog(endpoint)
    table = catalog.load_table(("default", table_name))
    before_spec = table.spec()
    before_snapshots = len(table.metadata.snapshots)
    result = _write_partitioned(
        endpoint,
        table_name,
        [["2", "2024-01-03", "second"]],
        [
            {"column": "id", "transform": "bucket[8]"},
            {"column": "event_day", "transform": "day"},
        ],
    )

    assert not result.ok
    assert result.error.startswith("IcebergPartitionSpecError:")
    table = catalog.load_table(("default", table_name))
    assert table.spec() == before_spec
    assert len(table.metadata.snapshots) == before_snapshots
    assert table.scan().to_arrow().num_rows == 1


def test_sql_catalog_evolves_partition_spec_and_keeps_old_data(
    tmp_path,
) -> None:
    from connectors.iceberg_catalog import load_catalog

    table_name = "partition_evolution"
    endpoint = _sql_endpoint(tmp_path, table_name)
    initial_spec = [
        {"column": "id", "transform": "bucket[16]"},
        {"column": "event_day", "transform": "day"},
    ]
    assert _write_partitioned(
        endpoint, table_name, [["1", "2024-01-02", "first"]], initial_spec
    ).ok

    result = _write_partitioned(
        endpoint,
        table_name,
        [["2", "2024-01-03", "second"]],
        [
            {"column": "id", "transform": "bucket[8]"},
            {"column": "event_day", "transform": "day"},
        ],
        allow_evolution=True,
    )
    assert result.ok, result.error

    table = load_catalog(endpoint).load_table(("default", table_name))
    assert {
        (table.schema().find_column_name(field.source_id), str(field.transform))
        for field in table.spec().fields
    } == {("id", "bucket[8]"), ("event_day", "day")}
    assert table.scan().to_arrow().num_rows == 2
    rows = table.scan().to_arrow().to_pylist()
    assert {row["id"]: row["value"] for row in rows} == {
        1: "first",
        2: "second",
    }


def _sql_lookup_table(tmp_path):
    from connectors.iceberg_catalog import load_catalog
    from pyiceberg.exceptions import NamespaceAlreadyExistsError
    from pyiceberg.types import NestedField

    table_name = "pk_lookup"
    endpoint = _sql_endpoint(tmp_path, table_name)
    catalog = load_catalog(endpoint)
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass
    catalog.create_table(
        ("default", table_name),
        schema=Schema(
            NestedField(1, "id", StringType(), required=True),
            NestedField(2, "_df_lsn", StringType()),
            NestedField(3, "value", StringType()),
        ),
    )
    rows = [
        {
            "id": f"k{index:03d}",
            "_df_lsn": "0/3" if index == 200 else "0/1",
            "value": str(index),
        }
        for index in range(201)
    ]
    catalog.load_table(("default", table_name)).append(
        pa.Table.from_pylist(
            rows,
            schema=pa.schema(
                [
                    pa.field("id", pa.string(), nullable=False),
                    pa.field("_df_lsn", pa.string()),
                    pa.field("value", pa.string()),
                ]
            ),
        )
    )
    return endpoint, catalog, table_name


def test_eos_and_delete_pk_lookups_use_sliced_row_filters(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from connectors import iceberg_eos, iceberg_writer
    from pyiceberg.table import Table

    endpoint, catalog, table_name = _sql_lookup_table(tmp_path)
    table = catalog.load_table(("default", table_name))
    original_scan = Table.scan
    filters = []
    unfiltered_scans = []

    def spy_scan(self, *args, **kwargs):
        row_filter = kwargs.get("row_filter", args[0] if args else None)
        if row_filter is None:
            unfiltered_scans.append(True)
        else:
            filters.append(row_filter)
        return original_scan(self, *args, **kwargs)

    all_keys = {f"k{index:03d}" for index in range(201)}
    with monkeypatch.context() as patch:
        patch.setattr(Table, "scan", spy_scan)
        rows = iceberg_eos._scan_dest_rows(table, ["id"], all_keys)
    assert len(rows) == 201
    assert len(filters) == 2
    assert not unfiltered_scans

    filters.clear()
    unfiltered_scans.clear()
    with monkeypatch.context() as patch:
        patch.setattr(Table, "scan", spy_scan)
        deleted = iceberg_writer._delete_pyiceberg(
            endpoint,
            ["id"],
            all_keys,
            incoming_lsn="0/2",
            lsn_column="_df_lsn",
        )
    assert deleted == 200
    assert len(filters) >= 2
    assert not unfiltered_scans

    remaining = catalog.load_table(("default", table_name)).scan().to_arrow().to_pylist()
    assert [(row["id"], row["_df_lsn"]) for row in remaining] == [
        ("k200", "0/3")
    ]


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


@requires_rest
def test_live_rest_partition_create_evolve_and_duckdb_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.iceberg_writer import write_mapped_rows
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    load_catalog = _install_rest_catalog_loader(monkeypatch)
    table_name = f"m4_partition_{uuid4().hex[:12]}"
    endpoint = {
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "schema": "default",
        "table": table_name,
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
    }
    catalog = load_catalog(endpoint)
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass

    initial_spec = [
        {"column": "id", "transform": "bucket[16]"},
        {"column": "event_day", "transform": "day"},
    ]
    first = write_mapped_rows(
        **_writer_args(endpoint, table_name, [["1", "2024-01-02", "first"]]),
        partition_spec=initial_spec,
    )
    assert first.ok, first.error

    second = write_mapped_rows(
        **_writer_args(endpoint, table_name, [["2", "2024-01-03", "second"]]),
        partition_spec=[
            {"column": "id", "transform": "bucket[8]"},
            {"column": "event_day", "transform": "day"},
        ],
        allow_partition_evolution=True,
    )
    assert second.ok, second.error

    table = catalog.load_table(("default", table_name))
    assert {
        (table.schema().find_column_name(field.source_id), str(field.transform))
        for field in table.spec().fields
    } == {("id", "bucket[8]"), ("event_day", "day")}
    assert table.scan().to_arrow().num_rows == 2

    try:
        import duckdb
    except ImportError:
        pytest.skip("DuckDB is not installed")

    duck = duckdb.connect()
    try:
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
        rows = sorted(
            duck.execute(
                "SELECT id, value FROM iceberg_scan(?)",
                [table.metadata_location],
            ).fetchall()
        )
        assert rows == [(1, "first"), (2, "second")]
    finally:
        duck.close()
