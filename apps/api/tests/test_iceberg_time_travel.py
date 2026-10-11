from __future__ import annotations

import time
from datetime import datetime, timezone
from uuid import uuid4

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from connectors.iceberg_catalog import load_catalog
from connectors.iceberg_reader import read_table_batch

try:
    from connectors.iceberg_reader import IcebergTimeTravelError
except ImportError:
    IcebergTimeTravelError = None


def _endpoint(tmp_path, name: str) -> dict:
    return {
        "connection_string": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": str(tmp_path / "warehouse"),
        "table": name,
        "extra": {"catalog_type": "sql"},
    }


def _table(tmp_path, name: str):
    endpoint = _endpoint(tmp_path, name)
    catalog = load_catalog(endpoint)
    catalog.create_namespace("default")
    table = catalog.create_table(
        ("default", name),
        schema=Schema(NestedField(1, "id", StringType(), required=False)),
    )
    return endpoint, catalog, table


def _append(table, *ids: str) -> None:
    table.append(
        pa.Table.from_pylist(
            [{"id": value} for value in ids],
            schema=pa.schema([pa.field("id", pa.string())]),
        )
    )


def test_read_by_snapshot_id_timestamp_ms_and_iso_as_of(tmp_path) -> None:
    name = f"travel_{uuid4().hex[:10]}"
    endpoint, catalog, table = _table(tmp_path, name)
    _append(table, "old")
    first = catalog.load_table(("default", name)).current_snapshot()
    assert first is not None
    time.sleep(0.01)
    _append(catalog.load_table(("default", name)), "new")

    by_id = read_table_batch(cfg=endpoint, table=name, columns=["id"])
    # The default path reads the current snapshot.
    assert sorted(row[0] for row in by_id.rows) == ["new", "old"]
    for option, value in (
        ("snapshot_id", first.snapshot_id),
        ("as_of_timestamp_ms", first.timestamp_ms),
        (
            "as_of",
            datetime.fromtimestamp(
                first.timestamp_ms / 1000, tz=timezone.utc
            ).isoformat(),
        ),
    ):
        historical = read_table_batch(
            cfg={**endpoint, option: value},
            table=name,
            columns=["id"],
        )
        assert historical.rows == [["old"]]


def test_unknown_and_pre_snapshot_requests_raise_typed_error(tmp_path) -> None:
    assert issubclass(IcebergTimeTravelError, ValueError)
    name = f"unknown_{uuid4().hex[:10]}"
    endpoint, catalog, table = _table(tmp_path, name)
    _append(table, "only")
    snapshot = catalog.load_table(("default", name)).current_snapshot()
    assert snapshot is not None
    for option, value in (
        ("snapshot_id", snapshot.snapshot_id + 999_999),
        ("as_of_timestamp_ms", snapshot.timestamp_ms - 1),
    ):
        with pytest.raises(IcebergTimeTravelError, match=option):
            read_table_batch(
                cfg={**endpoint, option: value},
                table=name,
                columns=["id"],
            )


def test_conflicting_and_invalid_options_name_the_key(tmp_path) -> None:
    name = f"invalid_{uuid4().hex[:10]}"
    endpoint, _catalog, table = _table(tmp_path, name)
    _append(table, "only")
    with pytest.raises(ValueError, match="snapshot_id.*as_of"):
        read_table_batch(
            cfg={
                **endpoint,
                "snapshot_id": 1,
                "extra": {"catalog_type": "sql", "as_of": "2024-01-01T00:00:00Z"},
            },
            table=name,
        )
    with pytest.raises(ValueError, match="snapshot_id"):
        read_table_batch(
            cfg={**endpoint, "snapshot_id": "latest"},
            table=name,
        )
    with pytest.raises(ValueError, match="as_of"):
        read_table_batch(
            cfg={**endpoint, "as_of": "not-a-date"},
            table=name,
        )
