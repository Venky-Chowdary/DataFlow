"""Local Iceberg warehouses on a Windows drive letter, and namespace reporting.

Three defects sat on the same write:

* ``C:\\wh`` reaches pyiceberg as a URI whose scheme is ``c`` — *Unrecognized
  filesystem type in URI: c*.
* the ``file:///C:/wh`` form that fixes it hands PyArrow ``/C:/wh``, which the
  Windows local filesystem refuses (``WinError 123``).
* the filesystem writer reported the *table directory* as the target schema, so
  reconciliation re-resolved ``warehouse/<table dir>/<table>``, read the
  destination as empty, and called every source key MISSING_TARGET after a
  correct write.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from connectors.iceberg_catalog import (
    PY_IO_IMPL,
    _warehouse_location,
    local_path_from_location,
    parse_iceberg_catalog_config,
)
from connectors.iceberg_pyarrow_io import strip_uri_drive_slash
from connectors.iceberg_writer import write_mapped_rows

_WINDOWS = os.name == "nt"


def test_file_uri_round_trips_to_the_same_local_path(tmp_path: Path) -> None:
    wh = tmp_path / "warehouse"
    wh.mkdir()
    assert local_path_from_location(str(wh)) == wh.resolve()
    assert local_path_from_location(wh.as_uri()) == wh.resolve()


@pytest.mark.skipif(not _WINDOWS, reason="drive-letter parsing is Windows-only")
def test_drive_letter_uri_is_not_rooted_at_the_share_separator() -> None:
    assert local_path_from_location("file:///C:/warehouse") == Path("C:/warehouse")
    assert local_path_from_location("C:\\warehouse") == Path("C:\\warehouse")


def test_local_warehouse_is_handed_to_pyiceberg_as_a_uri(tmp_path: Path) -> None:
    location = _warehouse_location(str(tmp_path / "wh"))
    assert location.startswith("file://")
    # A drive letter must not survive as the URI scheme.
    assert not location.lower().startswith(("c:", "d:"))


def test_pyarrow_local_io_drops_the_uri_required_leading_slash() -> None:
    assert strip_uri_drive_slash("/C:/warehouse/t/data.parquet") == ("C:/warehouse/t/data.parquet")
    assert strip_uri_drive_slash("/var/lib/warehouse/t") == "/var/lib/warehouse/t"
    assert strip_uri_drive_slash("") == ""


def test_local_sql_catalog_binds_the_local_uri_file_io(tmp_path: Path) -> None:
    cfg = parse_iceberg_catalog_config(
        {
            "connection_string": f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
            "database": str(tmp_path / "wh"),
            "table": "events",
            "schema": "sales",
        }
    )
    props = cfg["properties"]
    assert str(props["warehouse"]).startswith("file://")
    assert props[PY_IO_IMPL].endswith("LocalUriPyArrowFileIO")


def test_remote_warehouse_uris_are_left_alone(tmp_path: Path) -> None:
    cfg = parse_iceberg_catalog_config(
        {
            "connection_string": f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
            "database": "s3://bucket/warehouse",
            "table": "events",
        }
    )
    props = cfg["properties"]
    assert props["warehouse"] == "s3://bucket/warehouse"
    assert PY_IO_IMPL not in props


def _write(tmp_path: Path, *, schema: str, table: str) -> object:
    return write_mapped_rows(
        host="",
        database=str(tmp_path / "wh"),
        username="",
        password="",
        schema=schema,
        table_name=table,
        headers=["id"],
        data_rows=[("1",), ("2",)],
        mappings=[{"source": "id", "target": "id", "transform": "direct"}],
        column_types={"id": "string"},
        connection_string=str(tmp_path / "wh"),
    )


def test_filesystem_write_reports_the_namespace_not_the_table_directory(
    tmp_path: Path,
) -> None:
    result = _write(tmp_path, schema="sales", table="orders")
    assert result.ok, result.error
    assert result.target_schema == "sales"
    assert (tmp_path / "wh" / "sales" / "orders" / "metadata").is_dir()


def test_dotted_table_reports_its_leading_namespace(tmp_path: Path) -> None:
    result = _write(tmp_path, schema="", table="sales.orders")
    assert result.ok, result.error
    assert result.target_schema == "sales"
    assert result.table_name == "orders"


def test_bare_table_reports_no_namespace(tmp_path: Path) -> None:
    result = _write(tmp_path, schema="", table="orders")
    assert result.ok, result.error
    assert result.target_schema == ""
