"""Gate-8 Iceberg COUNT must address namespace / table, not warehouse / table.

SKU and cartesian bind filesystem CoW with schema=default. Writer lands
rows at warehouse/default/<table>. Counting with an empty schema reports
dest=0 after a correct write (records_transferred=0, dest_count=2).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from connectors.iceberg_writer import write_mapped_rows
from services.reconciliation import verify_iceberg_table, verify_target


def _write_namespaced(warehouse: Path, table: str = "orders") -> None:
    result = write_mapped_rows(
        database=str(warehouse),
        warehouse=str(warehouse),
        schema="default",
        table_name=table,
        headers=["id", "amount"],
        data_rows=[["1", "1000.00"], ["2", "2000.50"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ],
        column_types={"id": "INTEGER", "amount": "DECIMAL(18,2)"},
        create_table=True,
        write_mode="overwrite",
    )
    assert result.ok, result.error
    assert result.rows_written == 2


def test_verify_iceberg_table_counts_namespaced_filesystem_write(tmp_path: Path) -> None:
    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    _write_namespaced(warehouse)

    count, _chk = verify_iceberg_table(
        warehouse=str(warehouse),
        database=str(warehouse),
        table_name="orders",
        schema="default",
    )
    assert count == 2


def test_verify_target_forwards_iceberg_schema_and_database(tmp_path: Path) -> None:
    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    _write_namespaced(warehouse)

    dest = {
        "database": str(warehouse),
        "warehouse": "",
        "schema": "default",
        "type": "iceberg",
    }
    count, _chk = verify_target(
        "iceberg",
        dest,
        schema="default",
        table_name="orders",
        fallback_rows=-1,
        fallback_checksum="",
    )
    assert count == 2


def test_verify_target_routes_iceberg_schema_into_verify_iceberg_table() -> None:
    with patch(
        "services.reconciliation.verify_iceberg_table",
        return_value=(2, "abc"),
    ) as mock_v:
        count, chk = verify_target(
            "iceberg",
            {
                "database": "/tmp/wh",
                "warehouse": "",
                "schema": "default",
                "extra": {"catalog_type": "filesystem"},
            },
            schema="default",
            table_name="orders",
            fallback_rows=0,
            fallback_checksum="",
        )
    assert (count, chk) == (2, "abc")
    kwargs = mock_v.call_args.kwargs
    assert kwargs["schema"] == "default"
    assert kwargs["database"] == "/tmp/wh"
    assert kwargs["warehouse"] == "/tmp/wh"
    assert kwargs["table_name"] == "orders"
    assert kwargs["extra"] == {"catalog_type": "filesystem"}
