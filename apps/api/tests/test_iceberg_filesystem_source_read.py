"""Filesystem CoW Iceberg must be readable as a source without SqlCatalog.

Cartesian iceberg→* used to fail peek with ``SQL connection URI is required``
because ``load_catalog`` fell through to SqlCatalog. Writer/COUNT already use
the parquet snapshot; the reader must too. Peek passes ``columns=None``, so
the reader has to discover schema names — dest COUNT projection of empty
``cols`` is an empty list, not every column.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from connectors.iceberg_catalog import load_catalog
from connectors.iceberg_reader import read_table_batch
from connectors.iceberg_writer import write_mapped_rows


def _write_two_rows(warehouse: Path, table: str = "orders") -> None:
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
        extra={"catalog_type": "filesystem"},
    )
    assert result.ok, result.error
    assert result.rows_written == 2


def test_load_catalog_refuses_sqlcatalog_for_filesystem(tmp_path: Path) -> None:
    endpoint = {
        "database": str(tmp_path / "wh"),
        "warehouse": str(tmp_path / "wh"),
        "schema": "default",
        "table": "orders",
        "extra": {"catalog_type": "filesystem"},
    }
    with pytest.raises(RuntimeError, match="filesystem CoW|SQL connection URI invent"):
        load_catalog(endpoint)


def test_filesystem_source_read_returns_two_rows_without_sqlcatalog(
    tmp_path: Path,
) -> None:
    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    _write_two_rows(warehouse)

    cfg = {
        "database": str(warehouse),
        "warehouse": str(warehouse),
        "schema": "default",
        "table": "orders",
        "type": "iceberg",
        "extra": {"catalog_type": "filesystem"},
    }
    with patch("connectors.iceberg_catalog.load_catalog") as mocked:
        mocked.side_effect = AssertionError("SqlCatalog must not load for filesystem CoW")
        batch = read_table_batch(cfg=cfg, table="orders", limit=1000)

    assert "id" in batch.headers
    assert "amount" in batch.headers
    id_idx = batch.headers.index("id")
    amt_idx = batch.headers.index("amount")
    by_id = {row[id_idx]: row[amt_idx] for row in batch.rows}
    assert set(by_id) == {"1", "2"}
    assert "1000" in str(by_id["1"])
    assert "2000" in str(by_id["2"])
    mocked.assert_not_called()


def test_filesystem_iceberg_uniqueness_probe_runs(tmp_path: Path) -> None:
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    _write_two_rows(warehouse)
    cfg = {
        "type": "iceberg",
        "database": str(warehouse),
        "warehouse": str(warehouse),
        "schema": "default",
        "table": "orders",
        "extra": {"catalog_type": "filesystem"},
    }
    result = probe_source_duplicate_keys_result(
        source_config=cfg,
        source_table="orders",
        primary_key="id",
    )
    assert result.status == "ran", result.message
    assert result.findings == []
    assert result.ran is True
