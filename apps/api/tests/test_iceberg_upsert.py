"""Iceberg CoW upsert with _df_lsn guard."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from connectors.iceberg_writer import _merge_upsert_rows, _write_data_file, write_mapped_rows


def test_merge_upsert_later_lsn_wins() -> None:
    existing = [{"id": "1", "v": "old", "_df_lsn": "0/100"}]
    incoming = [{"id": "1", "v": "new", "_df_lsn": "0/200"}]
    merged = _merge_upsert_rows(existing, incoming, pk_cols=["id"])
    assert len(merged) == 1
    assert merged[0]["v"] == "new"


def test_merge_upsert_earlier_lsn_discarded() -> None:
    existing = [{"id": "1", "v": "keep", "_df_lsn": "0/300"}]
    incoming = [{"id": "1", "v": "stale", "_df_lsn": "0/100"}]
    merged = _merge_upsert_rows(existing, incoming, pk_cols=["id"])
    assert merged[0]["v"] == "keep"


def test_iceberg_parquet_preserves_decimal_arrow_type(tmp_path: Path) -> None:
    """Declared DECIMAL must land as decimal128 in Parquet — not float64 inference."""
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    data_dir = tmp_path / "tbl" / "data"
    rel, n, _chk, _warnings = _write_data_file(
        data_dir,
        ["id", "amt"],
        [{"id": "1", "amt": Decimal("10.5000")}, {"id": "2", "amt": Decimal("0.0001")}],
        column_types={"id": "TEXT", "amt": "DECIMAL(12,4)"},
    )
    assert n == 2
    assert rel.endswith(".parquet")
    table = pq.read_table(tmp_path / "tbl" / rel)
    assert pa.types.is_decimal(table.schema.field("amt").type)
    vals = [str(x) for x in table.column("amt").to_pylist()]
    assert "10.5000" in vals[0] or vals[0].startswith("10.5")


def test_iceberg_upsert_requires_explicit_pk(tmp_path: Path) -> None:
    warehouse = str(tmp_path / "wh")
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    result = write_mapped_rows(
        connection_string=warehouse,
        table_name="orders",
        headers=["id", "v"],
        data_rows=[["1", "a"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=[],
    )
    assert result.ok is False
    assert "conflict_columns" in (result.error or "").lower()


def test_iceberg_upsert_write_roundtrip(tmp_path: Path) -> None:
    warehouse = str(tmp_path / "wh")
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
        {"source": "_df_lsn", "target": "_df_lsn", "transform": "direct"},
    ]
    r1 = write_mapped_rows(
        connection_string=warehouse,
        table_name="orders",
        headers=["id", "v", "_df_lsn"],
        data_rows=[["1", "a", "0/10"], ["2", "b", "0/10"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r1.ok
    assert r1.rows_written == 2

    r2 = write_mapped_rows(
        connection_string=warehouse,
        table_name="orders",
        headers=["id", "v", "_df_lsn"],
        data_rows=[["1", "a2", "0/20"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r2.ok
    assert r2.rows_written == 2  # CoW full rewrite: id1 updated + id2 kept

    from connectors.iceberg_writer import _load_existing_rows, _load_metadata

    table_dir = Path(warehouse) / "orders"
    versions = sorted((table_dir / "metadata").glob("v*.metadata.json"))
    meta = _load_metadata(versions[-1])
    rows = _load_existing_rows(table_dir, ["id", "v", "_df_lsn"], meta)
    by_id = {str(r["id"]): r for r in rows}
    assert by_id["1"]["v"] == "a2"
    assert by_id["2"]["v"] == "b"


def _make_sql_catalog(tmp_path: Path) -> tuple[str, str, str]:
    """Return (warehouse, connection_string, table) for a temporary SqlCatalog."""
    pytest.importorskip("pyiceberg")
    warehouse = str(tmp_path / "wh")
    catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    return warehouse, catalog_uri, "default.orders"


def test_iceberg_sql_catalog_append_and_read(tmp_path: Path) -> None:
    """Append rows into a real pyiceberg SqlCatalog and read them back."""
    warehouse, uri, table = _make_sql_catalog(tmp_path)
    from connectors.iceberg_reader import read_table_batch
    from connectors.iceberg_writer import write_mapped_rows

    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    r = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name=table,
        headers=["id", "v"],
        data_rows=[["1", "a"], ["2", "b"]],
        mappings=mappings,
        write_mode="append",
        create_table=True,
    )
    assert r.ok
    assert r.rows_written == 2

    batch = read_table_batch(
        cfg={
            "connection_string": uri,
            "warehouse": warehouse,
            "table": "orders",
            "schema": "default",
            "type": "iceberg",
        },
        table="orders",
        limit=1000,
    )
    by_id = {row[0]: row[1] for row in batch.rows}
    assert by_id == {"1": "a", "2": "b"}


def test_iceberg_sql_catalog_upsert_merge(tmp_path: Path) -> None:
    """Real MERGE semantics: existing id is updated, new id is inserted."""
    warehouse, uri, table = _make_sql_catalog(tmp_path)
    from connectors.iceberg_reader import read_table_batch
    from connectors.iceberg_writer import write_mapped_rows

    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name=table,
        headers=["id", "v"],
        data_rows=[["1", "a"], ["2", "b"]],
        mappings=mappings,
        write_mode="append",
        create_table=True,
    )
    r = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name=table,
        headers=["id", "v"],
        data_rows=[["1", "A"], ["3", "c"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r.ok
    assert r.rows_written == 2

    batch = read_table_batch(
        cfg={
            "connection_string": uri,
            "warehouse": warehouse,
            "table": "orders",
            "schema": "default",
            "type": "iceberg",
        },
        table="orders",
        limit=1000,
    )
    by_id = {row[0]: row[1] for row in batch.rows}
    assert by_id == {"1": "A", "2": "b", "3": "c"}


def test_iceberg_sql_catalog_overwrite(tmp_path: Path) -> None:
    """Overwrite replaces all prior data."""
    warehouse, uri, table = _make_sql_catalog(tmp_path)
    from connectors.iceberg_reader import read_table_batch
    from connectors.iceberg_writer import write_mapped_rows

    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
    ]
    write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name=table,
        headers=["id"],
        data_rows=[["1"], ["2"]],
        mappings=mappings,
        write_mode="append",
        create_table=True,
    )
    r = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name=table,
        headers=["id"],
        data_rows=[["9"]],
        mappings=mappings,
        write_mode="overwrite",
    )
    assert r.ok
    assert r.rows_written == 1

    batch = read_table_batch(
        cfg={
            "connection_string": uri,
            "warehouse": warehouse,
            "table": "orders",
            "schema": "default",
            "type": "iceberg",
        },
        table="orders",
        limit=1000,
    )
    assert [row[0] for row in batch.rows] == ["9"]


def test_iceberg_sql_catalog_schema_evolution(tmp_path: Path) -> None:
    """Additive column changes are applied via pyiceberg union_by_name."""
    warehouse, uri, table = _make_sql_catalog(tmp_path)
    from connectors.iceberg_reader import read_table_batch
    from connectors.iceberg_writer import write_mapped_rows

    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name=table,
        headers=["id", "v"],
        data_rows=[["1", "a"]],
        mappings=mappings,
        write_mode="append",
        create_table=True,
    )
    mappings2 = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
        {"source": "w", "target": "w", "transform": "direct"},
    ]
    r = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name=table,
        headers=["id", "v", "w"],
        data_rows=[["2", "b", "x"]],
        mappings=mappings2,
        write_mode="append",
    )
    assert r.ok

    batch = read_table_batch(
        cfg={
            "connection_string": uri,
            "warehouse": warehouse,
            "table": "orders",
            "schema": "default",
            "type": "iceberg",
        },
        table="orders",
        limit=1000,
    )
    headers = batch.headers
    assert "w" in headers
    rows = {row[headers.index("id")]: row for row in batch.rows}
    assert rows["1"][headers.index("w")] == ""
    assert rows["2"][headers.index("w")] == "x"
