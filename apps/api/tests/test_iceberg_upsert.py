"""Iceberg CoW upsert with _df_lsn guard."""

from __future__ import annotations

from pathlib import Path

from connectors.iceberg_writer import _merge_upsert_rows, write_mapped_rows


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
