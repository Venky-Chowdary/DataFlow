"""Iceberg CDC deletes: CoW exclude + LSN guard (at-least-once safe)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.iceberg_writer import delete_by_primary_keys, write_mapped_rows  # noqa: E402
from connectors.writer_common import DF_LSN_COL  # noqa: E402


def _cfg(tmp_path: Path) -> dict:
    return {
        "host": "",
        "database": str(tmp_path),
        "connection_string": str(tmp_path),
        "schema": "",
    }


def test_iceberg_delete_removes_row_and_honors_newer_lsn(tmp_path: Path) -> None:
    table = "orders"
    common = {
        **_cfg(tmp_path),
        "table_name": table,
        "headers": ["id", "note", DF_LSN_COL],
        "mappings": [
            {"source": "id", "target": "id", "confidence": 1},
            {"source": "note", "target": "note", "confidence": 1},
            {"source": DF_LSN_COL, "target": DF_LSN_COL, "confidence": 1},
        ],
        "column_types": {
            "id": "string",
            "note": "string",
            DF_LSN_COL: "string",
        },
        "create_table": True,
        "write_mode": "upsert",
        "conflict_columns": ["id"],
    }
    r1 = write_mapped_rows(
        **common,
        data_rows=[["1", "keep", "0/200"], ["2", "gone", "0/100"]],
    )
    assert r1.ok, r1.error

    # Stale delete must not wipe id=1 recreated/updated at newer LSN.
    deleted_stale = delete_by_primary_keys(
        _cfg(tmp_path),
        table,
        "id",
        ["1"],
        incoming_lsn="0/100",
        lsn_column=DF_LSN_COL,
    )
    assert deleted_stale == 0

    deleted = delete_by_primary_keys(
        _cfg(tmp_path),
        table,
        "id",
        ["2"],
        incoming_lsn="0/150",
        lsn_column=DF_LSN_COL,
    )
    assert deleted == 1

    # Re-read via upsert merge path load
    from connectors.iceberg_writer import _load_existing_rows, _load_metadata, _resolve_iceberg_table_dir

    table_dir = _resolve_iceberg_table_dir(_cfg(tmp_path), table, None)
    versions = sorted((table_dir / "metadata").glob("v*.metadata.json"))
    meta = _load_metadata(versions[-1])
    rows = _load_existing_rows(table_dir, ["id", "note", DF_LSN_COL], meta)
    ids = {str(r.get("id")) for r in rows}
    assert "1" in ids
    assert "2" not in ids
    keep = next(r for r in rows if str(r.get("id")) == "1")
    assert keep.get("note") == "keep"
    assert str(keep.get(DF_LSN_COL)) == "0/200"


def test_table_manager_routes_iceberg_deletes(tmp_path: Path) -> None:
    from connectors.table_manager import delete_by_primary_keys as tm_delete

    table = "events"
    write_mapped_rows(
        host="",
        database=str(tmp_path),
        connection_string=str(tmp_path),
        schema="",
        table_name=table,
        headers=["id", "v"],
        data_rows=[["a", "1"], ["b", "2"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 1},
            {"source": "v", "target": "v", "confidence": 1},
        ],
        column_types={"id": "string", "v": "string"},
        create_table=True,
        write_mode="append",
    )
    n = tm_delete(
        "iceberg",
        _cfg(tmp_path),
        table,
        "id",
        ["a"],
    )
    assert n == 1
