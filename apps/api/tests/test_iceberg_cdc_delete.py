"""Iceberg CDC deletes: v2 equality-delete MoR + LSN guard (at-least-once)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

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

    deletes = list(meta.get("delete-files") or [])
    assert deletes, "CDC delete must write an equality-delete file, not CoW-rewrite"
    assert deletes[-1].get("content") == 2
    assert deletes[-1].get("equality-ids") == [1]
    assert deletes[-1].get("sequence-number") is not None
    data_refs = list(meta.get("data-files") or [])
    assert data_refs
    assert all(ref.get("sequence-number") is not None for ref in data_refs)
    assert (meta.get("properties") or {}).get("dataflow.write_strategy") == (
        "merge-on-read"
    )
    from services.dest_precount import destination_row_count

    assert destination_row_count("iceberg", _cfg(tmp_path), schema="", table_name=table) == 1


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


def test_iceberg_composite_delete_honors_lsn_and_row_identity(tmp_path: Path) -> None:
    """Composite CoW delete uses unit-separator identity, not a joined column name."""
    from services.cdc_snapshot_window import _PK_SEP

    table = "lines"
    common = {
        **_cfg(tmp_path),
        "table_name": table,
        "headers": ["order_id", "line_id", "note", DF_LSN_COL],
        "mappings": [
            {"source": "order_id", "target": "order_id", "confidence": 1},
            {"source": "line_id", "target": "line_id", "confidence": 1},
            {"source": "note", "target": "note", "confidence": 1},
            {"source": DF_LSN_COL, "target": DF_LSN_COL, "confidence": 1},
        ],
        "column_types": {
            "order_id": "string",
            "line_id": "string",
            "note": "string",
            DF_LSN_COL: "string",
        },
        "create_table": True,
        "write_mode": "upsert",
        "conflict_columns": ["order_id", "line_id"],
    }
    r1 = write_mapped_rows(
        **common,
        data_rows=[
            ["1", "1", "keep", "0/200"],
            ["1", "2", "gone", "0/100"],
        ],
    )
    assert r1.ok, r1.error

    stale = delete_by_primary_keys(
        _cfg(tmp_path),
        table,
        ["order_id", "line_id"],
        [f"1{_PK_SEP}1"],
        incoming_lsn="0/100",
        lsn_column=DF_LSN_COL,
    )
    assert stale == 0

    deleted = delete_by_primary_keys(
        _cfg(tmp_path),
        table,
        ["order_id", "line_id"],
        [f"1{_PK_SEP}2"],
        incoming_lsn="0/150",
        lsn_column=DF_LSN_COL,
    )
    assert deleted == 1

    from connectors.iceberg_writer import _load_existing_rows, _load_metadata, _resolve_iceberg_table_dir

    table_dir = _resolve_iceberg_table_dir(_cfg(tmp_path), table, None)
    versions = sorted((table_dir / "metadata").glob("v*.metadata.json"))
    meta = _load_metadata(versions[-1])
    rows = _load_existing_rows(
        table_dir, ["order_id", "line_id", "note", DF_LSN_COL], meta
    )
    pairs = {(str(r.get("order_id")), str(r.get("line_id"))) for r in rows}
    assert ("1", "1") in pairs
    assert ("1", "2") not in pairs


def test_iceberg_delete_predicate_composite_is_and_or_not_joined_column() -> None:
    """Catalog leftover apply must not treat ``order_id,line_id`` as one field."""
    from types import SimpleNamespace

    from connectors.iceberg_writer import (
        _iceberg_delete_predicate,
        _iceberg_typed_literal,
    )
    from pyiceberg.expressions import In, Or
    from pyiceberg.types import LongType, StringType
    from services.cdc_snapshot_window import _PK_SEP

    class _Schema:
        def __init__(self, fields: dict[str, object]) -> None:
            self._fields = fields

        def find_field(self, name: str, case_sensitive: bool = True) -> object:
            key = name if case_sensitive else name.lower()
            lookup = {k.lower(): v for k, v in self._fields.items()}
            return lookup[key]

    class _Table:
        def __init__(self, fields: dict[str, object]) -> None:
            self._schema = _Schema(fields)

        def schema(self) -> _Schema:
            return self._schema

    string_tbl = _Table(
        {"id": SimpleNamespace(field_type=StringType())}
    )
    assert _iceberg_typed_literal(string_tbl, "id", "99") == "99"
    pred_in = _iceberg_delete_predicate(string_tbl, ["id"], {"99", "1"})
    assert isinstance(pred_in, In)

    long_tbl = _Table(
        {
            "order_id": SimpleNamespace(field_type=LongType()),
            "line_id": SimpleNamespace(field_type=LongType()),
        }
    )
    assert _iceberg_typed_literal(long_tbl, "order_id", "9") == 9
    pred = _iceberg_delete_predicate(
        long_tbl,
        ["order_id", "line_id"],
        {f"9{_PK_SEP}9", f"1{_PK_SEP}2"},
    )
    assert isinstance(pred, Or)
    dumped = str(pred)
    assert "order_id,line_id" not in dumped
    assert "order_id" in dumped and "line_id" in dumped
    with pytest.raises(ValueError, match="arity"):
        _iceberg_delete_predicate(long_tbl, ["order_id", "line_id"], {"9,9"})


def test_resolve_projected_names_requires_every_pk_part() -> None:
    from services.dest_precount import _resolve_projected_names

    assert _resolve_projected_names({"Order_Id", "Line_Id", "qty"}, ["order_id", "line_id"]) == [
        ("order_id", "Order_Id"),
        ("line_id", "Line_Id"),
    ]
    assert _resolve_projected_names({"order_id", "qty"}, ["order_id", "line_id"]) is None
    assert _resolve_projected_names({"id"}, ["id", "id"]) is None


def test_iceberg_upsert_after_mor_delete_does_not_resurrect(tmp_path: Path) -> None:
    """MoR upsert must not resurrect a previously equality-deleted key."""
    from services.dest_precount import destination_row_count

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
    assert write_mapped_rows(
        **common, data_rows=[["1", "keep", "0/200"], ["2", "gone", "0/100"]]
    ).ok
    assert delete_by_primary_keys(_cfg(tmp_path), table, "id", ["2"]) == 1
    assert destination_row_count("iceberg", _cfg(tmp_path), schema="", table_name=table) == 1

    r2 = write_mapped_rows(**common, data_rows=[["1", "keep2", "0/300"]])
    assert r2.ok, r2.error

    from connectors.iceberg_writer import (
        _load_existing_rows,
        _load_metadata,
        _resolve_iceberg_table_dir,
    )

    table_dir = _resolve_iceberg_table_dir(_cfg(tmp_path), table, None)
    versions = sorted((table_dir / "metadata").glob("v*.metadata.json"))
    meta = _load_metadata(versions[-1])
    deletes = list(meta.get("delete-files") or [])
    assert deletes, "MoR upsert keeps prior equality deletes and adds update deletes"
    assert (meta.get("properties") or {}).get("dataflow.write_strategy") == (
        "merge-on-read"
    )
    rows = _load_existing_rows(table_dir, ["id", "note", DF_LSN_COL], meta)
    ids = {str(r.get("id")) for r in rows}
    assert ids == {"1"}
    keep = next(r for r in rows if str(r.get("id")) == "1")
    assert keep.get("note") == "keep2"
    assert destination_row_count("iceberg", _cfg(tmp_path), schema="", table_name=table) == 1


def test_iceberg_upsert_reinserts_deleted_key_after_mor_compact(tmp_path: Path) -> None:
    """A later CDC insert of a deleted PK is a new row, not a phantom undelete."""
    from services.dest_precount import destination_row_count

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
    assert write_mapped_rows(
        **common, data_rows=[["1", "keep", "0/200"], ["2", "gone", "0/100"]]
    ).ok
    assert delete_by_primary_keys(_cfg(tmp_path), table, "id", ["2"]) == 1
    r2 = write_mapped_rows(**common, data_rows=[["2", "reborn", "0/400"]])
    assert r2.ok, r2.error
    from connectors.iceberg_writer import (
        _load_existing_rows,
        _load_metadata,
        _resolve_iceberg_table_dir,
    )

    table_dir = _resolve_iceberg_table_dir(_cfg(tmp_path), table, None)
    versions = sorted((table_dir / "metadata").glob("v*.metadata.json"))
    meta = _load_metadata(versions[-1])
    rows = _load_existing_rows(table_dir, ["id", "note"], meta)
    ids = {str(r.get("id")) for r in rows}
    assert ids == {"1", "2"}
    reborn = next(r for r in rows if str(r.get("id")) == "2")
    assert reborn.get("note") == "reborn"
    assert destination_row_count("iceberg", _cfg(tmp_path), schema="", table_name=table) == 2
