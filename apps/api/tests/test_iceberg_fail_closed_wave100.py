"""Wave 100 L3/L4: Iceberg fail-closed catalog dispatch + sparse unknown PK."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from services.value_serializer import DF_MISSING_SENTINEL


def test_catalog_endpoint_fails_closed_without_pyiceberg(tmp_path):
    from connectors.iceberg_writer import resolve_iceberg_write_path, write_mapped_rows

    endpoint = {
        "connection_string": "https://lakehouse.example:8181",
        "table": "events",
        "schema": "default",
        "extra": {"catalog_type": "rest"},
    }
    with patch(
        "connectors.iceberg_writer._pyiceberg_available", return_value=False
    ):
        with pytest.raises(RuntimeError, match="not ready|Apache Iceberg"):
            resolve_iceberg_write_path(endpoint)

        result = write_mapped_rows(
            host="",
            port=0,
            database="",
            username="",
            password="",
            schema="default",
            connection_string="https://lakehouse.example:8181",
            ssl=False,
            table_name="events",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "string"},
            create_table=True,
            extra={"catalog_type": "rest"},
        )
    assert result.ok is False
    assert "Iceberg" in (result.error or "")
    # Must not invent a local warehouse named after the REST URL.
    phantom = tmp_path / "http:"
    assert not phantom.exists()


def test_filesystem_path_still_resolves_without_pyiceberg(tmp_path):
    from connectors.iceberg_writer import resolve_iceberg_write_path

    endpoint = {
        "connection_string": str(tmp_path / "warehouse"),
        "table": "events",
        "schema": "default",
        "extra": {"catalog_type": "filesystem"},
    }
    with patch(
        "connectors.iceberg_writer._pyiceberg_available", return_value=False
    ):
        assert resolve_iceberg_write_path(endpoint) == "filesystem"


def test_filesystem_existing_partial_physical_refuses(tmp_path):
    """Existing Iceberg field with empty type must not invent string — refuse."""
    import json
    from pathlib import Path

    from connectors.iceberg_writer import (
        _iceberg_type_to_logical_carrier,
        write_mapped_rows,
    )

    assert _iceberg_type_to_logical_carrier("") == ""
    assert _iceberg_type_to_logical_carrier({"type": ""}) == ""
    assert _iceberg_type_to_logical_carrier(None) == ""

    warehouse = Path(tmp_path) / "wh"
    table_dir = warehouse / "orders"
    meta_dir = table_dir / "metadata"
    meta_dir.mkdir(parents=True)
    schema = {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "id", "type": "string", "required": False},
            {"id": 2, "name": "qty", "type": "", "required": False},
        ],
    }
    meta = {
        "format-version": 2,
        "table-uuid": "test-uuid",
        "location": str(table_dir),
        "schemas": [schema],
        "current-schema-id": 0,
        "schema": schema,
        "snapshots": [],
    }
    (meta_dir / "v1.metadata.json").write_text(json.dumps(meta), encoding="utf-8")

    result = write_mapped_rows(
        host="",
        port=0,
        database="",
        username="",
        password="",
        schema="",
        connection_string=str(warehouse),
        ssl=False,
        table_name="orders",
        headers=["id", "qty"],
        data_rows=[["1", "7"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
        ],
        column_types={"id": "VARCHAR", "qty": "VARCHAR"},
        create_table=True,
        extra={"catalog_type": "filesystem"},
    )
    assert result.ok is False
    assert "qty" in (result.error or "").lower()
    assert "refuse" in (result.error or "").lower()


def test_sparse_unknown_pk_refused_on_filesystem_merge():
    from connectors.iceberg_writer import _merge_upsert_rows

    with pytest.raises(ValueError, match="unknown primary key"):
        _merge_upsert_rows(
            existing=[],
            incoming=[
                {
                    "id": "9",
                    "note": "only-note",
                    "extra": DF_MISSING_SENTINEL,
                    "_df_lsn": "0/1",
                }
            ],
            pk_cols=["id"],
        )


def test_sparse_known_pk_still_overlays():
    from connectors.iceberg_writer import _merge_upsert_rows

    merged = _merge_upsert_rows(
        existing=[{"id": "1", "note": "keep", "extra": "stay", "_df_lsn": "0/1"}],
        incoming=[
            {
                "id": "1",
                "note": "updated",
                "extra": DF_MISSING_SENTINEL,
                "_df_lsn": "0/2",
            }
        ],
        pk_cols=["id"],
    )
    assert merged[0]["note"] == "updated"
    assert merged[0]["extra"] == "stay"


def test_filesystem_merge_refuses_null_dense_pk():
    from connectors.iceberg_writer import _merge_upsert_rows

    with pytest.raises(ValueError, match="null/empty primary-key"):
        _merge_upsert_rows(
            existing=[],
            incoming=[{"id": None, "note": "x"}],
            pk_cols=["id"],
        )


def test_filesystem_merge_skips_existing_null_pk_no_none_string_collision():
    """None must not stringify to 'None' and collide with literal PK 'None'."""
    from connectors.iceberg_writer import _merge_upsert_rows

    merged = _merge_upsert_rows(
        existing=[{"id": None, "note": "ghost"}, {"id": "None", "note": "literal"}],
        incoming=[{"id": "None", "note": "updated"}],
        pk_cols=["id"],
    )
    by_id = {r["id"]: r for r in merged}
    assert "None" in by_id
    assert by_id["None"]["note"] == "updated"
    assert None not in by_id


def test_scan_absorb_skips_null_pk_no_none_collision():
    """Catalog PK scan must not key SQL NULL as the string 'None'."""
    from connectors.writer_common import _is_nullish_conflict_key

    # Mirror _scan_existing_by_pk absorb keying without needing a live table.
    pk_cols = ["id"]
    rows = [
        {"id": None, "extra": "ghost"},
        {"id": "None", "extra": "literal"},
    ]
    existing: dict = {}
    for row in rows:
        if any(_is_nullish_conflict_key(row.get(c)) for c in pk_cols):
            continue
        key = tuple(
            "" if _is_nullish_conflict_key(row.get(c)) else str(row.get(c))
            for c in pk_cols
        )
        existing[key] = row
    assert list(existing.keys()) == [("None",)]
    assert existing[("None",)]["extra"] == "literal"

