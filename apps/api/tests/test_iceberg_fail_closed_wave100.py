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
