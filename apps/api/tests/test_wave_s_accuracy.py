"""Wave S accuracy: SaaS pagination defaults, deny-create, catalog honesty."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_airtable_shopify_zendesk_pagination_defaults():
    from connectors.rest_api import _resolve_config

    airtable = _resolve_config({"type": "airtable", "host": "https://api.airtable.com", "table": "tbl"})
    assert airtable["pagination_type"] == "cursor"
    assert airtable["cursor_param"] == "offset"
    assert airtable["next_path"] == "offset"
    assert airtable["data_path"] == "records"

    shopify = _resolve_config({"type": "shopify", "host": "https://shop.myshopify.com"})
    assert shopify["pagination_type"] == "link"

    zendesk = _resolve_config(
        {"type": "zendesk", "host": "https://acme.zendesk.com", "table": "api/v2/tickets"}
    )
    assert zendesk["pagination_type"] == "link"
    assert zendesk["data_path"] == "tickets"


def test_airtable_cursor_uses_offset_token():
    from connectors.rest_api import read_object

    seen: list[dict] = []

    def fake_page(cfg, pagination, next_url=None):
        seen.append(dict(pagination))
        if "offset" not in pagination or pagination.get("offset") in (None, ""):
            return [{"id": "1"}], "offset_token_2", True
        return [{"id": "2"}], None, False

    with patch("connectors.rest_api._read_page", side_effect=fake_page):
        batch = read_object(
            cfg={"type": "airtable", "host": "https://api.airtable.com", "api_key": "key"},
            object="Base/Table",
            limit=50,
        )
    assert len(batch.rows) == 2
    assert seen[1].get("offset") == "offset_token_2"


def test_generic_sql_create_table_false_missing_table():
    from connectors.generic_sql import write_mapped_rows

    inspector = MagicMock()
    inspector.has_table.return_value = False
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = conn
    engine.dialect = MagicMock(name="postgresql")

    with patch("connectors.generic_sql._engine", return_value=engine), patch(
        "connectors.generic_sql.inspect", return_value=inspector
    ), patch(
        "connectors.generic_sql._cfg_from_params",
        return_value={"type": "clickhouse", "database": "db"},
    ):
        result = write_mapped_rows(
            host="localhost",
            port=8123,
            database="db",
            username="default",
            password="",
            schema="default",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            type="clickhouse",
            create_table=False,
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    conn.execute.assert_not_called()


def test_weaviate_create_table_false_missing_class():
    from connectors.weaviate_writer import write_mapped_rows

    session = MagicMock()
    session.get.return_value = MagicMock(status_code=404)
    with patch(
        "connectors.weaviate_writer.vectorize_records",
        return_value=[{"content": "hi", "embedding": [0.1, 0.2], "source_id": "1", "chunk_index": 0}],
    ), patch(
        "connectors.weaviate_writer.build_weaviate_objects",
        return_value=([{"class": "Doc", "properties": {}}], []),
    ), patch(
        "connectors.weaviate_writer._requests_session",
        return_value=session,
    ), patch(
        "services.vector_embedding.resolve_embedding_dimension",
        return_value=(2, None),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=8080,
            database="",
            username="",
            password="key",
            schema="",
            connection_string="",
            ssl=False,
            table_name="Doc",
            headers=["id", "content"],
            data_rows=[["1", "hi"]],
            mappings=[],
            column_types={},
            create_table=False,
            embedding_model="hash/32",
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    session.post.assert_not_called()


def test_milvus_create_table_false_missing_collection():
    from connectors.milvus_writer import write_mapped_rows

    session = MagicMock()
    with patch(
        "connectors.milvus_writer.vectorize_records",
        return_value=[{"content": "hi", "embedding": [0.1, 0.2], "source_id": "1", "chunk_index": 0}],
    ), patch(
        "connectors.milvus_writer.build_milvus_entities",
        return_value=([{"id": "1", "vector": [0.1, 0.2]}], []),
    ), patch(
        "connectors.milvus_writer._has_collection",
        return_value=False,
    ), patch(
        "connectors.milvus_writer._requests_session",
        return_value=session,
    ), patch(
        "connectors.milvus_writer._ensure_collection",
    ) as ensure, patch(
        "services.vector_embedding.resolve_embedding_dimension",
        return_value=(2, None),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=19530,
            database="default",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="chunks",
            headers=["id", "content"],
            data_rows=[["1", "hi"]],
            mappings=[],
            column_types={},
            create_table=False,
            embedding_model="hash/32",
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    ensure.assert_not_called()
    session.post.assert_not_called()


def test_qdrant_create_table_false_missing_collection():
    from connectors.qdrant_writer import write_mapped_rows

    session = MagicMock()
    session.get.return_value = MagicMock(status_code=404)
    with patch(
        "connectors.qdrant_writer.vectorize_records",
        return_value=[{"content": "hi", "embedding": [0.1, 0.2], "source_id": "1", "chunk_index": 0}],
    ), patch(
        "connectors.qdrant_writer.build_qdrant_points",
        return_value=([{"id": "1", "vector": [0.1, 0.2], "payload": {}}], []),
    ), patch(
        "connectors.qdrant_writer._requests_session",
        return_value=session,
    ), patch(
        "connectors.qdrant_writer._ensure_collection",
    ) as ensure, patch(
        "services.vector_embedding.resolve_embedding_dimension",
        return_value=(2, None),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=6333,
            database="",
            username="",
            password="key",
            schema="",
            connection_string="",
            ssl=False,
            table_name="chunks",
            headers=["id", "content"],
            data_rows=[["1", "hi"]],
            mappings=[],
            column_types={},
            create_table=False,
            embedding_model="hash/32",
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    ensure.assert_not_called()
    session.put.assert_not_called()


def test_feather_arrow_ipc_not_claimed_structured():
    from services.connector_capability_registry import (
        STRUCTURED_FILE_FORMATS,
        UNIMPLEMENTED_FILE_FORMATS,
        classify_payload,
    )

    assert "feather" not in STRUCTURED_FILE_FORMATS
    assert "arrow" not in STRUCTURED_FILE_FORMATS
    assert "ipc" not in STRUCTURED_FILE_FORMATS
    assert UNIMPLEMENTED_FILE_FORMATS >= {"feather", "arrow", "ipc", "protobuf"}
    assert "protobuf" not in STRUCTURED_FILE_FORMATS
    assert "orc" in STRUCTURED_FILE_FORMATS
    assert classify_payload(source_format="feather")["shape"] != "structured"
    assert classify_payload(source_format="protobuf")["shape"] != "structured"
    assert classify_payload(source_format="parquet")["shape"] == "structured"
