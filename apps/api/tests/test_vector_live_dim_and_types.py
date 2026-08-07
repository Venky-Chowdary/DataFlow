"""Vector write honesty — live types overlay + Qdrant/Pinecone/Milvus dim fail-closed."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_prepare_records_overlays_destination_column_types():
    from connectors.writer_common import prepare_records_for_vector_write

    records, rejected, abort = prepare_records_for_vector_write(
        headers=["id", "qty"],
        data_rows=[["1", "7"], ["2", "x"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
        ],
        column_types={"id": "VARCHAR", "qty": "VARCHAR"},
        error_policy="quarantine",
        dest_kind="qdrant",
        label="qdrant",
        destination_column_types={"qty": "INTEGER", "id": "VARCHAR"},
    )
    assert abort is None
    # Unfit "x" should quarantine under live INTEGER, not pass as VARCHAR string.
    assert any((d.get("column") or "").lower() == "qty" for d in rejected) or len(
        records
    ) == 1


def test_weaviate_existing_class_partial_schema_refuses():
    """Existing Weaviate class with incomplete properties → require_physical refuse."""
    from unittest.mock import MagicMock, patch

    from connectors.weaviate_writer import write_mapped_rows

    class_resp = MagicMock()
    class_resp.status_code = 200
    class_resp.json.return_value = {
        "class": "Orders",
        "properties": [{"name": "id", "dataType": ["text"]}],
    }
    session = MagicMock()
    session.get.return_value = class_resp

    with (
        patch("connectors.weaviate_writer._requests_session", return_value=session),
        patch("connectors.weaviate_writer._headers", return_value={}),
        patch("connectors.weaviate_writer._base_url", return_value="http://localhost:8080"),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=8080,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="Orders",
            headers=["id", "qty"],
            data_rows=[["1", "7"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "qty": "VARCHAR"},
            api_key="key",
            create_table=True,
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "qty" in (result.error or "").lower()
    assert "refuse" in (result.error or "").lower()


def test_qdrant_live_vector_size_unnamed():
    from connectors.qdrant_writer import _qdrant_live_vector_size

    assert (
        _qdrant_live_vector_size(
            {
                "result": {
                    "config": {
                        "params": {"vectors": {"size": 384, "distance": "Cosine"}}
                    }
                }
            }
        )
        == 384
    )


def test_qdrant_live_vector_size_named():
    from connectors.qdrant_writer import _qdrant_live_vector_size

    assert (
        _qdrant_live_vector_size(
            {
                "result": {
                    "config": {
                        "params": {
                            "vectors": {
                                "default": {"size": 768, "distance": "Cosine"}
                            }
                        }
                    }
                }
            }
        )
        == 768
    )


def test_pinecone_live_dimension():
    from connectors.pinecone_writer import _pinecone_live_dimension

    assert _pinecone_live_dimension({"dimension": 1536, "totalVectorCount": 0}) == 1536
    assert _pinecone_live_dimension({"dimension": "0"}) is None
    assert _pinecone_live_dimension({}) is None


def test_milvus_live_vector_dim_from_describe():
    from connectors.milvus_writer import _milvus_live_vector_dim

    session = MagicMock()
    session.post.return_value.status_code = 200
    session.post.return_value.content = b"{}"
    session.post.return_value.json.return_value = {
        "code": 0,
        "data": {
            "fields": [
                {"fieldName": "id", "dataType": "VarChar"},
                {
                    "fieldName": "vector",
                    "dataType": "FloatVector",
                    "elementTypeParams": {"dim": "768"},
                },
            ]
        },
    }
    assert (
        _milvus_live_vector_dim(session, "http://localhost:19530", {}, "chunks") == 768
    )


def test_milvus_existing_collection_partial_schema_refuses():
    """Existing Milvus collection missing a mapped field carrier → require_physical."""
    from connectors.milvus_writer import write_mapped_rows

    session = MagicMock()

    def _post(url, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"{}"
        if "collections/has" in str(url):
            resp.json.return_value = {"code": 0, "data": {"has": True}}
        else:
            # Describe returns only id — chunk_index exists on create schema but
            # incomplete describe must refuse Map bind for that mapped field.
            resp.json.return_value = {
                "code": 0,
                "data": {
                    "fields": [
                        {"fieldName": "id", "dataType": "VarChar"},
                        {
                            "fieldName": "vector",
                            "dataType": "FloatVector",
                            "elementTypeParams": {"dim": 3},
                        },
                        {"fieldName": "chunk_index", "dataType": ""},
                    ]
                },
            }
        return resp

    session.post.side_effect = _post

    with patch("connectors.milvus_writer._requests_session", return_value=session):
        result = write_mapped_rows(
            host="localhost",
            port=19530,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="chunks",
            headers=["id", "chunk_index"],
            data_rows=[["1", "0"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {
                    "source": "chunk_index",
                    "target": "chunk_index",
                    "target_type": "VARCHAR",
                },
            ],
            column_types={"id": "VARCHAR", "chunk_index": "VARCHAR"},
            api_key="key",
            create_table=True,
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "chunk_index" in (result.error or "").lower()
    assert "refuse" in (result.error or "").lower()


def test_pgvector_existing_table_partial_physical_refuses():
    """Existing pgvector table with incomplete live DDL → require_physical refuse."""
    from connectors.pgvector_writer import write_mapped_rows

    cur = MagicMock()
    # to_regclass → table exists
    cur.fetchone.return_value = ("public.chunks",)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with (
        patch("connectors.pgvector_writer.get_connection", return_value=conn),
        patch(
            "connectors.postgresql_writer._fetch_pg_column_types",
            return_value={"id": "TEXT"},  # chunk_index missing → invent cliff
        ),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=5432,
            database="db",
            username="u",
            password="p",
            schema="public",
            connection_string="",
            ssl=False,
            table_name="chunks",
            headers=["id", "chunk_index"],
            data_rows=[["1", "0"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {
                    "source": "chunk_index",
                    "target": "chunk_index",
                    "target_type": "VARCHAR",
                },
            ],
            column_types={"id": "VARCHAR", "chunk_index": "VARCHAR"},
            create_table=True,
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "chunk_index" in (result.error or "").lower()
    assert "refuse" in (result.error or "").lower()


def test_weaviate_live_property_types():
    from connectors.weaviate_writer import _weaviate_live_property_types

    physical = _weaviate_live_property_types(
        {
            "class": "Chunk",
            "properties": [
                {"name": "content", "dataType": ["text"]},
                {"name": "chunk_index", "dataType": ["int"]},
                {"name": "score", "dataType": ["number"]},
                {"name": "flag", "dataType": ["boolean"]},
            ],
        }
    )
    assert physical.get("content") == "TEXT"
    assert physical.get("chunk_index") == "INTEGER"
    assert physical.get("score") == "FLOAT"
    assert physical.get("flag") == "BOOLEAN"


def test_hubspot_describe_failure_refuses_map_only():
    from connectors.hubspot_writer import write_mapped_rows

    with patch(
        "connectors.hubspot.describe_properties",
        side_effect=RuntimeError("timeout"),
    ):
        result = write_mapped_rows(
            host="api.hubapi.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="contacts",
            headers=["email", "qty"],
            data_rows=[["a@b.com", "1"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"},
                {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            ],
            column_types={"email": "VARCHAR", "qty": "VARCHAR"},
            api_key="pat-xxx",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "describe unavailable" in (result.error or "").lower()


def test_hubspot_describe_auth_failure_fail_closed():
    from connectors.hubspot_writer import write_mapped_rows

    with patch(
        "connectors.hubspot.describe_properties",
        side_effect=Exception("401 Unauthorized"),
    ):
        result = write_mapped_rows(
            host="api.hubapi.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="contacts",
            headers=["email"],
            data_rows=[["a@b.com"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"}
            ],
            column_types={"email": "VARCHAR"},
            destination_column_types={"email": "VARCHAR"},
            api_key="pat-xxx",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "auth" in (result.error or "").lower()


def test_weaviate_schema_auth_refuses_map_bind():
    from connectors.weaviate_writer import write_mapped_rows

    session = MagicMock()
    resp = MagicMock()
    resp.status_code = 401
    session.get.return_value = resp
    with patch("connectors.weaviate_writer._requests_session", return_value=session):
        result = write_mapped_rows(
            host="localhost",
            port=8080,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="Chunk",
            headers=["content"],
            data_rows=[["hello"]],
            mappings=[
                {"source": "content", "target": "content", "target_type": "VARCHAR"}
            ],
            column_types={"content": "VARCHAR"},
            api_key="key",
            create_table=True,
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "auth" in (result.error or "").lower()
