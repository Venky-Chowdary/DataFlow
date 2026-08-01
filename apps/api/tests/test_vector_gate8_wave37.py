"""Wave 37: Vector destination Gate-8 meta + independent verify routing."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_vector_gate8_meta_drops_nothing_but_stamps_ids():
    from connectors.writer_common import vector_gate8_meta

    meta = vector_gate8_meta(
        [{"id": "v1", "source_id": "s1", "content": "hello"}],
    )
    assert meta["source_row_count"] == 1
    assert meta["written_ids"] == ["v1"]
    assert meta["reconcile_sample"][0]["content"] == "hello"


def test_pinecone_qdrant_weaviate_milvus_pgvector_meta_helpers():
    from connectors.milvus_writer import _milvus_gate8_meta
    from connectors.pgvector_writer import _pgvector_gate8_meta
    from connectors.pinecone_writer import _pinecone_gate8_meta
    from connectors.qdrant_writer import _qdrant_gate8_meta
    from connectors.weaviate_writer import _weaviate_gate8_meta

    assert _pinecone_gate8_meta(
        [{"id": "a", "metadata": {"k": 1}, "values": [0.1]}]
    )["written_ids"] == ["a"]
    assert _qdrant_gate8_meta(
        [{"id": "b", "payload": {"content": "x"}, "vector": [0.1]}]
    )["reconcile_sample"][0]["content"] == "x"
    assert _weaviate_gate8_meta(
        [{"id": "c", "properties": {"source_id": "s"}}]
    )["written_ids"] == ["c"]
    milvus = _milvus_gate8_meta([{"id": "d", "content": "t", "vector": [0.2, 0.3]}])
    assert "vector" not in milvus["reconcile_sample"][0]
    assert _pgvector_gate8_meta(
        [{"id": "e", "source_id": "s", "content": "c", "metadata": {"p": 1}}]
    )["written_ids"] == ["e"]


def test_verify_target_routes_vector_destinations():
    from services.reconciliation import verify_target

    routes = (
        ("pinecone", "verify_pinecone_namespace"),
        ("qdrant", "verify_qdrant_collection"),
        ("weaviate", "verify_weaviate_class"),
        ("milvus", "verify_milvus_collection"),
    )
    for driver, fn in routes:
        with patch(
            f"services.reconciliation.{fn}",
            return_value=(4, driver[:2]),
        ) as mocked:
            assert verify_target(
                driver,
                {
                    "host": "localhost",
                    "password": "tok",
                    "connection_string": "https://example.test",
                    "database": "db1",
                },
                schema="",
                table_name="chunks",
                fallback_rows=-1,
                fallback_checksum="",
            ) == (4, driver[:2])
            assert mocked.called


def test_verify_pinecone_namespace_uses_stats_and_fetch():
    from services.reconciliation import verify_pinecone_namespace

    session = MagicMock()
    stats = MagicMock()
    stats.status_code = 200
    stats.content = b"{}"
    stats.json.return_value = {
        "namespaces": {"ns1": {"vector_count": 2}},
        "totalVectorCount": 2,
    }
    fetch = MagicMock()
    fetch.status_code = 200
    fetch.json.return_value = {
        "vectors": {
            "id-1": {"metadata": {"source_id": "s1"}},
            "id-2": {"metadata": {"source_id": "s2"}},
        }
    }
    session.post.return_value = stats
    session.get.return_value = fetch

    with patch("connectors.pinecone_writer._requests_session", return_value=session):
        count, chk = verify_pinecone_namespace(
            host="https://idx.svc.pinecone.io",
            password="key",
            namespace="ns1",
            written_ids=["id-1", "id-2"],
            limit=10,
        )
    assert count == 2
    assert isinstance(chk, str) and len(chk) > 0


def test_verify_qdrant_collection_scroll_payload():
    from services.reconciliation import verify_qdrant_collection

    session = MagicMock()
    info = MagicMock()
    info.status_code = 200
    info.json.return_value = {"result": {"points_count": 1}}
    scroll = MagicMock()
    scroll.status_code = 200
    scroll.json.return_value = {
        "result": {"points": [{"id": "p1", "payload": {"content": "hi"}}]}
    }
    session.get.return_value = info
    session.post.return_value = scroll

    with patch("connectors.qdrant_writer._requests_session", return_value=session):
        count, chk = verify_qdrant_collection(
            host="localhost",
            port=6333,
            collection="dataflow_vectors",
            limit=10,
        )
    assert count == 1
    assert chk
