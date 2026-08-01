"""Wave 38: Vector Gate-8 sample read-back + written_ids through reconcile."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_verify_target_forwards_written_ids_to_pinecone():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_pinecone_namespace",
        return_value=(2, "ab"),
    ) as mocked:
        assert verify_target(
            "pinecone",
            {"host": "https://idx.test", "password": "k"},
            schema="",
            table_name="ns1",
            fallback_rows=-1,
            fallback_checksum="",
            written_ids=["id-1", "id-2"],
        ) == (2, "ab")
        assert mocked.call_args.kwargs.get("written_ids") == ["id-1", "id-2"]


def test_verify_target_reads_written_ids_from_dest_cfg():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_qdrant_collection",
        return_value=(1, "qd"),
    ) as mocked:
        assert verify_target(
            "qdrant",
            {"host": "localhost", "written_ids": ["p1"]},
            schema="",
            table_name="chunks",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (1, "qd")
        assert mocked.call_args.kwargs.get("written_ids") == ["p1"]


def test_read_target_sample_pinecone_fetch_by_ids():
    from services.reconciliation import read_target_sample

    session = MagicMock()
    fetch = MagicMock()
    fetch.status_code = 200
    fetch.json.return_value = {
        "vectors": {"doc-1": {"metadata": {"source_id": "s1", "content": "hi"}}}
    }
    session.get.return_value = fetch

    with patch("connectors.pinecone_writer._requests_session", return_value=session):
        rows = read_target_sample(
            "pinecone",
            {"host": "https://idx.test", "password": "key"},
            schema="",
            table_name="ns1",
            columns=["id", "source_id", "content"],
            limit=10,
            sort_key="id",
            key_values=["doc-1"],
        )
    assert rows == [{"id": "doc-1", "source_id": "s1", "content": "hi"}]


def test_read_target_sample_qdrant_retrieve():
    from services.reconciliation import read_target_sample

    session = MagicMock()
    retrieve = MagicMock()
    retrieve.status_code = 200
    retrieve.json.return_value = {
        "result": [{"id": "p1", "payload": {"content": "hello", "source_id": "s"}}]
    }
    session.post.return_value = retrieve

    with patch("connectors.qdrant_writer._requests_session", return_value=session):
        rows = read_target_sample(
            "qdrant",
            {"host": "localhost", "port": 6333},
            schema="",
            table_name="dataflow_vectors",
            columns=["id", "content", "source_id"],
            limit=10,
            sort_key="id",
            key_values=["p1"],
        )
    assert rows == [{"id": "p1", "content": "hello", "source_id": "s"}]


def test_read_target_sample_weaviate_and_milvus_route():
    from services.reconciliation import read_target_sample

    # Weaviate keyed get
    w_session = MagicMock()
    w_resp = MagicMock()
    w_resp.status_code = 200
    w_resp.json.return_value = {
        "id": "uuid-1",
        "properties": {"content": "w", "source_id": "s"},
    }
    w_session.get.return_value = w_resp
    with patch("connectors.weaviate_writer._requests_session", return_value=w_session):
        w_rows = read_target_sample(
            "weaviate",
            {"host": "localhost", "api_key": "k"},
            schema="",
            table_name="DataflowChunk",
            columns=["id", "content", "source_id"],
            limit=5,
            sort_key="id",
            key_values=["uuid-1"],
        )
    assert w_rows[0]["content"] == "w"

    # Milvus keyed query
    m_session = MagicMock()
    m_resp = MagicMock()
    m_resp.status_code = 200
    m_resp.content = b"{}"
    m_resp.json.return_value = {
        "code": 0,
        "data": [{"id": "m1", "content": "x", "source_id": "s", "vector": [0.1]}],
    }
    m_session.post.return_value = m_resp
    with patch("connectors.milvus_writer._requests_session", return_value=m_session):
        m_rows = read_target_sample(
            "milvus",
            {"host": "localhost", "password": "Milvus"},
            schema="",
            table_name="dataflow_chunks",
            columns=["id", "content", "source_id"],
            limit=5,
            sort_key="id",
            key_values=["m1"],
        )
    assert m_rows == [{"id": "m1", "content": "x", "source_id": "s"}]
    assert "vector" not in m_rows[0]


def test_verify_target_forwards_written_ids_end_to_end():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_pinecone_namespace",
        return_value=(2, "ab"),
    ) as mocked:
        verify_target(
            "pinecone",
            {"host": "https://x", "password": "k"},
            schema="",
            table_name="ns",
            fallback_rows=-1,
            fallback_checksum="",
            written_ids=["a", "b"],
        )
        assert mocked.call_args.kwargs["written_ids"] == ["a", "b"]


def test_reconcile_step_module_forwards_written_ids_kwarg():
    """Ensure reconcile_step source contains the written_ids wiring."""
    text = Path(__file__).resolve().parents[1].joinpath(
        "src/transfer/reconcile_step.py"
    ).read_text(encoding="utf-8")
    assert "written_ids=" in text
    assert 'dest_summary.get("written_ids")' in text
