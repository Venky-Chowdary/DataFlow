from __future__ import annotations

import socket
import uuid

import pytest


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


_ENGINES = [
    pytest.param(
        "weaviate",
        id="weaviate-live",
        marks=pytest.mark.skipif(not _reachable(8080), reason="Weaviate is not reachable"),
    ),
    pytest.param(
        "pinecone",
        id="pinecone-local",
        marks=pytest.mark.skipif(
            not _reachable(5081), reason="Pinecone Local is not reachable"
        ),
    ),
    pytest.param(
        "milvus",
        id="milvus-standalone",
        marks=pytest.mark.skipif(not _reachable(19530), reason="Milvus is not reachable"),
    ),
]
_DOC_ID = "m6-doc"
_LONG_BODY = "m6vectorcontract" * 120


@pytest.fixture(params=_ENGINES)
def m6_destination(request):
    engine = str(request.param)
    suffix = uuid.uuid4().hex[:10]
    target = {
        "weaviate": f"M6Vector{suffix}",
        "pinecone": f"m6-vector-{suffix}",
        "milvus": f"m6_vector_{suffix}",
    }[engine]
    cfg = {
        "weaviate": {
            "host": "localhost",
            "port": 8080,
            "database": "",
            "username": "",
            "password": "",
            "schema": "",
            "api_key": "",
            "ssl": False,
            "connection_string": "",
        },
        "pinecone": {
            "host": "http://localhost:5081",
            "port": 5081,
            "database": "",
            "username": "",
            "password": "",
            "schema": "",
            "ssl": False,
            "api_key": "local",
            "connection_string": "",
        },
        "milvus": {
            "host": "localhost",
            "port": 19530,
            "database": "",
            "username": "",
            "password": "",
            "ssl": False,
            "connection_string": "",
        },
    }[engine]
    destination = {"engine": engine, "target": target, "cfg": cfg}
    yield destination
    _drop_destination(destination)


def _drop_destination(destination):
    import requests

    engine = destination["engine"]
    target = destination["target"]
    if engine == "weaviate":
        requests.delete(
            f"http://localhost:8080/v1/schema/{target}", timeout=15
        )
    elif engine == "pinecone":
        requests.post(
            "http://localhost:5081/vectors/delete",
            headers={"Api-Key": "local", "Content-Type": "application/json"},
            json={"namespace": target, "deleteAll": True},
            timeout=15,
        )
    elif engine == "milvus":
        requests.post(
            "http://localhost:19530/v2/vectordb/collections/drop",
            json={"collectionName": target},
            timeout=30,
        )


def _write(destination, *, body=_LONG_BODY, model="hash/32", **options):
    engine = destination["engine"]
    cfg = destination["cfg"]
    from connectors.milvus_writer import write_mapped_rows as write_milvus
    from connectors.pinecone_writer import write_mapped_rows as write_pinecone
    from connectors.weaviate_writer import write_mapped_rows as write_weaviate

    writer = {
        "weaviate": write_weaviate,
        "pinecone": write_pinecone,
        "milvus": write_milvus,
    }[engine]
    headers = ["id", "body"]
    mappings = [
        {"source": name, "target": name, "target_type": "VARCHAR"}
        for name in headers
    ]
    return writer(
        **cfg,
        table_name=destination["target"],
        headers=headers,
        data_rows=[[_DOC_ID, body]],
        mappings=mappings,
        column_types={"id": "string", "body": "string"},
        destination_column_types={"id": "VARCHAR", "body": "VARCHAR"},
        destination_pk_columns=["id"],
        conflict_columns=["id"],
        error_policy="quarantine",
        content_column="body",
        embedding_model=model,
        chunk_size=24,
        chunk_overlap=0,
        skip_chunking=False,
        create_table=True,
        write_mode="upsert",
        sync_mode="cdc",
        **options,
    )


def _rows_for_doc(destination):
    import requests

    engine = destination["engine"]
    target = destination["target"]
    if engine == "weaviate":
        response = requests.get(
            "http://localhost:8080/v1/objects",
            params={"class": target, "limit": 1000},
            timeout=30,
        )
        assert response.status_code == 200, response.text
        return [
            obj
            for obj in response.json().get("objects", [])
            if str((obj.get("properties") or {}).get("source_id")) == _DOC_ID
        ]
    if engine == "pinecone":
        headers = {"Api-Key": "local", "Content-Type": "application/json"}
        listed = requests.get(
            "http://localhost:5081/vectors/list",
            headers=headers,
            params={"namespace": target, "limit": 99},
            timeout=30,
        )
        assert listed.status_code == 200, listed.text
        ids = [
            str(item.get("id") or item)
            for item in listed.json().get("vectors", [])
        ]
        if not ids:
            return []
        fetch_params = [("ids", vector_id) for vector_id in ids]
        fetch_params.append(("namespace", target))
        fetched = requests.get(
            "http://localhost:5081/vectors/fetch",
            headers=headers,
            params=fetch_params,
            timeout=30,
        )
        assert fetched.status_code == 200, fetched.text
        vectors = fetched.json().get("vectors") or {}
        return [
            {**(vector.get("metadata") or {}), "id": vector_id}
            for vector_id, vector in vectors.items()
            if str((vector.get("metadata") or {}).get("source_id")) == _DOC_ID
        ]
    response = requests.post(
        "http://localhost:19530/v2/vectordb/entities/query",
        json={
            "collectionName": target,
            "filter": f'source_id == "{_DOC_ID}"',
            "outputFields": ["id", "source_id", "chunk_index", "content"],
            "limit": 1000,
        },
        timeout=30,
    )
    body = response.json()
    assert response.status_code == 200 and body.get("code", 0) == 0, body
    return body.get("data") or []


def test_m6_writer_cleans_stale_chunks_after_shrink(m6_destination):
    first = _write(m6_destination)
    assert first.ok, first.error
    first_rows = _rows_for_doc(m6_destination)
    assert len(first_rows) >= 5
    assert len({str(row.get("id")) for row in first_rows}) == len(first_rows)

    second = _write(m6_destination, body="short body")
    assert second.ok, second.error
    rows = _rows_for_doc(m6_destination)
    assert len(rows) == 1, f"stale chunks remain: {len(rows)}"


def test_m6_unchanged_document_skips_embedding(m6_destination):
    first = _write(m6_destination)
    assert first.ok, first.error

    second = _write(m6_destination)
    assert second.ok, second.error
    meta = second.meta or {}
    assert meta.get("vector_docs_unchanged_skipped") == 1
    assert meta.get("vector_docs_embedded") == 0
    assert (meta.get("embedding_usage") or {}).get("calls") == 0


def test_m6_document_key_delete_is_verified(m6_destination):
    written = _write(m6_destination)
    assert written.ok, written.error
    assert _rows_for_doc(m6_destination)

    from connectors.table_manager import delete_by_primary_keys

    deleted = delete_by_primary_keys(
        m6_destination["engine"],
        m6_destination["cfg"],
        m6_destination["target"],
        "id",
        [_DOC_ID],
    )
    assert deleted > 0
    assert _rows_for_doc(m6_destination) == []


def test_m6_fingerprint_rejects_a_different_model(m6_destination, monkeypatch):
    from services import vectorization

    def fake_embedder(_model=None):
        embedder = vectorization._HashEmbedder(dimension=32)
        embedder.backend = "sentence_transformers"
        return embedder

    monkeypatch.setattr(vectorization, "_get_embedder", fake_embedder)
    first = _write(m6_destination, model="sentence-transformers/m6-model-a")
    assert first.ok, first.error
    second = _write(m6_destination, model="sentence-transformers/m6-model-b")
    assert not second.ok
    assert "fingerprint mismatch" in (second.error or "").lower()


def test_m6_writer_reports_embedding_usage(m6_destination):
    result = _write(m6_destination, body=f"{_LONG_BODY}-{uuid.uuid4().hex}")
    assert result.ok, result.error
    usage = (result.meta or {}).get("embedding_usage")
    assert isinstance(usage, dict)
    assert usage["calls"] > 0
