"""D231: Weaviate document-state reads must see every chunk of a document.

QA on prod: Qdrant -> new Weaviate class wrote 159 objects (one long row was
chunked to 130), then stale cleanup failed "expected 130 matching points or
rows but found 89" because the GraphQL Get carried no limit and Weaviate
returned only QUERY_DEFAULTS_LIMIT (100) objects across the batch.

Live Weaviate on 127.0.0.1:8080; skipped when absent.
"""
from __future__ import annotations

import uuid

import pytest
import requests

BASE = "http://127.0.0.1:8080"
CFG = {"host": "127.0.0.1", "port": 8080}


def _up() -> bool:
    try:
        return requests.get(f"{BASE}/v1/meta", timeout=2).status_code == 200
    except requests.RequestException:
        return False


@pytest.fixture
def weaviate_class():
    if not _up():
        pytest.skip("Weaviate not reachable on 127.0.0.1:8080")
    name = "D231State" + uuid.uuid4().hex[:8]
    requests.post(
        f"{BASE}/v1/schema",
        json={
            "class": name,
            "vectorizer": "none",
            "properties": [
                {"name": "source_id", "dataType": ["text"], "tokenization": "field"},
                {"name": "chunk_index", "dataType": ["int"]},
            ],
        },
        timeout=10,
    ).raise_for_status()
    long_ids = [str(uuid.uuid4()) for _ in range(130)]
    objs = [
        {"class": name, "id": oid, "properties": {"source_id": "doc-7", "chunk_index": i},
         "vector": [0.1, 0.2, 0.3, 0.4]}
        for i, oid in enumerate(long_ids)
    ]
    short = {f"doc-{n}": str(uuid.uuid4()) for n in range(100, 129)}
    objs += [
        {"class": name, "id": oid, "properties": {"source_id": sid, "chunk_index": 0},
         "vector": [0.1, 0.2, 0.3, 0.4]}
        for sid, oid in short.items()
    ]
    requests.post(f"{BASE}/v1/batch/objects", json={"objects": objs}, timeout=30).raise_for_status()
    try:
        yield name, set(long_ids), short
    finally:
        requests.delete(f"{BASE}/v1/schema/{name}", timeout=10)


def test_document_state_reads_every_chunk_past_the_default_query_limit(weaviate_class):
    from services.vector_sync import vector_engine_read_document_states

    name, long_ids, short = weaviate_class
    states = vector_engine_read_document_states("weaviate", CFG, name, ["doc-7", *short])
    assert set(states["doc-7"]["chunk_ids"]) == long_ids
    assert all(set(states[sid]["chunk_ids"]) == {oid} for sid, oid in short.items())


def test_stale_cleanup_verifies_a_document_with_more_than_100_chunks(weaviate_class):
    from services.vector_sync import vector_engine_delete_stale_chunks

    name, long_ids, short = weaviate_class
    keep = {"doc-7": set(long_ids), **{sid: {oid} for sid, oid in short.items()}}
    assert vector_engine_delete_stale_chunks("weaviate", CFG, name, keep) == 0
