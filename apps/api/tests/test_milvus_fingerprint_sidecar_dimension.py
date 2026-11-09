"""The Milvus fingerprint sidecar must be a collection Milvus can create.

Milvus refuses a FloatVector field with ``dim < 2`` ("invalid dimension: 1.
should be in range 2 ~ 32768"). The sidecar was created with ``dimension=1``,
so every first write into a new Milvus collection failed after the data
collection itself had been created (PRODUCTION_SKU file/csv → milvus).
"""

from __future__ import annotations

import pytest

import connectors.milvus_writer as milvus_writer
from services.vector_fingerprint import (
    _MILVUS_SIDECAR_VECTOR,
    EmbeddingFingerprint,
    _enforce_milvus,
)


class _Stop(Exception):
    pass


def test_sidecar_collection_dimension_is_one_milvus_accepts(monkeypatch):
    seen: dict[str, int] = {}

    def fake_ensure(_session, _base, _headers, name, dimension, db_name=""):
        seen[name] = dimension
        raise _Stop

    monkeypatch.setattr(milvus_writer, "_ensure_collection", fake_ensure)
    fp = EmbeddingFingerprint(
        provider="hash", model="hash/32", dimension=32, distance="cosine", chunker={}
    )
    with pytest.raises(_Stop):
        _enforce_milvus({"host": "127.0.0.1", "port": 19530}, "payments", fp)
    assert seen == {"_df_vector_collections": len(_MILVUS_SIDECAR_VECTOR)}
    assert 2 <= len(_MILVUS_SIDECAR_VECTOR) <= 32768
    assert any(_MILVUS_SIDECAR_VECTOR), "COSINE refuses an all-zero vector"


def test_json_field_decodes_milvus_24_base64_rest_encoding():
    """Milvus 2.4 REST returns JSON fields base64-encoded; reading that as {}
    made every stored fingerprint look empty on the next run."""
    import base64
    import json

    from connectors.milvus_writer import milvus_json_field

    meta = {"target": "payments", "digest": "abc"}
    encoded = base64.b64encode(json.dumps(meta).encode()).decode()
    assert milvus_json_field(encoded) == meta
    assert milvus_json_field(json.dumps(meta)) == meta
    assert milvus_json_field(meta) == meta
    assert milvus_json_field("not json") == {}
    assert milvus_json_field(None) == {}



def test_verification_read_polls_milvus_until_the_write_is_visible(monkeypatch):
    """Milvus 2.4 hides an upsert from the next query; first-write stale
    cleanup called that ``VectorDeleteUnverifiedError``."""
    import services.vector_sync as vs

    reads = iter([{}, {}, {"doc": {"chunk_ids": {"c1"}}}])
    monkeypatch.setattr(vs, "vector_engine_read_document_states", lambda *_a: next(reads))
    monkeypatch.setattr(vs.time, "sleep", lambda _s: None)
    states = vs._read_states_settled("milvus", {}, "t", ["doc"], lambda st: bool(st))
    assert states == {"doc": {"chunk_ids": {"c1"}}}


def test_verification_read_does_not_poll_engines_with_read_after_write(monkeypatch):
    import services.vector_sync as vs

    calls: list[int] = []

    def read(*_a):
        calls.append(1)
        return {}

    monkeypatch.setattr(vs, "vector_engine_read_document_states", read)
    assert vs._read_states_settled("qdrant", {}, "t", ["doc"], lambda st: bool(st)) == {}
    assert len(calls) == 1
