from __future__ import annotations

import json

import pytest


def _fingerprint(*, model: str = "hash/32", chunk_size: int = 512):
    from services.vector_fingerprint import fingerprint_for_write

    return fingerprint_for_write(
        model=model,
        dimension=32,
        distance="cosine",
        chunk_size=chunk_size,
        chunk_overlap=50,
        skip_chunking=False,
        embedding_column=None,
    )


def test_fingerprint_digest_is_canonical_and_stable():
    from services.vector_fingerprint import EmbeddingFingerprint

    first = EmbeddingFingerprint(
        provider="hash",
        model="hash/32",
        dimension=32,
        distance="cosine",
        chunker={"chunk_overlap": 50, "chunk_size": 512, "strategy": "recursive"},
    )
    second = EmbeddingFingerprint(
        provider="hash",
        model="hash/32",
        dimension=32,
        distance="cosine",
        chunker={"strategy": "recursive", "chunk_size": 512, "chunk_overlap": 50},
    )
    assert first.digest == second.digest


def test_deterministic_model_alias_matches_hash_model():
    deterministic = _fingerprint(model="deterministic/32")
    hash_model = _fingerprint(model="hash/32")

    assert deterministic.to_dict() == hash_model.to_dict()
    assert deterministic.digest == hash_model.digest


def test_fingerprint_diff_names_changed_model_and_chunk_settings():
    from services.vector_fingerprint import VectorFingerprintMismatchError

    stored = _fingerprint()
    incoming = _fingerprint(model="openai/text-embedding-3-small", chunk_size=128)
    diffs = stored.diff(incoming)

    assert any("model: 'hash/32' (stored)" in diff for diff in diffs)
    assert any("chunker.chunk_size: 512 (stored)" in diff for diff in diffs)
    message = str(VectorFingerprintMismatchError(stored, incoming))
    assert "Write to a new collection/table" in message
    assert "drop it (full refresh)" in message
    assert "set the Studio embedding model/chunk settings back" in message
    assert "hash/32" in message


def test_fingerprint_resolves_environment_default(monkeypatch):
    from services import vectorization
    from services.vector_fingerprint import fingerprint_for_write

    monkeypatch.setenv("DATAFLOW_EMBEDDING_MODEL", "hash/16")
    vectorization._get_embedder.cache_clear()
    try:
        fingerprint = fingerprint_for_write(
            model=None,
            dimension=16,
            distance="cosine",
            chunk_size=256,
            chunk_overlap=20,
            skip_chunking=True,
            embedding_column=None,
        )
    finally:
        vectorization._get_embedder.cache_clear()

    assert fingerprint.provider == "hash"
    assert fingerprint.model == "hash/16"


def test_fingerprint_does_not_include_api_credentials(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-be-persisted")
    fingerprint = _fingerprint(model="openai/text-embedding-3-small")

    serialized = json.dumps(fingerprint.to_dict(), sort_keys=True)
    assert fingerprint.provider == "openai"
    assert fingerprint.model == "openai/text-embedding-3-small"
    assert "must-not-be-persisted" not in serialized
    assert "OPENAI_API_KEY" not in serialized


@pytest.mark.parametrize(
    ("model", "provider", "canonical"),
    [
        ("text-embedding-3-small", "openai", "openai/text-embedding-3-small"),
        ("azure/prod-deployment", "azure_openai", "prod-deployment"),
        ("openai-compatible/vendor-model", "openai_compatible", "vendor-model"),
        ("cohere/embed-english-v3.0", "cohere", "embed-english-v3.0"),
        (
            "bedrock/amazon.titan-embed-text-v2:0",
            "bedrock",
            "amazon.titan-embed-text-v2:0",
        ),
    ],
)
def test_fingerprint_uses_credential_free_provider_identity(
    model, provider, canonical
):
    fingerprint = _fingerprint(model=model)
    assert fingerprint.provider == provider
    assert fingerprint.model == canonical


def test_supplied_embedding_column_uses_source_embedding_provider():
    from services.vector_fingerprint import fingerprint_for_write

    fingerprint = fingerprint_for_write(
        model="hash/32",
        dimension=32,
        distance="Cosine",
        chunk_size=512,
        chunk_overlap=50,
        skip_chunking=False,
        embedding_column="embedding",
    )

    assert fingerprint.provider == "source_embedding"
    assert fingerprint.model == "column:embedding"


def test_qdrant_live_distance_reads_unnamed_and_named_vectors():
    from connectors.qdrant_writer import _qdrant_live_vector_distance

    unnamed = {
        "result": {
            "config": {"params": {"vectors": {"size": 32, "distance": "Dot"}}}
        }
    }
    named = {
        "result": {
            "config": {
                "params": {
                    "vectors": {"text": {"size": 32, "distance": "Euclid"}}
                }
            }
        }
    }

    assert _qdrant_live_vector_distance(unnamed) == "Dot"
    assert _qdrant_live_vector_distance(named) == "Euclid"


def test_qdrant_drop_removes_sidecar_point(monkeypatch):
    from connectors import qdrant_writer, table_manager

    class Response:
        status_code = 200
        text = ""

    class Session:
        def __init__(self):
            self.calls = []

        def delete(self, url, **_kwargs):
            self.calls.append(("delete", url))
            return Response()

        def get(self, url, **_kwargs):
            self.calls.append(("get", url))
            return Response()

        def post(self, url, **_kwargs):
            self.calls.append(("post", url))
            return Response()

        def close(self):
            return None

    session = Session()
    monkeypatch.setattr(
        qdrant_writer, "qdrant_rest", lambda _cfg: (session, "http://qdrant", {})
    )

    assert table_manager.drop_table("qdrant", {}, "target") is True
    assert session.calls == [
        ("delete", "http://qdrant/collections/target"),
        ("get", "http://qdrant/collections/_df_vector_collections"),
        (
            "post",
            "http://qdrant/collections/_df_vector_collections/points/delete?wait=true",
        ),
    ]


def test_qdrant_drop_fingerprint_failure_is_typed(monkeypatch):
    from connectors import qdrant_writer, table_manager
    from connectors.table_manager import TableDropError

    class Response:
        def __init__(self, status_code):
            self.status_code = status_code
            self.text = "sidecar unavailable"

    class Session:
        def delete(self, _url, **_kwargs):
            return Response(200)

        def get(self, _url, **_kwargs):
            return Response(200)

        def post(self, _url, **_kwargs):
            return Response(503)

        def close(self):
            return None

    monkeypatch.setattr(
        qdrant_writer,
        "qdrant_rest",
        lambda _cfg: (Session(), "http://qdrant", {}),
    )

    try:
        table_manager.drop_table("qdrant", {}, "target")
    except TableDropError as exc:
        assert "fingerprint delete failed" in str(exc)
    else:
        raise AssertionError("sidecar deletion failure was swallowed")
