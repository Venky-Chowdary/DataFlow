"""CLM-11: vector embeddings come from a provider someone chose, never a silent TF-IDF.

sentence-transformers is not in the API image, so every vector load fell back
to TF-IDF vectors (the pgvector run stored 32 dimensions, not 384) while the
destination was badged Transfer-ready. Now:

* a missing semantic model is refused, at Validate (G2) and at embed time;
* ``tfidf/local`` is the explicit non-semantic choice, stamped on every row;
* Kafka and the vector stores carry no Certified badge without CI proof
  (tests/test_clm_certification_from_evidence.py).
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from services import vectorization
from services.embedding_providers.base import EmbeddingConfigError


@pytest.fixture()
def no_minilm(monkeypatch):
    vectorization._get_embedder.cache_clear()
    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    monkeypatch.delenv("DATAFLOW_EMBEDDING_MODEL", raising=False)
    import src.ai.rag.embedding_service as embedding_service

    monkeypatch.setattr(embedding_service, "_embedding_service", None)
    yield
    vectorization._get_embedder.cache_clear()
    vectorization.take_embedding_fallback_notice()


def test_an_unchosen_model_is_refused_not_substituted(no_minilm) -> None:
    with pytest.raises(EmbeddingConfigError) as exc:
        vectorization.embed(["orders row"], use_cache=False)
    assert "not installed" in str(exc.value)
    assert "tfidf/local" in str(exc.value)


def test_tfidf_is_an_explicit_choice_and_says_so(no_minilm) -> None:
    rows = vectorization.vectorize_records(
        [{"id": "1", "content": "orders row"}],
        content_column="content",
        model="tfidf/local",
        skip_chunking=True,
    )
    assert rows[0]["metadata"]["_df_embedding_backend"] == "tfidf_fallback"
    assert "not semantic" in (vectorization.take_embedding_fallback_notice() or "")


@pytest.mark.parametrize("dest", ["pgvector", "qdrant", "weaviate", "pinecone", "milvus"])
def test_validate_refuses_a_vector_destination_with_no_runnable_model(no_minilm, dest) -> None:
    probe = {"db_type": dest, "connected": True, "can_write": True}
    vectorization.refuse_unchosen_embedding_provider(probe, SimpleNamespace(extra={}))
    assert probe["can_write"] is False
    assert "Choose the embedding provider" in probe["message"]
    for chosen in ("tfidf/local", "hash/32", "openai/text-embedding-3-small"):
        ok = {"db_type": dest, "connected": True, "can_write": True}
        vectorization.refuse_unchosen_embedding_provider(ok, SimpleNamespace(extra={"embedding_model": chosen}))
        assert ok["can_write"] is True, chosen


def test_non_vector_destinations_are_untouched(no_minilm) -> None:
    probe = {"db_type": "postgresql", "connected": True, "can_write": True}
    vectorization.refuse_unchosen_embedding_provider(probe, SimpleNamespace(extra={}))
    assert probe["can_write"] is True
