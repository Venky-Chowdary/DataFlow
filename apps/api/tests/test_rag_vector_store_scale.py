"""RAG retrieval must stay bounded and stay correct as the store is re-ingested.

The Map step analyzes every source column, and each column runs one retrieval
over the whole store. Two defects made that unusable on a long-lived API:
re-ingesting the knowledge base appended a second copy of every document
(349 grew to 20190 in one session), and each retrieval re-derived both cosine
norms per document. A single column took ~11s.
"""

from __future__ import annotations

import math
import time

from src.ai.rag.document_ingestion import DataTransferDocumentIngestion
from src.ai.rag.vector_store import DataTransferVectorStore


def _store() -> DataTransferVectorStore:
    store = DataTransferVectorStore()
    store.delete_all()
    return store


def test_reingesting_the_knowledge_base_does_not_duplicate_documents():
    store = _store()
    ingestion = DataTransferDocumentIngestion()
    ingestion.vector_store = store

    first = ingestion.ingest_knowledge_base()["ingested"]
    assert store.document_count == first

    for _ in range(3):
        ingestion.ingest_knowledge_base()

    assert store.document_count == first
    ids = [doc["id"] for doc in store._memory_docs]
    assert len(ids) == len(set(ids))


def test_upsert_replaces_the_document_and_its_index_row():
    store = _store()
    store.add_documents(["postal code of the customer"], [{"type": "t"}], ["c1"])
    store.add_documents(["telephone number of the customer"], [{"type": "t"}], ["c1"])

    assert store.document_count == 1
    assert len(store._unit_rows) == 1
    top = store.search("telephone number", n_results=1)[0]
    assert top.text == "telephone number of the customer"


def test_search_scores_are_cosine_similarities():
    store = _store()
    store.add_documents(
        ["customer email address", "shipment tracking identifier"],
        [{"type": "t"}, {"type": "t"}],
        ["d1", "d2"],
    )
    query = "customer email address"
    hits = {doc.text: doc.score for doc in store.search(query, n_results=5)}

    embed = store._embedding_service
    for doc in store._memory_docs:
        expected = embed.similarity(embed.embed_single(query), doc["embedding"])
        assert math.isclose(hits[doc["text"]], expected, abs_tol=1e-9)


def test_metadata_filter_still_selects_only_matching_documents():
    store = _store()
    store.add_documents(
        ["customer email address", "customer email address"],
        [{"type": "keep"}, {"type": "drop"}],
        ["d1", "d2"],
    )
    hits = store.search("customer email", n_results=5, filter_metadata={"type": "keep"})
    assert [doc.id for doc in hits] == ["d1"]


def test_retrieval_stays_fast_on_a_full_knowledge_base():
    store = _store()
    ingestion = DataTransferDocumentIngestion()
    ingestion.vector_store = store
    ingestion.ingest_knowledge_base()
    assert store.document_count > 300

    store.search("warm up", n_results=5)
    started = time.perf_counter()
    for _ in range(20):
        store.search("customer email address", n_results=5)
    per_search = (time.perf_counter() - started) / 20

    # A 20-column Map runs 40 retrievals; hold each one well inside a frame.
    assert per_search < 0.05, f"{per_search:.4f}s per retrieval"
