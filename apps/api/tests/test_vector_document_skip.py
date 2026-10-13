from __future__ import annotations

from services.vector_sync import (
    vector_document_hash,
    vector_document_is_unchanged,
    vector_metadata_hash,
)


def test_document_hash_and_chunk_count_match_skip():
    doc_hash = vector_document_hash("rendered content", "fingerprint")
    metadata_hash = vector_metadata_hash({"category": "guide"})
    state = {
        "doc_hash": doc_hash,
        "chunk_count": 3,
        "actual_chunk_count": 3,
        "metadata_hash": metadata_hash,
    }
    assert vector_document_is_unchanged(
        state,
        doc_hash=doc_hash,
        chunk_count=3,
        metadata_hash=metadata_hash,
    )


def test_chunk_count_mismatch_forces_reembedding():
    state = {
        "doc_hash": vector_document_hash("same", "fingerprint"),
        "chunk_count": 4,
        "actual_chunk_count": 4,
        "metadata_hash": vector_metadata_hash({}),
    }
    assert not vector_document_is_unchanged(
        state,
        doc_hash=state["doc_hash"],
        chunk_count=1,
        metadata_hash=state["metadata_hash"],
    )


def test_legacy_rows_without_hash_are_never_assumed_unchanged():
    assert not vector_document_is_unchanged(
        {
            "doc_hash": "",
            "chunk_count": 1,
            "actual_chunk_count": 1,
            "metadata_hash": "",
        },
        doc_hash=vector_document_hash("text", "fingerprint"),
        chunk_count=1,
        metadata_hash=vector_metadata_hash({}),
    )


def test_skip_can_be_disabled_and_metadata_changes_are_not_skipped():
    doc_hash = vector_document_hash("same text", "fingerprint")
    old_metadata_hash = vector_metadata_hash({"category": "old"})
    new_metadata_hash = vector_metadata_hash({"category": "new"})
    state = {
        "doc_hash": doc_hash,
        "chunk_count": 1,
        "actual_chunk_count": 1,
        "metadata_hash": old_metadata_hash,
    }
    assert not vector_document_is_unchanged(
        state,
        doc_hash=doc_hash,
        chunk_count=1,
        metadata_hash=new_metadata_hash,
    )
    assert not vector_document_is_unchanged(
        state,
        doc_hash=doc_hash,
        chunk_count=1,
        metadata_hash=old_metadata_hash,
        enabled=False,
    )
