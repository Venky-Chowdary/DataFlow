from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    ("row", "columns"),
    [
        ({"doc_id": 7}, ["doc_id"]),
        ({"tenant": "west", "doc_id": 7}, ["tenant", "doc_id"]),
        ({"doc_id": "0"}, ["doc_id"]),
    ],
)
def test_writer_source_ids_match_cdc_delete_doc_keys(row, columns):
    from services.cdc_snapshot_window import _pk_value
    from services.row_conservation import parse_delete_keys
    from services.vector_sync import vector_doc_key
    from services.vectorization import vectorize_records

    record = {**row, "content": "identity parity"}
    written = vectorize_records(
        [record],
        content_column="content",
        model="hash/32",
        identity_columns=columns,
    )
    cdc_key = _pk_value(row, columns)
    parsed = parse_delete_keys([cdc_key], len(columns))
    assert len(parsed) == 1
    assert written[0]["source_id"] == vector_doc_key(parsed[0])


@pytest.mark.parametrize(
    "dest_type",
    [
        pytest.param("pgvector", id="pgvector-target"),
        pytest.param("qdrant", id="qdrant"),
    ],
)
def test_cdc_vector_delete_zero_is_an_idempotent_success(dest_type):
    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import _apply_change_batch

    with (
        patch("services.row_conservation.census_change_batch", return_value=None),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
    ):
        result = _apply_change_batch(
            dest_type=dest_type,
            destination=None,
            dest_cfg={},
            dest_table="missing",
            change=ChangeBatch(deletes=["0"]),
            mappings=[],
            column_types={},
            headers=["id"],
            pk_target_col="id",
            chunk_idx=0,
            total_chunks=1,
        )
    assert result[3] == 0


def test_document_chunk_ids_ignore_text_edits_and_share_document_identity():
    from services.document_chunking import _record

    first = _record(
        source_id="document-1",
        filename="guide.html",
        content="first version",
        chunk_index=0,
    )
    edited = _record(
        source_id="document-1",
        filename="guide.html",
        content="edited version",
        chunk_index=0,
    )
    second_chunk = _record(
        source_id="document-1",
        filename="guide.html",
        content="another section",
        chunk_index=1,
    )
    assert first["id"] == edited["id"]
    assert first["source_id"] == edited["source_id"] == second_chunk["source_id"]
    assert first["id"] != second_chunk["id"]


def test_vector_sinks_are_at_least_once_and_not_lsn_guard_eligible():
    from services.cdc_effectively_once import classify_sink_delivery
    from services.connector_capability_registry import get_connector_capability

    for destination in ("pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
        capabilities = get_connector_capability(destination)
        assert capabilities["supports_lsn_guard"] is False
        posture = classify_sink_delivery(
            dest_type=destination,
            has_primary_key=True,
            write_mode="upsert",
        )
        assert posture["delivery_class"] == "at_least_once"
        assert posture["exactly_once"] is False
        assert posture["effectively_once_pk_sink"] is False


def test_vector_delete_unverified_error_is_a_destination_delete_error():
    from connectors.table_manager import DestinationDeleteError
    from services.vector_sync import VectorDeleteUnverifiedError

    error = VectorDeleteUnverifiedError("collection", 3)
    assert isinstance(error, DestinationDeleteError)
    assert error.remaining == 3
    assert "3" in str(error)


@pytest.mark.parametrize("destination", ["postgresql", "qdrant"])
def test_canonical_document_key_preserves_unit_separator(destination):
    from services.vector_sync import vector_doc_key

    assert vector_doc_key((destination, 0, "0")) == f"{destination}\x1f0\x1f0"


def test_stale_cleanup_reporting_helpers():
    from services.vector_sync import (
        log_stale_cleanup_skipped,
        stale_cleanup_meta,
        stale_cleanup_skipped_docs,
    )

    rejected_ids = {"doc-a"}
    rejected_details = [
        {"row": "1"},
        {"row": "1"},
        {"row": "2"},
        {"row": ""},
    ]
    assert stale_cleanup_skipped_docs(rejected_ids, rejected_details) == 2
    assert (
        stale_cleanup_skipped_docs(
            rejected_ids, rejected_details, has_identityless=True
        )
        == 3
    )
    assert stale_cleanup_meta(9, 2) == {
        "stale_chunks_deleted": 9,
        "vector_stale_cleanup_skipped_docs": 2,
    }
    log_stale_cleanup_skipped("Qdrant", "collection demo", 2, "chunk rejected")
