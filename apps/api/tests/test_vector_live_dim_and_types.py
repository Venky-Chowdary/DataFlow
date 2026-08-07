"""Vector write honesty — live types overlay + Qdrant dimension fail-closed."""

from __future__ import annotations


def test_prepare_records_overlays_destination_column_types():
    from connectors.writer_common import prepare_records_for_vector_write

    records, rejected, abort = prepare_records_for_vector_write(
        headers=["id", "qty"],
        data_rows=[["1", "7"], ["2", "x"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
        ],
        column_types={"id": "VARCHAR", "qty": "VARCHAR"},
        error_policy="quarantine",
        dest_kind="qdrant",
        label="qdrant",
        destination_column_types={"qty": "INTEGER", "id": "VARCHAR"},
    )
    assert abort is None
    # Unfit "x" should quarantine under live INTEGER, not pass as VARCHAR string.
    assert any((d.get("column") or "").lower() == "qty" for d in rejected) or len(
        records
    ) == 1


def test_qdrant_live_vector_size_unnamed():
    from connectors.qdrant_writer import _qdrant_live_vector_size

    assert (
        _qdrant_live_vector_size(
            {
                "result": {
                    "config": {
                        "params": {"vectors": {"size": 384, "distance": "Cosine"}}
                    }
                }
            }
        )
        == 384
    )


def test_qdrant_live_vector_size_named():
    from connectors.qdrant_writer import _qdrant_live_vector_size

    assert (
        _qdrant_live_vector_size(
            {
                "result": {
                    "config": {
                        "params": {
                            "vectors": {
                                "default": {"size": 768, "distance": "Cosine"}
                            }
                        }
                    }
                }
            }
        )
        == 768
    )
