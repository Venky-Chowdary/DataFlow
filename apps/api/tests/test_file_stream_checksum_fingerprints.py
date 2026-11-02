from __future__ import annotations

import hashlib


def test_nonempty_file_without_fingerprints_never_uses_empty_digest():
    from src.transfer.file_stream import _checksum_without_fingerprints

    checksum, mode = _checksum_without_fingerprints(
        source_rows=2,
        fingerprint_rows=0,
        writer_checksum=hashlib.sha256(b"").hexdigest(),
    )

    assert checksum == ""
    assert mode == "source_reread_unavailable"


def test_pending_object_store_mappings_fingerprint_from_spool():
    from connectors.engine_record_spill import (
        iter_fingerprints_from_spool,
        spill_engine_write_records,
    )
    from src.transfer.file_stream import _resolve_file_stream_target_columns

    columns = ["id", "amount"]
    mappings = [
        {
            "source": "id",
            "target": "id",
            "assignment_strategy": "pending_dest_schema",
        },
        {
            "source": "amount",
            "target": "amount",
            "assignment_strategy": "pending_dest_schema",
        },
    ]
    column_types = {"id": "INTEGER", "amount": "DECIMAL(6,2)"}
    target_cols, logical_types = _resolve_file_stream_target_columns(
        "gcs", mappings, column_types
    )
    spill = spill_engine_write_records(
        [{"id": 1, "amount": "2.50"}, {"id": 2, "amount": "5.00"}],
        columns,
        mappings,
        clear_records=True,
    )
    try:
        fingerprints = list(
            iter_fingerprints_from_spool(
                spill.spool,
                mappings,
                target_cols,
                headers=columns,
                column_types=column_types,
                dest_db_type="gcs",
                dest_types=dict(zip(target_cols, logical_types)),
                empty_cells_as_null=True,
            )
        )
    finally:
        spill.close()

    assert target_cols == ["id", "amount"]
    assert spill.unexpanded_row_count == 2
    assert len(fingerprints) == 2
    assert all(value_hash for _identity, value_hash in fingerprints)
    assert fingerprints[0][1] != fingerprints[1][1]
