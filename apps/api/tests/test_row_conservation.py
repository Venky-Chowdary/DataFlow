"""Independent dest COUNT(*) closes conservation — writer ack never does.

AWS DMS Full Load can succeed while validation later reports MISSING_TARGET:
the writer counted rows the dest engine does not hold. This module is the
named identity so the certificate cannot circularly balance a short write
against itself.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from services.dest_precount import (
    ARTIFACT_COUNT_KEY,
    CURRENT_ROWS_KEY,
    DEST_COUNT_ARTIFACT,
    DEST_COUNT_CURRENT,
    DEST_COUNT_IDENTITY,
    EXTRA_KEYS_KEY,
    HISTORY_ROWS_KEY,
    IDENTITY_COUNT_KEY,
    MISSING_KEYS_KEY,
    SOURCE_ID_SCAN_COMPLETE,
    SOURCE_ID_SCAN_MISSING,
    SOURCE_ID_SCAN_NO_FIELD,
    SOURCE_ID_SCAN_TRUNCATED,
    SOURCE_ID_SCAN_UNMEASURED,
    VECTOR_IDENTITY_ENGINES,
    VECTOR_ROWS_KEY,
    count_artifact_rows,
    count_scd2_current,
    destination_key_list,
    destination_keyset_census,
    destination_row_count,
    identity_count_from_source_id_scan,
    records_to_key_tuples,
    stamp_artifact_census,
    stamp_keyset_census,
    stamp_scd2_census,
    stamp_vector_census,
)
from services.row_conservation import (
    DEST_ACTIVE_READBACK,
    DEST_ARTIFACT_READBACK,
    DEST_CURRENT_READBACK,
    DEST_IDENTITY_READBACK,
    DEST_PER_STREAM,
    DEST_READBACK,
    DEST_UNMEASURED,
    KIND_APPEND_DELTA,
    KIND_EMPTY_PASS,
    KIND_JOB,
    KIND_KEYED,
    KIND_MIRROR,
    KIND_OVERWRITE,
    KIND_SCD2,
    KIND_VECTOR,
    account_job,
    account_job_streams,
    account_population,
    conservation_kind,
    dest_count_from_recon,
    hold_outs,
    apply_inferred_leftover_deletes,
)


def test_hold_outs_exclude_coerced_null_rows_that_landed():
    assert hold_outs(rejected_rows=5, coerced_null_rows=2) == 3
    assert hold_outs(rejected_rows=2, coerced_null_rows=2) == 0
    assert hold_outs(rejected_rows=0, coerced_null_rows=3) == 0


def test_writer_ack_phase_without_dest_digest_is_not_a_dest_count():
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "phase": "post_write_writer_ack",
            "coverage": "writer_ack",
            "assurance_level": "writer_ack",
            "message": "verified by writer checksum",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_writer_ack_source_digest_still_exposes_independent_dest_count():
    """Streaming Gate-8: source digest is writer ack; dest COUNT(*) is dest."""
    count, source = dest_count_from_recon(
        {
            "passed": True,
            "phase": "post_write_writer_ack",
            "coverage": "writer_ack",
            "assurance_level": "writer_ack",
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "abc123",
            "source_checksum": "abc123",
            "source_checksum_provenance": "writer_ack",
            "message": "Row fidelity verified — source and target checksums match (4 rows)",
        }
    )
    assert count == 4
    assert source == DEST_READBACK


def test_skipped_readback_stuffs_writer_ack_and_is_refused():
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "skipped_readback": True,
            "unproven": True,
            "message": "File/object export wrote successfully",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_artifact_readback_closes_on_file_count_not_writer_ack():
    """DMS hole for files: writer rows never close dest; re-opened records do."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "skipped_readback": True,
            "unproven": True,
            "migration_proven": False,
            "dest_count_source": DEST_ARTIFACT_READBACK,
            ARTIFACT_COUNT_KEY: 3,
            "message": "File/object export wrote successfully — Gate-8 cell fidelity unproven",
        }
    )
    assert count == 3
    assert source == DEST_ARTIFACT_READBACK


def test_artifact_source_without_artifact_count_is_unmeasured():
    """Forged dest_count_source + stuffed target_rows is still writer ack."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "skipped_readback": True,
            "dest_count_source": DEST_ARTIFACT_READBACK,
            "message": "File/object export wrote successfully",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_artifact_overwrite_balances_on_file_count_not_writer_ack():
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_ARTIFACT_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="incremental_append",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.rows_written == 3
    assert ledger.rows_written_source == DEST_ARTIFACT_READBACK
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack_delta == -9997
    assert "artifact" in ledger.note.lower()
    assert "destination table" not in ledger.note.lower()


def test_count_artifact_rows_csv_jsonl_json_independent_of_writer(tmp_path: Path):
    csv_path = tmp_path / "export.csv"
    csv_path.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")
    assert count_artifact_rows(csv_path, fmt="csv") == 3

    empty = tmp_path / "empty.csv"
    empty.write_text("id,name\n", encoding="utf-8")
    assert count_artifact_rows(empty, fmt="csv") == 0

    jsonl_path = tmp_path / "export.jsonl"
    jsonl_path.write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")
    assert count_artifact_rows(jsonl_path, fmt="jsonl") == 2

    json_path = tmp_path / "export.json"
    json_path.write_text('[{"id":1},{"id":2},{"id":3}]', encoding="utf-8")
    assert count_artifact_rows(json_path, fmt="json") == 3

    import gzip

    gz_path = tmp_path / "export.csv.gz"
    gz_path.write_bytes(gzip.compress(b"id\n1\n2\n"))
    assert count_artifact_rows(gz_path, fmt="csv") == 2

    bad = tmp_path / "bad.jsonl"
    bad.write_text("{not json\n", encoding="utf-8")
    assert count_artifact_rows(bad, fmt="jsonl") is None

    missing = tmp_path / "nope.csv"
    assert count_artifact_rows(missing, fmt="csv") is None
    assert count_artifact_rows("s3://bucket/key.csv", fmt="csv") is None


def test_stamp_artifact_census_never_keeps_writer_target_rows(tmp_path: Path):
    csv_path = tmp_path / "out.csv"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(csv_path), "format": "csv"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 2
    assert stamped["dest_count_source"] == DEST_COUNT_ARTIFACT
    assert stamped["target_rows"] == 2
    assert stamped["target_rows_before"] == 0

    unmeasured = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": "s3://bucket/export.csv", "format": "csv"},
    )
    assert ARTIFACT_COUNT_KEY not in unmeasured
    assert unmeasured["target_rows"] is None


def test_identity_readback_closes_on_distinct_source_id_not_vector_count():
    """RAG hole: 2 documents / 5 chunks / writer 10,000 never closes dest as 5."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 5,
            "target_checksum": "abc123",
            "skipped_readback": True,
            "unproven": True,
            "migration_proven": False,
            "dest_count_source": DEST_IDENTITY_READBACK,
            IDENTITY_COUNT_KEY: 2,
            VECTOR_ROWS_KEY: 5,
            "message": "pgvector write completed — Gate-8 embedding cell fidelity unproven",
        }
    )
    assert count == 2
    assert source == DEST_IDENTITY_READBACK


def test_identity_source_without_identity_rows_is_unmeasured():
    """Forged dest_count_source + stuffed vector COUNT(*) is still not dest."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "target_checksum": "abc123",
            "skipped_readback": True,
            "dest_count_source": DEST_IDENTITY_READBACK,
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_skipped_identity_readback_refuses_physical_vector_count():
    count, source = dest_count_from_recon(
        {
            "target_rows": 5,
            "target_checksum": "abc123",
            "dest_count_source": "skipped_identity_readback",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_vector_overwrite_balances_on_identities_not_chunks_or_writer_ack():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_IDENTITY_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="full_refresh_overwrite",
        vector={"identity_rows": 2, "vector_rows": 5},
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.rows_written == 2
    assert ledger.rows_written_source == DEST_IDENTITY_READBACK
    assert ledger.identity_count == 2
    assert ledger.vector_rows == 5
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack_delta == -9998
    assert "identity" in ledger.note.lower() or "source_id" in ledger.note.lower()
    assert "chunk" in ledger.note.lower() or "vector" in ledger.note.lower()


def test_vector_physical_count_does_not_close_as_overwrite_surplus():
    """If dest_count were physical 5 against reader 2, overwrite would invent dupes."""
    ledger = account_population(
        rows_read=2,
        dest_count=5,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=5,
        sync_mode="full_refresh_overwrite",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.balanced is False
    assert ledger.unaccounted == -3


def test_vector_nonempty_dest_stays_unproven_without_source_id_census():
    ledger = account_population(
        rows_read=2,
        dest_count=12,
        dest_count_source=DEST_IDENTITY_READBACK,
        dest_count_before=10,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=5,
        sync_mode="incremental_append",
        vector={"identity_rows": 12, "vector_rows": 40},
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.balanced is False
    assert ledger.unaccounted is None
    assert ledger.dest_count == 12
    assert ledger.dest_count_before == 10
    assert ledger.dest_delta == 2
    assert "unproven" in ledger.note.lower()


def test_vector_dest_before_unmeasured_does_not_close():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_IDENTITY_READBACK,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=5,
        sync_mode="full_refresh_overwrite",
        vector={"identity_rows": 2, "vector_rows": 5},
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.balanced is False
    assert ledger.unaccounted is None


def test_stamp_vector_census_milvus_unreachable_is_skipped_identity_not_rowcount():
    """Unreachable Milvus must not close dest as collection rowCount."""
    stamped = stamp_vector_census(
        {"target_rows": 5, "target_checksum": "abc"},
        {},
        schema="public",
        table_name="docs",
        dest_engine="milvus",
    )
    assert IDENTITY_COUNT_KEY not in stamped
    assert stamped.get("dest_count_source") == "skipped_identity_readback"
    assert stamped["target_rows"] == 5


def test_stamp_vector_census_pinecone_rowcount_is_not_identity():
    """Pinecone has no dest-engine DISTINCT source_id — leave rowCount alone."""
    stamped = stamp_vector_census(
        {"target_rows": 5, "target_checksum": "abc"},
        {},
        schema="",
        table_name="docs",
        dest_engine="pinecone",
    )
    assert IDENTITY_COUNT_KEY not in stamped
    assert stamped.get("dest_count_source") != DEST_COUNT_IDENTITY
    assert "pinecone" not in VECTOR_IDENTITY_ENGINES
    assert "weaviate" not in VECTOR_IDENTITY_ENGINES


def test_identity_count_from_source_id_scan_is_distinct_not_chunk_count():
    """5 chunks / 2 documents / empty ids / truncated prefix — SQL COUNT DISTINCT."""
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_MISSING, None) == 0
    assert identity_count_from_source_id_scan(
        SOURCE_ID_SCAN_COMPLETE,
        ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"],
    ) == 2
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_COMPLETE, []) == 0
    assert identity_count_from_source_id_scan(
        SOURCE_ID_SCAN_COMPLETE, ["doc-1", "", None, "  "]
    ) == 1
    assert identity_count_from_source_id_scan(
        SOURCE_ID_SCAN_TRUNCATED, ["doc-1"] * 20_000
    ) is None
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_NO_FIELD, ["x"]) is None
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_UNMEASURED, ["x"]) is None


def test_stamp_vector_census_milvus_closes_on_distinct_source_id(monkeypatch):
    def fake_scan(cfg, *, table_name, max_entities):
        assert table_name == "docs"
        assert max_entities >= 5
        return SOURCE_ID_SCAN_COMPLETE, ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"]

    monkeypatch.setattr("connectors.milvus_writer.scan_source_ids", fake_scan)
    stamped = stamp_vector_census(
        {"target_rows": 10_000, "target_checksum": "writer"},
        {"host": "127.0.0.1", "port": 19530},
        schema="",
        table_name="docs",
        dest_engine="milvus",
    )
    assert stamped[IDENTITY_COUNT_KEY] == 2
    assert stamped["dest_count_source"] == DEST_COUNT_IDENTITY
    assert stamped[VECTOR_ROWS_KEY] == 10_000
    count, source = dest_count_from_recon(stamped)
    assert count == 2
    assert source == DEST_IDENTITY_READBACK


def test_stamp_vector_census_qdrant_truncated_scan_is_unmeasured(monkeypatch):
    def fake_scan(cfg, *, table_name, max_entities):
        return SOURCE_ID_SCAN_TRUNCATED, []

    monkeypatch.setattr("connectors.qdrant_writer.scan_source_ids", fake_scan)
    stamped = stamp_vector_census(
        {"target_rows": 5, "target_checksum": "writer"},
        {"host": "127.0.0.1", "port": 6333},
        schema="",
        table_name="docs",
        dest_engine="qdrant",
    )
    assert IDENTITY_COUNT_KEY not in stamped
    assert stamped.get("dest_count_source") == "skipped_identity_readback"
    count, source = dest_count_from_recon(stamped)
    assert count is None
    assert source == DEST_UNMEASURED


def test_current_readback_closes_on_is_current_not_history_count():
    """SCD2 hole: 2 current / 3 history / writer 10,000 never closes dest as 3."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 3,
            "target_checksum": "writer-active",
            "skipped_readback": True,
            "unproven": True,
            "migration_proven": False,
            "dest_count_source": DEST_CURRENT_READBACK,
            CURRENT_ROWS_KEY: 2,
            HISTORY_ROWS_KEY: 3,
            "message": "SCD2 merge — Gate-8 stuffed active_rows is writer-path",
        }
    )
    assert count == 2
    assert source == DEST_CURRENT_READBACK


def test_current_source_without_current_rows_is_unmeasured():
    """Forged dest_count_source + stuffed history COUNT(*) is still not dest."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "target_checksum": "abc123",
            "skipped_readback": True,
            "dest_count_source": DEST_CURRENT_READBACK,
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_skipped_current_readback_refuses_physical_history_count():
    count, source = dest_count_from_recon(
        {
            "target_rows": 3,
            "target_checksum": "abc123",
            "dest_count_source": "skipped_current_readback",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_scd2_first_load_closes_on_current_not_history_or_writer_ack():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_CURRENT_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 2},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.rows_written == 2
    assert ledger.rows_written_source == DEST_CURRENT_READBACK
    assert ledger.current_count == 2
    assert ledger.history_rows == 2
    assert ledger.dest_count == 2
    assert ledger.active_count is None
    assert ledger.dest_delta is None
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack_delta == -9998
    assert "is_current" in ledger.note.lower() or "current" in ledger.note.lower()
    assert "history" in ledger.note.lower()


def test_scd2_physical_history_does_not_close_as_overwrite_surplus():
    """2 identities / 3 history rows must not close overwrite as dest=3."""
    ledger = account_population(
        rows_read=2,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert ledger.rows_written_source == DEST_UNMEASURED
    assert ledger.active_count is None


def test_scd2_incremental_change_batch_stays_unproven():
    """Watermarked SCD2: reader=1 changed row, current=2, history=3."""
    ledger = account_population(
        rows_read=1,
        dest_count=2,
        dest_count_source=DEST_CURRENT_READBACK,
        dest_count_before=3,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=1,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is False
    assert ledger.unaccounted is None
    assert ledger.rows_written_source == DEST_CURRENT_READBACK
    assert ledger.dest_count == 2
    assert ledger.history_rows == 3
    assert ledger.dest_delta is None
    assert "incremental" in ledger.note.lower()


def test_scd2_full_snapshot_resync_closes_when_reader_equals_current():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_CURRENT_READBACK,
        dest_count_before=3,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=1,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is True
    assert ledger.unaccounted == 0
    assert ledger.dest_count == 2
    assert ledger.history_rows == 3
    assert ledger.dest_delta is None
    assert "re-sync" in ledger.note.lower() or "resync" in ledger.note.lower()


def test_scd2_writer_active_checksum_is_not_mirror_and_does_not_close():
    """SCD2 dest_summary stamps active_rows + active_checksum — that is not _deleted."""
    ledger = account_job(
        {
            "sync_mode": "scd2",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 2,
                "target_rows": 2,
                "target_checksum": "writer-active",
            },
            "destination_summary": {
                "active_rows": 2,
                "active_checksum": "writer-active",
                "scd2": {"active_rows": 2, "rows_written": 1},
            },
        }
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert ledger.rows_written_source == DEST_UNMEASURED
    assert ledger.active_count is None


def test_scd2_kind_is_not_overwrite_or_keyed_even_when_dest_is_empty():
    assert conservation_kind("scd2", dest_count_before=0) == KIND_SCD2
    assert conservation_kind("scd2", dest_count_before=3) == KIND_SCD2
    assert conservation_kind("slowly_changing_dimension", dest_count_before=0) == KIND_SCD2


def test_stamp_scd2_census_pgvector_is_a_noop():
    stamped = stamp_scd2_census(
        {"target_rows": 5, "target_checksum": "abc"},
        {"host": "127.0.0.1"},
        schema="public",
        table_name="docs",
        dest_engine="pgvector",
    )
    assert CURRENT_ROWS_KEY not in stamped
    assert stamped.get("dest_count_source") != DEST_COUNT_CURRENT


def test_scd2_live_sqlite_current_not_history(tmp_path: Path):
    """apply_scd2 twice: current=2, history=3; conservation uses 2 not 3."""
    from src.transfer.models import EndpointConfig

    from services.scd2_engine import apply_scd2

    db = tmp_path / "scd2_current.db"
    endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{db}",
        database=str(db),
        table="products",
    )
    columns = ["id", "name", "price"]
    schema = {"id": "string", "name": "string", "price": "decimal"}
    first = [
        {"id": "1", "name": "A", "price": "10.00"},
        {"id": "2", "name": "B", "price": "20.00"},
    ]
    apply_scd2(endpoint, first, columns, schema, None, ["id"])
    missing = count_scd2_current("sqlite", {"database": str(db)}, schema="", table_name="gone")
    assert missing == 0
    first_current = count_scd2_current(
        "sqlite", {"database": str(db)}, schema="", table_name="products"
    )
    assert first_current == 2
    changed = [
        {"id": "1", "name": "A-updated", "price": "10.00"},
        {"id": "2", "name": "B", "price": "20.00"},
    ]
    apply_scd2(endpoint, changed, columns, schema, None, ["id"])
    stamped = stamp_scd2_census(
        {
            "source_rows": 2,
            "target_rows": 10_000,
            "target_checksum": "writer-active",
            "skipped_readback": True,
        },
        {"database": str(db), "type": "sqlite"},
        schema="",
        table_name="products",
        dest_engine="sqlite",
    )
    assert stamped[CURRENT_ROWS_KEY] == 2
    assert stamped[HISTORY_ROWS_KEY] == 3
    assert stamped["dest_count_source"] == DEST_COUNT_CURRENT
    assert stamped["target_rows"] == 10_000
    count, source = dest_count_from_recon(stamped)
    assert count == 2
    assert source == DEST_CURRENT_READBACK
    ledger = account_population(
        rows_read=2,
        dest_count=count,
        dest_count_source=source,
        dest_count_before=2,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.balanced is True
    assert ledger.dest_count == 2
    assert ledger.history_rows == 3
    assert ledger.writer_ack == 10_000

    bare = stamp_scd2_census(
        {"target_rows": 4},
        {"database": str(db), "type": "sqlite"},
        schema="",
        table_name="products",
        dest_engine="sqlite",
    )
    # Table without is_current: create a non-SCD2 table.
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE plain (id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO plain (id) VALUES ('x')")
        conn.commit()
    none = count_scd2_current(
        "sqlite", {"database": str(db)}, schema="", table_name="plain"
    )
    assert none is None
    skipped = stamp_scd2_census(
        {"target_rows": 1},
        {"database": str(db)},
        schema="",
        table_name="plain",
        dest_engine="sqlite",
    )
    assert skipped.get("dest_count_source") == "skipped_current_readback"
    assert CURRENT_ROWS_KEY not in skipped
    assert bare[CURRENT_ROWS_KEY] == 2


def test_job_rollup_two_scd2_streams_sums_current_not_history():
    def _scd2(name: str, current: int, history: int) -> dict:
        return {
            "name": name,
            "row_accounting": account_population(
                rows_read=current,
                dest_count=current,
                dest_count_source=DEST_CURRENT_READBACK,
                dest_count_before=0,
                rejected_rows=0,
                coerced_null_rows=0,
                rows_skipped=0,
                writer_ack=history * 100,
                sync_mode="scd2",
                scd2={"current_rows": current, "history_rows": history},
            ).to_dict(),
        }

    job = {
        "records_processed": 10_000,
        "destination_summary": {
            "streams": [
                _scd2("dim_a", 2, 3),
                _scd2("dim_b", 3, 5),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is True
    assert ledger.dest_count == 5
    assert ledger.rows_written == 5
    assert ledger.rows_written_source == DEST_CURRENT_READBACK
    assert ledger.active_count is None
    assert ledger.per_stream[0]["dest_count"] == 2
    assert ledger.per_stream[1]["dest_count"] == 3


def test_account_job_scd2_recon_never_uses_writer_or_history_count():
    ledger = account_job(
        {
            "sync_mode": "scd2",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 2,
                "target_rows": 3,
                "target_checksum": "history",
                "skipped_readback": True,
                "dest_count_source": DEST_CURRENT_READBACK,
                CURRENT_ROWS_KEY: 2,
                HISTORY_ROWS_KEY: 3,
                "target_rows_before": 0,
            },
        }
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.dest_count == 2
    assert ledger.current_count == 2
    assert ledger.history_rows == 3
    assert ledger.writer_ack == 10_000
    assert ledger.balanced is True
    assert ledger.active_count is None


def test_count_star_nets_missing_and_extra_keys_to_a_false_balance():
    """DMS hole: dest {2,3,99} vs source {1,2,3} is COUNT(*)=3 but not the same keys."""
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="full_refresh_overwrite",
        keyset={MISSING_KEYS_KEY: 1, EXTRA_KEYS_KEY: 1},
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.unaccounted == 0
    assert ledger.balanced is False
    assert ledger.missing_keys == 1
    assert ledger.extra_keys == 1
    assert ledger.writer_ack == 10_000
    assert "MISSING_TARGET" in ledger.note
    assert "EXTRA_TARGET" in ledger.note
    assert "inferred" in ledger.note.lower()


def test_keyset_closed_when_every_source_key_is_on_dest_and_no_extras():
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=3,
        sync_mode="full_refresh_overwrite",
        keyset={MISSING_KEYS_KEY: 0, EXTRA_KEYS_KEY: 0},
    )
    assert ledger.balanced is True
    assert ledger.missing_keys == 0
    assert ledger.extra_keys == 0
    assert "keyset closed" in ledger.note.lower()


def test_incremental_without_keyset_does_not_invent_leftover_from_batch():
    """A CDC batch is not S. dest_count − hits(batch) would be almost every dest row."""
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=3,
        sync_mode="full_refresh_overwrite",
    )
    assert ledger.balanced is True
    assert ledger.missing_keys is None
    assert ledger.extra_keys is None


def test_records_to_key_tuples_requires_one_row_one_key():
    assert records_to_key_tuples(
        [{"id": 1}, {"id": 2}, {"id": 3}],
        ["id"],
    ) == [(1,), (2,), (3,)]
    assert records_to_key_tuples([{"id": 1}, {"id": 1}], ["id"]) is None
    assert records_to_key_tuples([{"id": 1}, {"name": "x"}], ["id"]) is None
    assert records_to_key_tuples([], ["id"]) is None


def test_stamp_keyset_census_does_not_run_on_pgvector():
    stamped = stamp_keyset_census(
        {"target_rows": 3},
        {},
        schema="public",
        table_name="docs",
        dest_engine="pgvector",
        key_columns=["id"],
        keys=[(1,), (2,)],
    )
    assert MISSING_KEYS_KEY not in stamped
    assert EXTRA_KEYS_KEY not in stamped


def test_failed_gate8_still_exposes_independent_dest_count():
    """MISSING_TARGET class: dest COUNT is 9997 even though the write 'succeeded'."""
    count, source = dest_count_from_recon(
        {
            "passed": False,
            "phase": "post_write_failed",
            "target_rows": 9997,
            "source_rows": 10_000,
            "message": "Row count mismatch",
        }
    )
    assert count == 9997
    assert source == DEST_READBACK


def test_overwrite_balances_on_dest_count_not_writer_ack():
    ledger = account_population(
        rows_read=10_000,
        dest_count=9997,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="full_refresh_overwrite",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.rows_written == 9997
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 3
    assert ledger.balanced is False
    assert ledger.writer_ack_delta == -3


def test_coerced_null_rows_are_on_the_destination():
    ledger = account_population(
        rows_read=10,
        dest_count=10,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=2,
        coerced_null_rows=2,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="overwrite",
    )
    assert ledger.rows_quarantined == 0
    assert ledger.unaccounted == 0
    assert ledger.balanced is True


def test_true_quarantine_hold_outs_close_with_dest_count():
    ledger = account_population(
        rows_read=10,
        dest_count=8,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=2,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=8,
        sync_mode="overwrite",
    )
    assert ledger.rows_quarantined == 2
    assert ledger.balanced is True


def test_append_uses_dest_delta_not_whole_table_count():
    ledger = account_population(
        rows_read=10,
        dest_count=40,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="full_refresh_append",
    )
    assert ledger.conservation_kind == KIND_APPEND_DELTA
    assert ledger.rows_written == 10
    assert ledger.balanced is True


def test_append_without_precount_is_unmeasured():
    ledger = account_population(
        rows_read=10,
        dest_count=40,
        dest_count_source=DEST_READBACK,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="append",
    )
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert "Append delta unverified" in ledger.note


def test_upsert_into_nonempty_dest_has_no_count_identity():
    ledger = account_population(
        rows_read=10,
        dest_count=35,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
    )
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.balanced is False
    assert ledger.rows_written is None


def test_upsert_into_empty_dest_is_insert_cardinality():
    ledger = account_population(
        rows_read=10,
        dest_count=10,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.balanced is True


def test_incremental_empty_pass_is_measured_zero():
    ledger = account_population(
        rows_read=0,
        dest_count=None,
        dest_count_source=DEST_UNMEASURED,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=0,
        sync_mode="incremental_append",
    )
    assert ledger.conservation_kind == KIND_EMPTY_PASS
    assert ledger.balanced is True
    assert ledger.unaccounted == 0


def test_account_job_ignores_records_processed_when_dest_count_exists():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "phase": "post_write_verified",
            "source_rows": 10_000,
            "target_rows": 9997,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "target_checksum": "deadbeef",
            "message": "Verified",
        },
        "destination_summary": {"rows": 10_000, "rejected": 50},
    }
    ledger = account_job(job)
    assert ledger.rows_written == 9997
    assert ledger.writer_ack == 10_000
    assert ledger.rows_quarantined == 0
    assert ledger.balanced is False


def test_extract_batch_keys_are_distinct_and_skip_nulls():
    from services.row_conservation import extract_batch_keys

    keys = extract_batch_keys(
        [
            {"id": 1, "label": "a"},
            {"id": 1, "label": "a2"},
            {"id": 2, "label": "b"},
            {"id": None, "label": "x"},
        ],
        ["id"],
    )
    assert keys == [(1,), (2,)]


def test_key_census_splits_inserts_from_updates():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=10, dest_preexisting=7)
    assert census.inserts == 3
    assert census.updates == 7
    assert census.deletes == 0
    assert census.expected_delta == 3
    assert KeyCensus.from_mapping({"unique_batch_keys": 2, "dest_preexisting": 5}) is None


def test_keyed_census_closes_on_dest_delta_not_writer_ack():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=10, dest_preexisting=9)
    ledger = account_population(
        rows_read=10,
        dest_count=31,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.inserts == 1
    assert ledger.updates == 9
    assert ledger.dest_delta == 1
    assert ledger.rows_written == 1
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack == 10
    assert ledger.writer_ack_delta == -9


def test_keyed_census_detects_dest_shortfall():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=4, dest_preexisting=1)
    ledger = account_population(
        rows_read=4,
        dest_count=32,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=4,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.inserts == 3
    assert ledger.dest_delta == 2
    assert ledger.unaccounted == 1
    assert ledger.balanced is False


def test_keyed_census_with_quarantine_stays_unproven():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=4, dest_preexisting=3)
    ledger = account_population(
        rows_read=4,
        dest_count=31,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=1,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=3,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert "quarantined" in ledger.note


def test_account_job_reads_keyed_census_from_dest_summary():
    job = {
        "records_processed": 10,
        "sync_mode": "upsert",
        "reconciliation": {
            "phase": "post_write_verified",
            "source_rows": 10,
            "target_rows": 31,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "target_checksum": "deadbeef",
            "message": "Verified",
        },
        "destination_summary": {
            "target_rows_before": 30,
            "keyed_census": {"unique_batch_keys": 10, "dest_preexisting": 9},
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.balanced is True
    assert ledger.inserts == 1
    assert ledger.updates == 9
    assert ledger.rows_written == 1
    assert ledger.writer_ack == 10


def test_sqlite_destination_key_hits_are_dest_engine_distinct(tmp_path: Path):
    import sqlite3

    from services.dest_precount import destination_key_hits

    path = tmp_path / "p9_hits.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()
    hits = destination_key_hits(
        "sqlite",
        {"database": str(path)},
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (9,), (1,)],
    )
    assert hits == 2
    empty = destination_key_hits(
        "sqlite",
        {"database": str(path)},
        schema="",
        table_name="missing",
        key_columns=["id"],
        keys=[(1,)],
    )
    assert empty == 0


def test_stream_accumulator_reconstructs_preexisting_from_per_batch_hits():
    from services.row_conservation import KeyCensusAccumulator

    acc = KeyCensusAccumulator()
    acc.add_batch([(1,), (2,)], dest_hits=2)
    acc.add_batch([(3,), (4,)], dest_hits=1)
    census = acc.to_census()
    assert census is not None
    assert census.unique_batch_keys == 4
    assert census.dest_preexisting == 3
    assert census.inserts == 1
    assert census.updates == 3
    assert census.deletes == 0


def test_partition_last_op_wins_delete_then_insert_is_live():
    from services.row_conservation import partition_keyed_records

    part = partition_keyed_records(
        [
            {"id": 1, "label": "gone", "__deleted": True},
            {"id": 1, "label": "back", "__deleted": False},
            {"id": 2, "label": "x", "__deleted": False},
            {"id": 2, "label": "x2", "__op": "d"},
        ],
        ["id"],
    )
    assert part.live_keys == [(1,)]
    assert part.tombstone_keys == [(2,)]
    assert part.live_records[0]["label"] == "back"


def test_keyed_census_tombstone_of_missing_key_is_not_an_insert():
    from services.row_conservation import KeyCensus

    # 3 live keys dest already holds + 1 tombstone dest does not hold.
    census = KeyCensus(
        unique_batch_keys=3,
        dest_preexisting=3,
        tombstones=0,
        unique_tombstone_keys=1,
    )
    assert census.inserts == 0
    assert census.deletes == 0
    assert census.expected_delta == 0


def test_keyed_census_closes_on_insert_minus_dest_held_delete():
    from services.row_conservation import KeyCensus

    census = KeyCensus(
        unique_batch_keys=3,
        dest_preexisting=2,
        tombstones=1,
        unique_tombstone_keys=1,
    )
    assert census.inserts == 1
    assert census.updates == 2
    assert census.deletes == 1
    assert census.expected_delta == 0
    ledger = account_population(
        rows_read=4,
        dest_count=30,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.balanced is True
    assert ledger.dest_delta == 0
    assert ledger.rows_written == 0
    assert ledger.deletes == 1
    assert ledger.inserts == 1
    assert ledger.writer_ack_delta == -10_000


def test_stream_accumulator_delete_only_batch_is_a_census():
    from services.row_conservation import KeyCensusAccumulator

    acc = KeyCensusAccumulator()
    acc.add_batch([], dest_hits=0)
    acc.add_tombstones(2, unique_keys=3)
    census = acc.to_census()
    assert census is not None
    assert census.unique_batch_keys == 0
    assert census.inserts == 0
    assert census.deletes == 2
    assert census.unique_tombstone_keys == 3
    assert census.expected_delta == -2


def test_sqlite_prepare_keyed_upsert_hard_deletes_dest_held_keys(tmp_path: Path):
    import sqlite3

    from services.row_conservation import prepare_keyed_upsert

    path = tmp_path / "p9_tomb.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()

    live, payload = prepare_keyed_upsert(
        [
            {"id": 1, "label": "A", "is_deleted": 0},
            {"id": 2, "label": "b", "is_deleted": 1},
            {"id": 4, "label": "d", "is_deleted": 0},
            {"id": 9, "label": "ghost", "is_deleted": 1},
        ],
        key_columns=["id"],
        mappings=None,
        db_type="sqlite",
        cfg={"database": str(path)},
        schema="",
        table_name="items",
        dest_nonempty=True,
    )
    assert [r["id"] for r in live] == [1, 4]
    assert payload is not None
    assert payload["inserts"] == 1
    assert payload["updates"] == 1
    assert payload["deletes"] == 1
    assert payload["unique_tombstone_keys"] == 2
    assert payload["expected_delta"] == 0

    conn = sqlite3.connect(str(path))
    try:
        rows = list(conn.execute("SELECT id, label FROM items ORDER BY id"))
        count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    finally:
        conn.close()
    # Hard-DELETE of dest-held key 2; key 9 was never present (no-op).
    # Live upserts have not run yet — prepare only strips + deletes.
    assert count == 2
    assert rows == [(1, "a"), (3, "c")]


def test_mysql_hard_delete_survives_connection_close():
    """PyMySQL ``autocommit = True`` assignment used to roll back DELETE on close."""
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=0.4):
            pass
    except OSError:
        import pytest

        pytest.skip("MariaDB not listening")

    import uuid

    import pymysql

    from services.row_conservation import apply_hard_deletes

    cfg = {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    table = f"p9_mysql_del_{uuid.uuid4().hex[:8]}"
    conn = pymysql.connect(
        host=cfg["host"], port=cfg["port"], database=cfg["database"],
        user=cfg["username"], password=cfg["password"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, label VARCHAR(8))"
            )
            cur.execute(
                f"INSERT INTO `{table}` (id, label) VALUES (1,'a'),(2,'b'),(3,'c')"
            )
        deleted = apply_hard_deletes(
            db_type="mysql",
            cfg=cfg,
            schema="",
            table_name=table,
            key_columns=["id"],
            keys=[(2,)],
        )
        assert deleted == 1
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM `{table}` ORDER BY id")
            left = [int(r[0]) for r in cur.fetchall()]
        assert left == [1, 3]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()


def test_attach_conservation_only_on_terminal():
    from services.row_conservation import attach_conservation_to_updates

    running = {"records_processed": 10}
    attach_conservation_to_updates("running", running)
    assert "row_accounting" not in running

    done = {
        "records_processed": 10_000,
        "sync_mode": "full_refresh_overwrite",
        "reconciliation": {
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "abc",
            "source_checksum": "abc",
            "phase": "post_write_verified",
            "coverage": "full",
            "assurance_level": "full_checksum",
        },
    }
    attach_conservation_to_updates("completed", done)
    ledger = done["row_accounting"]
    assert ledger["dest_count"] == 4
    assert ledger["rows_written"] == 4
    assert ledger["writer_ack"] == 10_000
    assert ledger["writer_ack_delta"] != 0
    assert ledger["rows_written_source"] == DEST_READBACK
    assert ledger["balanced"] is True


def test_ledger_from_transfer_result_does_not_close_on_writer_ack():
    from dataclasses import dataclass, field

    from services.row_conservation import ledger_from_transfer_result

    @dataclass
    class _Result:
        records_transferred: int = 10_000
        operation: str = "full_refresh_overwrite"
        destination_summary: dict = field(default_factory=dict)
        reconciliation: dict = field(default_factory=dict)

    result = _Result(
        reconciliation={
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "abc",
            "source_checksum": "abc",
            "phase": "post_write_verified",
            "coverage": "full",
            "assurance_level": "full_checksum",
        },
    )
    ledger = ledger_from_transfer_result(result, sync_mode="full_refresh_overwrite")
    assert ledger["dest_count"] == 4
    assert ledger["writer_ack"] == 10_000
    assert ledger["rows_written"] != ledger["writer_ack"]
    assert ledger["balanced"] is True


def test_mirror_kind_is_not_overwrite_even_on_empty_dest():
    assert conservation_kind("full_refresh_mirror", dest_count_before=0) == KIND_MIRROR
    assert conservation_kind("mirror", dest_count_before=3) == KIND_MIRROR


def test_mirror_closes_on_active_population_not_physical_or_writer_ack():
    """Gate-8 stuffs target_rows with COUNT(*) WHERE NOT _deleted.

    Physical COUNT(*) stays (Fivetran _fivetran_deleted hole). Writer ack
    of 10,000 must not close the identity or hide leftover dest keys.
    """
    ledger = account_job(
        {
            "sync_mode": "full_refresh_mirror",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "active-digest",
                "source_checksum": "source-digest",
                "phase": "post_write_verified",
                "coverage": "full",
            },
            "destination_summary": {
                "mirror": {
                    "mode": "mirror",
                    "active_rows": 3,
                    "soft_deleted": 1,
                    "reactivated": 0,
                    "rows_scanned": 4,
                    "soft_delete_column": "_deleted",
                }
            },
        }
    ).to_dict()
    assert ledger["conservation_kind"] == KIND_MIRROR
    assert ledger["balanced"] is True
    assert ledger["active_count"] == 3
    assert ledger["rows_written"] == 3
    assert ledger["dest_count"] == 4
    assert ledger["inferred_deletes"] == 1
    assert ledger["reactivated"] == 0
    assert ledger["writer_ack"] == 10_000
    assert ledger["writer_ack_delta"] != 0
    assert ledger["rows_written_source"] == DEST_ACTIVE_READBACK
    assert ledger["unaccounted"] == 0


def test_mirror_stream_path_closes_on_top_level_active_rows():
    """Stream path stamps active_rows at dest_summary top-level; no rows_scanned."""
    ledger = account_job(
        {
            "sync_mode": "mirror",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "active-digest",
            },
            "destination_summary": {
                "active_rows": 3,
                "active_checksum": "active-digest",
                "soft_delete_column": "_deleted",
            },
        }
    ).to_dict()
    assert ledger["conservation_kind"] == KIND_MIRROR
    assert ledger["balanced"] is True
    assert ledger["active_count"] == 3
    assert ledger["rows_written"] == 3
    assert ledger["dest_count"] is None
    assert ledger["inferred_deletes"] is None
    assert ledger["writer_ack"] == 10_000
    assert ledger["rows_written_source"] == DEST_ACTIVE_READBACK


def test_mirror_without_active_census_is_unmeasured():
    ledger = account_job(
        {
            "sync_mode": "mirror",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "stuffed",
            },
            "destination_summary": {},
        }
    ).to_dict()
    assert ledger["conservation_kind"] == KIND_MIRROR
    assert ledger["balanced"] is False
    assert ledger["rows_written"] is None
    assert ledger["rows_written_source"] == DEST_UNMEASURED
    assert ledger["active_count"] is None


def test_accumulator_redelivery_of_same_key_is_not_a_second_insert():
    from services.row_conservation import KeyCensusAccumulator

    acc = KeyCensusAccumulator()
    acc.add_events(5)
    acc.add_batch([(1,)], dest_hits=0)
    acc.add_events(5)
    acc.add_batch([(1,)], dest_hits=0)
    census = acc.to_census()
    assert census is not None
    assert census.unique_batch_keys == 1
    assert census.inserts == 1
    assert census.dest_preexisting == 0
    assert census.events_read == 10
    assert census.expected_delta == 1


def test_keyed_ledger_closes_on_keys_not_duplicate_events():
    from services.row_conservation import KeyCensus

    census = KeyCensus(
        unique_batch_keys=3,
        dest_preexisting=3,
        events_read=10,
    )
    ledger = account_population(
        rows_read=10,
        dest_count=30,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="cdc",
        census=census,
    )
    assert ledger.balanced is True
    assert ledger.dest_delta == 0
    assert ledger.inserts == 0
    assert ledger.updates == 3
    assert ledger.events_read == 10
    assert ledger.unique_batch_keys == 3
    assert ledger.writer_ack == 10_000
    assert "10 event" in ledger.note
    assert "3 live key" in ledger.note


def test_sqlite_duplicate_events_census_is_keys_not_rowcount(tmp_path: Path):
    import sqlite3

    from services.row_conservation import prepare_keyed_upsert

    path = tmp_path / "p9_events.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()

    live, payload = prepare_keyed_upsert(
        [
            {"id": 1, "label": "A"},
            {"id": 1, "label": "A2"},
            {"id": 2, "label": "B"},
            {"id": 2, "label": "B2"},
            {"id": 3, "label": "C"},
            {"id": 3, "label": "C2"},
        ],
        key_columns=["id"],
        mappings=None,
        db_type="sqlite",
        cfg={"database": str(path)},
        schema="",
        table_name="items",
        dest_nonempty=True,
    )
    assert payload is not None
    assert payload["events_read"] == 6
    assert payload["unique_batch_keys"] == 3
    assert payload["dest_preexisting"] == 3
    assert payload["inserts"] == 0
    assert payload["expected_delta"] == 0
    assert len(live) == 3

    from services.dest_precount import PRECOUNT_KEY

    ledger = account_job(
        {
            "sync_mode": "cdc",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 6,
                "target_rows": 3,
                "target_checksum": "dest-digest",
                PRECOUNT_KEY: 3,
            },
            "destination_summary": {
                PRECOUNT_KEY: 3,
                "keyed_census": payload,
            },
        }
    )
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.balanced is True
    assert ledger.events_read == 6
    assert ledger.unique_batch_keys == 3
    assert ledger.dest_delta == 0
    assert ledger.writer_ack == 10_000


def _overwrite_stream(name: str, dest_count: int, *, writer_ack: int | None = None) -> dict:
    ack = dest_count if writer_ack is None else writer_ack
    return {
        "name": name,
        "records_processed": ack,
        "row_accounting": account_job(
            {
                "records_processed": ack,
                "sync_mode": "overwrite",
                "reconciliation": {
                    "source_rows": dest_count,
                    "target_rows": dest_count,
                    "target_checksum": f"digest-{name}",
                    "phase": "post_write_row_count",
                    "coverage": "row_count",
                },
            }
        ).to_dict(),
    }


def test_job_rollup_sums_overwrite_streams_not_last_table():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "source_rows": 3,
            "target_rows": 3,
            "target_checksum": "last-table-only",
            "coverage": "row_count",
        },
        "destination_summary": {
            "streams": [
                _overwrite_stream("customers", 2, writer_ack=10_000),
                _overwrite_stream("orders", 3, writer_ack=10_000),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.dest_count == 5
    assert ledger.rows_read == 5
    assert ledger.balanced is True
    assert ledger.summable is True
    assert ledger.stream_count == 2
    assert ledger.measured_streams == 2
    assert ledger.writer_ack == 20_000
    assert ledger.writer_ack_delta == 5 - 20_000
    assert ledger.per_stream[0]["stream"] == "customers"
    assert ledger.per_stream[1]["dest_count"] == 3


def test_job_rollup_open_when_first_stream_unmeasured():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "source_rows": 3,
            "target_rows": 3,
            "target_checksum": "last-table-only",
            "coverage": "row_count",
        },
        "destination_summary": {
            "streams": [
                {"name": "customers", "records_processed": 2},
                _overwrite_stream("orders", 3),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is False
    assert ledger.dest_count is None
    assert ledger.measured_streams == 1
    assert ledger.stream_count == 2


def test_job_rollup_open_when_first_stream_unbalanced():
    short = account_job(
        {
            "records_processed": 2,
            "sync_mode": "overwrite",
            "reconciliation": {
                "source_rows": 4,
                "target_rows": 2,
                "target_checksum": "short",
                "coverage": "row_count",
            },
        }
    ).to_dict()
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "destination_summary": {
            "streams": [
                {"name": "customers", "row_accounting": short},
                _overwrite_stream("orders", 3),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is False
    assert ledger.summable is True
    assert ledger.dest_count == 5


def test_job_rollup_does_not_sum_mixed_kinds():
    keyed = account_job(
        {
            "sync_mode": "cdc",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 4,
                "target_checksum": "k",
                "target_rows_before": 3,
            },
            "destination_summary": {
                "target_rows_before": 3,
                "keyed_census": {
                    "unique_batch_keys": 4,
                    "dest_preexisting": 3,
                    "tombstones": 0,
                    "unique_tombstone_keys": 0,
                    "events_read": 10,
                },
            },
        }
    ).to_dict()
    job = {
        "records_processed": 10_000,
        "destination_summary": {
            "streams": [
                _overwrite_stream("customers", 2),
                {"name": "orders", "row_accounting": keyed},
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is False
    assert ledger.dest_count is None
    assert ledger.rows_written_source == DEST_PER_STREAM


def test_single_stream_still_uses_table_identity():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "one",
            "coverage": "row_count",
        },
        "destination_summary": {
            "streams": [_overwrite_stream("items", 4)],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.dest_count == 4
    assert account_job_streams(job["destination_summary"]["streams"]) is None


def test_dest_before_census_counts_once_per_table(tmp_path: Path):
    """Second capture must not observe dest-after (that would close a false delta)."""
    import sqlite3

    from src.transfer.models import EndpointConfig
    from services.dest_precount import DestBeforeCensus, count_endpoint_rows

    path = tmp_path / "before.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO items (id) VALUES (1), (2), (3)")
        conn.commit()
    finally:
        conn.close()
    endpoint = EndpointConfig(kind="database", format="sqlite", database=str(path), table="items")
    census = DestBeforeCensus()
    first = census.capture(endpoint, table_name="items", aliases=("items_alias",))
    assert first == 3
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("INSERT INTO items (id) VALUES (4)")
        conn.commit()
    finally:
        conn.close()
    second = census.capture(endpoint, table_name="items")
    assert second == 3
    assert census.get("items_alias") == 3
    assert count_endpoint_rows(endpoint, table_name="items") == 4
    summary: dict = {}
    assert census.stamp(summary, "items")
    assert summary["target_rows_before"] == 3


def _iceberg_cfg(warehouse: Path) -> dict:
    return {
        "connection_string": str(warehouse),
        "database": str(warehouse),
        "host": "",
        "schema": "",
    }


def test_iceberg_missing_table_is_measured_zero(tmp_path: Path):
    """Create-on-first-write is dest-before 0, not unmeasured."""
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "iceberg", _iceberg_cfg(tmp_path / "wh"), schema="", table_name="orders"
    )
    assert n == 0


def test_iceberg_filesystem_dest_count_and_key_hits_independent_of_writer(tmp_path: Path):
    """Lakehouse MERGE conservation: dest snapshot COUNT and key hits, not upsert ack."""
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import (
        DestBeforeCensus,
        count_endpoint_rows,
        destination_key_hits,
        destination_row_count,
    )
    from src.transfer.models import EndpointConfig

    warehouse = tmp_path / "wh"
    cfg = _iceberg_cfg(warehouse)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
        {"source": "_df_lsn", "target": "_df_lsn", "transform": "direct"},
    ]
    first = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v", "_df_lsn"],
        data_rows=[["1", "a", "0/10"], ["2", "b", "0/10"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert first.ok, first.error
    assert destination_row_count("iceberg", cfg, schema="", table_name="orders") == 2
    assert (
        destination_key_hits(
            "iceberg",
            cfg,
            schema="",
            table_name="orders",
            key_columns=["id"],
            keys=[("1",), ("9",)],
        )
        == 1
    )

    endpoint = EndpointConfig(
        kind="database",
        format="iceberg",
        connection_string=str(warehouse),
        database=str(warehouse),
        table="orders",
    )
    census = DestBeforeCensus()
    before = census.capture(endpoint, table_name="orders")
    assert before == 2
    second = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v", "_df_lsn"],
        data_rows=[["1", "a2", "0/20"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert second.ok, second.error
    assert census.capture(endpoint, table_name="orders") == 2
    assert count_endpoint_rows(endpoint, table_name="orders") == 2
    summary: dict = {}
    census.stamp(summary, "orders")
    assert summary["target_rows_before"] == 2


def test_write_destination_database_stamps_iceberg_dest_before(tmp_path: Path):
    """Adapters precount uses dest-engine Iceberg COUNT — missing table is 0."""
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    warehouse = tmp_path / "wh"
    dest = EndpointConfig(
        kind="database",
        format="iceberg",
        database=str(warehouse),
        table="orders",
        connection_string=str(warehouse),
    )
    records = [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]
    columns = ["id", "v"]
    mappings = [{"source": c, "target": c} for c in columns]
    schema = {"id": "string", "v": "string"}
    written, _ddl, summary = write_destination_database(
        dest, records, columns, schema, mappings
    )
    assert written == 2, summary
    assert summary.get("target_rows_before") == 0
    written2, _ddl2, summary2 = write_destination_database(
        dest, records, columns, schema, mappings
    )
    assert written2 == 2
    assert summary2.get("target_rows_before") == 2


def test_s3_missing_object_is_measured_zero():
    """Missing object is dest-before 0 (create-on-first-write), not writer ack."""
    moto = pytest.importorskip("moto")
    import boto3

    from services.dest_precount import destination_row_count

    with moto.mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="df-count")
        n = destination_row_count(
            "s3",
            {"database": "df-count", "host": "us-east-1"},
            schema="",
            table_name="exports/missing.json",
        )
        assert n == 0
        boto3.client("s3", region_name="us-east-1").put_object(
            Bucket="df-count",
            Key="exports/data.json",
            Body=b'[{"id":1},{"id":2}]',
        )
        assert (
            destination_row_count(
                "s3",
                {"database": "df-count", "host": "us-east-1"},
                schema="",
                table_name="exports/data.json",
            )
            == 2
        )


def test_job_rollup_two_keyed_streams_closed_not_summed():
    """Keyed dest COUNT(*) is not additive. Job dest stays per-stream."""
    def _keyed(name: str, *, before: int, after: int, inserts: int, updates: int) -> dict:
        return {
            "name": name,
            "row_accounting": account_job(
                {
                    "sync_mode": "cdc",
                    "records_processed": 10_000,
                    "reconciliation": {
                        "source_rows": inserts + updates,
                        "target_rows": after,
                        "target_checksum": f"k-{name}",
                        "target_rows_before": before,
                    },
                    "destination_summary": {
                        "target_rows_before": before,
                        "keyed_census": {
                            "unique_batch_keys": inserts + updates,
                            "dest_preexisting": updates,
                            "tombstones": 0,
                            "unique_tombstone_keys": 0,
                            "events_read": inserts + updates,
                        },
                    },
                }
            ).to_dict(),
        }

    job = {
        "records_processed": 10_000,
        "sync_mode": "cdc",
        "destination_summary": {
            "streams": [
                _keyed("customers", before=2, after=3, inserts=1, updates=2),
                _keyed("orders", before=3, after=3, inserts=0, updates=3),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is False
    assert ledger.dest_count is None
    assert ledger.rows_written_source == DEST_PER_STREAM
    assert ledger.per_stream[0]["conservation_kind"] == KIND_KEYED
    assert ledger.per_stream[1]["conservation_kind"] == KIND_KEYED
    assert ledger.per_stream[0]["dest_count"] == 3
    assert ledger.per_stream[1]["dest_count"] == 3


def test_job_rollup_two_vector_streams_sums_identities_not_chunks():
    def _vector(name: str, identities: int, vectors: int) -> dict:
        return {
            "name": name,
            "row_accounting": account_population(
                rows_read=identities,
                dest_count=identities,
                dest_count_source=DEST_IDENTITY_READBACK,
                dest_count_before=0,
                rejected_rows=0,
                coerced_null_rows=0,
                rows_skipped=0,
                writer_ack=vectors * 100,
                sync_mode="full_refresh_overwrite",
                vector={"identity_rows": identities, "vector_rows": vectors},
            ).to_dict(),
        }

    job = {
        "records_processed": 10_000,
        "destination_summary": {
            "streams": [
                _vector("docs_a", 2, 5),
                _vector("docs_b", 3, 9),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is True
    assert ledger.dest_count == 5
    assert ledger.rows_written == 5
    assert ledger.rows_written_source == DEST_IDENTITY_READBACK
    assert ledger.per_stream[0]["dest_count"] == 2
    assert ledger.per_stream[1]["dest_count"] == 3


def test_account_job_vector_recon_never_uses_writer_or_chunk_count():
    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 2,
                "target_rows": 5,
                "target_checksum": "chunks",
                "skipped_readback": True,
                "dest_count_source": DEST_IDENTITY_READBACK,
                IDENTITY_COUNT_KEY: 2,
                VECTOR_ROWS_KEY: 5,
                "target_rows_before": 0,
            },
        }
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.dest_count == 2
    assert ledger.identity_count == 2
    assert ledger.vector_rows == 5
    assert ledger.writer_ack == 10_000
    assert ledger.balanced is True


def _pg_up() -> bool:
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_cfg() -> dict:
    import os

    return {
        "host": os.environ.get("P9_PG_HOST", "127.0.0.1"),
        "port": int(os.environ.get("P9_PG_PORT", "5432")),
        "database": os.environ.get("P9_PG_DB", "dataflow"),
        "username": os.environ.get("P9_PG_USER", "dataflow"),
        "password": os.environ.get("P9_PG_PASSWORD", "dataflow"),
    }


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_pgvector_identity_count_distinct_source_id_not_physical_rows():
    """Identity COUNT does not require the vector extension — source_id is TEXT."""
    import psycopg2

    from services.dest_precount import DestBeforeCensus, destination_row_count, stamp_vector_census
    from src.transfer.models import EndpointConfig

    cfg = _pg_cfg()
    table = "p9_vector_identity_chunks"
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["username"],
        password=cfg["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
            cur.execute(
                f"CREATE TABLE public.{table} (id TEXT PRIMARY KEY, source_id TEXT, chunk_index INT)"
            )
            cur.executemany(
                f"INSERT INTO public.{table} (id, source_id, chunk_index) VALUES (%s, %s, %s)",
                [
                    ("d1-0", "doc-1", 0),
                    ("d1-1", "doc-1", 1),
                    ("d1-2", "doc-1", 2),
                    ("d2-0", "doc-2", 0),
                    ("d2-1", "doc-2", 1),
                ],
            )
        conn.commit()
        assert destination_row_count("pgvector", cfg, schema="public", table_name=table) == 2
        assert destination_row_count("postgresql", cfg, schema="public", table_name=table) == 5
        missing = destination_row_count(
            "pgvector", cfg, schema="public", table_name="p9_vector_identity_missing"
        )
        assert missing == 0

        stamped = stamp_vector_census(
            {"target_rows": 10_000, "target_checksum": "writer"},
            cfg,
            schema="public",
            table_name=table,
            dest_engine="pgvector",
        )
        assert stamped[IDENTITY_COUNT_KEY] == 2
        assert stamped["dest_count_source"] == DEST_IDENTITY_READBACK
        assert stamped[VECTOR_ROWS_KEY] == 10_000
        assert stamped["target_rows"] == 10_000
        count, source = dest_count_from_recon(stamped)
        assert count == 2
        assert source == DEST_IDENTITY_READBACK

        endpoint = EndpointConfig(
            kind="database",
            format="pgvector",
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            schema="public",
            table=table,
        )
        census = DestBeforeCensus()
        before = census.capture(endpoint, table_name=table)
        assert before == 2
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO public.{table} (id, source_id, chunk_index) VALUES (%s, %s, %s)",
                ("d3-0", "doc-3", 0),
            )
        conn.commit()
        assert census.capture(endpoint, table_name=table) == 2
        assert destination_row_count("pgvector", cfg, schema="public", table_name=table) == 3
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
        conn.commit()
        conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_pgvector_table_without_source_id_is_unmeasured_not_physical_count():
    import psycopg2

    from services.dest_precount import destination_row_count

    cfg = _pg_cfg()
    table = "p9_vector_no_source_id"
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["username"],
        password=cfg["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
            cur.execute(f"CREATE TABLE public.{table} (id TEXT PRIMARY KEY, body TEXT)")
            cur.execute(f"INSERT INTO public.{table} (id, body) VALUES ('a', 'x'), ('b', 'y')")
        conn.commit()
        assert destination_row_count("pgvector", cfg, schema="public", table_name=table) is None
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
        conn.commit()
        conn.close()


def test_sqlite_keyset_census_splits_missing_from_extra_target(tmp_path: Path):
    """Dest {2,3,99} vs source {1,2,3}: COUNT(*)=3, missing=1, extra=1."""
    import sqlite3

    path = tmp_path / "p9_keyset.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(2, "b"), (3, "c"), (99, "ghost")],
        )
        conn.commit()
    finally:
        conn.close()
    cfg = {"database": str(path)}
    census = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert census is not None
    assert census["dest_count"] == 3
    assert census["dest_key_hits"] == 2
    assert census[MISSING_KEYS_KEY] == 1
    assert census[EXTRA_KEYS_KEY] == 1

    stamped = stamp_keyset_census(
        {"target_rows": 3, "target_checksum": "same-count"},
        cfg,
        schema="",
        table_name="items",
        dest_engine="sqlite",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert stamped[MISSING_KEYS_KEY] == 1
    assert stamped[EXTRA_KEYS_KEY] == 1
    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "same-count",
                MISSING_KEYS_KEY: 1,
                EXTRA_KEYS_KEY: 1,
            },
        }
    )
    assert ledger.balanced is False
    assert ledger.unaccounted == 0
    assert ledger.missing_keys == 1
    assert ledger.extra_keys == 1
    assert ledger.writer_ack == 10_000


def test_inferred_leftover_delete_refuses_incomplete_snapshot(tmp_path: Path):
    """Incremental CDC must not infer-delete dest keys the batch did not send."""
    import sqlite3

    path = tmp_path / "p9_no_infer.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg={"database": str(path)},
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,)],
        complete_snapshot=False,
    )
    assert deleted is None
    assert destination_row_count(
        "sqlite", {"database": str(path)}, schema="", table_name="items"
    ) == 3


def test_inferred_leftover_delete_skips_pgvector():
    deleted = apply_inferred_leftover_deletes(
        db_type="pgvector",
        cfg={"host": "127.0.0.1"},
        schema="public",
        table_name="docs",
        key_columns=["id"],
        keys=[("a",), ("b",)],
        complete_snapshot=True,
    )
    assert deleted is None


def test_overwrite_merge_deletes_leftover_dest_keys_not_in_complete_s(tmp_path: Path):
    """Dest {1,2,3,99} vs S {1,2,3}: MERGE-delete 99. COUNT(*) becomes 3, extra=0.

    Fivetran would soft-flag 99 (_fivetran_deleted) so COUNT(*) stays 4.
    Airbyte incremental would leave 99. DMS EXTRA_TARGET measures 99.
    Complete overwrite snapshot hard-deletes dest \\ S, then proves extra=0.
    """
    import sqlite3

    path = tmp_path / "p9_leftover_merge.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c"), (99, "ghost")],
        )
        conn.commit()
    finally:
        conn.close()
    cfg = {"database": str(path)}
    listed = destination_key_list(
        "sqlite", cfg, schema="", table_name="items", key_columns=["id"]
    )
    assert listed is not None
    assert sorted(listed) == [(1,), (2,), (3,), (99,)]
    before = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert before is not None
    assert before["dest_count"] == 4
    assert before[EXTRA_KEYS_KEY] == 1
    assert before[MISSING_KEYS_KEY] == 0

    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg=cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    leftover = destination_key_list(
        "sqlite", cfg, schema="", table_name="gone", key_columns=["id"]
    )
    assert leftover == []

    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "destination_summary": {"leftover_deleted": 1},
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "after-merge",
                MISSING_KEYS_KEY: 0,
                EXTRA_KEYS_KEY: 0,
                "leftover_deleted": 1,
            },
        }
    )
    assert ledger.balanced is True
    assert ledger.dest_count == 3
    assert ledger.extra_keys == 0
    assert ledger.missing_keys == 0
    assert ledger.leftover_deleted == 1
    assert ledger.writer_ack == 10_000
    assert "merge" in ledger.note.lower() or "leftover" in ledger.note.lower()


def test_overwrite_merge_does_not_invent_missing_source_keys(tmp_path: Path):
    """Dest {2,3,99} vs S {1,2,3}: delete 99, dest=2, missing=1 still unclosed."""
    import sqlite3

    path = tmp_path / "p9_leftover_missing.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(2, "b"), (3, "c"), (99, "ghost")],
        )
        conn.commit()
    finally:
        conn.close()
    cfg = {"database": str(path)}
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg=cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    census = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert census is not None
    assert census["dest_count"] == 2
    assert census[EXTRA_KEYS_KEY] == 0
    assert census[MISSING_KEYS_KEY] == 1
    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 2,
                "target_checksum": "after-merge",
                MISSING_KEYS_KEY: 1,
                EXTRA_KEYS_KEY: 0,
                "leftover_deleted": 1,
            },
        }
    )
    assert ledger.balanced is False
    assert ledger.dest_count == 2
    assert ledger.missing_keys == 1
    assert ledger.extra_keys == 0
    assert ledger.leftover_deleted == 1
    assert ledger.unaccounted == 1


def test_iceberg_destination_key_list_missing_table_is_empty(tmp_path: Path):
    listed = destination_key_list(
        "iceberg",
        _iceberg_cfg(tmp_path / "wh"),
        schema="",
        table_name="orders",
        key_columns=["id"],
    )
    assert listed == []


def test_iceberg_overwrite_merge_deletes_leftover_snapshot_keys(tmp_path: Path):
    """Lakehouse leftover MERGE: dest {1,2,3,99} vs S {1,2,3} → CoW-delete 99.

    Same identity as SQL leftover MERGE. Metadata record-count and writer
    upsert ack never close. Incremental remains a hard no-op.
    """
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import destination_keyset_census, destination_row_count

    warehouse = tmp_path / "wh"
    cfg = _iceberg_cfg(warehouse)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    written = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v"],
        data_rows=[["1", "a"], ["2", "b"], ["3", "c"], ["99", "ghost"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert written.ok, written.error
    listed = destination_key_list(
        "iceberg", cfg, schema="", table_name="orders", key_columns=["id"]
    )
    assert listed is not None
    assert len(listed) == 4
    before = destination_keyset_census(
        "iceberg",
        cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert before is not None
    assert before["dest_count"] == 4
    assert before[EXTRA_KEYS_KEY] == 1
    assert before[MISSING_KEYS_KEY] == 0

    refused = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count("iceberg", cfg, schema="", table_name="orders") == 4

    deleted = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "iceberg",
        cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    assert destination_row_count("iceberg", cfg, schema="", table_name="orders") == 3


def test_iceberg_overwrite_merge_does_not_invent_missing_source_keys(tmp_path: Path):
    """Dest {2,3,99} vs S {1,2,3}: delete 99, dest=2, missing=1 still unclosed."""
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import destination_keyset_census

    warehouse = tmp_path / "wh"
    cfg = _iceberg_cfg(warehouse)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    written = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v"],
        data_rows=[["2", "b"], ["3", "c"], ["99", "ghost"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert written.ok, written.error
    deleted = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=True,
    )
    assert deleted == 1
    census = destination_keyset_census(
        "iceberg",
        cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert census is not None
    assert census["dest_count"] == 2
    assert census[EXTRA_KEYS_KEY] == 0
    assert census[MISSING_KEYS_KEY] == 1


class _ScriptedWarehouseEngine:
    """In-process dest engine: COUNT(*) / SELECT pk / named-bind hits. No stats views."""

    def __init__(self, *, count: int = 0, rows: list[tuple] | None = None, error: BaseException | None = None):
        self.count = count
        self.rows = list(rows or [])
        self.error = error
        self.sql: list[str] = []
        self.params: list[object] = []

    def connect(self):
        return self

    def dispose(self) -> None:
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, stmt: object, params: object = None):
        from types import SimpleNamespace

        sql = str(stmt)
        self.sql.append(sql)
        self.params.append(params)
        if self.error is not None:
            raise self.error
        upper = sql.upper()
        if "SYS.PARTITIONS" in upper or "DM_DB_PARTITION_STATS" in upper or "NUM_ROWS" in upper:
            raise AssertionError(f"warehouse COUNT must not use stats views: {sql}")
        if "COUNT(DISTINCT" in upper or "_DF_KEY_HITS" in upper:
            dest = {row[0] for row in self.rows}
            values = []
            if isinstance(params, dict):
                values = [v for k, v in params.items() if str(k).startswith("k")]
            hits = len(dest.intersection(values))
            return SimpleNamespace(scalar=lambda: hits, fetchall=lambda: [])
        if "COUNT(*)" in upper:
            return SimpleNamespace(scalar=lambda: self.count, fetchall=lambda: [])
        return SimpleNamespace(scalar=lambda: None, fetchall=lambda: list(self.rows))


def _patch_warehouse(monkeypatch: pytest.MonkeyPatch, engine: _ScriptedWarehouseEngine) -> _ScriptedWarehouseEngine:
    monkeypatch.setattr(
        "connectors.generic_sql.get_sqlalchemy_engine",
        lambda cfg: engine,
    )
    monkeypatch.setattr("services.engine_pool.release_engine", lambda eng: None)
    return engine


def test_sqlserver_dest_count_quotes_dbo_and_never_uses_partition_stats(monkeypatch: pytest.MonkeyPatch):
    """Azure SQL / SQL Server leftover MERGE listing needs [dbo].[table] COUNT(*)."""
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=4, rows=[(1,), (2,), (3,), (99,)]),
    )
    cfg = {"host": "db.example", "username": "sa", "database": "app"}
    assert destination_row_count("azure_sql_database", cfg, schema="", table_name="items") == 4
    assert any("COUNT(*)" in sql.upper() for sql in engine.sql)
    assert any("[dbo].[items]" in sql for sql in engine.sql)
    assert all("sys.partitions" not in sql.lower() for sql in engine.sql)
    listed = destination_key_list(
        "sqlserver", cfg, schema="", table_name="items", key_columns=["id"]
    )
    assert listed is not None
    assert sorted(listed) == [(1,), (2,), (3,), (99,)]
    hits = destination_keyset_census(
        "amazon_rds_sql_server",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert hits is not None
    assert hits["dest_count"] == 4
    assert hits[EXTRA_KEYS_KEY] == 1
    assert hits[MISSING_KEYS_KEY] == 0


def test_oracle_dest_count_folds_schema_and_missing_table_is_zero(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    cfg = {"host": "db.example", "username": "app", "database": "ORCL"}
    missing = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError("SELECT", {}, Exception("ORA-00942: table or view does not exist")),
        ),
    )
    assert destination_row_count("amazon_rds_oracle", cfg, schema="", table_name="orders") == 0
    assert any('"APP"."ORDERS"' in sql for sql in missing.sql)

    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=2, rows=[(10,), (20,)]),
    )
    assert destination_row_count("oracle_autonomous_warehouse", cfg, schema="hr", table_name="orders") == 2
    assert any('"HR"."ORDERS"' in sql for sql in engine.sql)
    listed = destination_key_list(
        "oracle", cfg, schema="hr", table_name="orders", key_columns=["id"]
    )
    assert listed == [(10,), (20,)]


def test_sqlserver_missing_table_is_zero_not_unmeasured(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError("SELECT", {}, Exception("Invalid object name 'dbo.fresh'")),
        ),
    )
    n = destination_row_count("sqlserver", {"host": "h"}, schema="dbo", table_name="fresh")
    assert n == 0
    assert engine.sql  # COUNT(*) was attempted, not skipped as unsupported


def test_sqlserver_login_failure_is_unmeasured_not_empty(monkeypatch: pytest.MonkeyPatch):
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(error=RuntimeError("Login failed for user 'sa'")),
    )
    assert destination_row_count("sqlserver", {"host": "h"}, schema="dbo", table_name="items") is None
    assert engine.sql


def test_snowflake_and_bigquery_dest_count_stay_unmeasured():
    """Clustering / INFORMATION_SCHEMA approximations are not COUNT(*). Leave unproven."""
    assert destination_row_count("snowflake", {"host": "h"}, schema="PUBLIC", table_name="T") is None
    assert destination_row_count("bigquery", {"project": "p"}, schema="ds", table_name="T") is None
    assert destination_key_list(
        "snowflake", {"host": "h"}, schema="PUBLIC", table_name="T", key_columns=["id"]
    ) is None


def test_oracle_composite_key_hits_use_and_or_not_tuple_in(monkeypatch: pytest.MonkeyPatch):
    """Oracle 19c has no row-value IN; leftover MERGE composite hits must be AND/OR."""
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=2, rows=[(1, "a"), (2, "b")]),
    )
    cfg = {"username": "app"}
    census = destination_keyset_census(
        "oracle",
        cfg,
        schema="app",
        table_name="pair",
        key_columns=["id", "kind"],
        keys=[(1, "a"), (2, "b")],
    )
    assert census is not None
    assert census["dest_count"] == 2
    hit_sql = [sql for sql in engine.sql if "_df_key_hits" in sql or "_DF_KEY_HITS" in sql.upper()]
    assert hit_sql
    compact = hit_sql[0].replace(" ", "")
    assert "IN((" not in compact
    assert " = :k0_0" in hit_sql[0]
    assert " AND " in hit_sql[0]


def test_azure_sql_leftover_merge_deletes_keys_not_in_complete_s(monkeypatch: pytest.MonkeyPatch):
    """Catalog SKU azure_sql_database must apply leftover = D \\ S, not return unapplied."""
    monkeypatch.setattr(
        "services.dest_precount.destination_key_list",
        lambda *a, **k: [(1,), (2,), (3,), (99,)],
    )
    deleted: list[str] = []

    def _delete(**kwargs: object) -> int:
        deleted.extend(list(kwargs["keys"]))  # type: ignore[arg-type]
        return len(kwargs["keys"])  # type: ignore[arg-type]

    monkeypatch.setattr("connectors.table_manager.delete_by_primary_keys", _delete)
    n = apply_inferred_leftover_deletes(
        db_type="azure_sql_database",
        cfg={"host": "db.example"},
        schema="dbo",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert n == 1
    assert deleted == ["99"]
    refused = apply_inferred_leftover_deletes(
        db_type="azure_sql_database",
        cfg={"host": "db.example"},
        schema="dbo",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=False,
    )
    assert refused is None


def test_amazon_rds_oracle_leftover_merge_routes_delete(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "services.dest_precount.destination_key_list",
        lambda *a, **k: [(1,), (99,)],
    )
    seen: dict[str, object] = {}

    def _delete(**kwargs: object) -> int:
        seen.update(kwargs)
        return 1

    monkeypatch.setattr("connectors.table_manager.delete_by_primary_keys", _delete)
    n = apply_inferred_leftover_deletes(
        db_type="amazon_rds_oracle",
        cfg={"username": "app"},
        schema="APP",
        table_name="ORDERS",
        key_columns=["id"],
        keys=[(1,)],
        complete_snapshot=True,
    )
    assert n == 1
    assert seen["db_type"] == "amazon_rds_oracle"
    assert seen["keys"] == ["99"]


def test_sqlserver_live_leftover_merge_when_reachable():
    """Live SQL Server: dest {1,2,3,99} vs S {1,2,3} → DELETE 99, extra=0.

    Skip when :1433 does not answer or the driver cannot authenticate. Never
    invent green. COUNT(*) from dest-engine, never sys.partitions.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError:
        pytest.skip("SQL Server not listening on 1433")

    cfg = {
        "type": "sqlserver",
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": "Datawrap_CDC_2022!",
        "schema": "dbo",
    }
    table = "df_p9_leftover_merge"
    try:
        from connectors.generic_sql import get_sqlalchemy_engine
        import sqlalchemy as sa

        engine = get_sqlalchemy_engine(cfg)
    except Exception as exc:
        pytest.skip(f"SQL Server engine unavailable: {exc}")
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}"))
            conn.execute(
                sa.text(f"CREATE TABLE dbo.{table} (id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)")
            )
            conn.execute(
                sa.text(f"INSERT INTO dbo.{table} (id, label) VALUES (1, N'a'), (2, N'b'), (3, N'c'), (99, N'ghost')")
            )
            conn.commit()
    except Exception as exc:
        pytest.skip(f"SQL Server setup failed: {exc}")

    assert destination_row_count("sqlserver", cfg, schema="dbo", table_name=table) == 4
    listed = destination_key_list(
        "sqlserver", cfg, schema="dbo", table_name=table, key_columns=["id"]
    )
    assert listed is not None
    assert sorted(listed) == [(1,), (2,), (3,), (99,)]
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlserver",
        cfg=cfg,
        schema="dbo",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "sqlserver",
        cfg,
        schema="dbo",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}"))
            conn.commit()
    except Exception:
        pass


def test_oracle_live_leftover_merge_when_reachable():
    """Live Oracle: same leftover identity. Skip when :1521 does not answer."""
    import os
    import socket

    host = os.environ.get("DATAFLOW_ORACLE_HOST", "127.0.0.1")
    port = int(os.environ.get("DATAFLOW_ORACLE_PORT", "1521"))
    try:
        socket.create_connection((host, port), timeout=1).close()
    except OSError:
        pytest.skip(f"Oracle not listening on {host}:{port}")

    cfg = {
        "type": "oracle",
        "host": host,
        "port": port,
        "database": os.environ.get("DATAFLOW_ORACLE_SERVICE", "ORCL"),
        "username": os.environ.get("DATAFLOW_ORACLE_USER", "system"),
        "password": os.environ.get("DATAFLOW_ORACLE_PASSWORD", ""),
        "schema": "",
    }
    if not cfg["password"]:
        pytest.skip("Oracle password not configured")
    table = "DF_P9_LEFTOVER"
    try:
        from connectors.generic_sql import get_sqlalchemy_engine
        import sqlalchemy as sa

        engine = get_sqlalchemy_engine(cfg)
        with engine.connect() as conn:
            conn.execute(sa.text(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"))
            conn.execute(sa.text(f'CREATE TABLE "{table}" (id NUMBER PRIMARY KEY, label VARCHAR2(32))'))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (1, 'a')"))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (2, 'b')"))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (3, 'c')"))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (99, 'ghost')"))
            conn.commit()
    except Exception as exc:
        pytest.skip(f"Oracle setup failed: {exc}")

    assert destination_row_count("oracle", cfg, schema="", table_name=table) == 4
    deleted = apply_inferred_leftover_deletes(
        db_type="oracle",
        cfg=cfg,
        schema="",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "oracle",
        cfg,
        schema="",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; EXCEPTION WHEN OTHERS THEN NULL; END;"))
            conn.commit()
    except Exception:
        pass
