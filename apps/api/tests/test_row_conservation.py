"""Independent dest COUNT(*) closes conservation — writer ack never does.

AWS DMS Full Load can succeed while validation later reports MISSING_TARGET:
the writer counted rows the dest engine does not hold. This module is the
named identity so the certificate cannot circularly balance a short write
against itself.
"""

from __future__ import annotations

from pathlib import Path

from services.dest_precount import (
    ARTIFACT_COUNT_KEY,
    DEST_COUNT_ARTIFACT,
    count_artifact_rows,
    stamp_artifact_census,
)
from services.row_conservation import (
    DEST_ACTIVE_READBACK,
    DEST_ARTIFACT_READBACK,
    DEST_PER_STREAM,
    DEST_READBACK,
    DEST_UNMEASURED,
    KIND_APPEND_DELTA,
    KIND_EMPTY_PASS,
    KIND_JOB,
    KIND_KEYED,
    KIND_MIRROR,
    KIND_OVERWRITE,
    account_job,
    account_job_streams,
    account_population,
    conservation_kind,
    dest_count_from_recon,
    hold_outs,
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
    census.stamp(summary, "items")
    assert summary["target_rows_before"] == 3


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
