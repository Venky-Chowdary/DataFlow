"""Independent dest COUNT(*) closes conservation — writer ack never does.

AWS DMS Full Load can succeed while validation later reports MISSING_TARGET:
the writer counted rows the dest engine does not hold. This module is the
named identity so the certificate cannot circularly balance a short write
against itself.
"""

from __future__ import annotations

from pathlib import Path

from services.row_conservation import (
    DEST_READBACK,
    DEST_UNMEASURED,
    KIND_APPEND_DELTA,
    KIND_EMPTY_PASS,
    KIND_KEYED,
    KIND_OVERWRITE,
    account_job,
    account_population,
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
