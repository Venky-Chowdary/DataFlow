"""Wave 98 — silent-data-loss and production-resilience regressions.

Each test names the concrete defect it closes. These are not "the code path
exists" smoke tests: they assert the previously-broken outcome is now
unreachable. A future regression that re-introduces any of these defects must
fail loudly here.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# F1 — inverted tombstone: is_active=true must NEVER delete live rows
# ---------------------------------------------------------------------------


class TestTombstoneDetectionIsConservative:
    def test_is_active_is_not_a_tombstone(self) -> None:
        from src.transfer.cdc_transfer import _detect_tombstone_column

        # The bug: `is_active` was in the exact-match set with no polarity
        # handling, so every active row was converted into a destination DELETE.
        assert (
            _detect_tombstone_column(
                {}, ["id", "name", "is_active", "updated_at"]
            )
            is None
        )

    def test_deleted_by_is_not_a_tombstone(self) -> None:
        from src.transfer.cdc_transfer import _detect_tombstone_column

        # The bug: `"delete" in name` captured audit columns as tombstones.
        assert (
            _detect_tombstone_column(
                {}, ["id", "deleted_by", "deleted_reason", "delete_count"]
            )
            is None
        )

    def test_deleted_at_is_a_tombstone(self) -> None:
        from src.transfer.cdc_transfer import _detect_tombstone_column

        assert (
            _detect_tombstone_column({}, ["id", "deleted_at", "name"]) == "deleted_at"
        )

    def test_is_deleted_true_means_deleted(self) -> None:
        from src.transfer.cdc_transfer import _is_tombstone_set

        assert _is_tombstone_set({"is_deleted": True}, "is_deleted") is True
        assert _is_tombstone_set({"is_deleted": False}, "is_deleted") is False
        assert _is_tombstone_set({"is_deleted": "yes"}, "is_deleted") is True
        assert _is_tombstone_set({"is_deleted": "0"}, "is_deleted") is False

    def test_deleted_at_null_means_alive(self) -> None:
        from src.transfer.cdc_transfer import _is_tombstone_set

        assert _is_tombstone_set({"deleted_at": None}, "deleted_at") is False
        assert _is_tombstone_set({"deleted_at": ""}, "deleted_at") is False
        assert _is_tombstone_set({"deleted_at": "0000-00-00"}, "deleted_at") is False
        assert (
            _is_tombstone_set({"deleted_at": "2024-01-15T12:00:00Z"}, "deleted_at")
            is True
        )

    def test_unrecognised_boolean_value_fails_safe_to_present(self, caplog) -> None:
        from src.transfer.cdc_transfer import _is_tombstone_set

        with caplog.at_level(logging.WARNING):
            # An ambiguous value must refuse to delete — recoverable wrongness
            # beats irreversible wrongness.
            assert (
                _is_tombstone_set({"is_deleted": "maybe"}, "is_deleted") is False
            )
        assert any("unrecognised" in r.message.lower() for r in caplog.records)

    def test_split_batch_with_is_active_keeps_every_row(self) -> None:
        from src.transfer.cdc_transfer import CdcEngine

        # Even if an operator somehow forced `tombstone_column="is_active"`,
        # auto-detection never selects it — and without an explicit override
        # every active row must land as an insert.
        engine = CdcEngine(
            src_cfg={},
            src_type="postgresql",
            table_name="accounts",
            cursor_field="updated_at",
            primary_key="id",
            watermark=None,
            columns=["id", "is_active", "name"],
            schema={"id": "INTEGER", "is_active": "BOOLEAN", "name": "TEXT"},
        )
        assert engine.tombstone_column is None
        batch = engine._split_batch(
            [
                {"id": "1", "is_active": True, "name": "alive"},
                {"id": "2", "is_active": False, "name": "dormant"},
            ]
        )
        assert len(batch.inserts) == 2
        assert batch.deletes == []


# ---------------------------------------------------------------------------
# D1 — full_refresh DROP failure must fail the job, never become append
# ---------------------------------------------------------------------------


class TestFullRefreshDropIsFailClosed:
    def test_drop_helper_raises_typed_error_on_driver_failure(self) -> None:
        from connectors.table_manager import TableDropError, _drop_sqlite

        with pytest.raises(TableDropError) as ei:
            # A path that cannot exist forces the driver to raise.
            _drop_sqlite(
                {"database": "/no/such/path/for/wave98/drop.db"},
                "orders",
                None,
            )
        assert ei.value.table_name == "orders"
        assert ei.value.cause is not None

    def test_engine_drop_surfaces_as_full_refresh_drop_failed(self) -> None:
        from services.error_handling import FullRefreshDropFailed, classify_error
        from src.transfer.engine import _drop_destination_table
        from src.transfer.models import EndpointConfig

        destination = EndpointConfig(
            kind="database",
            format="sqlite",
            database="/no/such/path/for/wave98/engine_drop.db",
            table="orders",
        )
        with pytest.raises(FullRefreshDropFailed) as ei:
            _drop_destination_table(destination)
        assert ei.value.table_name == "orders"
        # Must never be retried by with_retry — that would append onto the
        # uncleared table.
        classification = classify_error(ei.value)
        assert classification["retriable"] is False
        assert "full_refresh_drop_failed" in classification["evidence"]

    def test_stream_drop_also_raises(self) -> None:
        from services.error_handling import FullRefreshDropFailed
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import _drop_destination_endpoint

        destination = EndpointConfig(
            kind="database",
            format="sqlite",
            database="/no/such/path/for/wave98/stream_drop.db",
            table="orders",
        )
        with pytest.raises(FullRefreshDropFailed):
            _drop_destination_endpoint(destination)


# ---------------------------------------------------------------------------
# D2 — CDC delete failure must raise, never look like "keys already absent"
# ---------------------------------------------------------------------------


class TestCdcDeleteFailureIsFailClosed:
    def test_delete_helper_raises_typed_error(self) -> None:
        from connectors.table_manager import DestinationDeleteError, _delete_sqlite

        with pytest.raises(DestinationDeleteError) as ei:
            _delete_sqlite(
                {"database": "/no/such/path/for/wave98/delete.db"},
                "orders",
                "id",
                ["1", "2"],
                None,
            )
        assert ei.value.table_name == "orders"

    def test_zero_deletes_on_supported_driver_still_means_already_absent(self) -> None:
        """Sanity: a genuine `0` (keys gone) remains a legitimate success."""
        import sqlite3
        import tempfile
        from pathlib import Path

        from connectors.table_manager import delete_by_primary_keys

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ok.db"
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, amount INT)")
            conn.commit()
            conn.close()
            # Keys never existed — returning 0 is correct and must not raise.
            deleted = delete_by_primary_keys(
                "sqlite",
                {"database": str(db)},
                "orders",
                "id",
                ["missing-1", "missing-2"],
            )
            assert deleted == 0


# ---------------------------------------------------------------------------
# F11 — NULL must survive an incremental snapshot chunk
# ---------------------------------------------------------------------------


class TestSnapshotPreservesNull:
    def test_snapshot_records_keep_none(self) -> None:
        from services.cdc_incremental_snapshot import snapshot_records_from_rows

        records = snapshot_records_from_rows(
            ["id", "note", "amount"],
            [("1", None, 10), ("2", "", 20)],
        )
        assert records[0]["note"] is None
        assert records[1]["note"] == ""
        # Typed values stay typed — no str() coercion.
        assert records[0]["amount"] == 10


# ---------------------------------------------------------------------------
# F12 — composite PK is a first-class keyset, not an identifier crash
# ---------------------------------------------------------------------------


class TestCompositePkKeyset:
    def test_keyset_successor_is_lexicographic(self) -> None:
        from services.cdc_snapshot_window import _PK_SEP, keyset_successor_predicate

        where, params = keyset_successor_predicate(
            ['"a"', '"b"', '"c"'], f"1{_PK_SEP}2{_PK_SEP}3"
        )
        # (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
        assert where.count(">") == 3
        assert where.count("=") == 3  # two eq from middle + one from last? wait
        # Actually: i=0: 0 eqs; i=1: 1 eq; i=2: 2 eqs → 3 total.
        assert where.count("=") == 3
        assert params == ["1", "1", "2", "1", "2", "3"]

    def test_keyset_arity_mismatch_raises(self) -> None:
        from services.cdc_snapshot_window import keyset_successor_predicate

        with pytest.raises(ValueError, match="arity mismatch"):
            keyset_successor_predicate(['"a"', '"b"'], "only-one-part")

    def test_signal_pk_columns_normalises_list(self) -> None:
        from services.cdc_incremental_snapshot import signal_pk_columns

        assert signal_pk_columns(["tenant_id", "id"]) == ["tenant_id", "id"]
        assert signal_pk_columns("id") == ["id"]
        assert signal_pk_columns("a,b") == ["a", "b"]


# ---------------------------------------------------------------------------
# F3 — untagged data batch must not silently land in tables[0]
# ---------------------------------------------------------------------------


class TestSharedReaderRefusesMisattribution:
    def test_untagged_data_batch_raises(self) -> None:
        from services.cdc_engine import ChangeBatch
        from services.sync_cursor import SyncContract
        from src.transfer.cdc_transfer import _run_cdc_shared_multi_table
        from src.transfer.models import EndpointConfig

        class FakeCdc:
            def __init__(self, *a, **k):
                pass

            def is_available(self):
                return True

            def snapshot(self):
                yield ChangeBatch(
                    inserts=[{"id": "1"}],
                    resume_token="slot=s|lsn=0/1",
                    # Deliberately untagged — this used to silently land in
                    # tables[0] ("orders"), writing users' rows into orders.
                )
                return
                yield  # pragma: no cover

            def poll(self):
                return
                yield  # pragma: no cover

            def ack(self, token=None):
                return None

            def close(self):
                return None

        source = EndpointConfig(
            kind="database",
            format="postgresql",
            database="app",
            table="orders",
            schema="public",
        )
        destination = EndpointConfig(
            kind="database",
            format="sqlite",
            database=":memory:",
            table="orders",
        )
        selected = [
            SyncContract(name="orders", primary_key="id", sync_mode="cdc"),
            SyncContract(name="users", primary_key="id", sync_mode="cdc"),
        ]

        with patch(
            "src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakeCdc
        ), patch(
            "src.transfer.cdc_transfer._apply_change_batch",
            side_effect=AssertionError("must not write a misattributed batch"),
        ), patch(
            "src.transfer.cdc_transfer.resolve_dest_table",
            side_effect=lambda *_a, **_k: "t",
        ), patch.dict(
            "os.environ",
            {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"},
        ):
            with pytest.raises(ValueError, match="Refusing to write"):
                _run_cdc_shared_multi_table(
                    source,
                    destination,
                    [{"source": "id", "target": "id"}],
                    {"id": "string"},
                    None,
                    sync_mode="cdc",
                    stream_contracts=[
                        {"name": "orders", "selected": True, "primary_key": "id"},
                        {"name": "users", "selected": True, "primary_key": "id"},
                    ],
                    selected=selected,
                    job_id="wave98-misattr",
                    checkpoint=None,
                    checkpoint_service=None,
                    backfill_new_fields=False,
                    validation_mode="strict",
                    limit=0,
                )


# ---------------------------------------------------------------------------
# F6 — snapshot chunks must carry a stampable LSN when a watermark exists
# ---------------------------------------------------------------------------


class TestLsnLowWatermarkIsInclusive:
    def test_event_at_watermark_is_stale(self) -> None:
        from connectors.postgresql_change_stream import _lsn_at_or_before

        # Inclusive: equal to the low watermark must also be discarded.
        assert _lsn_at_or_before("0/16B3A40", "0/16B3A40") is True
        assert _lsn_at_or_before("0/16B3A3F", "0/16B3A40") is True
        assert _lsn_at_or_before("0/16B3A41", "0/16B3A40") is False
        assert _lsn_at_or_before("", "0/16B3A40") is False


class TestSnapshotLsnStamp:
    def test_low_watermark_is_folded_into_resume_token(self) -> None:
        from services.cdc_incremental_runner import _snapshot_low_watermark
        from services.cdc_incremental_snapshot import SnapshotSignal

        sig = SnapshotSignal(
            id="s1",
            source_key="pg",
            table="orders",
            gtid_low="uuid:1-10",
        )
        assert _snapshot_low_watermark(sig) == {"gtid": "uuid:1-10"}

        sig2 = SnapshotSignal(
            id="s2", source_key="pg", table="orders", lsn_low="0/16B3A40"
        )
        assert _snapshot_low_watermark(sig2) == {"lsn": "0/16B3A40"}

        # No watermark → empty dict, so the runner leaves the chunk unstamped
        # rather than inventing a position.
        sig3 = SnapshotSignal(id="s3", source_key="pg", table="orders")
        assert _snapshot_low_watermark(sig3) == {}

    def test_extract_cdc_lsn_reads_snapshot_token(self) -> None:
        from connectors.writer_common import extract_cdc_lsn

        # The exact shape the runner now emits.
        token = {
            "incremental_snapshot": True,
            "signal_id": "s1",
            "table": "orders",
            "lsn": "0/16B3A40",
        }
        assert extract_cdc_lsn(token) == "0/16B3A40"


# ---------------------------------------------------------------------------
# D8 — checkpoint rejection details are bounded
# ---------------------------------------------------------------------------


class TestCheckpointRejectionIsBounded:
    def test_rejected_details_cap_and_counter(self) -> None:
        from services.checkpoint_service import MAX_REJECTED_DETAILS, Checkpoint

        cp = Checkpoint(job_id="wave98-cap")
        # Flood past the cap.
        flood = [{"row": i, "reason": "bad"} for i in range(MAX_REJECTED_DETAILS + 250)]
        cp.add_rejected_details(flood)
        assert len(cp.rejected_details) == MAX_REJECTED_DETAILS
        assert cp.rejected_details_truncated == 250
        # A subsequent append past the cap keeps counting.
        cp.add_rejected_details([{"row": "extra"}])
        assert len(cp.rejected_details) == MAX_REJECTED_DETAILS
        assert cp.rejected_details_truncated == 251

    def test_to_dict_carries_truncated_count(self) -> None:
        from services.checkpoint_service import Checkpoint

        cp = Checkpoint(job_id="wave98-cap", rejected_details_truncated=7)
        assert cp.to_dict()["rejected_details_truncated"] == 7


# ---------------------------------------------------------------------------
# D9 / D10 — bounded accumulators
# ---------------------------------------------------------------------------


class TestBoundedCollections:
    def test_bounded_strings_dedupe_and_cap(self) -> None:
        from services.bounded_collections import BoundedStrings

        b = BoundedStrings(cap=3)
        b.extend(["a", "a", "b", "c", "d", "b"])
        assert list(b) == ["a", "b", "c"]
        assert b.dropped == 1
        assert b.truncated is True

    def test_lineage_ring_buffer_evicts_oldest(self) -> None:
        from services import lineage_telemetry as lt

        lt.clear_events()
        # Temporarily shrink the ring so the test stays fast.
        original = lt.LINEAGE_EVENTS
        from collections import deque

        lt.LINEAGE_EVENTS = deque(maxlen=3)
        try:
            for i in range(5):
                lt._emit("test.event", {"i": i})
            events = lt.get_events()
            assert len(events) == 3
            assert [e["payload"]["i"] for e in events] == [2, 3, 4]
        finally:
            lt.LINEAGE_EVENTS = original
            lt.clear_events()


# ---------------------------------------------------------------------------
# D6 — cancel race: terminal status cannot be overwritten by progress
# ---------------------------------------------------------------------------


class TestCancelRaceIsClosed:
    def test_terminal_status_blocks_running_update(self) -> None:
        """A cancelled job must refuse a subsequent 'running' write."""
        from services.mongodb_service import MongoDBService, TERMINAL_JOB_STATUSES

        assert "cancelled" in TERMINAL_JOB_STATUSES

        # Drive update_job_status against a fake collection that already holds a
        # cancelled document. The filter must short-circuit before any write.
        svc = MongoDBService.__new__(MongoDBService)
        fake_coll = MagicMock()
        fake_coll.find_one.return_value = {"status": "cancelled", "event_log": []}
        fake_db = MagicMock()
        fake_db.__getitem__.return_value = fake_coll
        with patch.object(svc, "get_database", return_value=fake_db), patch(
            "services.mongodb_service._as_object_id", return_value="oid"
        ):
            ok = svc.update_job_status("job-1", "running", message="Writing…")
        assert ok is False
        fake_coll.update_one.assert_not_called()

    def test_resume_is_allowed_to_leave_terminal(self) -> None:
        from services.mongodb_service import MongoDBService

        svc = MongoDBService.__new__(MongoDBService)
        fake_coll = MagicMock()
        fake_coll.find_one.return_value = {"status": "cancelled", "event_log": []}
        fake_coll.update_one.return_value = MagicMock(modified_count=1, matched_count=1)
        fake_db = MagicMock()
        fake_db.__getitem__.return_value = fake_coll
        with patch.object(svc, "get_database", return_value=fake_db), patch(
            "services.mongodb_service._as_object_id", return_value="oid"
        ):
            ok = svc.update_job_status(
                "job-1", "pending", message="Resume", allow_terminal_exit=True
            )
        assert ok is True
        fake_coll.update_one.assert_called_once()


# ---------------------------------------------------------------------------
# D7 — aborted dispatcher drops queued chunks instead of writing them
# ---------------------------------------------------------------------------


class TestChunkAbortDropsQueuedWrites:
    def test_abort_prevents_queued_chunks_from_writing(self) -> None:
        from services.parallel_chunks import ChunkAborted, ChunkDispatcher

        written: list[int] = []
        started = threading.Event()
        release = threading.Event()

        def process(idx: int, item: int) -> int:
            # First chunk blocks so later ones sit in the queue.
            if idx == 0:
                started.set()
                release.wait(timeout=2)
            written.append(idx)
            return idx

        with ChunkDispatcher(max_workers=1, max_inflight=4) as dispatcher:
            dispatcher.submit(0, 0, process)
            assert started.wait(timeout=2)
            # Queue more work while the first chunk is mid-write.
            for i in range(1, 4):
                dispatcher.submit(i, i, process)
            # Abort: already-started chunk 0 finishes; queued ones must not write.
            dispatcher.abort()
            release.set()
            with pytest.raises(ChunkAborted):
                # results() surfaces the ChunkAborted from a queued future.
                list(dispatcher.results())

        assert written == [0], f"queued chunks wrote after abort: {written}"


# ---------------------------------------------------------------------------
# D28 / D29 — structured logging carries correlation + job identity
# ---------------------------------------------------------------------------


class TestStructuredLogging:
    def test_context_filter_stamps_job_and_correlation(self) -> None:
        from services.logging_config import (
            ContextFilter,
            job_log_context,
            reset_for_tests,
        )
        from services.tracing import set_correlation_id

        reset_for_tests()
        set_correlation_id("corr-wave98")
        filt = ContextFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        with job_log_context("job-wave98"):
            assert filt.filter(record) is True
        assert record.job_id == "job-wave98"
        assert record.correlation_id == "corr-wave98"

    def test_json_formatter_emits_one_object_per_line(self) -> None:
        import json as _json

        from services.logging_config import JsonFormatter

        record = logging.LogRecord(
            name="dataflow.engine",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="wrote %s rows",
            args=(42,),
            exc_info=None,
        )
        record.job_id = "job-1"
        record.correlation_id = "corr-1"
        record.trace_id = ""
        line = JsonFormatter().format(record)
        payload = _json.loads(line)
        assert payload["msg"] == "wrote 42 rows"
        assert payload["job_id"] == "job-1"
        assert payload["correlation_id"] == "corr-1"
        assert payload["level"] == "INFO"

    def test_configure_logging_is_idempotent(self) -> None:
        from services.logging_config import configure_logging, reset_for_tests

        reset_for_tests()
        configure_logging(force=True)
        handlers_before = list(logging.getLogger().handlers)
        configure_logging()  # no-op
        assert logging.getLogger().handlers == handlers_before
        reset_for_tests()
