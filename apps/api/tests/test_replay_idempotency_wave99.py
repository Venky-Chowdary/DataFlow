"""Regression tests for duplicate-write and concurrency defects.

Each class pins one defect that let a transfer report success while the
destination held wrong data. The tests assert on observable outcomes — rows
actually in the table, cursors actually persisted, claims actually granted —
rather than on internal call counts, so they keep their meaning if the
implementation is refactored.

Defects covered:

* **D3** an insert-mode write replayed after a transient failure appended the
  batch a second time on every destination except Postgres and MySQL.
* **D4** two overlapping submissions of the same transfer ran two writers
  against the same destination table.
* **F4** a per-table CDC cursor was published mid-transaction, so resuming that
  table alone skipped changes a sibling table never received.
* **F9** the Postgres heartbeat emitted WAL through the slot without ever
  advancing it, growing retention on an idle slot instead of releasing it.
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import sqlalchemy as sa  # noqa: E402

import connectors.generic_sql as generic_sql  # noqa: E402
from connectors.write_resilience import (  # noqa: E402
    LEDGER_TABLE,
    ensure_sqlalchemy_write_ledger,
    mark_sqlalchemy_chunk_committed,
    sqlalchemy_chunk_rows_written,
    sqlalchemy_ledger_table,
)
from services.error_handling import (  # noqa: E402
    AmbiguousWriteOutcome,
    RetryBudget,
    classify_error,
    humanize_transfer_failure,
    with_retry,
)
from services.job_idempotency import (  # noqa: E402
    claim_key,
    normalize_client_key,
    request_fingerprint,
)
from services.mongodb_service import MemoryMongoDBService  # noqa: E402
from services.replay_safety import (  # noqa: E402
    classify_replay_safety,
    destination_has_chunk_ledger,
    error_outcome_is_ambiguous,
)
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_MAPPINGS = [
    {"source": "id", "target": "id", "target_type": "INTEGER"},
    {"source": "name", "target": "name", "target_type": "VARCHAR(50)"},
]
_COLUMN_TYPES = {"id": "integer", "name": "string"}


def _sqlite_cfg(tmpdir: str) -> tuple[dict, str]:
    db = os.path.join(tmpdir, "dest.db")
    cfg = {
        "host": "",
        "port": 0,
        "database": db,
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": f"sqlite:///{db}",
        "ssl": False,
        "type": "sqlite",
    }
    return cfg, db


def _row_count(db_path: str, table: str = "t1") -> int:
    engine = sa.create_engine(f"sqlite:///{db_path}")
    try:
        with engine.connect() as conn:
            return int(conn.execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0)  # nosec: B608
    finally:
        engine.dispose()


# --------------------------------------------------------------------------
# D3 — replayed insert must not duplicate rows
# --------------------------------------------------------------------------


class TestChunkLedgerPreventsDuplicateRows:
    def test_replaying_the_same_batch_does_not_append_it_twice(self):
        """The defect: attempt two appended a second copy of every landed row."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg, db = _sqlite_cfg(tmp)
            rows = [[str(i), f"n{i}"] for i in range(25)]
            common = dict(
                table_name="t1",
                headers=["id", "name"],
                data_rows=rows,
                mappings=_MAPPINGS,
                column_types=_COLUMN_TYPES,
                create_table=True,
                write_mode="insert",
                job_id="job-d3",
            )

            first = generic_sql.write_mapped_rows(**cfg, **common)
            assert first.ok, first.error
            assert _row_count(db) == 25

            second = generic_sql.write_mapped_rows(**cfg, **common)
            assert second.ok, second.error
            assert _row_count(db) == 25, "replay duplicated rows"
            # The replay must still report the rows as present, or reconcile
            # would flag a phantom shortfall against the source count.
            assert second.rows_written == 25

    def test_replay_is_reported_so_it_is_not_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, _ = _sqlite_cfg(tmp)
            common = dict(
                table_name="t1",
                headers=["id", "name"],
                data_rows=[["1", "a"]],
                mappings=_MAPPINGS,
                column_types=_COLUMN_TYPES,
                create_table=True,
                write_mode="insert",
                job_id="job-report",
            )
            generic_sql.write_mapped_rows(**cfg, **common)
            second = generic_sql.write_mapped_rows(**cfg, **common)
            assert any("already committed" in w for w in second.warnings), second.warnings

    def test_a_different_job_is_not_treated_as_a_replay(self):
        """Two genuinely separate runs must both land. Skipping one would lose rows."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg, db = _sqlite_cfg(tmp)
            base = dict(
                table_name="t1",
                headers=["id", "name"],
                data_rows=[["1", "a"], ["2", "b"]],
                mappings=_MAPPINGS,
                column_types=_COLUMN_TYPES,
                create_table=True,
                write_mode="insert",
            )
            generic_sql.write_mapped_rows(**cfg, **base, job_id="run-1")
            generic_sql.write_mapped_rows(**cfg, **base, job_id="run-2")
            assert _row_count(db) == 4

    def test_upsert_writes_skip_the_ledger_because_they_converge(self):
        """An upsert is already idempotent; a ledger there is pure overhead."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg, db = _sqlite_cfg(tmp)
            common = dict(
                table_name="t1",
                headers=["id", "name"],
                data_rows=[["1", "a"], ["2", "b"]],
                mappings=_MAPPINGS,
                column_types=_COLUMN_TYPES,
                create_table=True,
                write_mode="upsert",
                conflict_columns=["id"],
                job_id="job-upsert",
            )
            generic_sql.write_mapped_rows(**cfg, **common)
            generic_sql.write_mapped_rows(**cfg, **common)
            assert _row_count(db) == 2
            engine = sa.create_engine(f"sqlite:///{db}")
            try:
                with engine.connect() as conn:
                    assert not sa.inspect(conn).has_table(LEDGER_TABLE)
            finally:
                engine.dispose()

    def test_ledger_records_the_actual_row_count_not_the_batch_size(self):
        """A chunk that quarantined rows must replay its real count, not the batch length."""
        with tempfile.TemporaryDirectory() as tmp:
            _, db = _sqlite_cfg(tmp)
            engine = sa.create_engine(f"sqlite:///{db}")
            try:
                with engine.connect() as conn:
                    table = ensure_sqlalchemy_write_ledger(conn)
                    assert table is not None
                    conn.commit()
                    mark_sqlalchemy_chunk_committed(
                        conn, table, job_id="j", batch_key="t1", chunk_idx=0, rows_written=7
                    )
                    conn.commit()
                    assert (
                        sqlalchemy_chunk_rows_written(
                            conn, table, job_id="j", batch_key="t1", chunk_idx=0
                        )
                        == 7
                    )
                    assert (
                        sqlalchemy_chunk_rows_written(
                            conn, table, job_id="j", batch_key="t1", chunk_idx=1
                        )
                        is None
                    )
            finally:
                engine.dispose()

    def test_marking_the_same_chunk_twice_is_not_an_error(self):
        """Concurrent attempts converging on one chunk is the outcome we want."""
        with tempfile.TemporaryDirectory() as tmp:
            _, db = _sqlite_cfg(tmp)
            engine = sa.create_engine(f"sqlite:///{db}")
            try:
                with engine.connect() as conn:
                    table = ensure_sqlalchemy_write_ledger(conn)
                    conn.commit()
                    for _ in range(2):
                        mark_sqlalchemy_chunk_committed(
                            conn, table, job_id="j", batch_key="k", chunk_idx=0, rows_written=1
                        )
                        conn.commit()
            finally:
                engine.dispose()

    @pytest.mark.parametrize(
        "dialect", ["sqlite", "postgresql", "mysql", "mssql", "oracle"]
    )
    def test_ledger_ddl_compiles_for_every_shipped_dialect(self, dialect):
        """Unbounded VARCHAR in a primary key is rejected by Oracle and SQL Server."""
        from sqlalchemy.schema import CreateTable

        mock = sa.create_mock_engine(f"{dialect}://", lambda *a, **k: None)
        ddl = str(
            CreateTable(sqlalchemy_ledger_table(sa.MetaData())).compile(
                dialect=mock.dialect
            )
        )
        assert LEDGER_TABLE in ddl
        assert "PRIMARY KEY" in ddl.upper()


# --------------------------------------------------------------------------
# D3b — replay safety classification gates the retry loop
# --------------------------------------------------------------------------


class TestReplaySafetyVerdict:
    def test_keyed_upsert_is_replay_safe(self):
        v = classify_replay_safety(
            dest_type="kafka", write_mode="upsert", conflict_columns=["id"]
        )
        assert v.safe and v.mechanism == "idempotent_upsert"

    def test_warehouse_insert_is_safe_via_ledger(self):
        v = classify_replay_safety(
            dest_type="oracle", write_mode="insert", job_id="j1"
        )
        assert v.safe and v.mechanism == "chunk_ledger"

    def test_ledger_destination_without_job_id_is_not_safe(self):
        """No job id means an interrupted attempt cannot be distinguished from a new one."""
        v = classify_replay_safety(dest_type="oracle", write_mode="insert")
        assert not v.safe

    def test_append_only_sink_is_not_replay_safe(self):
        v = classify_replay_safety(dest_type="kafka", write_mode="insert", job_id="j")
        assert not v.safe and v.duplicate_risk
        assert "second copy" in v.reason

    def test_document_sink_with_primary_key_is_safe(self):
        v = classify_replay_safety(
            dest_type="mongodb", write_mode="insert", job_id="j", has_primary_key=True
        )
        assert v.safe and v.mechanism == "keyed_document"

    def test_document_sink_without_primary_key_is_not_safe(self):
        """A generated _id makes every replay a fresh document."""
        v = classify_replay_safety(
            dest_type="mongodb", write_mode="insert", job_id="j", has_primary_key=False
        )
        assert not v.safe

    def test_every_ledger_destination_is_recognised(self):
        """Membership follows the writer a destination dispatches to.

        Snowflake and BigQuery are deliberately absent: their writers load a
        batch as one unit outside a cross-statement transaction, so a ledger row
        could commit while its data write rolls back and the retry would skip a
        chunk that never landed. See tests/test_replay_ledger_registry.py, which
        asserts this set against the live routing table.
        """
        for dest in ("postgresql", "mysql", "oracle", "sqlserver", "duckdb", "sqlite"):
            assert destination_has_chunk_ledger(dest), dest
        for dest in ("kafka", "s3", "gcs", "mongodb", "elasticsearch", "snowflake", "bigquery"):
            assert not destination_has_chunk_ledger(dest), dest

    def test_verdict_never_leaks_credentials(self):
        v = classify_replay_safety(dest_type="snowflake", write_mode="insert", job_id="j")
        blob = str(v.to_dict())
        assert "password" not in blob.lower()


class TestAmbiguousErrorsAreNotReplayed:
    def test_timeout_is_ambiguous(self):
        assert error_outcome_is_ambiguous(TimeoutError("write timed out"))

    def test_connection_reset_is_ambiguous(self):
        assert error_outcome_is_ambiguous(ConnectionError("connection reset by peer"))

    def test_refused_connection_proves_nothing_landed(self):
        assert not error_outcome_is_ambiguous(ConnectionError("connection refused"))

    def test_auth_failure_proves_nothing_landed(self):
        assert not error_outcome_is_ambiguous(RuntimeError("authentication failed"))

    def test_append_only_sink_stops_instead_of_duplicating(self):
        """The defect: with_retry re-sent a batch that may already have landed."""
        safety = classify_replay_safety(
            dest_type="kafka", write_mode="insert", job_id="j"
        )
        attempts = []

        def flaky():
            attempts.append(1)
            raise ConnectionError("connection reset by peer")

        with pytest.raises(AmbiguousWriteOutcome):
            with_retry(
                flaky,
                budget=RetryBudget(max_attempts=5, base_delay_seconds=0.001),
                replay_safety=safety,
            )
        assert len(attempts) == 1, "batch was re-sent despite unknown outcome"

    def test_append_only_sink_still_retries_a_pre_dispatch_failure(self):
        """Availability is not sacrificed when the error proves nothing was written."""
        safety = classify_replay_safety(
            dest_type="kafka", write_mode="insert", job_id="j"
        )
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("connection refused")
            return "ok"

        assert (
            with_retry(
                flaky,
                budget=RetryBudget(max_attempts=5, base_delay_seconds=0.001),
                replay_safety=safety,
            )
            == "ok"
        )
        assert len(attempts) == 3

    def test_replay_safe_destination_keeps_its_full_retry_budget(self):
        safety = classify_replay_safety(
            dest_type="oracle", write_mode="insert", job_id="j"
        )
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("connection reset by peer")
            return "ok"

        assert (
            with_retry(
                flaky,
                budget=RetryBudget(max_attempts=5, base_delay_seconds=0.001),
                replay_safety=safety,
            )
            == "ok"
        )
        assert len(attempts) == 3

    def test_reads_without_a_verdict_retry_as_before(self):
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 2:
                raise ConnectionError("connection reset by peer")
            return "ok"

        assert with_retry(
            flaky, budget=RetryBudget(max_attempts=3, base_delay_seconds=0.001)
        ) == "ok"

    def test_ambiguous_outcome_is_not_retriable_by_an_outer_wrapper(self):
        safety = classify_replay_safety(dest_type="kafka", write_mode="insert", job_id="j")
        exc = AmbiguousWriteOutcome(ConnectionError("connection reset"), safety)
        assert classify_error(exc)["retriable"] is False

    def test_operator_is_told_to_resume_not_to_retry(self):
        safety = classify_replay_safety(dest_type="kafka", write_mode="insert", job_id="j")
        guidance = humanize_transfer_failure(
            AmbiguousWriteOutcome(TimeoutError("write timed out"), safety)
        )
        assert guidance["code"] == "ambiguous_write_outcome"
        assert "resume" in guidance["fix"].lower()
        assert guidance["retriable"] is False


# --------------------------------------------------------------------------
# D4 — concurrent submissions of the same transfer
# --------------------------------------------------------------------------


def _request(table: str = "orders", sync: str = "full_refresh_append", **kw):
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", host="h", database="d", table=table
        ),
        destination=EndpointConfig(
            kind="database", format="snowflake", database="WH", table=table
        ),
        sync_mode=sync,
        workspace_id=kw.pop("workspace_id", "ws1"),
        **kw,
    )


class TestRequestFingerprint:
    def test_same_intent_produces_the_same_fingerprint(self):
        assert request_fingerprint(_request()) == request_fingerprint(_request())

    def test_a_different_table_is_a_different_transfer(self):
        assert request_fingerprint(_request("orders")) != request_fingerprint(
            _request("items")
        )

    def test_a_different_sync_mode_is_a_different_transfer(self):
        assert request_fingerprint(
            _request(sync="full_refresh_append")
        ) != request_fingerprint(_request(sync="incremental_upsert"))

    def test_cosmetic_fields_do_not_change_the_fingerprint(self):
        """A resubmit that only carries a new actor label is still the same run."""
        assert request_fingerprint(_request(triggered_by="a@x")) == request_fingerprint(
            _request(triggered_by="b@x")
        )

    def test_reordered_mappings_are_the_same_transfer(self):
        a = _request()
        a.mappings = [{"source": "x", "target": "x"}, {"source": "y", "target": "y"}]
        b = _request()
        b.mappings = [{"source": "y", "target": "y"}, {"source": "x", "target": "x"}]
        assert request_fingerprint(a) == request_fingerprint(b)

    def test_different_file_content_is_a_different_transfer(self):
        a = _request()
        a.source = EndpointConfig(kind="file", format="csv")
        a.source_filename = "u.csv"
        a.source_content = b"id\n1\n"
        b = _request()
        b.source = EndpointConfig(kind="file", format="csv")
        b.source_filename = "u.csv"
        b.source_content = b"id\n2\n"
        assert request_fingerprint(a) != request_fingerprint(b)

    def test_credentials_never_appear_in_the_key(self):
        req = _request()
        req.destination.password = "sup3rs3cret"  # noqa: S105
        fp = request_fingerprint(req)
        assert "sup3rs3cret" not in fp

    def test_credentials_still_distinguish_two_hosts(self):
        a, b = _request(), _request()
        a.destination.password = "one"  # noqa: S105
        b.destination.password = "two"  # noqa: S105
        assert request_fingerprint(a) != request_fingerprint(b)

    def test_workspaces_are_namespaced_apart(self):
        assert claim_key(workspace_id="ws1", key="k") != claim_key(
            workspace_id="ws2", key="k"
        )

    def test_blank_client_key_is_treated_as_absent(self):
        assert normalize_client_key("   ") == ""
        assert normalize_client_key(None) == ""

    def test_client_key_is_bounded(self):
        assert len(normalize_client_key("x" * 5000)) == 200


class TestConcurrentSubmissionIsDeduplicated:
    def test_only_one_of_many_simultaneous_submits_wins(self):
        """The defect: every submit started a writer against the same table."""
        svc = MemoryMongoDBService()
        key = claim_key(workspace_id="ws1", key="fp")
        outcomes: list[tuple[str, bool, str]] = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def submit(i: int) -> None:
            job_id = svc.create_transfer_job({"name": f"j{i}"})
            barrier.wait()
            acquired, holder, _ = svc.claim_job_idempotency(key=key, job_id=job_id)
            with lock:
                outcomes.append((job_id, acquired, holder))

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [o for o in outcomes if o[1]]
        losers = [o for o in outcomes if not o[1]]
        assert len(winners) == 1, outcomes
        assert len(losers) == 7
        assert all(o[2] == winners[0][0] for o in losers), "loser was not told the winner"

    def test_a_running_job_still_blocks_a_new_submit(self):
        svc = MemoryMongoDBService()
        key = claim_key(workspace_id="ws1", key="fp")
        first = svc.create_transfer_job({"name": "first"})
        assert svc.claim_job_idempotency(key=key, job_id=first)[0]
        svc.update_job_status(first, "running")
        acquired, holder, status = svc.claim_job_idempotency(key=key, job_id="second")
        assert not acquired
        assert holder == first
        assert status == "running"

    def test_rerunning_after_completion_is_allowed(self):
        """Re-running the same transfer later is normal and must not be blocked."""
        svc = MemoryMongoDBService()
        key = claim_key(workspace_id="ws1", key="fp")
        first = svc.create_transfer_job({"name": "first"})
        svc.claim_job_idempotency(key=key, job_id=first)
        svc.update_job_status(first, "completed")
        assert svc.claim_job_idempotency(key=key, job_id="second")[0]

    def test_rerunning_after_failure_is_allowed(self):
        svc = MemoryMongoDBService()
        key = claim_key(workspace_id="ws1", key="fp")
        first = svc.create_transfer_job({"name": "first"})
        svc.claim_job_idempotency(key=key, job_id=first)
        svc.update_job_status(first, "failed")
        assert svc.claim_job_idempotency(key=key, job_id="retry")[0]

    def test_release_frees_the_slot(self):
        svc = MemoryMongoDBService()
        key = claim_key(workspace_id="ws1", key="fp")
        job = svc.create_transfer_job({"name": "j"})
        svc.claim_job_idempotency(key=key, job_id=job)
        assert svc.release_job_idempotency(key, job)
        assert svc.claim_job_idempotency(key=key, job_id="next")[0]

    def test_a_superseded_run_cannot_release_the_current_claim(self):
        """A late release from an old job must not open the door for a third writer."""
        svc = MemoryMongoDBService()
        key = claim_key(workspace_id="ws1", key="fp")
        svc.claim_job_idempotency(key=key, job_id="current")
        assert not svc.release_job_idempotency(key, "stale-old-job")
        assert not svc.claim_job_idempotency(key=key, job_id="third")[0]

    def test_an_expired_claim_is_reclaimed(self):
        """A worker that dies without releasing must not wedge the pipeline."""
        from datetime import datetime, timedelta, timezone

        svc = MemoryMongoDBService()
        key = claim_key(workspace_id="ws1", key="fp")
        dead = svc.create_transfer_job({"name": "dead"})
        svc.claim_job_idempotency(key=key, job_id=dead)
        svc.update_job_status(dead, "running")
        svc._claims[key]["expires_at"] = datetime.now(timezone.utc) - timedelta(hours=1)
        assert svc.claim_job_idempotency(key=key, job_id="fresh")[0]

    def test_different_transfers_do_not_block_each_other(self):
        svc = MemoryMongoDBService()
        a = claim_key(workspace_id="ws1", key=request_fingerprint(_request("orders")))
        b = claim_key(workspace_id="ws1", key=request_fingerprint(_request("items")))
        assert svc.claim_job_idempotency(key=a, job_id="j1")[0]
        assert svc.claim_job_idempotency(key=b, job_id="j2")[0]

    def test_same_transfer_in_two_workspaces_does_not_collide(self):
        svc = MemoryMongoDBService()
        fp = request_fingerprint(_request())
        assert svc.claim_job_idempotency(
            key=claim_key(workspace_id="ws1", key=fp), job_id="j1"
        )[0]
        assert svc.claim_job_idempotency(
            key=claim_key(workspace_id="ws2", key=fp), job_id="j2"
        )[0]


class TestEngineRaisesOnDuplicateSubmission:
    def test_second_submit_raises_and_names_the_running_job(self):
        from src.transfer.engine import (
            DuplicateTransferSubmission,
            UniversalTransferEngine,
        )

        engine = UniversalTransferEngine()
        svc = MemoryMongoDBService()
        import services.mongodb_service as mongo_mod
        import src.transfer.engine as engine_mod

        original = engine_mod.get_mongodb_service
        engine_mod.get_mongodb_service = lambda: svc
        mongo_original = mongo_mod.get_mongodb_service
        mongo_mod.get_mongodb_service = lambda: svc
        try:
            first = engine._create_pending_job(_request())
            svc.update_job_status(first, "running")
            with pytest.raises(DuplicateTransferSubmission) as caught:
                engine._create_pending_job(_request())
            assert caught.value.existing_job_id == first
            assert caught.value.existing_status == "running"
        finally:
            engine_mod.get_mongodb_service = original
            mongo_mod.get_mongodb_service = mongo_original

    def test_guard_can_be_disabled_by_configuration(self):
        from src.transfer.engine import UniversalTransferEngine

        engine = UniversalTransferEngine()
        prev = os.environ.get("DATAFLOW_JOB_IDEMPOTENCY")
        os.environ["DATAFLOW_JOB_IDEMPOTENCY"] = "0"
        try:
            assert engine._idempotency_key(_request()) == ""
        finally:
            if prev is None:
                os.environ.pop("DATAFLOW_JOB_IDEMPOTENCY", None)
            else:
                os.environ["DATAFLOW_JOB_IDEMPOTENCY"] = prev

    def test_an_explicit_client_key_overrides_the_fingerprint(self):
        from src.transfer.engine import UniversalTransferEngine

        engine = UniversalTransferEngine()
        a = _request()
        a.idempotency_key = "client-supplied"
        b = _request("a-totally-different-table")
        b.idempotency_key = "client-supplied"
        assert engine._idempotency_key(a) == engine._idempotency_key(b)


# --------------------------------------------------------------------------
# F4 — per-table CDC cursors must be transaction-atomic
# --------------------------------------------------------------------------


class TestPerTableCursorIsTransactionAtomic:
    def test_only_the_last_demuxed_batch_carries_the_barrier(self):
        from services.cdc_multi_table import (
            MultiTableTransactionBuffer,
            should_ack_shared_batch,
        )

        buf = MultiTableTransactionBuffer()
        buf.begin("tx-1")
        buf.insert("orders", {"id": 1})
        buf.insert("items", {"id": 2})
        batches = buf.commit(lsn="0/100", resume_token="tok-100", table_order=["orders", "items"])

        assert len(batches) == 2
        assert [b.table for b in batches] == ["orders", "items"]
        assert not should_ack_shared_batch(batches[0]), "mid-txn batch must not ack"
        assert should_ack_shared_batch(batches[1])
        # Every batch of one transaction shares the position, which is why
        # publishing it early looks correct while being wrong.
        assert {b.resume_token for b in batches} == {"tok-100"}

    def test_cursors_are_withheld_until_the_transaction_completes(self):
        """The defect: table A's cursor was published before table B's batch landed.

        Reproduces the staging rule directly: a run interrupted after the first
        table's batch must leave no cursor behind, so a resume replays the whole
        transaction instead of starting past changes B never received.
        """
        from services.cdc_multi_table import (
            MultiTableTransactionBuffer,
            should_ack_shared_batch,
        )

        buf = MultiTableTransactionBuffer()
        buf.begin("tx-1")
        buf.insert("orders", {"id": 1})
        buf.insert("items", {"id": 2})
        batches = buf.commit(lsn="0/100", resume_token="tok-100", table_order=["orders", "items"])

        published: dict[str, str] = {}
        staged: dict[str, str] = {}

        def apply(batch, crash_after: bool = False):
            if batch.total_changes:
                staged[batch.table] = str(batch.resume_token)
            if should_ack_shared_batch(batch):
                published.update(staged)
                staged.clear()

        apply(batches[0])
        assert published == {}, "cursor published before the transaction finished"

        apply(batches[1])
        assert published == {"orders": "tok-100", "items": "tok-100"}

    def test_a_position_only_barrier_advances_no_table_cursor(self):
        """A heartbeat releases WAL but must not claim any table made progress."""
        from services.cdc_engine import ChangeBatch
        from services.cdc_multi_table import should_ack_shared_batch

        beat = ChangeBatch(resume_token="tok-200", ack_barrier=True)
        assert should_ack_shared_batch(beat)
        assert beat.total_changes == 0

        staged: dict[str, str] = {}
        if beat.total_changes:
            staged[beat.table] = str(beat.resume_token)
        assert staged == {}

    def test_a_table_untouched_by_the_transaction_keeps_its_position(self):
        from services.cdc_multi_table import MultiTableTransactionBuffer

        buf = MultiTableTransactionBuffer()
        buf.begin("tx-2")
        buf.insert("orders", {"id": 9})
        batches = buf.commit(
            lsn="0/300", resume_token="tok-300", table_order=["orders", "items"]
        )
        assert [b.table for b in batches] == ["orders"]


# --------------------------------------------------------------------------
# F9 — the Postgres heartbeat must release WAL, not add to it
# --------------------------------------------------------------------------


class _FakeCursor:
    """Minimal cursor that records SQL and answers the heartbeat's probes."""

    def __init__(self, state: dict):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        self.state["sql"].append(text)
        self._last = (text, params)
        # Recorded on execute, not on fetch: the production code issues the
        # advance and never reads a result, so a fetch-based recorder would
        # report "not advanced" for a slot that had in fact moved.
        if "pg_replication_slot_advance" in text:
            self.state["advanced_to"] = params[1] if params else None

    def fetchone(self):
        sql = self._last[0]
        if "pg_current_wal_lsn" in sql:
            return (self.state["current_wal"],)
        if "peek" in sql:
            return (1,) if self.state["pending_changes"] else None
        if "confirmed_flush_lsn" in sql:
            return (self.state["confirmed"],)
        return None

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self, state: dict):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return _FakeCursor(self.state)

    def commit(self):
        self.state["commits"] += 1


def _reader_with(state: dict, **attrs):
    """Build a bare change-stream object wired to the fake connection."""
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    reader = object.__new__(PostgreSqlChangeStreamCdc)
    reader.slot_name = "df_slot"
    reader.publication_name = "df_pub"
    reader.output_plugin = "test_decoding"
    reader.consistent_point_lsn = state["confirmed"]
    reader.source_key = "src-key"
    reader._last_heartbeat_at = None
    reader._pending_ack_lsn = None
    reader._lease = type("L", (), {"acquired": False})()
    reader._conn = lambda: _FakeConn(state)
    for k, v in attrs.items():
        setattr(reader, k, v)
    return reader


def _state(current="0/900", confirmed="0/100", pending=False):
    return {
        "sql": [],
        "commits": 0,
        "current_wal": current,
        "confirmed": confirmed,
        "pending_changes": pending,
        "advanced_to": None,
    }


class TestIdleSlotReleasesWal:
    def test_heartbeat_no_longer_writes_wal_through_the_slot(self):
        """The defect: each heartbeat emitted a message the slot then had to retain."""
        state = _state()
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False
        reader.heartbeat()
        assert not any("pg_logical_emit_message" in s for s in state["sql"]), state["sql"]

    def test_idle_slot_is_advanced_so_wal_is_released(self):
        state = _state(current="0/900", confirmed="0/100")
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False
        reader.heartbeat()
        assert state["advanced_to"] == "0/900"
        assert reader.consistent_point_lsn == "0/900"

    def test_undecoded_changes_are_never_skipped(self):
        """Advancing past pending changes would silently lose them."""
        state = _state(pending=True)
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False
        reader.heartbeat()
        assert state["advanced_to"] is None
        assert not any("pg_replication_slot_advance" in s for s in state["sql"])

    def test_the_target_is_captured_before_the_peek(self):
        """Ordering is what makes the advance race-free against a concurrent commit."""
        state = _state()
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False
        reader.heartbeat()
        joined = state["sql"]
        wal_at = next(i for i, s in enumerate(joined) if "pg_current_wal_lsn" in s)
        peek_at = next(i for i, s in enumerate(joined) if "peek" in s)
        assert wal_at < peek_at

    def test_applied_but_unacked_work_leaves_the_slot_alone(self):
        state = _state()
        reader = _reader_with(state)
        reader._pending_ack_lsn = "0/500"
        reader._incremental_snapshot_open = lambda: False
        reader.heartbeat()
        assert state["advanced_to"] is None

    def test_an_open_incremental_snapshot_keeps_its_wal(self):
        """The snapshot window compares against events the advance would drop."""
        state = _state()
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: True
        reader.heartbeat()
        assert state["advanced_to"] is None

    def test_unknown_snapshot_state_keeps_the_wal(self):
        """Fail closed: if we cannot tell, retaining WAL is the recoverable choice."""
        import services.cdc_incremental_snapshot as snap_mod

        state = _state()
        reader = _reader_with(state)
        original = snap_mod.list_signals
        snap_mod.list_signals = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down"))
        try:
            assert reader._incremental_snapshot_open() is True
        finally:
            snap_mod.list_signals = original

    def test_an_already_current_slot_is_not_advanced_again(self):
        state = _state(current="0/100", confirmed="0/100")
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False
        reader.heartbeat()
        assert state["advanced_to"] is None

    def test_rate_limit_still_applies(self):
        from datetime import datetime, timezone

        state = _state()
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False
        reader._last_heartbeat_at = datetime.now(timezone.utc)
        reader.heartbeat()
        assert state["sql"] == []

    def test_advance_can_be_disabled_by_configuration(self):
        state = _state()
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False
        prev = os.environ.get("DATAFLOW_CDC_IDLE_SLOT_ADVANCE")
        os.environ["DATAFLOW_CDC_IDLE_SLOT_ADVANCE"] = "0"
        try:
            reader.heartbeat()
            assert state["advanced_to"] is None
        finally:
            if prev is None:
                os.environ.pop("DATAFLOW_CDC_IDLE_SLOT_ADVANCE", None)
            else:
                os.environ["DATAFLOW_CDC_IDLE_SLOT_ADVANCE"] = prev

    def test_a_database_error_does_not_break_the_poll(self):
        """Losing a retention optimisation is acceptable; failing the poll is not."""
        state = _state()
        reader = _reader_with(state)
        reader._incremental_snapshot_open = lambda: False

        def boom():
            raise RuntimeError("connection lost")

        reader._conn = boom
        reader.heartbeat()  # must not raise

    def test_pgoutput_peek_passes_the_publication(self):
        state = _state()
        reader = _reader_with(state, output_plugin="pgoutput")
        reader._incremental_snapshot_open = lambda: False
        reader.heartbeat()
        peek = next(s for s in state["sql"] if "peek" in s)
        assert "pg_logical_slot_peek_binary_changes" in peek
