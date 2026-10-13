"""Gate-8 verdict, quarantine/replay and CDC cycle operational events.

Counts, verdicts, connector types and position *kind* only — never row values,
table/slot names, or the LSN/binlog/token value itself.
"""

from __future__ import annotations

import logging

import pytest

import services.quarantine_dlq as dlq
import src.transfer.cdc_transfer as cdc_transfer
import src.transfer.reconcile_step as reconcile_step
from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from src.transfer.models import EndpointConfig

VALUE = "ROWVALUE-SENTINEL-93"
TABLE = "zz_secret_table"
LSN = "0/DEADBEEF"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "event", None):
            self.records.append(record)

    def named(self, name: str) -> list[logging.LogRecord]:
        return [r for r in self.records if r.getMessage() == name]


@pytest.fixture
def capture():
    handler = _Capture()
    loggers = [
        logging.getLogger(n)
        for n in (
            "src.transfer.reconcile_step",
            "services.quarantine_dlq",
            "src.transfer.cdc_transfer",
            "dataflow.transfer",
        )
    ]
    levels = [log.level for log in loggers]
    for log in loggers:
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        for log, level in zip(loggers, levels):
            log.removeHandler(handler)
            log.setLevel(level)


def _clean(records) -> None:
    for record in records:
        for fmt in (JsonFormatter(), TextFormatter()):
            out = fmt.format(record)
            for secret in (VALUE, TABLE, LSN):
                assert secret not in out, (secret, out)


def _reconcile(monkeypatch, recon):
    def fake_impl(**_kwargs):
        if isinstance(recon, BaseException):
            raise recon
        return dict(recon)

    monkeypatch.setattr(reconcile_step, "_run_reconciliation_impl", fake_impl)
    monkeypatch.setattr(reconcile_step, "_apply_unique_rule_checks", lambda *a, **k: None)
    return reconcile_step.run_reconciliation(
        endpoint=EndpointConfig(format="postgresql", table=TABLE),
        records=[{"v": VALUE}], columns=["v"], rows_written=1,
        writer_checksum="x", dest_summary={},
    )


# Verdict classification is proven on real report shapes in
# tests/test_gate8_verdict_real_shapes.py.


def test_gate8_exception_emits_errored_and_reraises(monkeypatch, capture):
    boom = RuntimeError(f"driver said {VALUE}")
    with pytest.raises(RuntimeError) as info:
        _reconcile(monkeypatch, boom)
    assert info.value is boom
    (event,) = capture.named("gate8.verify.completed")
    assert (event.outcome, event.error_type) == ("errored", "RuntimeError")
    _clean(capture.records)


def test_quarantine_persist_emits_counts(monkeypatch, capture):
    import services.quarantine_row_contract as contract

    monkeypatch.setattr(dlq, "append_dlq_event", lambda **kw: {"rows": kw["rows"]})
    monkeypatch.setattr(contract, "normalize_quarantine_rows", lambda rows, **_: list(rows))
    monkeypatch.setattr(contract, "assert_quarantine_rows_contract", lambda *a, **k: None)
    out = dlq.persist_rejected_rows(
        job_id="job-q1", workspace_id="ws-1", connector=TABLE,
        rejected_details=[{"row": {"v": VALUE}, "reason": f"bad {VALUE}"}] * 3,
    )
    assert out["rows"] == 3
    (event,) = capture.named("quarantine.rows.persisted")
    assert (event.job_id, event.rows, event.chunks, event.outcome) == ("job-q1", 3, 1, "succeeded")
    _clean(capture.records)


def test_replay_recorded_emits_closure(capture):
    findings = [{"row": {"v": VALUE}, "retry_status": "open"}]
    dlq.record_replay(
        findings, child_rejected=[], gate8_passed=True, child_job_id="job-child",
        rows_written=1, rejected=0,
    )
    (event,) = capture.named("quarantine.replay.recorded")
    assert (event.child_job_id, event.gate8_passed, event.rows_written) == ("job-child", True, 1)
    assert event.closure_verdict
    _clean(capture.records)


@pytest.mark.parametrize(
    "src, has_position, kind",
    [("postgresql", True, "lsn"), ("mysql", True, "binlog"), ("sqlite", True, "cursor"),
     ("postgresql", False, "none")],
)
def test_cdc_cycle_event_counts_lag_and_position_kind(capture, src, has_position, kind):
    cdc_transfer._emit_cdc_cycle(
        job_id="job-c1", workspace_id="ws-1", src_type=src, dest_type="postgresql",
        inserts=3, updates=2, deletes=1,
        lag_fields={"cdc_lag_seconds": 1.5, "replication_lag_bytes": 128,
                    "cdc_lag_basis": "commit_ts", "cdc_slot_name": TABLE},
        delivery="at-least-once", has_position=has_position,
    )
    (event,) = capture.named("cdc.cycle.completed")
    assert (event.inserts, event.updates, event.deletes) == (3, 2, 1)
    assert (event.lag_seconds, event.lag_bytes, event.position_kind) == (1.5, 128, kind)
    assert event.levelno == logging.INFO
    _clean(capture.records)


def _persist_with_source(monkeypatch, source):
    import services.quarantine_row_contract as contract

    monkeypatch.setattr(dlq, "append_dlq_event", lambda **kw: {"rows": kw["rows"]})
    monkeypatch.setattr(contract, "normalize_quarantine_rows", lambda rows, **_: list(rows))
    monkeypatch.setattr(contract, "assert_quarantine_rows_contract", lambda *a, **k: None)
    dlq.persist_rejected_rows(
        job_id="job-q2", source=source, rejected_details=[{"row": {"v": VALUE}}],
    )


@pytest.mark.parametrize("source", sorted(dlq._QUARANTINE_SOURCES))
def test_quarantine_source_literals_are_kept(monkeypatch, capture, source):
    _persist_with_source(monkeypatch, source)
    (event,) = capture.named("quarantine.rows.persisted")
    assert event.source == source


@pytest.mark.parametrize("source", [TABLE, f"s3://bucket/{TABLE}.csv", "public.orders"])
def test_quarantine_source_customer_identifier_becomes_other(monkeypatch, capture, source):
    _persist_with_source(monkeypatch, source)
    (event,) = capture.named("quarantine.rows.persisted")
    assert event.source == "other"
    _clean(capture.records)


def test_every_persist_caller_passes_a_known_source_literal():
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    found = set()
    for path in list((root / "src").rglob("*.py")) + list((root / "services").rglob("*.py")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for call in re.finditer(r"persist_rejected_rows\((.*?)\)", text, re.S):
            m = re.search(r"source=([\"'][^\"']*[\"']|[A-Za-z_][\w.]*)", call.group(1))
            if m:
                found.add(m.group(1).strip())
    assert found, "no persist_rejected_rows(source=...) callers found"
    for literal in found:
        assert literal.strip("\"'") in dlq._QUARANTINE_SOURCES, literal


def test_shared_multi_table_reader_emits_one_cycle_event_without_table_names(
    tmp_path, monkeypatch, capture
):
    """Drive ``_run_cdc_shared_multi_table`` in-process (stub log reader, SQLite
    dest, stub apply). One reader = one position and one lag, so one event."""
    from unittest.mock import patch

    from services.cdc_engine import ChangeBatch
    from services.sync_cursor import SyncContract

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    token = f"slot={TABLE}_slot|phase=streaming|lsn={LSN}"

    class FakeCdc:
        def __init__(self, *a, **k):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(inserts=[{"id": VALUE}], resume_token=token, table=TABLE)
            yield ChangeBatch(inserts=[{"id": "u1"}], resume_token=token, table="users")
            yield ChangeBatch(resume_token=token, ack_barrier=True)

        polled = False

        def poll(self):
            if FakeCdc.polled:
                return
            FakeCdc.polled = True
            yield ChangeBatch(
                updates=[{"id": VALUE, "v": VALUE}], deletes=[{"id": "u1"}],
                resume_token=token, table=TABLE, ack_barrier=True,
            )

        def ack(self, token=None):
            pass

        def close(self):
            pass

    def fake_apply(*args, **kwargs):
        change = args[4]
        return (len(change.inserts) + len(change.updates), "ck", {}, len(change.deletes))

    source = EndpointConfig(kind="database", format="postgresql", database="app", table=TABLE)
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "d.db"), table=TABLE
    )
    with patch.object(cdc_transfer, "PostgreSqlChangeStreamCdc", FakeCdc), \
         patch.object(cdc_transfer, "_apply_change_batch", side_effect=fake_apply), \
         patch.object(cdc_transfer, "resolve_dest_table", side_effect=lambda *_a, **_k: "t"), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"}):
        rows, _ddl, summary, _ = cdc_transfer._run_cdc_shared_multi_table(
            source, destination, [{"source": "id", "target": "id"}], {"id": "string"}, None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": TABLE, "selected": True, "primary_key": "id"},
                {"name": "users", "selected": True, "primary_key": "id"},
            ],
            selected=[
                SyncContract(name=TABLE, primary_key="id", sync_mode="cdc"),
                SyncContract(name="users", primary_key="id", sync_mode="cdc"),
            ],
            job_id="job-shared", checkpoint=None, checkpoint_service=None,
            backfill_new_fields=False, validation_mode="strict", limit=0,
            workspace_id="ws-shared",
        )

    assert summary.get("cdc_shared_reader") is True
    (event,) = capture.named("cdc.cycle.completed")
    assert event.outcome == "succeeded" and event.levelno == logging.INFO
    assert (event.job_id, event.workspace_id) == ("job-shared", "ws-shared")
    assert (event.source_type, event.destination_type) == ("postgresql", "sqlite")
    assert event.table_count == 2
    assert (event.upserts, event.deletes) == (3, 1)
    assert event.upserts + event.deletes == rows
    assert event.inserts is None and event.updates is None
    assert event.position_kind == ("lsn" if summary["cdc"]["watermark"] else "none")
    assert event.delivery == "at-least-once"
    _clean(capture.records)
    for record in capture.records:
        assert "users" not in JsonFormatter().format(record)


# --- cdc.cycle.completed on the failure paths -------------------------------

_SECRET_ERR = "boom at lsn=0/DEADBEEF in zz_secret_table"


def _failure_cases():
    from services.error_handling import TransferCancelled

    return [
        pytest.param(RuntimeError(_SECRET_ERR), "errored", logging.ERROR, id="error"),
        pytest.param(KeyboardInterrupt(), "cancelled", logging.INFO, id="interrupt"),
        pytest.param(TransferCancelled(_SECRET_ERR), "cancelled", logging.INFO, id="job-cancel"),
    ]


def _raising_reader(token, error, *, table):
    """Stub log reader: snapshot applies, the stream applies one batch, then raises."""
    from services.cdc_engine import ChangeBatch

    class FakeCdc:
        def __init__(self, *a, **k):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(inserts=[{"id": VALUE}, {"id": "2"}], resume_token=token, table=table)
            yield ChangeBatch(resume_token=token, ack_barrier=True)

        def poll(self):
            yield ChangeBatch(
                updates=[{"id": VALUE, "v": VALUE}], deletes=[{"id": "2"}],
                resume_token=token, table=table, ack_barrier=True,
            )
            raise error

        def ack(self, token=None):
            pass

        def close(self):
            pass

    return FakeCdc


def _fake_apply(*args, **kwargs):
    change = args[4]
    return (len(change.inserts) + len(change.updates), "ck", {}, len(change.deletes))


def _assert_failure_event(capture, error, outcome, level):
    (event,) = capture.named("cdc.cycle.completed")
    assert (event.outcome, event.levelno) == (outcome, level)
    assert event.error_type == type(error).__name__
    _clean(capture.records)
    for record in capture.records:
        for fmt in (JsonFormatter(), TextFormatter()):
            out = fmt.format(record)
            assert "DEADBEEF" not in out and "zz_secret_table" not in out
    return event


@pytest.mark.parametrize(("error", "outcome", "level"), _failure_cases())
def test_shared_reader_raising_mid_cycle_emits_counts_so_far_and_reraises(
    tmp_path, monkeypatch, capture, error, outcome, level
):
    from unittest.mock import patch

    from services.sync_cursor import SyncContract

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    token = f"slot={TABLE}_slot|phase=streaming|lsn={LSN}"
    source = EndpointConfig(kind="database", format="postgresql", database="app", table=TABLE)
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "d.db"), table=TABLE
    )
    with patch.object(cdc_transfer, "PostgreSqlChangeStreamCdc", _raising_reader(token, error, table=TABLE)), \
         patch.object(cdc_transfer, "_apply_change_batch", side_effect=_fake_apply), \
         patch.object(cdc_transfer, "resolve_dest_table", side_effect=lambda *_a, **_k: "t"), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"}), \
         pytest.raises(BaseException) as raised:
        cdc_transfer._run_cdc_shared_multi_table(
            source, destination, [{"source": "id", "target": "id"}], {"id": "string"}, None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": TABLE, "selected": True, "primary_key": "id"},
                {"name": "users", "selected": True, "primary_key": "id"},
            ],
            selected=[
                SyncContract(name=TABLE, primary_key="id", sync_mode="cdc"),
                SyncContract(name="users", primary_key="id", sync_mode="cdc"),
            ],
            job_id="job-shared", checkpoint=None, checkpoint_service=None,
            backfill_new_fields=False, validation_mode="strict", limit=0,
            workspace_id="ws-shared",
        )
    assert raised.value is error
    event = _assert_failure_event(capture, error, outcome, level)
    assert (event.job_id, event.workspace_id, event.table_count) == ("job-shared", "ws-shared", 2)
    assert (event.upserts, event.deletes) == (3, 1)


@pytest.mark.parametrize(("error", "outcome", "level"), _failure_cases())
def test_single_stream_raising_mid_cycle_emits_counts_so_far_and_reraises(
    tmp_path, monkeypatch, capture, error, outcome, level
):
    from unittest.mock import patch

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    token = f"slot={TABLE}_slot|phase=streaming|lsn={LSN}"
    source = EndpointConfig(kind="database", format="postgresql", database="app", table=TABLE)
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "d.db"), table=TABLE
    )
    with patch.object(cdc_transfer, "PostgreSqlChangeStreamCdc", _raising_reader(token, error, table=None)), \
         patch.object(cdc_transfer, "_apply_change_batch", side_effect=_fake_apply), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"}), \
         pytest.raises(BaseException) as raised:
        cdc_transfer._run_cdc_single_stream(
            source, destination, [{"source": "id", "target": "id"}], {"id": "string"}, None,
            sync_mode="cdc",
            stream_contracts=[{"name": TABLE, "selected": True, "primary_key": "id", "sync_mode": "cdc"}],
            job_id="job-single", workspace_id="ws-single", validation_mode="strict",
        )
    assert raised.value is error
    event = _assert_failure_event(capture, error, outcome, level)
    assert (event.job_id, event.workspace_id) == ("job-single", "ws-single")
    assert (event.inserts, event.updates, event.deletes) == (2, 1, 1)


def test_lag_fields_failure_on_the_error_path_is_logged_at_debug_by_class(monkeypatch, caplog):
    def broken(_cdc):
        raise ValueError(_SECRET_ERR)

    monkeypatch.setattr(cdc_transfer, "_cdc_lag_fields", broken)
    with caplog.at_level(logging.DEBUG, logger=cdc_transfer.logger.name):
        assert cdc_transfer._cdc_lag_fields_or_empty(object()) == {}
    (record,) = [r for r in caplog.records if r.name == cdc_transfer.logger.name]
    assert record.levelno == logging.DEBUG
    assert "ValueError" in record.getMessage()
    assert "DEADBEEF" not in record.getMessage() and not record.exc_info


def test_shared_reader_health_step_failure_after_close_propagates_without_a_cycle_event(
    tmp_path, monkeypatch, capture
):
    """The per-table health step runs after the reader closed. A failure there
    adds no CDC event; the original exception must still leave the reader so
    the engine records ``transfer.run.failed`` (proved live in
    ``test_shared_reader_health_failure_reaches_transfer_run_failed_live``)."""
    from unittest.mock import patch

    from services.cdc_engine import ChangeBatch
    from services.sync_cursor import SyncContract

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    token = f"slot={TABLE}_slot|phase=streaming|lsn={LSN}"
    boom = RuntimeError(_SECRET_ERR)

    class FakeCdc:
        closed = False

        def __init__(self, *a, **k):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(inserts=[{"id": VALUE}], resume_token=token, table=TABLE)
            yield ChangeBatch(resume_token=token, ack_barrier=True)

        def poll(self):
            return iter(())

        def ack(self, token=None):
            pass

        def close(self):
            FakeCdc.closed = True

    def health(*_a, **_k):
        assert FakeCdc.closed, "health step ran before the reader closed"
        raise boom

    source = EndpointConfig(kind="database", format="postgresql", database="app", table=TABLE)
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "d.db"), table=TABLE
    )
    with patch.object(cdc_transfer, "PostgreSqlChangeStreamCdc", FakeCdc), \
         patch.object(cdc_transfer, "_apply_change_batch", side_effect=_fake_apply), \
         patch.object(cdc_transfer, "resolve_dest_table", side_effect=lambda *_a, **_k: "t"), \
         patch("services.row_conservation.record_stream_health", side_effect=health), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "1"}), \
         pytest.raises(RuntimeError) as raised:
        cdc_transfer._run_cdc_shared_multi_table(
            source, destination, [{"source": "id", "target": "id"}], {"id": "string"}, None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": TABLE, "selected": True, "primary_key": "id"},
                {"name": "users", "selected": True, "primary_key": "id"},
            ],
            selected=[
                SyncContract(name=TABLE, primary_key="id", sync_mode="cdc"),
                SyncContract(name="users", primary_key="id", sync_mode="cdc"),
            ],
            job_id="job-shared", checkpoint=None, checkpoint_service=None,
            backfill_new_fields=False, validation_mode="strict", limit=0,
        )
    assert raised.value is boom
    assert capture.named("cdc.cycle.completed") == []


def test_shared_reader_fallback_warning_logs_the_class_not_the_message(monkeypatch, caplog):
    boom = RuntimeError(_SECRET_ERR)

    def shared(*_a, **_k):
        raise boom

    monkeypatch.setattr(cdc_transfer, "_run_cdc_shared_multi_table", shared)
    monkeypatch.setattr(
        cdc_transfer, "_run_cdc_multi_stream_sequential", lambda *a, **k: (0, [], {}, [])
    )
    src = EndpointConfig(kind="database", format="postgresql", database="app", table=TABLE)
    dst = EndpointConfig(kind="database", format="sqlite", database="d.db", table=TABLE)
    from services.sync_cursor import SyncContract

    with caplog.at_level(logging.DEBUG, logger=cdc_transfer.logger.name):
        cdc_transfer._run_cdc_multi_stream(
            src, dst, [], {}, None,
            sync_mode="cdc",
            stream_contracts=[{"name": TABLE, "selected": True}, {"name": "users", "selected": True}],
            selected=[SyncContract(name=TABLE, sync_mode="cdc"), SyncContract(name="users", sync_mode="cdc")],
            job_id="j", checkpoint=None, checkpoint_service=None,
            backfill_new_fields=False, validation_mode="strict", limit=0,
        )
    (warning,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning.getMessage() == "cdc.reader.fallback"
    assert warning.error_type == "RuntimeError" and not warning.exc_info
    for fmt in (JsonFormatter(), TextFormatter()):
        out = fmt.format(warning)
        assert "DEADBEEF" not in out and TABLE not in out, out


def _live_pg_logical() -> bool:
    try:
        from connectors.postgresql_conn import get_connection

        with get_connection(**_LIVE_PG, connection_string="", ssl=False) as conn, conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            return cur.fetchone()[0] == "logical"
    except Exception:
        return False


_LIVE_PG = dict(host="localhost", port=5432, database="dataflow", username="dataflow", password="dataflow")


@pytest.mark.skipif(not _live_pg_logical(), reason="PostgreSQL wal_level=logical not reachable on :5432")
def test_shared_reader_health_failure_reaches_transfer_run_failed_live(capture):
    """Live PG logical CDC, two tables on the shared reader, through execute_tracked:
    a health-step failure after the reader closed fails the job and is recorded
    as ``transfer.run.failed`` — with no extra CDC cycle event."""
    import uuid
    from unittest.mock import patch

    from connectors.postgresql_conn import get_connection
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    def sql(stmt, params=None):
        with get_connection(**_LIVE_PG, connection_string="", ssl=False) as conn, conn.cursor() as cur:
            cur.execute(stmt, params)
            rows = cur.fetchall() if cur.description else []
            conn.commit()
            return rows

    tag = uuid.uuid4().hex[:8]
    tables = [f"obs_h_{tag}_a", f"obs_h_{tag}_b"]
    dest_schema = f"obs_hdest_{tag}"
    maps = [
        {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER", "confidence": 0.99},
        {"source": "label", "target": "label", "source_type": "TEXT", "target_type": "TEXT", "confidence": 0.99},
    ]
    ep = dict(kind="database", format="postgresql", ssl=False, **_LIVE_PG)
    boom = RuntimeError(_SECRET_ERR)
    job_id = "obs-health-" + tag
    try:
        sql(f'CREATE SCHEMA "{dest_schema}"')
        for t in tables:
            sql(f'CREATE TABLE public."{t}" (id INT PRIMARY KEY, label TEXT)')
            sql(f"INSERT INTO public.\"{t}\" VALUES (1, 'a'), (2, 'b')")
        request = TransferRequest(
            source=EndpointConfig(**ep, schema="public", table=tables[0]),
            destination=EndpointConfig(**ep, schema=dest_schema, table=""),
            mappings=maps,
            sync_mode="cdc",
            validation_mode="strict",
            skip_preflight=True,
            delivery_guarantee="at_least_once",
            stream_contracts=[
                {"name": t, "selected": True, "snapshot_mode": "initial", "primary_key": "id",
                 "sync_mode": "cdc", "mappings": maps}
                for t in tables
            ],
        )
        with patch("services.row_conservation.record_stream_health", side_effect=boom) as health:
            result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert health.called, "the shared reader's health step never ran"
        assert result.success is False
        (failed,) = capture.named("transfer.run.failed")
        assert failed.levelno == logging.ERROR
        assert failed.job_id == job_id
        assert capture.named("transfer.run.succeeded") == []
        assert _SECRET_ERR in (result.error or ""), result.error
        assert capture.named("cdc.reader.fallback") == []
        assert capture.named("cdc.cycle.completed") == []
        _clean(capture.records)
    finally:
        for (slot,) in sql(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s",
            (f"%{tag}%",),
        ):
            sql("SELECT pg_drop_replication_slot(%s)", (slot,))
        for t in tables:
            sql(f'DROP TABLE IF EXISTS public."{t}" CASCADE')
        sql(f'DROP SCHEMA IF EXISTS "{dest_schema}" CASCADE')
