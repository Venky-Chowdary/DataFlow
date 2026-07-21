"""Chaos proofs for shared multi-table CDC ack barriers.

Invariant: a shared LSN/GTID must not advance until every demuxed table batch
for that token has been applied (``ack_barrier``). Crash mid-demux must
redeliver the whole window (at-least-once) — never silent skip.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc, encode_pg_resume_token
from services.cdc_engine import ChangeBatch
from services.cdc_multi_table import (
    MultiTableTransactionBuffer,
    SharedApplyChaosReport,
    should_ack_shared_batch,
    simulate_shared_apply_chaos,
)


def test_should_ack_only_on_barrier() -> None:
    mid = ChangeBatch(
        inserts=[{"id": "1"}],
        resume_token="lsn=1",
        table="orders",
        ack_barrier=False,
    )
    barrier = ChangeBatch(
        inserts=[{"id": "2"}],
        resume_token="lsn=1",
        table="users",
        ack_barrier=True,
    )
    heartbeat = ChangeBatch(resume_token="lsn=2", ack_barrier=True)
    empty = ChangeBatch(ack_barrier=True)
    assert should_ack_shared_batch(mid) is False
    assert should_ack_shared_batch(barrier) is True
    assert should_ack_shared_batch(heartbeat) is True
    assert should_ack_shared_batch(empty) is False


def test_chaos_crash_before_barrier_never_early_acks() -> None:
    buf = MultiTableTransactionBuffer()
    buf.begin("9", lsn="0/1")
    buf.insert("orders", {"id": "1"}, lsn="0/2")
    buf.insert("users", {"id": "u1"}, lsn="0/3")
    batches = buf.commit(
        lsn="0/4",
        resume_token="slot=s|phase=streaming|lsn=0/4",
        table_order=["orders", "users"],
    )
    report = simulate_shared_apply_chaos(batches, crash_before_barrier=True)
    assert report.early_ack is False
    assert report.redelivered is True
    assert report.ok is True
    assert report.applied_tables == ["orders"]  # crashed before users
    assert report.final_tables == ["orders", "users"]
    assert len(report.ack_calls) == 1
    assert report.ack_calls[0] == "slot=s|phase=streaming|lsn=0/4"


def test_chaos_no_crash_acks_once_at_barrier() -> None:
    batches = [
        ChangeBatch(
            inserts=[{"id": "1"}],
            resume_token="t1",
            table="a",
            ack_barrier=False,
        ),
        ChangeBatch(
            updates=[{"id": "2"}],
            resume_token="t1",
            table="b",
            ack_barrier=True,
        ),
    ]
    report = simulate_shared_apply_chaos(batches, crash_before_barrier=False)
    assert isinstance(report, SharedApplyChaosReport)
    assert report.early_ack is False
    # Without crash, pass-1 also acks at barrier, then pass-2 acks again
    # (at-least-once redelivery simulation) — never early.
    assert all(tok == "t1" for tok in report.ack_calls)
    assert report.final_tables == ["a", "b"]


def test_pg_shared_poll_chaos_ack_ordering() -> None:
    """End-to-end: demuxed PG poll window must not expose early-ack batches."""
    cdc = PostgreSqlChangeStreamCdc(
        {"database": "test", "schema": "public"},
        table=["orders", "users"],
        primary_key="id",
        cursor_key="chaos-ck",
        output_plugin="test_decoding",
        resume_token=encode_pg_resume_token("df_chaos", lsn="0/1000", phase="streaming"),
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("0/1", "BEGIN 1"),
        ("0/2", "table public.orders: INSERT: id[int4]:1 amount[numeric]:10"),
        ("0/3", "table public.users: INSERT: id[int4]:9 name[text]:'x'"),
        ("0/4", "COMMIT"),
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("connectors.postgresql_change_stream.get_connection", return_value=conn), \
         patch.object(cdc, "_ensure_slot", return_value="0/1000"), \
         patch.object(cdc, "_ensure_decode_schema", return_value={}), \
         patch.object(cdc, "_poll_signal_table"), \
         patch("services.cdc_incremental_runner.interleave_incremental_snapshot", return_value=iter(())):
        batches = [b for b in cdc.poll() if b.total_changes]

    report = simulate_shared_apply_chaos(batches, crash_before_barrier=True)
    assert report.ok
    assert report.early_ack is False
    assert set(report.final_tables) == {"orders", "users"}


def test_shared_transfer_uses_ack_barrier_helper(tmp_path) -> None:
    """Transfer shared path must call cdc.ack only after barrier batch."""
    from services.sync_cursor import SyncContract
    from src.transfer.cdc_transfer import _run_cdc_shared_multi_table
    from src.transfer.models import EndpointConfig

    acks: list = []

    class FakeCdc:
        def __init__(self):
            self._polled = False

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(resume_token="s0", ack_barrier=True)
            return
            yield  # pragma: no cover

        def poll(self):
            if self._polled:
                return
                yield  # pragma: no cover
            self._polled = True
            yield ChangeBatch(
                inserts=[{"id": "1"}],
                resume_token="s1",
                table="orders",
                ack_barrier=False,
            )
            yield ChangeBatch(
                inserts=[{"id": "2"}],
                resume_token="s1",
                table="users",
                ack_barrier=True,
            )
            return
            yield  # pragma: no cover

        def ack(self, token=None):
            acks.append(token)

        def close(self):
            pass

    source = EndpointConfig(
        kind="database", format="postgresql", database="app", table="orders", schema="public"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "d.db"), table="orders"
    )
    selected = [
        SyncContract(name="orders", primary_key="id", sync_mode="cdc"),
        SyncContract(name="users", primary_key="id", sync_mode="cdc"),
    ]

    def fake_apply(*args, **kwargs):
        change = args[4]
        return (change.total_changes, "ck", {}, 0)

    with patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", return_value=FakeCdc()), \
         patch("src.transfer.cdc_transfer._apply_change_batch", side_effect=fake_apply), \
         patch("src.transfer.cdc_transfer.resolve_dest_table", return_value="t"), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"}):
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
            job_id="chaos-job",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    # Snapshot may be skipped when a shared watermark already exists; the
    # chaos invariant under test is poll demux: never ack before barrier.
    assert acks.count("s1") == 1
    assert acks[-1] == "s1"
