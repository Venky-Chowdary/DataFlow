"""Multi-table single-reader CDC (Debezium-class demux) proofs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc, encode_pg_resume_token
from services.cdc_engine import ChangeBatch
from services.cdc_multi_table import (
    MultiTableTransactionBuffer,
    can_share_log_reader,
    normalize_table_list,
    shared_route_cursor_key,
    tables_digest,
)


def test_normalize_and_digest_stable() -> None:
    assert normalize_table_list(["Users", "orders", "users"]) == ["Users", "orders"]
    assert tables_digest(["b", "a"]) == tables_digest(["a", "b"])
    assert can_share_log_reader("postgresql", 2) is True
    assert can_share_log_reader("mysql", 2) is True
    assert can_share_log_reader("sqlserver", 2) is True
    assert can_share_log_reader("mssql", 2) is True
    assert can_share_log_reader("oracle", 2) is True
    assert can_share_log_reader("mongodb", 2) is False
    assert can_share_log_reader("postgresql", 1) is False
    assert can_share_log_reader("sqlserver", 1) is False
    assert can_share_log_reader("oracle", 1) is False


def test_multi_table_txn_buffer_demux_and_ack_barrier() -> None:
    buf = MultiTableTransactionBuffer()
    buf.begin("42", lsn="0/1")
    buf.insert("orders", {"id": "1"}, lsn="0/2")
    buf.insert("users", {"id": "u1"}, lsn="0/3")
    buf.update("orders", {"id": "1", "amount": "9"}, lsn="0/4")
    batches = buf.commit(lsn="0/5", resume_token="slot=x|phase=streaming|lsn=0/5", table_order=["orders", "users"])
    assert len(batches) == 2
    assert batches[0].table == "orders"
    assert batches[0].ack_barrier is False
    assert len(batches[0].inserts) == 1 and len(batches[0].updates) == 1
    assert batches[1].table == "users"
    assert batches[1].ack_barrier is True
    assert batches[0].resume_token == batches[1].resume_token


def test_pg_shared_reader_slot_and_publication_names() -> None:
    cdc = PostgreSqlChangeStreamCdc(
        {"database": "app", "job_id": "j1"},
        table=["orders", "users"],
        primary_key="id",
        primary_keys={"orders": "id", "users": "user_id"},
        cursor_key="cdc-shared:postgresql:app:x:j1",
        output_plugin="test_decoding",
    )
    assert cdc.tables == ["orders", "users"]
    assert "mt_" in cdc.slot_name
    assert cdc.primary_keys["users"] == "user_id"
    assert cdc._lease.meta.get("shared_reader") is True


def test_pg_shared_poll_demuxes_two_tables() -> None:
    cdc = PostgreSqlChangeStreamCdc(
        {"database": "test", "schema": "public"},
        table=["orders", "users"],
        primary_key="id",
        cursor_key="shared-ck",
        output_plugin="test_decoding",
        resume_token=encode_pg_resume_token("df_test_mt", lsn="0/1000", phase="streaming"),
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("0/16B3700", "BEGIN 9"),
        ("0/16B3710", "table public.orders: INSERT: id[int4]:1 amount[numeric]:10"),
        ("0/16B3720", "table public.users: INSERT: id[int4]:2 name[text]:'a'"),
        ("0/16B3748", "COMMIT"),
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
        batches = list(cdc.poll())

    tagged = [b for b in batches if b.total_changes]
    assert len(tagged) == 2
    by_table = {b.table: b for b in tagged}
    assert "orders" in by_table and "users" in by_table
    assert by_table["orders"].inserts[0]["id"] == "1"
    assert by_table["users"].inserts[0]["id"] == "2"
    assert tagged[-1].ack_barrier is True
    assert tagged[0].ack_barrier is False


def test_shared_transfer_path_applies_per_table(tmp_path, monkeypatch) -> None:
    from src.transfer.cdc_transfer import _run_cdc_shared_multi_table
    from src.transfer.models import EndpointConfig
    from services.sync_cursor import SyncContract

    class FakeCdc:
        def __init__(self, *a, **k):
            self.closed = False

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(
                inserts=[{"id": "1"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1",
                table="orders",
            )
            yield ChangeBatch(
                inserts=[{"id": "u1"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1",
                table="users",
            )
            yield ChangeBatch(
                resume_token="slot=s|phase=streaming|lsn=0/1",
                ack_barrier=True,
            )

        def poll(self):
            yield ChangeBatch(
                updates=[{"id": "1", "amount": "2"}],
                resume_token="slot=s|phase=streaming|lsn=0/2",
                table="orders",
                ack_barrier=False,
            )
            yield ChangeBatch(
                inserts=[{"id": "u2"}],
                resume_token="slot=s|phase=streaming|lsn=0/2",
                table="users",
                ack_barrier=True,
            )
            return
            yield  # pragma: no cover

        def ack(self, token=None):
            self.acked = token

        def close(self):
            self.closed = True

    source = EndpointConfig(
        kind="database", format="postgresql", database="app", table="orders", schema="public"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="orders"
    )
    selected = [
        SyncContract(name="orders", primary_key="id", sync_mode="cdc"),
        SyncContract(name="users", primary_key="id", sync_mode="cdc"),
    ]
    applied: list[str] = []

    def fake_apply(*args, **kwargs):
        change = args[4]
        applied.append(change.table or "")
        return (len(change.inserts) + len(change.updates), "ck", {}, len(change.deletes))

    with patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakeCdc), \
         patch("src.transfer.cdc_transfer._apply_change_batch", side_effect=fake_apply), \
         patch("src.transfer.cdc_transfer.resolve_dest_table", side_effect=lambda *_a, **_k: "t"), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"}):
        rows, ddl, summary, _ = _run_cdc_shared_multi_table(
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
            job_id="job-shared",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    assert rows >= 4
    assert "shared_reader" in ddl[0]
    assert summary.get("cdc_shared_reader") is True
    assert "orders" in applied and "users" in applied
    assert shared_route_cursor_key(
        engine="postgresql", database="app", tables=["orders", "users"], job_id="job-shared"
    ).startswith("cdc-shared:")
