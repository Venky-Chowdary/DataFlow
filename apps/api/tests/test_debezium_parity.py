"""Debezium-parity algorithm tests: txn buffer, snapshot modes, incremental, native paths."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    _parse_sql_redo,
    decode_logminer_token,
    encode_logminer_token,
)
from connectors.pgoutput_decoder import PgOutputDecoder
from connectors.sqlserver_cdc_native import (
    SqlServerNativeCdc,
    decode_mssql_cdc_token,
    encode_mssql_cdc_token,
)
from services.cdc_engine import ChangeBatch
from services.cdc_incremental_snapshot import (
    claim_next_signal,
    complete_signal,
    request_incremental_snapshot,
)
from services.cdc_incremental_runner import interleave_incremental_snapshot
from services.cdc_snapshot_mode import (
    SnapshotMode,
    parse_snapshot_mode,
    should_run_snapshot,
    should_run_stream,
)
from services.cdc_transaction_buffer import TransactionBuffer


def test_transaction_buffer_emits_only_on_commit() -> None:
    buf = TransactionBuffer()
    buf.begin("42")
    buf.insert({"id": "1"})
    buf.update({"id": "1", "v": "2"})
    buf.delete("9")
    assert buf.commit(resume_token="lsn-1") == ChangeBatch(
        inserts=[{"id": "1"}],
        updates=[{"id": "1", "v": "2"}],
        deletes=["9"],
        resume_token="lsn-1",
    )


def test_transaction_buffer_rollback_discards() -> None:
    buf = TransactionBuffer()
    buf.begin("1")
    buf.insert({"id": "1"})
    buf.rollback()
    assert buf.commit(resume_token="x") == ChangeBatch(resume_token="x")


def test_transaction_buffer_holds_without_flush_open_semantics() -> None:
    """Peek windows must not force-flush; open txn stays until COMMIT."""
    buf = TransactionBuffer()
    buf.begin("42")
    buf.insert({"id": "1"})
    assert buf.open_xid == "42"
    # Caller chooses not to flush_open — buffer still holds.
    assert buf.open_xid is not None
    batch = buf.commit(resume_token="lsn-commit")
    assert batch is not None
    assert batch.inserts == [{"id": "1"}]
    assert buf.open_xid is None


def test_cdc_transfer_txn_held_counts_as_activity() -> None:
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    source = EndpointConfig(kind="database", format="postgresql", database="t", table="orders")
    destination = EndpointConfig(kind="database", format="sqlite", database="/tmp/x.db", table="d")
    polls = {"n": 0}

    class Fake:
        def __init__(self, *a, **k):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            return iter([])

        def poll(self):
            polls["n"] += 1
            if polls["n"] == 1:
                yield ChangeBatch(resume_token={"txn_held": True, "open_xid": "9"})
            else:
                yield ChangeBatch(updates=[{"id": "1"}], resume_token="tok")

        def ack(self, token=None):
            pass

    with (
        patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", Fake),
        patch("src.transfer.cdc_transfer._write_batch", return_value=(1, "c", {})),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
        patch("src.transfer.cdc_transfer.get_watermark", return_value="existing"),
        patch("src.transfer.cdc_transfer.set_watermark"),
        patch.dict(
            "os.environ",
            {
                "DATAFLOW_CDC_MAX_IDLE_POLLS": "3",
                "DATAFLOW_CDC_MAX_POLL_ROUNDS": "5",
                "DATAFLOW_CDC_TXN_HOLD_SLEEP_SEC": "0",
            },
        ),
    ):
        rows, _ddl, summary, _headers = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "destination": "id"}],
            schema={"id": "text"},
            stream_contracts=[
                {
                    "sync_mode": "cdc",
                    "primary_key": "id",
                    "cursor_field": "id",
                    "snapshot_mode": "never",
                }
            ],
            sync_mode="cdc",
        )
    assert polls["n"] >= 2
    assert summary["cdc"]["updates"] >= 1


def test_side_channel_tokens_do_not_clobber_watermark() -> None:
    from services.cdc_resume_tokens import (
        is_durable_log_resume_token,
        is_side_channel_resume_token,
    )

    assert is_side_channel_resume_token({"incremental_snapshot": True, "last_pk": "1"})
    assert is_side_channel_resume_token({"txn_held": True, "open_xid": "9"})
    assert not is_durable_log_resume_token({"incremental_snapshot": True})
    assert is_durable_log_resume_token({"file": "bin.1", "pos": 10, "gtid": "u:1-2"})
    assert is_durable_log_resume_token("slot=df_x|phase=streaming|lsn=0/1A")


def test_postgres_lsn_guard_sql_safe_for_gtid_stamps() -> None:
    from connectors.writer_common import postgres_lsn_update_guard_sql

    sql = postgres_lsn_update_guard_sql("orders")
    assert "::pg_lsn" in sql
    assert "IS DISTINCT FROM" in sql
    assert "~" in sql


def test_cdc_lease_blocks_second_holder() -> None:
    from services.cdc_lease import CdcLeaseConflict, acquire_lease, release_lease

    a = acquire_lease("ck1", resource="pg_slot:s1", holder_id="worker-a")
    assert a.holder_id == "worker-a"
    with pytest.raises(CdcLeaseConflict):
        acquire_lease("ck1", resource="pg_slot:s1", holder_id="worker-b")
    # Same resource under different cursor also conflicts.
    with pytest.raises(CdcLeaseConflict):
        acquire_lease("ck2", resource="pg_slot:s1", holder_id="worker-b")
    assert release_lease("ck1", holder_id="worker-a") is True
    b = acquire_lease("ck1", resource="pg_slot:s1", holder_id="worker-b")
    assert b.holder_id == "worker-b"


def test_cdc_lease_stale_steal() -> None:
    from services.cdc_lease import acquire_lease, get_store

    first = acquire_lease("ck", resource="mysql_server_id:42", holder_id="old", ttl_sec=1.0)
    assert first.generation == 1
    get_store().debug_set_heartbeat("ck", 0.0)
    stolen = acquire_lease("ck", resource="mysql_server_id:42", holder_id="new", ttl_sec=1.0)
    assert stolen.holder_id == "new"
    assert stolen.generation == 2


def test_cdc_lease_guard_mssql_oracle_resources() -> None:
    from services.cdc_lease import (
        CdcLeaseConflict,
        CdcLeaseGuard,
        mssql_cdc_resource,
        oracle_cdc_resource,
    )

    mssql_res = mssql_cdc_resource("AppDb", "dbo", "Orders", mode="cdc")
    assert mssql_res == "mssql_cdc:appdb:dbo.orders"
    oracle_res = oracle_cdc_resource("APP", "ORDERS", mode="logminer", host="db1")
    assert oracle_res == "oracle_logminer:db1:APP.ORDERS"

    g1 = CdcLeaseGuard(cursor_key="ck-mssql", resource=mssql_res, holder_id="worker-a")
    g1.ensure()
    fields = g1.theater_fields()
    assert fields["cdc_lease_holder"] == "worker-a"
    assert fields["cdc_lease_resource"] == mssql_res
    assert fields.get("cdc_lease_backend") == "memory"

    g2 = CdcLeaseGuard(cursor_key="ck-mssql-other", resource=mssql_res, holder_id="worker-b")
    with pytest.raises(CdcLeaseConflict) as excinfo:
        g2.ensure()
    assert excinfo.value.holder_id == "worker-a"
    assert excinfo.value.to_dict()["code"] == "cdc_lease_conflict"
    g1.release()

    g3 = CdcLeaseGuard(cursor_key="ck-oracle", resource=oracle_res, holder_id="ora-a")
    g3.ensure()
    g3.renew()
    assert g3.acquired is True
    g3.release()
    assert g3.acquired is False


def test_sqlserver_oracle_connectors_expose_lease_api() -> None:
    from connectors.oracle_change_stream import OracleFlashbackCdc
    from connectors.oracle_logminer import OracleLogMinerCdc
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc
    from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc

    mssql = SqlServerNativeCdc(
        {"database": "app", "job_id": "j1"},
        table="orders",
        primary_key="id",
        cursor_key="ck:mssql",
    )
    assert mssql._lease.resource.startswith("mssql_cdc:")
    assert "cdc_lease_holder" in mssql.cdc_metadata() or mssql.cdc_metadata().get("plugin")

    ct = SqlServerChangeTrackingCdc(
        {"database": "app"}, table="orders", primary_key="id", cursor_key="ck:ct"
    )
    assert ct._lease.resource.startswith("mssql_ct:")

    ora = OracleLogMinerCdc(
        {"username": "APP", "host": "ora"},
        table="orders",
        primary_key="id",
        cursor_key="ck:ora",
    )
    assert "oracle_logminer" in ora._lease.resource

    fb = OracleFlashbackCdc(
        {"username": "APP", "host": "ora"},
        table="orders",
        primary_key="id",
        cursor_key="ck:fb",
    )
    assert "oracle_flashback" in fb._lease.resource


def test_signal_table_execute_and_stop(tmp_path, monkeypatch) -> None:
    import services.cdc_incremental_snapshot as snap
    from services.cdc_signal_table import apply_signal_row

    monkeypatch.setattr(snap, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap, "_DATA_DIR", str(tmp_path))

    out = apply_signal_row(
        source_key="src:pg",
        signal_id="sig-1",
        signal_type="execute-snapshot",
        data={"data-collections": ["public.orders"], "type": "incremental"},
    )
    assert out and out["action"] == "execute-snapshot"
    assert out["created"][0]["table"] == "orders"
    assert claim_next_signal("src:pg", table="orders") is not None

    stop = apply_signal_row(
        source_key="src:pg",
        signal_id="sig-2",
        signal_type="stop-snapshot",
        data={"data-collections": ["orders"]},
    )
    assert stop and stop["action"] == "stop-snapshot"
    assert claim_next_signal("src:pg", table="orders") is None


def test_signal_table_poll_tracks_processed_ids(tmp_path, monkeypatch) -> None:
    import services.cdc_incremental_snapshot as snap
    from services.cdc_signal_table import poll_signal_table

    monkeypatch.setattr(snap, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap, "_DATA_DIR", str(tmp_path))

    class Cur:
        def execute(self, *a, **k):
            pass

        def fetchall(self):
            return [("s1", "execute-snapshot", '{"table":"orders"}')]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Conn:
        def cursor(self):
            return Cur()

    results, seen = poll_signal_table(Conn(), source_key="src:pg", default_table="orders")
    assert len(results) == 1
    assert "s1" in seen
    results2, seen2 = poll_signal_table(
        Conn(), source_key="src:pg", default_table="orders", processed_ids=seen
    )
    assert results2 == []
    assert seen2 == seen


def test_snapshot_modes_debezium_compatible() -> None:
    assert parse_snapshot_mode("initial") == SnapshotMode.INITIAL
    assert should_run_snapshot(SnapshotMode.INITIAL, watermark=None) is True
    assert should_run_snapshot(SnapshotMode.INITIAL, watermark="slot|lsn") is False
    assert should_run_snapshot(SnapshotMode.ALWAYS, watermark="x") is True
    assert should_run_stream(SnapshotMode.INITIAL_ONLY) is False
    with pytest.raises(ValueError):
        should_run_snapshot(SnapshotMode.NEVER, watermark=None)
    assert should_run_snapshot(SnapshotMode.WHEN_NEEDED, watermark=None) is True
    assert should_run_snapshot(SnapshotMode.WHEN_NEEDED, watermark="x", resume_broken=True) is True


def test_incremental_snapshot_interleaved(tmp_path, monkeypatch) -> None:
    import services.cdc_incremental_snapshot as snap

    monkeypatch.setattr(snap, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap, "_DATA_DIR", str(tmp_path))

    sig = request_incremental_snapshot("src:pg", "orders", primary_key="id", chunk_size=2)
    assert sig.status == "pending"

    calls = {"n": 0}

    def fetch(signal):
        calls["n"] += 1
        if calls["n"] == 1:
            return [{"id": "1"}, {"id": "2"}], "2", False
        return [{"id": "3"}], "3", True

    batches = list(
        interleave_incremental_snapshot("src:pg", table="orders", fetch_chunk=fetch, max_chunks_per_poll=2)
    )
    assert len(batches) >= 1
    assert batches[0].inserts[0]["id"] == "1"
    # Second claim continues running signal
    claimed = claim_next_signal("src:pg", table="orders")
    assert claimed is not None
    complete_signal(claimed.id)


def test_incremental_snapshot_window_stream_wins(tmp_path, monkeypatch) -> None:
    import services.cdc_incremental_snapshot as snap

    monkeypatch.setattr(snap, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap, "_DATA_DIR", str(tmp_path))

    request_incremental_snapshot("src:pg", "orders", primary_key="id", chunk_size=10)

    def fetch(signal):
        return [{"id": "1", "v": "snap"}], "1", True

    def stream_during(signal):
        return [{"op": "u", "row": {"id": "1", "v": "live"}}]

    batches = list(
        interleave_incremental_snapshot(
            "src:pg",
            table="orders",
            fetch_chunk=fetch,
            stream_events_during_chunk=stream_during,
        )
    )
    assert len(batches) == 1
    assert batches[0].inserts[0]["v"] == "live"
    assert batches[0].resume_token["snapshot_window"]["stream_overrides"] == 1


def test_cancel_and_get_snapshot_signal(tmp_path, monkeypatch) -> None:
    import services.cdc_incremental_snapshot as snap
    from services.cdc_incremental_snapshot import cancel_signal, get_signal

    monkeypatch.setattr(snap, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap, "_DATA_DIR", str(tmp_path))

    sig = request_incremental_snapshot("src:mysql", "orders", primary_key="id")
    fetched = get_signal(sig.id)
    assert fetched is not None and fetched.status == "pending"
    cancelled = cancel_signal(sig.id)
    assert cancelled is not None and cancelled.status == "cancelled"
    assert claim_next_signal("src:mysql", table="orders") is None


def test_mysql_poll_preserves_gtid_on_resume_token(monkeypatch) -> None:
    import sys
    import types

    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    cdc = MySqlChangeStreamCdc(
        {"host": "localhost", "database": "db", "username": "u", "password": "p", "lease_holder_id": "gtid-test"},
        table="orders",
        primary_key="id",
        resume_token={"file": "bin.000001", "pos": 100, "table": "orders"},
        cursor_key=f"test:gtid:preserve",
    )

    class FakeStream:
        log_file = "bin.000001"
        log_pos = 250

        def __iter__(self):
            return iter([])

        def close(self):
            pass

    pkg = types.ModuleType("pymysqlreplication")
    pkg.BinLogStreamReader = MagicMock(return_value=FakeStream())
    event_mod = types.ModuleType("pymysqlreplication.event")
    event_mod.QueryEvent = type("QueryEvent", (), {})
    event_mod.RotateEvent = type("RotateEvent", (), {})
    event_mod.XidEvent = type("XidEvent", (), {})
    row_mod = types.ModuleType("pymysqlreplication.row_event")
    row_mod.DeleteRowsEvent = type("DeleteRowsEvent", (), {})
    row_mod.UpdateRowsEvent = type("UpdateRowsEvent", (), {})
    row_mod.WriteRowsEvent = type("WriteRowsEvent", (), {})
    monkeypatch.setitem(sys.modules, "pymysqlreplication", pkg)
    monkeypatch.setitem(sys.modules, "pymysqlreplication.event", event_mod)
    monkeypatch.setitem(sys.modules, "pymysqlreplication.row_event", row_mod)

    with (
        patch.object(cdc, "_binlog_kwargs", return_value={"server_id": 1}),
        patch.object(cdc, "_ensure_decode_schema"),
        patch.object(cdc, "heartbeat"),
        patch.object(cdc, "_poll_signal_table"),
        patch.object(
            cdc,
            "_current_binlog_position",
            return_value={"file": "bin.000001", "pos": 250, "gtid": "uuid:1-10", "table": "orders"},
        ),
        patch(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            return_value=iter([]),
        ),
    ):
        batches = list(cdc.poll())
    assert batches
    # Idle poll still refreshes GTID into resume token (commit-boundary path).
    token = batches[0].resume_token
    if isinstance(token, dict) and token.get("txn_held"):
        token = token.get("token") or {}
    assert token.get("gtid") == "uuid:1-10"


def test_extract_cdc_lsn_supports_gtid_mongo_scn() -> None:
    from connectors.writer_common import extract_cdc_lsn

    assert extract_cdc_lsn({"file": "bin.1", "pos": 9}) == "bin.1:9"
    assert extract_cdc_lsn({"gtid": "uuid:1-3"}) == "gtid:uuid:1-3"
    assert extract_cdc_lsn({"_data": "mongo-token"}) == "mongo-token"
    assert extract_cdc_lsn({"scn": 99}) == "99"
    assert extract_cdc_lsn("slot=x|phase=streaming|lsn=0/1A") == "0/1A"


def test_pg_heartbeat_emits_logical_message() -> None:
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    cdc = PostgreSqlChangeStreamCdc(
        {"host": "localhost", "database": "db", "username": "u", "password": "p"},
        table="orders",
        primary_key="id",
        cursor_key="ck",
        schema="public",
    )
    cdc.slot_name = "df_slot"
    cdc._pending_ack_lsn = None
    cur = MagicMock()
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=None)
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    with patch.object(cdc, "_conn", return_value=conn):
        cdc.heartbeat()
    assert cdc._last_heartbeat_at is not None
    assert cur.execute.called
    sql = cur.execute.call_args[0][0]
    assert "pg_logical_emit_message" in sql


def test_sqlserver_and_oracle_have_incremental_chunk() -> None:
    mssql = SqlServerNativeCdc({"host": "localhost"}, table="orders", primary_key="id")
    oracle = OracleLogMinerCdc({"username": "APP"}, table="orders", primary_key="id")
    assert hasattr(mssql, "_fetch_incremental_chunk")
    assert hasattr(oracle, "_fetch_incremental_chunk")
    assert mssql.source_key
    assert oracle.source_key


def test_pgoutput_begin_commit_markers() -> None:
    import struct

    decoder = PgOutputDecoder()
    # Minimal Begin: B + 16 bytes + xid int32
    begin = b"B" + (b"\x00" * 16) + struct.pack("!i", 99)
    changes = decoder.feed(begin)
    assert changes and changes[0].op == "begin" and changes[0].xid == "99"
    assert decoder.feed(b"C" + b"\x00" * 25)[0].op == "commit"


def test_sqlserver_native_token_and_unavailable() -> None:
    token = encode_mssql_cdc_token("0x001122", table="orders", phase="streaming")
    assert decode_mssql_cdc_token(token)["lsn"] in {"001122", "0x001122"}
    cdc = SqlServerNativeCdc({"host": "localhost"}, table="orders", primary_key="id")
    with patch.object(cdc, "_conn", side_effect=RuntimeError("no db")):
        assert cdc.is_available() is False


def test_oracle_logminer_sql_parse_and_token() -> None:
    row = _parse_sql_redo('UPDATE "T" SET "AMOUNT" = \'99\' WHERE "ID" = \'1\'', op="update")
    assert row["AMOUNT"] == "99"
    assert row["ID"] == "1"
    token = encode_logminer_token(12345, table="ORDERS")
    assert decode_logminer_token(token)["scn"] == 12345
    cdc = OracleLogMinerCdc({"username": "APP"}, table="orders", primary_key="id")
    with patch.object(cdc, "_conn", side_effect=RuntimeError("no db")):
        assert cdc.is_available() is False


def test_cdc_transfer_honors_snapshot_mode_never() -> None:
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    source = EndpointConfig(kind="database", format="postgresql", database="t", table="orders")
    destination = EndpointConfig(kind="database", format="sqlite", database="/tmp/x.db", table="d")

    state = {"snapped": False}

    class Fake:
        def __init__(self, *a, **k):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            state["snapped"] = True
            yield ChangeBatch(inserts=[{"id": "1"}])

        def poll(self):
            yield ChangeBatch(updates=[{"id": "1", "v": "2"}], resume_token="tok")

        def ack(self, token=None):
            pass

    with (
        patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", Fake),
        patch("src.transfer.cdc_transfer._write_batch", return_value=(1, "c", {})),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
        patch("src.transfer.cdc_transfer.get_watermark", return_value="existing"),
        patch("src.transfer.cdc_transfer.set_watermark"),
        patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "1"}),
    ):
        _rows, ddl, summary, _ = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}],
            schema={"id": "string"},
            stream_contracts=[
                {
                    "sync_mode": "cdc",
                    "primary_key": "id",
                    "cursor_field": "id",
                    "snapshot_mode": "never",
                }
            ],
            job_id="dz-never",
        )
    assert state["snapped"] is False
    assert summary["cdc"]["updates"] >= 1
    assert any("snapshot_mode=never" in line for line in ddl)
