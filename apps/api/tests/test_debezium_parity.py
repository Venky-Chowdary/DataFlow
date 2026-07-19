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
