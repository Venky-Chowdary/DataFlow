"""Complete SQL Server Change Tracking + Oracle flashback CDC unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.oracle_change_stream import (
    OracleFlashbackCdc,
    decode_oracle_resume_token,
    encode_oracle_resume_token,
)
from connectors.sqlserver_change_stream import (
    SqlServerChangeTrackingCdc,
    decode_sqlserver_resume_token,
    encode_sqlserver_resume_token,
)
from services.cdc_engine import ChangeBatch


def test_sqlserver_resume_token_roundtrip() -> None:
    token = encode_sqlserver_resume_token(42, table="orders", phase="snapshot", offset=100)
    state = decode_sqlserver_resume_token(token)
    assert state["version"] == 42
    assert state["phase"] == "snapshot"
    assert state["offset"] == 100
    legacy = decode_sqlserver_resume_token("mssql-ct:orders:7")
    assert legacy["version"] == 7
    assert legacy["phase"] == "streaming"


def test_sqlserver_snapshot_dumps_rows_and_handoff() -> None:
    cdc = SqlServerChangeTrackingCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        batch_size=2,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("amount",)]
    # current version, then two dump pages, then empty
    cur.fetchone.side_effect = [(10,),]
    cur.fetchall.side_effect = [
        [("1", "10"), ("2", "20")],
        [("3", "30")],
        [],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        batches = list(cdc.snapshot())

    assert len(batches) == 3  # 2 data + handoff
    assert len(batches[0].inserts) == 2
    assert decode_sqlserver_resume_token(batches[0].resume_token)["phase"] == "snapshot"
    assert decode_sqlserver_resume_token(batches[-1].resume_token)["phase"] == "streaming"
    assert decode_sqlserver_resume_token(batches[-1].resume_token)["version"] == 10
    assert sum(len(b.inserts) for b in batches) == 3


def test_sqlserver_poll_hydrates_and_deletes() -> None:
    token = encode_sqlserver_resume_token(5, table="orders", phase="streaming")
    cdc = SqlServerChangeTrackingCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        resume_token=token,
    )
    conn = MagicMock()
    cur = MagicMock()
    # CT rows then hydrate SELECT *
    cur.description = [("id",), ("amount",)]
    cur.fetchall.side_effect = [
        [(6, "I", "1"), (7, "U", "2"), (8, "D", "3")],
        [("1", "100"), ("2", "200")],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        batches = list(cdc.poll())

    assert len(batches) == 1
    batch = batches[0]
    assert batch.deletes == ["3"]
    assert any(r.get("id") == "1" for r in batch.inserts)
    assert any(r.get("id") == "2" for r in batch.updates)
    assert decode_sqlserver_resume_token(batch.resume_token)["version"] == 8


def test_oracle_resume_token_and_snapshot() -> None:
    token = encode_oracle_resume_token(1000, table="ORDERS", phase="snapshot", offset=50)
    state = decode_oracle_resume_token(token)
    assert state["scn"] == 1000
    assert state["offset"] == 50

    cdc = OracleFlashbackCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        batch_size=2,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",), ("DF_RN",)]
    cur.fetchone.side_effect = [(9000,)]
    cur.fetchall.side_effect = [
        [("1", "10", 1), ("2", "20", 2)],
        [],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        batches = list(cdc.snapshot())

    assert len(batches[0].inserts) == 2
    assert "DF_RN" not in batches[0].inserts[0]
    assert decode_oracle_resume_token(batches[-1].resume_token)["scn"] == 9000
    assert decode_oracle_resume_token(batches[-1].resume_token)["phase"] == "streaming"


def test_cdc_transfer_sqlserver_branch_end_to_end() -> None:
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    source = EndpointConfig(
        kind="database", format="sqlserver", database="app", table="orders", schema="dbo"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database="/tmp/x.db", table="dst"
    )

    class FakeCt:
        def __init__(self, *a, **k):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(
                inserts=[{"id": "1", "amount": "10"}],
                resume_token=encode_sqlserver_resume_token(1, table="orders", phase="snapshot", offset=1),
            )
            yield ChangeBatch(
                resume_token=encode_sqlserver_resume_token(1, table="orders", phase="streaming"),
            )

        def poll(self):
            yield ChangeBatch(
                updates=[{"id": "1", "amount": "11"}],
                deletes=["9"],
                resume_token=encode_sqlserver_resume_token(2, table="orders", phase="streaming"),
            )
            return
            yield  # pragma: no cover

        def ack(self, token=None):
            self.acked = token

        def lag_seconds(self):
            return 0.0

    mock_write = MagicMock(return_value=(1, "chk", {}))
    mock_delete = MagicMock(return_value=1)

    with (
        patch("src.transfer.cdc_transfer.SqlServerChangeTrackingCdc", FakeCt),
        patch("connectors.sqlserver_change_stream.SqlServerChangeTrackingCdc", FakeCt),
        patch("src.transfer.cdc_transfer.resolve_driver_type", return_value="sqlserver"),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", mock_delete),
        patch("src.transfer.cdc_transfer.get_watermark", return_value=None),
        patch("src.transfer.cdc_transfer.set_watermark"),
        patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"}),
    ):
        rows, ddl, summary, _ = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            schema={"id": "string", "amount": "string"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "id", "cursor_field": "id"}],
            job_id="cdc-mssql",
        )

    assert rows >= 1
    assert summary["cdc"]["inserts"] >= 1
    assert summary["cdc"]["deletes"] >= 1
    assert any("change_tracking" in line for line in ddl)
    assert mock_delete.called
