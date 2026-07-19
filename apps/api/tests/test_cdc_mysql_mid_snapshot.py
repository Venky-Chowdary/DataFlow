"""MySQL/Mongo mid-snapshot resume tokens must never be replaced by PK cursors."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.mysql_change_stream import MySqlChangeStreamCdc
from services.cdc_engine import ChangeBatch


def test_mysql_snapshot_batches_carry_binlog_resume_token() -> None:
    cdc = MySqlChangeStreamCdc(
        {
            "host": "localhost",
            "port": 3306,
            "database": "dataflow",
            "username": "u",
            "password": "p",
        },
        table="orders",
        primary_key="id",
        batch_size=2,
    )

    batch1 = MagicMock()
    batch1.headers = ["id", "amount"]
    batch1.rows = [["1", "10"], ["2", "20"]]
    batch2 = MagicMock()
    batch2.headers = ["id", "amount"]
    batch2.rows = [["3", "30"]]
    empty = MagicMock()
    empty.rows = []

    with (
        patch.object(cdc, "_current_binlog_position", return_value={"file": "mysql-bin.000001", "pos": 4}),
        patch.object(cdc, "_ensure_decode_schema"),
        patch(
            "connectors.mysql_change_stream.read_table_batch",
            side_effect=[batch1, batch2, empty],
        ),
    ):
        batches = list(cdc.snapshot())

    assert len(batches) == 3
    for b in batches[:-1]:
        assert isinstance(b.resume_token, dict)
        assert b.resume_token["phase"] == "snapshot"
        assert b.resume_token["file"] == "mysql-bin.000001"
        assert b.resume_token["pos"] == 4
    assert batches[-1].resume_token["phase"] == "streaming"
    assert batches[0].resume_token["offset"] == 2
    assert batches[1].resume_token["offset"] == 3


def test_mysql_poll_resumes_incomplete_snapshot() -> None:
    cdc = MySqlChangeStreamCdc(
        {
            "host": "localhost",
            "port": 3306,
            "database": "dataflow",
            "username": "u",
            "password": "p",
        },
        table="orders",
        primary_key="id",
        batch_size=10,
        resume_token={
            "phase": "snapshot",
            "offset": 2,
            "file": "mysql-bin.000001",
            "pos": 4,
            "table": "orders",
        },
    )
    called = {"snapshot": False}

    def fake_snapshot():
        called["snapshot"] = True
        yield ChangeBatch(resume_token={"phase": "streaming", "file": "mysql-bin.000001", "pos": 4})

    with patch.object(cdc, "snapshot", side_effect=fake_snapshot):
        list(cdc.poll())
    assert called["snapshot"] is True
