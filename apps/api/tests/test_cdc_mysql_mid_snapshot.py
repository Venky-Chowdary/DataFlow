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
            "connectors.mysql_change_stream.read_table_scan_batch",
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
    assert cdc.resume_token["phase"] == "streaming"
    assert cdc.resume_token["file"] == "mysql-bin.000001"
    assert cdc.resume_token["pos"] == 4


def test_mysql_locked_snapshot_handoff_stamps_gtid() -> None:
    """Debezium-class handoff: locked SHOW MASTER STATUS + gtid_executed."""
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
    )

    class _Cur:
        def __init__(self):
            self._sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            self._sql = sql

        def fetchone(self):
            if "gtid_executed" in self._sql.lower():
                return ("uuid:1-10",)
            if "MASTER" in self._sql.upper() or "BINARY LOG" in self._sql.upper():
                return ("mysql-bin.000009", 1204)
            return None

    class _Conn:
        def autocommit(self, _v):
            return None

        def cursor(self):
            return _Cur()

        def close(self):
            return None

    batch = MagicMock()
    batch.headers = ["id"]
    batch.rows = [["1"]]
    empty = MagicMock()
    empty.rows = []

    with (
        patch.object(cdc, "_acquire_cdc_lease"),
        patch.object(cdc, "_ensure_decode_schema"),
        patch.object(cdc, "heartbeat"),
        patch(
            "connectors.mysql_change_stream.get_connection",
            return_value=_Conn(),
        ),
        patch(
            "connectors.mysql_change_stream.read_table_scan_batch",
            side_effect=[batch, empty],
        ),
    ):
        batches = list(cdc.snapshot())

    assert batches
    handoff = batches[-1].resume_token
    assert handoff["phase"] == "streaming"
    assert handoff["file"] == "mysql-bin.000009"
    assert handoff["pos"] == 1204
    assert handoff.get("gtid") == "uuid:1-10"
    # Mid-snapshot tokens must carry the same GTID for resume honesty.
    assert batches[0].resume_token.get("gtid") == "uuid:1-10"


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
