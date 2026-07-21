"""Tests for the MySQL binlog CDC parser and integration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
_row_event = pytest.importorskip(
    "pymysqlreplication.row_event",
    reason="requires the optional MySQL replication test dependency",
)
DeleteRowsEvent = _row_event.DeleteRowsEvent
UpdateRowsEvent = _row_event.UpdateRowsEvent
WriteRowsEvent = _row_event.WriteRowsEvent

from connectors.mysql_change_stream import MySqlChangeStreamCdc
from services.cdc_engine import ChangeBatch


@pytest.fixture
def base_cfg():
    return {
        "host": "localhost",
        "port": 3306,
        "database": "test",
        "username": "root",
        "password": "",
        "connection_string": "",
        "ssl": False,
    }


def test_resume_token_parsing(base_cfg: dict) -> None:
    cdc = MySqlChangeStreamCdc(
        base_cfg,
        table="orders",
        primary_key="id",
        resume_token='{"file": "mysql-bin.000001", "pos": 1234}',
    )
    assert cdc.resume_token == {"file": "mysql-bin.000001", "pos": 1234}


def test_poll_parses_binlog_events(base_cfg: dict) -> None:
    cdc = MySqlChangeStreamCdc(
        base_cfg,
        table="orders",
        primary_key="id",
        max_wait_seconds=0.5,
    )

    write = MagicMock(spec=WriteRowsEvent)
    write.schema = "test"
    write.table = "orders"
    # Real pymysqlreplication wraps write rows as {"values": {...}}.
    write.rows = [{"values": {"id": 1, "amount": 100.0}}]
    write.log_pos = 100

    update = MagicMock(spec=UpdateRowsEvent)
    update.schema = "test"
    update.table = "orders"
    update.rows = [{"after_values": {"id": 2, "amount": 200.0}}]
    update.log_pos = 200

    delete = MagicMock(spec=DeleteRowsEvent)
    delete.schema = "test"
    delete.table = "orders"
    delete.rows = [{"values": {"id": 3, "amount": 300.0}}]
    delete.log_pos = 300

    stream = MagicMock()
    stream.log_file = "mysql-bin.000001"
    stream.log_pos = 300
    stream.__iter__ = MagicMock(return_value=iter([write, update, delete]))
    stream.close = MagicMock()

    with patch("pymysqlreplication.BinLogStreamReader", return_value=stream), \
         patch.object(cdc, "_ensure_decode_schema", return_value={}):
        changes = list(cdc.poll())

    assert len(changes) == 1
    batch = changes[0]
    assert len(batch.inserts) == 1
    assert len(batch.updates) == 1
    assert batch.deletes == ["3"]
    assert batch.resume_token["file"] == "mysql-bin.000001"
    assert batch.resume_token["pos"] == 300
    assert batch.resume_token.get("tables") == ["orders"] or "tables" not in batch.resume_token


def test_run_cdc_database_transfer_uses_mysql_binlog():
    """Exercise the MySQL binlog branch of the CDC runner."""
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    source = EndpointConfig(kind="database", format="mysql", database="test", table="orders")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")

    mock_write = MagicMock(return_value=(2, "abc", {}))
    mock_delete = MagicMock(return_value=0)

    class FakeMySqlCdc:
        def __init__(self, *args, **kwargs):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(inserts=[{"id": "1", "amount": "100.00"}, {"id": "2", "amount": "200.00"}])

        def poll(self):
            return iter([])

    with (
        patch("src.transfer.cdc_transfer.MySqlChangeStreamCdc", FakeMySqlCdc),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", mock_delete),
        patch("src.transfer.cdc_transfer.get_watermark", return_value=None),
    ):
        rows_written, ddl_log, dest_summary, columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            schema={"id": "integer", "amount": "decimal"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "id"}],
            job_id="cdc-mysql-test",
        )

    assert rows_written == 2
    assert dest_summary["cdc"]["inserts"] == 2
    assert any("binlog" in line for line in ddl_log)
