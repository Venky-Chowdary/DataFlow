"""Unit tests for the CDC transfer runner.

Real database connectivity is not required; the source and destination readers/
writers are patched so we can exercise the CDC engine logic end-to-end.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.cdc_transfer import CdcEngine, run_cdc_database_transfer
from src.transfer.models import EndpointConfig


def _batch(headers: list[str], rows: list[list[str]]) -> SimpleNamespace:
    return SimpleNamespace(headers=headers, rows=rows)


def test_cdc_engine_snapshot_returns_insert_only_batches():
    headers = ["id", "value"]
    rows = [["1", "a"], ["2", "b"]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [(_batch(headers, rows), None), (_batch(headers, []), None)]
        engine = CdcEngine(
            src_cfg={"database": "test"},
            src_type="generic_sql",
            table_name="src",
            cursor_field="id",
            primary_key="id",
            watermark=None,
            columns=headers,
        )
        batches = list(engine.snapshot())
    assert len(batches) == 1
    assert len(batches[0].inserts) == 2
    assert not batches[0].deletes


def test_cdc_engine_poll_filters_by_watermark():
    headers = ["id", "value"]
    rows = [["3", "c"], ["4", "d"]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [(_batch(headers, rows), None), (_batch(headers, []), None)]
        engine = CdcEngine(
            src_cfg={"database": "test"},
            src_type="generic_sql",
            table_name="src",
            cursor_field="id",
            primary_key="id",
            watermark="2",
            columns=headers,
        )
        batches = list(engine.poll())
    assert len(batches) == 1
    assert [r["id"] for r in batches[0].inserts] == ["3", "4"]


def test_cdc_engine_detects_soft_delete_tombstone():
    headers = ["id", "value", "deleted_at"]
    rows = [["1", "a", ""], ["2", "b", "2024-01-01"], ["3", "c", ""]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [(_batch(headers, rows), None), (_batch(headers, []), None)]
        engine = CdcEngine(
            src_cfg={"database": "test"},
            src_type="generic_sql",
            table_name="src",
            cursor_field="id",
            primary_key="id",
            watermark=None,
            columns=headers,
            schema={"id": "integer", "value": "string", "deleted_at": "timestamp"},
        )
        batches = list(engine.snapshot())
    assert len(batches[0].inserts) == 2
    assert batches[0].deletes == ["2"]


def test_run_cdc_database_transfer_requires_pk_and_cursor():
    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    with pytest.raises(ValueError, match="primary_key"):
        run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}],
            schema={"id": "integer", "value": "string"},
            stream_contracts=[{"sync_mode": "cdc"}],
        )


def test_run_cdc_database_transfer_performs_initial_snapshot():
    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    headers = ["id", "value"]
    rows = [["1", "a"], ["2", "b"]]

    mock_read = MagicMock(side_effect=[(_batch(headers, rows), None), (_batch(headers, []), None)])
    mock_write = MagicMock(return_value=(2, "abc", {}))
    mock_delete = MagicMock(return_value=0)

    with (
        patch("src.transfer.cdc_transfer._read_batch", mock_read),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", mock_delete),
    ):
        rows_written, ddl_log, dest_summary, columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "value", "target": "value"}],
            schema={"id": "integer", "value": "string"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "id", "cursor_field": "id"}],
            job_id="cdc-test",
        )

    assert rows_written == 2
    assert dest_summary["cdc"]["inserts"] == 2
    assert columns == ["id", "value"]
    assert mock_write.call_count == 1
    assert mock_write.call_args[1]["write_mode"] == "upsert"


def test_run_cdc_database_transfer_uses_mongodb_change_stream():
    """Exercise the MongoDB change-stream branch of the CDC runner."""
    from services.cdc_engine import ChangeBatch

    source = EndpointConfig(kind="database", format="mongodb", database="test", table="orders")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")

    mock_write = MagicMock(return_value=(2, "abc", {}))
    mock_delete = MagicMock(return_value=0)

    class FakeChangeStreamCdc:
        def __init__(self, *args, **kwargs):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(inserts=[{"_id": "1", "amount": "100.00"}, {"_id": "2", "amount": "200.00"}])

        def poll(self):
            return iter([])

    with (
        patch("src.transfer.cdc_transfer.MongodbChangeStreamCdc", FakeChangeStreamCdc),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", mock_delete),
        patch("src.transfer.cdc_transfer.get_watermark", return_value=None),
    ):
        rows_written, ddl_log, dest_summary, columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "_id", "target": "_id"}, {"source": "amount", "target": "amount"}],
            schema={"_id": "string", "amount": "decimal"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "_id"}],
            job_id="cdc-mongo-test",
        )

    assert rows_written == 2
    assert dest_summary["cdc"]["inserts"] == 2
    assert any("change_stream" in line for line in ddl_log)
