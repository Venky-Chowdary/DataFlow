"""Wave 102: MySQL DDD-3 peek must include TableMapEvent (DBZ-3577)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_peek_binlog_kwargs_include_table_map_and_rotate():
    from pymysqlreplication.event import GtidEvent, RotateEvent
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        TableMapEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    stream = MySqlChangeStreamCdc(
        {
            "host": "localhost",
            "port": 3306,
            "database": "app",
            "username": "u",
            "password": "p",
        },
        table="orders",
        primary_key="id",
    )

    captured: dict = {}

    class FakeReader:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def __iter__(self):
            return iter(())

        def close(self):
            return None

    sig = MagicMock()
    sig.primary_key = "id"
    sig.table = "orders"
    sig.chunk_size = 50
    sig.gtid_low = ""

    with patch(
        "pymysqlreplication.BinLogStreamReader", FakeReader
    ), patch.object(
        stream, "_binlog_kwargs", wraps=stream._binlog_kwargs
    ) as wrapped:
        # Force _binlog_kwargs to return something iterable-friendly.
        def _kwargs(blocking, only_events):
            return {
                "blocking": blocking,
                "only_events": only_events,
                "server_id": 10000,
            }

        wrapped.side_effect = _kwargs
        stream._peek_stream_events_during_chunk(sig)

    only = captured.get("only_events") or []
    assert TableMapEvent in only, (
        "peek omitted TableMapEvent — every RowsEvent would be silently dropped"
    )
    assert RotateEvent in only
    assert WriteRowsEvent in only
    assert UpdateRowsEvent in only
    assert DeleteRowsEvent in only
    assert GtidEvent in only
