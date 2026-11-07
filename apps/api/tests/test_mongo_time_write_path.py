"""Mongo / warehouse / generic SQL TIME bind through bind_time_iso.

Mongo used ``str(value)`` so True became ``True`` and a leftover datetime
became a datetime string on a TIME column. Warehouse Snowflake TIME fell
through parse_sql_datetime and could invent a full timestamp. One clock
helper: reader-null is None, clocks are ISO, unfit cells raise.
"""

from __future__ import annotations

import sys
from datetime import time
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.mongodb_writer import write_mapped_rows  # noqa: E402
from connectors.sql_temporal import bind_time_clock, bind_time_iso  # noqa: E402
from connectors.warehouse_temporal import (  # noqa: E402
    format_bigquery_bind,
    format_snowflake_bind,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_bind_time_iso_clock_and_reader_null():
    assert bind_time_iso("15:30:00") == "15:30:00"
    assert bind_time_iso(time(15, 30, 0)) == "15:30:00"
    assert bind_time_clock("15:30:00") == time(15, 30, 0)
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__"):
        assert bind_time_iso(wire) is None, wire


def test_bind_time_iso_refuses_bool_epoch_and_informal():
    with pytest.raises(ValueError, match="refused|epoch|clock"):
        bind_time_iso(True)
    with pytest.raises(ValueError, match="refused|epoch|clock"):
        bind_time_iso(0)
    with pytest.raises(ValueError, match="refused|epoch|clock"):
        bind_time_iso("maybe")


def test_warehouse_time_uses_clock_not_datetime_invent():
    assert format_bigquery_bind("15:30:00", "TIME") == "15:30:00"
    assert format_snowflake_bind("15:30:00", "TIME") == "15:30:00"
    assert format_bigquery_bind(SQL_NULL_SENTINEL, "TIME") is None
    with pytest.raises(ValueError, match="refused|epoch|clock"):
        format_snowflake_bind(True, "TIME")
    with pytest.raises(ValueError, match="refused|epoch|clock"):
        format_bigquery_bind("2024-08-09T01:58:42Z", "TIME")


def _write_mongo_time(rows: list[list], dest_type: str = "TIME"):
    captured: list = []

    class _Coll:
        def find(self, *a, **k):
            return []

        def insert_many(self, docs, ordered=False):
            captured.extend(docs)
            return type("R", (), {"inserted_ids": list(range(len(docs)))})()

        def bulk_write(self, ops, ordered=False):
            captured.extend(ops)

    class _Db:
        def __getitem__(self, name):
            return _Coll()

        def list_collection_names(self, filter=None):  # noqa: A002
            return []

    class _Client:
        def __getitem__(self, name):
            return _Db()

        def close(self):
            return None

    with patch("connectors.mongodb_common._mongo_client", return_value=_Client()):
        result = write_mapped_rows(
            host="localhost",
            port=27017,
            database="testdb",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="shifts",
            headers=["id", "start"],
            data_rows=rows,
            mappings=[
                {"source": "id", "target": "id", "confidence": 1},
                {
                    "source": "start",
                    "target": "start",
                    "confidence": 1,
                    "target_type": dest_type,
                },
            ],
            column_types={"id": "string", "start": dest_type},
            dest_types={"id": "string", "start": dest_type},
            error_policy="quarantine",
        )
    return result, captured


def test_mongo_time_binds_iso_clock_not_str_true():
    result, captured = _write_mongo_time(
        [["1", "15:30:00"], ["2", SQL_NULL_SENTINEL], ["3", True]]
    )
    docs = [item for item in captured if isinstance(item, dict)]
    assert result.ok is True
    assert result.rows_written == 2
    assert result.rejected_rows == 1
    assert docs == [
        {"id": "1", "start": "15:30:00"},
        {"id": "2", "start": None},
    ]
    assert all(d.get("start") != "True" for d in docs)
    reasons = [str(d.get("reason") or "") for d in (result.rejected_details or [])]
    assert any("time" in r.lower() for r in reasons)
