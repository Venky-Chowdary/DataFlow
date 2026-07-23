"""Wave AA accuracy: Avro/ORC caps, Kafka tombstone cursors, Azure SQL, REST page, Mongo upsert."""

from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_avro_over_max_rows_fail_closed():
    pytest.importorskip("fastavro")
    import fastavro

    from services.file_parser import FileParser

    schema = {
        "type": "record",
        "name": "Row",
        "fields": [{"name": "id", "type": "int"}],
    }
    buf = io.BytesIO()
    fastavro.writer(buf, schema, [{"id": 1}, {"id": 2}, {"id": 3}])
    result = FileParser.parse_avro(buf.getvalue(), max_rows=2)
    assert result.success is False
    assert result.row_count >= 3
    assert "streaming" in (result.error or "").lower()


def test_kafka_empty_output_keeps_pending_offsets():
    from connectors.kafka_reader import read_topic_batch

    consumer = MagicMock()
    consumer.poll_rows.return_value = []  # tombstones / skips → no rows
    consumer.pending_offsets.return_value = [{"topic": "t", "partition": 0, "offset": 42}]
    consumer.close = MagicMock()

    with patch("connectors.kafka_reader.KafkaDebeziumConsumer", return_value=consumer):
        batch, cursor = read_topic_batch(
            cfg={"host": "localhost", "group_id": "g"},
            topic="t",
            columns=None,
            limit=10,
        )

    assert batch.rows == []
    assert cursor is not None
    assert cursor["pending_offsets"][0]["offset"] == 42


def test_azure_sql_fallback_uses_offset_fetch_syntax():
    from connectors.generic_sql import _read_table_raw

    conn = MagicMock()
    probe = MagicMock()
    probe.keys.return_value = ["id", "name"]
    result = MagicMock()
    result.keys.return_value = ["id", "name"]
    result.fetchall.return_value = [(1, "a")]
    conn.execute.side_effect = [probe, result]

    headers, rows = _read_table_raw(
        conn, "t", "dbo", offset=100, limit=50, dialect="azure_sql_database"
    )
    assert headers == ["id", "name"]
    assert rows == [["1", "a"]]
    probe_sql = conn.execute.call_args_list[0][0][0].text
    page_sql = conn.execute.call_args_list[1][0][0].text
    assert "TOP 0" in probe_sql
    assert "OFFSET 100 ROWS" in page_sql
    assert "FETCH NEXT 50 ROWS ONLY" in page_sql
    assert "LIMIT" not in page_sql


def test_rest_page_pagination_uses_row_offset_not_page_number(monkeypatch):
    import connectors.rest_api as ra

    seen: list[dict] = []

    def fake_read_page(cfg, pagination, next_url=None):
        seen.append(dict(pagination))
        page = int(pagination.get("page") or 1)
        if page == 3:
            return [{"id": i} for i in range(100, 150)], None, False
        return [], None, False

    monkeypatch.setattr(ra, "_read_page", fake_read_page)
    monkeypatch.setattr(
        ra,
        "_resolve_config",
        lambda cfg: {
            **cfg,
            "pagination_type": "page",
            "offset_param": "offset",
            "limit_param": "limit",
            "page_param": "page",
            "cursor_param": "cursor",
            "data_path": "",
        },
    )
    batch = ra.read_object(cfg={"host": "http://example"}, limit=50, offset=100)
    assert len(batch.rows) == 50
    # Row offset 100 at page_size 50 → page 3 (not page 101 from old offset-as-page bug).
    assert seen[0]["page"] == 3
    assert seen[0]["page"] != 101
    assert seen[0]["limit"] == 50


def test_mongodb_upsert_refuses_unmapped_conflict_key():
    from connectors.mongodb_writer import write_mapped_rows

    coll = MagicMock()
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)
    client.close = MagicMock()

    with patch("connectors.mongodb_common._mongo_client", return_value=client):
        result = write_mapped_rows(
            host="localhost",
            port=27017,
            database="app",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["amount"],
            data_rows=[["10"]],
            mappings=[{"source": "amount", "target": "amount"}],
            column_types={"amount": "DECIMAL"},
            write_mode="upsert",
            conflict_columns=["order_id"],  # not mapped
            create_table=True,
        )

    assert result.ok is False
    assert "mapped conflict columns" in (result.error or "").lower()
    coll.insert_many.assert_not_called()
    coll.bulk_write.assert_not_called()
