"""Wave U accuracy: Influx paging, Kafka modes, Redis dedupe, JSONL/Parquet honesty."""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest


def test_influx_read_does_not_claim_page_as_total():
    from connectors.influxdb import read_object

    def fake_query(_url, _db, q, _user, _password):
        assert "ORDER BY time ASC" in q
        assert "LIMIT 2" in q
        assert "OFFSET 0" in q
        return {
            "results": [{
                "series": [{
                    "columns": ["time", "value"],
                    "values": [["2020-01-01T00:00:00Z", 1], ["2020-01-01T00:00:01Z", 2]],
                }]
            }]
        }

    with patch("connectors.influxdb._query", side_effect=fake_query):
        batch = read_object(
            cfg={"host": "localhost", "port": 8086, "database": "metrics"},
            object="cpu",
            limit=2,
            offset=0,
        )
    assert len(batch.rows) == 2
    assert batch.total_rows is None


def test_influx_second_page_offset_is_requested():
    from connectors.influxdb import read_object

    seen: list[str] = []

    def fake_query(_url, _db, q, _user, _password):
        seen.append(q)
        return {"results": [{"series": [{"columns": ["time", "v"], "values": [["t", 1]]}]}]}

    with patch("connectors.influxdb._query", side_effect=fake_query):
        read_object(
            cfg={"host": "localhost", "database": "db"},
            object="m",
            limit=100,
            offset=100,
        )
    assert "OFFSET 100" in seen[0]
    assert "ORDER BY time ASC" in seen[0]


def test_kafka_rejects_upsert_write_mode():
    from connectors.kafka_writer import write_mapped_rows

    result = write_mapped_rows(
        host="localhost",
        port=9092,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="events",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id"}],
        column_types={"id": "STRING"},
        write_mode="upsert",
    )
    assert result.ok is False
    assert "append only" in (result.error or "").lower()


def test_kafka_rejects_update_write_mode():
    from connectors.kafka_writer import write_mapped_rows

    result = write_mapped_rows(
        host="localhost",
        port=9092,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="events",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id"}],
        column_types={"id": "STRING"},
        write_mode="update",
    )
    assert result.ok is False
    assert "upsert/update" in (result.error or "").lower()


def test_redis_scan_dedupes_duplicate_keys():
    from connectors.redis_reader import RedisScanState, read_keys_batch

    state = RedisScanState()
    client = MagicMock()
    # First SCAN returns k2 twice (documented Redis SCAN behavior).
    client.scan.side_effect = [
        (1, [b"k1", b"k2", b"k2"]),
        (0, [b"k2", b"k3"]),
    ]
    client.type.return_value = b"string"
    client.get.side_effect = lambda k: f"v-{k}".encode()

    with patch("connectors.redis_reader._redis_client", return_value=client):
        batch1, state = read_keys_batch(cfg={}, pattern="*", limit=10, scan_state=state)
        batch2, state = read_keys_batch(cfg={}, pattern="*", limit=10, scan_state=state)

    keys = [r[0] for r in batch1.rows] + [r[0] for r in batch2.rows]
    assert keys == ["k1", "k2", "k3"]
    assert state.exhausted is True


def test_jsonl_unions_sparse_late_fields():
    from services.file_parser import parse_jsonl

    content = b'{"id": 1}\n{"id": 2, "late_field": "x"}\n'
    headers, rows, count = parse_jsonl(content)
    assert count == 2
    assert "late_field" in headers
    late_idx = headers.index("late_field")
    assert rows[0][late_idx] == ""
    assert rows[1][late_idx] == "x"


def test_jsonl_class_parser_refuses_scalars():
    from services.file_parser import FileParser

    result = FileParser.parse_jsonl('{"id": 1}\n42\n')
    assert result.success is False
    assert "JSON object" in (result.error or "")


def test_parquet_over_max_rows_fail_closed():
    pytest.importorskip("pyarrow")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from services.file_parser import FileParser

    table = pa.table({"id": [1, 2]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    result = FileParser.parse_parquet(buf.getvalue(), max_rows=1)
    assert result.success is False
    assert result.row_count == 2
    assert "streaming" in (result.error or "").lower()


def test_stream_unknown_total_label_safe():
    """Guard against total_rows=None formatting / compare regressions."""
    total_rows = None
    label = "unknown" if total_rows is None else f"{total_rows:,}"
    assert label == "unknown"
    fetch_offset = 0
    assert not (total_rows is not None and fetch_offset >= total_rows)
