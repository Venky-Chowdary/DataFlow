"""Tests for streaming file peek without full memory load."""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.file_stream import peek_file_source, should_stream_file
from src.transfer.models import EndpointConfig


def _make_csv(rows: int) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "name", "amount"])
    for i in range(rows):
        w.writerow([i, f"user{i}", i * 1.5])
    return buf.getvalue().encode()


def test_peek_file_source_csv_row_count():
    content = _make_csv(12000)
    columns, schema, total, sample = peek_file_source(content, "large.csv")
    assert total == 12000
    assert columns == ["id", "name", "amount"]
    assert len(sample) <= 100
    assert "id" in schema


def test_should_stream_file_csv():
    content = _make_csv(100)
    dest = EndpointConfig(kind="database", format="postgresql", table="t")
    assert should_stream_file(content, "small.csv", dest) is True


def test_should_stream_file_json_not_supported():
    dest = EndpointConfig(kind="database", format="postgresql", table="t")
    assert should_stream_file(b'{"a":1}', "data.json", dest) is False
