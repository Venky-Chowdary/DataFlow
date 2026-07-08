"""Unit tests for streaming DB→DB transfer routing."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.models import EndpointConfig
from src.transfer.stream import supports_streaming


def test_supports_streaming_pg_mysql_pairs():
    src = EndpointConfig(kind="database", format="postgresql", table="orders")
    dst = EndpointConfig(kind="database", format="mysql", table="orders")
    assert supports_streaming(src, dst) is True

    assert supports_streaming(
        EndpointConfig(kind="database", format="mysql", table="t"),
        EndpointConfig(kind="database", format="postgresql", table="t"),
    ) is True

    assert supports_streaming(
        EndpointConfig(kind="database", format="postgresql", table="t"),
        EndpointConfig(kind="database", format="postgresql", table="t"),
    ) is True


def test_supports_streaming_mongo_pairs():
    pg = EndpointConfig(kind="database", format="postgresql", table="t")
    mongo = EndpointConfig(kind="database", format="mongodb", collection="c")
    assert supports_streaming(mongo, pg) is True
    assert supports_streaming(pg, mongo) is True

    file_src = EndpointConfig(kind="file", format="csv")
    assert supports_streaming(file_src, pg) is False


def test_supports_file_streaming_csv():
    from src.transfer.file_stream import supports_file_streaming

    dest = EndpointConfig(kind="database", format="postgresql", table="t")
    assert supports_file_streaming("file", "data.csv", dest) is True
    assert supports_file_streaming("file", "data.json", dest) is False
    assert supports_file_streaming("database", "data.csv", dest) is False
