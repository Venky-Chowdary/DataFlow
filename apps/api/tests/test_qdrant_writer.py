"""Tests for the Qdrant vector destination writer.

Tests skip automatically when a local Qdrant instance is not reachable, so CI
without the vector store stack still passes.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.qdrant_writer import test_qdrant as probe_qdrant
from connectors.qdrant_writer import write_mapped_rows


def _qdrant_available() -> bool:
    try:
        with socket.create_connection(("localhost", 6333), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _qdrant_available(), reason="Qdrant not reachable on localhost:6333")
def test_qdrant_probe_returns_true_for_reachable():
    ok, msg = probe_qdrant(host="localhost", port=6333, api_key="", ssl=False)
    assert ok, msg


def test_qdrant_probe_returns_false_for_unreachable():
    ok, msg = probe_qdrant(host="localhost", port=0, api_key="", ssl=False)
    assert not ok


@pytest.mark.skipif(not _qdrant_available(), reason="Qdrant not reachable on localhost:6333")
def test_qdrant_write_mapped_rows_upserts_points():
    headers = ["id", "content"]
    rows = [["1", "hello world"], ["2", "test vector"]]
    result = write_mapped_rows(
        host="localhost",
        port=6333,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name=f"test_qdrant_{pytest.importorskip('uuid').uuid4().hex[:8]}",
        headers=headers,
        data_rows=rows,
        mappings=[{"source": "id", "target": "id"}, {"source": "content", "target": "content"}],
        column_types={"id": "INTEGER", "content": "STRING"},
        content_column="content",
    )
    assert result.ok, result.error
    assert result.rows_written == 2


def test_qdrant_write_mapped_rows_gracefully_fails_when_unreachable():
    headers = ["id", "content"]
    rows = [["1", "hello world"]]
    result = write_mapped_rows(
        host="localhost",
        port=0,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="test_unreachable",
        headers=headers,
        data_rows=rows,
        mappings=[{"source": "id", "target": "id"}, {"source": "content", "target": "content"}],
        column_types={"id": "INTEGER", "content": "STRING"},
        content_column="content",
    )
    assert not result.ok
    assert "refused" in result.error.lower() or "connection" in result.error.lower()
