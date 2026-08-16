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

from connectors.qdrant_writer import scan_source_ids
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
        embedding_model="hash/32",
    )
    assert not result.ok
    assert "refused" in result.error.lower() or "connection" in result.error.lower()


class _FakeResp:
    def __init__(self, status: int, payload: dict):
        self.status_code = status
        self.content = b"{}"
        self.text = ""
        self._payload = payload

    def json(self):
        return self._payload


class _QdrantSession:
    def __init__(self, *, status: int, points_count: int | None, pages: list[dict]):
        self.status = status
        self.points_count = points_count
        self.pages = list(pages)
        self._i = 0

    def get(self, url, headers=None, timeout=None):
        if self.status == 404:
            return _FakeResp(404, {"status": {"error": "not found"}})
        if self.status != 200:
            return _FakeResp(self.status, {})
        body: dict = {"result": {}}
        if self.points_count is not None:
            body["result"]["points_count"] = self.points_count
        return _FakeResp(200, body)

    def post(self, url, data=None, headers=None, timeout=None):
        if self._i >= len(self.pages):
            return _FakeResp(200, {"result": {"points": [], "next_page_offset": None}})
        page = self.pages[self._i]
        self._i += 1
        return _FakeResp(200, {"result": page})


def test_qdrant_scan_source_ids_missing_collection_is_zero(monkeypatch):
    monkeypatch.setattr(
        "connectors.qdrant_writer._requests_session",
        lambda: _QdrantSession(status=404, points_count=None, pages=[]),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 6333}, table_name="docs"
    )
    assert state == "missing"
    assert values == []


def test_qdrant_scan_source_ids_distinct_not_points_count(monkeypatch):
    pages = [
        {
            "points": [
                {"id": 1, "payload": {"source_id": "doc-1"}},
                {"id": 2, "payload": {"source_id": "doc-1"}},
                {"id": 3, "payload": {"source_id": "doc-1"}},
            ],
            "next_page_offset": 3,
        },
        {
            "points": [
                {"id": 4, "payload": {"source_id": "doc-2"}},
                {"id": 5, "payload": {"source_id": "doc-2"}},
            ],
            "next_page_offset": None,
        },
    ]
    monkeypatch.setattr(
        "connectors.qdrant_writer._requests_session",
        lambda: _QdrantSession(status=200, points_count=5, pages=pages),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 6333}, table_name="docs"
    )
    assert state == "complete"
    assert values == ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"]
    from services.dest_precount import identity_count_from_source_id_scan

    assert identity_count_from_source_id_scan(state, values) == 2


def test_qdrant_scan_source_ids_truncated_past_bound(monkeypatch):
    monkeypatch.setattr(
        "connectors.qdrant_writer._requests_session",
        lambda: _QdrantSession(status=200, points_count=20_001, pages=[]),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 6333}, table_name="docs", max_entities=20_000
    )
    assert state == "truncated"
    assert values == []


@pytest.mark.skipif(not _qdrant_available(), reason="Qdrant not reachable on localhost:6333")
def test_qdrant_live_identity_count_distinct_source_id_not_points_count():
    from services.dest_precount import destination_row_count

    collection = f"p9_qdrant_identity_{pytest.importorskip('uuid').uuid4().hex[:8]}"
    result = write_mapped_rows(
        host="localhost",
        port=6333,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name=collection,
        headers=["id", "content"],
        data_rows=[["doc-1", "hello world"], ["doc-2", "test vector"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "content", "target": "content"},
        ],
        column_types={"id": "STRING", "content": "STRING"},
        content_column="content",
        skip_chunking=True,
    )
    assert result.ok, result.error
    cfg = {"host": "localhost", "port": 6333}
    assert destination_row_count("qdrant", cfg, schema="", table_name=collection) == 2
    missing = destination_row_count(
        "qdrant", cfg, schema="", table_name=f"{collection}_absent"
    )
    assert missing == 0
