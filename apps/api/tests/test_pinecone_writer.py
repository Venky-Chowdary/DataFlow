"""CI-stable proofs for Pinecone destination writer (no live index required)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.pinecone_writer import (
    build_pinecone_vectors,
    scan_source_ids,
    test_pinecone as probe_pinecone,
    write_mapped_rows,
)


def test_build_pinecone_vectors_maps_rows():
    rows = [
        {
            "id": "vec-1",
            "content": "hello",
            "source_id": "1",
            "chunk_index": 0,
            "embedding": [0.1, 0.2, 0.3],
            "metadata": {"page": "1", "tags": ["a", "b"]},
        }
    ]
    vectors, _rejected = build_pinecone_vectors(rows, dimension=3)
    assert len(vectors) == 1
    assert vectors[0]["id"] == "vec-1"
    assert vectors[0]["values"] == [0.1, 0.2, 0.3]
    assert vectors[0]["metadata"]["content"] == "hello"
    assert vectors[0]["metadata"]["page"] == "1"
    assert vectors[0]["metadata"]["tags"] == ["a", "b"]


def test_build_pinecone_vectors_rejects_missing_embedding():
    rows = [
        {"id": "a", "content": "x", "embedding": [0.1, 0.2, 0.3]},
        {"id": "b", "content": "y", "embedding": None},
        {"id": "c", "content": "z", "embedding": [0.1, 0.2]},  # dim mismatch
    ]
    vectors, rejected = build_pinecone_vectors(rows, dimension=3)
    assert len(vectors) == 1
    assert len(rejected) == 2
    assert any("missing" in (r.get("reason") or "").lower() or "refuse" in (r.get("reason") or "").lower() for r in rejected)


def test_build_pinecone_vectors_sql_null_id_falls_back_not_sentinel():
    """SQL NULL / sentinel must not become a literal Pinecone id."""
    from services.value_serializer import SQL_NULL_SENTINEL

    rows = [
        {
            "id": SQL_NULL_SENTINEL,
            "content": "hello",
            "source_id": "src-1",
            "chunk_index": 0,
            "embedding": [0.1, 0.2, 0.3],
            "metadata": {},
        }
    ]
    vectors, rejected = build_pinecone_vectors(rows, dimension=3)
    assert not rejected
    assert len(vectors) == 1
    assert vectors[0]["id"] != SQL_NULL_SENTINEL
    assert len(vectors[0]["id"]) == 64  # sha256 hex fallback


def test_build_pinecone_vectors_nan_id_falls_back_not_empty():
    """NaN serializes to '' via cell_to_string — must not upsert empty vector id."""
    rows = [
        {
            "id": float("nan"),
            "content": "hello",
            "source_id": "src-nan",
            "chunk_index": 0,
            "embedding": [0.1, 0.2, 0.3],
            "metadata": {},
        }
    ]
    vectors, rejected = build_pinecone_vectors(rows, dimension=3)
    assert not rejected
    assert len(vectors) == 1
    assert vectors[0]["id"]  # non-empty
    assert vectors[0]["id"] != "nan"


def test_pinecone_probe_requires_host_and_key():
    ok, msg = probe_pinecone(host="", connection_string="", api_key="")
    assert not ok
    assert "host" in msg.lower()

    ok2, msg2 = probe_pinecone(host="https://example.invalid", api_key="")
    assert not ok2
    assert "key" in msg2.lower()


def test_pinecone_probe_unreachable_fail_closed():
    ok, msg = probe_pinecone(
        host="https://127.0.0.1:1",
        api_key="test-key",
    )
    assert not ok
    assert msg


def test_pinecone_write_missing_host_fail_closed():
    result = write_mapped_rows(
        host="",
        port=443,
        database="",
        username="",
        password="key",
        schema="",
        connection_string="",
        ssl=True,
        table_name="ns",
        headers=["id", "content", "vec"],
        data_rows=[["1", "hello", "[0.1,0.2]"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "content", "target": "content"},
            {"source": "vec", "target": "vec"},
        ],
        column_types={"id": "STRING", "content": "STRING", "vec": "STRING"},
        content_column="content",
        embedding_column="vec",
    )
    assert not result.ok
    assert "host" in (result.error or "").lower()


def test_pinecone_write_unreachable_fail_closed():
    result = write_mapped_rows(
        host="https://127.0.0.1:1",
        port=443,
        database="",
        username="",
        password="test-key",
        schema="",
        connection_string="",
        ssl=True,
        table_name="ns",
        headers=["id", "content", "vec"],
        data_rows=[["1", "hello", "[0.1,0.2,0.3]"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "content", "target": "content"},
            {"source": "vec", "target": "vec"},
        ],
        column_types={"id": "STRING", "content": "STRING", "vec": "STRING"},
        content_column="content",
        embedding_column="vec",
    )
    assert not result.ok
    assert result.error


class _FakeResp:
    def __init__(self, status: int, payload: dict | None = None):
        self.status_code = status
        self.content = b"{}" if payload is not None else b""
        self._payload = payload or {}

    def json(self):
        return self._payload


class _PineconeSession:
    """Dest-engine list+fetch. vectorCount is physical — tests must not treat it as identity."""

    def __init__(
        self,
        *,
        stats_status: int = 200,
        list_status: int = 200,
        fetch_status: int = 200,
        vector_count: int = 0,
        ids: list[str] | None = None,
        metadata: dict[str, dict] | None = None,
        namespace: str = "docs",
    ):
        self.stats_status = stats_status
        self.list_status = list_status
        self.fetch_status = fetch_status
        self.vector_count = vector_count
        self.ids = list(ids or [])
        self.metadata = dict(metadata or {})
        self.namespace = namespace
        self.listed_urls: list[str] = []

    def get(self, url, headers=None, params=None, timeout=None):
        self.listed_urls.append(str(url))
        if url.endswith("/describe_index_stats"):
            if self.stats_status != 200:
                return _FakeResp(self.stats_status, {})
            return _FakeResp(
                200,
                {
                    "namespaces": {self.namespace: {"vectorCount": self.vector_count}},
                    "totalVectorCount": self.vector_count,
                },
            )
        if "/vectors/list" in str(url):
            if self.list_status != 200:
                return _FakeResp(self.list_status, {})
            return _FakeResp(200, {"vectors": [{"id": i} for i in self.ids], "pagination": {}})
        return _FakeResp(500, {})

    def post(self, url, data=None, headers=None, timeout=None):
        if url.endswith("/vectors/fetch"):
            if self.fetch_status != 200:
                return _FakeResp(self.fetch_status, {})
            import json as _json

            body = _json.loads(data or "{}")
            wanted = list(body.get("ids") or [])
            vectors = {}
            for vid in wanted:
                meta = self.metadata.get(vid, {})
                vectors[vid] = {"id": vid, "metadata": meta}
            return _FakeResp(200, {"vectors": vectors})
        return _FakeResp(500, {})


def test_pinecone_scan_source_ids_empty_namespace_is_zero(monkeypatch):
    monkeypatch.setattr(
        "connectors.pinecone_writer._requests_session",
        lambda: _PineconeSession(vector_count=0, ids=[]),
    )
    state, values = scan_source_ids(
        {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
        table_name="docs",
    )
    assert state == "complete"
    assert values == []
    from services.dest_precount import identity_count_from_source_id_scan

    assert identity_count_from_source_id_scan(state, values) == 0


def test_pinecone_scan_source_ids_distinct_not_vector_count(monkeypatch):
    ids = ["v1", "v2", "v3", "v4", "v5"]
    meta = {
        "v1": {"source_id": "doc-1"},
        "v2": {"source_id": "doc-1"},
        "v3": {"source_id": "doc-1"},
        "v4": {"source_id": "doc-2"},
        "v5": {"source_id": "doc-2"},
    }
    monkeypatch.setattr(
        "connectors.pinecone_writer._requests_session",
        lambda: _PineconeSession(vector_count=5, ids=ids, metadata=meta),
    )
    state, values = scan_source_ids(
        {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
        table_name="docs",
    )
    assert state == "complete"
    assert values == ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"]
    from services.dest_precount import identity_count_from_source_id_scan

    assert identity_count_from_source_id_scan(state, values) == 2


def test_pinecone_scan_source_ids_list_unsupported_is_unmeasured_not_vectorcount(monkeypatch):
    """Pod indexes have no /vectors/list — vectorCount must not close identity."""
    session = _PineconeSession(vector_count=5, ids=["v1"], list_status=404)
    monkeypatch.setattr("connectors.pinecone_writer._requests_session", lambda: session)
    state, values = scan_source_ids(
        {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
        table_name="docs",
    )
    assert state == "unmeasured"
    assert values == []
    from services.dest_precount import identity_count_from_source_id_scan, destination_row_count

    assert identity_count_from_source_id_scan(state, values) is None
    n = destination_row_count(
        "pinecone",
        {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
        schema="",
        table_name="docs",
    )
    assert n is None


def test_pinecone_scan_source_ids_no_source_id_metadata(monkeypatch):
    monkeypatch.setattr(
        "connectors.pinecone_writer._requests_session",
        lambda: _PineconeSession(
            vector_count=2,
            ids=["v1", "v2"],
            metadata={"v1": {"content": "a"}, "v2": {"content": "b"}},
        ),
    )
    state, values = scan_source_ids(
        {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
        table_name="docs",
    )
    assert state == "no_field"
    assert values == []


def test_pinecone_scan_source_ids_truncated_past_bound(monkeypatch):
    monkeypatch.setattr(
        "connectors.pinecone_writer._requests_session",
        lambda: _PineconeSession(vector_count=20_001, ids=[]),
    )
    state, values = scan_source_ids(
        {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
        table_name="docs",
        max_entities=20_000,
    )
    assert state == "truncated"
    assert values == []