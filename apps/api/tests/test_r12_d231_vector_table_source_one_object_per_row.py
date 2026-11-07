"""D231: a table source lands one vector object per row unless chunking is declared.

QA on prod: Qdrant qaenv_seed (30 points) -> new Weaviate class wrote 159
objects because the writer chunked an undeclared ~62k-char ``long_txt`` cell
into 130 chunks. Owner decision (A): one object per source row by default,
full text kept byte-exact as a property; chunking only when a content column,
chunking option or document layout is declared.

The live leg needs Weaviate on 127.0.0.1:8080 and skips otherwise.
"""
from __future__ import annotations

import uuid

import pytest
import requests

from src.transfer.adapters import _apply_vector_extra
from services.vectorization import vector_chunking_declared

BASE = "http://127.0.0.1:8080"
LONG = "".join(f"line {i:05d} 東京 🚀 — long text\n" for i in range(2400))


class _Endpoint:
    def __init__(self, extra):
        self.extra = extra


@pytest.mark.parametrize(
    ("extra", "skip"),
    [
        ({}, True),
        ({"embedding_model": "hash"}, True),
        ({"content_column": "long_txt"}, False),
        ({"chunk_size": 256}, False),
        ({"vector_layout": "document"}, False),
        ({"content_column": "long_txt", "skip_chunking": True}, True),
    ],
)
def test_chunking_only_when_declared(extra, skip):
    common: dict = {}
    _apply_vector_extra(common, _Endpoint(extra))
    assert common["skip_chunking"] is skip
    assert vector_chunking_declared(extra) is (not skip)


def _up() -> bool:
    try:
        return requests.get(f"{BASE}/v1/meta", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def test_long_cell_row_lands_as_one_object_with_full_text():
    if not _up():
        pytest.skip("Weaviate not reachable on 127.0.0.1:8080")
    from connectors.weaviate_writer import write_mapped_rows

    assert len(LONG) > 62_000
    cls = "D231Row" + uuid.uuid4().hex[:8]
    headers = ["id", "name", "long_txt"]
    rows = [[str(i), f"n{i}", LONG if i == 7 else f"short {i}"] for i in range(1, 31)]
    common: dict = {}
    _apply_vector_extra(common, _Endpoint({"embedding_model": "hash/4"}))
    kwargs = dict(
        host="127.0.0.1", port=8080, database="", username="", password="",
        schema="", connection_string="", ssl=False, table_name=cls,
        headers=headers, data_rows=rows,
        mappings=[{"source": h, "target": h} for h in headers],
        column_types={"id": "STRING", "name": "STRING", "long_txt": "STRING"},
        destination_pk_columns=["id"], **common,
    )
    try:
        for _run in (1,):
            result = write_mapped_rows(**kwargs)
            assert result.ok, result.error
            q = (
                f"{{ Get {{ {cls}(limit: 1000) {{ id_ name long_txt source_id "
                f"chunk_index }} }} }}"
            )
            body = requests.post(f"{BASE}/v1/graphql", json={"query": q}, timeout=30).json()
            if body.get("errors"):
                q = f"{{ Get {{ {cls}(limit: 1000) {{ name long_txt source_id chunk_index }} }} }}"
                body = requests.post(f"{BASE}/v1/graphql", json={"query": q}, timeout=30).json()
            objs = body["data"]["Get"][cls]
            assert len(objs) == 30, f"run {_run}: {len(objs)} objects from 30 rows"
            by_sid = {o["source_id"]: o for o in objs}
            assert by_sid["7"]["long_txt"] == LONG
            assert {o["chunk_index"] for o in objs} == {0}
            for i in range(1, 31):
                assert by_sid[str(i)]["name"] == f"n{i}"
    finally:
        requests.delete(f"{BASE}/v1/schema/{cls}", timeout=10)
