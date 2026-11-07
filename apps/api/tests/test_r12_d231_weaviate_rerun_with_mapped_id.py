"""D231: a rerun into the Weaviate class DataFlow created must not refuse ``id``.

A mapped ``id`` is the object identity (UUID + ``source_id``), never a class
property, so run 1 created the class without it and run 2 failed "physical DDL
missing for mapped column(s) id". The owner's acceptance needs an idempotent
rerun (still 30 objects). Live Weaviate on 127.0.0.1:8080; skipped when absent.
"""
from __future__ import annotations

import uuid

import pytest
import requests

BASE = "http://127.0.0.1:8080"


def _up() -> bool:
    try:
        return requests.get(f"{BASE}/v1/meta", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def test_rerun_into_created_class_is_idempotent():
    if not _up():
        pytest.skip("Weaviate not reachable on 127.0.0.1:8080")
    from connectors.weaviate_writer import write_mapped_rows

    cls = "D231Rerun" + uuid.uuid4().hex[:8]
    kwargs = dict(
        host="127.0.0.1", port=8080, database="", username="", password="",
        schema="", connection_string="", ssl=False, table_name=cls,
        headers=["id", "name"],
        data_rows=[[str(i), f"n{i}"] for i in range(1, 31)],
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        column_types={"id": "STRING", "name": "STRING"},
        embedding_model="hash/4", destination_pk_columns=["id"], skip_chunking=True,
    )
    try:
        for run in (1, 2):
            result = write_mapped_rows(**kwargs)
            assert result.ok, f"run {run}: {result.error}"
            q = f"{{ Aggregate {{ {cls} {{ meta {{ count }} }} }} }}"
            body = requests.post(f"{BASE}/v1/graphql", json={"query": q}, timeout=10).json()
            assert body["data"]["Aggregate"][cls][0]["meta"]["count"] == 30
    finally:
        requests.delete(f"{BASE}/v1/schema/{cls}", timeout=10)
