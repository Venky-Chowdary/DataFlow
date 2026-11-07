"""D242: sampling a destination-only vector engine says so, not "no SQLAlchemy".

Weaviate/Pinecone/Milvus have no DataFlow reader (capability read=False).
sample_connector_object sent them down the SQL query path, which failed
"SQLAlchemy dialect/driver for 'weaviate' is not available".
"""

from __future__ import annotations

import uuid

import pytest


@pytest.mark.parametrize("kind", ["weaviate", "pinecone", "milvus"])
def test_dest_only_sample_names_the_real_limit(kind: str) -> None:
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.query_tools import sample_connector_object

    conn = create_connector({"name": f"d242-{kind}-{uuid.uuid4().hex[:6]}", "type": kind, "host": "vec.d242.test"})
    try:
        res = sample_connector_object(connector_id=conn.id, table="QaenvSeed", limit=5)
    finally:
        delete_connector(conn.id)
    assert not res.success
    assert "SQLAlchemy" not in res.error
    assert "destination-only" in res.error
    assert "count" in res.error
