"""D141 (c) — an aware timestamp into a vector store DataFlow creates.

Qdrant / Weaviate / Milvus / Pinecone carry a row's non-vector columns as
payload metadata; DataFlow's create-new stamp there is text, and the writer's
one serializer (``vector_prepare_metadata``) renders an aware ``datetime`` as
RFC 3339 with its offset and microseconds. MySQL/Timescale ``TIMESTAMPTZ ->
TEXT`` into such a store was graded an open-text collapse of DataFlow's own
carrier, refusing every timestamp column. A typed engine's TEXT column (where
the offset contract really is dropped) and a zoneless source stay refused.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from connectors.writer_common import vector_prepare_metadata
from services.decision_kernel import create_new_mapping_target_type
from services.type_coercion import is_lossy_coercion

_VECTOR = ["qdrant", "weaviate", "milvus", "pinecone"]


@pytest.mark.parametrize("dest", _VECTOR)
@pytest.mark.parametrize("source_db", ["mysql", "timescaledb", "oracle", "sqlserver"])
def test_aware_instant_onto_the_vector_payload_text_is_not_lossy(dest, source_db):
    carrier = create_new_mapping_target_type("TIMESTAMPTZ", dest, source_db=source_db)
    assert is_lossy_coercion("TIMESTAMPTZ", carrier, dest_db=dest, source_db=source_db) is False


def test_the_payload_wire_keeps_offset_and_microseconds():
    aware = datetime(2024, 3, 10, 2, 30, 0, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    assert vector_prepare_metadata({"ts": aware}) == {"ts": "2024-03-10T02:30:00.123456+05:30"}


def test_typed_text_column_still_drops_the_offset_contract():
    assert is_lossy_coercion("TIMESTAMPTZ", "TEXT", dest_db="postgresql") is True
    assert is_lossy_coercion("TIMESTAMPTZ", "TEXT", dest_db="mysql") is True
