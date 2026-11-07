"""D147 — MongoDB documents with BSON dates and a nested sub-document.

QA: Mongo -> MySQL / Timescale was refused for ordinary documents: "ts
TIMESTAMP -> TIMESTAMP" reported as a collapse, and EMPTY_VALUE_NOT_NULLABLE
on the flattened ``payload_nested`` columns of a NEW table because one
document carries ``payload: {}``. Expected: dates land as DATETIME /
TIMESTAMPTZ with their milliseconds, flattened columns are created NULLable,
and the ``{}`` document lands with NULLs. Regression guard (already true on
3e258e90f): live MongoDB 7 -> new MySQL 8 / PostgreSQL tables, run twice.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime, timezone

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def _docs():
    docs = []
    for i in range(1, 30):
        doc = {
            "id": i, "name": "東京" if i == 3 else ("" if i == 4 else f"n{i}"),
            "ts": datetime(2024, 1, 1, 10, 0, i, 123000),
            "updated_at": None if i == 5 else datetime(2024, 3, 1, 0, 0, i, 5000),
            "payload": {"k": i, "nested": {"a": [i, i + 1]}, "s": f"s{i}"},
        }
        if i == 7:
            doc["payload"] = {}
        docs.append(doc)
    return docs


@pytest.mark.parametrize("dest", ["mysql", "postgresql"])
def test_mongo_documents_land_in_a_new_relational_table_twice(isolated, dest):  # noqa: F811
    pymongo = pytest.importorskip("pymongo")
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError:
        pytest.skip("MongoDB not reachable")
    from src.transfer.models import EndpointConfig

    coll = f"d147_{uuid.uuid4().hex[:8]}"
    out = f"{coll}_{dest[:3]}"
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    client["dataflow_test"][coll].insert_many(_docs())
    src = EndpointConfig(kind="database", format="mongodb", host="127.0.0.1", port=27017,
                         database="dataflow_test", collection=coll, table=coll)
    try:
        for _ in range(2):
            result = h.execute(isolated, src, h.endpoint(dest, out), sync_mode="full_refresh_overwrite")
            assert result.success, result.error
        nullable = dict(h.run_sql(
            dest, "SELECT column_name, is_nullable FROM information_schema.columns "  # nosec B608
            f"WHERE table_name = '{out}'",
        ))
        assert nullable["payload_nested"] == "YES" and nullable["payload_nested_a"] == "YES"
        rows = {r[0]: r[1:] for r in h.run_sql(
            dest, f"SELECT id, name, ts, updated_at, payload_k, payload_nested_a "  # nosec B608
            f"FROM {h.quote(dest, out)} ORDER BY id",
        )}
        assert len(rows) == 29
        naive = lambda v: v.astimezone(timezone.utc).replace(tzinfo=None) if v and v.tzinfo else v  # noqa: E731
        assert naive(rows[1][1]) == datetime(2024, 1, 1, 10, 0, 1, 123000)
        assert rows[3][0] == "東京" and rows[4][0] == ""
        assert rows[5][2] is None
        assert rows[7][3] is None and rows[7][4] is None  # the {} document
    finally:
        client["dataflow_test"][coll].drop()
        client.close()
        h.drop(dest, out)
