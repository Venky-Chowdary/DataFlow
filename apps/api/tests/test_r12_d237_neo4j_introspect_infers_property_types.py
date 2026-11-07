"""D237: Neo4j introspection typed every property TEXT.

The plan's column_types said TEXT for amount/flag/d while type_conversions
cast them to DECIMAL/BOOLEAN/DATE: the label schema was hard-coded TEXT
instead of being inferred from the sampled node properties.
"""

from __future__ import annotations

import connectors.neo4j as neo4j
from connectors.base import ReadBatch
from services.schema_introspect import introspect_schema

HEADERS = ["_neo4j_element_id", "_neo4j_labels", "id", "amount", "flag", "d", "name", "note"]
ROWS = [
    [f"4:x:{i}", '["QaenvSeed"]', str(i), f"{i}.25", "true" if i % 2 else "false",
     f"2024-03-{10 + i:02d}", f"name {i}", "" if i % 4 == 0 else f"n{i}"]
    for i in range(1, 16)
]


def test_neo4j_label_columns_are_inferred_not_all_text(monkeypatch):
    monkeypatch.setattr(neo4j, "list_labels", lambda **_: ["QaenvSeed"])
    monkeypatch.setattr(
        neo4j,
        "read_object",
        lambda **_: ReadBatch(headers=HEADERS, rows=ROWS, offset=0, total_rows=None),
    )
    out = introspect_schema(db_type="neo4j", host="127.0.0.1", table="QaenvSeed")
    assert out["ok"], out
    types = {c["name"]: str(c["inferred_type"]).upper() for c in out["columns"]}
    assert types["amount"].startswith(("DECIMAL", "NUMERIC", "FLOAT", "DOUBLE")), types
    assert types["flag"] == "BOOLEAN", types
    assert types["d"] == "DATE", types
    assert types["id"] in {"INTEGER", "BIGINT"}, types
    assert types["name"].startswith(("TEXT", "VARCHAR")), types
    assert all(c["nullable"] for c in out["columns"])
