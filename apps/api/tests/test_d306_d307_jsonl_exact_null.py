"""D306 and D307 — JSONL keeps an exact decimal and keeps SQL NULL apart from JSON null.

A DECIMAL(18,6) value must not become a float (D306).

D307 asked for an explicit ``null`` for a SQL NULL in a JSON column. That
cannot be done without losing data: a JSON/JSONB column also holds the JSON
value ``null``, which PostgreSQL keeps distinct from SQL NULL ("JSON null ...
SQL NULL is a different concept", https://www.postgresql.org/docs/current/datatype-json.html).
If both are written as ``null`` the file cannot say which one the source held,
and Gate-8 correctly fails the export (test_qa_r3_defects). The object-store
JSON/JSONL writers therefore write a document column's SQL NULL as an absent
key and its JSON ``null`` as ``null`` (D144); every other NULL column keeps its
key with ``null``.
"""

from __future__ import annotations

import json
from decimal import Decimal

from connectors.object_store_materialize import ObjectStoreEncoder
from services.value_serializer import json_loads_exact


def _lines(rows):
    encoder = ObjectStoreEncoder(
        key="out.jsonl",
        target_cols=["id", "amount", "payload", "note"],
        dest_types={"id": "int", "amount": "decimal", "payload": "json", "note": "text"},
    )
    encoder.append_rows(rows)
    export = encoder.finish()
    return export._spool.read().decode().splitlines()


def test_jsonl_writes_the_exact_decimal():
    (line,) = _lines([(9, Decimal("99999999999.999999"), "{}", "x")])
    row = json_loads_exact(line)
    assert row["amount"] == Decimal("99999999999.999999")
    assert "100000000000" not in line
    (tiny,) = _lines([(10, Decimal("-0.000001"), "{}", "x")])
    assert json.loads(tiny, parse_float=Decimal)["amount"] == Decimal("-0.000001")


def test_jsonl_keeps_sql_null_and_json_null_apart_in_a_json_column():
    sql_null, json_null, empty = (
        json.loads(line)
        for line in _lines([(1, Decimal("1"), None, None), (2, Decimal("1"), "null", None), (3, Decimal("1"), "{}", "")])
    )
    # SQL NULL in the JSON column: the key is absent. JSON null: present, null.
    assert "payload" not in sql_null
    assert "payload" in json_null and json_null["payload"] is None
    assert empty["payload"] == {}
    # A NULL in any other column keeps its key, as null; '' stays ''.
    assert sql_null["note"] is None and json_null["note"] is None
    assert empty["note"] == ""
