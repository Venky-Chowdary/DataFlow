"""D306 and D307 — JSONL keeps an exact decimal and writes JSON null.

A DECIMAL(18,6) value must not become a float. A SQL NULL in a JSON column
is the JSON null, the same as every other NULL column, not a missing key.
"""

from __future__ import annotations

import json
from decimal import Decimal

from connectors.object_store_materialize import ObjectStoreEncoder
from services.value_serializer import json_loads_exact


def test_jsonl_writes_the_exact_decimal_and_an_explicit_json_null():
    encoder = ObjectStoreEncoder(
        key="out.jsonl",
        target_cols=["id", "amount", "payload"],
        dest_types={"id": "int", "amount": "decimal", "payload": "json"},
    )
    encoder.append_rows([
        (9, Decimal("99999999999.999999"), None),
    ])
    export = encoder.finish()
    line = export._spool.read().decode()
    row = json_loads_exact(line)
    assert row["amount"] == Decimal("99999999999.999999")
    assert "payload" in row
    assert row["payload"] is None
    assert "100000000000" not in line
    assert json.loads(line)["payload"] is None
