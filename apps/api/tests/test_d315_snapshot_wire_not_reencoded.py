"""D315 — a CDC snapshot page is already wire and must not be encoded again.

A missing nested field arrives as ``__DF_SQL_NULL__``. Encoding that cell a
second time stored ``__DF_ESC____DF_SQL_NULL__``, which an INTEGER column
refused. The snapshot keeps the reader's wire cell.
"""

from __future__ import annotations

from connectors.mongodb_change_stream import _wire_row_to_record
from services.value_serializer import SQL_NULL_SENTINEL, WireCell


def test_missing_nested_field_stays_a_null_sentinel():
    record = _wire_row_to_record(
        ["_id", "payload_k"],
        ["abc", SQL_NULL_SENTINEL],
        ["_id", "payload_k"],
    )
    assert isinstance(record["payload_k"], WireCell)
    assert str(record["payload_k"]) == SQL_NULL_SENTINEL
    assert "__DF_ESC__" not in str(record["payload_k"])
