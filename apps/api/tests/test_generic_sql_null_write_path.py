"""Generic SQL bind treats reader-null as SQL NULL, not the extract token.

``_to_sa_value`` only passed Python None. After extract emits
SQL_NULL_SENTINEL, STRING/TEXT wrote the wire spelling and ExactJSON
quoted it as a JSON string. Typed carriers already collapsed the token;
string must match. 0 / false / empty stay present. Missing stays Missing.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.generic_sql import _ExactJSON, _to_sa_value  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_to_sa_value_reader_null_is_sql_null_on_every_logical():
    for logical in ("string", "text", "integer", "decimal", "boolean", "date", "json"):
        for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__"):
            assert _to_sa_value(wire, logical) is None, (logical, wire)


def test_to_sa_value_keeps_zero_false_empty_and_missing():
    assert _to_sa_value(0, "integer") == 0
    assert _to_sa_value(False, "boolean") is False
    assert _to_sa_value("", "string") == ""
    assert _to_sa_value("kept", "string") == "kept"
    assert _to_sa_value(Missing, "string") is Missing
    assert _to_sa_value(DF_MISSING_SENTINEL, "integer") == DF_MISSING_SENTINEL


def test_exact_json_bind_reader_null_is_sql_null_not_token_text():
    proc = _ExactJSON().bind_processor(None)
    assert proc is not None
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__"):
        assert proc(wire) is None, wire
    assert SQL_NULL_SENTINEL not in str(proc("{\"k\":1}"))
