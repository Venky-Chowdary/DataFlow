"""DuckDB JSON bind uses json_document_wire, not stdlib json.loads.

json.loads + dumps collapsed 1.234567890123456789 and turned the JSON
string \"1\" into the number 1. Valid JSON text keeps digits and polarity.
None is SQL NULL. Empty string still refuses.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.generic_sql import _DuckDBJSON  # noqa: E402

LONG = "1.234567890123456789"


def _proc():
    return _DuckDBJSON(none_as_null=True).bind_processor(None)


def test_duckdb_json_bind_keeps_long_fraction_text():
    proc = _proc()
    raw = f'{{"amt": {LONG}}}'
    assert proc(raw) == raw
    collapsed = json.dumps(json.loads(raw), ensure_ascii=False, separators=(",", ":"))
    assert proc(raw) != collapsed
    assert LONG in proc(raw)
    assert LONG not in collapsed


def test_duckdb_json_bind_keeps_string_one_polarity():
    proc = _proc()
    assert proc('"1"') == '"1"'
    assert proc("true") == "true"
    assert proc('"true"') == '"true"'
    assert json.loads('"1"') == "1"
    assert proc('"1"') != json.loads('"1"')


def test_duckdb_json_bind_none_and_tree():
    proc = _proc()
    assert proc(None) is None
    assert proc({"a": 1, "s": "1"}) == '{"a":1,"s":"1"}'
    assert proc([1, "1", True]) == '[1,"1",true]'


def test_duckdb_json_bind_empty_refuses():
    with pytest.raises(ValueError, match="empty string"):
        _proc()("")
