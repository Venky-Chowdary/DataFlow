"""Wave 47: Sybase ASE MERGE + STRUCT/MAP bind (Airbyte Destinations V2 class)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_sybase_merge_temp_stage_and_null_safe_on():
    from connectors.generic_sql import _sybase_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    class _Table:
        name = "GlobalSales"
        schema = "dbo"

    n = _sybase_merge_upsert(
        _Conn(),
        _Table(),
        [{"Item_number": 1, "Quantity": 5}],
        ["Item_number"],
        ["Item_number", "Quantity"],
        ["Quantity"],
    )
    assert n == 1
    blob = "\n".join(executed)
    assert "SELECT" in blob and "INTO #DF_MRG_" in blob
    assert "WHERE 1=0" in blob
    assert "MERGE INTO" in blob
    assert "IS NULL" in blob
    assert "WHEN MATCHED THEN UPDATE SET" in blob
    assert any("DROP TABLE" in e for e in executed)


def test_upsert_batch_routes_sybase():
    from connectors import generic_sql as gs

    calls: list[str] = []

    def _sy(*_a, **_k):  # noqa: ANN001
        calls.append("sybase")
        return 9

    class _Col:
        def __eq__(self, other):  # noqa: ANN001
            return ("eq", other)

    class _Table:
        name = "t"
        schema = None
        c = {"id": _Col(), "v": _Col()}

        def insert(self):
            return "INSERT"

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            return MagicMock(rowcount=1)

        def rollback(self):
            pass

    orig = gs._sybase_merge_upsert
    gs._sybase_merge_upsert = _sy  # type: ignore[assignment]
    try:
        for dialect in ("sybase", "ase", "sap_ase", "sybase_ase"):
            n = gs._upsert_batch(
                _Conn(),
                _Table(),
                [{"id": 1, "v": "x"}],
                ["id"],
                ["id", "v"],
                dialect,
            )
            assert n == 9
    finally:
        gs._sybase_merge_upsert = orig  # type: ignore[assignment]

    assert calls == ["sybase"] * 4


def test_coerce_struct_wire_object_only():
    from connectors.sql_bind import coerce_struct_wire, normalize_sql_bind_value

    out = coerce_struct_wire({"a": 1, "b": "x"})
    assert json.loads(out) == {"a": 1, "b": "x"}
    assert coerce_struct_wire('{"k":true}') == '{"k":true}'
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_struct_wire([1, 2])
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_struct_wire(42)
    assert normalize_sql_bind_value({"x": 1}, "STRUCT<a:INT>") == '{"x":1}'


def test_coerce_map_wire_object_and_kv_pairs():
    from connectors.sql_bind import coerce_map_wire, normalize_sql_bind_value

    assert json.loads(coerce_map_wire({"en": "hi"})) == {"en": "hi"}
    pairs = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
    assert json.loads(coerce_map_wire(pairs)) == {"a": 1, "b": 2}
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_map_wire([1, 2, 3])
    assert normalize_sql_bind_value({"k": "v"}, "MAP<STRING,STRING>") == '{"k":"v"}'


def test_sybase_empty_rows():
    from connectors.generic_sql import _sybase_merge_upsert

    class _Conn:
        def execute(self, *_a, **_k):  # noqa: ANN001
            raise AssertionError("should not execute")

    class _Table:
        name = "t"
        schema = None

    assert _sybase_merge_upsert(_Conn(), _Table(), [], ["id"], ["id"], ["id"]) == 0
