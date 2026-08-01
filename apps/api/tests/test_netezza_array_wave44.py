"""Wave 44: Netezza MERGE + Postgres ARRAY bind (no JSON invent)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_netezza_merge_temp_stage_and_null_safe_on():
    from connectors.generic_sql import _netezza_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    class _Table:
        name = "DIM_CUSTOMER"
        schema = "DW"

    n = _netezza_merge_upsert(
        _Conn(),
        _Table(),
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        ["id"],
        ["id", "name"],
        ["name"],
    )
    assert n == 2
    blob = "\n".join(executed)
    assert "CREATE TEMP TABLE" in blob
    assert "LIMIT 0" in blob
    assert "MERGE INTO" in blob
    assert "IS NULL" in blob
    assert "WHEN MATCHED THEN UPDATE SET" in blob
    assert any("DROP TABLE" in e for e in executed)


def test_upsert_batch_routes_netezza():
    from connectors import generic_sql as gs

    calls: list[str] = []

    def _nz(*_a, **_k):  # noqa: ANN001
        calls.append("netezza")
        return 5

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

    orig = gs._netezza_merge_upsert
    gs._netezza_merge_upsert = _nz  # type: ignore[assignment]
    try:
        for dialect in ("netezza", "nzpsql", "nzpy"):
            n = gs._upsert_batch(
                _Conn(),
                _Table(),
                [{"id": 1, "v": "x"}],
                ["id"],
                ["id", "v"],
                dialect,
            )
            assert n == 5
    finally:
        gs._netezza_merge_upsert = orig  # type: ignore[assignment]

    assert calls == ["netezza", "netezza", "netezza"]


def test_coerce_array_wire_postgres_list_and_json():
    from connectors.sql_bind import coerce_array_wire, normalize_sql_bind_value

    assert coerce_array_wire([1, 2], engine="postgresql") == [1, 2]
    assert coerce_array_wire("[1,2,3]", engine="postgres") == [1, 2, 3]
    assert coerce_array_wire("{a,b}", engine="postgresql") == "{a,b}"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_array_wire({"a": 1}, engine="postgresql")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_array_wire("not-an-array", engine="postgresql")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_array_wire(42, engine="postgresql")

    # Non-PG engines that map ARRAY→JSON keep text JSON bind.
    out = coerce_array_wire([1, 2], engine="mysql")
    assert out == "[1,2]"

    assert normalize_sql_bind_value("[\"x\"]", "ARRAY", engine="postgresql") == ["x"]
    assert normalize_sql_bind_value(["x"], "text[]", engine="postgresql") == ["x"]


def test_netezza_empty_rows():
    from connectors.generic_sql import _netezza_merge_upsert

    class _Conn:
        def execute(self, *_a, **_k):  # noqa: ANN001
            raise AssertionError("should not execute")

    class _Table:
        name = "t"
        schema = None

    assert _netezza_merge_upsert(_Conn(), _Table(), [], ["id"], ["id"], ["id"]) == 0
