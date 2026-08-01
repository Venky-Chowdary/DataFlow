"""Wave 43: SAP HANA MERGE + Vertica MERGE (NULL-safe ON, local temp stage)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_hana_merge_local_temp_and_null_safe_on():
    from connectors.generic_sql import _hana_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    class _Table:
        name = "ORDERS"
        schema = "SALES"

    n = _hana_merge_upsert(
        _Conn(),
        _Table(),
        [{"id": 1, "amt": 10}, {"id": 2, "amt": 20}],
        ["id"],
        ["id", "amt"],
        ["amt"],
    )
    assert n == 2
    blob = "\n".join(executed)
    assert "CREATE LOCAL TEMPORARY COLUMN TABLE #DF_MRG_" in blob
    assert "MERGE INTO" in blob
    assert 'T."ID" = S."ID"' in blob or "IS NULL" in blob
    assert "IS NULL" in blob  # NULL-safe ON
    assert "WHEN MATCHED THEN UPDATE SET" in blob
    assert "WHEN NOT MATCHED THEN INSERT" in blob
    assert any("DROP TABLE" in e for e in executed)


def test_vertica_merge_local_temp_and_null_safe_on():
    from connectors.generic_sql import _vertica_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    class _Table:
        name = "facts"
        schema = "analytics"

    n = _vertica_merge_upsert(
        _Conn(),
        _Table(),
        [{"id": 1, "v": "a"}],
        ["id"],
        ["id", "v"],
        ["v"],
    )
    assert n == 1
    blob = "\n".join(executed)
    assert "CREATE LOCAL TEMPORARY TABLE" in blob
    assert "ON COMMIT PRESERVE ROWS" in blob
    assert "WHERE FALSE" in blob
    assert "MERGE INTO" in blob
    assert "IS NULL" in blob
    assert "WHEN MATCHED THEN UPDATE SET" in blob
    assert any("DROP TABLE" in e for e in executed)


def test_upsert_batch_routes_hana_and_vertica():
    from connectors import generic_sql as gs

    calls: list[str] = []

    def _hana(*_a, **_k):  # noqa: ANN001
        calls.append("hana")
        return 3

    def _vert(*_a, **_k):  # noqa: ANN001
        calls.append("vertica")
        return 4

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

    orig_h = gs._hana_merge_upsert
    orig_v = gs._vertica_merge_upsert
    gs._hana_merge_upsert = _hana  # type: ignore[assignment]
    gs._vertica_merge_upsert = _vert  # type: ignore[assignment]
    try:
        n1 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "hana"
        )
        n2 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "sap_hana"
        )
        n3 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "vertica"
        )
    finally:
        gs._hana_merge_upsert = orig_h  # type: ignore[assignment]
        gs._vertica_merge_upsert = orig_v  # type: ignore[assignment]

    assert n1 == 3 and n2 == 3 and n3 == 4
    assert calls == ["hana", "hana", "vertica"]


def test_hana_vertica_empty_rows():
    from connectors.generic_sql import _hana_merge_upsert, _vertica_merge_upsert

    class _Conn:
        def execute(self, *_a, **_k):  # noqa: ANN001
            raise AssertionError("should not execute")

    class _Table:
        name = "t"
        schema = None

    assert _hana_merge_upsert(_Conn(), _Table(), [], ["id"], ["id"], ["id"]) == 0
    assert _vertica_merge_upsert(_Conn(), _Table(), [], ["id"], ["id"], ["id"]) == 0
