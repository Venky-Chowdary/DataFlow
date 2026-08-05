"""Wave 42: Teradata MERGE (PI equality ON) + Trino/Presto MERGE (NULL-safe ON)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_teradata_merge_uses_volatile_stage_and_pi_equality_on():
    from connectors.generic_sql import _teradata_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    class _Table:
        name = "ORDERS"
        schema = "APP"

    n = _teradata_merge_upsert(
        _Conn(),
        _Table(),
        [{"id": 1, "amt": 10}, {"id": 2, "amt": 20}],
        ["id"],
        ["id", "amt"],
        ["amt", "id"],  # id must be stripped from UPDATE SET (PI)
    )
    assert n == 2
    blob = "\n".join(executed)
    assert "CREATE MULTISET VOLATILE TABLE" in blob
    assert "PRIMARY INDEX" in blob
    assert "ON COMMIT PRESERVE ROWS" in blob
    assert "MERGE INTO" in blob
    assert 'T."ID" = S."ID"' in blob or 'T."id" = S."id"' in blob.replace(
        '"ID"', '"id"'
    )
    # Teradata forbids NULL equate in ON — must NOT use null-safe OR form.
    assert "IS NULL" not in blob
    # PI / conflict col must not appear in UPDATE SET.
    assert "UPDATE SET" in blob
    update_part = blob.split("UPDATE SET", 1)[1].split("WHEN NOT MATCHED", 1)[0]
    assert '"AMT"' in update_part
    assert '"ID" =' not in update_part and 'S."ID"' not in update_part
    assert any("DROP TABLE" in e for e in executed)


def test_teradata_merge_on_helper_is_equality_only():
    from connectors.generic_sql import _teradata_merge_on

    on = _teradata_merge_on(["id", "tenant"])
    assert on == 't."id" = s."id" AND t."tenant" = s."tenant"'
    assert "IS NULL" not in on


def test_trino_merge_null_safe_on_and_values_stage():
    from connectors.generic_sql import _trino_merge_upsert

    executed: list[tuple[str, dict | None]] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append((str(getattr(stmt, "text", stmt)).upper(), params))

    class _Table:
        name = "events"
        schema = "hive.default"

    n = _trino_merge_upsert(
        _Conn(),
        _Table(),
        [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}],
        ["id"],
        ["id", "v"],
        ["v"],
        chunk_size=50,
    )
    assert n == 2
    assert len(executed) == 1
    sql, params = executed[0]
    assert "MERGE INTO" in sql
    assert '"HIVE"."DEFAULT"."EVENTS"' in sql or (
        '"hive"."default"."events"' in sql.lower()
    )
    assert "USING (VALUES" in sql
    assert "IS NULL" in sql  # NULL-safe ON
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert params is not None
    assert params.get("r0_id") == 1
    assert params.get("r1_v") == "b"


def test_trino_qualified_table_splits_catalog_schema():
    from connectors.generic_sql import _trino_qualified_table

    class _Table:
        name = "t"
        schema = "iceberg.analytics"

    assert _trino_qualified_table(_Table()) == '"iceberg"."analytics"."t"'


def test_upsert_batch_routes_teradata_and_trino():
    from connectors import generic_sql as gs

    calls: list[str] = []

    def _td(*_a, **_k):  # noqa: ANN001
        calls.append("teradata")
        return 1

    def _tr(*_a, **_k):  # noqa: ANN001
        calls.append("trino")
        return 2

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

    orig_td = gs._teradata_merge_upsert
    orig_tr = gs._trino_merge_upsert
    gs._teradata_merge_upsert = _td  # type: ignore[assignment]
    gs._trino_merge_upsert = _tr  # type: ignore[assignment]
    try:
        n1 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "teradatasql"
        )
        n2 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "trino"
        )
        n3 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "presto"
        )
    finally:
        gs._teradata_merge_upsert = orig_td  # type: ignore[assignment]
        gs._trino_merge_upsert = orig_tr  # type: ignore[assignment]

    assert n1 == 1 and n2 == 2 and n3 == 2
    assert calls == ["teradata", "trino", "trino"]


def test_teradata_merge_empty_rows():
    from connectors.generic_sql import _teradata_merge_upsert, _trino_merge_upsert

    class _Conn:
        def execute(self, *_a, **_k):  # noqa: ANN001
            raise AssertionError("should not execute")

    class _Table:
        name = "t"
        schema = None

    assert _teradata_merge_upsert(_Conn(), _Table(), [], ["id"], ["id"], ["id"]) == 0
    assert _trino_merge_upsert(_Conn(), _Table(), [], ["id"], ["id"], ["id"]) == 0
