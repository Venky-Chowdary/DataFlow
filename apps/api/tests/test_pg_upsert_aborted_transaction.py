"""A failed Postgres COPY upsert must not hide the first error.

MariaDB → Postgres upsert staged with COPY. The merge failed, the
transaction aborted, and the values fallback then raised
``current transaction is aborted`` instead of the original failure.
"""

from __future__ import annotations

import pytest

from connectors import postgresql_writer as writer


def test_aborted_upsert_reraises_the_copy_error(monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []

    class _Conn:
        def rollback(self) -> None:
            order.append("rollback")

    class _Cur:
        def __init__(self) -> None:
            self.connection = _Conn()

        def execute(self, *_args, **_kwargs) -> None:
            order.append("execute")
            raise RuntimeError("duplicate key value violates unique constraint")

    def _values(*_args, **_kwargs) -> None:
        order.append("values")
        raise RuntimeError("current transaction is aborted, commands ignored until end of transaction block")

    class _Ident:
        def __init__(self, *_args: object) -> None:
            pass

    class _Sql:
        def __init__(self, _text: str) -> None:
            pass

        def format(self, *_args: object) -> "_Sql":
            return self

    class _Mod:
        SQL = _Sql
        Identifier = _Ident

    monkeypatch.setattr(writer, "_execute_values_insert", _values)
    with pytest.raises(RuntimeError, match="duplicate key value"):
        writer._copy_upsert_batch(
            _Cur(),
            _Mod,
            schema="public",
            table_name="orders",
            target_cols=["id", "qty"],
            conflict_cols=["id"],
            batch=[(1, 2)],
            insert_sql="INSERT",
        )
    assert order.index("rollback") < order.index("values")
