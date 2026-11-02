"""Route-purge DELETE must quote recipe-controlled key column names."""

from __future__ import annotations

from types import SimpleNamespace

from src.transfer.stream import _purge_route_key_tuples

_INJECTED = "x\"::text)) OR (('1'='1"


class _Cursor:
    def __init__(self, log: list[tuple[str, object]]) -> None:
        self._log = log
        self.rowcount = 0

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self._log.append((sql, params))

    def fetchone(self) -> tuple[object]:
        return ("present",)


class _Conn:
    def __init__(self) -> None:
        self.log: list[tuple[str, object]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.log)


def _purge(route: dict, *, pk_target_cols: list[str], emitted: list[str]) -> list[str]:
    conn = _Conn()
    _purge_route_key_tuples(
        conn,
        router_step=SimpleNamespace(options={"routes": [route], "targets": {}}),
        recipe=SimpleNamespace(route_output_columns=lambda _name: emitted),
        dest_table="orders",
        dest_cfg={"schema": "public"},
        batch=SimpleNamespace(headers=["id", "v"], rows=[["1", "a"], ["2", "b"]]),
        pk_source_cols=["id"],
        pk_target_cols=pk_target_cols,
        pk_audit_cols=[],
        diverted_keys=[],
    )
    return [sql for sql, _ in conn.log if sql.startswith("DELETE")]


def test_mapped_route_key_with_double_quote_is_escaped():
    deletes = _purge(
        {"name": "eu", "map": {"id": _INJECTED, "v": "v"}},
        pk_target_cols=["id"],
        emitted=[],
    )
    assert deletes == [
        'DELETE FROM "public"."orders__eu" '
        'WHERE ("x""::text)) OR ((\'1\'=\'1"::text) IN (VALUES (%s),(%s))'
    ]


def test_pass_through_route_pk_target_with_double_quote_is_escaped():
    deletes = _purge(
        {"name": "us"},
        pk_target_cols=[_INJECTED],
        emitted=[_INJECTED, "v"],
    )
    assert len(deletes) == 1
    assert 'WHERE ("x""::text)) OR' in deletes[0]
    assert "OR (('1'='1\"::text) IN (VALUES" in deletes[0]


def test_plain_key_column_is_double_quoted():
    deletes = _purge(
        {"name": "eu", "map": {"id": "order_id", "v": "v"}},
        pk_target_cols=["id"],
        emitted=[],
    )
    assert deletes == [
        'DELETE FROM "public"."orders__eu" WHERE ("order_id"::text) IN (VALUES (%s),(%s))'
    ]
