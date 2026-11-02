"""Destination object identity must come from the catalog, never from a fold.

Regression cover for the Oracle split-destination defect: an append into an
existing quoted lower-case table created a *second*, folded table beside it,
and the two runs of one job wrote to different objects while every gate passed.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from services.sql_object_identity import ObjectIdentity, resolve_object_identity


class _Dialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _OracleConn:
    """Minimal ALL_TABLES / ALL_TAB_COLUMNS stand-in."""

    def __init__(self, tables: dict[tuple[str, str], list[str]]) -> None:
        self.tables = tables

    def execute(self, _stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        params = params or {}
        if "t" in params and "s" in params:
            want = str(params["t"])
            owner = params.get("s")
            return _Result(
                [
                    (o, t)
                    for (o, t) in self.tables
                    if t.upper() == want and (owner is None or o.upper() == owner)
                ]
            )
        key = (str(params.get("o")), str(params.get("t")))
        return _Result([(c,) for c in self.tables.get(key, [])])

    def __enter__(self) -> _OracleConn:
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _OracleEngine:
    def __init__(self, tables: dict[tuple[str, str], list[str]]) -> None:
        self.dialect = _Dialect("oracle")
        self.tables = tables

    def connect(self) -> _OracleConn:
        return _OracleConn(self.tables)


def _engine_with_both_spellings() -> _OracleEngine:
    return _OracleEngine(
        {
            ("SYSTEM", "USERS"): ["ID", "EMAIL"],
            ("SYSTEM", "users"): ["id", "email"],
        }
    )


def test_exact_spelling_wins_when_both_case_variants_exist() -> None:
    ident = resolve_object_identity(
        _engine_with_both_spellings(), "users", "system", columns=["id", "email"]
    )
    assert (ident.exists, ident.schema, ident.table) == (True, "SYSTEM", "users")
    assert ident.columns == {"id": "id", "email": "email"}


def test_folded_spelling_resolves_to_the_folded_object() -> None:
    ident = resolve_object_identity(
        _engine_with_both_spellings(), "USERS", "system", columns=["id"]
    )
    assert ident.table == "USERS"
    assert ident.columns == {"id": "ID"}


def test_case_insensitive_match_when_only_one_spelling_exists() -> None:
    engine = _OracleEngine({("SYSTEM", "USERS"): ["ID"]})
    ident = resolve_object_identity(engine, "users", "system", columns=["id"])
    assert (ident.exists, ident.table, ident.columns) == (True, "USERS", {"id": "ID"})


def test_absent_object_is_reported_absent_not_unresolved() -> None:
    engine = _OracleEngine({("SYSTEM", "USERS"): ["ID"]})
    ident = resolve_object_identity(engine, "orders", "system")
    assert (ident.exists, ident.resolved) == (False, True)


def test_unreadable_catalog_is_unresolved_never_absent() -> None:
    class _Broken(_OracleEngine):
        def connect(self) -> _OracleConn:
            raise RuntimeError("ORA-01031: insufficient privileges")

    ident = resolve_object_identity(_Broken({}), "users", "system")
    assert (ident.exists, ident.resolved) == (False, False)


def test_non_folding_engine_resolves_through_the_inspector() -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(sa.text('CREATE TABLE "Orders" (id INTEGER, "Amount" TEXT)'))
    ident = resolve_object_identity(engine, "orders", None, columns=["amount", "id"])
    assert (ident.exists, ident.table) == (True, "Orders")
    assert ident.columns == {"amount": "Amount", "id": "id"}


def test_qualified_name_is_split_before_catalog_lookup() -> None:
    """``SYSTEM.users`` plus a fallback schema must not look up ``SYSTEM.users``."""
    ident = resolve_object_identity(
        _engine_with_both_spellings(), "SYSTEM.users", "PUBLIC", columns=["id"]
    )
    assert (ident.exists, ident.schema, ident.table) == (True, "SYSTEM", "users")
    assert ident.columns == {"id": "id"}


def test_empty_table_name_is_absent() -> None:
    assert resolve_object_identity(_OracleEngine({}), "", "system") == ObjectIdentity(
        "system", "", False
    )
