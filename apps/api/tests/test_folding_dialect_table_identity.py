"""Destination spelling on case-folding engines must match what read-back sees.

Oracle/Snowflake/DB2 fold unquoted identifiers to upper case, but every CREATE
in generic_sql is emitted quoted. A lower-case name typed in Studio therefore
landed a lower-case-quoted table that reconciliation (which folds, like every
other client) could not read: ORA-00942 on a table the write had just reported
written.
"""

from __future__ import annotations

from typing import Any

import pytest

from connectors.generic_sql import _resolve_physical_table_ident


class _Dialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _Conn:
    """Answers the catalog queries ``resolve_object_identity`` issues."""

    def __init__(self, existing: set[tuple[str, str | None]]) -> None:
        self.existing = existing

    def execute(self, _stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        params = params or {}
        if "t" not in params:  # column lookup — no columns requested in these tests
            return _Result([])
        wanted_table = str(params.get("t") or "")
        wanted_owner = params.get("s")
        rows = [
            (schema or "", table)
            for table, schema in self.existing
            if table.upper() == wanted_table
            and (wanted_owner is None or (schema or "").upper() == wanted_owner)
        ]
        return _Result(rows)

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _Engine:
    def __init__(self, name: str, existing: set[tuple[str, str | None]] | None = None):
        self.dialect = _Dialect(name)
        self.existing = existing or set()

    def connect(self) -> _Conn:
        return _Conn(self.existing)


class _Inspector:
    def __init__(self, engine: _Engine) -> None:
        self._engine = engine

    def get_schema_names(self) -> list[str]:
        return sorted({s for _t, s in self._engine.existing if s})

    def get_table_names(self, schema: str | None = None) -> list[str]:
        return [t for t, s in self._engine.existing if s == schema]

    def get_view_names(self, schema: str | None = None) -> list[str]:
        return []


@pytest.fixture(autouse=True)
def _patch_inspect(monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlalchemy as sa

    monkeypatch.setattr(sa, "inspect", lambda engine: _Inspector(engine))


def test_new_oracle_table_is_created_under_the_folded_name() -> None:
    assert _resolve_physical_table_ident(_Engine("oracle"), "nd_mixed", "system") == (
        "ND_MIXED",
        "SYSTEM",
    )


def test_existing_lowercase_table_keeps_its_own_spelling() -> None:
    engine = _Engine("oracle", existing={("nd_mixed", "system")})
    assert _resolve_physical_table_ident(engine, "nd_mixed", "system") == (
        "nd_mixed",
        "system",
    )


def test_mixed_case_is_an_intentional_quoted_identifier() -> None:
    assert _resolve_physical_table_ident(_Engine("oracle"), "NdMixed", "System") == (
        "NdMixed",
        "System",
    )


def test_non_folding_dialects_are_untouched() -> None:
    for name in ("postgresql", "mysql", "mssql", "sqlite"):
        assert _resolve_physical_table_ident(_Engine(name), "nd_mixed", "public") == (
            "nd_mixed",
            "public",
        )


def test_unreadable_catalog_keeps_the_operator_spelling() -> None:
    class _BrokenEngine(_Engine):
        def connect(self) -> _Conn:
            raise RuntimeError("catalog unavailable")

    assert _resolve_physical_table_ident(_BrokenEngine("oracle"), "nd_mixed", None) == (
        "nd_mixed",
        None,
    )
