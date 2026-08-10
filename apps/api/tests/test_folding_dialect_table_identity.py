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


class _Engine:
    def __init__(self, name: str, existing: set[tuple[str, str | None]] | None = None):
        self.dialect = _Dialect(name)
        self.existing = existing or set()


class _Inspector:
    def __init__(self, engine: _Engine) -> None:
        self._engine = engine

    def has_table(self, table: str, schema: str | None = None) -> bool:
        return (table, schema) in self._engine.existing


@pytest.fixture(autouse=True)
def _patch_inspect(monkeypatch: pytest.MonkeyPatch) -> None:
    import connectors.generic_sql as gs

    monkeypatch.setattr(gs.sa, "inspect", lambda engine: _Inspector(engine))


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
    import connectors.generic_sql as gs

    def _boom(_engine: Any) -> Any:
        raise RuntimeError("catalog unavailable")

    original = gs.sa.inspect
    gs.sa.inspect = _boom  # type: ignore[assignment]
    try:
        assert _resolve_physical_table_ident(_Engine("oracle"), "nd_mixed", None) == (
            "nd_mixed",
            None,
        )
    finally:
        gs.sa.inspect = original  # type: ignore[assignment]
