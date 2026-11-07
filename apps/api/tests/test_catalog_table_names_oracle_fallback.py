"""Oracle tables the SQLAlchemy inspector refuses to list must still be found.

``Inspector.get_table_names`` skips every Oracle table stored in the SYSTEM /
SYSAUX tablespaces, so a destination living there reflected as *absent*: the
identity watermark reported "column is not a GENERATED AS IDENTITY column" and
the RI probe reported the table missing, on a table that existed and generated
keys. The catalog is the authority when the inspector answers nothing — and the
names it returns must come back in the inspector's own normalized spelling,
because SQLAlchemy reads a raw ``EMPLOYEES`` as a quoted, case-sensitive name.
"""

from __future__ import annotations

from typing import Any

from services.physical_state_diff import catalog_table_names, resolve_stored_name


class _Dialect:
    def normalize_name(self, name: str) -> str:
        # Oracle's rule: an all-upper stored name is the case-insensitive one.
        return name.lower() if name.upper() == name else name


class _Result:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[str]]:
        return self._rows


class _Conn:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.dialect = _Dialect()
        self._rows = rows
        self.executed: list[dict[str, Any]] = []

    def execute(self, _stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        self.executed.append(params or {})
        return _Result(self._rows)


class _Inspector:
    def __init__(self, names: list[str]) -> None:
        self._names = names

    def get_table_names(self, schema: str | None = None) -> list[str]:
        return list(self._names)


def test_inspector_answer_is_used_when_it_has_one():
    conn = _Conn([("SHOULD_NOT_BE_READ",)])
    names = catalog_table_names(
        _Inspector(["orders"]), "app", conn=conn, dialect="oracle"
    )
    assert names == ["orders"]
    assert conn.executed == []


def test_oracle_fallback_reads_the_catalog_and_normalizes_the_spelling():
    conn = _Conn([("EMPLOYEES",), ("IDDST_X",), ("mixedCase",)])
    names = catalog_table_names(_Inspector([]), "system", conn=conn, dialect="oracle")
    # Normalized like the inspector would: reflection fails on a raw EMPLOYEES.
    assert names == ["employees", "iddst_x", "mixedCase"]
    assert conn.executed == [{"own": "SYSTEM"}]
    assert resolve_stored_name(names, "iddst_x") == "iddst_x"


def test_fallback_is_oracle_only_and_never_invents_names():
    conn = _Conn([("EMPLOYEES",)])
    assert catalog_table_names(_Inspector([]), "public", conn=conn, dialect="postgresql") == []
    # No connection to ask: an empty catalog stays empty, not guessed.
    assert catalog_table_names(_Inspector([]), "system", dialect="oracle") == []


def test_unreadable_catalog_is_empty_not_an_exception():
    class _Boom(_Inspector):
        def get_table_names(self, schema: str | None = None) -> list[str]:
            raise RuntimeError("catalog unavailable")

    class _BoomConn(_Conn):
        def execute(self, _stmt: Any, params: dict[str, Any] | None = None) -> _Result:
            raise RuntimeError("catalog unavailable")

    assert (
        catalog_table_names(
            _Boom([]), "system", conn=_BoomConn([]), dialect="oracle"
        )
        == []
    )
