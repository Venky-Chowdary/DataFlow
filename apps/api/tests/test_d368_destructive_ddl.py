"""D368: a widen never narrows, and never emits an unsized character type.

SQL Server treats ``VARCHAR`` with no length as ``VARCHAR(1)`` in a data
definition. NVARCHAR → VARCHAR drops Unicode. INT → BIGINT is not applied
when the source is still a 32-bit integer.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from connectors.schema_drift import (  # noqa: E402
    _build_widen_ddl,
    character_ddl_is_unsized,
    existing_carrier_holds_source,
    is_wider_type,
)
from services.schema_drift import dest_widen_proposal, widened_dest_carrier  # noqa: E402


_UNSIZED_IN_SQL = __import__("re").compile(
    r"\b(?:N?VARCHAR2?|N?CHAR|CHARACTER(?:\s+VARYING)?|NVARCHAR2)\b(?!\s*\()",
    __import__("re").I,
)


@pytest.mark.parametrize(
    "dialect",
    ["sqlserver", "mssql", "mysql", "mariadb", "postgresql", "oracle", "duckdb"],
)
@pytest.mark.parametrize("bare", ["VARCHAR", "CHAR", "NVARCHAR", "NCHAR"])
def test_no_dialect_emits_an_unsized_character_alter(dialect: str, bare: str) -> None:
    """Refuse, or materialize to a sized / LOB carrier. Never emit bare VARCHAR."""
    try:
        ddl = _build_widen_ddl(dialect, "dbo", "q3_ms_up", "name", bare, "VARCHAR(40)")
    except ValueError as exc:
        assert "unsized character" in str(exc) or "drops Unicode" in str(exc)
        return
    assert _UNSIZED_IN_SQL.search(ddl) is None, ddl


def test_nvarchar_to_varchar_is_not_a_widen() -> None:
    assert is_wider_type("NVARCHAR(400)", "VARCHAR", dest_db="sqlserver") is False
    # A longer source varchar is a width witness. The ALTER keeps NVARCHAR.
    assert is_wider_type("NVARCHAR(400)", "VARCHAR(800)", dest_db="sqlserver") is True
    assert is_wider_type("NVARCHAR(400)", "NVARCHAR(800)", dest_db="sqlserver") is True
    assert character_ddl_is_unsized("VARCHAR") is True
    assert character_ddl_is_unsized("NVARCHAR(MAX)") is False
    assert character_ddl_is_unsized("TEXT") is False
    with pytest.raises(ValueError, match="drops Unicode"):
        _build_widen_ddl(
            "sqlserver", "dbo", "q3_ms_up", "name", "VARCHAR(800)", "NVARCHAR(400)"
        )


def test_unsized_varchar_does_not_propose_a_widen_of_nvarchar() -> None:
    proposal = dest_widen_proposal(
        {},
        "name",
        "name",
        "VARCHAR",
        "NVARCHAR(400)",
        dest_db="sqlserver",
    )
    assert proposal is None
    assert widened_dest_carrier("NVARCHAR(400)", "VARCHAR") == "NVARCHAR(400)"


def test_real_varchar_widen_keeps_the_national_family() -> None:
    assert widened_dest_carrier("NVARCHAR(400)", "VARCHAR(800)") == "NVARCHAR(800)"
    proposal = dest_widen_proposal(
        {},
        "name",
        "name",
        "VARCHAR(800)",
        "NVARCHAR(400)",
        dest_db="sqlserver",
    )
    assert proposal is not None
    assert proposal["new_type"] == "NVARCHAR(800)"
    ddl = _build_widen_ddl(
        "sqlserver", "dbo", "q3_ms_up", "name", proposal["new_type"], "NVARCHAR(400)"
    )
    assert "NVARCHAR(800)" in ddl.upper().replace(" ", "")
    assert " VARCHAR" not in f" {ddl.upper()}"


def test_int_is_not_widened_to_bigint_when_the_source_is_32_bit() -> None:
    assert existing_carrier_holds_source("INTEGER", "INT", dest_db="sqlserver") is True
    assert existing_carrier_holds_source("INT4", "INTEGER", dest_db="sqlserver") is True
    assert existing_carrier_holds_source("BIGINT", "INT", dest_db="sqlserver") is False
    assert is_wider_type("INT", "BIGINT", dest_db="sqlserver") is True


def test_sa_widen_leaves_nvarchar_and_int_alone(monkeypatch) -> None:
    from connectors import generic_sql as gs

    executed: list[str] = []

    class _Type:
        def __init__(self, spelling: str) -> None:
            self.spelling = spelling

        def compile(self, dialect=None):  # noqa: ARG002
            return self.spelling

    class _Conn:
        def execute(self, stmt):
            executed.append(str(stmt))
            return MagicMock()

        def commit(self):
            return None

    class _Inspector:
        def get_columns(self, table_name, schema=None):  # noqa: ARG002
            return [
                {"name": "name", "type": _Type("NVARCHAR(400)")},
                {"name": "qty", "type": _Type("INT")},
            ]

    monkeypatch.setattr(gs.sa, "inspect", lambda _conn: _Inspector())
    engine = SimpleNamespace(dialect=SimpleNamespace(name="mssql"))
    restore: list[str] = []
    log = gs._widen_existing_columns_sa(
        _Conn(),
        engine,
        "sqlserver",
        "dbo",
        "q3_ms_up",
        ["name", "qty"],
        {"name": "VARCHAR", "qty": "BIGINT"},
        source_types={"name": "VARCHAR", "qty": "INTEGER"},
        restore_out=restore,
    )
    assert log == []
    assert executed == []
    assert restore == []


def test_a_real_bigint_widen_records_a_restore(monkeypatch) -> None:
    from connectors import generic_sql as gs

    executed: list[str] = []

    class _Type:
        def compile(self, dialect=None):  # noqa: ARG002
            return "INT"

    class _Conn:
        def execute(self, stmt):
            executed.append(str(stmt))
            return MagicMock()

        def commit(self):
            return None

    class _Inspector:
        def get_columns(self, table_name, schema=None):  # noqa: ARG002
            return [{"name": "qty", "type": _Type()}]

    monkeypatch.setattr(gs.sa, "inspect", lambda _conn: _Inspector())
    engine = SimpleNamespace(dialect=SimpleNamespace(name="mssql"))
    restore: list[str] = []
    log = gs._widen_existing_columns_sa(
        _Conn(),
        engine,
        "sqlserver",
        "dbo",
        "q3_ms_up",
        ["qty"],
        {"qty": "BIGINT"},
        source_types={"qty": "BIGINT"},
        restore_out=restore,
    )
    assert len(log) == 1
    assert "BIGINT" in log[0].upper()
    assert restore and "INT" in restore[0].upper()


def test_pg_query_cursor_keeps_varchar_length() -> None:
    from services.decimal_observe import cursor_declared_carriers

    description = (
        ("name", 1043, None, 400, None, None, None),
        ("qty", 23, None, 4, None, None, None),
        ("note", 25, None, -1, None, None, None),
    )
    declared = cursor_declared_carriers(
        ["name", "qty", "note"], description, dialect="postgresql"
    )
    assert declared["name"] == "VARCHAR(400)"
    assert declared["qty"] == "INTEGER"
    assert declared["note"] == "VARCHAR"


def test_failed_write_restore_replays_the_original_type() -> None:
    from connectors.generic_sql import _apply_widen_restore

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt):
            executed.append(str(stmt))

        def commit(self):
            executed.append("COMMIT")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

    _apply_widen_restore(
        _Engine(),
        ["ALTER TABLE [dbo].[q3_ms_up] ALTER COLUMN [name] NVARCHAR(400)"],
    )
    assert any("NVARCHAR(400)" in stmt for stmt in executed)
    assert "COMMIT" in executed
