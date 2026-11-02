"""A full_refresh must clear the destination it points at — and recreate *that* one.

Three defects on case-folding destinations (Oracle/Snowflake/DB2), each found by
the live route matrix on a quoted lower-case Oracle table:

1. ``DROP TABLE IF EXISTS`` is a syntax error on Oracle, so the drop fell through
   to a fallback whose unquoted ``Table()`` name folded to ``SCN_DST``,
   ``checkfirst`` read the folded name as absent, and the drop became a *silent
   no-op*: the overwrite then appended onto rows it had reported cleared
   (ORA-00001 with a key, doubled data without one).
2. The recreate folded the name, so the destination came back as a different
   object than the one the operator (and everything reading it) points at.
3. Column-spelling probing folded the table name too, so mapped targets kept
   their Map spelling and the write asked Oracle for ``"email"`` beside a stored
   ``EMAIL`` — ORA-00904 on a column that is plainly there.
"""

from __future__ import annotations

from typing import Any

import pytest

from connectors.generic_sql import (
    _drop_table_sql,
    _require_table_gone,
    _resolve_physical_table_ident,
)
from src.transfer.connector_dispatch import writer_extra_kwargs


class _Dialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _Engine:
    def __init__(self, name: str) -> None:
        self.dialect = _Dialect(name)


@pytest.mark.parametrize(
    ("dialect", "expected_fragment"),
    [
        ("oracle", "EXECUTE IMMEDIATE"),
        ("mssql", "OBJECT_ID"),
        ("postgresql", "DROP TABLE IF EXISTS"),
        ("mysql", "DROP TABLE IF EXISTS"),
    ],
)
def test_conditional_drop_speaks_each_dialect(dialect: str, expected_fragment: str):
    sql = _drop_table_sql(_Engine(dialect), '"scn_dst"', "scn_dst")
    assert expected_fragment in sql
    if dialect == "oracle":
        # -942 is "table does not exist"; anything else must still raise.
        assert "IF EXISTS" not in sql
        assert "-942" in sql


def test_drop_that_left_the_table_behind_is_refused(monkeypatch: pytest.MonkeyPatch):
    """A no-op drop must never be reported as a cleared destination."""

    class _Ident:
        resolved = True
        exists = True

    import services.sql_object_identity as identity

    monkeypatch.setattr(
        identity, "resolve_object_identity", lambda *a, **k: _Ident()
    )
    with pytest.raises(RuntimeError, match="still in"):
        _require_table_gone(_Engine("oracle"), "scn_dst", "SYSTEM")


def test_drop_is_accepted_when_the_catalog_no_longer_has_the_table(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Ident:
        resolved = True
        exists = False

    import services.sql_object_identity as identity

    monkeypatch.setattr(
        identity, "resolve_object_identity", lambda *a, **k: _Ident()
    )
    _require_table_gone(_Engine("oracle"), "scn_dst", "SYSTEM")


def _patch_absent_table(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Ident:
        resolved = True
        exists = False
        table = "scn_dst"
        schema = "SYSTEM"

    import services.sql_object_identity as identity

    monkeypatch.setattr(
        identity, "resolve_object_identity", lambda *a, **k: _Ident()
    )


def test_recreate_keeps_the_spelling_the_destination_had(
    monkeypatch: pytest.MonkeyPatch,
):
    _patch_absent_table(monkeypatch)
    table, schema = _resolve_physical_table_ident(
        _Engine("oracle"), "scn_dst", "SYSTEM", prior_spelling="scn_dst"
    )
    assert (table, schema) == ("scn_dst", "SYSTEM")


def test_first_load_of_a_missing_table_still_folds(monkeypatch: pytest.MonkeyPatch):
    """No prior spelling means no prior table: fold, so every client can see it."""
    _patch_absent_table(monkeypatch)
    table, _schema = _resolve_physical_table_ident(
        _Engine("oracle"), "scn_dst", "SYSTEM"
    )
    assert table == "SCN_DST"


def test_prior_spelling_of_a_different_table_is_ignored(
    monkeypatch: pytest.MonkeyPatch,
):
    _patch_absent_table(monkeypatch)
    table, _schema = _resolve_physical_table_ident(
        _Engine("oracle"), "scn_dst", "SYSTEM", prior_spelling="other_table"
    )
    assert table == "SCN_DST"


def test_prior_spelling_reaches_every_writer_path():
    """One owner: both the adapter path and the streaming path read it here.

    The stamp lived on the endpoint and the streaming path built its own kwargs,
    so the overwrite recreate folded the name on exactly the path Studio uses.
    """
    dest: Any = type("_Dest", (), {"extra": {"dest_table_prior_spelling": "scn_dst"}})()
    for driver in ("oracle", "snowflake", "generic_sql", "sqlserver"):
        assert writer_extra_kwargs(driver, cfg={}, dest=dest)[
            "dest_table_prior_spelling"
        ] == "scn_dst"
    # Absent stamp must not inject an empty override.
    assert "dest_table_prior_spelling" not in writer_extra_kwargs(
        "oracle", cfg={}, dest=None
    )
