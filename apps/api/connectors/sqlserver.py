"""First-class SQL Server connector — thin dialect wrapper over generic_sql."""

from __future__ import annotations

from typing import Any

from connectors.generic_sql import test_generic_sql


def test_sqlserver(**kwargs: Any) -> tuple[bool, str]:
    """Probe SQL Server via SQLAlchemy (pyodbc / pymssql dialect)."""
    kwargs = dict(kwargs)
    kwargs.setdefault("type", "sqlserver")
    return test_generic_sql(**kwargs)
