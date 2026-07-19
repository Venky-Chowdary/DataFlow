"""First-class Oracle connector — thin dialect wrapper over generic_sql."""

from __future__ import annotations

from typing import Any

from connectors.generic_sql import test_generic_sql


def test_oracle(**kwargs: Any) -> tuple[bool, str]:
    """Probe Oracle via SQLAlchemy (oracledb dialect)."""
    kwargs = dict(kwargs)
    kwargs.setdefault("type", "oracle")
    return test_generic_sql(**kwargs)
