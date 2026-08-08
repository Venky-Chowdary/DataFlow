"""Dialect MERGE / upsert strategy registry (Phase F8 facade).

``generic_sql`` still owns the dialect SQL bodies. This module is the
machine-readable inventory of which dialect uses which strategy so Decision
Kernel / preflight / docs do not scrape the 5.5k-line file.
"""

from __future__ import annotations

from typing import Any, Final

# Strategies are declarative — implementations remain in generic_sql until
# per-dialect modules are extracted under connectors/merge/.
MERGE_STRATEGIES: Final[dict[str, dict[str, Any]]] = {
    "postgresql": {
        "strategy": "insert_on_conflict",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "mysql": {
        "strategy": "insert_on_duplicate_key",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "mariadb": {
        "strategy": "insert_on_duplicate_key",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "sqlite": {
        "strategy": "insert_on_conflict",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "sqlserver": {
        "strategy": "merge_holdlock",
        "null_safe_pk": True,
        "at_least_once": True,
        "fallback": "delete_insert",
    },
    "mssql": {
        "strategy": "merge_holdlock",
        "null_safe_pk": True,
        "at_least_once": True,
        "fallback": "delete_insert",
    },
    "oracle": {
        "strategy": "merge",
        "null_safe_pk": True,
        "at_least_once": True,
        "fallback": "delete_insert",
    },
    "snowflake": {
        "strategy": "merge",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "bigquery": {
        "strategy": "merge",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "redshift": {
        "strategy": "delete_insert_stage",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "duckdb": {
        "strategy": "insert_on_conflict",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "hana": {
        "strategy": "merge",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "db2": {
        "strategy": "merge",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "teradata": {
        "strategy": "merge",
        "null_safe_pk": True,
        "at_least_once": True,
    },
    "sybase_ase": {
        "strategy": "merge",
        "null_safe_pk": True,
        "at_least_once": True,
    },
}


def merge_strategy_for(dialect: str) -> dict[str, Any]:
    """Return the declared MERGE/upsert strategy for a SQL dialect."""
    key = (dialect or "").strip().lower()
    if key in MERGE_STRATEGIES:
        return dict(MERGE_STRATEGIES[key])
    # Hosted twins / aliases → postgresql/mysql family when unknown.
    if "postgres" in key or key in ("pg", "citus", "cratedb"):
        return dict(MERGE_STRATEGIES["postgresql"])
    if "mysql" in key or "maria" in key:
        return dict(MERGE_STRATEGIES["mysql"])
    return {
        "strategy": "generic_sqlalchemy",
        "null_safe_pk": True,
        "at_least_once": True,
        "declared": False,
    }
