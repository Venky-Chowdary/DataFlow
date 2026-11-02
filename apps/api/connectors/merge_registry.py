"""Dialect MERGE / upsert strategy registry (Phase F8 facade).

``generic_sql`` still owns the dialect SQL bodies. This module is the
machine-readable inventory of which dialect uses which strategy so Decision
Kernel / preflight / docs do not scrape the 5.5k-line file.

When dest-owned lattice columns (mirror ``_deleted``) are present,
``generic_sql`` replaces ``delete_insert`` with portable UPDATE+INSERT
(``merge_dialects.update_insert_upsert``). Native strategies already exclude
the lattice from SET. Registry ``fallback`` is the no-lattice path.

``identity`` answers "how does this engine enforce upsert identity?" — the one
answer Validate's disclosure, the DDL writers and the merge writers read:

* ``unique_index`` — the native upsert (``ON CONFLICT`` / ``ON DUPLICATE KEY``)
  only fires on a unique constraint, so an existing table without one gets a
  disclosed ``CREATE UNIQUE INDEX`` (PostgreSQL; MySQL on a full-length key).
* ``merge`` — the writer matches the key itself (``MERGE`` or keyed
  delete+insert) and DataFlow never creates an index: Snowflake has no unique
  indexes and does not enforce UNIQUE, SQL Server / Oracle / BigQuery /
  Databricks MERGE without one. There is no DDL to disclose; duplicate keys
  already at rest are still a block (the match would be ambiguous).

A third answer, ``impossible``, is per key, not per engine — e.g. a MySQL JSON
key. :mod:`services.upsert_identity` decides it from the live key columns.
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


UPSERT_IDENTITY_UNIQUE_INDEX: Final = "unique_index"
UPSERT_IDENTITY_MERGE: Final = "merge"
UPSERT_IDENTITY_IMPOSSIBLE: Final = "impossible"

# Only these engines' writers create the index their native upsert needs.
# Every other dialect is keyed by the writer's own match on the key
# (generic_sql MERGE / delete+insert, Snowflake/BigQuery MERGE) — no index.
_UNIQUE_INDEX_DIALECTS: Final = frozenset({"postgresql", "mysql", "mariadb"})

for _dialect, _strategy in MERGE_STRATEGIES.items():
    _strategy["identity"] = (
        UPSERT_IDENTITY_UNIQUE_INDEX
        if _dialect in _UNIQUE_INDEX_DIALECTS
        else UPSERT_IDENTITY_MERGE
    )


def upsert_identity(dialect: str) -> str:
    """How an upsert into ``dialect`` enforces key identity (see module doc)."""
    return str(merge_strategy_for(dialect).get("identity") or UPSERT_IDENTITY_MERGE)


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
        # generic_sql keys an undeclared dialect by delete+insert on the key
        # and never creates an index.
        "identity": UPSERT_IDENTITY_MERGE,
    }
