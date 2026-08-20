"""Independent source re-read for Gate-8 — Fivetran/HVR Compare class.

Write-pass fingerprints hash the same remapped rows the writer just sent.
Dest read-back of those bytes proves the warehouse stored what we sent, not
that we read the source correctly. ``full_checksum`` / ``migration_proven``
require a second source digest that did not travel with the write.

This module owns two decisions so stream.py cannot drift:

* **When** to re-read (operator env + heterogeneous warehouse auto).
* **How** to page the re-read. Snapshot-scan sources must not OFFSET-page
  (O(n²) and skip/duplicate under concurrent writes) — the same cliff the
  extract already closed.
"""

from __future__ import annotations

from typing import Any, Final

from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES

#: Destinations that can independently SELECT the written population.
WAREHOUSE_VERIFY_DESTS: Final[frozenset[str]] = SNAPSHOT_SCAN_SOURCES

#: Sources the Gate-8 re-read loop knows how to page. A source outside this set
#: keeps the write-pass digest even when ``should_reread_source`` says yes, so
#: the write pass must not skip its inline fingerprints for those routes.
REREAD_SCAN_SOURCES: Final[frozenset[str]] = frozenset(
    {
        "postgresql",
        "redshift",
        "mysql",
        "snowflake",
        "bigquery",
        "sqlite",
        "generic_sql",
        "mongodb",
        "s3",
        "gcs",
        "adls",
    }
)

_PG_FAMILY: Final[frozenset[str]] = frozenset(
    {"postgresql", "postgres", "pg", "timescaledb", "alloydb", "supabase", "redshift"}
)
_MYSQL_FAMILY: Final[frozenset[str]] = frozenset({"mysql", "mariadb", "tidb"})
_MSSQL_FAMILY: Final[frozenset[str]] = frozenset({"sqlserver", "mssql", "azure_sql"})


def engine_family(engine: str | None) -> str:
    """Canonical family so ``postgres`` and ``postgresql`` do not look heterogeneous."""
    raw = (engine or "").strip().lower()
    if raw in _PG_FAMILY:
        return "postgresql"
    if raw in _MYSQL_FAMILY:
        return "mysql"
    if raw in _MSSQL_FAMILY:
        return "sqlserver"
    return raw


def _env_reread_mode() -> str:
    from services.brand_env import getenv_brand

    raw = (getenv_brand("RECONCILE_SOURCE_REREAD", "auto") or "auto").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return "off"
    if raw in {"1", "true", "yes", "on"}:
        return "on"
    return "auto"


def should_reread_source(
    *,
    src_type: str,
    dest_type: str,
    incremental: bool = False,
    partial_write_pass: bool = False,
) -> bool:
    """True when Gate-8 must open a second source scan.

    A partial write-pass (resume covering only the tail) always re-reads —
    comparing a session digest to a full destination is a false mismatch.
    ``DATAFLOW_RECONCILE_SOURCE_REREAD=0`` still cannot suppress that.

    Auto (default): heterogeneous warehouse → warehouse full refresh. Same-engine
    routes keep the cheap write-pass unless the operator forces ``=1``.
    Snowflake→Postgres is the named hole: engine HASH_AGG is PostgreSQL-family
    same-type only, so without this re-read the 150k TPC-H path stayed writer-ack.
    """
    if partial_write_pass:
        return True
    mode = _env_reread_mode()
    if mode == "off":
        return False
    if mode == "on":
        return True
    if incremental:
        return False
    src = engine_family(src_type)
    dest = engine_family(dest_type)
    if not src or not dest or src == dest:
        return False
    if src_type not in SNAPSHOT_SCAN_SOURCES:
        return False
    if dest_type not in WAREHOUSE_VERIFY_DESTS:
        return False
    return True


def reread_pagination_plan(
    *,
    src_type: str,
    incremental: bool = False,
) -> dict[str, Any]:
    """How the independent re-read pages.

    ``mode=scan`` carries a fresh ``scan_state`` so ``_read_batch`` holds one
    SELECT/find + fetchmany/getmore. ``use_offset`` is False on that path —
    the reader ignores the numeric offset.
    """
    if incremental:
        return {"mode": "cursor_or_offset", "scan_state": None, "use_offset": True}
    if src_type in SNAPSHOT_SCAN_SOURCES:
        return {"mode": "scan", "scan_state": {}, "use_offset": False}
    return {"mode": "offset", "scan_state": None, "use_offset": True}
