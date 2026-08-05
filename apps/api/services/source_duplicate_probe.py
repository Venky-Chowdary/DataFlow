"""Source-side duplicate-key probe for preflight.

Database sources can be larger than the sample used by G9, so a 100-row sample
may miss duplicates that later cause a write-batch failure. This module runs a
cheap source-side query (SQL GROUP BY or MongoDB aggregation) to find repeated
values for the resolved identity key before the transfer is approved.

Honesty contract:
- ``status="ran"`` means the probe query completed (findings may be empty = clean).
- ``skipped_unsupported`` / ``error`` must NEVER be stamped as full-selected coverage.
- Callers fail-closed when uniqueness is required but the probe did not run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)

ProbeStatus = Literal[
    "ran",
    "skipped_no_pk",
    "skipped_no_source",
    "skipped_unsupported",
    "error",
]

# SQL path covers generic_sql and all SQL-like catalog IDs.
SQLISH_SOURCE_TYPES = frozenset({
    "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb", "supabase",
    "mysql", "mariadb", "singlestore",
    "sqlserver", "mssql", "synapse", "azure_sql_database",
    "oracle", "db2", "sqlite", "duckdb", "generic_sql", "h2",
    "snowflake", "bigquery", "databricks", "clickhouse", "trino", "presto", "questdb",
})

PROBED_SOURCE_TYPES = SQLISH_SOURCE_TYPES | frozenset({"mongodb", "mongodb_atlas"})


@dataclass
class SourceDuplicateProbeResult:
    """Structured probe outcome — never invent population proof from a skip."""

    findings: list[dict[str, Any]] = field(default_factory=list)
    status: ProbeStatus = "skipped_no_source"
    message: str = ""
    db_type: str = ""

    @property
    def ran(self) -> bool:
        return self.status == "ran"


def _sql_duplicates(cfg: dict[str, Any], table: str, pk: str, limit: int = 5) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    engine = _engine(cfg)
    schema = (cfg.get("schema") or "").strip() or None

    # Build a dialect-agnostic query so SQLAlchemy emits the right LIMIT/TOP/FETCH
    # syntax for SQL Server, Oracle, etc.
    tbl = sa.table(table, schema=schema)
    pk_col = sa.column(pk)
    cnt = sa.func.count().label("_cnt")
    stmt = (
        sa.select(pk_col, cnt)
        .select_from(tbl)
        .group_by(pk_col)
        .having(cnt > 1)
        .limit(limit)
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    return [
        {"value": row[0] if row else None, "count": int(row[1]) if len(row) > 1 else 1}
        for row in rows
    ]


def _mongo_duplicates(cfg: dict[str, Any], collection: str, pk: str, limit: int = 5) -> list[dict[str, Any]]:
    from pymongo import MongoClient

    from connectors.mongodb_common import normalize_mongodb_connection_string

    uri = normalize_mongodb_connection_string(
        cfg.get("connection_string", ""),
        database=cfg.get("database", ""),
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 0),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        ssl=bool(cfg.get("ssl")),
        auth_source=cfg.get("auth_source", ""),
    )
    client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db_name = cfg.get("database") or cfg.get("auth_source") or "test"
    db = client[db_name]
    coll = db[collection]

    try:
        pipeline = [
            {"$group": {"_id": f"${pk}", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$limit": limit},
        ]
        rows = list(coll.aggregate(pipeline, maxTimeMS=5000))
    finally:
        client.close()

    return [{"value": r.get("_id"), "count": int(r.get("count", 1))} for r in rows]


def probe_source_duplicate_keys_result(
    *,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    source_collection: str = "",
    primary_key: str = "",
    workspace_id: str | None = None,
    limit: int = 5,
) -> SourceDuplicateProbeResult:
    """Run the source duplicate probe and return an honest status + findings."""
    if not primary_key:
        return SourceDuplicateProbeResult(
            status="skipped_no_pk",
            message="No primary key resolved for uniqueness probe",
        )
    if not source_table and not source_collection:
        return SourceDuplicateProbeResult(
            status="skipped_no_source",
            message="No source table/collection for uniqueness probe",
        )
    if not source_connector_id and not source_config:
        return SourceDuplicateProbeResult(
            status="skipped_no_source",
            message="No source connector id or inline config for uniqueness probe",
        )

    cfg: dict[str, Any] | None = None
    db_type = ""
    try:
        if source_connector_id:
            from services.connector_store import get_connector

            conn = get_connector(source_connector_id, workspace_id=workspace_id)
            if conn:
                from services.connector_probe import probe_cfg_from_saved

                cfg = probe_cfg_from_saved(conn)
                db_type = (conn.type or "").lower()

        if cfg is None and source_config:
            cfg = dict(source_config)
            db_type = (
                cfg.get("type")
                or cfg.get("db_type")
                or cfg.get("format")
                or ""
            ).lower()

        if not cfg:
            return SourceDuplicateProbeResult(
                status="skipped_no_source",
                message="Source connector could not be loaded for uniqueness probe",
                db_type=db_type,
            )

        # Inline endpoint configs use "format" for the database type; normalize to
        # "type" so the generic_sql engine builder can build a URL.
        if db_type:
            cfg = dict(cfg)
            cfg.setdefault("type", db_type)

        if db_type in ("mongodb", "mongodb_atlas"):
            coll = source_collection or source_table
            if not coll:
                return SourceDuplicateProbeResult(
                    status="skipped_no_source",
                    message="Mongo source missing collection for uniqueness probe",
                    db_type=db_type,
                )
            findings = _mongo_duplicates(cfg, coll, primary_key, limit=limit)
            return SourceDuplicateProbeResult(
                findings=findings,
                status="ran",
                message=f"Mongo uniqueness probe on {coll}.{primary_key}",
                db_type=db_type,
            )

        if db_type not in SQLISH_SOURCE_TYPES:
            msg = f"Duplicate-key probe not implemented for source type {db_type or 'unknown'}"
            logger.debug(msg)
            return SourceDuplicateProbeResult(
                status="skipped_unsupported",
                message=msg,
                db_type=db_type,
            )

        table = source_table or source_collection
        if not table:
            return SourceDuplicateProbeResult(
                status="skipped_no_source",
                message="SQL source missing table for uniqueness probe",
                db_type=db_type,
            )
        findings = _sql_duplicates(cfg, table, primary_key, limit=limit)
        return SourceDuplicateProbeResult(
            findings=findings,
            status="ran",
            message=f"SQL uniqueness probe on {table}.{primary_key}",
            db_type=db_type,
        )
    except Exception as exc:
        logger.warning("Source duplicate-key probe failed: %s", exc, exc_info=exc)
        return SourceDuplicateProbeResult(
            status="error",
            message=f"Source uniqueness probe failed: {exc}"[:400],
            db_type=db_type,
        )


def probe_source_duplicate_keys(
    *,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    source_collection: str = "",
    primary_key: str = "",
    workspace_id: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Return findings only — prefer :func:`probe_source_duplicate_keys_result` for SSOT.

    Empty list is ambiguous (clean vs skipped). New callers must use the result API.
    """
    return probe_source_duplicate_keys_result(
        source_connector_id=source_connector_id,
        source_config=source_config,
        source_table=source_table,
        source_collection=source_collection,
        primary_key=primary_key,
        workspace_id=workspace_id,
        limit=limit,
    ).findings
