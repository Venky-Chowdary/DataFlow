"""Source-side duplicate-key probe for preflight.

Database sources can be larger than the sample used by G9, so a 100-row sample
may miss duplicates that later cause a write-batch failure. This module runs a
 cheap source-side query (SQL GROUP BY or MongoDB aggregation) to find repeated
values for the resolved identity key before the transfer is approved.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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
    """Return up to ``limit`` duplicate values for ``primary_key`` in the source.

    Accepts either a saved connector id or an inline source config. Inline configs
    are used when the UI sends the endpoint details directly (no saved connector).
    Returns an empty list when the source is not a database, the connector cannot
    be loaded, or the probe fails (probe failures are logged, not raised, so a
    transient source hiccup does not block validation).
    """
    if not primary_key or (not source_table and not source_collection):
        return []
    if not source_connector_id and not source_config:
        return []

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
            db_type = (cfg.get("type") or cfg.get("db_type") or cfg.get("format") or "").lower()

        if not cfg:
            return []

        # Inline endpoint configs use "format" for the database type; normalize to
        # "type" so the generic_sql engine builder can build a URL.
        if db_type:
            cfg = dict(cfg)
            cfg.setdefault("type", db_type)

        if db_type == "mongodb":
            coll = source_collection or source_table
            if not coll:
                return []
            return _mongo_duplicates(cfg, coll, primary_key, limit=limit)

        # SQL path covers generic_sql and all SQL-like catalog IDs.
        sqlish = {
            "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb", "supabase",
            "mysql", "mariadb", "singlestore",
            "sqlserver", "mssql", "synapse", "azure_sql_database",
            "oracle", "db2", "sqlite", "duckdb", "generic_sql", "h2",
            "snowflake", "bigquery", "databricks", "clickhouse", "trino", "presto", "questdb",
        }
        if db_type not in sqlish:
            logger.debug("Duplicate-key probe not implemented for source type %s", db_type)
            return []

        table = source_table or source_collection
        if not table:
            return []
        return _sql_duplicates(cfg, table, primary_key, limit=limit)
    except Exception as exc:
        logger.warning("Source duplicate-key probe failed: %s", exc, exc_info=exc)
        return []
