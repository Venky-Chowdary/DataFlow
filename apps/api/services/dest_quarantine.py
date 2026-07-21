"""Destination-side quarantine / dead-letter table.

Honesty
-------
Control-plane JSONL + job ``rejected_details`` remain the audit index.
This module **also** writes rejected findings into a real destination table
(``{table}_df_quarantine``) so operators can query / promote with SQL tools.

Promote reuses the existing quarantine replay writer path, then stamps
``_df_promoted_at`` on DLQ rows. Delivery of promoted rows is still
at-least-once upsert into the primary table — not exactly-once.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Destinations that can host a durable DLQ table via the SQL writer path.
DEST_DLQ_SUPPORTED = frozenset({
    "sqlite",
    "postgresql",
    "postgres",
    "mysql",
    "sqlserver",
    "mssql",
    "oracle",
    "snowflake",
    "redshift",
    "generic_sql",
})

META_COLUMNS = (
    "_df_qid",
    "_df_job_id",
    "_df_row",
    "_df_column",
    "_df_target",
    "_df_value",
    "_df_reason",
    "_df_policy",
    "_df_payload",
    "_df_created_at",
    "_df_promoted_at",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def dest_supports_dlq_table(dest_type: str) -> bool:
    from src.transfer.connector_capabilities import resolve_driver_type

    driver = resolve_driver_type(dest_type or "")
    return driver in DEST_DLQ_SUPPORTED or (dest_type or "").lower() in DEST_DLQ_SUPPORTED


def dlq_table_name(dest_table: str | None) -> str:
    """Stable per-target DLQ table name (not per-job — queryable across runs).

    Uses a single underscore separator — some SQL writers sanitize ``__``.
    """
    base = re.sub(r"[^a-zA-Z0-9_]", "_", (dest_table or "import").strip()) or "import"
    base = re.sub(r"_+", "_", base).strip("_")[:48] or "import"
    name = f"{base}_df_quarantine"
    return name[:63]  # PG identifier safety


def dlq_endpoint(destination: Any, *, dest_table: str | None = None) -> Any:
    """Clone destination endpoint pointing at the DLQ table/collection."""
    table = dest_table or getattr(destination, "table", None) or getattr(destination, "collection", None) or "import"
    dlq = dlq_table_name(str(table))
    return replace(destination, table=dlq, collection=dlq)


def rejected_details_to_dlq_records(
    details: list[dict[str, Any]],
    *,
    job_id: str,
) -> list[dict[str, str]]:
    """Map writer ``rejected_details`` into DLQ table rows (string cells)."""
    rows: list[dict[str, str]] = []
    created = _now()
    for detail in details or []:
        if not isinstance(detail, dict):
            continue
        payload = detail.get("values") if isinstance(detail.get("values"), dict) else {}
        qid = str(detail.get("_df_qid") or uuid.uuid4())
        detail["_df_qid"] = qid  # stamp for later promote
        rows.append({
            "_df_qid": qid,
            "_df_job_id": str(job_id or ""),
            "_df_row": str(detail.get("row") if detail.get("row") is not None else ""),
            "_df_column": str(detail.get("column") or ""),
            "_df_target": str(detail.get("target") or ""),
            "_df_value": "" if detail.get("value") is None else str(detail.get("value")),
            "_df_reason": str(detail.get("reason") or ""),
            "_df_policy": str(detail.get("policy") or ""),
            "_df_payload": json.dumps(
                {str(k): "" if v is None else str(v) for k, v in payload.items()},
                ensure_ascii=False,
            ),
            "_df_created_at": created,
            "_df_promoted_at": "",
        })
    return rows


def write_dest_quarantine(
    destination: Any,
    details: list[dict[str, Any]],
    *,
    job_id: str,
) -> dict[str, Any]:
    """Persist rejected findings into ``{table}_df_quarantine`` on the destination.

    Returns a summary dict for ``destination_summary``. Skips (with reason) when
    the destination cannot host tables — control-plane JSONL still holds the audit.
    """
    from src.transfer.adapters import write_destination_database
    from src.transfer.connector_capabilities import resolve_driver_type

    dest_type = resolve_driver_type(getattr(destination, "format", "") or "")
    if not dest_supports_dlq_table(dest_type):
        return {
            "ok": False,
            "skipped": True,
            "reason": f"destination '{dest_type or 'unknown'}' cannot host a DLQ table",
            "rows_written": 0,
        }
    if getattr(destination, "kind", "") == "file_export":
        return {
            "ok": False,
            "skipped": True,
            "reason": "file_export has no destination table",
            "rows_written": 0,
        }

    records = rejected_details_to_dlq_records(details, job_id=job_id)
    if not records:
        return {"ok": True, "skipped": True, "reason": "empty", "rows_written": 0}

    endpoint = dlq_endpoint(destination)
    columns = list(META_COLUMNS)
    schema = {c: "string" for c in columns}
    mappings = [{"source": c, "target": c, "confidence": 1.0} for c in columns]

    rows_written, ddl_log, summary = write_destination_database(
        endpoint,
        records,
        columns,
        schema,
        mappings,
        validation_mode="balanced",
        write_mode="insert",
        job_id=f"{job_id}_dlq" if job_id else None,
    )
    table = endpoint.table or dlq_table_name("import")
    return {
        "ok": True,
        "skipped": False,
        "rows_written": int(rows_written or 0),
        "table": table,
        "ddl": list(ddl_log or [])[:20],
        "writer_summary": {
            k: summary.get(k)
            for k in ("rejected_rows", "ok")
            if isinstance(summary, dict) and k in summary
        },
    }


def _sqlite_db_path(destination: Any) -> str:
    from connectors.sqlite_common import sqlite_file_path

    return sqlite_file_path(
        getattr(destination, "database", "") or "",
        getattr(destination, "connection_string", "") or "",
        getattr(destination, "host", "") or "",
    )


def _qualified_dlq_table(cfg: dict[str, Any], table: str) -> str:
    from connectors.generic_sql import get_sql_schema
    from connectors.writer_common import quote_sql_identifier

    schema_name = get_sql_schema(cfg) or ""
    table_q = quote_sql_identifier(table)
    if schema_name:
        return f"{quote_sql_identifier(schema_name)}.{table_q}"
    return table_q


def mark_dlq_promoted(
    destination: Any,
    *,
    qids: list[str],
    job_id: str = "",
) -> dict[str, Any]:
    """Stamp ``_df_promoted_at`` on DLQ rows after a successful promote/replay.

    Prefer explicit ``qids``. When empty but ``job_id`` is set, stamp all open
    rows for that job (Studio replay may omit qids on older job payloads).
    """
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type
    from connectors.writer_common import quote_sql_identifier

    dest_type = resolve_driver_type(getattr(destination, "format", "") or "")
    if not dest_supports_dlq_table(dest_type):
        return {"updated": 0, "skipped": True}

    endpoint = dlq_endpoint(destination)
    table = endpoint.table or dlq_table_name("import")
    promoted_at = _now()
    ids = [str(q) for q in qids if str(q).strip()][:500]
    by_job = bool(job_id) and not ids
    if not ids and not by_job:
        return {"updated": 0}

    try:
        if dest_type == "sqlite":
            import sqlite3

            path = _sqlite_db_path(destination)
            if not path:
                return {"updated": 0, "error": "sqlite path unresolved", "table": table}
            if by_job:
                sql = (
                    f'UPDATE "{table}" SET "_df_promoted_at" = ? '
                    f'WHERE "_df_job_id" = ? '
                    f'AND ("_df_promoted_at" IS NULL OR "_df_promoted_at" = \'\')'
                )
                params: list[Any] = [promoted_at, job_id]
            else:
                placeholders = ", ".join("?" for _ in ids)
                sql = (
                    f'UPDATE "{table}" SET "_df_promoted_at" = ? '
                    f'WHERE "_df_qid" IN ({placeholders})'
                )
                params = [promoted_at, *ids]
                if job_id:
                    sql += ' AND "_df_job_id" = ?'
                    params.append(job_id)
            with sqlite3.connect(path, timeout=8) as conn:
                cur = conn.execute(sql, params)
                updated = int(cur.rowcount or 0)
                conn.commit()
            return {"updated": updated, "table": table, "promoted_at": promoted_at}

        import sqlalchemy as sa
        from connectors.generic_sql import get_sqlalchemy_engine

        cfg = resolve_connector_config(destination)
        qualified = _qualified_dlq_table(cfg, table)
        engine = get_sqlalchemy_engine(cfg)
        params_sa: dict[str, Any] = {"promoted_at": promoted_at}
        if by_job:
            sql = (
                f"UPDATE {qualified} SET {quote_sql_identifier('_df_promoted_at')} = :promoted_at "
                f"WHERE {quote_sql_identifier('_df_job_id')} = :job_id "
                f"AND ({quote_sql_identifier('_df_promoted_at')} IS NULL "
                f"OR {quote_sql_identifier('_df_promoted_at')} = '')"
            )
            params_sa["job_id"] = job_id
        else:
            placeholders = ", ".join(f":id{i}" for i in range(len(ids)))
            params_sa.update({f"id{i}": ids[i] for i in range(len(ids))})
            sql = (
                f"UPDATE {qualified} SET {quote_sql_identifier('_df_promoted_at')} = :promoted_at "
                f"WHERE {quote_sql_identifier('_df_qid')} IN ({placeholders})"
            )
            if job_id:
                sql += f" AND {quote_sql_identifier('_df_job_id')} = :job_id"
                params_sa["job_id"] = job_id
        with engine.begin() as conn:
            result = conn.execute(sa.text(sql), params_sa)
            updated = int(result.rowcount or 0)
        return {"updated": updated, "table": table, "promoted_at": promoted_at}
    except Exception as exc:
        logger.warning("mark_dlq_promoted failed: %s", exc)
        return {"updated": 0, "error": str(exc)[:300], "table": table}


def count_open_dlq_rows(destination: Any, *, job_id: str = "") -> dict[str, Any]:
    """Count unpromoted DLQ rows (optional job filter) for Studio badges."""
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type
    from connectors.writer_common import quote_sql_identifier

    dest_type = resolve_driver_type(getattr(destination, "format", "") or "")
    if not dest_supports_dlq_table(dest_type):
        return {"supported": False, "open_rows": 0}

    endpoint = dlq_endpoint(destination)
    table = endpoint.table or dlq_table_name("import")
    try:
        if dest_type == "sqlite":
            import sqlite3

            path = _sqlite_db_path(destination)
            if not path:
                return {"supported": True, "open_rows": 0, "table": table, "error": "sqlite path unresolved"}
            sql = (
                f'SELECT COUNT(*) FROM "{table}" '
                f'WHERE ("_df_promoted_at" IS NULL OR "_df_promoted_at" = \'\')'
            )
            params: list[Any] = []
            if job_id:
                sql += ' AND "_df_job_id" = ?'
                params.append(job_id)
            with sqlite3.connect(path, timeout=8) as conn:
                row = conn.execute(sql, params).fetchone()
                n = int(row[0] or 0) if row else 0
            return {"supported": True, "open_rows": n, "table": table}

        import sqlalchemy as sa
        from connectors.generic_sql import get_sqlalchemy_engine

        cfg = resolve_connector_config(destination)
        qualified = _qualified_dlq_table(cfg, table)
        engine = get_sqlalchemy_engine(cfg)
        sql = (
            f"SELECT COUNT(*) FROM {qualified} "
            f"WHERE ({quote_sql_identifier('_df_promoted_at')} IS NULL "
            f"OR {quote_sql_identifier('_df_promoted_at')} = '')"
        )
        params_sa: dict[str, Any] = {}
        if job_id:
            sql += f" AND {quote_sql_identifier('_df_job_id')} = :job_id"
            params_sa["job_id"] = job_id
        with engine.connect() as conn:
            row = conn.execute(sa.text(sql), params_sa).fetchone()
            n = int(row[0] or 0) if row else 0
        return {"supported": True, "open_rows": n, "table": table}
    except Exception as exc:
        # Table may not exist yet — not an error for the operator.
        logger.debug("count_open_dlq_rows: %s", exc)
        return {"supported": True, "open_rows": 0, "table": table, "error": str(exc)[:200]}
