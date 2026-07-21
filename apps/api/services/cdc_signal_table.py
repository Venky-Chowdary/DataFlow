"""Debezium-compatible CDC signal table (execute-snapshot / stop-snapshot).

Debezium connectors watch a signal table for operator commands. DataFlow mirrors
that contract:

  CREATE TABLE dataflow_signal (
    id   VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data TEXT
  );

Supported ``type`` values (case-insensitive):
  - execute-snapshot / incremental  → enqueue incremental snapshot signal
  - stop-snapshot / cancel          → cancel matching running/pending signals

``data`` is JSON, e.g. ``{"data-collections":["public.orders"],"type":"incremental"}``
or a plain table name.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TABLE = "dataflow_signal"
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def signal_table_name(cfg: dict[str, Any] | None = None) -> str:
    name = str((cfg or {}).get("signal_table") or DEFAULT_TABLE)
    if not _SAFE_IDENT.match(name):
        return DEFAULT_TABLE
    return name


def ensure_signal_table(conn, *, table: str = DEFAULT_TABLE, dialect: str = "postgresql") -> None:
    """Create the signal table if missing (idempotent)."""
    tbl = signal_table_name({"signal_table": table})
    if dialect in {"postgresql", "postgres"}:
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
          id VARCHAR(64) PRIMARY KEY,
          type VARCHAR(32) NOT NULL,
          data TEXT
        )
        """
    elif dialect in {"mysql", "mariadb"}:
        ddl = f"""
        CREATE TABLE IF NOT EXISTS `{tbl}` (
          id VARCHAR(64) PRIMARY KEY,
          type VARCHAR(32) NOT NULL,
          data TEXT
        )
        """
    else:
        raise ValueError(f"Unsupported signal-table dialect: {dialect}")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _parse_data(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    text = str(raw).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"table": str(parsed)}
    except Exception:
        return {"table": text}


def _collections(data: dict[str, Any]) -> list[str]:
    cols = data.get("data-collections") or data.get("data_collections") or data.get("collections")
    if isinstance(cols, list):
        return [str(c) for c in cols if c]
    table = data.get("table") or data.get("data-collection")
    if table:
        return [str(table)]
    return []


def apply_signal_row(
    *,
    source_key: str,
    signal_id: str,
    signal_type: str,
    data: Any,
    default_table: str = "",
    primary_key: str = "id",
) -> dict[str, Any] | None:
    """Map one signal-table row into DataFlow incremental snapshot APIs."""
    from services.cdc_incremental_snapshot import (
        cancel_signal,
        list_signals,
        request_incremental_snapshot,
    )

    stype = str(signal_type or "").strip().lower().replace("_", "-")
    payload = _parse_data(data)
    tables = _collections(payload) or ([default_table] if default_table else [])
    pk = str(payload.get("primary_key") or payload.get("pk") or primary_key or "id")
    chunk = int(payload.get("chunk_size") or payload.get("chunk-size") or 1000)

    if stype in {"execute-snapshot", "incremental", "execute-incremental-snapshot"}:
        created = []
        for table in tables:
            # Strip schema prefix for claim matching (connectors use bare table names).
            bare = table.split(".")[-1] if "." in table else table
            sig = request_incremental_snapshot(
                source_key,
                bare,
                primary_key=pk,
                chunk_size=chunk,
            )
            created.append(sig.to_dict())
        return {"action": "execute-snapshot", "signal_id": signal_id, "created": created}

    if stype in {"stop-snapshot", "cancel", "stop-incremental-snapshot"}:
        cancelled = []
        targets = {t.split(".")[-1] if "." in t else t for t in tables} if tables else set()
        for sig in list_signals(source_key):
            if sig.status not in {"pending", "running"}:
                continue
            if targets and sig.table not in targets:
                continue
            out = cancel_signal(sig.id)
            if out:
                cancelled.append(out.to_dict())
        return {"action": "stop-snapshot", "signal_id": signal_id, "cancelled": cancelled}

    logger.debug("Ignoring unknown CDC signal type %s id=%s", stype, signal_id)
    return None


def poll_signal_table(
    conn,
    *,
    source_key: str,
    table: str = DEFAULT_TABLE,
    default_table: str = "",
    primary_key: str = "id",
    processed_ids: set[str] | None = None,
    dialect: str = "postgresql",
    limit: int = 50,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Read new signal rows, apply them, return (results, updated processed id set).

    Rows are left in the table (Debezium-compatible); callers track processed ids
    in connector memory / watermark metadata to avoid re-applying.
    """
    tbl = signal_table_name({"signal_table": table})
    seen = set(processed_ids or ())
    results: list[dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            if dialect in {"mysql", "mariadb"}:
                cur.execute(f"SELECT id, type, data FROM `{tbl}` ORDER BY id LIMIT %s", (limit,))
            else:
                cur.execute(f"SELECT id, type, data FROM {tbl} ORDER BY id LIMIT %s", (limit,))
            rows = cur.fetchall() or []
    except Exception as exc:
        logger.debug("CDC signal table poll skipped: %s", exc)
        return results, seen

    for row in rows:
        sid = str(row[0] or "")
        if not sid or sid in seen:
            continue
        applied = apply_signal_row(
            source_key=source_key,
            signal_id=sid,
            signal_type=str(row[1] or ""),
            data=row[2],
            default_table=default_table,
            primary_key=primary_key,
        )
        seen.add(sid)
        if applied:
            results.append(applied)
    return results, seen


def poll_mongo_signal_collection(
    db: Any,
    *,
    source_key: str,
    collection: str = DEFAULT_TABLE,
    default_table: str = "",
    primary_key: str = "_id",
    processed_ids: set[str] | None = None,
    limit: int = 50,
    ensure_index: bool = False,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Read Debezium-style signal docs from a MongoDB collection."""
    seen = set(processed_ids or ())
    results: list[dict[str, Any]] = []
    try:
        coll = db[collection]
        if ensure_index:
            try:
                coll.create_index("id", unique=True, sparse=True)
            except Exception:
                pass
        cursor = coll.find({}).sort("id", 1).limit(limit)
        for doc in cursor:
            sid = str(doc.get("id") or doc.get("_id") or "")
            if not sid or sid in seen:
                continue
            applied = apply_signal_row(
                source_key=source_key,
                signal_id=sid,
                signal_type=str(doc.get("type") or ""),
                data=doc.get("data"),
                default_table=default_table,
                primary_key=primary_key,
            )
            seen.add(sid)
            if applied:
                results.append(applied)
    except Exception as exc:
        logger.debug("Mongo CDC signal collection poll skipped: %s", exc)
    return results, seen
