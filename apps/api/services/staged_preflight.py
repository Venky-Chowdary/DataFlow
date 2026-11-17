"""Confirm reuses the preflight staging already ran (D308).

Staging opens the source once and saves the gate result under a run id.
Confirm carries that id. Reuse happens only when the normalized request
Confirm will execute hashes to the request staging checked, the staged
result is younger than ten minutes, and a fresh read of the destination
schema still matches the schema staging read. Anything else runs the gates
again. Plan and execute hash the same fields, so they cannot disagree about
what was checked.
"""

from __future__ import annotations

import contextvars
import copy
import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

_LOCK = threading.Lock()
_RUNS: OrderedDict[str, dict[str, Any]] = OrderedDict()
_MAX = 50
_TTL_SECONDS = 600
# Bump when the gate set changes so a staged result from the previous
# plan cannot be reused by an execute that checks something new.
PREFLIGHT_PLAN_VERSION = 2
_BOUND: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "df_staged_preflight", default=None
)
logger = logging.getLogger(__name__)


def _canonical_mappings(mappings: Any) -> list[dict[str, str]]:
    rows = []
    for item in mappings or []:
        if not isinstance(item, dict):
            continue
        rows.append({
            "source": str(item.get("source") or ""),
            "target": str(item.get("target") or item.get("source") or ""),
            "transform": str(item.get("transform") or ""),
        })
    rows.sort(key=lambda row: (row["source"], row["target"], row["transform"]))
    return rows


def _canonical_extra(extra: Any) -> dict[str, Any]:
    if not isinstance(extra, dict):
        return {}
    return {str(key): extra[key] for key in sorted(extra, key=str)}


def preflight_input_fingerprint(**parts: Any) -> str:
    """Identity of a preflight. Sample rows and row counts are not part of it.

    Confirm has loaded the batch staging only sampled. Those differ on every
    run and must not force a second catalog walk. Everything that changes a
    gate is in the hash: destination schema, database and extra, duplicate
    policy, backfill, reversibility, every acknowledgment, contracts, locales,
    and the plan version. A field left out of an older fingerprint does not
    match this one, so that staged result is not reused.
    """
    payload = {
        "plan_version": PREFLIGHT_PLAN_VERSION,
        "source_connector_id": str(parts.get("source_connector_id") or ""),
        "source_table": str(parts.get("source_table") or ""),
        "source_schema": str(parts.get("source_schema") or ""),
        "source_database": str(parts.get("source_database") or ""),
        "source_format": str(parts.get("source_format") or "").strip().lower(),
        "source_extra": _canonical_extra(parts.get("source_extra")),
        "destination_connector_id": str(parts.get("destination_connector_id") or ""),
        "destination_table": str(parts.get("destination_table") or ""),
        "destination_schema": str(parts.get("destination_schema") or ""),
        "destination_database": str(parts.get("destination_database") or ""),
        "destination_db_type": str(parts.get("destination_db_type") or "").strip().lower(),
        "destination_extra": _canonical_extra(parts.get("destination_extra")),
        "sync_mode": str(parts.get("sync_mode") or ""),
        "schema_policy": str(parts.get("schema_policy") or ""),
        "validation_mode": str(parts.get("validation_mode") or ""),
        "duplicate_policy": str(parts.get("duplicate_policy") or ""),
        "backfill_new_fields": bool(parts.get("backfill_new_fields")),
        "require_reversibility": bool(parts.get("require_reversibility")),
        "allow_duplicates": bool(parts.get("allow_duplicates")),
        "write_via_staging": bool(parts.get("write_via_staging")),
        "source_filter": parts.get("source_filter") or {},
        "shape_recipe": parts.get("shape_recipe") or {},
        "read_options": parts.get("read_options") or {},
        "mappings": _canonical_mappings(parts.get("mappings")),
        "column_types": parts.get("column_types") or {},
        "stream_contracts": parts.get("stream_contracts") or [],
        "contract_id": str(parts.get("contract_id") or ""),
        "require_signed_contract": bool(parts.get("require_signed_contract")),
        "enforce_contract": bool(parts.get("enforce_contract", True)),
        "compliance_acknowledged": bool(parts.get("compliance_acknowledged")),
        "schema_drift_acknowledged": bool(parts.get("schema_drift_acknowledged")),
        "fk_risk_acknowledged": bool(parts.get("fk_risk_acknowledged")),
        "ddl_changes_acknowledged": bool(parts.get("ddl_changes_acknowledged")),
        "empty_source_overwrite_acknowledged": bool(
            parts.get("empty_source_overwrite_acknowledged")
        ),
        "load_volume_acknowledged": bool(parts.get("load_volume_acknowledged")),
        "acknowledgment_actor": str(parts.get("acknowledgment_actor") or ""),
        "acknowledgment_reason": str(parts.get("acknowledgment_reason") or ""),
        "date_locale": str(parts.get("date_locale") or ""),
        "number_locale": str(parts.get("number_locale") or ""),
        "number_locale_assumed": bool(parts.get("number_locale_assumed")),
        "delivery_guarantee": str(parts.get("delivery_guarantee") or ""),
        "limit": int(parts.get("limit") or 0),
        "workspace_id": str(parts.get("workspace_id") or ""),
        "approved_shape_recipe_hash": str(parts.get("approved_shape_recipe_hash") or ""),
        "approved_ddl_identity_hash": str(parts.get("approved_ddl_identity_hash") or ""),
        "approved_decision_artifact_hash": str(
            parts.get("approved_decision_artifact_hash") or ""
        ),
        "preflight_scope": str(parts.get("preflight_scope") or ""),
    }
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def remember_staged_preflight(
    run_id: str,
    fingerprint: str,
    result: dict[str, Any],
    *,
    schema_version: str = "",
) -> None:
    rid = str(run_id or "").strip()
    if not rid or not isinstance(result, dict):
        return
    with _LOCK:
        _RUNS[rid] = {
            "fingerprint": fingerprint,
            "result": copy.deepcopy(result),
            "saved_at": time.time(),
            "schema_version": str(schema_version or ""),
        }
        _RUNS.move_to_end(rid)
        while len(_RUNS) > _MAX:
            _RUNS.popitem(last=False)


def staged_preflight_for_bound() -> dict[str, Any] | None:
    """The bound run, already checked against this request's inputs."""
    row = _BOUND.get()
    if not row:
        return None
    out = copy.deepcopy(row["result"])
    out["reused_staged_preflight"] = True
    return out


def _schema_still_current(stored: str, fresh: str | None) -> bool:
    """A recorded schema must be re-read and still match. ``unread`` never matches."""
    if not stored:
        return True
    if stored == "unread" or not fresh or fresh == "unread":
        return False
    return stored == fresh


@contextmanager
def bind_staged_preflight(
    run_id: str,
    fingerprint: str = "",
    schema_version: str | None = None,
) -> Iterator[None]:
    """Bind the staged run only when it is the same check and still fresh.

    A different fingerprint, a result older than ten minutes, or a destination
    schema that no longer matches the one staging read all skip reuse. Skipping
    runs the gates. It does not reuse a check of different inputs.
    """
    rid = str(run_id or "").strip()
    with _LOCK:
        row = _RUNS.get(rid) if rid else None
        if row is not None:
            row = dict(row)
    if row is not None and row.get("fingerprint") != fingerprint:
        row = None
    if row is not None and time.time() - float(row.get("saved_at") or 0) > _TTL_SECONDS:
        row = None
    if row is not None and not _schema_still_current(
        str(row.get("schema_version") or ""), schema_version
    ):
        row = None
    token = _BOUND.set(row)
    try:
        yield
    finally:
        _BOUND.reset(token)


def align_staged_preflight(run_id: str, request: Any) -> None:
    """Point the staged run at the request Confirm will execute.

    Plan hashes that request, not a narrower subset. A fingerprint left on
    the old subset does not match, so reuse is skipped.
    """
    rid = str(run_id or "").strip()
    if not rid:
        return
    version = destination_schema_version(getattr(request, "destination", None))
    with _LOCK:
        row = _RUNS.get(rid)
        if not row:
            return
        row["fingerprint"] = request_preflight_fingerprint(request)
        row["saved_at"] = time.time()
        if version:
            row["schema_version"] = version


def request_preflight_fingerprint(request: Any) -> str:
    source = getattr(request, "source", None)
    dest = getattr(request, "destination", None)
    return preflight_input_fingerprint(
        source_connector_id=getattr(source, "connector_id", "") or "",
        source_table=getattr(source, "table", "") or getattr(source, "collection", "") or "",
        source_schema=getattr(source, "schema", "") or "",
        source_database=getattr(source, "database", "") or "",
        source_format=getattr(source, "format", "") or "",
        source_extra=getattr(source, "extra", None) or {},
        destination_connector_id=getattr(dest, "connector_id", "") or "",
        destination_table=getattr(dest, "table", "") or getattr(dest, "collection", "") or "",
        destination_schema=getattr(dest, "schema", "") or "",
        destination_database=getattr(dest, "database", "") or "",
        destination_db_type=getattr(dest, "format", "") or "",
        destination_extra=getattr(dest, "extra", None) or {},
        mappings=getattr(request, "mappings", None) or [],
        column_types=getattr(request, "column_types", None) or {},
        sync_mode=getattr(request, "sync_mode", "") or "",
        schema_policy=getattr(request, "schema_policy", "") or "",
        validation_mode=getattr(request, "validation_mode", "") or "",
        duplicate_policy=getattr(request, "duplicate_policy", "") or "",
        backfill_new_fields=getattr(request, "backfill_new_fields", False),
        require_reversibility=getattr(request, "require_reversibility", False),
        allow_duplicates=getattr(request, "allow_duplicates", False),
        write_via_staging=getattr(request, "write_via_staging", False),
        source_filter=getattr(request, "source_filter", None) or {},
        shape_recipe=getattr(request, "shape_recipe", None) or {},
        read_options=getattr(request, "read_options", None) or {},
        stream_contracts=getattr(request, "stream_contracts", None) or [],
        contract_id=getattr(request, "contract_id", "") or "",
        require_signed_contract=getattr(request, "require_signed_contract", False),
        enforce_contract=getattr(request, "enforce_contract", True),
        compliance_acknowledged=getattr(request, "compliance_acknowledged", False),
        schema_drift_acknowledged=getattr(request, "schema_drift_acknowledged", False),
        fk_risk_acknowledged=getattr(request, "fk_risk_acknowledged", False),
        ddl_changes_acknowledged=getattr(request, "ddl_changes_acknowledged", False),
        empty_source_overwrite_acknowledged=getattr(
            request, "empty_source_overwrite_acknowledged", False
        ),
        load_volume_acknowledged=getattr(request, "load_volume_acknowledged", False),
        acknowledgment_actor=getattr(request, "acknowledgment_actor", "") or "",
        acknowledgment_reason=getattr(request, "acknowledgment_reason", "") or "",
        date_locale=getattr(request, "date_locale", "") or "",
        number_locale=getattr(request, "number_locale", "") or "",
        number_locale_assumed=getattr(request, "number_locale_assumed", False),
        delivery_guarantee=getattr(request, "delivery_guarantee", "") or "",
        limit=getattr(request, "limit", 0) or 0,
        workspace_id=getattr(request, "workspace_id", "") or "",
        approved_shape_recipe_hash=getattr(request, "approved_shape_recipe_hash", "") or "",
        approved_ddl_identity_hash=getattr(request, "approved_ddl_identity_hash", "") or "",
        approved_decision_artifact_hash=getattr(
            request, "approved_decision_artifact_hash", ""
        )
        or "",
        preflight_scope=getattr(request, "preflight_scope", "") or "",
    )


def destination_schema_version(dest: Any) -> str:
    """Hash of the destination table's columns, read now.

    ``""`` when this endpoint has no host to dial (a file, or a request whose
    connector has not been filled in). ``unread`` when a dial was required and
    failed: that result must not be reused. The hash covers name, type,
    length, collation and nullability, so a widen or a new column changes it.
    """
    if dest is None:
        return ""
    if isinstance(dest, dict):
        host = str(dest.get("host") or "")
        port = int(dest.get("port") or 0)
        database = str(dest.get("database") or "")
        username = str(dest.get("username") or "")
        password = str(dest.get("password") or "")
        schema = str(dest.get("schema") or "")
        table = str(dest.get("table") or dest.get("table_name") or "")
        kind = str(dest.get("type") or dest.get("format") or dest.get("db_type") or "").lower()
        connector_id = str(dest.get("id") or dest.get("connector_id") or "")
    else:
        host = str(getattr(dest, "host", "") or "")
        port = int(getattr(dest, "port", 0) or 0)
        database = str(getattr(dest, "database", "") or "")
        username = str(getattr(dest, "username", "") or "")
        password = str(getattr(dest, "password", "") or "")
        schema = str(getattr(dest, "schema", "") or "")
        table = str(getattr(dest, "table", "") or getattr(dest, "collection", "") or "")
        kind = str(getattr(dest, "format", "") or getattr(dest, "type", "") or "").lower()
        connector_id = str(getattr(dest, "connector_id", "") or "")
    if not host and connector_id:
        loaded = _connector_endpoint(connector_id)
        if loaded:
            host = host or loaded["host"]
            port = port or loaded["port"]
            database = database or loaded["database"]
            username = username or loaded["username"]
            password = password or loaded["password"]
            schema = schema or loaded["schema"]
            kind = kind or loaded["type"]
    if not host or not table:
        return ""
    try:
        rows = _read_column_rows(
            kind, host, port, database, username, password, schema, table
        )
    except Exception:
        logger.debug("destination schema version was not read", exc_info=True)
        return "unread"
    raw = json.dumps(rows, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _connector_endpoint(connector_id: str) -> dict[str, Any] | None:
    try:
        from services.connector_store import get_connector

        saved = get_connector(connector_id)
    except Exception:
        return None
    if saved is None:
        return None
    return {
        "host": str(getattr(saved, "host", "") or ""),
        "port": int(getattr(saved, "port", 0) or 0),
        "database": str(getattr(saved, "database", "") or ""),
        "username": str(getattr(saved, "username", "") or ""),
        "password": str(getattr(saved, "password", "") or ""),
        "schema": str(getattr(saved, "schema", "") or ""),
        "type": str(getattr(saved, "type", "") or "").lower(),
    }


def _read_column_rows(
    kind: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    table: str,
) -> list[tuple]:
    if kind in {"postgresql", "postgres"}:
        import psycopg2

        conn = psycopg2.connect(
            host=host, port=port or 5432, dbname=database, user=username,
            password=password, connect_timeout=2,
        )
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT column_name, data_type, character_maximum_length, "
                "numeric_precision, numeric_scale, collation_name, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema or "public", table),
            )
            return list(cur.fetchall())
        finally:
            conn.close()
    if kind in {"mysql", "mariadb"}:
        import pymysql

        conn = pymysql.connect(
            host=host, port=port or 3306, user=username, password=password,
            database=database, connect_timeout=2,
        )
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
                "NUMERIC_PRECISION, NUMERIC_SCALE, COLLATION_NAME, IS_NULLABLE "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
                "ORDER BY ORDINAL_POSITION",
                (table,),
            )
            return list(cur.fetchall())
        finally:
            conn.close()
    if kind in {"sqlserver", "mssql", "azure_sql"}:
        import pymssql

        conn = pymssql.connect(
            server=host, port=str(port or 1433), user=username, password=password,
            database=database, login_timeout=2, timeout=2,
        )
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT c.name, ty.name, c.max_length, c.precision, c.scale, "
                "c.collation_name, c.is_nullable "
                "FROM sys.columns c "
                "JOIN sys.types ty ON c.user_type_id = ty.user_type_id "
                "JOIN sys.tables t ON c.object_id = t.object_id "
                "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                "WHERE s.name = %s AND t.name = %s ORDER BY c.column_id",
                (schema or "dbo", table),
            )
            return list(cur.fetchall())
        finally:
            conn.close()
    raise RuntimeError(f"schema version is not read for {kind or 'this engine'}")
