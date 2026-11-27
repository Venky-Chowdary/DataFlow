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
import secrets
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

_LOCK = threading.Lock()
_RUNS: OrderedDict[str, dict[str, Any]] = OrderedDict()
_MAX = 50
_TTL_SECONDS = 600
# Bump when the gate set changes so a staged result from the previous
# plan cannot be reused by an execute that checks something new.
PREFLIGHT_PLAN_VERSION = 3
_BOUND: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "df_staged_preflight", default=None
)
logger = logging.getLogger(__name__)


_SECRET_FIELDS = frozenset({
    "password", "connection_string", "api_key", "private_key", "service_account",
    "secret_access_key", "access_key_secret", "token", "refresh_token", "client_secret",
})
_SAMPLE_FIELDS = frozenset({
    "sample", "samples", "sample_rows", "sample_values", "sample_values_by_source",
})
# Staged runs live in this process only, so the key that seals secrets in a
# fingerprint is drawn per process, never a constant in the source (D308).
_FP_KEY = secrets.token_bytes(32)


def _keyed_secret(value: str) -> str:
    """A secret changes the fingerprint without being copied into the payload."""
    import hmac

    return hmac.new(_FP_KEY, value.encode("utf-8"), hashlib.sha256).hexdigest()


def _seal(value: Any, key_name: str = "") -> Any:
    if key_name.lower() in _SAMPLE_FIELDS:
        return None
    if key_name.lower() in _SECRET_FIELDS and isinstance(value, str):
        return _keyed_secret(value) if value else ""
    if isinstance(value, dict):
        return {
            str(key): _seal(item, str(key))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _SAMPLE_FIELDS
        }
    if isinstance(value, list):
        return [_seal(item) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return None
    return value


def _canonical_mappings(mappings: Any) -> list[Any]:
    rows = []
    for item in mappings or []:
        if not isinstance(item, dict):
            continue
        # Every mapping field (target_type, casts, contracts), not only
        # source/target/transform. Sample rows on a mapping are not an input.
        rows.append(_seal(item))
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True, default=str))
    return rows


def _endpoint_canonical(endpoint: Any) -> dict[str, Any]:
    if endpoint is None:
        return {}
    fields = (
        "kind", "format", "connector_id", "host", "port", "database", "schema",
        "table", "collection", "username", "password", "connection_string", "ssl",
        "warehouse", "auth_mode", "auth_source", "region", "endpoint_url",
        "path_style", "extra",
    )
    raw = {name: getattr(endpoint, name, "") for name in fields}
    raw["port"] = int(raw.get("port") or 0)
    raw["ssl"] = bool(raw.get("ssl"))
    raw["path_style"] = bool(raw.get("path_style"))
    return _seal(raw)


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
        "source_extra": _seal(parts.get("source_extra") or {}),
        "source_host": str(parts.get("source_host") or ""),
        "source_port": int(parts.get("source_port") or 0),
        "source_ssl": bool(parts.get("source_ssl")),
        "source_username": str(parts.get("source_username") or ""),
        "source_password": _keyed_secret(str(parts.get("source_password") or "")) if parts.get("source_password") else "",
        "source_connection_string": _keyed_secret(str(parts.get("source_connection_string") or "")) if parts.get("source_connection_string") else "",
        "source_filename": str(parts.get("source_filename") or ""),
        "source_path": str(parts.get("source_path") or ""),
        "source_object_uri": str(parts.get("source_object_uri") or ""),
        "destination_connector_id": str(parts.get("destination_connector_id") or ""),
        "destination_table": str(parts.get("destination_table") or ""),
        "destination_schema": str(parts.get("destination_schema") or ""),
        "destination_database": str(parts.get("destination_database") or ""),
        "destination_db_type": str(parts.get("destination_db_type") or "").strip().lower(),
        "destination_extra": _seal(parts.get("destination_extra") or {}),
        "destination_host": str(parts.get("destination_host") or ""),
        "destination_port": int(parts.get("destination_port") or 0),
        "destination_ssl": bool(parts.get("destination_ssl")),
        "destination_username": str(parts.get("destination_username") or ""),
        "destination_password": _keyed_secret(str(parts.get("destination_password") or "")) if parts.get("destination_password") else "",
        "destination_connection_string": _keyed_secret(str(parts.get("destination_connection_string") or "")) if parts.get("destination_connection_string") else "",
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
        "session_zone_acknowledged": bool(parts.get("session_zone_acknowledged")),
        "session_zone_ack_actor": str(parts.get("session_zone_ack_actor") or ""),
        "session_zone_ack_reason": str(parts.get("session_zone_ack_reason") or ""),
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
        "transform_recipe_id": str(parts.get("transform_recipe_id") or ""),
        "transform_recipe_version": int(parts.get("transform_recipe_version") or 0),
        "transform_recipe_params": _seal(parts.get("transform_recipe_params") or {}),
        "document_model": _seal(parts.get("document_model") or {}),
        "data_region": str(parts.get("data_region") or ""),
        "historical_resync": bool(parts.get("historical_resync")),
        "source_timezone": str(parts.get("source_timezone") or ""),
        "transform_recipe_version_mode": str(parts.get("transform_recipe_version_mode") or ""),
        # Every dial field of each endpoint (region, endpoint_url, warehouse,
        # auth, collection...), sealed: an input left out could change a gate
        # without changing the hash (D308).
        "source_endpoint": parts.get("source_endpoint") or {},
        "destination_endpoint": parts.get("destination_endpoint") or {},
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
    """Reuse only when staging stored a schema version and Confirm re-read the same one.

    An empty stored version, or a confirm-time read that failed (``unread``
    or missing), does not reuse (D308).
    """
    if not stored or stored == "unread":
        return False
    if not fresh or fresh == "unread":
        return False
    return stored == fresh


@contextmanager
def bind_staged_preflight(
    run_id: str,
    fingerprint: str = "",
    schema_version: str | None | Callable[[], str | None] = None,
) -> Iterator[None]:
    """Bind the staged run only when it is the same check and still fresh.

    A different fingerprint, a result older than ten minutes, or a destination
    schema that no longer matches the one staging read all skip reuse. Skipping
    runs the gates. It does not reuse a check of different inputs.

    ``schema_version`` may be a callable: the destination is then read only
    when a staged run with this fingerprint exists. A run with nothing staged
    does not dial the destination here (D19: that dial was unbounded and froze
    a job on a silent destination before its connect deadline started).
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
    if row is not None:
        fresh = schema_version() if callable(schema_version) else schema_version
        if not _schema_still_current(str(row.get("schema_version") or ""), fresh):
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
    with _LOCK:
        row = _RUNS.get(rid)
        stored = str((row or {}).get("schema_version") or "")
    if row is None:
        return
    # Staging already read the destination: a second read is a second login
    # for the same answer (D308). Only a staging that stored none reads here,
    # inside the staging tool call's own deadline.
    version = "" if stored else destination_schema_version(
        getattr(request, "destination", None)
    )
    with _LOCK:
        row = _RUNS.get(rid)
        if not row:
            return
        row["fingerprint"] = request_preflight_fingerprint(request)
        row["saved_at"] = time.time()
        if version:
            row["schema_version"] = version


def bounded_destination_schema_version(dest: Any) -> str:
    """:func:`destination_schema_version` under the destination connect deadline.

    A destination that accepts the connection and never answers ends this
    read at the deadline (R10) and yields ``unread``, which never reuses a
    staged result: the gates then run and fail on their own bounded probe.
    """
    from services.connector_executor import get_executor
    from services.workspace_filter import current_workspace_stamp
    from src.ai.copilot.tool_deadline import _Progress, deadline_scope, interrupt_connections
    from src.transfer.engine_destination import _destination_connect_deadline

    seconds = _destination_connect_deadline()
    progress = _Progress(limit=seconds)

    def _run() -> str:
        with deadline_scope(seconds, progress):
            return destination_schema_version(dest)

    try:
        return str(get_executor().run(
            "destination_connect",
            _run,
            workspace=current_workspace_stamp(for_write=False),
            seconds=seconds,
            describe_timeout=lambda: "destination schema version timed out",
            label=lambda: "destination_connect: destination schema version",
            interrupt=lambda: interrupt_connections(progress),
        ) or "")
    except Exception:  # noqa: BLE001 - a timeout or refusal only means "do not reuse"
        logger.debug("destination schema version was not read in time", exc_info=True)
        return "unread"


def _resolved_view(endpoint: Any, workspace_id: str) -> Any:
    """The endpoint as a run dials it now, whichever side is hashing.

    Staging hashes the approval (a connector id and a table); Confirm hashes
    the job's request after its saved connectors were resolved (host, port,
    credentials, TLS, stamped extras). Hashing those two shapes never matched,
    so a staged preflight was never reused (D308 AC-2). Both sides now hash the
    saved connector's *current* resolution of the job's own choices (table,
    schema, an explicit database override): ``refresh_saved_endpoint`` strips
    what an earlier resolution filled in and resolves again. A connector edited
    between staging and Confirm (host, credentials, TLS) resolves differently,
    so the hash misses and the gates run. Secrets are HMAC-sealed, never kept.
    A connector that cannot be resolved hashes as given, which never matches.
    """
    if endpoint is None or not str(getattr(endpoint, "connector_id", "") or "").strip():
        return endpoint
    try:
        from src.transfer.adapters import refresh_saved_endpoint

        return refresh_saved_endpoint(copy.deepcopy(endpoint), workspace_id=workspace_id or None)
    except Exception:  # noqa: BLE001 - an unresolvable connector only means "no reuse"
        logger.debug("staged preflight endpoint was not resolved", exc_info=True)
        return endpoint


def _requested_delivery(request: Any) -> str:
    """The guarantee the operator asked for; an unpinned request is ``auto``.

    The engine later replaces ``auto`` with the route's selected class, so the
    selected value is not an input of the check staging ran.
    """
    from services.cdc_exactly_once import operator_pinned_delivery

    raw = str(getattr(request, "delivery_guarantee", "") or "").strip().lower()
    return raw if operator_pinned_delivery(raw) else "auto"


def request_preflight_fingerprint(request: Any) -> str:
    workspace = str(getattr(request, "workspace_id", "") or "")
    source = _resolved_view(getattr(request, "source", None), workspace)
    dest = _resolved_view(getattr(request, "destination", None), workspace)
    return preflight_input_fingerprint(
        source_connector_id=getattr(source, "connector_id", "") or "",
        source_table=getattr(source, "table", "") or getattr(source, "collection", "") or "",
        source_schema=getattr(source, "schema", "") or "",
        source_database=getattr(source, "database", "") or "",
        source_format=getattr(source, "format", "") or "",
        source_extra=getattr(source, "extra", None) or {},
        source_host=getattr(source, "host", "") or "",
        source_port=getattr(source, "port", 0) or 0,
        source_ssl=getattr(source, "ssl", False),
        source_username=getattr(source, "username", "") or "",
        source_password=getattr(source, "password", "") or "",
        source_connection_string=getattr(source, "connection_string", "") or "",
        source_filename=getattr(request, "source_filename", "") or "",
        source_path=getattr(request, "source_path", "") or "",
        source_object_uri=getattr(request, "source_object_uri", "") or "",
        destination_connector_id=getattr(dest, "connector_id", "") or "",
        destination_table=getattr(dest, "table", "") or getattr(dest, "collection", "") or "",
        destination_schema=getattr(dest, "schema", "") or "",
        destination_database=getattr(dest, "database", "") or "",
        destination_db_type=getattr(dest, "format", "") or "",
        destination_extra=getattr(dest, "extra", None) or {},
        destination_host=getattr(dest, "host", "") or "",
        destination_port=getattr(dest, "port", 0) or 0,
        destination_ssl=getattr(dest, "ssl", False),
        destination_username=getattr(dest, "username", "") or "",
        destination_password=getattr(dest, "password", "") or "",
        destination_connection_string=getattr(dest, "connection_string", "") or "",
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
        session_zone_acknowledged=getattr(request, "session_zone_acknowledged", False),
        session_zone_ack_actor=getattr(request, "session_zone_ack_actor", "") or "",
        session_zone_ack_reason=getattr(request, "session_zone_ack_reason", "") or "",
        acknowledgment_actor=getattr(request, "acknowledgment_actor", "") or "",
        acknowledgment_reason=getattr(request, "acknowledgment_reason", "") or "",
        date_locale=getattr(request, "date_locale", "") or "",
        number_locale=getattr(request, "number_locale", "") or "",
        number_locale_assumed=getattr(request, "number_locale_assumed", False),
        delivery_guarantee=_requested_delivery(request),
        limit=getattr(request, "limit", 0) or 0,
        workspace_id=getattr(request, "workspace_id", "") or "",
        approved_shape_recipe_hash=getattr(request, "approved_shape_recipe_hash", "") or "",
        approved_ddl_identity_hash=getattr(request, "approved_ddl_identity_hash", "") or "",
        approved_decision_artifact_hash=getattr(
            request, "approved_decision_artifact_hash", ""
        )
        or "",
        preflight_scope=getattr(request, "preflight_scope", "") or "",
        transform_recipe_id=getattr(request, "transform_recipe_id", "") or "",
        transform_recipe_version=getattr(request, "transform_recipe_version", 0) or 0,
        transform_recipe_params=getattr(request, "transform_recipe_params", None) or {},
        document_model=getattr(request, "document_model", None) or {},
        data_region=getattr(request, "data_region", "") or "",
        historical_resync=getattr(request, "historical_resync", False),
        source_timezone=getattr(request, "source_timezone", "") or "",
        transform_recipe_version_mode=getattr(request, "transform_recipe_version_mode", "") or "",
        source_endpoint=_endpoint_canonical(source),
        destination_endpoint=_endpoint_canonical(dest),
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
    loaded = _connector_endpoint(connector_id) if not host and connector_id else None
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
    own_ssl = bool(dest.get("ssl") if isinstance(dest, dict) else getattr(dest, "ssl", False))
    own_cs = str(
        dest.get("connection_string") if isinstance(dest, dict)
        else getattr(dest, "connection_string", "") or ""
    )
    own_extra = (
        dest.get("extra") if isinstance(dest, dict) and isinstance(dest.get("extra"), dict)
        else (getattr(dest, "extra", None) or {})
    )
    cfg = {
        "type": kind,
        "host": host,
        "port": port or 0,
        "database": database,
        "username": username,
        "password": password,
        "schema": schema,
        "table": table,
        "ssl": own_ssl if own_ssl or not loaded else bool(loaded.get("ssl")),
        "connection_string": own_cs or (loaded.get("connection_string") if loaded else "") or "",
        "extra": own_extra or (loaded.get("extra") if loaded else {}) or {},
    }
    try:
        rows = _read_schema_snapshot(cfg)
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
        "ssl": bool(getattr(saved, "ssl", False)),
        "connection_string": str(getattr(saved, "connection_string", "") or ""),
        "extra": getattr(saved, "extra", None) or {},
    }


def _read_schema_snapshot(cfg: dict[str, Any]) -> dict[str, Any]:
    """Columns, keys, indexes, foreign keys, checks, defaults and identity.

    Inside staging's tool call the read borrows the session the preflight's
    own introspection opened, so it is not a second login per endpoint (D308).
    Outside one it dials the guarded engine, the path a transfer uses, so the
    egress guard and the connector's TLS settings apply.
    """
    from services.staged_schema_snapshot import read_schema_snapshot

    return read_schema_snapshot(cfg)
