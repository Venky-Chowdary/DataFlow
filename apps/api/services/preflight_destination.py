"""Destination-side preflight: live probe, staging capacity, destination shape.

Extracted from :mod:`services.preflight_service` (Phase F8 size freeze) with no
behaviour change. The destination is read from its own catalog through the same
probe Connectors -> Test uses, so Validate never invents different credentials
or a different object identity. ``preflight_service`` re-exports these names.
"""

from __future__ import annotations

import logging
from typing import Any

from services.secret_config import RedactedConfig, probe_config_from_endpoint

logger = logging.getLogger(__name__)

def probe_destination(endpoint) -> tuple[bool, str]:
    """Live connectivity probe for database destinations (Gate G2).

    When a saved ``connector_id`` is set, use the exact same probe as
    Connectors → Test so Validate never invents different credentials.
    """
    if endpoint.kind != "database":
        return True, "Non-database destination"

    if getattr(endpoint, "connector_id", None):
        from services.connector_probe import probe_saved_connector

        ok, msg, _cfg = probe_saved_connector(endpoint.connector_id)
        return ok, msg

    from src.transfer.adapters import resolve_connector_config, resolve_dest_table
    from src.transfer.connector_registry import run_probe

    cfg = resolve_connector_config(endpoint)
    db_type = (cfg.get("type") or endpoint.format or "").lower()
    if db_type == "dynamodb":
        cfg["table"] = resolve_dest_table(db_type, endpoint)
    # A destination object the run is about to create must not be required to
    # exist: demanding it reported a healthy server as "Destination unreachable:
    # Authentication failed" on every first write to a new remote path.
    cfg["require_object"] = False
    return run_probe(db_type, cfg)


def _available_staging_bytes(estimated_bytes: int) -> int:
    """Estimate writable staging capacity from local exports volume."""
    import shutil
    from pathlib import Path

    export_dir = Path(__file__).resolve().parents[2] / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    try:
        usage = shutil.disk_usage(export_dir)
        # Reserve 15% headroom; require at least 3× estimated transfer size
        usable = int(usage.free * 0.85)
        required = max(estimated_bytes * 3, 1_048_576)
        return max(usable, required) if usable >= required else usable
    except OSError:
        return max(estimated_bytes * 3, 8_388_608)


def inspect_destination_for_preflight(
    *,
    connector_id: str | None = None,
    dest_type: str | None = None,
    dest_host: str | None = None,
    dest_port: int | None = None,
    dest_database: str | None = None,
    dest_table: str | None = None,
    dest_collection: str | None = None,
    dest_schema: str | None = None,
    dest_username: str | None = None,
    dest_password: str | None = None,
    dest_connection_string: str | None = None,
    dest_warehouse: str | None = None,
    dest_auth_source: str | None = None,
    dest_auth_mode: str | None = None,
    dest_auth_role: str | None = None,
    dest_api_key: str | None = None,
    dest_service_account: str | None = None,
    dest_kind: str = "database",
    dest_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Introspect destination for table existence and column schema.

    ``dest_extra`` carries connector-specific settings that have no named
    parameter here — today SFTP host-key trust. Without it this path rebuilt the
    endpoint from named fields only, so Validate connected under *looser* trust
    than the write would use, which is exactly the mismatch
    :func:`connectors.sftp_common.host_key_settings` exists to prevent.
    """
    out: dict[str, Any] = {
        "connected": False,
        "table_exists": None,
        "can_create_table": False,
        "column_types": {},
        "column_nullability": {},
        "column_defaults": {},
        "identity_columns": [],
        "generated_columns": [],
        "columns": [],
        "db_type": (dest_type or "").lower(),
        "message": "",
    }
    if dest_kind == "file_export":
        out["connected"] = True
        out["can_create_table"] = True
        out["message"] = "File export destination"
        return out

    from src.transfer.models import EndpointConfig

    if connector_id:
        # CRITICAL: Validate G2 must use the same decrypted secrets as Connectors Test.
        # Never rebuild an EndpointConfig from empty Studio form fields (password /
        # connection_string omitted when connector_id is set) — that path defaulted
        # host→localhost and produced "auth failed" while Test still passed.
        from services.connector_probe import (
            endpoint_from_saved_connector,
            probe_saved_connector,
        )
        from services.destination_identity import stamp_destination_identity

        ok, msg, cfg = probe_saved_connector(connector_id)
        db_type = (cfg.get("type") or dest_type or "").lower()
        out["db_type"] = db_type
        # Credentials travel to the collision probe / engine inside a general
        # metadata dict — carry them redaction-safe so a stray log or response
        # cannot print the destination password. The privilege probe below still
        # reads the real values through the mapping interface.
        out["_saved_cfg"] = RedactedConfig(cfg)
        out["_probe_cfg"] = RedactedConfig(cfg)
        if not ok:
            out["connected"] = False
            out["message"] = msg or "Destination unreachable"
            return out

        endpoint = endpoint_from_saved_connector(
            connector_id,
            table=dest_table or "",
            collection=dest_collection or dest_table or "",
            schema=dest_schema or "",
            database=dest_database or "",
        )
        if not endpoint:
            out["message"] = f"Connector '{connector_id}' not found"
            return out
        stamp_destination_identity(out, endpoint)
        # Prefer operator-chosen auth_source override from Studio when present.
        if dest_auth_source:
            endpoint.auth_source = dest_auth_source
    elif dest_host or dest_connection_string:
        db_type = (dest_type or "mongodb").lower()
        out["db_type"] = db_type
        from services.dialect_profiles import normalize_schema

        endpoint = EndpointConfig(
            kind="database",
            format=db_type,
            host=dest_host or "localhost",
            port=int(dest_port or 0),
            database=dest_database or "",
            schema=normalize_schema(db_type, dest_schema, username=dest_username) or "",
            table=dest_table or "",
            collection=dest_collection or dest_table or "",
            username=dest_username or "",
            password=dest_password or "",
            connection_string=dest_connection_string or "",
            warehouse=dest_warehouse or "",
            auth_source=dest_auth_source or "",
            auth_mode=dest_auth_mode or "",
            auth_role=dest_auth_role or "",
            api_key=dest_api_key or "",
            service_account=dest_service_account or "",
            extra=dict(dest_extra or {}),
        )
        out["_probe_cfg"] = probe_config_from_endpoint(db_type, endpoint)
    else:
        out["message"] = "Destination not configured"
        return out

    # Same honesty contract as /transfer/introspect: never steal columns from
    # another DB/schema when Validate re-probes the destination.
    endpoint.extra = {
        **(endpoint.extra or {}),
        **dict(dest_extra or {}),
        "introspect_purpose": "destination",
    }

    from src.transfer.endpoint_intelligence import introspect_endpoint

    info = introspect_endpoint(endpoint)
    # Connectivity already proven via probe_saved_connector when connector_id set;
    # trust that over a second introspect failure (schema-only hiccups).
    if connector_id and out.get("db_type"):
        out["connected"] = True
        if not info.get("connected"):
            # Schema introspect failed but ping passed — keep connected, surface note.
            out["message"] = info.get("message") or msg or "Connected"
        else:
            out["message"] = info.get("message") or msg or "Connected"
    else:
        out["connected"] = bool(info.get("connected"))
        out["message"] = info.get("message", "")
    schema = info.get("schema") or {}
    cols = info.get("columns") or list(schema.keys())
    out["columns"] = cols
    out["column_types"] = schema
    out["column_nullability"] = {
        str(k): bool(v) for k, v in dict(info.get("schema_nullability") or {}).items()
    }
    # What fills a required column when the mapping does not (G14).
    out["column_defaults"] = {
        str(k): str(v) for k, v in dict(info.get("schema_defaults") or {}).items()
    }
    out["identity_columns"] = [str(c) for c in (info.get("identity_columns") or [])]
    out["generated_columns"] = [str(c) for c in (info.get("generated_columns") or [])]
    # Live UNIQUE / PK catalog — feeds identity uniqueness + append PK enforce.
    out["primary_key_columns"] = list(info.get("primary_key_columns") or [])
    out["unique_keys"] = list(info.get("unique_keys") or [])
    out["pk_columns"] = list(out["primary_key_columns"])
    # Pass through FK metadata when introspect provides it — never invent FKs.
    out["foreign_keys"] = list(
        info.get("foreign_keys") or info.get("destination_foreign_keys") or []
    )
    # Advisory-key / introspect honesty notes (BQ NOT ENFORCED, Redshift
    # informational, Snowflake NOT ENFORCED) — warn-only, never invent blockers.
    dest_warnings = [str(w) for w in (info.get("warnings") or []) if w]
    if dest_warnings:
        out["warnings"] = dest_warnings
        out["schema_warnings"] = dest_warnings
    stream = dest_collection or dest_table or endpoint.collection or endpoint.table
    # Prefer introspect's explicit existence (True / False / None). Recomputing
    # with exact string match broke public.jobs vs jobs and wiped create-new.
    if "table_exists" in info:
        out["table_exists"] = info.get("table_exists")
    elif stream and cols:
        out["table_exists"] = True
    elif stream and info.get("objects"):
        from src.transfer.endpoint_intelligence import _object_name_match

        names = [
            str(o.get("name") or "")
            for o in (info.get("objects") or [])
            if isinstance(o, dict)
        ]
        matched = _object_name_match(names, str(stream))
        out["table_exists"] = bool(matched)
    out["can_create_table"] = out["connected"]
    out["can_write"] = out["connected"]

    # Enterprise G2: measure write/create via privilege metadata (never CREATE/INSERT probe).
    if out["connected"]:
        try:
            from services.destination_privilege_probe import (
                probe_destination_privileges,
                resolve_write_flags,
            )

            cfg: dict[str, Any] = {}
            if out.get("_saved_cfg"):
                cfg = dict(out.pop("_saved_cfg") or {})
            elif connector_id:
                from services.connector_probe import probe_saved_connector

                _ok, _msg, cfg = probe_saved_connector(connector_id)
            else:
                cfg = {
                    "host": getattr(endpoint, "host", "") or "",
                    "port": int(getattr(endpoint, "port", 0) or 0),
                    "database": getattr(endpoint, "database", "") or "",
                    "username": getattr(endpoint, "username", "") or "",
                    "password": getattr(endpoint, "password", "") or "",
                    "connection_string": getattr(endpoint, "connection_string", "")
                    or "",
                    "schema": getattr(endpoint, "schema", "") or "",
                    "type": out.get("db_type") or "",
                    "warehouse": getattr(endpoint, "warehouse", "")
                    or dest_warehouse
                    or "",
                    "role": getattr(endpoint, "auth_role", "") or dest_auth_role or "",
                    "service_account": getattr(endpoint, "service_account", "")
                    or dest_service_account
                    or "",
                    "ssl": bool(getattr(endpoint, "ssl", False)),
                    "private_key": getattr(endpoint, "private_key", "") or "",
                }
            # Connector-specific settings (SFTP host-key trust) live in extra and
            # have no named cfg key; without them the probe would connect under
            # weaker trust than the write.
            for key, value in dict(getattr(endpoint, "extra", None) or {}).items():
                cfg.setdefault(key, value)

            probe_schema = str(
                dest_schema
                or cfg.get("schema")
                or cfg.get("dataset")
                or getattr(endpoint, "schema", "")
                or ""
            )
            probe = probe_destination_privileges(
                out.get("db_type") or cfg.get("type") or "",
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or 0),
                database=str(cfg.get("database") or cfg.get("project_id") or ""),
                schema=probe_schema,
                table=str(
                    dest_table
                    or dest_collection
                    or getattr(endpoint, "table", "")
                    or getattr(endpoint, "collection", "")
                    or ""
                ),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                table_exists=(
                    out.get("table_exists")
                    if isinstance(out.get("table_exists"), bool)
                    else None
                ),
                ssl=bool(cfg.get("ssl") or False),
                warehouse=str(
                    cfg.get("warehouse")
                    or dest_warehouse
                    or getattr(endpoint, "warehouse", "")
                    or ""
                ),
                role=str(
                    cfg.get("role")
                    or cfg.get("auth_role")
                    or dest_auth_role
                    or getattr(endpoint, "auth_role", "")
                    or ""
                ),
                account=str(cfg.get("account") or cfg.get("host") or ""),
                project_id=str(cfg.get("project_id") or cfg.get("database") or ""),
                dataset=str(cfg.get("dataset") or probe_schema),
                service_account=str(
                    cfg.get("service_account")
                    or dest_service_account
                    or getattr(endpoint, "service_account", "")
                    or ""
                ),
                location=str(cfg.get("location") or ""),
                auth_source=str(
                    cfg.get("auth_source")
                    or dest_auth_source
                    or getattr(endpoint, "auth_source", "")
                    or ""
                ),
                api_key=str(
                    cfg.get("api_key") or getattr(endpoint, "api_key", "") or ""
                ),
                private_key=str(
                    cfg.get("private_key") or getattr(endpoint, "private_key", "") or ""
                ),
                # SFTP: probe under the same host-key trust the write will use.
                host_key=str(cfg.get("host_key") or ""),
                known_hosts=str(cfg.get("known_hosts") or ""),
                host_key_policy=str(cfg.get("host_key_policy") or ""),
            )
            can_write, can_create, priv_meta = resolve_write_flags(True, probe)
            out["can_write"] = can_write
            out["can_create_table"] = can_create
            out["privilege_probe"] = priv_meta
            if probe.status == "denied" and probe.detail:
                # Surface explicit deny in message without wiping connectivity success.
                out["message"] = probe.detail
            elif probe.status == "unavailable" and probe.detail:
                out["privilege_probe_warning"] = probe.detail
        except Exception as exc:  # noqa: BLE001
            # Never leave pre-probe invent (connected ⇒ can_create=True).
            # Unavailable: write may proceed; create-table must not soft-pass.
            from services.destination_privilege_probe import (
                PrivilegeProbeResult,
                resolve_write_flags,
            )

            probe = PrivilegeProbeResult(
                can_write=None,
                can_create_table=None,
                status="unavailable",
                detail=f"Privilege probe failed: {exc}"[:400],
            )
            can_write, can_create, priv_meta = resolve_write_flags(True, probe)
            out["can_write"] = can_write
            out["can_create_table"] = can_create
            out["privilege_probe"] = priv_meta
            out["privilege_probe_warning"] = str(exc)[:400]
    # Persist auto-resolved Mongo authSource so Validate/Execute match Connectors Test.
    resolved_auth = (getattr(endpoint, "auth_source", "") or "").strip()
    if (
        out["connected"]
        and resolved_auth
        and (out.get("db_type") or "").lower() == "mongodb"
    ):
        out["auth_source"] = resolved_auth
        if connector_id:
            try:
                from services.connector_store import get_connector, update_connector

                conn = get_connector(connector_id)
                if conn and (conn.auth_source or "") != resolved_auth:
                    update_connector(connector_id, {"auth_source": resolved_auth})
            except Exception as exc:
                logger.debug(
                    "mongodb auth_source persistence failed: %s", exc, exc_info=exc
                )
    db = (out.get("db_type") or "").lower()
    if db in {"redshift", "amazon_redshift", "redshift_serverless"} and out.get("connected"):
        from connectors.redshift_copy import probe_redshift_staging

        probe_extra: dict[str, Any] = {}
        probe_extra.update(dict(getattr(endpoint, "extra", None) or {}))
        probe_extra.update(dict(dest_extra or {}))
        out["redshift_staging_probe"] = probe_redshift_staging(probe_extra)
    return out
