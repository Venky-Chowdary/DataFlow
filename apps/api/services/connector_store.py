"""Saved database connector configurations — MongoDB-first with file fallback.

This module stores reusable source/destination connector profiles. In production
it persists to MongoDB so connectors survive container restarts; in local/test
mode it falls back to a JSON file under `data_dir()`.
"""

from __future__ import annotations

import json
import logging
import os
from services.brand_env import getenv_brand
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from services.platform_config import data_dir
from services.secret_config import mask_secrets_in_text
from services.secret_vault import decrypt_secret, encrypt_secret, tenant_id_from_workspace

_CONNECTOR_SECRET_KEYS = (
    "password",
    "connection_string",
    "api_key",
    "service_account",
    "private_key",
    "secret_access_key",
    "access_key_secret",
    "token",
    "refresh_token",
    "client_secret",
)
from services.value_serializer import json_default

STORE_PATH = data_dir() / "connectors.json"

logger = logging.getLogger(__name__)


class ConnectorStoreError(RuntimeError):
    """The configured connector store failed a write; nothing was saved."""


def _mongo_write_failed(op: str, connector_id: str, exc: Exception) -> ConnectorStoreError:
    # Mongo is the system of record once selected: a local-file fallback would
    # report success, leave Mongo unchanged and put credentials on local disk.
    # The full driver error stays in the server log; pymongo messages can carry
    # hosts and URI fragments, so the operator-facing text is credential-masked.
    logger.error(
        "MongoDB %s failed for connector %s; refusing file-store fallback",
        op, connector_id or "<new>", exc_info=exc,
    )
    return ConnectorStoreError(
        f"Connector {op} failed in MongoDB (the configured connector store); "
        f"nothing was saved: {_mask_conn_str(str(exc))}"
    )

# Databases / warehouses / object stores that are valid as source *and* destination.
# Catalog UI may pass role=source|destination from the filter tab — that must not
# lock the saved profile into a one-sided capability.
_BIDIRECTIONAL_TYPES = frozenset({
    "mysql", "mariadb", "singlestore",
    "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb", "supabase",
    "sqlserver", "mssql", "synapse", "oracle", "db2", "generic_sql",
    "sqlite", "duckdb", "h2",
    "mongodb", "dynamodb", "cassandra", "couchbase", "elasticsearch", "redis",
    "snowflake", "bigquery", "databricks", "clickhouse", "trino", "presto", "questdb",
    "s3", "amazon_s3", "gcs", "google_cloud_storage", "adls", "azure_blob", "azure_blob_storage",
    "kafka", "apache_kafka", "iceberg", "apache_iceberg",
    "salesforce", "hubspot",
})


def normalize_connector_role(connector_type: str, role: str | None) -> str:
    """Return a persisted topology role. Dual-use types always store ``both``."""
    t = (connector_type or "").strip().lower()
    # The capability registry is the topology authority — a dest-only driver
    # (pgvector, qdrant, weaviate, pinecone, milvus) must not persist ``both``
    # however the caller spelled its role (QA C09: test_connector reported
    # role=both for pgvector while the driver declares read=False).
    try:
        from src.transfer.connector_capabilities import (
            _declared_capabilities,
            resolve_driver_type,
        )

        caps = _declared_capabilities(resolve_driver_type(t) or t)
        if caps.get("dest_only") or caps.get("write") and not caps.get("read"):
            return "destination"
        if caps.get("source_only") or caps.get("read") and not caps.get("write"):
            return "source"
    except Exception:
        pass
    if t in _BIDIRECTIONAL_TYPES:
        return "both"
    r = (role or "both").strip().lower()
    if r in ("destination", "dest"):
        return "destination"
    if r == "source":
        return "source"
    return "both"


def _resolve_connector_schema(
    conn_type: str,
    schema: str | None,
    username: str | None = None,
) -> str:
    """Dialect-aware schema default — never force Postgres ``public`` onto other engines."""
    from services.dialect_profiles import normalize_schema

    resolved = normalize_schema(conn_type, schema, username=username)
    return resolved or ""


def _store_path() -> Path:
    """Return the effective file store path.

    ``DATAFLOW_CONNECTOR_STORE`` overrides the default so deployments can mount
    a persistent volume at a known location.
    """
    env = getenv_brand("CONNECTOR_STORE", "").strip()
    if env:
        return Path(env)
    return STORE_PATH

_backend_choice: str | None = None


def listen_port_for_connector(conn_type: str, raw: Any) -> int:
    """Listen port stored on a connector.

    Delegates to :func:`stored_listen_port`, the same rule the dial path uses.
    Missing, ``0``, historical ``5432``, and Neo4j Bolt ``7687`` become the
    driver port. An explicit other port is kept.
    """
    try:
        from src.transfer.connector_capabilities import stored_listen_port
    except ImportError:
        from transfer.connector_capabilities import stored_listen_port

    return stored_listen_port(conn_type, raw)


@dataclass
class SavedConnector:
    id: str
    name: str
    type: str
    role: str  # source | destination | both
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    schema: str = ""  # filled via dialect_profiles — never assume Postgres public
    connection_string: str = ""
    ssl: bool = True
    warehouse: str = ""
    auth_mode: str = ""
    auth_role: str = ""
    api_key: str = ""
    service_account: str = ""
    private_key: str = ""
    endpoint_url: str = ""
    path_style: bool = False
    auth_source: str = ""
    workspace_id: str = ""
    #: Connector-specific options that are not first-class columns. SFTP
    #: host-key trust (``host_key`` / ``known_hosts`` / ``host_key_policy``)
    #: lives here so a scheduled beat opens the same pinned transport Studio
    #: Test verified.
    extra: dict[str, Any] = field(default_factory=dict)
    last_tested_at: str | None = None
    last_test_ok: bool | None = None
    last_used_at: str | None = None
    #: Set when a transfer that used this connector completed. A failed probe
    #: older than this is stale — the connection has since moved data.
    last_transfer_ok_at: str | None = None
    credentials_rotated_at: str | None = None
    created_at: str = field(default_factory=lambda: _now())

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Never expose raw password in list responses — masked at API layer
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SavedConnector:
        workspace_id = data.get("workspace_id", "") or ""
        tenant_id = tenant_id_from_workspace(workspace_id)
        password = decrypt_secret(data.get("password", "") or "", tenant_id=tenant_id)
        conn_str = decrypt_secret(data.get("connection_string", "") or "", tenant_id=tenant_id)
        conn_type = data["type"]
        from connectors.sftp_common import apply_sftp_uri_endpoint

        lifted = apply_sftp_uri_endpoint(
            {
                "type": conn_type,
                "host": data.get("host", "") or "",
                "port": data.get("port"),
                "username": data.get("username", "") or "",
                "password": password,
                "database": data.get("database", "") or "",
                "connection_string": conn_str,
            }
        )
        password = str(lifted.get("password") or "")
        return cls(
            id=data["id"],
            name=data["name"],
            type=conn_type,
            role=normalize_connector_role(conn_type, data.get("role")),
            host=str(lifted.get("host") or ""),
            port=listen_port_for_connector(conn_type, lifted.get("port")),
            database=str(lifted.get("database") or ""),
            username=str(lifted.get("username") or ""),
            password=password,
            schema=_resolve_connector_schema(conn_type, data.get("schema"), data.get("username")),
            connection_string=conn_str,
            ssl=bool(data.get("ssl", True)),
            warehouse=data.get("warehouse", ""),
            auth_mode=data.get("auth_mode", ""),
            auth_role=data.get("auth_role", ""),
            api_key=decrypt_secret(data.get("api_key", "") or "", tenant_id=tenant_id),
            service_account=decrypt_secret(data.get("service_account", "") or "", tenant_id=tenant_id),
            private_key=decrypt_secret(data.get("private_key", "") or "", tenant_id=tenant_id),
            endpoint_url=data.get("endpoint_url", ""),
            path_style=bool(data.get("path_style", False)),
            auth_source=data.get("auth_source", ""),
            workspace_id=data.get("workspace_id", ""),
            extra=connector_extra_from_payload(data),
            last_tested_at=data.get("last_tested_at"),
            last_test_ok=data.get("last_test_ok") if "last_test_ok" in data else None,
            last_used_at=data.get("last_used_at"),
            last_transfer_ok_at=data.get("last_transfer_ok_at"),
            credentials_rotated_at=data.get("credentials_rotated_at"),
            created_at=data.get("created_at", _now()),
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


_SFTP_TRUST_KEYS = ("host_key", "known_hosts", "host_key_policy")


def connector_extra_from_payload(data: Mapping[str, Any] | None) -> dict[str, Any]:
    """Persist operator extras, lifting SFTP trust off the payload root.

    Studio / the API may send ``host_key`` next to host/port rather than
    nested under ``extra``. A schedule that later rebuilds the endpoint
    must still see the pin.
    """
    payload = data or {}
    extra = dict(payload.get("extra") or {}) if isinstance(payload.get("extra"), dict) else {}
    for key in _SFTP_TRUST_KEYS:
        value = payload.get(key)
        if value not in (None, ""):
            extra[key] = value
    return extra


def _resolve_backend() -> str:
    """Pick the persistence backend."""
    global _backend_choice
    if _backend_choice is not None:
        return _backend_choice

    env = getenv_brand("CONNECTOR_STORE_BACKEND", "auto").lower()
    if env in ("mongo", "mongodb"):
        _backend_choice = "mongo"
        logger.info("Connector store backend: mongo (explicit)")
        return _backend_choice
    if env == "file":
        _backend_choice = "file"
        logger.info("Connector store backend: file (explicit)")
        return _backend_choice

    # Auto: prefer Mongo when a Mongo URI is explicitly configured and reachable.
    explicit = os.getenv("MONGODB_URI") or os.getenv("MONGO_URL") or os.getenv("MONGO_PRIVATE_URL") or os.getenv("MONGO_PUBLIC_URL")
    if explicit:
        try:
            from src.services.mongodb_service import MongoDBService

            svc = MongoDBService(explicit)
            if svc.connect():
                _backend_choice = "mongo"
                svc.disconnect()
                logger.info("Connector store backend: mongo (auto-detected)")
                return _backend_choice
        except Exception as exc:
            logger.debug("MongoDB not reachable for connector store: %s", exc)

    _backend_choice = "file"
    logger.info("Connector store backend: file")
    return _backend_choice


def _use_mongo() -> bool:
    return _resolve_backend() == "mongo"


def connector_persistence_status() -> dict[str, Any]:
    """Health signal for connector persistence (file and/or Mongo).

    Uses a short socket timeout so readiness probes cannot hang the request.
    """
    backend = _resolve_backend()
    path = _store_path()
    file_ok = path.exists()
    mongo_ok = False
    count: int | None = None
    if backend == "mongo":
        try:
            coll = _mongo_collection()
            # Prefer a bounded ping over an unbounded collection scan.
            coll.database.client.admin.command("ping", maxTimeMS=2000)
            try:
                count = int(coll.estimated_document_count(maxTimeMS=2000))
            except TypeError:
                count = int(coll.estimated_document_count())
            mongo_ok = True
        except Exception as exc:
            return {
                "backend": backend,
                "ok": False,
                "file_present": file_ok,
                "mongo_reachable": False,
                "detail": str(exc)[:200],
            }
    ok = mongo_ok if backend == "mongo" else file_ok
    return {
        "backend": backend,
        "ok": ok,
        "file_present": file_ok,
        "mongo_reachable": mongo_ok,
        "count": count,
    }


def _mongo_collection() -> Any:
    from src.services.mongodb_service import get_mongodb_service

    svc = get_mongodb_service()
    return svc.get_database()["connectors"]


def _encrypt_connector_doc(d: dict[str, Any]) -> dict[str, Any]:
    """Encrypt secret fields with tenant BYOK when an active key exists."""
    tenant_id = tenant_id_from_workspace(d.get("workspace_id") or "")
    for key in _CONNECTOR_SECRET_KEYS:
        val = d.get(key)
        if isinstance(val, str) and val and val != "****" and not val.startswith("["):
            d[key] = encrypt_secret(val, tenant_id=tenant_id, label=f"connector-{key}")
    return d


def _connector_to_doc(c: SavedConnector) -> dict[str, Any]:
    d = c.to_dict()
    d["_id"] = d.pop("id")
    return _encrypt_connector_doc(d)


def _doc_to_connector(doc: dict[str, Any]) -> SavedConnector:
    d = dict(doc)
    d["id"] = str(d.pop("_id", d.get("id", "")))
    if "created_at" in d and hasattr(d["created_at"], "isoformat"):
        d["created_at"] = d["created_at"].isoformat()
    if "updated_at" in d:
        d.pop("updated_at", None)
    return SavedConnector.from_dict(d)


def _load_all() -> list[SavedConnector]:
    if _use_mongo():
        try:
            coll = _mongo_collection()
            return [_doc_to_connector(c) for c in coll.find()]
        except Exception as exc:
            logger.warning("MongoDB connector load failed, falling back to file: %s", exc)

    store_path = _store_path()
    if not store_path.exists():
        return _seed_defaults() if _seed_enabled() else []
    try:
        raw = json.loads(store_path.read_text(encoding="utf-8"))
        items = [SavedConnector.from_dict(c) for c in raw.get("connectors", [])]
    except Exception:
        return _seed_defaults() if _seed_enabled() else []
    if not _seed_enabled():
        items = [c for c in items if not c.id.startswith("demo-")]
    return items


def _save_all(connectors: list[SavedConnector]) -> None:
    store_path = _store_path()
    store_path.parent.mkdir(parents=True, exist_ok=True)
    payload = []
    for c in connectors:
        payload.append(_encrypt_connector_doc(c.to_dict()))
    text = json.dumps({"connectors": payload}, indent=2, default=json_default)
    # Atomic write so a crash mid-write cannot leave a half-written file.
    tmp = store_path.with_suffix(store_path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(store_path)


def _seed_enabled() -> bool:
    return getenv_brand("SEED_DEMO", "").lower() in ("1", "true", "yes")


def _seed_defaults() -> list[SavedConnector]:
    """Example profiles — loaded from the environment so no credentials are hard-coded."""
    defaults = [
        SavedConnector(
            id="demo-pg-source",
            name="PostgreSQL · Source (demo)",
            type="postgresql",
            role="source",
            connection_string=getenv_brand("DEMO_PG_CONNECTION_STRING", ""),
        ),
        SavedConnector(
            id="demo-snowflake-dest",
            name="Snowflake · Warehouse (demo)",
            type="snowflake",
            role="destination",
            connection_string=getenv_brand("DEMO_SNOWFLAKE_CONNECTION_STRING", ""),
            warehouse=getenv_brand("DEMO_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        ),
        SavedConnector(
            id="demo-mongo-dest",
            name="MongoDB · Analytics (demo)",
            type="mongodb",
            role="destination",
            connection_string=getenv_brand("DEMO_MONGO_CONNECTION_STRING", ""),
        ),
    ]
    _save_all(defaults)
    return defaults


def _list_file(role: str | None, workspace_id: str | None = None) -> list[SavedConnector]:
    items = _load_all()
    if workspace_id is not None:
        items = [c for c in items if c.workspace_id == workspace_id or c.workspace_id == ""]
    if role:
        items = [c for c in items if c.role == role or c.role == "both"]
    return items


def _list_mongo(role: str | None, workspace_id: str | None = None) -> list[SavedConnector]:
    coll = _mongo_collection()
    filters: list[dict[str, Any]] = []
    if role:
        filters.append({"$or": [{"role": role}, {"role": "both"}]})
    if workspace_id is not None:
        filters.append({"$or": [{"workspace_id": workspace_id}, {"workspace_id": {"$in": ["", None]}}]})
    query = {"$and": filters} if len(filters) > 1 else (filters[0] if filters else {})
    return [_doc_to_connector(c) for c in coll.find(query)]


def connector_name_conflict_message(name: str) -> str:
    shown = (name or "").strip() or "that name"
    return (
        f'A connector named "{shown}" already exists. '
        "Choose a different name, or open the existing connector."
    )


def connector_name_taken(name: str, workspace_id: str | None = None) -> bool:
    """True when this workspace already has a connector with this name.

    Pilot create used to fall through into an in-place update, and a plaintext
    secret on that update surfaced as a Fernet error. A create is not an update.
    """
    target = (name or "").strip().casefold()
    if not target:
        return False
    scope = (workspace_id or "").strip() or None
    for existing in list_connectors(workspace_id=scope):
        if str(existing.name or "").strip().casefold() == target:
            return True
    return False


def list_connectors(role: str | None = None, workspace_id: str | None = None) -> list[SavedConnector]:
    if _use_mongo():
        try:
            return _list_mongo(role, workspace_id)
        except Exception as exc:
            logger.warning("MongoDB list_connectors failed, falling back to file: %s", exc)
    return _list_file(role, workspace_id)


def _get_file(connector_id: str, workspace_id: str | None = None) -> SavedConnector | None:
    for c in _load_all():
        if c.id == connector_id and (workspace_id is None or c.workspace_id == workspace_id or c.workspace_id == ""):
            return c
    return None


def _get_mongo(connector_id: str, workspace_id: str | None = None) -> SavedConnector | None:
    coll = _mongo_collection()
    query: dict[str, Any] = {"_id": connector_id}
    if workspace_id is not None:
        query["$or"] = [{"workspace_id": workspace_id}, {"workspace_id": {"$in": ["", None]}}]
    doc = coll.find_one(query)
    return _doc_to_connector(doc) if doc else None


def get_connector(connector_id: str, workspace_id: str | None = None) -> SavedConnector | None:
    if _use_mongo():
        try:
            return _get_mongo(connector_id, workspace_id)
        except Exception as exc:
            logger.warning("MongoDB get_connector failed, falling back to file: %s", exc)
    return _get_file(connector_id, workspace_id)


def create_connector(data: dict[str, Any]) -> SavedConnector:
    from connectors.sftp_common import apply_sftp_uri_endpoint

    data = apply_sftp_uri_endpoint(data)
    conn_type = data["type"]
    conn = SavedConnector(
        id=str(uuid.uuid4()),
        name=data["name"],
        type=conn_type,
        role=normalize_connector_role(conn_type, data.get("role")),
        host=data.get("host", ""),
        port=listen_port_for_connector(conn_type, data.get("port")),
        database=data.get("database", ""),
        username=data.get("username", ""),
        password=data.get("password", ""),
        schema=_resolve_connector_schema(conn_type, data.get("schema"), data.get("username")),
        connection_string=data.get("connection_string", ""),
        ssl=bool(data.get("ssl", True)),
        warehouse=data.get("warehouse", ""),
        auth_mode=data.get("auth_mode", ""),
        auth_role=data.get("auth_role", ""),
        api_key=data.get("api_key", ""),
        service_account=data.get("service_account", ""),
        private_key=data.get("private_key", ""),
        endpoint_url=data.get("endpoint_url", ""),
        path_style=bool(data.get("path_style", False)),
        auth_source=data.get("auth_source", ""),
        workspace_id=data.get("workspace_id", ""),
        extra=connector_extra_from_payload(data),
    )

    # Connector names are unique per workspace/role/type. A same-named create
    # keeps the existing connector's id and takes the new configuration:
    # schedules, pipelines and jobs bind connectors by id, so replacing the
    # document under a fresh id would orphan them ("connector missing").
    same_named = [
        existing
        for existing in list_connectors(role=conn.role, workspace_id=conn.workspace_id)
        if existing.name == conn.name and existing.type == conn.type
    ]
    if same_named:
        keep, *stale = same_named
        for dup in stale:
            delete_connector(dup.id, workspace_id=conn.workspace_id)
        payload = {**conn.to_dict(), "id": keep.id, "created_at": keep.created_at}
        updated = update_connector(keep.id, payload, workspace_id=conn.workspace_id)
        if updated is not None:
            return updated

    if _use_mongo():
        try:
            coll = _mongo_collection()
            coll.insert_one(_connector_to_doc(conn))
            return conn
        except Exception as exc:
            raise _mongo_write_failed("create", conn.id, exc) from exc

    connectors = _load_all()
    connectors.append(conn)
    _save_all(connectors)
    return conn


def _is_masked_placeholder(value: Any) -> bool:
    """Detect placeholder strings that must not overwrite saved secrets."""
    if value is None:
        return True
    if isinstance(value, str):
        v = value.strip()
        if not v or v == "****":
            return True
        if "****" in v or "<redacted>" in v.lower():
            return True
    return False


def _coerce_secret_update(new: Any, existing: Any) -> Any:
    """Preserve an existing secret when the update payload only carries a placeholder."""
    if _is_masked_placeholder(new) and existing not in (None, ""):
        return existing
    return new


def update_connector(connector_id: str, data: dict[str, Any], workspace_id: str | None = None) -> SavedConnector | None:
    def _merge(existing: SavedConnector) -> SavedConnector:
        merged = {**existing.to_dict(), **data, "id": connector_id}
        # Never let a masked UI payload overwrite a saved secret / connection string.
        for secret_field in ("password", "api_key", "service_account", "private_key"):
            merged[secret_field] = _coerce_secret_update(merged.get(secret_field), existing.to_dict().get(secret_field))
        merged["connection_string"] = _coerce_secret_update(
            merged.get("connection_string"), existing.to_dict().get("connection_string")
        )
        merged["role"] = normalize_connector_role(
            str(merged.get("type") or existing.type),
            merged.get("role"),
        )
        return SavedConnector.from_dict(merged)

    if _use_mongo():
        try:
            existing = _get_mongo(connector_id, workspace_id)
            if not existing:
                return None
            updated = _merge(existing)
            coll = _mongo_collection()
            coll.replace_one({"_id": connector_id}, _connector_to_doc(updated))
            return updated
        except Exception as exc:
            raise _mongo_write_failed("update", connector_id, exc) from exc

    connectors = _load_all()
    for i, c in enumerate(connectors):
        if c.id != connector_id:
            continue
        if workspace_id is not None and c.workspace_id not in (workspace_id, ""):
            continue
        updated = _merge(c)
        connectors[i] = updated
        _save_all(connectors)
        return updated
    return None


def delete_connector(connector_id: str, workspace_id: str | None = None) -> bool:
    if _use_mongo():
        try:
            coll = _mongo_collection()
            query: dict[str, Any] = {"_id": connector_id}
            if workspace_id is not None:
                query["$or"] = [{"workspace_id": workspace_id}, {"workspace_id": {"$in": ["", None]}}]
            result = coll.delete_one(query)
            return result.deleted_count > 0
        except Exception as exc:
            raise _mongo_write_failed("delete", connector_id, exc) from exc

    connectors = _load_all()
    before = len(connectors)
    filtered = [
        c for c in connectors
        if not (
            c.id == connector_id
            and (workspace_id is None or c.workspace_id == workspace_id or c.workspace_id == "")
        )
    ]
    if len(filtered) == before:
        return False
    _save_all(filtered)
    return True


def mark_tested(connector_id: str, ok: bool) -> None:
    """Persist probe result. ``last_test_ok`` is the authority for UI health."""
    tested_at = _now()
    patch = {"last_tested_at": tested_at, "last_test_ok": bool(ok)}
    if _use_mongo():
        try:
            coll = _mongo_collection()
            coll.update_one({"_id": connector_id}, {"$set": patch})
            # Keep file mirror in sync when present so dual-backend reads cannot
            # resurrect a stale failed badge after a green probe.
            try:
                connectors = _load_all()
                for i, c in enumerate(connectors):
                    if c.id == connector_id:
                        connectors[i] = SavedConnector.from_dict({**c.to_dict(), **patch})
                        _save_all(connectors)
                        break
            except Exception as mirror_exc:
                logger.debug("File mirror mark_tested skipped: %s", mirror_exc)
            return
        except Exception as exc:
            logger.warning("MongoDB mark_tested failed, falling back to file: %s", exc)

    connectors = _load_all()
    for i, c in enumerate(connectors):
        if c.id == connector_id:
            connectors[i] = SavedConnector.from_dict({**c.to_dict(), **patch})
            _save_all(connectors)
            return


def note_transfer_succeeded(*connector_ids: str | None) -> int:
    """Stamp a completed transfer. This is not a probe pass.

    ``last_test_ok`` stays false until the operator tests again. Health
    treats a later successful transfer as newer evidence than that probe.
    """
    now = _now()
    patch = {"last_transfer_ok_at": now}
    seen: set[str] = set()
    stamped = 0
    for raw in connector_ids:
        cid = str(raw or "").strip()
        if not cid or cid in seen:
            continue
        seen.add(cid)
        if _use_mongo():
            try:
                coll = _mongo_collection()
                result = coll.update_one({"_id": cid}, {"$set": patch})
                if result.matched_count:
                    stamped += 1
                    continue
            except Exception as exc:  # noqa: BLE001 - file store is the fallback
                logger.warning("MongoDB note_transfer_succeeded failed, falling back to file: %s", exc)
        connectors = _load_all()
        for i, c in enumerate(connectors):
            if c.id == cid:
                connectors[i] = SavedConnector.from_dict({**c.to_dict(), **patch})
                _save_all(connectors)
                stamped += 1
                break
    return stamped


def _parse_instant(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def connector_health(conn: Any) -> str:
    """``passed`` / ``failed`` / ``untested`` — the one health rule for every reader.

    A failed probe is overruled only by a transfer that completed after it;
    the web client (``connectorHealth.ts``) applies the same rule.
    """
    get = conn.get if isinstance(conn, Mapping) else (lambda k: getattr(conn, k, None))
    ok = get("last_test_ok")
    if ok in (True, 1, "true", "1"):
        return "passed"
    if ok in (False, 0, "false", "0"):
        used = _parse_instant(get("last_transfer_ok_at"))
        if used is not None:
            probed = _parse_instant(get("last_tested_at"))
            if probed is None or used > probed:
                return "passed"
        return "failed"
    return "untested"


def connector_ui_status(conn: Any) -> str:
    get = conn.get if isinstance(conn, Mapping) else (lambda k: getattr(conn, k, None))
    if connector_health(conn) == "failed" and get("last_tested_at"):
        return "error"
    return "configured"


def mark_used(*connector_ids: str | None) -> int:
    """Stamp last_used_at on saved connectors that actually ran a transfer."""
    now = _now()
    patch = {"last_used_at": now}
    seen: set[str] = set()
    stamped = 0
    for raw in connector_ids:
        cid = str(raw or "").strip()
        if not cid or cid in seen:
            continue
        seen.add(cid)
        if _use_mongo():
            try:
                coll = _mongo_collection()
                result = coll.update_one({"_id": cid}, {"$set": patch})
                if result.matched_count:
                    stamped += 1
                    try:
                        connectors = _load_all()
                        for i, c in enumerate(connectors):
                            if c.id == cid:
                                connectors[i] = SavedConnector.from_dict({**c.to_dict(), **patch})
                                _save_all(connectors)
                                break
                    except Exception as mirror_exc:
                        logger.debug("File mirror mark_used skipped: %s", mirror_exc)
                    continue
            except Exception as exc:
                logger.warning("MongoDB mark_used failed, falling back to file: %s", exc)
        connectors = _load_all()
        for i, c in enumerate(connectors):
            if c.id == cid:
                connectors[i] = SavedConnector.from_dict({**c.to_dict(), **patch})
                _save_all(connectors)
                stamped += 1
                break
    return stamped


def mask_connector(c: SavedConnector) -> dict[str, Any]:
    d = c.to_dict()
    if d.get("password"):
        d["password"] = "****"  # nosec B105
    if d.get("connection_string"):
        d["connection_string"] = _mask_conn_str(d["connection_string"])
    if d.get("api_key"):
        d["api_key"] = "****"
    if d.get("service_account"):
        d["service_account"] = "****"
    if d.get("private_key"):
        d["private_key"] = "****"
    d.setdefault("workspace_id", "")
    d.setdefault("endpoint_url", "")
    d.setdefault("path_style", False)
    return d


def _mask_conn_str(s: str) -> str:
    return mask_secrets_in_text(s)
