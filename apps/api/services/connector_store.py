"""Saved database connector configurations — MongoDB-first with file fallback.

This module stores reusable source/destination connector profiles. In production
it persists to MongoDB so connectors survive container restarts; in local/test
mode it falls back to a JSON file under `data_dir()`.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.secret_vault import decrypt_secret, encrypt_secret
from services.value_serializer import json_default

STORE_PATH = data_dir() / "connectors.json"

logger = logging.getLogger(__name__)


def _resolve_connector_schema(
    conn_type: str,
    schema: str | None,
    username: str | None = None,
) -> str:
    """Dialect-aware schema default — never force Postgres ``public`` onto other engines."""
    from services.dialect_profiles import normalize_schema

    resolved = normalize_schema(conn_type, schema, username=username)
    return resolved or ""


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
    if t in _BIDIRECTIONAL_TYPES:
        return "both"
    r = (role or "both").strip().lower()
    if r in ("destination", "dest"):
        return "destination"
    if r == "source":
        return "source"
    return "both"


def _store_path() -> Path:
    """Return the effective file store path.

    ``DATAFLOW_CONNECTOR_STORE`` overrides the default so deployments can mount
    a persistent volume at a known location.
    """
    env = os.getenv("DATAFLOW_CONNECTOR_STORE", "").strip()
    if env:
        return Path(env)
    return STORE_PATH

_backend_choice: str | None = None


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
    last_tested_at: str | None = None
    last_test_ok: bool | None = None
    created_at: str = field(default_factory=lambda: _now())

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Never expose raw password in list responses — masked at API layer
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SavedConnector:
        password = decrypt_secret(data.get("password", "") or "")
        conn_str = decrypt_secret(data.get("connection_string", "") or "")
        conn_type = data["type"]
        return cls(
            id=data["id"],
            name=data["name"],
            type=conn_type,
            role=normalize_connector_role(conn_type, data.get("role")),
            host=data.get("host", ""),
            port=int(data.get("port", 5432)),
            database=data.get("database", ""),
            username=data.get("username", ""),
            password=password,
            schema=_resolve_connector_schema(conn_type, data.get("schema"), data.get("username")),
            connection_string=conn_str,
            ssl=bool(data.get("ssl", True)),
            warehouse=data.get("warehouse", ""),
            auth_mode=data.get("auth_mode", ""),
            auth_role=data.get("auth_role", ""),
            api_key=data.get("api_key", ""),
            service_account=data.get("service_account", ""),
            private_key=decrypt_secret(data.get("private_key", "") or ""),
            endpoint_url=data.get("endpoint_url", ""),
            path_style=bool(data.get("path_style", False)),
            auth_source=data.get("auth_source", ""),
            workspace_id=data.get("workspace_id", ""),
            last_tested_at=data.get("last_tested_at"),
            last_test_ok=data.get("last_test_ok") if "last_test_ok" in data else None,
            created_at=data.get("created_at", _now()),
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_backend() -> str:
    """Pick the persistence backend."""
    global _backend_choice
    if _backend_choice is not None:
        return _backend_choice

    env = os.getenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "auto").lower()
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


def _connector_to_doc(c: SavedConnector) -> dict[str, Any]:
    d = c.to_dict()
    d["_id"] = d.pop("id")
    if d.get("password"):
        d["password"] = encrypt_secret(d["password"])
    if d.get("connection_string"):
        d["connection_string"] = encrypt_secret(d["connection_string"])
    if d.get("private_key"):
        d["private_key"] = encrypt_secret(d["private_key"])
    return d


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
        d = c.to_dict()
        if d.get("password"):
            d["password"] = encrypt_secret(d["password"])
        if d.get("connection_string"):
            d["connection_string"] = encrypt_secret(d["connection_string"])
        if d.get("private_key"):
            d["private_key"] = encrypt_secret(d["private_key"])
        payload.append(d)
    text = json.dumps({"connectors": payload}, indent=2, default=json_default)
    # Atomic write so a crash mid-write cannot leave a half-written file.
    tmp = store_path.with_suffix(store_path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(store_path)


def _seed_enabled() -> bool:
    return os.getenv("DATAFLOW_SEED_DEMO", "").lower() in ("1", "true", "yes")


def _seed_defaults() -> list[SavedConnector]:
    """Example profiles — loaded from the environment so no credentials are hard-coded."""
    defaults = [
        SavedConnector(
            id="demo-pg-source",
            name="PostgreSQL · Source (demo)",
            type="postgresql",
            role="source",
            connection_string=os.getenv("DATAFLOW_DEMO_PG_CONNECTION_STRING", ""),
        ),
        SavedConnector(
            id="demo-snowflake-dest",
            name="Snowflake · Warehouse (demo)",
            type="snowflake",
            role="destination",
            connection_string=os.getenv("DATAFLOW_DEMO_SNOWFLAKE_CONNECTION_STRING", ""),
            warehouse=os.getenv("DATAFLOW_DEMO_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        ),
        SavedConnector(
            id="demo-mongo-dest",
            name="MongoDB · Analytics (demo)",
            type="mongodb",
            role="destination",
            connection_string=os.getenv("DATAFLOW_DEMO_MONGO_CONNECTION_STRING", ""),
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
    conn_type = data["type"]
    conn = SavedConnector(
        id=str(uuid.uuid4()),
        name=data["name"],
        type=conn_type,
        role=normalize_connector_role(conn_type, data.get("role")),
        host=data.get("host", ""),
        port=int(data.get("port", 5432)),
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
    )

    if _use_mongo():
        try:
            coll = _mongo_collection()
            coll.insert_one(_connector_to_doc(conn))
            return conn
        except Exception as exc:
            logger.warning("MongoDB create_connector failed, falling back to file: %s", exc)

    connectors = _load_all()
    connectors.append(conn)
    _save_all(connectors)
    return conn


def update_connector(connector_id: str, data: dict[str, Any], workspace_id: str | None = None) -> SavedConnector | None:
    def _merge(existing: SavedConnector) -> SavedConnector:
        merged = {**existing.to_dict(), **data, "id": connector_id}
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
            logger.warning("MongoDB update_connector failed, falling back to file: %s", exc)

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
            logger.warning("MongoDB delete_connector failed, falling back to file: %s", exc)

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
    if _use_mongo():
        try:
            coll = _mongo_collection()
            coll.update_one(
                {"_id": connector_id},
                {"$set": {"last_tested_at": _now(), "last_test_ok": ok}},
            )
            return
        except Exception as exc:
            logger.warning("MongoDB mark_tested failed, falling back to file: %s", exc)

    connectors = _load_all()
    for i, c in enumerate(connectors):
        if c.id == connector_id:
            connectors[i] = SavedConnector.from_dict(
                {**c.to_dict(), "last_tested_at": _now(), "last_test_ok": ok}
            )
            _save_all(connectors)
            return


def mask_connector(c: SavedConnector) -> dict[str, Any]:
    d = c.to_dict()
    if d.get("password"):
        d["password"] = "****"
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
    import re

    return re.sub(r":([^:@/]+)@", ":****@", s)
