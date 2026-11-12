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
import threading
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platform
    fcntl = None  # type: ignore[assignment]

from services.platform_config import data_dir
from services.log_redaction import REDACTED
from services.secret_config import mask_secrets_in_text
from services.secret_vault import decrypt_for_workspace, decrypt_secret, encrypt_secret, tenant_id_from_workspace
from services.workspace_filter import bound_allows, bound_workspace_filter, current_workspace_filter

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


def listen_port_for_connector(conn_type: str, raw: Any, host: Any = "") -> int:
    """Listen port stored on a connector.

    Delegates to :func:`stored_listen_port`, the same rule the dial path uses.
    Missing, ``0``, historical ``5432``, and Neo4j Bolt ``7687`` become the
    driver port. An explicit other port is kept. A URL-form host's own port
    wins: it is the port the driver dials.
    """
    try:
        from src.transfer.connector_capabilities import stored_listen_port, url_host_port
    except ImportError:
        from transfer.connector_capabilities import stored_listen_port, url_host_port

    return stored_listen_port(conn_type, url_host_port(host) or raw)


# --- TLS requirement -------------------------------------------------------
# ``ssl_mode`` records what the USER chose, so "TLS required" is a fact and not
# the stored default:
#   "require" - the user turned SSL/TLS on. The connection is strictly
#               encrypted and is never retried in plaintext.
#   "prefer"  - negotiate TLS, plaintext allowed (explicit, API/import only).
#   "disable" - MySQL family only: the user explicitly asked for a plaintext
#               session. Honoured only where the server allows it; a server
#               requiring secure transport is re-dialled with TLS and that is
#               recorded (connectors.mysql_conn.dial_pymysql). Carried to the
#               dial as the MySQL URI option ``ssl-mode=DISABLED``
#               (:func:`dial_connection_string`).
#   ""        - unset: the user never asked. Negotiates like "prefer"; a
#               legacy non-PostgreSQL ``ssl=True`` still means "require".
# Documents written before this field existed carry only the boolean ``ssl``,
# which this store used to default to True when a caller omitted it - so a
# legacy ``ssl=True`` cannot be told apart from an injected default. Such
# documents are read as unset ("") until the connector is saved again.
# For the PostgreSQL family ``SavedConnector.ssl`` is the derived strict flag
# (``ssl_mode == "require"``), so every downstream ``ssl=True`` means required.
SSL_MODES = ("", "prefer", "require", "disable")
_TLS_MODE_TYPES = frozenset({"postgresql", "redshift"})


def _has_tls_mode(conn_type: str) -> bool:
    """PostgreSQL-wire tiles, by the driver they resolve to.

    QA r12 bug 8: the check used the raw tile id, so ``amazon_aurora_postgresql``
    (and every other PostgreSQL catalog twin) stored ``ssl=True`` — TLS
    *required* for this family — when the operator never asked for TLS, while
    the plain PostgreSQL tile on the same endpoint negotiated.
    """
    key = (conn_type or "").strip().lower()
    if key in _TLS_MODE_TYPES:
        return True
    try:
        from src.transfer.connector_capabilities import resolve_driver_type
    except ImportError:  # pragma: no cover
        from transfer.connector_capabilities import resolve_driver_type
    return resolve_driver_type(key) in _TLS_MODE_TYPES


def default_ssl(conn_type: str) -> bool:
    """The ``ssl`` value for a caller that never said anything about TLS.

    Engines with an explicit TLS mode (PostgreSQL family) negotiate: only a user's
    choice may make TLS mandatory. Every other engine keeps its historical True.
    """
    return not _has_tls_mode(conn_type)


def stamp_ssl_mode(doc: dict[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    """Record the user's TLS choice from a create/update payload onto ``doc``.

    A payload that says nothing about TLS leaves ``doc`` untouched.
    """
    if patch.get("ssl_mode") in SSL_MODES:
        doc["ssl_mode"] = patch["ssl_mode"]
    elif "ssl" in patch:
        doc["ssl_mode"] = "require" if patch["ssl"] else ""
    return doc


def _is_mysql_family(conn_type: str) -> bool:
    try:
        from src.transfer.engine_family import family_or_kind
    except ImportError:  # pragma: no cover
        from transfer.engine_family import family_or_kind
    return family_or_kind(conn_type or "") == "mysql"


def validate_ssl_mode(conn_type: str, data: Mapping[str, Any]) -> None:
    """Refuse a TLS mode this connector cannot honour (raised as ValueError -> 4xx)."""
    mode = data.get("ssl_mode")
    if mode is None:
        return
    if mode not in SSL_MODES:
        raise ValueError(
            f"ssl_mode '{mode}' is not one of require, prefer, disable (or empty for unset)."
        )
    if mode == "disable" and not _is_mysql_family(conn_type):
        raise ValueError(
            "ssl_mode 'disable' is supported for MySQL-family connectors only; "
            f"a {conn_type} connector cannot promise a plaintext session."
        )
    if mode == "disable":
        # Raises ValueError (naming ssl_mode) on a DSN asking for REQUIRED / VERIFY_*.
        dial_connection_string(conn_type, mode, str(data.get("connection_string") or ""),
                               data.get("extra") if isinstance(data.get("extra"), Mapping)
                               else {k: data.get(k) for k in _MYSQL_TLS_OPTION_KEYS})


#: Update fields that change what a MySQL dial's TLS plan reads.
_TLS_PLAN_FIELDS = frozenset({"ssl_mode", "ssl", "connection_string", "extra", "type"})


def validate_merged_ssl_mode(patch: Mapping[str, Any], merged: Mapping[str, Any]) -> None:
    """An edit's TLS choice, checked against the connector it produces.

    A PATCH ``{"ssl_mode": "disable"}`` on a connector whose saved ``extra``
    holds ``ssl_ca`` used to be checked against the patch alone, so it was
    saved and every later dial failed. ``merged`` is the decrypted result of
    the edit (saved fields plus the patch, extras merged).
    """
    if not (_TLS_PLAN_FIELDS & set(patch)):
        return
    validate_ssl_mode(str(merged.get("type") or ""), {
        "ssl_mode": merged.get("ssl_mode") if merged.get("ssl_mode") in SSL_MODES else None,
        "connection_string": str(merged.get("connection_string") or ""),
        "extra": dict(merged.get("extra") or {}),
    })


def resolve_tls(conn_type: str, data: Mapping[str, Any]) -> tuple[bool, str]:
    """Return ``(ssl, ssl_mode)`` for a stored or incoming connector document.

    Back-compat: a document with no mode keeps its boolean (non-PostgreSQL
    ``ssl=True`` is "require", ``ssl=False`` is "prefer"); an explicit mode wins.
    """
    mode = data.get("ssl_mode")
    mode = mode if mode in SSL_MODES else ""
    if _has_tls_mode(conn_type):
        return mode == "require", mode
    if mode == "require":
        return True, mode
    if mode in {"prefer", "disable"}:
        return False, mode
    return bool(data.get("ssl", True)), mode


#: Form / saved-extra TLS options a MySQL dial reads from the DSN.
_MYSQL_TLS_OPTION_KEYS = ("ssl_ca", "ssl_capath", "ssl_cert", "ssl_key",
                          "ssl_verify_cert", "ssl_verify_identity")


def dial_connection_string(
    conn_type: str, ssl_mode: str, connection_string: str, options: Mapping[str, Any] | None = None
) -> str:
    """The connection string a MySQL dial uses: one TLS plan input for every path.

    PyMySQL, SQLAlchemy and the binlog reader all read TLS from the DSN
    (``connectors.mysql_tls.mysql_tls_plan``). The saved connector's TLS
    choice — ``ssl_mode`` "disable", and the form's CA / client certificate /
    verify options (``options``, the saved ``extra``) — rides there, so the
    three paths cannot disagree. A saved "disable" next to a DSN that asks
    for REQUIRED / VERIFY_* is a contradiction and raises (422 at save).
    """
    raw = connection_string or ""
    if not _is_mysql_family(conn_type):
        return raw
    from urllib.parse import urlencode

    from connectors.mysql_tls import TLS_DISABLED, TlsModeError, mysql_tls_plan, tls_request

    have = tls_request(raw)
    extra: list[tuple[str, str]] = []
    for key in _MYSQL_TLS_OPTION_KEYS:
        value = (options or {}).get(key)
        if value in (None, "", False):
            continue
        slot = key.removeprefix("ssl_")
        if slot in {"verify_cert", "verify_identity"}:
            if not have[slot]:
                extra.append((key, "true"))
        elif not have.get(slot):
            extra.append((key, str(value)))
    if ssl_mode == "disable":
        try:
            current = mysql_tls_plan(ssl=False, connection_string=raw, options=dict(options or {}))
            from_dsn = mysql_tls_plan(ssl=False, connection_string=raw, options={})
        except TlsModeError as exc:
            raise ValueError(f"ssl_mode 'disable' contradicts the connection's TLS settings: {exc}") from exc
        if current.mode != TLS_DISABLED:
            if from_dsn.must_encrypt:
                raise ValueError(
                    f"ssl_mode 'disable' contradicts ssl-mode={from_dsn.mode.upper()} in the "
                    "connection string; keep one of them."
                )
            if current.must_encrypt:
                # The DSN asks for nothing. The connector's own TLS options do:
                # a CA means verify the server, and a client certificate means
                # an encrypted session. Name those options, not the DSN.
                named = ", ".join(k for k in _MYSQL_TLS_OPTION_KEYS
                                  if (options or {}).get(k) not in (None, "", False))
                raise ValueError(
                    f"ssl_mode 'disable' contradicts this connector's TLS options ({named}); "
                    f"they ask for an encrypted ({current.mode.upper()}) session. Remove them, "
                    "or set ssl_mode to require."
                )
            extra.append(("ssl-mode", "DISABLED"))
    if not extra:
        return raw
    return f"{raw}{'&' if '?' in raw else '?'}{urlencode(extra)}"


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
    #: What the user chose for TLS (see SSL_MODES above); "" = never asked.
    ssl_mode: str = ""
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
    def from_dict(cls, data: dict[str, Any], *, secrets_encrypted: bool = True) -> SavedConnector:
        workspace_id = data.get("workspace_id", "") or ""

        def _secret(key: str) -> str:
            raw = str(data.get(key, "") or "")
            # In-memory merges already hold the decrypted secret. Decrypting
            # that plaintext again is what made update_connector raise a Fernet
            # error on a connector that had just been created (R8-004).
            if not secrets_encrypted:
                return raw
            return decrypt_for_workspace(raw, workspace_id)

        password = _secret("password")
        conn_str = _secret("connection_string")
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
        ssl, ssl_mode = resolve_tls(conn_type, data)
        return cls(
            id=data["id"],
            name=data["name"],
            type=conn_type,
            role=normalize_connector_role(conn_type, data.get("role")),
            host=str(lifted.get("host") or ""),
            port=listen_port_for_connector(conn_type, lifted.get("port"), lifted.get("host")),
            database=str(lifted.get("database") or ""),
            username=str(lifted.get("username") or ""),
            password=password,
            schema=_resolve_connector_schema(conn_type, data.get("schema"), data.get("username")),
            connection_string=conn_str,
            ssl=ssl,
            ssl_mode=ssl_mode,
            warehouse=data.get("warehouse", ""),
            auth_mode=data.get("auth_mode", ""),
            auth_role=data.get("auth_role", ""),
            api_key=_secret("api_key"),
            service_account=_secret("service_account"),
            private_key=_secret("private_key"),
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

#: Other connector options saved under ``extra`` — SQL Server's operator-
#: declared ``trust_server_certificate`` (generic_sql merges ``extra`` into the
#: driver cfg for connection_options) and a Kafka source's consumer
#: ``group_id`` (D84; without one each route derives its own).
_EXTRA_LIFT_KEYS = _SFTP_TRUST_KEYS + ("trust_server_certificate", "group_id")


def connector_extra_from_payload(data: Mapping[str, Any] | None) -> dict[str, Any]:
    """Persist operator extras, lifting declared trust fields off the payload root.

    Studio / the API may send ``host_key`` next to host/port rather than
    nested under ``extra``. A schedule that later rebuilds the endpoint
    must still see the pin.
    """
    payload = data or {}
    extra = dict(payload.get("extra") or {}) if isinstance(payload.get("extra"), dict) else {}
    for key in _EXTRA_LIFT_KEYS:
        value = payload.get(key)
        if value not in (None, ""):
            extra[key] = value
    return extra


#: Trust extras that identify one endpoint's key — never valid for another host.
_HOST_BOUND_TRUST_KEYS = ("host_key", "known_hosts")


def _endpoint_moved(before_host: Any, data: Mapping[str, Any] | None) -> bool:
    """True when the edit points the connector at a different host.

    A port-only edit keeps the pin: correcting a stale port (D208) reaches the
    same server, and a mismatched key still fails closed at connect.
    """
    host = str((data or {}).get("host") or "").strip().lower()
    return bool(host) and host != str(before_host or "").strip().lower()


def _merged_extra(
    saved: Any, data: Mapping[str, Any] | None, *, endpoint_moved: bool = False
) -> dict[str, Any]:
    """An edit keeps saved extras it does not restate.

    The edit form omits a blank host-key pin the same way it masks a secret;
    replacing ``extra`` wholesale would silently drop the pin and downgrade the
    next SFTP connect to trust-on-first-use. A pin belongs to one endpoint,
    though (D215): when the edit moves the host, the old host's pin and
    known_hosts are dropped unless restated. The policy stays, so a strict
    connector refuses the new host and names its fingerprint to pin.
    """
    base = dict(saved) if isinstance(saved, dict) else {}
    if endpoint_moved:
        for key in _HOST_BOUND_TRUST_KEYS:
            base.pop(key, None)
    return {**base, **connector_extra_from_payload(data)}


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


_DOC_SECRET_KEYS = (
    "password",
    "api_key",
    "service_account",
    "private_key",
    "connection_string",
)


def _doc_to_connector_degraded(doc: dict[str, Any]) -> SavedConnector | None:
    """Listing-only entry for a doc whose stored fields cannot be parsed —
    typically an undecryptable credential. Keeps the connector VISIBLE in
    lists (id/name/type/status read fine) while carrying no credential; the
    X-24 probe path still reports the record as unreadable on Test, and the
    record is marked so the UI can flag 're-save required'."""
    d = dict(doc)
    d["id"] = str(d.pop("_id", d.get("id", "")))
    for key in _DOC_SECRET_KEYS:
        d.pop(key, None)
    if "created_at" in d and hasattr(d["created_at"], "isoformat"):
        d["created_at"] = d["created_at"].isoformat()
    d.pop("updated_at", None)
    try:
        conn = SavedConnector.from_dict(d, secrets_encrypted=False)
    except Exception:
        return None
    conn.extra = dict(conn.extra or {})
    conn.extra["record_unreadable"] = True
    return conn


def _materialize_docs(docs: Any) -> list[SavedConnector]:
    """Per-doc materialization for list paths: one malformed record must
    never zero the whole saved-connector list (prod census defect — two
    corrupt trial docs made brief_workspace + /connectors/saved return []
    while the raw store held 66)."""
    from services.control_plane_store import ControlPlaneUnavailable

    out: list[SavedConnector] = []
    for doc in docs:
        try:
            out.append(_doc_to_connector(doc))
        except ControlPlaneUnavailable:
            # A BYOK record during a control-plane outage is not unreadable: the
            # list is a 503 (retry), never a shorter list or "re-save required".
            raise
        except Exception as exc:
            logger.warning(
                "saved connector doc %s unreadable (%s) — listing degraded entry",
                doc.get("_id") or doc.get("id") if isinstance(doc, dict) else "?",
                type(exc).__name__,
            )
            conn = _doc_to_connector_degraded(doc)
            if conn is not None:
                out.append(conn)
    return out


def _load_all() -> list[SavedConnector]:
    from services.control_plane_store import ControlPlaneUnavailable

    if _use_mongo():
        try:
            coll = _mongo_collection()
            return _materialize_docs(coll.find())
        except ControlPlaneUnavailable:
            raise
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


_FILE_MUTATION_GUARD = threading.RLock()
_FILE_MUTATION_STATE = threading.local()


@contextmanager
def _file_store_mutation() -> Iterator[None]:
    """Serialize one read-modify-write cycle on the file-backed store.

    Every mutation loads the whole list, changes one entry and writes the
    list back. Two writers interleaving (a job's terminal stamp on its
    connectors while a request saves a new one) each read the file, apply
    their own change and write back, the later write silently dropping the
    other's row: the connector a user had just saved was "not found" a
    moment later (D195 run 2 in CI). The write itself stays the atomic
    temp-file replace; this lock makes the cycle around it exclusive across
    threads (RLock) and across worker processes sharing the file (``flock``
    on a sidecar, POSIX only, as for the other JSON stores). Re-entrant, so
    a mutation that calls another (``create_connector`` → ``update_connector``)
    holds one lock.
    """
    depth = getattr(_FILE_MUTATION_STATE, "depth", 0)
    if depth:
        _FILE_MUTATION_STATE.depth = depth + 1
        try:
            yield
        finally:
            _FILE_MUTATION_STATE.depth = depth
        return
    with _FILE_MUTATION_GUARD:
        _FILE_MUTATION_STATE.depth = 1
        store_path = _store_path()
        store_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = store_path.with_suffix(store_path.suffix + ".lock")
        try:
            with open(lock_path, "a+", encoding="utf-8") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            _FILE_MUTATION_STATE.depth = 0


def _save_all(connectors: list[SavedConnector]) -> None:
    store_path = _store_path()
    store_path.parent.mkdir(parents=True, exist_ok=True)
    payload = []
    for c in connectors:
        payload.append(_encrypt_connector_doc(c.to_dict()))
    text = json.dumps({"connectors": payload}, indent=2, default=json_default)
    # Atomic write so a crash mid-write cannot leave a half-written file.
    # One temp file per writer: concurrent saves (parallel test_connector
    # calls stamp their result) shared one ``.tmp`` name, and the second
    # ``replace`` found it already moved (FileNotFoundError, D181 run).
    import tempfile

    fd, tmp_name = tempfile.mkstemp(
        prefix=store_path.name + ".", suffix=".tmp", dir=str(store_path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp_name, store_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


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


def _legacy_visible() -> bool:
    """Whether unscoped (legacy) connectors are visible to the bound caller.

    Always for system scope; for a request, only when its tenant scope says so
    (a fresh signup account's never does - D162)."""
    return bound_allows("")


def _workspace_or_legacy(workspace_id: str) -> dict[str, Any]:
    """Mongo filter: this workspace, plus the legacy rows when the caller may see them."""
    arms: list[dict[str, Any]] = [{"workspace_id": workspace_id}]
    if _legacy_visible():
        arms.append({"workspace_id": {"$in": ["", None]}})
    return {"$or": arms}


def _list_file(role: str | None, workspace_id: str | None = None) -> list[SavedConnector]:
    items = _load_all()
    if workspace_id is not None:
        items = [c for c in items if c.workspace_id == workspace_id or (c.workspace_id == "" and _legacy_visible())]
    if role:
        items = [c for c in items if c.role == role or c.role == "both"]
    return items


def _list_mongo(role: str | None, workspace_id: str | None = None) -> list[SavedConnector]:
    coll = _mongo_collection()
    filters: list[dict[str, Any]] = []
    if role:
        filters.append({"$or": [{"role": role}, {"role": "both"}]})
    if workspace_id is not None:
        filters.append(_workspace_or_legacy(workspace_id))
    query = {"$and": filters} if len(filters) > 1 else (filters[0] if filters else {})
    return _materialize_docs(coll.find(query))


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


def _workspace_scope(workspace_id: str | None) -> str | None:
    """Empty/whitespace workspace ids mean "no scope", not "scope = ''".

    ``connector_name_taken`` already normalizes this way; the list/get paths
    did not, so callers passing ``workspace_id=""`` (e.g. workspace briefing)
    matched only unscoped docs and reported zero saved connectors while
    ``list_connectors()`` showed all of them (prod census defect).
    """
    return (workspace_id or "").strip() or None


def list_connectors(role: str | None = None, workspace_id: str | None = None) -> list[SavedConnector]:
    """Saved connectors, limited to the tenant scope bound to this request.

    ``workspace_id`` narrows further. With nothing bound (schedulers, workers,
    scripts) this is system scope; inside a request a caller that forgot to
    pass a filter still cannot read another workspace's credentials (D154).
    """
    from services.control_plane_store import ControlPlaneUnavailable

    workspace_id = _workspace_scope(workspace_id)
    if _use_mongo():
        try:
            return _visible_connectors(_list_mongo(role, workspace_id))
        except ControlPlaneUnavailable:
            raise  # the file is not this deployment's connector list
        except Exception as exc:
            logger.warning("MongoDB list_connectors failed, falling back to file: %s", exc)
    return _visible_connectors(_list_file(role, workspace_id))


def _visible_connectors(connectors: list[SavedConnector]) -> list[SavedConnector]:
    visible = current_workspace_filter()
    if visible is None:
        return connectors
    return [c for c in connectors if visible.matches(c.workspace_id)]


def _get_file(connector_id: str, workspace_id: str | None = None) -> SavedConnector | None:
    for c in _load_all():
        if c.id == connector_id and (
            workspace_id is None
            or c.workspace_id == workspace_id
            or (c.workspace_id == "" and _legacy_visible())
        ):
            return c
    return None


def _connector_id_query(connector_id: str, workspace_id: str | None = None) -> dict[str, Any]:
    """Match a connector by its public id regardless of storage shape.

    Canonical docs store the connector id in ``_id`` (uuid or ObjectId).
    Docs written through older/other paths (e.g. ``save_connector`` given a
    payload that already carried ``id``) may keep the public id in an ``id``
    field under a generated ``_id``. A lookup restricted to ``_id`` leaves
    those connectors list-visible but untestable — match both.
    """
    query: dict[str, Any] = {"$or": [{"_id": connector_id}, {"id": connector_id}]}
    if workspace_id is not None:
        query = {
            "$and": [
                query,
                _workspace_or_legacy(workspace_id),
            ]
        }
    return query


def _get_mongo(connector_id: str, workspace_id: str | None = None) -> SavedConnector | None:
    coll = _mongo_collection()
    doc = coll.find_one(_connector_id_query(connector_id, workspace_id))
    return _doc_to_connector(doc) if doc else None


def get_connector(connector_id: str, workspace_id: str | None = None) -> SavedConnector | None:
    workspace_id = _workspace_scope(workspace_id)
    found: SavedConnector | None
    if _use_mongo():
        try:
            found = _get_mongo(connector_id, workspace_id)
            return found if found is None or bound_allows(found.workspace_id) else None
        except Exception as exc:
            logger.warning("MongoDB get_connector failed, falling back to file: %s", exc)
    found = _get_file(connector_id, workspace_id)
    return found if found is None or bound_allows(found.workspace_id) else None


def connector_owner_workspace(connector_id: str) -> tuple[bool, str]:
    """``(exists, workspace_id)`` for an id, read without the caller's scope.

    Only for authorization: a router asks who owns an id so it can refuse a
    caller outside that workspace (404) or without write access to it (403)
    instead of silently treating the connector as missing.
    """
    with bound_workspace_filter(None):
        conn = get_connector(connector_id)
    if conn is None:
        return False, ""
    return True, str(getattr(conn, "workspace_id", "") or "")


def create_connector(data: dict[str, Any]) -> SavedConnector:
    from connectors.sftp_common import apply_sftp_uri_endpoint, split_sftp_host_edit

    data = apply_sftp_uri_endpoint(split_sftp_host_edit(data, data.get("type")))
    conn_type = data["type"]
    validate_ssl_mode(conn_type, data)
    ssl, ssl_mode = resolve_tls(conn_type, stamp_ssl_mode(dict(data), data))
    conn = SavedConnector(
        id=str(uuid.uuid4()),
        name=data["name"],
        type=conn_type,
        role=normalize_connector_role(conn_type, data.get("role")),
        host=data.get("host", ""),
        port=listen_port_for_connector(conn_type, data.get("port"), data.get("host")),
        database=data.get("database", ""),
        username=data.get("username", ""),
        password=data.get("password", ""),
        schema=_resolve_connector_schema(conn_type, data.get("schema"), data.get("username")),
        connection_string=data.get("connection_string", ""),
        ssl=ssl,
        ssl_mode=ssl_mode,
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

    with _file_store_mutation():
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
        if not v or v == "****" or v == REDACTED:
            return True
        if "****" in v or "<redacted>" in v.lower():
            return True
    return False


SECRET_CLEAR_SENTINEL = "__clear__"


def _coerce_secret_update(new: Any, existing: Any) -> Any:
    """Preserve an existing secret when the update payload only carries a placeholder.

    ``SECRET_CLEAR_SENTINEL`` is the explicit erase: it writes ``""`` over the
    stored value. Anything masked/empty keeps the stored value verbatim —
    ciphertext is never decrypted just to re-encrypt it on save.
    """
    if isinstance(new, str) and new == SECRET_CLEAR_SENTINEL:
        return ""
    if _is_masked_placeholder(new) and existing not in (None, ""):
        return existing
    return new


def update_connector(connector_id: str, data: dict[str, Any], workspace_id: str | None = None) -> SavedConnector | None:
    workspace_id = _workspace_scope(workspace_id)
    if current_workspace_filter() is not None and get_connector(connector_id, workspace_id) is None:
        return None
    if str(data.get("host") or "").strip():
        # D208: a URL-shaped SFTP host carries the dial port; split it here so
        # the stored port is the URL's, never the one the record had before.
        from connectors.sftp_common import split_sftp_host_edit

        before = get_connector(connector_id, workspace_id)
        if before is not None:
            data = split_sftp_host_edit(data, data.get("type") or before.type, before.port)
    def _merge(existing: SavedConnector) -> SavedConnector:
        validate_ssl_mode(str(data.get("type") or existing.type), data)
        merged = stamp_ssl_mode({**existing.to_dict(), **data, "id": connector_id}, data)
        merged["extra"] = _merged_extra(
            existing.extra, data,
            endpoint_moved=_endpoint_moved(existing.host, data),
        )
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
        validate_merged_ssl_mode(data, merged)
        return SavedConnector.from_dict(merged, secrets_encrypted=False)

    if _use_mongo():
        try:
            coll = _mongo_collection()
            query = _connector_id_query(connector_id, workspace_id)
            raw = coll.find_one(query)
            if not raw:
                return None
            # Merge on the raw document, not the decrypted connector: a stored
            # enc:v1 secret is preserved byte-for-byte when the update carries
            # only a masked/empty placeholder, and a legacy plaintext secret is
            # re-encrypted by _encrypt_connector_doc — every successful update
            # heals the record instead of refusing it.
            validate_ssl_mode(str(data.get("type") or raw.get("type") or ""), data)
            merged = stamp_ssl_mode({**raw, **data, "_id": raw.get("_id")}, data)
            merged["extra"] = _merged_extra(
                raw.get("extra"), data,
                endpoint_moved=_endpoint_moved(raw.get("host"), data),
            )
            merged.pop("id", None)
            for secret_field in _DOC_SECRET_KEYS:
                merged[secret_field] = _coerce_secret_update(
                    merged.get(secret_field), raw.get(secret_field)
                )
            merged["role"] = normalize_connector_role(
                str(merged.get("type") or raw.get("type") or ""),
                merged.get("role"),
            )
            # ``merged`` mixes stored ciphertext with the edit's plaintext (the
            # Confirm route re-sends the decrypted password). Encrypt first so
            # every secret is ciphertext, then decrypt only the DSN the TLS check
            # reads: decrypting the mix made production's vault refuse the
            # plaintext and every secret-bearing edit fail (D292).
            doc = _encrypt_connector_doc(merged)
            if _TLS_PLAN_FIELDS & set(data):
                plain_dsn = decrypt_secret(
                    str(doc.get("connection_string") or ""),
                    tenant_id=tenant_id_from_workspace(doc.get("workspace_id") or ""),
                )
                validate_merged_ssl_mode(data, {**doc, "connection_string": plain_dsn})
            coll.replace_one(query, doc)
            try:
                return _doc_to_connector(doc)
            except Exception:
                # The write already healed what it could; report the stored
                # shape without decrypting rather than failing the update.
                return SavedConnector.from_dict(
                    {**doc, "id": str(doc.get("_id") or connector_id)},
                    secrets_encrypted=False,
                )
        except ValueError as exc:
            # A refused TLS choice is the request's fault (422 at the routes),
            # not a store failure. Nothing was written.
            if "ssl_mode" in str(exc):
                raise
            raise _mongo_write_failed("update", connector_id, exc) from exc
        except Exception as exc:
            raise _mongo_write_failed("update", connector_id, exc) from exc

    with _file_store_mutation():
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


def delete_connector(
    connector_id: str, workspace_id: str | None = None, *, exact_workspace: bool = False
) -> bool:
    """Delete a saved connector. ``exact_workspace`` (workspace teardown) matches
    ``workspace_id`` exactly — never an unscoped legacy row."""
    workspace_id = _workspace_scope(workspace_id)
    if current_workspace_filter() is not None and get_connector(connector_id, workspace_id) is None:
        return False
    if exact_workspace and not workspace_id:
        raise ValueError("an exact-workspace delete needs a workspace")
    if _use_mongo():
        try:
            coll = _mongo_collection()
            query = (
                {"$and": [{"$or": [{"_id": connector_id}, {"id": connector_id}]}, {"workspace_id": workspace_id}]}
                if exact_workspace
                else _connector_id_query(connector_id, workspace_id)
            )
            result = coll.delete_one(query)
            return result.deleted_count > 0
        except Exception as exc:
            raise _mongo_write_failed("delete", connector_id, exc) from exc

    with _file_store_mutation():
        connectors = _load_all()
        before = len(connectors)
        filtered = [
            c for c in connectors
            if not (
                c.id == connector_id
                and (
                    c.workspace_id == workspace_id
                    if exact_workspace
                    else (workspace_id is None or c.workspace_id == workspace_id or (c.workspace_id == "" and _legacy_visible()))
                )
            )
        ]
        if len(filtered) == before:
            return False
        _save_all(filtered)
    return True


def _patch_file_connector(connector_id: str, patch: dict[str, Any]) -> bool:
    """Merge ``patch`` into one stored connector of the file store; False when absent."""
    with _file_store_mutation():
        connectors = _load_all()
        for i, c in enumerate(connectors):
            if c.id == connector_id:
                connectors[i] = SavedConnector.from_dict({**c.to_dict(), **patch}, secrets_encrypted=False)
                _save_all(connectors)
                return True
    return False


def mark_tested(connector_id: str, ok: bool, *, tested_at: str | None = None) -> None:
    """Persist probe result. ``last_test_ok`` is the authority for UI health.

    ``tested_at`` is when the probe ran, if not now (a create that probed
    before its Confirm).
    """
    tested_at = tested_at or _now()
    patch = {"last_tested_at": tested_at, "last_test_ok": bool(ok)}
    if _use_mongo():
        try:
            coll = _mongo_collection()
            coll.update_one(_connector_id_query(connector_id), {"$set": patch})
            # Keep file mirror in sync when present so dual-backend reads cannot
            # resurrect a stale failed badge after a green probe.
            try:
                _patch_file_connector(connector_id, patch)
            except Exception as mirror_exc:
                logger.debug("File mirror mark_tested skipped: %s", mirror_exc)
            return
        except Exception as exc:
            logger.warning("MongoDB mark_tested failed, falling back to file: %s", exc)

    _patch_file_connector(connector_id, patch)


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
                result = coll.update_one(_connector_id_query(cid), {"$set": patch})
                if result.matched_count:
                    stamped += 1
                    continue
            except Exception as exc:  # noqa: BLE001 - file store is the fallback
                logger.warning("MongoDB note_transfer_succeeded failed, falling back to file: %s", exc)
        if _patch_file_connector(cid, patch):
            stamped += 1
    return stamped


def _parse_instant(raw: Any) -> datetime | None:
    from services.instant_text import parse_iso_instant

    return parse_iso_instant(raw, assume_utc=True)


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
                result = coll.update_one(_connector_id_query(cid), {"$set": patch})
                if result.matched_count:
                    stamped += 1
                    try:
                        _patch_file_connector(cid, patch)
                    except Exception as mirror_exc:
                        logger.debug("File mirror mark_used skipped: %s", mirror_exc)
                    continue
            except Exception as exc:
                logger.warning("MongoDB mark_used failed, falling back to file: %s", exc)
        if _patch_file_connector(cid, patch):
            stamped += 1
    return stamped


_RESPONSE_SECRET_KEYS = frozenset(
    {
        "password",
        "api_key",
        "service_account",
        "private_key",
        "private_key_passphrase",
        "secret",
        "token",
        "secret_access_key",
    }
)


def _redact_response_value(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if str(key).lower() in _RESPONSE_SECRET_KEYS and item:
                redacted[key] = "****"
            elif str(key).lower() in {"connection_string", "url", "uri"} and item:
                redacted[key] = _mask_conn_str(str(item))
            else:
                redacted[key] = _redact_response_value(item)
        return redacted
    if isinstance(value, list):
        return [_redact_response_value(item) for item in value]
    return value


def mask_connector(c: SavedConnector) -> dict[str, Any]:
    d = c.to_dict()
    from services.dial_target import displayed_dial

    # Computed from the plaintext URI before it is masked. The card must show
    # the host that will be dialed, including a loopback that is really loopback.
    dial_host, dial_loop = displayed_dial(
        str(d.get("host") or ""),
        str(d.get("connection_string") or ""),
        str(d.get("endpoint_url") or ""),
    )
    d["dial_host"] = dial_host
    d["dial_host_loopback"] = dial_loop
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
    if isinstance(d.get("extra"), dict):
        d["extra"] = _redact_response_value(d["extra"])
    d.setdefault("workspace_id", "")
    d.setdefault("endpoint_url", "")
    d.setdefault("path_style", False)
    return d


def _mask_conn_str(s: str) -> str:
    return mask_secrets_in_text(s)
