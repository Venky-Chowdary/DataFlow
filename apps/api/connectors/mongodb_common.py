"""Shared MongoDB URI helpers and client cache for reader, writer, and adapter probes."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import parse_qs, urlencode

# PyMongo clients manage their own connection pools and are thread-safe. Reusing a
# single client per connection string removes per-batch connection handshake
# overhead, which is the dominant cost for large streaming transfers.
_mongo_client_cache: dict[str, Any] = {}

logger = logging.getLogger(__name__)


def _new_mongo_client(conn_str: str) -> Any:
    """Build a fresh, *uncached* MongoClient.

    CDC change-stream consumers own their client lifecycle (they call
    ``close()`` on job end / lease release). They must NOT share the process
    pool, or a single stream shutdown would kill every concurrent bulk reader
    and writer on the same URI. Bulk paths use :func:`_mongo_client` (pooled);
    streaming paths use this.
    """
    from pymongo import MongoClient

    return MongoClient(
        conn_str,
        serverSelectionTimeoutMS=10000,
        socketTimeoutMS=120000,
        connectTimeoutMS=10000,
        maxPoolSize=10,
    )


def _client_is_closed(client: Any) -> bool:
    """Best-effort detection that a MongoClient has been closed.

    PyMongo exposes no public ``closed`` flag, but a closed client raises
    ``InvalidOperation`` on every operation. ``_topology._closed`` flips to
    ``True`` on ``close()`` and is stable across 4.x — use it defensively so a
    poisoned cache entry is rebuilt rather than handed back dead.
    """
    topology = getattr(client, "_topology", None)
    if topology is None:
        return False
    return bool(getattr(topology, "_closed", False))


def _mongo_client(conn_str: str) -> Any:
    """Return a cached, live MongoClient for ``conn_str``.

    Self-healing: if the cached client was closed by another code path, evict
    and rebuild it so callers never receive a ``Cannot use MongoClient after
    close`` client. This makes the shared pool robust even when a sibling job
    (e.g. a CDC stream on an older build) closes a client it did not own.
    """
    cached = _mongo_client_cache.get(conn_str)
    if cached is not None and not _client_is_closed(cached):
        return cached
    if cached is not None:
        _mongo_client_cache.pop(conn_str, None)
    client = _new_mongo_client(conn_str)
    _mongo_client_cache[conn_str] = client
    return client


def close_mongo_client(conn_str: str) -> None:
    """Close and evict the pooled client for ``conn_str`` (never leave it dead)."""
    client = _mongo_client_cache.pop(conn_str, None)
    if client is not None:
        try:
            client.close()
        except Exception as exc:  # pragma: no cover — teardown must not raise
            logger.warning("Exception suppressed during mongo client close: %s", exc)


def _is_localhost(uri: str) -> bool:
    """Detect whether a URI points to localhost and should be returned as-is."""
    from connectors.url_authority import parse_url_authority

    host = parse_url_authority(uri).host.lower()
    return host in ("localhost", "127.0.0.1", "::1")


def mongodb_database_from_uri(uri: str) -> str:
    """Return the database name encoded in a MongoDB URI path, if any."""
    from connectors.url_authority import parse_url_authority

    try:
        path = parse_url_authority(uri.strip()).path.strip("/")
        if path and not path.startswith("?"):
            return path
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return ""


def normalize_mongodb_connection_string(
    connection_string: str = "",
    *,
    database: str = "",
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    ssl: bool = False,
    auth_source: str = "",
) -> str:
    """Return a MongoDB URI that the driver can authenticate with.

    If a connection string is provided, it is used as the base.  When a database
    is supplied and the URI does not already include a database path, the
    database is appended as the default database.  authSource is left as-is if
    present in the URL; otherwise it defaults to the database name (or the
    explicit `auth_source` argument).  This lets a user connect to `trueresume`
    while the user lives in the `admin` database by adding `?authSource=admin`.

    When the pasted URI has no userinfo but form username/password are set,
    credentials are injected into the netloc (common for Railway/Atlas pastes
    that separate host URI from login fields).
    """
    from dataclasses import replace
    from urllib.parse import quote_plus

    from connectors.url_authority import parse_url_authority, rebuild_url

    uri = connection_string.strip()
    host = host or "localhost"
    if not uri:
        if username and password:
            netloc = f"{quote_plus(username)}:{quote_plus(password)}@{host}:{port or 27017}"
        elif username:
            netloc = f"{quote_plus(username)}@{host}:{port or 27017}"
        else:
            netloc = f"{host}:{port or 27017}"
        uri = f"mongodb://{netloc}/"

    if not uri.startswith(("mongodb://", "mongodb+srv://")):
        return uri

    authority = parse_url_authority(uri)
    qs = parse_qs(authority.query, keep_blank_values=True)

    # If the user pasted a connection string pointing to localhost but also
    # filled host/port, prefer the explicit form fields.
    if connection_string.strip() and _is_localhost(uri) and host and (username or password):
        return normalize_mongodb_connection_string(
            "", database=database, host=host, port=port, username=username, password=password,
            ssl=ssl, auth_source=auth_source,
        )

    user = username or authority.user
    secret = password or authority.password
    path = authority.path
    if database:
        if not path or path == "/":
            path = f"/{database}"

    # Determine authSource precedence:
    # 1. explicit auth_source argument / form field
    # 2. authSource query parameter already in the URL
    # 3. the database name (most common when using Database field)
    # 4. admin fallback when no database is provided
    effective_auth_source = auth_source.strip()
    if not effective_auth_source:
        effective_auth_source = qs.get("authSource", qs.get("authsource", [""]))[0]
    if not effective_auth_source:
        effective_auth_source = database or "admin"
    qs["authSource"] = [effective_auth_source]

    if ssl and "ssl" not in qs and "tls" not in qs:
        qs["ssl"] = ["true"]

    query = urlencode({k: v[0] if v else "" for k, v in qs.items()}, doseq=False)
    return rebuild_url(replace(authority, path=path, query=query), user=user, password=secret)
