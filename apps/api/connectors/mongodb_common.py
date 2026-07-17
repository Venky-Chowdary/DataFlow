"""Shared MongoDB URI helpers and client cache for reader, writer, and adapter probes."""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

# PyMongo clients manage their own connection pools and are thread-safe. Reusing a
# single client per connection string removes per-batch connection handshake
# overhead, which is the dominant cost for large streaming transfers.
_mongo_client_cache: dict[str, Any] = {}


def _mongo_client(conn_str: str) -> Any:
    """Return a cached MongoClient for ``conn_str``."""
    from pymongo import MongoClient

    if conn_str not in _mongo_client_cache:
        _mongo_client_cache[conn_str] = MongoClient(
            conn_str,
            serverSelectionTimeoutMS=10000,
            socketTimeoutMS=120000,
            connectTimeoutMS=10000,
            maxPoolSize=10,
        )
    return _mongo_client_cache[conn_str]


def _is_localhost(uri: str) -> bool:
    """Detect whether a URI points to localhost and should be returned as-is."""
    parsed = urlparse(uri)
    netloc = parsed.netloc or ""
    # Strip userinfo and port.
    host = (netloc.split(":")[-2] if "@" in netloc and ":" in netloc.split("@")[-1] else netloc.split(":")[0])
    if "@" in host:
        host = host.split("@")[-1]
    return host.lower() in ("localhost", "127.0.0.1", "::1")


def mongodb_database_from_uri(uri: str) -> str:
    """Return the database name encoded in a MongoDB URI path, if any."""
    try:
        parsed = urlparse(uri.strip())
        path = (parsed.path or "").strip("/")
        if path and not path.startswith("?"):
            return path
    except Exception:
        pass
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
    """
    uri = connection_string.strip()
    host = host or "localhost"
    if not uri:
        netloc = f"{username}:{password}@{host}:{port or 27017}" if username and password else f"{host}:{port or 27017}"
        uri = f"mongodb://{netloc}/"

    if not uri.startswith(("mongodb://", "mongodb+srv://")):
        return uri

    parsed = urlparse(uri)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    # If the user pasted a connection string pointing to localhost but also
    # filled host/port, prefer the explicit form fields.
    if connection_string.strip() and _is_localhost(uri) and host and (username or password):
        return normalize_mongodb_connection_string(
            "", database=database, host=host, port=port, username=username, password=password,
            auth_source=auth_source,
        )

    path = parsed.path
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
    return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, query, parsed.fragment))
