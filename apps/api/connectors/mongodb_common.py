"""Shared MongoDB URI helpers for reader, writer, and adapter probes."""

from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


def _is_localhost(uri: str) -> bool:
    """Detect whether a URI points to localhost and should be returned as-is."""
    parsed = urlparse(uri)
    netloc = parsed.netloc or ""
    # Strip userinfo and port.
    host = (netloc.split(":")[-2] if "@" in netloc and ":" in netloc.split("@")[-1] else netloc.split(":")[0])
    if "@" in host:
        host = host.split("@")[-1]
    return host.lower() in ("localhost", "127.0.0.1", "::1")


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
