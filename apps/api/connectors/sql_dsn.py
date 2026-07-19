"""Shared SQL DSN helpers — URL parse + private-cloud host hints."""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse


_MYSQL_SCHEMES = frozenset({"mysql", "mysql+pymysql", "mariadb"})
_PG_SCHEMES = frozenset({"postgresql", "postgres", "postgresql+psycopg2", "pgsql"})


def parse_sql_url(url: str, *, family: str) -> dict[str, Any]:
    """Parse a mysql:// or postgresql:// URL into discrete connection fields."""
    raw = (url or "").strip()
    if not raw or "://" not in raw:
        return {}
    parsed = urlparse(raw)
    scheme = (parsed.scheme or "").lower()
    allowed = _MYSQL_SCHEMES if family == "mysql" else _PG_SCHEMES
    if scheme not in allowed:
        return {}
    database = unquote((parsed.path or "").lstrip("/").split("/")[0] or "")
    return {
        "host": parsed.hostname or "",
        "port": int(parsed.port) if parsed.port else 0,
        "username": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
    }


def looks_like_sql_url(value: str, *, family: str) -> bool:
    raw = (value or "").strip().lower()
    if family == "mysql":
        return raw.startswith(("mysql://", "mysql+pymysql://", "mariadb://"))
    return raw.startswith(("postgresql://", "postgres://", "postgresql+psycopg2://", "pgsql://"))


def resolve_sql_endpoint(
    *,
    family: str,
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    default_port: int,
) -> dict[str, Any]:
    """Merge form fields + connection string (+ URL pasted into host)."""
    parsed = parse_sql_url(connection_string, family=family)
    # Allow pasting a full DSN into the Host field by mistake.
    if not parsed and looks_like_sql_url(host, family=family):
        parsed = parse_sql_url(host, family=family)
        host = ""

    final_host = (host or "").strip() or str(parsed.get("host") or "") or "localhost"
    final_port = int(port or 0) or int(parsed.get("port") or 0) or default_port
    final_user = (username or "").strip() or str(parsed.get("username") or "")
    if password not in (None, ""):
        final_password = password
    else:
        final_password = str(parsed.get("password") or "")
    final_database = (database or "").strip() or str(parsed.get("database") or "")

    return {
        "host": final_host,
        "port": final_port,
        "username": final_user,
        "password": final_password,
        "database": final_database,
        "connection_string": (connection_string or "").strip(),
    }


def private_cloud_host_hint(host: str = "", connection_string: str = "") -> str:
    """Plain-language hint when users paste provider-private hostnames."""
    blob = f"{host or ''} {connection_string or ''}".lower()
    if ".railway.internal" in blob:
        return (
            " This looks like a Railway *private* hostname (*.railway.internal), "
            "which only works when DataFlow runs inside the same Railway project. "
            "From your laptop or an external API, use the public proxy host "
            "(*.proxy.rlwy.net) and its public port from the Railway dashboard."
        )
    if blob.strip().endswith(".internal") or ".internal:" in blob or "@" in blob and ".internal" in blob:
        return (
            " This hostname looks private/internal to a cloud network. "
            "Use the provider's public proxy host and port unless DataFlow is running on that same private network."
        )
    return ""
