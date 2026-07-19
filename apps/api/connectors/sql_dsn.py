"""Shared SQL DSN helpers — URL parse + private-cloud host hints."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote, urlparse


_MYSQL_SCHEMES = frozenset({"mysql", "mysql+pymysql", "mariadb"})
_PG_SCHEMES = frozenset({"postgresql", "postgres", "postgresql+psycopg2", "pgsql"})

# user:pass@host:port/db  (scheme omitted — common Railway paste mistake)
_USERINFO_AT_HOST = re.compile(
    r"^(?P<user>[^:/@\s]+):(?P<password>[^@\s]+)@(?P<host>[^:/?\s]+)(?::(?P<port>\d+))?(?:/(?P<db>[^?\s]*))?",
    re.I,
)


def normalize_sql_dsn(url: str, *, family: str) -> str:
    """Ensure a SQL DSN has a scheme so urlparse / drivers can read it.

    Accepts common pastes like:
      postgres:secret@tokaido.proxy.rlwy.net:27396/railway
    and rewrites to:
      postgresql://postgres:secret@tokaido.proxy.rlwy.net:27396/railway
    """
    raw = (url or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        # postgres:// is fine for psycopg2; keep as-is
        return raw
    if _USERINFO_AT_HOST.match(raw):
        scheme = "mysql://" if family == "mysql" else "postgresql://"
        return scheme + raw
    return raw


def parse_sql_url(url: str, *, family: str) -> dict[str, Any]:
    """Parse a mysql:// or postgresql:// URL into discrete connection fields."""
    raw = normalize_sql_dsn((url or "").strip(), family=family)
    if not raw:
        return {}
    if "://" not in raw:
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
    raw = (value or "").strip()
    if not raw:
        return False
    lower = raw.lower()
    if family == "mysql":
        if lower.startswith(("mysql://", "mysql+pymysql://", "mariadb://")):
            return True
    elif lower.startswith(("postgresql://", "postgres://", "postgresql+psycopg2://", "pgsql://")):
        return True
    # Scheme-less user:pass@host…
    return bool(_USERINFO_AT_HOST.match(raw))


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
    """Merge form fields + connection string (+ URL pasted into host).

    When a connection string / DSN parses successfully, it is authoritative for
    host/port/user/password/database. Form defaults like localhost:5432 must not
    override a Railway public proxy URL (that was breaking Test connection).
    Discrete fields only fill blanks the URL did not provide.
    """
    normalized_cs = normalize_sql_dsn(connection_string, family=family)
    parsed = parse_sql_url(normalized_cs, family=family)
    # Allow pasting a full DSN into the Host field by mistake.
    host_raw = (host or "").strip()
    if not parsed and looks_like_sql_url(host_raw, family=family):
        parsed = parse_sql_url(normalize_sql_dsn(host_raw, family=family), family=family)
        host_raw = ""

    form_host = host_raw
    # Treat common placeholder defaults as empty so they never beat a real DSN.
    if form_host.lower() in ("localhost", "127.0.0.1", "host.docker.internal"):
        # Keep only if the URL did not supply a host
        if parsed.get("host"):
            form_host = ""

    form_port = int(port or 0)
    if form_port in (0, default_port) and parsed.get("port"):
        # Default catalog port (5432/3306) must not override proxy ports (27396…)
        form_port = 0

    if parsed.get("host"):
        final_host = str(parsed.get("host") or "") or form_host or "localhost"
        final_port = int(parsed.get("port") or 0) or form_port or default_port
        final_user = str(parsed.get("username") or "") or (username or "").strip()
        if parsed.get("password") not in (None, ""):
            final_password = str(parsed.get("password") or "")
        elif password not in (None, ""):
            final_password = password
        else:
            final_password = ""
        final_database = str(parsed.get("database") or "") or (database or "").strip()
    else:
        final_host = form_host or "localhost"
        final_port = form_port or default_port
        final_user = (username or "").strip()
        final_password = password if password not in (None, "") else ""
        final_database = (database or "").strip()

    return {
        "host": final_host,
        "port": int(final_port),
        "username": final_user,
        "password": final_password,
        "database": final_database,
        "connection_string": normalized_cs,
    }


def is_running_on_railway() -> bool:
    """True when this API process is inside a Railway deployment."""
    try:
        from services.platform_config import is_railway

        return bool(is_railway())
    except Exception:
        import os

        return bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_SERVICE_ID"))


def uses_railway_internal(host: str = "", connection_string: str = "") -> bool:
    blob = f"{host or ''} {connection_string or ''}".lower()
    return ".railway.internal" in blob


def private_cloud_host_hint(host: str = "", connection_string: str = "") -> str:
    """Plain-language hint when users paste provider-private hostnames.

    Inside Railway, *.railway.internal is valid — do not push them to the public proxy.
    Outside Railway, private DNS will not resolve; steer them to *.proxy.rlwy.net.
    """
    blob = f"{host or ''} {connection_string or ''}".lower()
    if ".railway.internal" in blob:
        if is_running_on_railway():
            return (
                " Could not reach this Railway private hostname from the API service. "
                "Confirm the database is in the *same* Railway project, the host matches "
                "the service private domain (e.g. mysql.railway.internal), and use the "
                "private port (MySQL 3306 / Postgres 5432) — not the public proxy port."
            )
        return (
            " This is a Railway *private* hostname (*.railway.internal). "
            "It only works when the DataFlow API runs inside the same Railway project. "
            "Use the public proxy instead: host like *.proxy.rlwy.net and the public port "
            "from Railway (TCP Proxy) — e.g. MySQL often uses port 32253, not 3306."
        )
    if blob.strip().endswith(".internal") or ".internal:" in blob or ("@" in blob and ".internal" in blob):
        if is_running_on_railway():
            return (
                " Private hostname failed. Check the service is linked to this Railway project "
                "and that host/port match the provider’s private networking docs."
            )
        return (
            " This hostname looks private/internal to a cloud network. "
            "Use the provider's public proxy host and port unless DataFlow is running on that same private network."
        )
    return ""
