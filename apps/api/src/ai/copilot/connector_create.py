"""Parse connector credentials from Pilot chat messages / tool args."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote, urlparse

from connectors.sql_dsn import parse_sql_url


_DEFAULT_PORTS = {
    "postgresql": 5432,
    "postgres": 5432,
    "mysql": 3306,
    "mariadb": 3306,
    "mongodb": 27017,
    "snowflake": 443,
    "redis": 6379,
    "sqlserver": 1433,
    "oracle": 1521,
    "redshift": 5439,
}

_TYPE_ALIASES = {
    "postgres": "postgresql",
    "pg": "postgresql",
    "psql": "postgresql",
    "mariadb": "mysql",
    "mongo": "mongodb",
    "mssql": "sqlserver",
    "sql server": "sqlserver",
}


def normalize_connector_type(raw: str) -> str:
    t = (raw or "").strip().lower().replace("_", " ")
    t = _TYPE_ALIASES.get(t, t.replace(" ", ""))
    if t == "postgres":
        t = "postgresql"
    return t


def parse_mongodb_url(url: str) -> dict[str, Any]:
    raw = (url or "").strip()
    if not raw.lower().startswith(("mongodb://", "mongodb+srv://")):
        return {}
    parsed = urlparse(raw)
    database = unquote((parsed.path or "").lstrip("/").split("/")[0] or "")
    return {
        "type": "mongodb",
        "host": parsed.hostname or "",
        "port": int(parsed.port) if parsed.port else (27017 if parsed.scheme == "mongodb" else 0),
        "username": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
        "connection_string": raw,
    }


def extract_url_credentials(message: str) -> dict[str, Any] | None:
    """Find the first database URL in free text."""
    text = message or ""
    m = re.search(r"(postgresql(?:\+psycopg2)?://|postgres://)[^\s\"']+", text, re.I)
    if m:
        parsed = parse_sql_url(m.group(0), family="postgresql")
        if parsed.get("host"):
            return {
                "type": "postgresql",
                "connection_string": m.group(0).rstrip(".,;"),
                **parsed,
            }
    m = re.search(r"(mysql(?:\+pymysql)?://|mariadb://)[^\s\"']+", text, re.I)
    if m:
        parsed = parse_sql_url(m.group(0), family="mysql")
        if parsed.get("host"):
            return {
                "type": "mysql",
                "connection_string": m.group(0).rstrip(".,;"),
                **parsed,
            }
    m = re.search(r"(mongodb(?:\+srv)?://)[^\s\"']+", text, re.I)
    if m:
        parsed = parse_mongodb_url(m.group(0).rstrip(".,;"))
        if parsed.get("host") or parsed.get("connection_string"):
            return parsed
    return None


def extract_field_credentials(message: str) -> dict[str, Any]:
    """Parse host/user/password/port/database lines from a chat message."""
    lower = message.lower()
    out: dict[str, Any] = {}

    type_m = re.search(
        r"\b(postgresql|postgres|mysql|mariadb|mongodb|mongo|snowflake|redis|sqlserver|oracle|redshift)\b",
        lower,
    )
    if type_m:
        out["type"] = normalize_connector_type(type_m.group(1))

    def _field(*names: str) -> str:
        for name in names:
            m = re.search(
                rf"(?:^|\n|,|;)\s*{name}\s*[:=]\s*([^\n,;]+)",
                message,
                re.I,
            )
            if m:
                return m.group(1).strip().strip("\"'")
        return ""

    host = _field("host", "hostname", "server", "mysql host", "postgres host")
    if host:
        out["host"] = host
    port_s = _field("port", "mysql port", "postgres port")
    if port_s.isdigit():
        out["port"] = int(port_s)
    db = _field("database", "db", "dbname")
    if db:
        out["database"] = db
    user = _field("username", "user", "uid")
    if user:
        out["username"] = user
    password = _field("password", "pass", "pwd")
    if password:
        out["password"] = password
    name = _field("name", "connector name", "label")
    if name:
        out["name"] = name
    return out


def wants_create_connector(message: str) -> bool:
    lower = (message or "").lower()
    verbs = (
        "create connector",
        "add connector",
        "save connector",
        "new connector",
        "set up connector",
        "setup connector",
        "connect to",
        "create a connection",
        "add a connection",
        "save this connection",
        "make a connector",
        "register connector",
    )
    if any(v in lower for v in verbs):
        return True
    # Credentials pasted with an explicit ask to save/use
    if extract_url_credentials(message) and any(
        w in lower for w in ("save", "create", "add", "connector", "connection", "use this")
    ):
        return True
    return False


def build_connector_draft(message: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
    """Merge tool args + message-extracted credentials into a connector draft."""
    args = dict(args or {})
    from_url = extract_url_credentials(message) or {}
    from_fields = extract_field_credentials(message)

    merged: dict[str, Any] = {**from_fields, **from_url}
    for k, v in args.items():
        if v not in (None, "", 0):
            merged[k] = v

    ctype = normalize_connector_type(str(merged.get("type") or ""))
    if not ctype and merged.get("connection_string"):
        cs = str(merged["connection_string"]).lower()
        if cs.startswith("mysql"):
            ctype = "mysql"
        elif cs.startswith("postgres"):
            ctype = "postgresql"
        elif cs.startswith("mongodb"):
            ctype = "mongodb"
    merged["type"] = ctype or "postgresql"

    port = int(merged.get("port") or 0) or _DEFAULT_PORTS.get(merged["type"], 5432)
    merged["port"] = port

    if not merged.get("name"):
        host = str(merged.get("host") or "db")
        label = host.split(".")[0] if host else merged["type"]
        merged["name"] = f"{merged['type'].title()} · {label}"[:64]

    merged.setdefault("database", "")
    merged.setdefault("username", "")
    merged.setdefault("password", "")
    merged.setdefault("host", "")
    merged.setdefault("connection_string", "")
    merged.setdefault("ssl", False)
    from services.dialect_profiles import default_schema_for

    merged.setdefault("schema", default_schema_for(merged["type"]) or "")
    merged.setdefault("auth_mode", "connection_string" if merged.get("connection_string") else "user_pass")
    return merged


def draft_is_complete(draft: dict[str, Any]) -> tuple[bool, str]:
    ctype = draft.get("type") or ""
    if draft.get("connection_string"):
        return True, ""
    if not draft.get("host"):
        return False, "I need a host (or a full connection URL) to create this connector."
    if ctype in ("postgresql", "mysql", "mariadb", "sqlserver", "oracle", "redshift"):
        if not draft.get("username") or not draft.get("password"):
            return False, "I need username and password (or a full connection URL)."
        if not draft.get("database"):
            return False, "Which database name should I use?"
    if ctype == "mongodb" and not (draft.get("connection_string") or draft.get("host")):
        return False, "I need a MongoDB URI or host."
    return True, ""
