"""Parse connector credentials from Pilot chat messages / tool args."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote, urlparse

from connectors.sql_dsn import parse_sql_url


# Longer tokens first. ``postgres`` must not swallow ``postgresql``, and
# ``mongo`` must not swallow ``mongodb``.
_DRIVER_TOKENS: tuple[tuple[str, str], ...] = (
    ("elasticsearch", "elasticsearch"),
    ("opensearch", "elasticsearch"),
    ("postgresql", "postgresql"),
    ("pgvector", "pgvector"),
    ("weaviate", "weaviate"),
    ("qdrant", "qdrant"),
    ("snowflake", "snowflake"),
    ("sqlserver", "sqlserver"),
    ("redshift", "redshift"),
    ("influxdb", "influxdb"),
    ("mariadb", "mysql"),
    ("mongodb", "mongodb"),
    ("postgres", "postgresql"),
    ("neo4j", "neo4j"),
    ("oracle", "oracle"),
    ("kafka", "kafka"),
    ("mysql", "mysql"),
    ("redis", "redis"),
    ("mongo", "mongodb"),
)

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
    m = re.search(r"(rediss?://)[^\s\"']+", text, re.I)
    if m:
        raw = m.group(0).rstrip(".,;")
        parsed = urlparse(raw)
        database = unquote((parsed.path or "").lstrip("/").split("/")[0] or "")
        return {
            "type": "redis",
            "connection_string": raw,
            "host": parsed.hostname or "",
            "port": int(parsed.port) if parsed.port else 6379,
            "username": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "database": database,
        }
    return None


def infer_driver_from_text(*parts: str) -> str:
    """Driver named in a connector label or chat line, or "" when none is.

    Word boundaries keep ``QA Redis Box`` on Redis and leave ``Demo PG`` alone
    until the message actually says postgres. An empty result is not a type:
    the caller still defaults a truly unnamed engine to PostgreSQL.
    """
    blob = " ".join(part for part in parts if part).lower()
    if not blob.strip():
        return ""
    for token, driver in _DRIVER_TOKENS:
        if re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", blob):
            return driver
    return ""


#: An endpoint stated in prose rather than labelled: "at localhost:5433",
#: "@ db.acme.com", "on 10.0.0.7:3306". The host shape is deliberately strict —
#: ``localhost``, an IPv4 address or a dotted hostname — so an ordinary "on
#: <connector name>" clause cannot be read as a host.
_ENDPOINT_PHRASE = re.compile(
    r"\b(?:at|on|@)\s+"
    r"(localhost|\d{1,3}(?:\.\d{1,3}){3}|[a-z0-9][\w-]*(?:\.[\w-]+)+)"
    r"(?::(\d{2,5}))?\b",
    re.I,
)


def extract_field_credentials(message: str) -> dict[str, Any]:
    """Parse host/user/password/port/database from labeled lines or inline prose."""
    lower = message.lower()
    out: dict[str, Any] = {}

    type_m = re.search(
        r"\b(elasticsearch|opensearch|pgvector|weaviate|qdrant|postgresql|postgres|"
        r"neo4j|kafka|mysql|mariadb|mongodb|mongo|snowflake|redis|sqlserver|oracle|redshift)\b",
        lower,
    )
    if type_m:
        out["type"] = normalize_connector_type(type_m.group(1))

    def _field(*names: str) -> str:
        for name in names:
            # Labeled: "host: db.example.com" / "host = …" after start/newline/comma
            m = re.search(
                rf"(?:^|\n|,|;)\s*{name}\s*[:=]\s*([^\n,;]+)",
                message,
                re.I,
            )
            if m:
                return m.group(1).strip().strip("\"'")
            # Inline prose: "host localhost user demo password secret"
            m = re.search(
                rf"\b{name}\s+([A-Za-z0-9_.:\-\[\]%]+)",
                message,
                re.I,
            )
            if m:
                return m.group(1).strip().strip("\"'")
        return ""

    host = _field("host", "hostname", "server", "mysql host", "postgres host")
    port_s = _field("port", "mysql port", "postgres port")
    if not host:
        # "create a connector to postgres at localhost:5433" states the endpoint
        # the way people say it, with no "host" label anywhere. Without this the
        # operator who gave a host was asked for one.
        endpoint = _ENDPOINT_PHRASE.search(message)
        if endpoint:
            host = endpoint.group(1)
            port_s = port_s or (endpoint.group(2) or "")
    if host:
        out["host"] = host
    if port_s.isdigit():
        out["port"] = int(port_s)
    db = _field("database", "db", "dbname")
    if db:
        out["database"] = db
    warehouse = _field("warehouse")
    if warehouse:
        out["warehouse"] = warehouse
    account = _field("account", "snowflake account")
    if account:
        out["account"] = account
        if not out.get("host"):
            out["host"] = account
    user = _field("username", "user", "uid")
    if user:
        out["username"] = user
    password = _field("password", "pass", "pwd")
    if password:
        out["password"] = password
    name = _field("name", "connector name", "label")
    if not name:
        named = re.search(
            r"\bnamed\s+[\"']?(.+?)[\"']?(?=\s+(?:host|hostname|server|user|username|password|port|database|db)\b|$)",
            message,
            re.I,
        )
        if named:
            name = named.group(1).strip().strip("\"'")
    if name:
        out["name"] = name
    return out


#: Words that only appear when the operator is handing over an endpoint to save.
_CONNECTION_DETAIL = re.compile(
    r"://|\bhost(?:name)?\b|\bport\b|\buser(?:name)?\b|\bpassword\b|\bpwd\b|"
    r"\bdatabase\b|\bdbname\b|\baccount\b|\bnamed\b|\bcalled\b|"
    r"\b\d{1,3}(?:\.\d{1,3}){3}\b|\b[\w-]+\.[\w.-]+\.[a-z]{2,}\b",
    re.I,
)

_HOW_TO_QUESTION = re.compile(
    r"\bhow\s+(?:do|can|would|should)\s+(?:i|we|you)\b"
    r"|\bhow\s+to\b"
    r"|\bwhere\s+(?:do|can)\s+(?:i|we)\b"
    r"|\bwhat(?:'s| is)\s+the\s+(?:way|process|procedure|steps?)\b"
    r"|\bwalk\s+me\s+through\b"
    # "Can I add a postgres connector" asks whether the product supports one;
    # "add a postgres connector at db.acme.com" hands one over. Only the second
    # carries endpoint detail, which is what separates them below.
    r"|\bcan\s+(?:i|we|you)\b"
    r"|\bdo\s+you\s+support\b"
    r"|\bis\s+it\s+possible\b",
    re.I,
)

#: The engines the create tool can actually build a connector for. Naming one is
#: endpoint detail in its own right: nobody says "postgres" while asking a
#: generic question about the Connectors page.
_ENGINE_NAME = (
    r"postgres(?:ql)?|mysql|mariadb|mongo(?:db)?|snowflake|sql\s*server|sqlite"
)

#: A creation verb applied to a connector, however the operator words it.
#: Enumerating the literal phrasings missed "create a connector to postgres at
#: localhost:5433" — an ordinary way to ask, which fell through to a listing of
#: the connectors that already exist.
_CREATE_CONNECTOR_OBJECT = re.compile(
    r"\b(?:create|add|save|register|make|set\s*up|setup)\s+"
    r"(?:a|an|the|this|new)?\s*(?:\w+\s+){0,2}?(?:connector|connection)\b",
    re.I,
)


def wants_create_connector(message: str) -> bool:
    """Whether the operator handed over an endpoint for Pilot to save.

    "Connect to" and "add a postgres" are also how an operator *asks about* the
    procedure, so "how do I connect to BigQuery" reached the create tool and was
    answered "I need a host (or a full connection URL)" — a request for
    credentials the operator never offered, in place of the documented steps. A
    how-to question with no endpoint detail in it is a question, not a handover.
    """
    text = message or ""
    if _HOW_TO_QUESTION.search(text) and not _CONNECTION_DETAIL.search(text):
        return False
    lower = text.lower()
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
        "create a postgres",
        "create a postgresql",
        "create a mysql",
        "create a mongodb",
        "create a snowflake",
        "create an postgres",
        "add a postgres",
        "add a mysql",
        "add postgres",
        "add mysql",
        "add mongodb",
        "add mongo",
        "add snowflake",
        "save this postgres",
        "save this postgresql",
        "save this mysql",
        "save this mongo",
        "save this mongodb",
        "save postgres",
        "save mysql",
    )
    if any(v in lower for v in verbs):
        return True
    # A creation verb on a connector, plus something that says *which* endpoint.
    # Without the second half, "add a connector" is as likely to be a request
    # for the procedure as a handover, so it keeps its existing route.
    if _CREATE_CONNECTOR_OBJECT.search(lower) and (
        _CONNECTION_DETAIL.search(text) or re.search(rf"\b(?:{_ENGINE_NAME})\b", lower)
    ):
        return True
    # "create a <engine> connector at host…" / "add mysql named warehouse host…"
    if re.search(
        r"\b(?:create|add|save|register|setup|set\s+up)\s+(?:a\s+|an\s+|this\s+)?"
        r"(?:postgres(?:ql)?|mysql|mariadb|mongo(?:db)?|snowflake|sql\s*server|sqlite)"
        r"(?:\s+connector)?\b",
        lower,
    ) and any(
        w in lower
        for w in ("host", "hostname", "user", "username", "password", "database", "named", "port", "://")
    ):
        return True
    if re.search(
        r"\b(?:create|add|save|register|setup|set\s+up)\s+(?:a\s+|an\s+)?"
        r"(?:postgres(?:ql)?|mysql|mariadb|mongo(?:db)?|snowflake|sql\s*server|sqlite)"
        r"\s+connector\b",
        lower,
    ):
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
        elif cs.startswith("redis"):
            ctype = "redis"
    if not ctype:
        ctype = infer_driver_from_text(
            str(merged.get("name") or ""),
            str(merged.get("host") or ""),
            message or "",
        )
    merged["type"] = ctype or "postgresql"

    from src.transfer.connector_capabilities import effective_port

    # 0 and empty are "no port chosen". Redis is 6379, Elasticsearch 9200,
    # Neo4j's HTTP Cypher port is 7474. An explicit port is kept.
    merged["port"] = effective_port(merged["type"], merged.get("port"))

    if not merged.get("name"):
        host = str(merged.get("host") or "db")
        label = host.split(".")[0] if host else merged["type"]
        merged["name"] = f"{merged['type'].title()} · {label}"[:64]

    merged.setdefault("database", "")
    merged.setdefault("username", "")
    merged.setdefault("password", "")
    merged.setdefault("host", "")
    merged.setdefault("connection_string", "")
    merged.setdefault("service_account", "")
    merged.setdefault("ssl", False)
    from services.dialect_profiles import default_schema_for

    merged.setdefault("schema", default_schema_for(merged["type"]) or "")
    merged.setdefault("auth_mode", "connection_string" if merged.get("connection_string") else "user_pass")
    return merged


def _path_connector_complete(draft: dict[str, Any]) -> tuple[bool, str]:
    """SQLite and DuckDB are a file, not a host.

    Completeness is ``validate_probe_auth`` — the same required-field check the
    probe uses — and a SQLite path is confined by ``sqlite_file_path``, the
    same allowlist the reader and writer already enforce.
    """
    from services.connector_auth import validate_probe_auth

    ctype = str(draft.get("type") or "")
    reason = validate_probe_auth(
        driver=ctype,
        auth_mode=str(draft.get("auth_mode") or ""),
        host=str(draft.get("host") or ""),
        port=int(draft.get("port") or 0),
        database=str(draft.get("database") or ""),
        username=str(draft.get("username") or ""),
        password=str(draft.get("password") or ""),
        connection_string=str(draft.get("connection_string") or ""),
    )
    if reason:
        return False, reason
    if ctype == "sqlite":
        from connectors.sqlite_common import sqlite_file_path

        try:
            resolved = sqlite_file_path(
                str(draft.get("database") or ""),
                str(draft.get("connection_string") or ""),
                str(draft.get("host") or ""),
            )
        except ValueError as exc:
            return False, str(exc)
        if not resolved:
            return False, "File path or database name is required for SQLite/DuckDB."
    return True, ""


def draft_is_complete(draft: dict[str, Any]) -> tuple[bool, str]:
    ctype = draft.get("type") or ""
    if ctype in {"sqlite", "duckdb"}:
        return _path_connector_complete(draft)
    if ctype == "bigquery":
        project = str(draft.get("database") or draft.get("project") or "").strip()
        creds = str(
            draft.get("service_account") or draft.get("connection_string") or ""
        ).strip()
        if not project:
            return False, "BigQuery needs the project id in the database field."
        if not creds.startswith("{"):
            return (
                False,
                "BigQuery needs the service account JSON key in service_account "
                "(or a JSON connection_string).",
            )
        return True, ""
    if draft.get("connection_string"):
        # Snowflake URLs are not fully supported yet — require structured fields.
        if ctype == "snowflake" and "snowflake" in str(draft.get("connection_string") or "").lower():
            return (
                False,
                "Snowflake connectors need account, warehouse, database, username, and password "
                "(or open Connectors to finish SSO / key-pair auth). Chat URL paste isn't enough yet.",
            )
        return True, ""
    if ctype == "snowflake":
        missing = []
        if not (draft.get("host") or draft.get("account")):
            missing.append("account (or host)")
        if not draft.get("warehouse"):
            missing.append("warehouse")
        if not draft.get("database"):
            missing.append("database")
        if not draft.get("username") or not draft.get("password"):
            missing.append("username and password")
        if missing:
            return (
                False,
                "Snowflake needs "
                + ", ".join(missing)
                + ". Or open **Connectors** to configure SSO / key-pair.",
            )
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
