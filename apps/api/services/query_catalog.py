"""Database and schema nodes for the query and transfer browsers.

One implementation. Snowflake may list schemas from the account. Every other
engine returns the database and schema the connector is configured with.
An unknown connection still returns one node — an empty list would look like
a catalog that had nothing in it.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def build_catalog_tree(
    *,
    connector_type: str,
    database: str,
    schema: str,
    live_schemas: list[str] | None = None,
) -> dict[str, Any]:
    engine = (connector_type or "").strip() or "connector"
    db_name = (database or "").strip()
    configured = (schema or "").strip()
    listed = _clean_names(live_schemas) if live_schemas is not None else None

    if listed:
        schema_names = listed
        message = "Schemas listed from the database."
        listed_live = True
    elif configured:
        schema_names = [configured]
        listed_live = False
        message = (
            "The account returned no schema names. Showing the configured schema."
            if live_schemas is not None
            else "Showing the configured schema. This connector does not publish a schema catalog."
        )
    else:
        schema_names = []
        listed_live = False
        message = "No schema is configured on this connector."

    children = [{"kind": "schema", "name": name, "children": []} for name in schema_names]
    if db_name:
        nodes = [{"kind": "database", "name": db_name, "children": children}]
    elif children:
        nodes = children
    else:
        nodes = [{"kind": "database", "name": engine, "children": []}]
        message = "This connector did not name a database or schema. Showing one node for the connection."

    return {
        "connected": True,
        "connector_type": engine if engine != "connector" else "",
        "database": db_name,
        "schema_name": configured,
        "nodes": nodes,
        "listed_live": listed_live,
        "message": message,
    }


def _clean_names(names: list[str] | None) -> list[str]:
    seen: list[str] = []
    for name in names or []:
        text = str(name or "").strip()
        if text and text not in seen:
            seen.append(text)
    return seen


def live_schema_names(endpoint: Any) -> list[str] | None:
    """Schema names the engine actually listed, or None when it does not list.

    None means "use the configured schema". An empty list means the account
    answered and had no names, which the tree reports instead of inventing one.
    """
    fmt = str(getattr(endpoint, "format", "") or "").lower()
    if fmt in _POSTGRES_FAMILY:
        return _postgres_schema_names(endpoint)
    if fmt != "snowflake":
        return None
    try:
        from connectors.snowflake_conn import get_connection, normalize_account
        from services.connector_auth import engine_login_role
        from services.schema_introspect import _snowflake_list_schemas
    except ImportError:
        logger.info("Snowflake schema listing is unavailable; using the configured schema")
        return None
    schema = str(getattr(endpoint, "schema", "") or "PUBLIC").strip() or "PUBLIC"
    try:
        conn = get_connection(
            account=normalize_account(getattr(endpoint, "host", "") or ""),
            username=getattr(endpoint, "username", "") or "",
            password=getattr(endpoint, "password", "") or "",
            database=getattr(endpoint, "database", "") or "",
            schema=schema.upper(),
            warehouse=getattr(endpoint, "warehouse", "") or "",
            connection_string=getattr(endpoint, "connection_string", "") or "",
            role=engine_login_role(getattr(endpoint, "auth_role", None), None),
            private_key=str(getattr(endpoint, "private_key", "") or ""),
            private_key_passphrase=str(getattr(endpoint, "password", "") or ""),
        )
    except Exception as exc:
        logger.info("Snowflake schema listing did not connect: %s", exc)
        return None
    try:
        with conn.cursor() as cur:
            names = _snowflake_list_schemas(cur)
        return names
    except Exception as exc:
        logger.info("Snowflake schema listing failed: %s", exc)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("Snowflake catalog connection close failed", exc_info=True)


#: Engines whose schemas are listed from ``information_schema.schemata`` (UX-9).
_POSTGRES_FAMILY = frozenset({"postgresql", "postgres"})

_PG_SCHEMAS_SQL = """
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
      AND schema_name NOT LIKE 'pg\\_toast%%'
      AND schema_name NOT LIKE 'pg\\_temp\\_%%'
    ORDER BY schema_name
    LIMIT 500
"""


def _postgres_schema_names(endpoint: Any) -> list[str] | None:
    """Schemas the login can see, so the browser can leave ``public`` (UX-9).

    None (use the configured schema) when the listing cannot run.
    """
    if not (getattr(endpoint, "database", "") or getattr(endpoint, "connection_string", "")):
        # Nothing names a database to list from: keep the configured schema.
        return None
    try:
        from connectors.postgresql_conn import get_connection
    except ImportError:
        return None
    try:
        conn = get_connection(
            host=getattr(endpoint, "host", "") or "localhost",
            port=int(getattr(endpoint, "port", 0) or 5432),
            database=getattr(endpoint, "database", "") or "",
            username=getattr(endpoint, "username", "") or "",
            password=getattr(endpoint, "password", "") or "",
            connection_string=getattr(endpoint, "connection_string", "") or "",
            ssl=bool(getattr(endpoint, "ssl", False)),
        )
    except Exception as exc:
        logger.info("PostgreSQL schema listing did not connect: %s", type(exc).__name__)
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(_PG_SCHEMAS_SQL)
            return [str(row[0]) for row in cur.fetchall() if row and row[0]]
    except Exception as exc:
        logger.info("PostgreSQL schema listing failed: %s", type(exc).__name__)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("PostgreSQL catalog connection close failed", exc_info=True)
