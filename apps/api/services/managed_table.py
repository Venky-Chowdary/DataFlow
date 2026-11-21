"""Whether a destination table was created by Datawrap.

The ownership comment (``datawrap:managed`` or the legacy ``dataflow:managed``)
is the signal. An owned table may widen non-key columns in the same type
family (DW-D334). ADD COLUMN still requires propagate. A probe that cannot
read the comment answers false — an operator-owned table is not altered.
"""

from __future__ import annotations

from typing import Any, Mapping


def owned_for_evolution(
    db_type: str,
    config: Mapping[str, Any] | None,
    *,
    schema: str = "",
    table: str = "",
    table_exists: bool | None = None,
    dest_recreated: bool = False,
) -> bool:
    """Whether preflight may treat this destination as Datawrap-owned.

    One call so the preflight module does not grow a probe of its own.
    """
    if table_exists is not True or dest_recreated or not config or not str(table or "").strip():
        return False
    try:
        return destination_table_is_managed(
            db_type, config, schema=schema, table=table
        )
    except Exception:
        return False


def destination_table_is_managed(
    db_type: str,
    config: Mapping[str, Any] | None,
    *,
    schema: str = "",
    table: str = "",
) -> bool:
    comment = read_table_comment(db_type, config, schema=schema, table=table)
    if not comment:
        return False
    from services.brand import is_own

    return is_own("managed_table_comment", comment)


def read_table_comment(
    db_type: str,
    config: Mapping[str, Any] | None,
    *,
    schema: str = "",
    table: str = "",
) -> str:
    name = str(table or "").strip()
    if not name or not config:
        return ""
    kind = (db_type or "").strip().lower()
    try:
        if kind in {"postgresql", "postgres"} or kind.startswith("postgres"):
            return _pg_comment(config, schema or str(config.get("schema") or "public"), name)
        if kind in {"mysql", "mariadb", "tidb"}:
            return _mysql_comment(config, name)
    except Exception:
        return ""
    return ""


def _pg_comment(config: Mapping[str, Any], schema: str, table: str) -> str:
    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=str(config.get("host") or ""),
        port=int(config.get("port") or 5432),
        database=str(config.get("database") or config.get("dbname") or ""),
        username=str(config.get("username") or config.get("user") or ""),
        password=str(config.get("password") or ""),
        connection_string=str(config.get("connection_string") or ""),
        ssl=bool(config.get("ssl")),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT obj_description(c.oid) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s",
                (schema or "public", table),
            )
            row = cur.fetchone()
            return str(row[0] or "") if row else ""
    finally:
        conn.close()


def _mysql_comment(config: Mapping[str, Any], table: str) -> str:
    from connectors.mysql_conn import get_connection

    conn = get_connection(
        host=str(config.get("host") or ""),
        port=int(config.get("port") or 3306),
        database=str(config.get("database") or ""),
        username=str(config.get("username") or ""),
        password=str(config.get("password") or ""),
        connection_string=str(config.get("connection_string") or ""),
        ssl=bool(config.get("ssl")),
        purpose="probe",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_comment FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
            row = cur.fetchone()
            return str(row[0] or "") if row else ""
    finally:
        conn.close()
