"""PostgreSQL connector — real connection probe when psycopg2 is available."""

from __future__ import annotations

from connectors.base import ConnectResult

#: Objects returned by a connection probe. A probe is a reachability check, not
#: a catalog dump, so the page stays bounded and truncation is reported instead.
_TABLE_PAGE = 200


def test_postgresql(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
) -> ConnectResult:
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=host or "localhost",
            port=port or 5432,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        schema_name = schema or "public"
        with conn.cursor() as cur:
            # Fetch one past the page so truncation is detected rather than
            # inferred from a full page, and report the real total: a probe that
            # says "200 tables" for a 636-table schema reads as the whole
            # inventory to anything resolving a name against it.
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                LIMIT %s
                """,
                (schema_name, _TABLE_PAGE + 1),
            )
            fetched = [row[0] for row in cur.fetchall()]
            truncated = len(fetched) > _TABLE_PAGE
            tables = fetched[:_TABLE_PAGE]
            total = len(tables)
            if truncated:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    """,
                    (schema_name,),
                )
                total = int((cur.fetchone() or [len(tables)])[0])

        conn.close()
        if truncated:
            message = (
                f"PostgreSQL connected — {total} tables in schema '{schema_name}' "
                f"(listing first {len(tables)})"
            )
        else:
            message = f"PostgreSQL connected — {total} tables in schema '{schema_name}'"
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in schema)"],
            message=message,
            driver="psycopg2",
            tables_truncated=truncated,
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="psycopg2")
