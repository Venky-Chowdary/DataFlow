"""PostgreSQL connector — real connection probe when psycopg2 is available."""

from __future__ import annotations

from connectors.base import ConnectResult


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
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                LIMIT 200
                """,
                (schema_name,),
            )
            tables = [row[0] for row in cur.fetchall()]

        conn.close()
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in schema)"],
            message=f"PostgreSQL connected — {len(tables)} tables in schema '{schema_name}'",
            driver="psycopg2",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="psycopg2")
