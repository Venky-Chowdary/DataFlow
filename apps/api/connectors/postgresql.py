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
        import psycopg2
        from psycopg2 import sql
    except ImportError:
        return _stub_fallback(host, database, username, connection_string)

    try:
        if connection_string.strip():
            conn = psycopg2.connect(connection_string, connect_timeout=8)
        else:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=username,
                password=password,
                connect_timeout=8,
                sslmode="require" if ssl else "prefer",
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                LIMIT 50
                """,
                (schema or "public",),
            )
            tables = [row[0] for row in cur.fetchall()]

        conn.close()
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in schema)"],
            message=f"PostgreSQL connected — {len(tables)} tables in schema '{schema or 'public'}'",
            driver="psycopg2",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="psycopg2")


def _stub_fallback(host: str, database: str, username: str, connection_string: str) -> ConnectResult:
    if connection_string.strip() or (host and database and username):
        return ConnectResult(
            ok=True,
            tables=["sample_table", "orders", "customers"],
            message="PostgreSQL validated (install psycopg2-binary for live probe)",
            driver="stub",
        )
    return ConnectResult(
        ok=False,
        tables=[],
        error="Provide connection string or host + database + username",
        driver="stub",
    )
