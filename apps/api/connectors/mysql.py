"""MySQL connector — connection probe when pymysql is available."""

from __future__ import annotations

from connectors.base import ConnectResult


def test_mysql(
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
    del schema
    try:
        import pymysql
    except ImportError:
        return _stub_fallback(host, database, username)

    try:
        conn = pymysql.connect(
            host=host or "localhost",
            port=port or 3306,
            user=username,
            password=password,
            database=database,
            connect_timeout=8,
            charset="utf8mb4",
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name LIMIT 50
                """,
                (database,),
            )
            tables = [row[0] for row in cur.fetchall()]
        conn.close()
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in database)"],
            message=f"MySQL connected — {len(tables)} tables in `{database}`",
            driver="pymysql",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="pymysql")


def _stub_fallback(host: str, database: str, username: str) -> ConnectResult:
    del host, database, username
    return ConnectResult(
        ok=False,
        tables=[],
        error="MySQL driver not installed — pip install pymysql",
        driver="none",
    )
