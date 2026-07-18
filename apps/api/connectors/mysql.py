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
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=host or "localhost",
            port=port or 3306,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
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
