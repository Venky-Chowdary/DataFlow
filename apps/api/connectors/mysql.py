"""MySQL connector — connection probe when pymysql is available."""

from __future__ import annotations

from connectors.base import ConnectResult

#: Objects returned by a connection probe. A probe is a reachability check, not
#: a catalog dump, so the page stays bounded and truncation is reported instead.
#: The page matches PostgreSQL: a 50-name cap with ``tables_truncated`` left
#: false made later tables look missing.
_TABLE_PAGE = 200


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
            purpose="probe",
        )
        with conn.cursor() as cur:
            db_name = (database or "").strip()
            if not db_name:
                cur.execute("SELECT DATABASE()")
                row = cur.fetchone()
                db_name = (row[0] if row else None) or ""
            if not db_name:
                cur.execute("SELECT SCHEMA()")
                row = cur.fetchone()
                db_name = (row[0] if row else None) or ""
            tables: list[str] = []
            truncated = False
            total = 0
            if db_name:
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    LIMIT %s
                    """,
                    (db_name, _TABLE_PAGE + 1),
                )
                fetched = [row[0] for row in cur.fetchall()]
                truncated = len(fetched) > _TABLE_PAGE
                tables = fetched[:_TABLE_PAGE]
                total = len(tables)
                if truncated:
                    cur.execute(
                        """
                        SELECT count(*) FROM information_schema.tables
                        WHERE table_schema = %s AND table_type = 'BASE TABLE'
                        """,
                        (db_name,),
                    )
                    total = int((cur.fetchone() or [len(tables)])[0])
            else:
                db_name = "(default)"
        conn.close()
        if truncated:
            message = (
                f"MySQL connected — {total} tables in `{db_name}` "
                f"(listing first {len(tables)})"
            )
        else:
            message = f"MySQL connected — {total} tables in `{db_name}`"
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in database)"],
            message=message,
            driver="pymysql",
            tables_truncated=truncated,
        )
    except Exception as exc:  # noqa: BLE001 — a probe reports the driver error
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="pymysql")
