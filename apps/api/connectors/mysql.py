"""MySQL connector — connection probe when pymysql is available."""

from __future__ import annotations

from connectors.base import ConnectResult, name_filter_pattern

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
    name_filter: str = "",
) -> ConnectResult:
    del schema
    # UX-9: narrow the bounded page to names containing the search text.
    pattern = name_filter_pattern(name_filter)
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
            table_types: dict[str, str] = {}
            truncated = False
            total = 0
            if db_name:
                # Views are listed beside tables, each with its kind (M18).
                cur.execute(
                    """
                    SELECT table_name, table_type FROM information_schema.tables
                    WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW')
                      AND (%s = '' OR LOWER(table_name) LIKE LOWER(%s))
                    ORDER BY table_name
                    LIMIT %s
                    """,
                    (db_name, pattern, pattern, _TABLE_PAGE + 1),
                )
                fetched = [(row[0], row[1]) for row in cur.fetchall()]
                truncated = len(fetched) > _TABLE_PAGE
                fetched = fetched[:_TABLE_PAGE]
                tables = [name for name, _kind in fetched]
                table_types = {
                    name: ("view" if str(kind).upper() == "VIEW" else "table")
                    for name, kind in fetched
                }
                total = len(tables)
                if truncated:
                    cur.execute(
                        """
                        SELECT count(*) FROM information_schema.tables
                        WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW')
                          AND (%s = '' OR LOWER(table_name) LIKE LOWER(%s))
                        """,
                        (db_name, pattern, pattern),
                    )
                    total = int((cur.fetchone() or [len(tables)])[0])
            else:
                db_name = "(default)"
        from connectors.mysql_conn import describe_session_encryption, session_encryption

        # QA D10: the test states the session's real encryption (Ssl_cipher),
        # not what the ssl flag suggested.
        encryption = describe_session_encryption(session_encryption(conn))
        conn.close()
        if truncated:
            message = (
                f"MySQL connected — {total} tables in `{db_name}` "
                f"(listing first {len(tables)}) · {encryption}"
            )
        else:
            message = f"MySQL connected — {total} tables in `{db_name}` · {encryption}"
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in database)"],
            message=message,
            driver="pymysql",
            tables_truncated=truncated,
            table_types=table_types,
            tables_total=total,
        )
    except Exception as exc:  # noqa: BLE001 — a probe reports the driver error
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="pymysql")
