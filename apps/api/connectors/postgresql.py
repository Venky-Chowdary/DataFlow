"""PostgreSQL connector — real connection probe when psycopg2 is available."""

from __future__ import annotations

from connectors.base import ConnectResult, name_filter_pattern

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
    name_filter: str = "",
) -> ConnectResult:
    """Reachability probe plus one bounded page of the schema's objects.

    ``name_filter`` narrows the page to names containing it (case-insensitive),
    so a browser can search a 9,000-table schema instead of reading only the
    first page (UX-9).
    """
    pattern = name_filter_pattern(name_filter)
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
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW')
                  AND (%s = '' OR table_name ILIKE %s ESCAPE '\\')
                ORDER BY table_name
                LIMIT %s
                """,
                (schema_name, pattern, pattern, _TABLE_PAGE + 1),
            )
            fetched_rows = cur.fetchall()
            truncated = len(fetched_rows) > _TABLE_PAGE
            fetched = fetched_rows[:_TABLE_PAGE]
            tables = [row[0] for row in fetched]
            # Keep the object kind so callers can tell a view from a table
            # (M18 — the listing showed every object as a plain table).
            table_types = {
                row[0]: ("view" if str(row[1]).upper() == "VIEW" else "table")
                for row in fetched
            }
            total = len(tables)
            if truncated:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW')
                      AND (%s = '' OR table_name ILIKE %s ESCAPE '\\')
                    """,
                    (schema_name, pattern, pattern),
                )
                total = int((cur.fetchone() or [len(tables)])[0])
            schema_missing = False
            if not tables:
                # ERR-26: "0 tables" for a schema that does not exist hid the cause.
                cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema_name,))
                schema_missing = cur.fetchone() is None

        conn.close()
        if schema_missing:
            from services.error_taxonomy import DatawrapError, classify

            named = classify(
                DatawrapError("object.schema_not_found", object={"schema": schema_name}),
                side="connection", engine="postgresql",
            )
            return ConnectResult(
                ok=True, tables=[], message=named.text if named else "", driver="psycopg2",
                tables_truncated=False, table_types={},
            )
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
            table_types=table_types,
            tables_total=total,
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="psycopg2")
