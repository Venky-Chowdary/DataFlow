"""Destination schema introspection for real target column discovery."""

from __future__ import annotations

from typing import Any


def introspect_schema(
    db_type: str,
    *,
    host: str = "",
    port: int = 5432,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "public",
    connection_string: str = "",
    ssl: bool = True,
    warehouse: str = "",
    table: str | None = None,
) -> dict[str, Any]:
    if db_type == "postgresql":
        return _introspect_postgresql(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema,
            connection_string=connection_string,
            ssl=ssl,
            table=table,
        )
    if db_type == "snowflake":
        return _introspect_snowflake(
            host=host,
            database=database,
            username=username,
            password=password,
            schema=schema or "PUBLIC",
            connection_string=connection_string,
            warehouse=warehouse,
            table=table,
        )
    return {"ok": False, "error": f"Schema introspection not implemented for {db_type}", "columns": [], "tables": []}


def _introspect_postgresql(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    schema = kwargs.get("schema") or "public"
    try:
        import psycopg2
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=kwargs.get("host", ""),
            port=kwargs.get("port", 5432),
            database=kwargs.get("database", ""),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            connection_string=kwargs.get("connection_string", ""),
            ssl=kwargs.get("ssl", True),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name LIMIT 100
                """,
                (schema,),
            )
            tables = [r[0] for r in cur.fetchall()]

            columns: list[dict] = []
            if table:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, table),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append(
                        {
                            "name": name,
                            "inferred_type": _pg_to_logical(dtype),
                            "nullable": nullable == "YES",
                        }
                    )
            elif tables:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, tables[0]),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append(
                        {
                            "name": name,
                            "inferred_type": _pg_to_logical(dtype),
                            "nullable": nullable == "YES",
                        }
                    )
        conn.close()
        return {"ok": True, "tables": tables, "columns": columns, "schema": schema}
    except ImportError:
        return _stub_columns(kwargs.get("database", ""), schema)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_snowflake(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    schema = (kwargs.get("schema") or "PUBLIC").upper()
    try:
        from connectors.snowflake_conn import get_connection, normalize_account

        conn = get_connection(
            account=normalize_account(kwargs.get("host", "")),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            database=kwargs.get("database", ""),
            schema=schema,
            warehouse=kwargs.get("warehouse", ""),
            connection_string=kwargs.get("connection_string", ""),
        )
        with conn.cursor() as cur:
            wh = kwargs.get("warehouse", "")
            if wh:
                cur.execute(f"USE WAREHOUSE {wh}")
            db = kwargs.get("database", "")
            if db:
                cur.execute(f"USE DATABASE {db}")
            cur.execute(f"USE SCHEMA {schema}")
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name LIMIT 100
                """,
                (schema,),
            )
            tables = [r[0] for r in cur.fetchall()]
            columns: list[dict] = []
            target_table = table or (tables[0] if tables else None)
            if target_table:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, target_table),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append(
                        {
                            "name": name,
                            "inferred_type": _sf_to_logical(dtype),
                            "nullable": nullable == "YES",
                        }
                    )
        conn.close()
        return {"ok": True, "tables": tables, "columns": columns, "schema": schema}
    except ImportError:
        return _stub_columns(kwargs.get("database", ""), schema, snowflake=True)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _pg_to_logical(dtype: str) -> str:
    d = dtype.lower()
    if "int" in d:
        return "INTEGER"
    if "numeric" in d or "decimal" in d or "double" in d or "real" in d:
        return "DECIMAL"
    if "bool" in d:
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if "timestamp" in d:
        return "TIMESTAMP"
    return "TEXT"


def _sf_to_logical(dtype: str) -> str:
    d = dtype.upper()
    if "NUMBER" in d or "INT" in d:
        return "INTEGER" if ",0" in d else "DECIMAL"
    if "BOOLEAN" in d:
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    if "TIMESTAMP" in d:
        return "TIMESTAMP"
    return "TEXT"


def _stub_columns(database: str, schema: str, snowflake: bool = False) -> dict[str, Any]:
    cols = [
        {"name": "customer_id", "inferred_type": "TEXT", "nullable": False},
        {"name": "payment_amount", "inferred_type": "DECIMAL", "nullable": True},
        {"name": "transaction_date", "inferred_type": "DATE", "nullable": True},
        {"name": "account_number", "inferred_type": "TEXT", "nullable": True},
        {"name": "currency_code", "inferred_type": "TEXT", "nullable": True},
        {"name": "reference_number", "inferred_type": "TEXT", "nullable": True},
        {"name": "status", "inferred_type": "TEXT", "nullable": True},
        {"name": "description", "inferred_type": "TEXT", "nullable": True},
    ]
    return {
        "ok": True,
        "tables": ["PAYMENTS", "CUSTOMERS"] if snowflake else ["payments", "customers"],
        "columns": cols,
        "schema": schema,
        "driver": "stub",
        "message": f"Stub schema for {database or 'database'} (install connector for live introspection)",
    }
