"""Database connector adapters."""

from __future__ import annotations

from connectors.base import ConnectResult


def test_database_connection(
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
) -> ConnectResult:
    if db_type == "postgresql":
        from connectors.postgresql import test_postgresql

        return test_postgresql(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema,
            connection_string=connection_string,
            ssl=ssl,
        )

    if db_type == "snowflake":
        from connectors.snowflake import test_snowflake

        return test_snowflake(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema or "PUBLIC",
            connection_string=connection_string,
            ssl=ssl,
            warehouse=warehouse,
        )

    if db_type == "mysql":
        from connectors.mysql import test_mysql

        return test_mysql(
            host=host,
            port=port or 3306,
            database=database,
            username=username,
            password=password,
            schema=schema,
            connection_string=connection_string,
            ssl=ssl,
        )

    if db_type == "bigquery":
        from connectors.bigquery import test_bigquery

        return test_bigquery(
            host=host,
            port=port or 443,
            database=database,
            username=username,
            password=password,
            schema=schema or "dataflow",
            connection_string=connection_string,
            ssl=ssl,
        )

    return ConnectResult(
        ok=False,
        tables=[],
        error=f"Unsupported database type: {db_type}",
        driver="none",
    )
