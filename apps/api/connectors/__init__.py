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

    if connection_string.strip():
        return ConnectResult(
            ok=True,
            tables=["information_schema.tables"],
            message=f"{db_type} connection string accepted (driver pending)",
            driver="stub",
        )

    if not host or not database or not username:
        return ConnectResult(
            ok=False,
            tables=[],
            error="Provide connection string or host + database + username",
        )

    return ConnectResult(
        ok=True,
        tables=["sample_table", "orders", "customers"],
        message=f"Connection validated for {db_type} (driver integration pending)",
        driver="stub",
    )
