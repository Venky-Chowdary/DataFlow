"""Amazon Redshift connector — PostgreSQL wire protocol on port 5439."""

from __future__ import annotations

from connectors.base import ConnectResult
from connectors.postgresql import test_postgresql


def test_redshift(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    warehouse: str = "",
) -> ConnectResult:
    del warehouse
    result = test_postgresql(
        host=host,
        port=port or 5439,
        database=database,
        username=username,
        password=password,
        schema=schema or "public",
        connection_string=connection_string,
        ssl=ssl,
    )
    if result.ok:
        return ConnectResult(
            ok=True,
            tables=result.tables,
            message=result.message.replace("PostgreSQL", "Redshift") if result.message else "Redshift connected",
            driver=result.driver,
        )
    err = result.error or "Connection failed"
    return ConnectResult(ok=False, tables=[], error=err.replace("PostgreSQL", "Redshift"), driver=result.driver)
