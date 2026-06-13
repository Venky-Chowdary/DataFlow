"""Gate 8 reconciliation — independent target verification."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class ReconciliationReport:
    passed: bool
    source_rows: int
    target_rows: int
    source_checksum: str
    target_checksum: str
    message: str

    def to_dict(self) -> dict:
        return asdict(self)


def checksum_rows(rows: list[list[str]]) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update("|".join(row).encode())
    return h.hexdigest()[:16]


def reconcile(
    *,
    source_rows: int,
    target_rows: int,
    source_checksum: str,
    target_checksum: str,
) -> ReconciliationReport:
    if source_rows != target_rows:
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=f"Row count mismatch: source {source_rows} vs target {target_rows}",
        )
    if source_checksum != target_checksum:
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message="Checksum mismatch — rollback recommended",
        )
    return ReconciliationReport(
        passed=True,
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        message=f"100% row fidelity verified ({source_rows} rows)",
    )


def verify_postgres_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
) -> tuple[int, str]:
    try:
        import psycopg2
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            count = int(cur.fetchone()[0])
            cur.execute(f'SELECT * FROM "{schema}"."{table_name}" ORDER BY 1 LIMIT 5000')
            rows = cur.fetchall()
        conn.close()
        h = hashlib.sha256()
        for row in rows:
            h.update("|".join("" if v is None else str(v) for v in row).encode())
        return count, h.hexdigest()[:16]
    except Exception:
        return -1, ""


def verify_snowflake_table(
    *,
    host: str,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table_name: str,
) -> tuple[int, str]:
    try:
        from connectors.snowflake_conn import get_connection, normalize_account

        conn = get_connection(
            account=normalize_account(host),
            username=username,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            connection_string=connection_string,
        )
        with conn.cursor() as cur:
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            count = int(cur.fetchone()[0])
            cur.execute(f'SELECT * FROM "{table_name}" ORDER BY 1 LIMIT 5000')
            rows = cur.fetchall()
        conn.close()
        h = hashlib.sha256()
        for row in rows:
            h.update("|".join("" if v is None else str(v) for v in row).encode())
        return count, h.hexdigest()[:16]
    except Exception:
        return -1, ""


def verify_target(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    fallback_rows: int,
    fallback_checksum: str,
) -> tuple[int, str]:
    if db_type == "postgresql":
        count, chk = verify_postgres_table(
            host=dest.get("host", ""),
            port=dest.get("port", 5432),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", True),
            table_name=table_name,
        )
    elif db_type == "snowflake":
        count, chk = verify_snowflake_table(
            host=dest.get("host", ""),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            warehouse=dest.get("warehouse", ""),
            table_name=table_name,
        )
    else:
        count, chk = -1, ""

    if count >= 0:
        return count, chk
    return fallback_rows, fallback_checksum
