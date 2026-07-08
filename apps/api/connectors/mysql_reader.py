"""MySQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

from dataclasses import dataclass

from connectors.mysql_conn import get_connection


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int
    total_rows: int


def read_table_batch(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
) -> ReadBatch:
    del schema
    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            total = int(cur.fetchone()[0])
            if columns:
                col_list = ", ".join(f"`{c}`" for c in columns)
                query = f"SELECT {col_list} FROM `{table}` LIMIT %s OFFSET %s"
            else:
                query = f"SELECT * FROM `{table}` LIMIT %s OFFSET %s"
            cur.execute(query, (limit, offset))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [["" if v is None else str(v) for v in row] for row in fetched]
            return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        conn.close()
