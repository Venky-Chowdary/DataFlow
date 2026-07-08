"""BigQuery table reader."""

from __future__ import annotations

from dataclasses import dataclass


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
    warehouse: str = "",
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
) -> ReadBatch:
    del port, username, password, ssl, warehouse
    project_id = database or host
    dataset_id = schema or "dataflow"

    try:
        from connectors.bigquery_conn import get_client

        client = get_client(project_id=project_id, credentials_path=connection_string)
        table_ref = f"`{project_id}.{dataset_id}.{table}`"
        count_q = f"SELECT COUNT(*) AS cnt FROM {table_ref}"
        total = int(list(client.query(count_q).result())[0]["cnt"])
        col_sql = ", ".join(f"`{c}`" for c in columns) if columns else "*"
        query = f"SELECT {col_sql} FROM {table_ref} LIMIT {limit} OFFSET {offset}"
        job = client.query(query)
        rows_iter = job.result()
        headers = [field.name for field in job.schema]
        rows = [["" if v is None else str(v) for v in row.values()] for row in rows_iter]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    except Exception:
        return ReadBatch(headers=columns or [], rows=[], offset=offset, total_rows=0)
