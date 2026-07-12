"""BigQuery table reader."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


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
    known_total_rows: int | None = None,
) -> ReadBatch:
    del port, username, password, ssl, warehouse
    project_id = database or host
    dataset_id = schema or "dataflow"
    table_ref = f"`{project_id}.{dataset_id}.{table}`"

    try:
        from connectors.bigquery_conn import get_client

        table_ref = f"`{project_id}.{dataset_id}.{table}`"
        client = get_client(project_id=project_id, credentials_path=connection_string)
        if known_total_rows is not None:
            total = known_total_rows
        else:
            count_q = f"SELECT COUNT(*) AS cnt FROM {table_ref}"
            total = int(list(client.query(count_q).result())[0]["cnt"])
        col_sql = ", ".join(f"`{c}`" for c in columns) if columns else "*"
        query = f"SELECT {col_sql} FROM {table_ref} LIMIT {limit} OFFSET {offset}"
        job = client.query(query)
        rows_iter = job.result()
        headers = [field.name for field in job.schema]
        rows = [[cell_to_string(v) for v in row.values()] for row in rows_iter]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    except Exception as exc:
        raise RuntimeError(f"BigQuery read failed for {table_ref}: {exc}") from exc
