"""BigQuery table reader."""

from __future__ import annotations

import sys
from pathlib import Path

from connectors.base import ReadBatch
from connectors.sql_identifiers import quote_column_list, quote_table_ref, require_safe_identifier

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


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
    service_account: str = "",
) -> ReadBatch:
    del username, password, ssl, warehouse
    project_id = database or host
    dataset_id = schema or "dataflow"
    table_ref = quote_table_ref(
        table,
        dialect="bigquery",
        project=project_id,
        dataset=dataset_id,
    )

    try:
        from connectors.bigquery_conn import get_client

        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            service_account=service_account,
            host=host,
            port=port,
            connection_string=connection_string,
        )
        if known_total_rows is not None:
            total = known_total_rows
        else:
            count_q = f"SELECT COUNT(*) AS cnt FROM {table_ref}"
            total = int(list(client.query(count_q).result())[0]["cnt"])
        col_sql = (
            quote_column_list(
                [require_safe_identifier(c, preserve_case=True) for c in columns],
                quote_char="`",
            )
            if columns
            else "*"
        )
        query = f"SELECT {col_sql} FROM {table_ref} LIMIT {int(limit)} OFFSET {int(offset)}"
        job = client.query(query)
        rows_iter = job.result()
        if job.schema:
            headers = [field.name for field in job.schema]
        else:
            safe_project = require_safe_identifier(project_id, preserve_case=True, max_len=1024)
            safe_dataset = require_safe_identifier(dataset_id, preserve_case=True, max_len=1024)
            safe_table = require_safe_identifier(table, preserve_case=True, max_len=1024)
            bq_table = client.get_table(f"{safe_project}.{safe_dataset}.{safe_table}")
            headers = [field.name for field in bq_table.schema]
        rows = [[cell_to_string(v) for v in row.values()] for row in rows_iter]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    except Exception as exc:
        raise RuntimeError(f"BigQuery read failed for {table_ref}: {exc}") from exc
