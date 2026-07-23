"""BigQuery table reader."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

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
        # Stable LIMIT/OFFSET requires ORDER BY — unordered pages silently
        # duplicate or skip rows under concurrent mutations / non-deterministic plans.
        order_cols = list(columns or [])
        if not order_cols:
            safe_project = require_safe_identifier(project_id, preserve_case=True, max_len=1024)
            safe_dataset = require_safe_identifier(dataset_id, preserve_case=True, max_len=1024)
            safe_table = require_safe_identifier(table, preserve_case=True, max_len=1024)
            bq_table = client.get_table(f"{safe_project}.{safe_dataset}.{safe_table}")
            order_cols = [field.name for field in (bq_table.schema or [])]
        if not order_cols:
            raise RuntimeError("BigQuery table has no columns for stable pagination")
        order_sql = quote_column_list(
            [require_safe_identifier(order_cols[0], preserve_case=True)],
            quote_char="`",
        )
        query = (
            f"SELECT {col_sql} FROM {table_ref} "
            f"ORDER BY {order_sql} LIMIT {int(limit)} OFFSET {int(offset)}"
        )
        job = client.query(query)
        rows_iter = job.result()
        if job.schema:
            headers = [field.name for field in job.schema]
        else:
            headers = list(order_cols)
        rows = [[cell_to_string(v, preserve_sql_null=True) for v in row.values()] for row in rows_iter]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    except Exception as exc:
        raise RuntimeError(f"BigQuery read failed for {table_ref}: {exc}") from exc


def read_table_cursor_batch(
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
    cursor_column: str,
    cursor_after: str | None = None,
    columns: list[str] | None = None,
    limit: int = 500,
    warehouse: str = "",
    service_account: str = "",
    cursor_primary_key: str | None = None,
) -> ReadBatch:
    """Keyset incremental read — never silently fall back to OFFSET under concurrent writes."""
    del username, password, ssl, warehouse
    from google.cloud import bigquery

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
        col_sql = (
            quote_column_list(
                [require_safe_identifier(c, preserve_case=True) for c in columns],
                quote_char="`",
            )
            if columns
            else "*"
        )
        cursor_q = quote_column_list(
            [require_safe_identifier(cursor_column, preserve_case=True)],
            quote_char="`",
        )
        pk = (cursor_primary_key or "").strip()
        params: list[Any] = []
        where = ""
        order = cursor_q
        if cursor_after is not None and cursor_after != "":
            if pk and pk != cursor_column and "|" in str(cursor_after):
                cur_val, pk_val = str(cursor_after).split("|", 1)
            elif pk and pk != cursor_column:
                cur_val, pk_val = cursor_after, ""
            else:
                cur_val, pk_val = cursor_after, None
            params.append(bigquery.ScalarQueryParameter("cursor", "STRING", str(cur_val)))
            if pk and pk != cursor_column and pk_val is not None:
                pk_q = quote_column_list(
                    [require_safe_identifier(pk, preserve_case=True)],
                    quote_char="`",
                )
                where = (
                    f" WHERE ({cursor_q} > @cursor OR "
                    f"({cursor_q} = @cursor AND {pk_q} > @pk))"
                )
                params.append(bigquery.ScalarQueryParameter("pk", "STRING", str(pk_val)))
                order = f"{cursor_q}, {pk_q}"
            else:
                where = f" WHERE {cursor_q} > @cursor"
        elif pk and pk != cursor_column:
            pk_q = quote_column_list(
                [require_safe_identifier(pk, preserve_case=True)],
                quote_char="`",
            )
            order = f"{cursor_q}, {pk_q}"
        query = (
            f"SELECT {col_sql} FROM {table_ref}{where} "
            f"ORDER BY {order} LIMIT {int(limit)}"
        )
        job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
        job = client.query(query, job_config=job_config)
        rows_iter = job.result()
        if job.schema:
            headers = [field.name for field in job.schema]
        else:
            headers = columns or []
        rows = [[cell_to_string(v, preserve_sql_null=True) for v in row.values()] for row in rows_iter]
        # Keyset pages are not a cardinality bound — page length must never
        # trip stream early-stop (fetch_offset >= total_rows).
        return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)
    except Exception as exc:
        raise RuntimeError(f"BigQuery cursor read failed for {table_ref}: {exc}") from exc
