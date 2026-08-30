"""BigQuery table reader."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.sql_identifiers import (
    quote_column_list,
    quote_table_ref,
    require_safe_identifier,
)

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

    from google.cloud import bigquery
    from connectors.bigquery_conn import _is_local_endpoint, get_client

    try:
        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            service_account=service_account,
            host=host,
            port=port,
            connection_string=connection_string,
        )
        is_local = _is_local_endpoint(host, connection_string or "")[0]

        # API reference uses the raw project/dataset/table names.  ``table_ref``
        # is the backtick-quoted SQL form used only for ``query()`` strings.
        api_ref = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id), table
        )

        if known_total_rows is not None:
            total = known_total_rows
        elif is_local:
            # Emulator path: `query().result()` can hang on some fake-BigQuery
            # implementations (e.g. goccy/bigquery-emulator), while `get_table`
            # and `list_rows` are reliable.
            bq_table = client.get_table(api_ref)
            total = bq_table.num_rows or 0
        else:
            count_q = f"SELECT COUNT(*) AS cnt FROM {table_ref}"  # nosec B608
            total = int(list(client.query(count_q).result(timeout=60))[0]["cnt"])

        # Determine columns/ordering.
        if columns:
            order_cols = list(columns)
        else:
            bq_table = client.get_table(api_ref)
            order_cols = [field.name for field in (bq_table.schema or [])]
        if not order_cols:
            raise RuntimeError("BigQuery table has no columns for stable pagination")

        if is_local:
            rows_iter = client.list_rows(
                api_ref,
                max_results=limit,
                start_index=offset,
            )
            rows_list = list(rows_iter)
            # Local emulators may ignore max_results/start_index; sort and slice
            # defensively so pagination/resume stays deterministic.
            if len(rows_list) > limit or offset:

                def _row_key(row):
                    values = row if isinstance(row, dict) else dict(row.items())
                    return tuple(values.get(c) for c in order_cols)

                rows_list = sorted(rows_list, key=_row_key)[offset : offset + limit]
            headers = columns or order_cols
            rows = [
                [cell_to_string(row.get(c) if isinstance(row, dict) else row[c], preserve_sql_null=True) for c in headers]
                for row in rows_list
            ]
            return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)

        col_sql = (
            quote_column_list(
                [require_safe_identifier(c, preserve_case=True) for c in columns],
                quote_char="`",
            )
            if columns
            else "*"
        )
        order_sql = quote_column_list(
            [require_safe_identifier(order_cols[0], preserve_case=True)],
            quote_char="`",
        )
        query = (
            f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
            f"ORDER BY {order_sql} LIMIT {int(limit)} OFFSET {int(offset)}"
        )
        job = client.query(query)
        rows_iter = job.result(timeout=60)
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

    from services.keyset_pagination import present_cursor_bookmark, split_cursor_bookmark

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
        bookmark = present_cursor_bookmark(cursor_after)
        if bookmark is not None:
            has_tiebreak = bool(pk and pk != cursor_column)
            cur_val, split_pk = split_cursor_bookmark(
                bookmark, has_tiebreak=has_tiebreak
            )
            pk_val = split_pk if has_tiebreak else None
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
            f"SELECT {col_sql} FROM {table_ref}{where} "  # nosec B608
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


def read_table_scan_batch(
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
    scan_state: dict[str, Any],
) -> ReadBatch:
    """One ``SELECT … ORDER BY`` job + iterator pages — no LIMIT/OFFSET."""
    from connectors.sql_snapshot_scan import close_table_scan

    del username, password, ssl, warehouse
    if scan_state.get("started"):
        return _bigquery_scan_page(scan_state, offset=offset, limit=limit)

    project_id = database or host
    dataset_id = schema or "dataflow"
    table_ref = quote_table_ref(
        table,
        dialect="bigquery",
        project=project_id,
        dataset=dataset_id,
    )
    from google.cloud import bigquery
    from connectors.bigquery_conn import _is_local_endpoint, get_client

    try:
        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            service_account=service_account,
            host=host,
            port=port,
            connection_string=connection_string,
        )
        is_local = _is_local_endpoint(host, connection_string or "")[0]
        api_ref = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id), table
        )
        if known_total_rows is not None:
            total = known_total_rows
        elif is_local:
            bq_table = client.get_table(api_ref)
            total = bq_table.num_rows or 0
        else:
            count_q = f"SELECT COUNT(*) AS cnt FROM {table_ref}"  # nosec B608
            total = int(list(client.query(count_q).result(timeout=60))[0]["cnt"])
        if columns:
            order_cols = list(columns)
        else:
            bq_table = client.get_table(api_ref)
            order_cols = [field.name for field in (bq_table.schema or [])]
        if not order_cols:
            raise RuntimeError("BigQuery table has no columns for stable pagination")
        if is_local:
            rows_list = list(client.list_rows(api_ref))

            def _row_key(row):
                values = row if isinstance(row, dict) else dict(row.items())
                return tuple(values.get(c) for c in order_cols)

            rows_list = sorted(rows_list, key=_row_key)
            headers = columns or order_cols
            scan_state.update(
                started=True,
                local_rows=rows_list,
                idx=0,
                headers=headers,
                total=total,
            )
        else:
            col_sql = (
                quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in columns],
                    quote_char="`",
                )
                if columns
                else "*"
            )
            order_sql = quote_column_list(
                [require_safe_identifier(order_cols[0], preserve_case=True)],
                quote_char="`",
            )
            query = f"SELECT {col_sql} FROM {table_ref} ORDER BY {order_sql}"  # nosec B608
            job = client.query(query)
            rows_iter = job.result(timeout=300)
            if job.schema:
                headers = [field.name for field in job.schema]
            else:
                headers = list(order_cols)
            scan_state.update(
                started=True,
                iter=iter(rows_iter),
                headers=headers,
                total=total,
            )
        return _bigquery_scan_page(scan_state, offset=offset, limit=limit)
    except Exception as exc:
        close_table_scan(scan_state)
        raise RuntimeError(f"BigQuery scan failed for {table_ref}: {exc}") from exc


def _bigquery_scan_page(
    scan_state: dict[str, Any], *, offset: int, limit: int
) -> ReadBatch:
    from connectors.sql_snapshot_scan import close_table_scan

    headers = list(scan_state.get("headers") or [])
    total = scan_state.get("total")
    local_rows = scan_state.get("local_rows")
    if local_rows is not None:
        idx = int(scan_state.get("idx") or 0)
        page = local_rows[idx : idx + max(1, int(limit))]
        scan_state["idx"] = idx + len(page)
        if not page:
            close_table_scan(scan_state)
            return ReadBatch(headers=headers, rows=[], offset=offset, total_rows=total)
        rows = [
            [
                cell_to_string(
                    row.get(c) if isinstance(row, dict) else row[c],
                    preserve_sql_null=True,
                )
                for c in headers
            ]
            for row in page
        ]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    it = scan_state.get("iter")
    page_rows: list[list[str]] = []
    if it is not None:
        for row in it:
            page_rows.append(
                [cell_to_string(v, preserve_sql_null=True) for v in row.values()]
            )
            if len(page_rows) >= max(1, int(limit)):
                break
    if not page_rows:
        close_table_scan(scan_state)
        return ReadBatch(headers=headers, rows=[], offset=offset, total_rows=total)
    return ReadBatch(headers=headers, rows=page_rows, offset=offset, total_rows=total)
