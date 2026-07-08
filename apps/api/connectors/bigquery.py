"""BigQuery connector — probe datasets/tables when google-cloud-bigquery is available."""

from __future__ import annotations

from connectors.base import ConnectResult


def test_bigquery(
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
    del port, username, password, ssl, warehouse
    project_id = database or host
    dataset_id = schema or "dataflow"
    if not project_id:
        return ConnectResult(ok=False, tables=[], error="Provide GCP project ID as database field", driver="stub")

    try:
        from google.cloud import bigquery  # noqa: F401
    except ImportError:
        return ConnectResult(
            ok=False,
            tables=[],
            error="BigQuery driver not installed — pip install google-cloud-bigquery",
            driver="none",
        )

    try:
        from connectors.bigquery_conn import get_client

        client = get_client(project_id=project_id, credentials_path=connection_string)
        tables: list[str] = []
        for table in client.list_tables(f"{project_id}.{dataset_id}", max_results=50):
            tables.append(table.table_id)
        return ConnectResult(
            ok=True,
            tables=tables or [f"(no tables in {dataset_id})"],
            message=f"BigQuery connected — {len(tables)} tables in `{project_id}.{dataset_id}`",
            driver="google-cloud-bigquery",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="google-cloud-bigquery")
