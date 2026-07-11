"""Google Cloud Storage connector — bucket probe."""

from __future__ import annotations

from connectors.base import ConnectResult
from connectors.gcs_common import gcs_client


def test_gcs(
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
    del port, username, schema, ssl, warehouse

    bucket = (database or connection_string or "").strip()
    if not bucket:
        return ConnectResult(ok=False, tables=[], error="Bucket name is required (Database field).")

    try:
        from google.cloud import storage  # noqa: F401
    except ImportError:
        from connectors.driver_guard import require_driver

        return ConnectResult(
            ok=False,
            tables=[],
            error=require_driver("google.cloud.storage", "google-cloud-storage"),
            driver="none",
        )

    try:
        client = gcs_client({
            "host": host,
            "connection_string": connection_string or password,
            "password": password,
        })
        bucket_obj = client.bucket(bucket)
        if not bucket_obj.exists():
            return ConnectResult(ok=False, tables=[], error=f"GCS bucket `{bucket}` not found.")
        keys = [b.name for b in client.list_blobs(bucket, max_results=100)]
        return ConnectResult(
            ok=True,
            tables=keys or [bucket],
            message=f"GCS bucket `{bucket}` reachable — {len(keys) or 1} object(s) listed.",
            driver="google-cloud-storage",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
