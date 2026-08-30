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
    service_account: str = "",
) -> ConnectResult:
    del username, schema, ssl, warehouse

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
            "port": port,
            "service_account": service_account,
            "connection_string": connection_string or password,
            "password": password,
        })
        from connectors.gcs_common import gcs_emulator_kwargs

        probe_kw = gcs_emulator_kwargs({
            "host": host,
            "port": port,
            "connection_string": connection_string or password,
        })
        bucket_obj = client.bucket(bucket)
        if not bucket_obj.exists(**probe_kw):
            # Create-new: a reachable emulator with no bucket is not "disconnected".
            return ConnectResult(
                ok=True,
                tables=[],
                message=(
                    f"GCS endpoint reachable — bucket `{bucket}` is missing and "
                    "will be created on first write."
                ),
                driver="google-cloud-storage",
            )
        keys = [b.name for b in client.list_blobs(bucket, max_results=100)]
        return ConnectResult(
            ok=True,
            tables=keys or [bucket],
            message=f"GCS bucket `{bucket}` reachable — {len(keys) or 1} object(s) listed.",
            driver="google-cloud-storage",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
