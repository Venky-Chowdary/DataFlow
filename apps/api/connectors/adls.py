"""Azure Blob Storage / ADLS Gen2 connector test."""

from __future__ import annotations

import itertools

from connectors.adls_common import blob_service_client
from connectors.base import ConnectResult


def test_adls(
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
    del ssl, warehouse
    container = database or schema
    cfg = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "database": container,
        "service_account": service_account,
        # Probe-only timeouts: fail fast on unreachable endpoints so tests
        # and the UI connector-test don't hang.
        "connection_timeout": 5,
        "read_timeout": 5,
        "retry_total": 0,
    }

    try:
        client = blob_service_client(cfg)
        # Lightweight connectivity probe
        list(client.list_containers())[:1]
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="azure-storage-blob")

    if not container:
        return ConnectResult(
            ok=True,
            tables=[],
            message="Azure Blob Storage reachable. Set Database/Container to list blobs.",
            driver="azure-storage-blob",
        )

    try:
        container_client = client.get_container_client(container)
        if not container_client.exists():
            return ConnectResult(
                ok=True,
                tables=[],
                message=f"Container `{container}` does not exist yet; DataFlow will create it during write.",
                driver="azure-storage-blob",
            )
        blobs = [b.name for b in itertools.islice(container_client.list_blobs(), 50)]
        return ConnectResult(
            ok=True,
            tables=blobs,
            message=f"Container `{container}` reachable — {len(blobs)} blob(s) listed.",
            driver="azure-storage-blob",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="azure-storage-blob")
