"""Elasticsearch connector — cluster health probe when elasticsearch-py is available."""

from __future__ import annotations

from connectors.base import ConnectResult


def test_elasticsearch(
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
    del schema, warehouse

    if not host and not connection_string:
        return ConnectResult(ok=False, tables=[], error="Elasticsearch host or URL is required.")

    if connection_string.strip():
        url = connection_string.strip()
    else:
        scheme = "https" if ssl or port == 443 else "http"
        url = f"{scheme}://{host or 'localhost'}:{port or 9200}"

    index_hint = (database or "").strip()

    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        from connectors.driver_guard import require_driver
        return ConnectResult(
            ok=False,
            tables=[],
            error=require_driver("elasticsearch", "elasticsearch"),
            driver="none",
        )

    try:
        kwargs: dict = {"hosts": [url], "request_timeout": 8}
        if username and password:
            kwargs["basic_auth"] = (username, password)
        client = Elasticsearch(**kwargs)
        health = client.cluster.health()
        status = health.get("status", "unknown")
        indices: list[str] = []
        if index_hint:
            if client.indices.exists(index=index_hint):
                indices = [index_hint]
            else:
                return ConnectResult(
                    ok=False,
                    tables=[],
                    error=f"Index `{index_hint}` not found (cluster status: {status}).",
                )
        else:
            cat = client.cat.indices(format="json", h="index")
            indices = [row["index"] for row in cat[:20] if not row["index"].startswith(".")]
        client.close()
        return ConnectResult(
            ok=True,
            tables=indices or ["(no user indices)"],
            message=f"Elasticsearch cluster reachable — status {status}, {len(indices)} index(es) listed.",
            driver="elasticsearch-py",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="elasticsearch-py")
