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
    api_key: str = "",
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
        elif api_key.strip():
            api_key = api_key.strip()
            if ":" in api_key:
                key_id, key_value = api_key.split(":", 1)
                kwargs["api_key"] = (key_id, key_value)
            else:
                kwargs["api_key"] = api_key
        client = Elasticsearch(**kwargs)
        health = client.cluster.health()
        status = health.get("status", "unknown")
        indices: list[str] = []
        if index_hint:
            if client.indices.exists(index=index_hint):
                indices = [index_hint]
            else:
                # The writer will create the index on demand; allow destination probes to pass.
                indices = [index_hint]
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
