"""Shared Elasticsearch identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / cluster ``_count`` after
refresh — never ``_cat/indices`` ``docs.count``, never reindex
``created`` ack, never bulk helpers. Same host+port+index declines.
Cross-endpoint ``_reindex`` declines (identity COPY stays on one
cluster). Occupancy is counted **before** delete. Desktop-lab
Elasticsearch on :9200 is not a customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable

_ES_FAMILY = frozenset({
    "elasticsearch",
    "opensearch",
    "amazon_elasticsearch",
    "amazon_opensearch",
    "elastic_cloud",
    "elastic",
})

_ES_COPY_SAFE_TYPES = frozenset({
    "long",
    "integer",
    "int",
    "short",
    "byte",
    "string",
    "text",
    "keyword",
    "boolean",
    "bool",
    "date",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "json",
    "object",
    "nested",
    "ip",
    "binary",
    "geo_point",
    "geo_shape",
    "dense_vector",
    "rank_feature",
    "flatten",
    "flattened",
})


def elasticsearch_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _ES_FAMILY:
        return "elasticsearch"
    return n


def elasticsearch_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("<", 1)[0].split("(", 1)[0].strip()
    return base in _ES_COPY_SAFE_TYPES


def elasticsearch_index(table: str) -> str:
    name = (table or "").strip()
    if not name:
        raise FastPathUnavailable("Elasticsearch index required")
    if any(ch in name for ch in "*, ?\\/"):
        raise FastPathUnavailable("Elasticsearch COPY refuses glob characters in the index")
    if name != name.lower():
        raise FastPathUnavailable("Elasticsearch COPY requires a lowercase index name")
    if name.startswith(".") or name.startswith("_"):
        raise FastPathUnavailable("Elasticsearch COPY refuses system index names")
    return name


def elasticsearch_endpoint_key(cfg: dict[str, Any]) -> str:
    host = str(cfg.get("host") or "").strip().lower()
    port = int(cfg.get("port") or 9200)
    cs = str(cfg.get("connection_string") or "").strip()
    if cs:
        from connectors.url_authority import parse_url_authority

        parsed = parse_url_authority(cs)
        if parsed.host:
            host = str(parsed.host).strip().lower()
        if parsed.port:
            port = int(parsed.port)
        elif "://" in cs:
            scheme = cs.split("://", 1)[0].lower()
            if not parsed.port:
                port = 443 if scheme == "https" else 9200
    host = host.replace("localhost", "127.0.0.1") or "127.0.0.1"
    return f"{host}:{port}"


def elasticsearch_object_id(cfg: dict[str, Any], index: str) -> tuple[str, str]:
    return (elasticsearch_endpoint_key(cfg), elasticsearch_index(index))


def elasticsearch_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def elasticsearch_dest_count(cfg: dict[str, Any], index: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "elasticsearch",
        {**cfg, "type": "elasticsearch", "format": "elasticsearch"},
        schema="",
        table_name=elasticsearch_index(index),
    )
    if n is None:
        raise ValueError(f"Elasticsearch dest COUNT unmeasured for {index}")
    return int(n)


def elasticsearch_index_exists(cfg: dict[str, Any], index: str) -> bool:
    from connectors.elasticsearch_reader import _client

    client = _client(cfg)
    try:
        return bool(client.indices.exists(index=elasticsearch_index(index)))
    finally:
        _close_quiet(client)


def elasticsearch_delete_index(cfg: dict[str, Any], index: str) -> None:
    from connectors.elasticsearch_reader import _client

    name = elasticsearch_index(index)
    client = _client(cfg)
    try:
        if client.indices.exists(index=name):
            client.indices.delete(index=name)
    finally:
        _close_quiet(client)


def elasticsearch_reindex(
    *,
    src_cfg: dict[str, Any],
    src_index: str,
    dest_index: str,
) -> None:
    """Cluster ``_reindex``. Never scroll+bulk / helpers.reindex the payload."""
    from connectors.elasticsearch_reader import _client

    client = _client(src_cfg)
    try:
        resp = client.reindex(
            source={"index": elasticsearch_index(src_index)},
            dest={"index": elasticsearch_index(dest_index)},
            wait_for_completion=True,
            refresh=True,
            timeout="5m",
        )
        body = resp.body if hasattr(resp, "body") else dict(resp)
        failures = body.get("failures") or []
        if failures:
            raise ValueError(
                f"Elasticsearch _reindex failures: {failures[:3]}"
            )
        if body.get("timed_out"):
            raise ValueError("Elasticsearch _reindex timed out")
    finally:
        _close_quiet(client)


def skip_complete_elasticsearch(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "index",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def _close_quiet(client: Any) -> None:
    try:
        client.close()
    except Exception:
        return
