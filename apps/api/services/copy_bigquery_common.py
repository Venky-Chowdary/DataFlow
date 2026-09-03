"""Shared BigQuery identity-COPY helpers.

Dest COUNT is ``destination_row_count`` → ``SELECT COUNT(*)`` through
the native client (``_bigquery_row_count``). Never ``Table.num_rows``
(metadata lags the streaming buffer; goccy often reports 0). Never
writer ``insert_rows_json`` ack, never leftover MERGE, never ``CLONE``
(goccy does not support CLONE; CLONE would copy unmapped columns).
Same project+dataset+table declines. Cross-project / cross-endpoint
declines. Dataset may differ on the same project+endpoint.
goccy/bigquery-emulator (``127.0.0.1:9050``) is an emulator, not a
customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable

_BIGQUERY_FAMILY = frozenset({
    "bigquery",
    "google_bigquery",
    "bigquery_us",
    "bigquery_eu",
})

_UNSAFE_BASES = frozenset({
    "GEOGRAPHY",
    "INTERVAL",
    "RANGE",
    "STRUCT",
    "RECORD",
    "VECTOR",
})

_SAFE_BASES = frozenset({
    "INT64",
    "INT",
    "INTEGER",
    "SMALLINT",
    "BIGINT",
    "TINYINT",
    "BYTEINT",
    "FLOAT64",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "NUMERIC",
    "BIGNUMERIC",
    "DECIMAL",
    "STRING",
    "BOOL",
    "BOOLEAN",
    "DATE",
    "DATETIME",
    "TIME",
    "TIMESTAMP",
    "BYTES",
    "JSON",
    "ARRAY",
    "LONG",
})

_LOCAL_HOSTS = frozenset({
    "localhost",
    "127.0.0.1",
    "::1",
    "0.0.0.0",
    "host.docker.internal",
})


def bigquery_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _BIGQUERY_FAMILY:
        return "bigquery"
    return n


def bigquery_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper().replace(" ", "")
    if not raw:
        return True
    base = raw.split("(", 1)[0]
    if base.startswith("ARRAY<"):
        inner = base[6:].rstrip(">")
        return bigquery_type_is_copy_safe(inner)
    if base in _UNSAFE_BASES or base.startswith("STRUCT") or base.startswith("RECORD"):
        return False
    return base in _SAFE_BASES


def bigquery_project_of(cfg: dict[str, Any]) -> str:
    return str(cfg.get("database") or cfg.get("project_id") or "").strip()


def bigquery_dataset_of(cfg: dict[str, Any]) -> str:
    return str(cfg.get("schema") or cfg.get("dataset") or "").strip() or "dataflow"


def bigquery_ident(name: str) -> str:
    """Quote one BigQuery identifier. Hyphens in project ids must survive."""
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return quote_sql_identifier(
        require_safe_identifier(name, allow_raw=True, max_len=1024),
        "`",
    )


def bigquery_table_ref(project: str, dataset: str, table: str) -> str:
    return ".".join(
        bigquery_ident(part) for part in (project, dataset, table) if part
    )


def bigquery_endpoint_key(cfg: dict[str, Any]) -> str:
    from connectors.bigquery_conn import _is_local_endpoint

    host = str(cfg.get("host") or "").strip()
    conn = str(cfg.get("connection_string") or "").strip()
    is_local, url = _is_local_endpoint(host, conn)
    if url:
        return url.replace("://localhost", "://127.0.0.1").rstrip("/").lower()
    if is_local or host.lower() in _LOCAL_HOSTS:
        port = int(cfg.get("port") or 9050)
        h = "127.0.0.1" if host.lower() in _LOCAL_HOSTS or not host else host.lower()
        return f"http://{h}:{port}"
    project = bigquery_project_of(cfg).lower()
    return f"bq://{project}" if project else "bq-default"


def bigquery_same_project(src_cfg: dict[str, Any], dest_cfg: dict[str, Any]) -> bool:
    src_ep = bigquery_endpoint_key(src_cfg)
    dest_ep = bigquery_endpoint_key(dest_cfg)
    src_proj = bigquery_project_of(src_cfg).lower()
    dest_proj = bigquery_project_of(dest_cfg).lower()
    if not src_ep or not dest_ep or not src_proj or not dest_proj:
        return False
    return src_ep == dest_ep and src_proj == dest_proj


def bigquery_same_table(
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    source_table: str,
    dest_table: str,
) -> bool:
    if not bigquery_same_project(src_cfg, dest_cfg):
        return False
    src_ds = bigquery_dataset_of(src_cfg).lower()
    dest_ds = bigquery_dataset_of(dest_cfg).lower()
    return (
        src_ds == dest_ds
        and source_table.strip().lower() == dest_table.strip().lower()
    )


def bigquery_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def bigquery_connect(cfg: dict[str, Any]) -> Any:
    from connectors.bigquery_conn import get_client

    project = bigquery_project_of(cfg)
    if not project:
        raise FastPathUnavailable("BigQuery project required")
    try:
        return get_client(
            project_id=project,
            credentials_path=str(cfg.get("credentials_path") or ""),
            service_account=str(cfg.get("service_account") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
            connection_string=str(cfg.get("connection_string") or ""),
        )
    except Exception as exc:
        raise FastPathUnavailable(f"BigQuery connect failed: {exc}") from exc


def bigquery_run_sql(client: Any, sql: str) -> None:
    """Identity path must never emit MERGE / CLONE / COPY / LOAD DATA."""
    compact = f" {sql.upper()} "
    stripped = compact.lstrip()
    if (
        "MERGE " in stripped[:20]
        or stripped.startswith("MERGE ")
        or " CLONE " in compact
        or stripped.startswith("COPY ")
        or "LOAD DATA" in compact
        or "INSERT_ROWS" in compact
    ):
        raise FastPathUnavailable(
            "BigQuery identity COPY refuses MERGE / CLONE / COPY / LOAD DATA"
        )
    from services.dest_precount import _bigquery_run_job

    _bigquery_run_job(client, sql)


def bigquery_table_exists(client: Any, project: str, dataset: str, table: str) -> bool:
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    from connectors.google_emulator import google_emulator_retry
    from services.dest_precount import _is_missing_warehouse_relation

    api_ref = bigquery.TableReference(
        bigquery.DatasetReference(project, dataset), table
    )
    try:
        client.get_table(api_ref, retry=google_emulator_retry(), timeout=8.0)
        return True
    except NotFound:
        return False
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, "bigquery"):
            return False
        raise FastPathUnavailable(f"BigQuery dest exists probe failed: {exc}") from exc


def bigquery_dest_count(cfg: dict[str, Any], table: str) -> int:
    """Dest-engine ``COUNT(*)``. Missing table is 0. Unknowable fails closed."""
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "bigquery",
        cfg,
        schema=bigquery_dataset_of(cfg),
        table_name=table,
    )
    if n is None:
        raise FastPathUnavailable("BigQuery dest COUNT(*) unknowable")
    return int(n)


def skip_complete_bigquery(
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
        "shard_mode": "table",
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
