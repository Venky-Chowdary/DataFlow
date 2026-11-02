"""Redshift COPY FROM S3 — Fivetran/Airbyte-class bulk load.

PostgreSQL-wire ``COPY FROM STDIN`` is disabled on port 5439. Production
Redshift loads stage a file to S3 and run ``COPY … FROM 's3://…'``.
This module is the SQL + staging contract. Missing staging config is opt-in
(existing PG-wire insert still works). When config *is* present, a failed
stage or COPY fails closed — no silent executemany fallback.

Default stage format is COPY TEXT (tab + ``\\N``). Large TSV bodies are
gzipped. ``redshift_stage_format=parquet`` stages typed Parquet instead.
"""

from __future__ import annotations

import gzip as gzip_mod
import re
import uuid
from dataclasses import dataclass
from typing import Any

from services.brand_env import getenv_brand

from connectors.postgresql_writer import _copy_text_value
from connectors.sql_identifiers import quote_sql_identifier

REDSHIFT_COPY_THRESHOLD = int(getenv_brand("REDSHIFT_COPY_THRESHOLD", "1000") or "1000")
REDSHIFT_COPY_GZIP_THRESHOLD = int(
    getenv_brand("REDSHIFT_COPY_GZIP_THRESHOLD", "1048576") or "1048576"
)

_S3_URI_RE = re.compile(r"^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/.+$", re.IGNORECASE)
_IAM_ROLE_RE = re.compile(r"^arn:aws:iam::\d{12}:role/[A-Za-z0-9+=,.@_-]+$")
_HEAD_DENIED = {"403", "AccessDenied"}
_HEAD_MISSING = {"404", "NoSuchBucket", "NotFound"}


@dataclass(frozen=True)
class RedshiftCopyConfig:
    bucket: str
    iam_role: str
    region: str = ""
    key_prefix: str = "dataflow/redshift-stage"
    endpoint_url: str = ""
    access_key: str = ""
    secret_key: str = ""


def resolve_redshift_copy_config(
    extra: dict[str, Any] | None,
    *,
    env_get: Any = None,
) -> RedshiftCopyConfig | None:
    """Return staging config when both bucket and IAM role are present."""
    import os

    getter = env_get or os.getenv
    extra = extra if isinstance(extra, dict) else {}
    bucket = str(
        extra.get("staging_bucket")
        or extra.get("redshift_staging_bucket")
        or getter("DATAFLOW_REDSHIFT_STAGING_BUCKET")
        or ""
    ).strip()
    role = str(
        extra.get("iam_role")
        or extra.get("redshift_iam_role")
        or getter("DATAFLOW_REDSHIFT_IAM_ROLE")
        or ""
    ).strip()
    if not bucket or not role:
        return None
    if "'" in bucket or "'" in role:
        raise ValueError("Redshift COPY staging_bucket / iam_role must not contain quotes")
    return RedshiftCopyConfig(
        bucket=bucket,
        iam_role=role,
        region=str(extra.get("region") or extra.get("aws_region") or getter("AWS_REGION") or "").strip(),
        key_prefix=str(extra.get("staging_prefix") or "dataflow/redshift-stage").strip()
        or "dataflow/redshift-stage",
        endpoint_url=str(extra.get("endpoint_url") or extra.get("s3_endpoint") or "").strip(),
        access_key=str(extra.get("aws_access_key_id") or extra.get("username") or "").strip(),
        secret_key=str(extra.get("aws_secret_access_key") or extra.get("password") or "").strip(),
    )


def resolve_redshift_stage_format(extra: dict[str, Any] | None) -> str:
    """``tsv`` (default COPY TEXT) or ``parquet``."""
    extra = extra if isinstance(extra, dict) else {}
    raw = str(extra.get("redshift_stage_format") or extra.get("stage_format") or "tsv").strip().lower()
    if raw in {"parquet", "pq"}:
        return "parquet"
    return "tsv"


def should_gzip_redshift_stage(
    body: bytes,
    *,
    stage_format: str = "tsv",
    threshold: int | None = None,
) -> bool:
    """Gzip TSV only. Parquet already has its own compression — do not wrap it."""
    if (stage_format or "tsv").lower() == "parquet":
        return False
    limit = REDSHIFT_COPY_GZIP_THRESHOLD if threshold is None else int(threshold)
    return len(body) >= max(0, limit)


def should_use_redshift_s3_copy_for_insert(
    *,
    copy_config: RedshiftCopyConfig | None,
    write_mode: str,
    conflict_columns: list[str] | None,
    row_count: int,
    threshold: int | None = None,
) -> bool:
    """Insert-only: skip the S3 round-trip for tiny batches. MERGE always COPYs when configured."""
    if copy_config is None:
        return False
    if str(write_mode or "").strip().lower() != "insert":
        return False
    if conflict_columns:
        return False
    limit = REDSHIFT_COPY_THRESHOLD if threshold is None else int(threshold)
    return int(row_count) >= max(0, limit)


def render_redshift_copy_text(rows: list[tuple] | list[list]) -> bytes:
    """Same COPY TEXT bytes PostgreSQL ``COPY FROM STDIN`` would send."""
    if not rows:
        return b""
    lines = ["\t".join(_copy_text_value(v) for v in row) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def build_redshift_copy_sql(
    schema: str,
    table: str,
    columns: list[str],
    s3_uri: str,
    *,
    iam_role: str,
    region: str = "",
    gzip: bool = False,
    stage_format: str = "tsv",
) -> str:
    """Build ``COPY … FROM 's3://…'`` for TSV (tab + ``\\N``) or Parquet."""
    if not columns:
        raise ValueError("Redshift COPY requires at least one column")
    if not _S3_URI_RE.match(s3_uri) or "'" in s3_uri:
        raise ValueError(f"Refusing unsafe Redshift COPY S3 URI: {s3_uri!r}")
    if not _IAM_ROLE_RE.match(iam_role) or "'" in iam_role:
        raise ValueError("Redshift COPY IAM_ROLE must be an IAM role ARN")
    col_list = ", ".join(quote_sql_identifier(c) for c in columns)
    if schema:
        target = f"{quote_sql_identifier(schema)}.{quote_sql_identifier(table)}"
    else:
        target = quote_sql_identifier(table)
    region_clause = f" REGION '{region}'" if region and "'" not in region else ""
    fmt = (stage_format or "tsv").strip().lower()
    if fmt == "parquet":
        return (
            f"COPY {target} ({col_list}) FROM '{s3_uri}' "
            f"IAM_ROLE '{iam_role}' "
            f"FORMAT AS PARQUET{region_clause}"
        )
    gzip_clause = " GZIP" if gzip else ""
    return (
        f"COPY {target} ({col_list}) FROM '{s3_uri}' "
        f"IAM_ROLE '{iam_role}' "
        f"DELIMITER '\\t' NULL AS '\\\\N'"
        f"{region_clause}{gzip_clause}"
    )


def staging_object_key(
    *,
    table: str,
    job_id: str = "",
    chunk_idx: int = 0,
    prefix: str = "dataflow/redshift-stage",
    stage_format: str = "tsv",
) -> str:
    run = re.sub(r"[^A-Za-z0-9_-]", "", str(job_id or ""))[:40] or uuid.uuid4().hex[:12]
    safe_table = re.sub(r"[^A-Za-z0-9_-]", "_", table or "table")[:40]
    ext = "parquet" if (stage_format or "tsv").lower() == "parquet" else "tsv"
    return f"{prefix.rstrip('/')}/{run}/{safe_table}/part-{int(chunk_idx):05d}.{ext}"


def stage_redshift_copy_object(
    *,
    client: Any,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str = "text/tab-separated-values",
    content_encoding: str = "",
) -> str:
    kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        "ContentType": content_type,
    }
    if content_encoding:
        kwargs["ContentEncoding"] = content_encoding
    client.put_object(**kwargs)
    return f"s3://{bucket}/{key}"


def default_s3_client(config: RedshiftCopyConfig) -> Any:
    from connectors.aws_common import boto3_client

    cfg = {
        "host": config.region or "us-east-1",
        "username": config.access_key,
        "password": config.secret_key,
        "endpoint_url": config.endpoint_url,
        "path_style": bool(config.endpoint_url),
    }
    return boto3_client("s3", cfg)


def _error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or "")
    return ""


def probe_redshift_staging(
    extra: dict[str, Any] | None,
    *,
    s3_client: Any | None = None,
    env_get: Any = None,
) -> dict[str, Any]:
    """Metadata-only staging-bucket probe. Never PutObject.

    Statuses:
    - ``not_configured`` — no bucket+role; PostgreSQL-wire insert remains valid
    - ``ok`` — ``head_bucket`` succeeded and ACL grants write
    - ``denied`` — configured but AccessDenied / missing bucket / no write grant
    - ``unavailable`` — client/API error; do not invent a grant or a block
    """
    try:
        config = resolve_redshift_copy_config(extra, env_get=env_get)
    except ValueError as exc:
        return {
            "status": "denied",
            "method": "config",
            "engine": "redshift",
            "detail": str(exc),
        }
    if config is None:
        return {
            "status": "not_configured",
            "method": "",
            "engine": "redshift",
            "detail": (
                "Redshift COPY FROM S3 is available when staging_bucket and "
                "iam_role are set; PostgreSQL-wire insert remains valid."
            ),
        }

    base = {
        "engine": "redshift",
        "bucket": config.bucket,
        "method": "head_bucket",
    }
    try:
        client = s3_client or default_s3_client(config)
        try:
            client.head_bucket(Bucket=config.bucket)
        except Exception as exc:
            code = _error_code(exc)
            if code in _HEAD_DENIED or code in _HEAD_MISSING:
                return {
                    **base,
                    "status": "denied",
                    "detail": (
                        f"Staging bucket `{config.bucket}` is not writable for COPY "
                        f"FROM S3 ({code or exc}). Check IAM and bucket name."
                    )[:400],
                }
            return {
                **base,
                "status": "unavailable",
                "detail": f"Staging head_bucket failed: {exc}"[:400],
            }

        try:
            acl = client.get_bucket_acl(Bucket=config.bucket)
        except Exception as exc:
            code = _error_code(exc)
            return {
                **base,
                "status": "unavailable",
                "method": "GetBucketAcl",
                "detail": (
                    f"Staging GetBucketAcl unavailable ({code or exc}); "
                    "Object Ownership may disable ACLs — Validate does not PutObject "
                    "to prove write. Re-check the IAM role and bucket policy."
                )[:400],
            }

        from services.destination_privilege_probe import evaluate_s3_acl_grants

        grants = list((acl or {}).get("Grants") or [])
        can_write, _can_create = evaluate_s3_acl_grants(
            grants,
            table_exists=True,
            key_prefix=config.key_prefix,
        )
        if can_write:
            return {
                **base,
                "status": "ok",
                "method": "GetBucketAcl",
                "detail": "Staging bucket ACL grants write for COPY FROM S3",
            }
        return {
            **base,
            "status": "denied",
            "method": "GetBucketAcl",
            "detail": (
                f"Staging bucket `{config.bucket}` ACL does not grant write "
                "for COPY FROM S3. This probe does not prove the Redshift IAM "
                "role trust policy end-to-end."
            ),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            **base,
            "status": "unavailable",
            "detail": f"Staging probe failed: {exc}"[:400],
        }


def copy_redshift_rows_from_s3(
    cur: Any,
    *,
    schema: str,
    table: str,
    columns: list[str],
    rows: list[tuple] | list[list],
    config: RedshiftCopyConfig,
    s3_client: Any | None = None,
    job_id: str = "",
    chunk_idx: int = 0,
    dest_types: dict[str, str] | None = None,
    stage_format: str = "tsv",
    gzip_threshold: int | None = None,
) -> str:
    """Stage TSV or Parquet to S3 and execute Redshift COPY. Returns the S3 URI."""
    if not rows:
        return ""
    fmt = (stage_format or "tsv").strip().lower()
    if fmt == "parquet":
        from services.arrow_write import mapped_rows_to_parquet_bytes

        tuples = [tuple(row) for row in rows]
        body, content_type = mapped_rows_to_parquet_bytes(
            tuples,
            columns,
            dest_types or {},
            dialect="parquet",
        )
        use_gzip = False
        content_encoding = ""
    else:
        body = render_redshift_copy_text(rows)
        use_gzip = should_gzip_redshift_stage(
            body, stage_format="tsv", threshold=gzip_threshold
        )
        if use_gzip:
            body = gzip_mod.compress(body)
            content_type = "application/gzip"
            content_encoding = "gzip"
        else:
            content_type = "text/tab-separated-values"
            content_encoding = ""
    key = staging_object_key(
        table=table,
        job_id=job_id,
        chunk_idx=chunk_idx,
        prefix=config.key_prefix,
        stage_format=fmt,
    )
    client = s3_client or default_s3_client(config)
    uri = stage_redshift_copy_object(
        client=client,
        bucket=config.bucket,
        key=key,
        body=body,
        content_type=content_type,
        content_encoding=content_encoding,
    )
    sql = build_redshift_copy_sql(
        schema,
        table,
        columns,
        uri,
        iam_role=config.iam_role,
        region=config.region,
        gzip=use_gzip,
        stage_format=fmt,
    )
    try:
        cur.execute(sql)
    except Exception:
        raise
    try:
        client.delete_object(Bucket=config.bucket, Key=key)
    except Exception:
        pass
    return uri
