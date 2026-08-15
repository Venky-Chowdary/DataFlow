"""Redshift COPY FROM S3 — Fivetran/Airbyte-class bulk load.

PostgreSQL-wire ``COPY FROM STDIN`` is disabled on port 5439. Production
Redshift loads stage a COPY TEXT file to S3 and run ``COPY … FROM 's3://…'``.
This module is the SQL + staging contract. Missing staging config is opt-in
(existing PG-wire insert still works). When config *is* present, a failed
stage or COPY fails closed — no silent executemany fallback.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any

from services.brand_env import getenv_brand

from connectors.postgresql_writer import _copy_text_value
from connectors.sql_identifiers import quote_sql_identifier

REDSHIFT_COPY_THRESHOLD = int(getenv_brand("REDSHIFT_COPY_THRESHOLD", "1000") or "1000")

_S3_URI_RE = re.compile(r"^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/.+$", re.IGNORECASE)
_IAM_ROLE_RE = re.compile(r"^arn:aws:iam::\d{12}:role/[A-Za-z0-9+=,.@_-]+$")


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
) -> str:
    """Build ``COPY … FROM 's3://…'`` matching PG COPY TEXT (tab + ``\\N``)."""
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
) -> str:
    run = re.sub(r"[^A-Za-z0-9_-]", "", str(job_id or ""))[:40] or uuid.uuid4().hex[:12]
    safe_table = re.sub(r"[^A-Za-z0-9_-]", "_", table or "table")[:40]
    return f"{prefix.rstrip('/')}/{run}/{safe_table}/part-{int(chunk_idx):05d}.tsv"


def stage_redshift_copy_object(
    *,
    client: Any,
    bucket: str,
    key: str,
    body: bytes,
) -> str:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/tab-separated-values",
    )
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
) -> str:
    """Stage COPY TEXT to S3 and execute Redshift COPY. Returns the S3 URI."""
    if not rows:
        return ""
    body = render_redshift_copy_text(rows)
    key = staging_object_key(
        table=table,
        job_id=job_id,
        chunk_idx=chunk_idx,
        prefix=config.key_prefix,
    )
    client = s3_client or default_s3_client(config)
    uri = stage_redshift_copy_object(client=client, bucket=config.bucket, key=key, body=body)
    sql = build_redshift_copy_sql(
        schema,
        table,
        columns,
        uri,
        iam_role=config.iam_role,
        region=config.region,
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
