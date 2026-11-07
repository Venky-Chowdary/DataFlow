"""Shared DynamoDB identity-COPY helpers.

Dest COUNT is ``Scan Select=COUNT`` via ``destination_row_count`` — never
``DescribeTable.ItemCount`` (stale ~6h), never ListTables length, never
PutItem / BatchWriteItem ack. DynamoDB Local has no ExportTable /
ImportTable / PITR clone, so identity bulk is a Scan of **raw**
AttributeValue maps plus ``BatchWriteItem`` PutRequest. Python never
TypeDeserializer / TypeSerializer the payload (that path invents Decimal
vs N). DynamoDB Local on :8000 is an emulator, not a customer-tenant
PRODUCTION_SKU.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from connectors.aws_common import boto3_client, resolve_endpoint_url
from services.copy_fast_path import FastPathResult, FastPathUnavailable

logger = logging.getLogger(__name__)

_DYNAMODB_FAMILY = frozenset({
    "dynamodb",
    "amazon_dynamodb",
})

_BATCH = 25  # DynamoDB BatchWriteItem hard limit


def dynamodb_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _DYNAMODB_FAMILY:
        return "dynamodb"
    return n


def dynamodb_table(cfg: dict[str, Any], table: str | None = None) -> str:
    name = str(table or cfg.get("table") or cfg.get("database") or "").strip()
    if not name:
        raise FastPathUnavailable("DynamoDB table required")
    return name


def dynamodb_endpoint_key(cfg: dict[str, Any]) -> str:
    raw = (
        resolve_endpoint_url(cfg)
        or str(cfg.get("endpoint_url") or cfg.get("connection_string") or "")
    ).strip().lower().rstrip("/")
    return raw.replace("://localhost", "://127.0.0.1") or "aws-dynamodb-default"


def dynamodb_object_id(cfg: dict[str, Any], table: str) -> tuple[str, str]:
    return (dynamodb_endpoint_key(cfg), dynamodb_table(cfg, table).strip().lower())


def dynamodb_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "endpoint_url", "dsn")
    )


def dynamodb_type_is_copy_safe(declared_or_key: str) -> bool:
    """Wire AttributeValues are identity-safe. Decline only non-item carriers."""
    raw = (declared_or_key or "").strip().lower().replace(" ", "")
    if not raw:
        return True
    base = raw.split("(", 1)[0]
    if base in {"javascript", "regex", "minkey", "maxkey", "dbref"}:
        return False
    return True


def dynamodb_dest_count(cfg: dict[str, Any], table: str) -> int:
    from services.dest_precount import destination_row_count

    name = dynamodb_table(cfg, table)
    n = destination_row_count(
        "dynamodb",
        {**cfg, "table": name, "type": "dynamodb"},
        schema="",
        table_name=name,
    )
    if n is None:
        raise ValueError(f"DynamoDB dest COUNT unmeasured for {name}")
    return int(n)


def dynamodb_describe_table(cfg: dict[str, Any], table: str) -> dict[str, Any] | None:
    from botocore.exceptions import ClientError

    client = boto3_client("dynamodb", cfg)
    name = dynamodb_table(cfg, table)
    try:
        return client.describe_table(TableName=name)["Table"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return None
        raise FastPathUnavailable(f"DynamoDB describe failed: {exc}") from exc


def dynamodb_key_names(info: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for key in info.get("KeySchema") or []:
        name = str(key.get("AttributeName") or "")
        if name:
            names.append(name)
    return names


def dynamodb_ensure_table_like_source(
    dest_cfg: dict[str, Any],
    dest_table: str,
    source_info: dict[str, Any],
) -> None:
    """Create dest with the source HASH/RANGE keys. GSIs are not cloned."""
    from botocore.exceptions import ClientError

    client = boto3_client("dynamodb", dest_cfg)
    name = dynamodb_table(dest_cfg, dest_table)
    try:
        client.describe_table(TableName=name)
        return
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise FastPathUnavailable(f"DynamoDB dest describe failed: {exc}") from exc
    key_schema = list(source_info.get("KeySchema") or [])
    if not key_schema:
        raise FastPathUnavailable("DynamoDB source KeySchema missing")
    key_names = {str(k.get("AttributeName") or "") for k in key_schema}
    attr_defs = [
        a
        for a in (source_info.get("AttributeDefinitions") or [])
        if str(a.get("AttributeName") or "") in key_names
    ]
    if not attr_defs:
        raise FastPathUnavailable("DynamoDB source key AttributeDefinitions missing")
    try:
        client.create_table(
            TableName=name,
            AttributeDefinitions=attr_defs,
            KeySchema=key_schema,
            BillingMode="PAY_PER_REQUEST",
        )
        client.get_waiter("table_exists").wait(TableName=name)
    except Exception as exc:
        try:
            client.describe_table(TableName=name)
            return
        except Exception:
            logger.debug("DynamoDB dest table re-probe skipped", exc_info=True)
        raise FastPathUnavailable(f"DynamoDB table create failed: {exc}") from exc


def dynamodb_delete_table(cfg: dict[str, Any], table: str) -> None:
    from botocore.exceptions import ClientError

    client = boto3_client("dynamodb", cfg)
    name = dynamodb_table(cfg, table)
    try:
        client.delete_table(TableName=name)
        client.get_waiter("table_not_exists").wait(TableName=name)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return
        logger.debug("DynamoDB delete_table skipped", exc_info=True)


def dynamodb_scan_items(cfg: dict[str, Any], table: str) -> Iterator[dict[str, Any]]:
    """Yield raw AttributeValue maps. Never deserialize."""
    client = boto3_client("dynamodb", cfg)
    name = dynamodb_table(cfg, table)
    start_key: dict[str, Any] | None = None
    while True:
        kwargs: dict[str, Any] = {"TableName": name}
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = client.scan(**kwargs)
        for item in resp.get("Items") or []:
            if isinstance(item, dict):
                yield item
        start_key = resp.get("LastEvaluatedKey") or None
        if not start_key:
            return


def dynamodb_batch_put_items(
    cfg: dict[str, Any], table: str, items: list[dict[str, Any]]
) -> int:
    """BatchWriteItem of raw AttributeValues. Not PutItem, not Export/Import."""
    if not items:
        return 0
    from connectors.dynamodb_writer import _batch_write_with_retry

    client = boto3_client("dynamodb", cfg)
    name = dynamodb_table(cfg, table)
    written = 0
    for i in range(0, len(items), _BATCH):
        chunk = items[i : i + _BATCH]
        requests = [{"PutRequest": {"Item": item}} for item in chunk]
        _batch_write_with_retry(client, name, requests)
        written += len(chunk)
    return written


def skip_complete_dynamodb(
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
