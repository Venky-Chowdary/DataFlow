"""Which batch keys a key-addressed store already holds (claims audit CLM-16).

A Redis or DynamoDB append writes each row at its key; a key already stored is
refused by the writer at run time. The SQL destinations answer this at Validate
with the append collision probe; these readers let the same probe ask Redis
(EXISTS on the writer's own key layout) and DynamoDB (GetItem on the table's
key schema).
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

KEY_ADDRESSED_STORES = frozenset({"redis", "dynamodb"})


def existing_keys(
    db_type: str, cfg: Mapping[str, Any], table: str, key_column: str, values: Sequence[str], limit: int
) -> list[dict[str, Any]]:
    if db_type == "redis":
        from connectors.redis_reader import _redis_client, redis_key_for, redis_key_format

        client = _redis_client(dict(cfg))
        fmt = redis_key_format(dict(cfg))
        pipe = client.pipeline(transaction=False)
        for value in values:
            pipe.exists(redis_key_for(table, value, key_format=fmt))
        hits = pipe.execute()
        return [{"column": key_column, "value": str(v)} for v, hit in zip(values, hits) if hit][:limit]
    if db_type == "dynamodb":
        try:
            present = _dynamodb_present(cfg, table, key_column, values)
        except KeyError:
            return []
        return [{"column": key_column, "value": str(v)} for v in present][:limit]
    return []


def _dynamodb_present(cfg: Mapping[str, Any], table: str, key_column: str, values: Sequence[str]) -> list[str]:
    from botocore.exceptions import ClientError

    from connectors.aws_common import boto3_client
    from connectors.dynamodb_reader import describe_key_schema
    from connectors.dynamodb_writer import _coerce_dynamo_cell, _to_attr

    try:
        schema = describe_key_schema(dict(cfg), table)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            raise KeyError(table) from exc
        raise
    declared = {str(i.get("name") or ""): str(i.get("attr_type") or "VARCHAR") for i in schema}
    if [n.lower() for n in declared] != [key_column.lower()]:
        return []
    name, logical = next(iter(declared.items()))
    letter = {"VARCHAR": "S", "DECIMAL": "N", "BINARY": "B"}.get(logical, "S")
    client = boto3_client("dynamodb", dict(cfg))
    present: list[str] = []
    for value in values:
        attr = _to_attr(_coerce_dynamo_cell(value, col=name, logical_type=logical, key_types={name: letter}), logical)
        if (client.get_item(TableName=table, Key={name: attr}, ConsistentRead=True) or {}).get("Item"):
            present.append(str(value))
    return present
