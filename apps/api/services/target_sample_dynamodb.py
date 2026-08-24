"""DynamoDB destination sample reads for Gate-8 value reconciliation.

Split out of :mod:`services.target_sample`, which owns the SQL-family readers
and the dispatch. DynamoDB is the one destination whose sample read is not a
``SELECT``: it addresses items by typed key through ``BatchGetItem``, so it
carries its own key-schema handling and does not belong in the SQL path.

A read failure raises :class:`services.reconciliation.TargetSampleUnavailable`
— never an empty list, which would read as "destination is empty" and green a
lost write.
"""

from __future__ import annotations

from typing import Any

from services.reconciliation import TargetSampleUnavailable


def read_dynamodb_target_sample(
    dest: dict[str, Any],
    *,
    table_name: str,
    keys: list[Any],
    sort_key: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Read written items back out of DynamoDB for Gate-8.

    Without this the reconciler reported "no sample reader is wired for
    destination type 'dynamodb'" and refused to treat the write as proven — so a
    committed SKU destination could be written but never verified, even though a
    reader existed a module away.

    A ``BatchGetItem`` on the table's own HASH key is used when the batch keys
    are known, because a scan on a large table can miss the very rows just
    written and would report a clean load as a mismatch. The key is typed from
    ``KeySchema``: Dynamo compares ``{"S": "1"}`` and ``{"N": "1"}`` as different
    items, so guessing the wrong one silently returns nothing.
    """
    from connectors.dynamodb_reader import (
        _item_to_record,
        describe_key_schema,
        read_all_paginated,
    )

    cfg = dict(dest)
    try:
        schema = describe_key_schema(cfg, table_name)
    except Exception as exc:
        raise TargetSampleUnavailable(
            f"Could not read destination sample from 'dynamodb'.{table_name!r}: {exc}"
        ) from exc

    hash_key = next((k for k in schema if k.get("key_type") == "HASH"), None)
    composite = any(k.get("key_type") == "RANGE" for k in schema)
    hash_name = str((hash_key or {}).get("name") or "")

    # A keyed read needs the batch's keys to address the table's own hash key.
    # A composite key cannot be addressed from the hash half alone.
    if keys and hash_name and not composite and (not sort_key or sort_key == hash_name):
        attr = str((hash_key or {}).get("attr_type") or "string").lower()
        wire = "N" if attr in {"number", "int", "integer", "decimal", "float"} else "S"
        from connectors.aws_common import boto3_client

        client = boto3_client("dynamodb", cfg)
        wanted = [{hash_name: {wire: str(k)}} for k in keys[: max(1, int(limit))]]
        out: list[dict[str, Any]] = []
        try:
            # BatchGetItem caps at 100 keys per call.
            for start in range(0, len(wanted), 100):
                chunk = wanted[start : start + 100]
                resp = client.batch_get_item(RequestItems={table_name: {"Keys": chunk}})
                for item in (resp.get("Responses") or {}).get(table_name, []):
                    out.append(_item_to_record(item))
            return out[: int(limit)]
        except Exception as exc:
            raise TargetSampleUnavailable(
                f"Could not read destination sample from 'dynamodb'.{table_name!r}: {exc}"
            ) from exc

    try:
        batch = read_all_paginated(cfg, table_name, limit=max(1, int(limit)))
    except Exception as exc:
        raise TargetSampleUnavailable(
            f"Could not read destination sample from 'dynamodb'.{table_name!r}: {exc}"
        ) from exc
    return [dict(zip(batch.headers, row)) for row in batch.rows][: int(limit)]
