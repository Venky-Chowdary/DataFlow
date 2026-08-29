"""Object-store leftover MERGE — dest-engine identity, not generation-id wipe.

Airbyte S3 overwrite tags objects with ``x-amz-meta-ab-generation-id`` and
deletes previous generations after a successful sync. Leftovers persist when
a sync fails between generations, when another connection writes the same
prefix, or when an operator lands a file by hand (airbytehq/airbyte#61522).
Fivetran warehouse leftover is a ``_fivetran_deleted`` soft-flag so
``COUNT(*)`` does not drop. Fivetran MDS Iceberg is CoW rewrite + snapshot
GC, not dest-engine row-identity leftover MERGE.

DataFlow dest-engine identity:

    leftover = D \\ S
    rewrite artifacts without leftover PKs (or delete leftover-only objects)
    dest COUNT (GET streams) drops
    extra → 0

Incremental leftover MERGE stays a hard no-op in
``apply_inferred_leftover_deletes``. This module never invents a second
confidence and never uses writer PUT rowcount as dest COUNT.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping, Sequence

logger = logging.getLogger(__name__)


def object_store_records(
    db_type: str,
    cfg: Mapping[str, Any],
    *,
    table_name: str,
) -> list[tuple[str, dict[str, Any]]] | None:
    """``(object_key, record)`` pairs from dest-engine GET streams.

    Same listing and GET handles as dest COUNT. Unreadable artifact is
    ``None`` (unmeasured). Missing listing is ``[]``.
    """
    from services.dest_precount import (
        UnmeasuredArtifact,
        _iter_artifact_records,
        _object_store_kind,
        _object_store_list_keys,
    )
    from services.object_streaming import open_object_store_binary

    bucket = str(cfg.get("database") or "").strip()
    key = str(table_name or "").strip()
    if not bucket or not key:
        return None
    kind = _object_store_kind(db_type)
    if kind not in {"s3", "gcs", "adls"}:
        return None
    listed = _object_store_list_keys(kind, dict(cfg), bucket, key)
    if listed is None:
        return None
    out: list[tuple[str, dict[str, Any]]] = []
    for obj_key in listed:
        opened = open_object_store_binary(kind, dict(cfg), bucket, str(obj_key))
        if opened is False:
            continue
        if opened is None:
            return None
        stream, closer = opened
        try:
            for rec in _iter_artifact_records(stream, name=str(obj_key)):
                if isinstance(rec, Mapping):
                    out.append((str(obj_key), dict(rec)))
        except UnmeasuredArtifact as exc:
            logger.info("object-store leftover listing unmeasured: %s", exc)
            return None
        finally:
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass
    return out


def object_store_key_list(
    db_type: str,
    cfg: Mapping[str, Any],
    *,
    table_name: str,
    cols: Sequence[str],
) -> list[tuple[Any, ...]] | None:
    """Dest-engine PK tuples from GET streams. Never listing cardinality."""
    from services.dest_precount import _row_values_for_cols

    records = object_store_records(db_type, cfg, table_name=table_name)
    if records is None:
        return None
    width = len(cols)
    out: list[tuple[Any, ...]] = []
    for _obj_key, rec in records:
        tup = _row_values_for_cols(rec, cols)
        if tup is None or len(tup) != width:
            continue
        out.append(tup)
    return out


def object_store_key_hits(
    db_type: str,
    cfg: Mapping[str, Any],
    *,
    table_name: str,
    cols: Sequence[str],
    keys: Sequence[tuple[Any, ...]],
) -> int | None:
    """How many of these keys dest holds — dest-engine GET, not writer ack."""
    from services.dest_precount import _norm_dest_key, _row_values_for_cols

    records = object_store_records(db_type, cfg, table_name=table_name)
    if records is None:
        return None
    wanted = {norm for key in keys if (norm := _norm_dest_key(key)) is not None}
    if not wanted:
        return 0
    seen: set[tuple[str, ...]] = set()
    for _obj_key, rec in records:
        tup = _row_values_for_cols(rec, cols)
        if tup is None:
            continue
        norm = _norm_dest_key(tup)
        if norm is not None and norm in wanted:
            seen.add(norm)
    return len(seen)


def delete_by_primary_keys(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str | list[str],
    keys: list[str],
    schema: str | None = None,
) -> int:
    """Rewrite dest artifacts without leftover PKs. Dest COUNT must drop.

    Leftover-only objects are deleted. Mixed objects are rewritten in the
    same format (JSON / JSONL / CSV / Parquet). A rewrite failure raises
    ``DestinationDeleteError`` — never return 0 for a failed PUT.
    """
    del schema
    from connectors.table_manager import DestinationDeleteError
    from services.cdc_snapshot_window import _pk_columns
    from services.dest_precount import (
        _infer_artifact_format,
        _norm_dest_key,
        _object_store_kind,
        _object_store_list_keys,
        _row_values_for_cols,
    )
    from services.row_conservation import parse_delete_keys

    kind = _object_store_kind(db_type)
    if kind not in {"s3", "gcs", "adls"}:
        return 0
    pk_cols = _pk_columns(primary_key_column)
    leftover = parse_delete_keys(keys, len(pk_cols))
    leftover_set = {norm for tup in leftover if (norm := _norm_dest_key(tup)) is not None}
    if not leftover_set:
        return 0
    bucket = str(cfg.get("database") or "").strip()
    key = str(table_name or "").strip()
    if not bucket or not key:
        raise DestinationDeleteError(table_name, ValueError("object-store leftover MERGE needs bucket and key"))
    listed = _object_store_list_keys(kind, cfg, bucket, key)
    if listed is None:
        raise DestinationDeleteError(table_name, RuntimeError("object-store leftover listing unmeasured"))
    records = object_store_records(db_type, cfg, table_name=table_name)
    if records is None:
        raise DestinationDeleteError(table_name, RuntimeError("object-store leftover GET unmeasured"))

    by_object: dict[str, list[dict[str, Any]]] = {obj: [] for obj in listed}
    deleted = 0
    for obj_key, rec in records:
        tup = _row_values_for_cols(rec, pk_cols)
        norm = _norm_dest_key(tup) if tup is not None else None
        if norm is not None and norm in leftover_set:
            deleted += 1
            continue
        by_object.setdefault(obj_key, []).append(rec)

    if deleted == 0:
        return 0
    try:
        for obj_key, kept in by_object.items():
            if not kept:
                _delete_object_key(kind, cfg, bucket, obj_key)
                continue
            body, content_type = _serialize_kept(obj_key, kept)
            _put_object_key(kind, cfg, bucket, obj_key, body, content_type)
    except DestinationDeleteError:
        raise
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc
    return deleted


def _serialize_kept(obj_key: str, kept: list[dict[str, Any]]) -> tuple[bytes, str]:
    from pathlib import Path

    from connectors.object_store_common import serialize_object_store_body
    from services.dest_precount import _infer_artifact_format

    fmt = _infer_artifact_format(Path(obj_key), None) or "json"
    cols: list[str] = []
    seen: set[str] = set()
    for rec in kept:
        for name in rec.keys():
            text = str(name)
            if text not in seen:
                seen.add(text)
                cols.append(text)
    if not cols:
        cols = ["id"]
    mapped = [tuple(rec.get(c) for c in cols) for rec in kept]
    write_key = obj_key
    if fmt == "json" and not write_key.lower().endswith(".json"):
        write_key = f"{write_key}.json"
    return serialize_object_store_body(
        key=write_key,
        mapped_rows=mapped,
        target_cols=cols,
    )


def _put_object_key(
    kind: str,
    cfg: Mapping[str, Any],
    bucket: str,
    obj_key: str,
    body: bytes,
    content_type: str,
) -> None:
    if kind == "s3":
        from connectors.aws_common import boto3_client

        client = boto3_client("s3", dict(cfg))
        client.put_object(
            Bucket=bucket,
            Key=obj_key,
            Body=body,
            ContentType=content_type or "application/json",
        )
        return
    if kind == "gcs":
        from connectors.gcs_common import gcs_client

        blob = gcs_client(dict(cfg)).bucket(bucket).blob(obj_key)
        blob.upload_from_string(body, content_type=content_type or "application/json")
        return
    if kind == "adls":
        from connectors.adls_common import blob_service_client

        client = blob_service_client(dict(cfg)).get_blob_client(bucket, obj_key)
        client.upload_blob(body, overwrite=True, content_type=content_type or "application/json")
        return
    raise RuntimeError(f"object-store leftover PUT unsupported for {kind}")


def _delete_object_key(kind: str, cfg: Mapping[str, Any], bucket: str, obj_key: str) -> None:
    if kind == "s3":
        from connectors.aws_common import boto3_client

        boto3_client("s3", dict(cfg)).delete_object(Bucket=bucket, Key=obj_key)
        return
    if kind == "gcs":
        from connectors.gcs_common import gcs_client

        try:
            gcs_client(dict(cfg)).bucket(bucket).blob(obj_key).delete()
        except Exception as exc:
            if "404" not in str(exc) and "NotFound" not in type(exc).__name__:
                raise
        return
    if kind == "adls":
        from connectors.adls_common import blob_service_client

        try:
            blob_service_client(dict(cfg)).get_blob_client(bucket, obj_key).delete_blob()
        except Exception as exc:
            if "BlobNotFound" not in str(exc) and "404" not in str(exc):
                raise
        return
    raise RuntimeError(f"object-store leftover DELETE unsupported for {kind}")
