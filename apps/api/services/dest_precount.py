"""Destination cardinality taken *before* the write.

Append into a non-empty table cannot be proven by whole-table digests, so
Gate-8 falls back to cardinality. ``target_rows >= expected_rows`` is not a
proof there: a table that already held 30 rows satisfies it even if the writer
appended nothing. The only honest cardinality proof for append is the delta

    rows_after - rows_before == expected_rows

which requires the count taken before the writer runs. Keyed / CDC
conservation is the same shape:

    dest_delta == inserts - deletes
    dest_delta = COUNT(*)_after - COUNT(*)_before

Counting after the first upsert of a table is dest-after, not dest-before.
This module owns that one query, so every destination family answers it
the same way and ``reconcile()`` / the conservation ledger can tell
"delta proven" apart from "delta unknown" instead of silently reporting
the second as the first.

File/object exports have no SQL engine. ``count_artifact_rows`` is the
same identity against the bytes on disk: re-open the written artifact and
COUNT records. Writer ``rows`` / bytes-landed is Airbyte/Fivetran S3
success — it does not close conservation. Independent artifact COUNT is
cardinality, not Gate-8 cell fidelity.

Lakehouse and object-store destinations already have dest-*after* read-back
(Iceberg scan, S3/GCS/ADLS GET). Dest-*before* must use the same COUNT so
append delta and first-write overwrite (missing table/object = 0) can close.
Writer ``Table.upsert`` / PUT rowcount is not that proof.

Vector destinations (pgvector) are a 1-source-row → N-chunk identity.
Physical ``COUNT(*)`` of embedding rows is **not** dest population — that
is the Fivetran ``_deleted`` analogue for RAG: 2 documents → 5 chunks
looks like silent duplication if chunk COUNT closes overwrite. Dest
population is ``COUNT(DISTINCT source_id)``. Missing table is 0. Writer
chunk-upsert ack never closes. Milvus/Qdrant/Pinecone/Weaviate stay
unmeasured until dest-engine DISTINCT source_id exists.

``None`` means the count is unavailable (unsupported engine, missing table,
unreachable destination, or an unreadable/unsupported artifact); callers
must degrade assurance rather than assume zero.
"""

from __future__ import annotations

import gzip
import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.transfer.models import EndpointConfig

logger = logging.getLogger(__name__)

__all__ = [
    "PRECOUNT_KEY",
    "ARTIFACT_COUNT_KEY",
    "DEST_COUNT_SOURCE_KEY",
    "DEST_COUNT_ARTIFACT",
    "destination_row_count",
    "destination_key_hits",
    "precount_destination",
    "precount_table",
    "count_endpoint_rows",
    "count_artifact_rows",
    "stamp_artifact_census",
    "stamp_vector_census",
    "IDENTITY_COUNT_KEY",
    "VECTOR_ROWS_KEY",
    "DEST_COUNT_IDENTITY",
    "DestBeforeCensus",
]

# Dest-engine IN-list chunk. Partitioning the key set (not overlapping) so
# summed COUNT(DISTINCT) equals the full census.
_KEY_HIT_CHUNK = 400

# Key used to carry the pre-write count on the writer's destination summary.
PRECOUNT_KEY = "target_rows_before"

# Independent record COUNT of a written file/object artifact. Analogous to
# SQL COUNT(*) — never the writer's ``rows`` / ``rows_written``. Cardinality
# of the bytes on disk, not Gate-8 cell fidelity.
ARTIFACT_COUNT_KEY = "artifact_row_count"
DEST_COUNT_SOURCE_KEY = "dest_count_source"
DEST_COUNT_ARTIFACT = "artifact_readback"

# Independent COUNT(DISTINCT source_id) of a vector/RAG destination.
# Physical embedding COUNT(*) is diagnostic (``vector_rows``) — never dest
# population. Analogous to mirror active vs physical COUNT(*).
IDENTITY_COUNT_KEY = "identity_rows"
VECTOR_ROWS_KEY = "vector_rows"
DEST_COUNT_IDENTITY = "identity_readback"
_VECTOR_IDENTITY_ENGINES = frozenset({"pgvector"})

_ARTIFACT_FORMATS = frozenset({"csv", "tsv", "json", "jsonl", "parquet"})
_OBJECT_STORE_DRIVERS = frozenset({
    "s3",
    "gcs",
    "adls",
    "azure_blob_storage",
    "azure_data_lake",
    "azure_data_lake_storage",
})


def _count(conn: Any, table_ref: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        cur.close()


def _count_distinct_source_id(conn: Any, table_ref: str) -> int:
    """Identities, not embedding rows. SQL COUNT(DISTINCT) already skips NULL."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(DISTINCT source_id) FROM {table_ref}")  # nosec B608
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        cur.close()


def _pgvector_identity_count(
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """``COUNT(DISTINCT source_id)`` for a pgvector (PostgreSQL) table.

    Physical ``COUNT(*)`` of chunks is not dest population. Missing table is
    0 (create-on-first-write). A live table without ``source_id`` cannot
    prove identity — return ``None``, never fall back to embedding COUNT(*).
    The vector extension is not required: identity is a TEXT column.
    """
    from connectors.postgresql_conn import get_connection
    from connectors.sql_identifiers import quote_table_ref

    conn = get_connection(
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 5432),
        database=str(cfg.get("database") or ""),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        ssl=bool(cfg.get("ssl", False)),
    )
    try:
        sch = schema or "public"
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'"{sch}"."{table_name}"',))
            row = cur.fetchone()
            if not row or row[0] is None:
                return 0
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "AND column_name = 'source_id'",
                (sch, table_name),
            )
            if cur.fetchone() is None:
                return None
        return _count_distinct_source_id(
            conn, quote_table_ref(table_name, sch, dialect="postgresql")
        )
    finally:
        conn.close()


def destination_row_count(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """Rows already in the destination table, or ``None`` when unknowable.

    A missing table counts as ``0`` — create-on-first-write is a known empty
    destination, which is a proof, not an unknown.
    """
    table = (table_name or "").strip()
    if not table:
        return None
    try:
        from connectors.sql_identifiers import quote_table_ref

        if db_type == "sqlite":
            import sqlite3

            database = str(cfg.get("database") or "")
            if not database:
                return None
            with sqlite3.connect(database) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not exists:
                    return 0
                return _count(conn, quote_table_ref(table, dialect="sqlite"))

        if db_type in {"postgresql", "redshift"}:
            from connectors.postgresql_conn import get_connection

            conn = get_connection(
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or (5439 if db_type == "redshift" else 5432)),
                database=str(cfg.get("database") or ""),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                sch = schema or "public"
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT to_regclass(%s)", (f'"{sch}"."{table}"',)
                    )
                    row = cur.fetchone()
                    if not row or row[0] is None:
                        return 0
                return _count(conn, quote_table_ref(table, sch, dialect="postgresql"))
            finally:
                conn.close()

        if db_type == "mysql":
            from connectors.mysql_conn import get_connection

            conn = get_connection(
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or 3306),
                database=str(cfg.get("database") or ""),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = %s",
                        (table,),
                    )
                    row = cur.fetchone()
                    if not row or not int(row[0]):
                        return 0
                return _count(conn, quote_table_ref(table, dialect="mysql"))
            finally:
                conn.close()

        if db_type == "mongodb":
            from pymongo import MongoClient

            from src.transfer.adapters import mongodb_connection_string

            client: MongoClient = MongoClient(
                mongodb_connection_string(cfg), serverSelectionTimeoutMS=5000
            )
            try:
                database = str(cfg.get("database") or "")
                if not database:
                    return None
                coll = client[database][table]
                # Exact, not estimated: an approximate count cannot prove a delta.
                return int(coll.count_documents({}))
            finally:
                client.close()

        if db_type == "pgvector":
            # Identities, not embedding rows. Physical COUNT(*) is not dest.
            return _pgvector_identity_count(cfg, schema=schema, table_name=table)

        if db_type in {"iceberg", "apache_iceberg"}:
            return _iceberg_row_count(cfg, schema=schema, table_name=table)

        if db_type in _OBJECT_STORE_DRIVERS:
            return _object_store_row_count(db_type, cfg, table_name=table)
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Pre-write destination count failed: %s", exc)
        return None
    return None


def destination_key_hits(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    key_columns: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    """How many of these keys dest already holds — dest-engine, not writer ack.

    Upsert/CDC ``records_processed`` counts updates as writes. ``COUNT(*)``
    does not move. The independent split is: keys in this batch that already
    exist on dest (updates) versus keys that do not (inserts). ``None`` means
    the probe could not run; callers must leave keyed conservation unproven.
    """
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    table = (table_name or "").strip()
    if not table or not cols:
        return None
    unique: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for raw in keys or []:
        tup = tuple(raw)
        if len(tup) != len(cols) or any(v is None for v in tup):
            continue
        if tup in seen:
            continue
        seen.add(tup)
        unique.append(tup)
    if not unique:
        return 0
    # Missing / empty dest: no hits, and IN against a missing table would error.
    n = destination_row_count(db_type, cfg, schema=schema, table_name=table)
    if n is None:
        return None
    if n == 0:
        return 0
    if db_type in {"iceberg", "apache_iceberg"}:
        try:
            return _iceberg_key_hits(
                cfg, schema=schema, table_name=table, cols=cols, keys=unique
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("Iceberg dest key census failed: %s", exc)
            return None
    try:
        return _key_hits_sql(db_type, cfg, schema=schema, table_name=table, cols=cols, keys=unique)
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Pre-write destination key census failed: %s", exc)
        return None


def _key_hits_sql(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref

    dialect = "mysql" if db_type == "mysql" else db_type
    qchar = "`" if dialect == "mysql" else '"'
    table_ref = quote_table_ref(
        table_name,
        schema if dialect == "postgresql" else None,
        dialect="postgresql" if dialect == "postgresql" else dialect,
    )
    col_sql = ", ".join(quote_sql_identifier(c, qchar) for c in cols)
    ph = "%s" if dialect in {"postgresql", "mysql"} else "?"
    total = 0
    if dialect == "sqlite":
        import sqlite3

        database = str(cfg.get("database") or "")
        if not database:
            return None
        with sqlite3.connect(database) as conn:
            total = _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        return total
    if dialect in {"postgresql", "redshift"}:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or (5439 if db_type == "redshift" else 5432)),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            return _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        finally:
            conn.close()
    if dialect == "mysql":
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 3306),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            return _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        finally:
            conn.close()
    return None


def _sum_distinct_hits(
    conn: Any,
    table_ref: str,
    col_sql: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
    ph: str,
) -> int:
    total = 0
    width = len(cols)
    for i in range(0, len(keys), _KEY_HIT_CHUNK):
        chunk = keys[i : i + _KEY_HIT_CHUNK]
        if width == 1:
            in_sql = ", ".join(ph for _ in chunk)
            sql = (
                f"SELECT COUNT(DISTINCT {col_sql}) FROM {table_ref} "  # nosec B608
                f"WHERE {col_sql} IN ({in_sql})"
            )
            params: tuple[Any, ...] = tuple(row[0] for row in chunk)
        else:
            row_ph = "(" + ", ".join(ph for _ in cols) + ")"
            in_sql = ", ".join(row_ph for _ in chunk)
            sql = (
                f"SELECT COUNT(*) FROM ("  # nosec B608
                f"SELECT DISTINCT {col_sql} FROM {table_ref} "
                f"WHERE ({col_sql}) IN ({in_sql})"
                f") _df_key_hits"
            )
            params = tuple(v for row in chunk for v in row)
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            total += int(row[0]) if row and row[0] is not None else 0
        finally:
            cur.close()
    return total


def _iceberg_endpoint(cfg: dict[str, Any], table_name: str, schema: str) -> dict[str, Any]:
    return {
        **dict(cfg),
        "table": table_name,
        "table_name": table_name,
        "schema": schema or cfg.get("schema") or "",
    }


def _iceberg_missing_table(exc: BaseException) -> bool:
    name = type(exc).__name__
    text = str(exc).lower()
    return "NoSuchTable" in name or "not found" in text or "does not exist" in text


def _iceberg_filesystem_count(cfg: dict[str, Any], table_name: str, schema: str) -> int | None:
    """Independent snapshot COUNT from current data files — not metadata record-count."""
    from connectors.iceberg_writer import (
        _load_existing_rows,
        _load_metadata,
        _resolve_iceberg_table_dir,
    )

    table_dir = _resolve_iceberg_table_dir(cfg, table_name, schema or None)
    meta_dir = table_dir / "metadata"
    if not meta_dir.is_dir():
        return 0
    versions = sorted(meta_dir.glob("v*.metadata.json"))
    if not versions:
        return 0
    current_meta = _load_metadata(versions[-1])
    if not current_meta:
        return 0
    schema_json = (current_meta.get("schemas") or [{}])[-1] or current_meta.get("schema") or {}
    columns = [str(f.get("name")) for f in (schema_json.get("fields") or []) if f.get("name")]
    rows = _load_existing_rows(table_dir, columns or ["_"], current_meta)
    return len(rows)


def _iceberg_catalog_count(cfg: dict[str, Any], table_name: str, schema: str) -> int | None:
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

    endpoint = _iceberg_endpoint(cfg, table_name, schema)
    parsed = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = parsed["namespace"] + (parsed["table_name"],)
    try:
        tbl = catalog.load_table(identifier)
    except Exception as exc:
        if _iceberg_missing_table(exc):
            return 0
        raise
    return int(tbl.scan().count())


def _iceberg_row_count(
    cfg: dict[str, Any], *, schema: str, table_name: str
) -> int | None:
    from connectors.iceberg_writer import resolve_iceberg_write_path

    endpoint = _iceberg_endpoint(cfg, table_name, schema)
    try:
        path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        logger.info("Iceberg dest COUNT unavailable: %s", exc)
        return None
    if path == "catalog":
        return _iceberg_catalog_count(cfg, table_name, schema)
    return _iceberg_filesystem_count(cfg, table_name, schema)


def _iceberg_snapshot_rows(
    cfg: dict[str, Any], *, schema: str, table_name: str, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    from connectors.iceberg_writer import resolve_iceberg_write_path

    endpoint = _iceberg_endpoint(cfg, table_name, schema)
    try:
        path = resolve_iceberg_write_path(endpoint)
    except RuntimeError:
        return None
    if path == "catalog":
        from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

        parsed = parse_iceberg_catalog_config(endpoint)
        catalog = load_catalog(endpoint)
        identifier = parsed["namespace"] + (parsed["table_name"],)
        try:
            tbl = catalog.load_table(identifier)
        except Exception as exc:
            if _iceberg_missing_table(exc):
                return []
            raise
        wanted = [str(c) for c in cols if str(c).strip()]
        arrow = tbl.scan().select(wanted).to_arrow() if wanted else tbl.scan().to_arrow()
        return list(arrow.to_pylist())
    from connectors.iceberg_writer import (
        _load_existing_rows,
        _load_metadata,
        _resolve_iceberg_table_dir,
    )

    table_dir = _resolve_iceberg_table_dir(cfg, table_name, schema or None)
    meta_dir = table_dir / "metadata"
    if not meta_dir.is_dir():
        return []
    versions = sorted(meta_dir.glob("v*.metadata.json"))
    if not versions:
        return []
    current_meta = _load_metadata(versions[-1])
    if not current_meta:
        return []
    schema_json = (current_meta.get("schemas") or [{}])[-1] or current_meta.get("schema") or {}
    columns = [str(f.get("name")) for f in (schema_json.get("fields") or []) if f.get("name")]
    load_cols = list(dict.fromkeys([*columns, *[str(c) for c in cols if str(c).strip()]]))
    return _load_existing_rows(table_dir, load_cols or list(cols), current_meta)


def _norm_dest_key(values: Sequence[Any]) -> tuple[str, ...] | None:
    """Comparable dest key — JSONL strings and catalog ints must hit the same PK."""
    out: list[str] = []
    for value in values:
        if value is None or value == "":
            return None
        out.append(str(value))
    return tuple(out)


def _iceberg_key_hits(
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    rows = _iceberg_snapshot_rows(cfg, schema=schema, table_name=table_name, cols=cols)
    if rows is None:
        return None
    wanted = {norm for key in keys if (norm := _norm_dest_key(key)) is not None}
    seen: set[tuple[str, ...]] = set()
    for row in rows:
        tup = _norm_dest_key(row.get(c) for c in cols)
        if tup is not None and tup in wanted:
            seen.add(tup)
    return len(seen)


def _object_store_kind(db_type: str) -> str:
    if db_type in {"adls", "azure_blob_storage", "azure_data_lake", "azure_data_lake_storage"}:
        return "adls"
    return db_type


def _object_store_list_and_get(
    kind: str, cfg: dict[str, Any], bucket: str, key: str
) -> list[tuple[str, bytes]] | None:
    """Payloads for dest COUNT. ``[]`` is missing (measured zero). ``None`` is unknowable."""
    from connectors.object_store_common import (
        normalize_object_base_key,
        object_parts_prefix,
        object_store_read_keys,
    )

    if kind == "s3":
        from connectors.s3_reader import list_objects
    elif kind == "gcs":
        from connectors.gcs_reader import list_objects
    elif kind == "adls":
        from connectors.adls_reader import list_objects
    else:
        return None

    base = normalize_object_base_key(key)
    parts_prefix = object_parts_prefix(base)
    try:
        listed = list_objects(cfg, bucket, parts_prefix) if parts_prefix else []
    except Exception as exc:
        logger.info("object-store list failed for dest COUNT: %s", exc)
        return None
    read_keys = object_store_read_keys(base, listed)
    payloads: list[tuple[str, bytes]] = []
    for obj_key in read_keys:
        body = _object_store_get_bytes(kind, cfg, bucket, obj_key)
        if body is False:
            continue
        if body is None:
            return None
        payloads.append((obj_key, body))
    return payloads


def _object_store_get_bytes(
    kind: str, cfg: dict[str, Any], bucket: str, key: str
) -> bytes | None | bool:
    """bytes on hit, False if missing, None if unknowable."""
    try:
        if kind == "s3":
            from botocore.exceptions import ClientError
            from connectors.aws_common import boto3_client

            try:
                return boto3_client("s3", cfg).get_object(Bucket=bucket, Key=key)["Body"].read()
            except ClientError as exc:
                code = str((exc.response or {}).get("Error", {}).get("Code") or "")
                http = str((exc.response or {}).get("ResponseMetadata", {}).get("HTTPStatusCode") or "")
                if code in {"404", "NoSuchKey", "NotFound"} or http == "404":
                    return False
                raise
        if kind == "gcs":
            from connectors.gcs_common import gcs_client

            blob = gcs_client(cfg).bucket(bucket).get_blob(key)
            if blob is None:
                return False
            return blob.download_as_bytes()
        if kind == "adls":
            from connectors.adls_common import blob_service_client

            try:
                return (
                    blob_service_client(cfg)
                    .get_blob_client(bucket, key)
                    .download_blob()
                    .readall()
                )
            except Exception as exc:
                name = type(exc).__name__
                if "NotFound" in name or "404" in str(exc):
                    return False
                raise
    except Exception as exc:
        logger.info("object-store GET failed for dest COUNT (%s/%s): %s", bucket, key, exc)
        return None
    return None


def _object_store_row_count(
    db_type: str, cfg: dict[str, Any], *, table_name: str
) -> int | None:
    bucket = str(cfg.get("database") or "").strip()
    key = str(table_name or "").strip()
    if not bucket or not key:
        return None
    kind = _object_store_kind(db_type)
    payloads = _object_store_list_and_get(kind, cfg, bucket, key)
    if payloads is None:
        return None
    if not payloads:
        return 0
    from services.reconciliation import _rows_from_object_bytes

    total = 0
    for obj_key, body in payloads:
        rows, _headers = _rows_from_object_bytes(body, obj_key)
        total += len(rows)
    return total


def precount_table(db_type: str, cfg: dict[str, Any], table_name: str) -> int | None:
    """Pre-write count for a table the streaming writers already resolved.

    Streaming paths compute the driver type and destination table themselves and
    call the batch writer directly, so they pass those in rather than
    re-resolving from the endpoint.
    """
    from services.dialect_profiles import schema_from_cfg

    return destination_row_count(
        db_type, cfg, schema=schema_from_cfg(db_type, cfg), table_name=table_name
    )


def precount_destination(
    endpoint: EndpointConfig, cfg: dict[str, Any]
) -> int | None:
    """Pre-write count for a resolved destination endpoint.

    Resolves the driver, schema and table exactly the way the writer will, so
    the delta is measured against the object the rows actually land in.
    """
    from src.transfer.adapters import resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type

    db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or ""))
    return precount_table(
        db_type, cfg, resolve_dest_table(db_type, endpoint, "dt_import")
    )


def count_endpoint_rows(
    endpoint: EndpointConfig | None,
    *,
    table_name: str | None = None,
) -> int | None:
    """Independent engine COUNT(*) of the object this endpoint currently names.

    Multi-stream jobs remap ``endpoint.table`` per stream. Count while that
    bind is still in place, or pass ``table_name`` after the bind is restored.
    ``None`` means unknowable — never substitute writer acknowledgement.
    """
    if endpoint is None:
        return None
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type

    try:
        cfg = resolve_connector_config(endpoint)
        db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or ""))
        name = (table_name or "").strip() or resolve_dest_table(
            db_type, endpoint, "dt_import"
        )
        return precount_table(db_type, cfg, name)
    except Exception as exc:
        logger.warning("Endpoint COUNT(*) failed: %s", exc)
        return None


class DestBeforeCensus:
    """Dest COUNT(*) taken once per named object, before that object is written.

    Append delta and keyed/CDC ``dest_delta`` both require this number.
    A second capture of the same name must not re-query: that would observe
    dest-after and close a false identity. ``None`` stored for a name means
    the probe ran and was unknowable — do not retry after writes have begun.
    """

    def __init__(self) -> None:
        self._before: dict[str, int | None] = {}

    def capture(
        self,
        endpoint: Any,
        *,
        table_name: str,
        aliases: Sequence[str] = (),
    ) -> int | None:
        names: list[str] = []
        for raw in (table_name, *aliases):
            name = str(raw or "").strip()
            if name and name not in names:
                names.append(name)
        if not names:
            return None
        for name in names:
            if name in self._before:
                value = self._before[name]
                for other in names:
                    self._before.setdefault(other, value)
                return value
        value = count_endpoint_rows(endpoint, table_name=names[0])
        for name in names:
            self._before[name] = value
        return value

    def get(self, table_name: str) -> int | None:
        return self._before.get(str(table_name or "").strip())

    def stamp(self, summary: dict[str, Any], table_name: str) -> dict[str, Any]:
        """Copy dest-before onto a stream summary. Never dest-after."""
        key = str(table_name or "").strip()
        if key not in self._before:
            return summary
        value = self._before[key]
        if value is None:
            return summary
        summary[PRECOUNT_KEY] = int(value)
        recon = dict(summary.get("reconciliation") or {})
        recon[PRECOUNT_KEY] = int(value)
        summary["reconciliation"] = recon
        return summary


def _infer_artifact_format(path: Path, fmt: str | None) -> str:
    explicit = str(fmt or "").strip().lower()
    aliases = {"ndjson": "jsonl", "xlsx": "excel", "xls": "excel"}
    if explicit in aliases:
        explicit = aliases[explicit]
    if explicit in _ARTIFACT_FORMATS:
        return explicit
    name = path.name.lower()
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    if name.endswith(".csv"):
        return "csv"
    if name.endswith(".tsv"):
        return "tsv"
    if name.endswith(".jsonl") or name.endswith(".ndjson"):
        return "jsonl"
    if name.endswith(".json"):
        return "json"
    if name.endswith(".parquet"):
        return "parquet"
    return ""


def _read_artifact_bytes(path: Path) -> bytes | None:
    try:
        if path.name.lower().endswith(".gz"):
            with gzip.open(path, "rb") as handle:
                return handle.read()
        return path.read_bytes()
    except OSError as exc:
        logger.info("artifact bytes unreadable at %s: %s", path, exc)
        return None


def _count_jsonl_bytes(content: bytes) -> int | None:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None
    count = 0
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None
        if not isinstance(obj, dict):
            return None
        count += 1
    return count


def _count_json_bytes(content: bytes) -> int | None:
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    if isinstance(payload, list):
        return len(payload)
    return None


def _count_parquet_path(path: Path) -> int | None:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    try:
        metadata = pq.ParquetFile(str(path)).metadata
        if metadata is None:
            return None
        return int(metadata.num_rows)
    except Exception as exc:
        logger.info("parquet artifact count unavailable at %s: %s", path, exc)
        return None


def count_artifact_rows(
    path: str | Path | None,
    *,
    fmt: str | None = None,
) -> int | None:
    """Independent record COUNT of a written file, or ``None`` if unknowable.

    Dest-engine analogue of ``destination_row_count`` for file/object exports.
    Re-opens the bytes on disk. Never returns the writer's ``rows_written``.
    Missing path, remote URI without a local file, unsupported format, or
    unparseable content stay ``None`` — conservation remains unmeasured.
    Empty but well-formed artifacts are measured zero.
    """
    raw = str(path or "").strip()
    if not raw:
        return None
    artifact = Path(raw)
    if not artifact.is_file():
        return None
    kind = _infer_artifact_format(artifact, fmt)
    if kind not in _ARTIFACT_FORMATS:
        return None
    if kind == "parquet":
        return _count_parquet_path(artifact)
    content = _read_artifact_bytes(artifact)
    if content is None:
        return None
    try:
        if kind in {"csv", "tsv"}:
            from services.csv_profiler import count_csv_rows

            return int(count_csv_rows(content))
        if kind == "jsonl":
            return _count_jsonl_bytes(content)
        if kind == "json":
            return _count_json_bytes(content)
    except Exception as exc:
        logger.info("artifact count failed for %s (%s): %s", artifact, kind, exc)
        return None
    return None


def stamp_artifact_census(
    recon: Mapping[str, Any],
    dest_summary: Mapping[str, Any] | None,
    *,
    fmt: str | None = None,
) -> dict[str, Any]:
    """Stamp independent artifact COUNT onto Gate-8. Never writer ack.

    File replace is dest-before 0: the engine opens the artifact ``wb``.
    Cell-fidelity flags (``skipped_readback`` / ``unproven``) stay with the
    caller — this only owns dest cardinality.
    """
    out = dict(recon)
    data = dict(dest_summary or {})
    path = data.get("path") or data.get("export_path")
    resolved_fmt = fmt or data.get("format")
    counted = count_artifact_rows(
        path if isinstance(path, str) else None,
        fmt=str(resolved_fmt or "") or None,
    )
    if counted is None:
        # Writer ``target_rows`` must not survive as dest COUNT.
        out["target_rows"] = None
        return out
    out[ARTIFACT_COUNT_KEY] = counted
    out[DEST_COUNT_SOURCE_KEY] = DEST_COUNT_ARTIFACT
    out["target_rows"] = counted
    out[PRECOUNT_KEY] = 0
    return out


def stamp_vector_census(
    recon: Mapping[str, Any],
    dest_cfg: Mapping[str, Any] | None,
    *,
    schema: str,
    table_name: str,
    dest_engine: str,
) -> dict[str, Any]:
    """Stamp COUNT(DISTINCT source_id). Never physical vector COUNT(*) or writer ack.

    Gate-8 ``target_rows`` on pgvector is embedding cardinality (chunks).
    That figure must not survive as dest population — 2 documents / 5
    chunks would close overwrite as a surplus. Cell fidelity of opaque
    embeddings stays with the caller (``skipped_readback`` /
    ``migration_proven=false``). This only owns identity cardinality.

    Engines without dest-engine DISTINCT ``source_id`` (Milvus, Qdrant,
    Pinecone, Weaviate) are left untouched — their ``rowCount`` is not
    identity.
    """
    out = dict(recon)
    engine = str(dest_engine or "").strip().lower()
    if engine not in _VECTOR_IDENTITY_ENGINES:
        return out
    identity = destination_row_count(
        engine,
        dict(dest_cfg or {}),
        schema=str(schema or ""),
        table_name=str(table_name or ""),
    )
    if identity is None:
        out[DEST_COUNT_SOURCE_KEY] = "skipped_identity_readback"
        return out
    out[IDENTITY_COUNT_KEY] = int(identity)
    out[DEST_COUNT_SOURCE_KEY] = DEST_COUNT_IDENTITY
    physical = out.get("target_rows")
    if isinstance(physical, int) and physical >= 0:
        out[VECTOR_ROWS_KEY] = physical
    return out
