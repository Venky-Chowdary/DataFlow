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
cardinality, not Gate-8 cell fidelity. Excel dest population is
value-bearing rows (``excel_parser.count_excel_rows``), never
openpyxl ``max_row`` / used-range. Avro is a streamed record COUNT, not
``parse_avro``'s ingest cap. ORC/Parquet footer ``nrows`` is dest-engine
cardinality of the file we wrote, not a warehouse catalog estimate.
XML dest population is the unique repeating record-path
(``count_xml_records``), never ``parse_xml`` ingest ``max_rows`` and
never a whole-document-as-one fallback. Ambiguous sibling collections
stay unmeasured. Empty ``<records/>`` is 0. Missing parser is
unmeasured, not dest=0.

Lakehouse and object-store destinations already have dest-*after* read-back
(Iceberg scan, S3/GCS/ADLS GET). Dest-*before* must use the same COUNT so
append delta and first-write overwrite (missing table/object = 0) can close.
Writer ``Table.upsert`` / PUT rowcount is not that proof. Object-store dest
COUNT is the same artifact machine as local file export (Excel value rows,
streamed Avro, Parquet/ORC footer, XML unique record-path). A JSON-parse fallback that yields ``[]``
is dest=0 — that would close overwrite on Parquet/Excel bytes. Unparseable
or truncated part listings stay unmeasured; never sum a prefix. Catalog
SKUs (``amazon_s3``) alias onto ``s3`` the same way Azure SQL aliases onto
``sqlserver``.

Oracle and SQL Server (Azure SQL, RDS, Autonomous SKUs) answer dest COUNT
and leftover MERGE listing with dest-engine ``COUNT(*)`` / ``SELECT pk``.
``sys.partitions`` / ``sys.dm_db_partition_stats.row_count`` and Oracle
``ALL_TABLES.num_rows`` are estimates or stale optimizer stats — they never
close conservation. Missing table is 0. Catalog aliases quote as
``sqlserver`` / ``oracle`` (brackets vs folded double-quotes). Snowflake /
BigQuery / DuckDB / Databricks use the same dest-engine ``COUNT(*)`` —
never ``INFORMATION_SCHEMA`` / ``__TABLES__.row_count`` / clustering
stats. Composite
key hits use portable AND/OR equality, not row-value ``IN`` (Oracle 19c
has no tuple IN). Incremental leftover MERGE stays a hard no-op.

Vector destinations are a 1-source-row → N-chunk identity.
Physical ``COUNT(*)`` of embedding rows is **not** dest population — that
is the Fivetran ``_deleted`` analogue for RAG: 2 documents → 5 chunks
looks like silent duplication if chunk COUNT closes overwrite. Dest
population is ``COUNT(DISTINCT source_id)`` from the dest engine
(pgvector SQL, Milvus entity query, Qdrant point scroll). Missing
collection is 0. Writer chunk-upsert ack and collection ``rowCount`` /
``num_entities`` / ``points_count`` never close. A truncated scan
(REST offset cap, census bound) is unmeasured — never DISTINCT of a
prefix. Pinecone list+fetch and Weaviate object listing use the same
state machine as Milvus/Qdrant; their ``vectorCount`` / Aggregate
``meta.count`` is physical chunks, not identity. Writer chunk-upsert
ack never closes.

A complete source PK census plus dest-engine key hits splits DMS
``MISSING_TARGET`` from ``EXTRA_TARGET``. ``COUNT(*)`` nets one missing
source key and one leftover dest key to a false balance. Incremental
CDC must not run that split — the batch is not the source key set, and
leftover would look like almost every dest row. This module **lists**
dest keys so a complete overwrite snapshot can MERGE-delete ``D \\ S``;
it never deletes them itself. Iceberg listing and dest COUNT are one
current-snapshot population (filesystem data files or catalog
``scan().to_arrow()``), never metadata ``record-count`` /
``scan().count()``. ``row_conservation.apply_inferred_leftover_deletes``
applies the anti-join only when the source census is complete overwrite
(SQL and Iceberg CoW). Incremental CDC must not call that apply. Mirror
already applies inferred soft-deletes on full re-sync. Iceberg MoR /
deletion vectors stay Planned — apply them in the snapshot population
once; the identity is still ``leftover = D \\ S``.

SCD Type 2 is the same 1-source-identity → N-history-row shape as
vector chunks. Physical ``COUNT(*)`` of versions grows on every
attribute change. Dest population is ``COUNT(*) WHERE is_current``.
Missing table is 0. A live table without ``is_current`` cannot prove
current identities — return ``None``, never fall back to history
``COUNT(*)``. Writer version-upsert ack and Gate-8 stuffed
``active_rows`` never close. Incremental watermarked SCD2 must not
treat a change batch as the current population.

Oracle and SQL Server SCD2 current COUNT reuse the leftover-MERGE
warehouse session (dest-engine ``COUNT(*)``, never partition stats).
``is_current`` is BIT / ``NUMBER(1)`` — the predicate is ``= 1``, not
``IS TRUE`` (T-SQL has no ``IS TRUE``; Oracle 19c has no BOOLEAN).
Catalog SKUs alias onto ``sqlserver`` / ``oracle`` quoting. A missing
``is_current`` column is unmeasured, never current=0. Snowflake /
BigQuery SCD2 COUNT stay unmeasured.

``None`` means the count is unavailable (unsupported engine, missing table,
unreachable destination, or an unreadable/unsupported artifact); callers
must degrade assurance rather than assume zero.
"""

from __future__ import annotations

import gzip
import json
import logging
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
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
    "stamp_keyset_census",
    "stamp_scd2_census",
    "VECTOR_IDENTITY_ENGINES",
    "identity_count_from_source_id_scan",
    "SOURCE_ID_SCAN_MISSING",
    "SOURCE_ID_SCAN_NO_FIELD",
    "SOURCE_ID_SCAN_TRUNCATED",
    "SOURCE_ID_SCAN_COMPLETE",
    "SOURCE_ID_SCAN_UNMEASURED",
    "count_scd2_current",
    "count_scd2_populations",
    "destination_keyset_census",
    "destination_key_list",
    "records_to_key_tuples",
    "IDENTITY_COUNT_KEY",
    "VECTOR_ROWS_KEY",
    "DEST_COUNT_IDENTITY",
    "CURRENT_ROWS_KEY",
    "HISTORY_ROWS_KEY",
    "DEST_COUNT_CURRENT",
    "MISSING_KEYS_KEY",
    "EXTRA_KEYS_KEY",
    "LEFTOVER_DELETED_KEY",
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
# Dest engines that can answer COUNT(DISTINCT source_id) independently of
# writer upsert ack. Physical collection ``rowCount`` / ``vectorCount`` /
# Aggregate ``meta.count`` is never identity.
VECTOR_IDENTITY_ENGINES = frozenset(
    {"pgvector", "milvus", "qdrant", "pinecone", "weaviate"}
)
_VECTOR_IDENTITY_ENGINES = VECTOR_IDENTITY_ENGINES

SOURCE_ID_SCAN_MISSING = "missing"
SOURCE_ID_SCAN_NO_FIELD = "no_field"
SOURCE_ID_SCAN_TRUNCATED = "truncated"
SOURCE_ID_SCAN_COMPLETE = "complete"
SOURCE_ID_SCAN_UNMEASURED = "unmeasured"

# Independent COUNT(*) WHERE is_current of an SCD2 destination.
# Physical history COUNT(*) is diagnostic (``history_rows``) — never dest
# population. Analogous to vector identity vs chunk COUNT(*). SCD2
# ``is_current`` is temporal current-version, not a tombstone (``_deleted``).
CURRENT_ROWS_KEY = "current_rows"
HISTORY_ROWS_KEY = "history_rows"
DEST_COUNT_CURRENT = "current_readback"

# Dest-engine keyset census (DMS MISSING_TARGET / EXTRA_TARGET).
# COUNT(*) nets one missing source key and one leftover dest key to a false
# balance. ``destination_key_hits`` of a *complete* source PK set splits
# them: missing = |S| − |D ∩ S|, extra = |D| − |D ∩ S|. Incremental CDC
# must not run this — a batch is not S, and leftover would be almost every
# dest row (false EXTRA_TARGET / false inferred delete).
MISSING_KEYS_KEY = "missing_keys"
EXTRA_KEYS_KEY = "extra_keys"
DEST_KEY_HITS_KEY = "dest_key_hits"
SOURCE_KEY_COUNT_KEY = "source_key_count"
LEFTOVER_DELETED_KEY = "leftover_deleted"
_KEYSET_CENSUS_MAX = 20_000
# Same bound as dest key listing: a prefix DISTINCT is a lie.
_IDENTITY_SCAN_MAX = _KEYSET_CENSUS_MAX

_ARTIFACT_FORMATS = frozenset({
    "csv", "tsv", "json", "jsonl", "parquet", "excel", "avro", "orc", "xml",
})


def _count(conn: Any, table_ref: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        cur.close()


def _count_where(conn: Any, table_ref: str, where_sql: str) -> int:
    """Population under an already-quoted predicate. Identifier-only WHERE."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_ref} WHERE {where_sql}")  # nosec B608
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


def identity_count_from_source_id_scan(
    state: str,
    values: Sequence[Any] | None,
) -> int | None:
    """COUNT(DISTINCT source_id) over a dest-engine scan.

    Missing collection is 0 (create-on-first-write). Incomplete scans
    (REST offset cap, census bound, missing ``source_id`` field, transport
    failure) are unmeasured — never DISTINCT of a prefix, never physical
    ``rowCount``. Empty / NULL ``source_id`` is not an identity (SQL
    COUNT DISTINCT skips NULL).
    """
    kind = str(state or "").strip().lower()
    if kind == SOURCE_ID_SCAN_MISSING:
        return 0
    if kind != SOURCE_ID_SCAN_COMPLETE:
        return None
    seen: set[str] = set()
    for raw in values or ():
        if raw is None:
            continue
        s = str(raw).strip()
        if s:
            seen.add(s)
    return len(seen)


def _vector_rest_identity_count(
    db_type: str,
    cfg: dict[str, Any],
    *,
    table_name: str,
) -> int | None:
    """Dest-engine DISTINCT source_id. Never collection rowCount / vectorCount."""
    engine = str(db_type or "").strip().lower()
    if engine == "milvus":
        from connectors.milvus_writer import scan_source_ids
    elif engine == "qdrant":
        from connectors.qdrant_writer import scan_source_ids
    elif engine == "pinecone":
        from connectors.pinecone_writer import scan_source_ids
    elif engine == "weaviate":
        from connectors.weaviate_writer import scan_source_ids
    else:
        return None
    state, values = scan_source_ids(
        cfg, table_name=table_name, max_entities=_IDENTITY_SCAN_MAX
    )
    return identity_count_from_source_id_scan(state, values)


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

        if db_type in {"milvus", "qdrant", "pinecone", "weaviate"}:
            return _vector_rest_identity_count(db_type, cfg, table_name=table)

        if db_type in {"iceberg", "apache_iceberg"}:
            return _iceberg_row_count(cfg, schema=schema, table_name=table)

        if _object_store_kind(db_type) in {"s3", "gcs", "adls"}:
            return _object_store_row_count(db_type, cfg, table_name=table)

        from services.dialect_profiles import warehouse_sql_quote_dialect

        dialect = warehouse_sql_quote_dialect(db_type)
        if dialect:
            return _warehouse_sql_row_count(
                db_type, cfg, schema=schema, table_name=table, dialect=dialect
            )
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
        from services.dialect_profiles import warehouse_sql_quote_dialect

        dialect = warehouse_sql_quote_dialect(db_type)
        if dialect:
            return _warehouse_sql_key_hits(
                db_type,
                cfg,
                schema=schema,
                table_name=table,
                cols=cols,
                keys=unique,
                dialect=dialect,
            )
        return _key_hits_sql(db_type, cfg, schema=schema, table_name=table, cols=cols, keys=unique)
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Pre-write destination key census failed: %s", exc)
        return None


def _unique_key_tuples(
    keys: Sequence[tuple[Any, ...]] | Sequence[Sequence[Any]],
    width: int,
) -> list[tuple[Any, ...]]:
    unique: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for raw in keys or []:
        tup = tuple(raw)
        if len(tup) != width or any(v is None for v in tup):
            continue
        if tup in seen:
            continue
        seen.add(tup)
        unique.append(tup)
    return unique


def records_to_key_tuples(
    records: Sequence[Mapping[str, Any]] | None,
    key_columns: Sequence[str],
    mappings: Sequence[Mapping[str, Any]] | None = None,
) -> list[tuple[Any, ...]] | None:
    """Complete 1-row-1-key snapshot, or ``None``.

    Duplicate PKs or a missing PK cell mean the keyset identity is not
    defined against COUNT(*) — leave EXTRA_TARGET unmeasured rather than
    invent a split. Incremental samples must not call this and then treat
    the result as S.
    """
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    if not cols or not records:
        return None
    source_fields: list[str] = []
    for target in cols:
        field = target
        want = target.lower()
        for mapping in mappings or []:
            if str(mapping.get("target") or "").lower() == want and mapping.get("source"):
                field = str(mapping["source"])
                break
        source_fields.append(field)
    tuples: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for rec in records:
        if not isinstance(rec, Mapping):
            return None
        row: list[Any] = []
        for field, target in zip(source_fields, cols):
            raw = rec.get(field)
            if raw is None and field != target:
                raw = rec.get(target)
            if raw is None or raw == "":
                return None
            row.append(raw)
        tup = tuple(row)
        if tup in seen:
            return None
        seen.add(tup)
        tuples.append(tup)
    if len(tuples) != len(records):
        return None
    return tuples


def destination_keyset_census(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    key_columns: list[str],
    keys: Sequence[tuple[Any, ...]] | Sequence[Sequence[Any]],
) -> dict[str, int] | None:
    """Split dest COUNT(*) into MISSING_TARGET vs EXTRA_TARGET.

    Requires a *complete* unique source PK set ``S``. Incremental CDC
    batches are not ``S`` — callers must not pass them.

        missing = |S| − |D ∩ S|
        extra   = |D| − |D ∩ S|

    ``extra`` is leftover dest keys (DMS EXTRA_TARGET). This function
    never deletes them. Dest without unique keys (hits > dest COUNT) is
    unmeasured, not a guessed leftover. Census larger than
    ``_KEYSET_CENSUS_MAX`` stays unmeasured rather than running an
    unbounded IN-list.
    """
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    table = (table_name or "").strip()
    if not table or not cols:
        return None
    unique = _unique_key_tuples(keys, len(cols))
    if not unique or len(unique) != len(keys):
        return None
    if len(unique) > _KEYSET_CENSUS_MAX:
        logger.info(
            "Keyset census skipped: %s unique keys exceeds %s",
            len(unique),
            _KEYSET_CENSUS_MAX,
        )
        return None
    dest_n = destination_row_count(db_type, cfg, schema=schema, table_name=table)
    hits = destination_key_hits(
        db_type,
        cfg,
        schema=schema,
        table_name=table,
        key_columns=cols,
        keys=list(unique),
    )
    if dest_n is None or hits is None:
        return None
    if hits > dest_n:
        return None
    missing = len(unique) - hits
    extra = dest_n - hits
    if missing < 0:
        return None
    return {
        SOURCE_KEY_COUNT_KEY: len(unique),
        "dest_count": dest_n,
        DEST_KEY_HITS_KEY: hits,
        MISSING_KEYS_KEY: missing,
        EXTRA_KEYS_KEY: extra,
    }


def stamp_keyset_census(
    recon: Mapping[str, Any],
    dest_cfg: Mapping[str, Any] | None,
    *,
    schema: str,
    table_name: str,
    dest_engine: str,
    key_columns: Sequence[str],
    keys: Sequence[tuple[Any, ...]] | Sequence[Sequence[Any]] | None,
) -> dict[str, Any]:
    """Stamp dest-engine missing/extra keys. Never infer-delete. Never writer ack.

    Incremental CDC must not call this with a batch key set. Vector
    destinations own identity COUNT(DISTINCT source_id), not this PK split.
    """
    out = dict(recon)
    engine = str(dest_engine or "").strip().lower()
    if engine in _VECTOR_IDENTITY_ENGINES:
        return out
    if not keys:
        return out
    census = destination_keyset_census(
        engine,
        dict(dest_cfg or {}),
        schema=str(schema or ""),
        table_name=str(table_name or ""),
        key_columns=[str(c).strip() for c in key_columns if str(c).strip()],
        keys=keys,
    )
    if census is None:
        return out
    out[MISSING_KEYS_KEY] = census[MISSING_KEYS_KEY]
    out[EXTRA_KEYS_KEY] = census[EXTRA_KEYS_KEY]
    out[DEST_KEY_HITS_KEY] = census[DEST_KEY_HITS_KEY]
    out[SOURCE_KEY_COUNT_KEY] = census[SOURCE_KEY_COUNT_KEY]
    return out


def destination_key_list(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    key_columns: Sequence[str],
) -> list[tuple[Any, ...]] | None:
    """Dest-engine PK tuples, bounded. ``None`` when listing is unmeasurable.

    Complete-snapshot MERGE needs the dest key *values* not in ``S``, not
    only ``|D| − |D ∩ S|``. Dest COUNT larger than ``_KEYSET_CENSUS_MAX``
    stays unlisted rather than scanning an unbounded table. Duplicate dest
    PKs (len(unique) != COUNT(*)) refuse — inferred delete would guess.
    Missing table is ``[]``. Iceberg lists the current snapshot, never
    writer ``Table.upsert`` rowcount. This function never deletes.
    """
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    table = (table_name or "").strip()
    if not table or not cols:
        return None
    dest_n = destination_row_count(db_type, cfg, schema=schema, table_name=table)
    if dest_n is None:
        return None
    if dest_n == 0:
        return []
    if dest_n > _KEYSET_CENSUS_MAX:
        logger.info(
            "Dest key list skipped: dest COUNT(*) %s exceeds %s",
            dest_n,
            _KEYSET_CENSUS_MAX,
        )
        return None
    try:
        if db_type in {"iceberg", "apache_iceberg"}:
            rows = _iceberg_key_list(
                cfg, schema=schema, table_name=table, cols=cols
            )
        else:
            from services.dialect_profiles import warehouse_sql_quote_dialect

            dialect = warehouse_sql_quote_dialect(db_type)
            if dialect:
                rows = _warehouse_sql_key_list(
                    db_type,
                    cfg,
                    schema=schema,
                    table_name=table,
                    cols=cols,
                    dialect=dialect,
                )
            else:
                rows = _key_list_sql(
                    db_type, cfg, schema=schema, table_name=table, cols=cols
                )
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Dest key list failed: %s", exc)
        return None
    if rows is None:
        return None
    unique = _unique_key_tuples(rows, len(cols))
    if len(unique) != dest_n or len(unique) != len(rows):
        return None
    return unique


def _warehouse_sql_table_ref(
    dialect: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
) -> str:
    from connectors.sql_identifiers import quote_table_ref
    from services.dialect_profiles import normalize_schema

    sch = normalize_schema(
        dialect,
        schema or cfg.get("schema"),
        username=cfg.get("username"),
    )
    project = None
    if dialect == "bigquery":
        project = str(cfg.get("project") or cfg.get("database") or "") or None
    return quote_table_ref(table_name, sch, dialect=dialect, project=project)


def _is_missing_warehouse_relation(exc: BaseException, dialect: str) -> bool:
    """True only for 'no such table'. Login/network/privilege-other stay unmeasured.

    SQL Server 208 / 42S02 / Invalid object name. Oracle ORA-00942.
    Snowflake object-does-not-exist (not 250001 auth). BigQuery
    ``Not found: Table``. DuckDB catalog table-missing. Databricks
    ``TABLE_OR_VIEW_NOT_FOUND``. Never stats-view absence.
    """
    text = str(exc).lower()
    orig = getattr(exc, "orig", None)
    orig_text = str(orig).lower() if orig is not None else ""
    combined = f"{text} {orig_text}"
    if dialect == "oracle":
        return "ora-00942" in combined
    if dialect == "snowflake":
        return "does not exist" in combined and "250001" not in combined and "password" not in combined
    if dialect == "bigquery":
        return "not found: table" in combined
    if dialect == "duckdb":
        return "catalog error" in combined and "does not exist" in combined
    if dialect == "databricks":
        return "table_or_view_not_found" in combined or "table or view not found" in combined
    args = getattr(orig, "args", ()) if orig is not None else ()
    if args and str(args[0]) in {"208", "42S02"}:
        return True
    code = getattr(orig, "sqlstate", None) or getattr(exc, "sqlstate", None)
    if str(code or "").upper() == "42S02":
        return True
    return "invalid object name" in combined


def _is_missing_warehouse_column(exc: BaseException, dialect: str) -> bool:
    """True only for 'no such column'. Missing table is a different classifier.

    SQL Server 207 / 42S22 / Invalid column name. Oracle ORA-00904.
    Snowflake invalid identifier. BigQuery unrecognized name. DuckDB
    missing column. Databricks unresolved column. Never current=0.
    """
    text = str(exc).lower()
    orig = getattr(exc, "orig", None)
    orig_text = str(orig).lower() if orig is not None else ""
    combined = f"{text} {orig_text}"
    if dialect == "oracle":
        return "ora-00904" in combined
    if dialect == "snowflake":
        return "invalid identifier" in combined
    if dialect == "bigquery":
        return "unrecognized name" in combined
    if dialect == "duckdb":
        return "referenced column" in combined or "does not have a column" in combined
    if dialect == "databricks":
        return "unresolved_column" in combined or "cannot be resolved" in combined
    args = getattr(orig, "args", ()) if orig is not None else ()
    if args and str(args[0]) in {"207", "42S22"}:
        return True
    code = getattr(orig, "sqlstate", None) or getattr(exc, "sqlstate", None)
    if str(code or "").upper() == "42S22":
        return True
    return "invalid column name" in combined


@contextmanager
def _warehouse_sql_engine(db_type: str, cfg: Mapping[str, Any]) -> Iterator[Any]:
    from connectors.generic_sql import get_sqlalchemy_engine
    from services.engine_pool import release_engine

    engine_cfg = dict(cfg)
    if not str(engine_cfg.get("type") or "").strip():
        engine_cfg["type"] = db_type
    engine = get_sqlalchemy_engine(engine_cfg)
    try:
        yield engine
    finally:
        release_engine(engine)


def _warehouse_sql_row_count(
    db_type: str,
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    dialect: str,
) -> int | None:
    """Dest-engine COUNT(*). Missing table is 0. Stats views never consulted."""
    import sqlalchemy as sa

    try:
        table_ref = _warehouse_sql_table_ref(dialect, cfg, schema, table_name)
        with _warehouse_sql_engine(db_type, cfg) as engine:
            with engine.connect() as conn:
                n = conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
        return int(n or 0)
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, dialect):
            return 0
        logger.warning("Warehouse dest COUNT(*) failed: %s", exc)
        return None


def _warehouse_sql_key_list(
    db_type: str,
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    dialect: str,
) -> list[tuple[Any, ...]] | None:
    import sqlalchemy as sa
    from connectors.sql_identifiers import quote_sql_identifier
    from services.dialect_profiles import quote_char_for

    qchar = quote_char_for(dialect) or '"'
    table_ref = _warehouse_sql_table_ref(dialect, cfg, schema, table_name)
    col_sql = ", ".join(quote_sql_identifier(c, qchar) for c in cols)
    sql = f"SELECT {col_sql} FROM {table_ref}"  # nosec B608
    try:
        with _warehouse_sql_engine(db_type, cfg) as engine:
            with engine.connect() as conn:
                rows = conn.execute(sa.text(sql)).fetchall() or []
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, dialect):
            return []
        logger.warning("Warehouse dest key list failed: %s", exc)
        return None
    out: list[tuple[Any, ...]] = []
    width = len(cols)
    for row in rows:
        tup = tuple(row[:width])
        if len(tup) != width or any(v is None for v in tup):
            continue
        out.append(tup)
    return out


def _warehouse_sql_key_hits(
    db_type: str,
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
    dialect: str,
) -> int | None:
    """How many of these keys dest holds. Named binds; composite is AND/OR.

    Oracle 19c (JPMorgan-class) has no row-value ``IN ((a,b),…)``. SQL Server
    does, but one portable predicate keeps leftover MERGE listing exact on
    both engines. Chunk size stays under SQL Server's 2100-parameter cap.
    """
    import sqlalchemy as sa
    from connectors.sql_identifiers import quote_sql_identifier
    from services.dialect_profiles import quote_char_for

    qchar = quote_char_for(dialect) or '"'
    table_ref = _warehouse_sql_table_ref(dialect, cfg, schema, table_name)
    qcols = [quote_sql_identifier(c, qchar) for c in cols]
    col_sql = ", ".join(qcols)
    width = len(cols)
    total = 0
    try:
        with _warehouse_sql_engine(db_type, cfg) as engine:
            with engine.connect() as conn:
                for i in range(0, len(keys), _KEY_HIT_CHUNK):
                    chunk = keys[i : i + _KEY_HIT_CHUNK]
                    if width == 1:
                        placeholders = ", ".join(f":k{j}" for j in range(len(chunk)))
                        sql = (
                            f"SELECT COUNT(DISTINCT {col_sql}) FROM {table_ref} "  # nosec B608
                            f"WHERE {col_sql} IN ({placeholders})"
                        )
                        params = {f"k{j}": row[0] for j, row in enumerate(chunk)}
                    else:
                        clauses: list[str] = []
                        params = {}
                        for j, row in enumerate(chunk):
                            parts = []
                            for c_i, quoted in enumerate(qcols):
                                name = f"k{j}_{c_i}"
                                parts.append(f"{quoted} = :{name}")
                                params[name] = row[c_i]
                            clauses.append("(" + " AND ".join(parts) + ")")
                        sql = (
                            f"SELECT COUNT(*) FROM ("  # nosec B608
                            f"SELECT DISTINCT {col_sql} FROM {table_ref} "
                            f"WHERE {' OR '.join(clauses)}"
                            f") _df_key_hits"
                        )
                    n = conn.execute(sa.text(sql), params).scalar()
                    total += int(n or 0)
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, dialect):
            return 0
        logger.warning("Warehouse dest key hits failed: %s", exc)
        return None
    return total


def _warehouse_sql_scd2_populations(
    db_type: str,
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    dialect: str,
) -> dict[str, int] | None:
    """Current vs history from dest-engine COUNT. BIT/NUMBER(1) use ``= 1``.

    Missing table is 0/0. Live table without ``is_current`` is unmeasured.
    Login/network failure is unmeasured. History COUNT(*) never closes.
    """
    import sqlalchemy as sa
    from connectors.sql_identifiers import quote_sql_identifier
    from services.dialect_profiles import denormalize_result_key, quote_char_for
    from services.scd2_engine import IS_CURRENT_COLUMN, scd2_is_current_predicate

    qchar = quote_char_for(dialect) or '"'
    table_ref = _warehouse_sql_table_ref(dialect, cfg, schema, table_name)
    col = denormalize_result_key(dialect, IS_CURRENT_COLUMN)
    current_q = quote_sql_identifier(col, qchar)
    pred = scd2_is_current_predicate(dialect, current_q)
    history_sql = f"SELECT COUNT(*) FROM {table_ref}"  # nosec B608
    current_sql = f"SELECT COUNT(*) FROM {table_ref} WHERE {pred}"  # nosec B608
    try:
        with _warehouse_sql_engine(db_type, cfg) as engine:
            with engine.connect() as conn:
                try:
                    history = conn.execute(sa.text(history_sql)).scalar()
                except Exception as exc:
                    if _is_missing_warehouse_relation(exc, dialect):
                        return {CURRENT_ROWS_KEY: 0, HISTORY_ROWS_KEY: 0}
                    raise
                try:
                    current = conn.execute(sa.text(current_sql)).scalar()
                except Exception as exc:
                    if _is_missing_warehouse_column(exc, dialect):
                        return None
                    raise
        return {
            CURRENT_ROWS_KEY: int(current or 0),
            HISTORY_ROWS_KEY: int(history or 0),
        }
    except Exception as exc:
        logger.warning("Warehouse SCD2 current-row count failed: %s", exc)
        return None


def _key_list_sql(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref

    dialect = "mysql" if db_type == "mysql" else db_type
    qchar = "`" if dialect == "mysql" else '"'
    table_ref = quote_table_ref(
        table_name,
        schema if dialect == "postgresql" else None,
        dialect="postgresql" if dialect == "postgresql" else dialect,
    )
    col_sql = ", ".join(quote_sql_identifier(c, qchar) for c in cols)
    sql = f"SELECT {col_sql} FROM {table_ref}"  # nosec B608
    if dialect == "sqlite":
        import sqlite3

        database = str(cfg.get("database") or "")
        if not database:
            return None
        with sqlite3.connect(database) as conn:
            return _fetch_key_rows(conn, sql, len(cols))
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
            return _fetch_key_rows(conn, sql, len(cols))
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
            return _fetch_key_rows(conn, sql, len(cols))
        finally:
            conn.close()
    return None


def _fetch_key_rows(conn: Any, sql: str, width: int) -> list[tuple[Any, ...]]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall() or []
    finally:
        cur.close()
    out: list[tuple[Any, ...]] = []
    for row in rows:
        tup = tuple(row[:width])
        if len(tup) != width or any(v is None for v in tup):
            continue
        out.append(tup)
    return out


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
    return (
        "NoSuchTable" in name
        or "NoSuchNamespace" in name
        or "not found" in text
        or "does not exist" in text
    )


def _iceberg_row_count(
    cfg: dict[str, Any], *, schema: str, table_name: str
) -> int | None:
    """Dest COUNT is ``len`` of the leftover-MERGE snapshot, never ``scan().count()``.

    pyiceberg ``scan().count()`` is typically manifest ``record-count`` — the
    same honesty hole as ``sys.partitions`` / Oracle ``ALL_TABLES.num_rows``.
    Missing table (or namespace) is 0. Unreadable snapshot is ``None``.
    """
    rows = _iceberg_snapshot_rows(cfg, schema=schema, table_name=table_name, cols=())
    if rows is None:
        return None
    return len(rows)


def _iceberg_snapshot_rows(
    cfg: dict[str, Any], *, schema: str, table_name: str, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    """Current snapshot rows. Dest COUNT, key list, and leftover MERGE share this.

    Catalog path materializes ``scan().to_arrow()``. Metadata ``record-count``
    and ``scan().count()`` are never this population. Missing table is ``[]``.
    """
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
        scan = tbl.scan().select(*wanted) if wanted else tbl.scan()
        return list(scan.to_arrow().to_pylist())
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


def _iceberg_key_list(
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """Current-snapshot PK tuples. Never metadata ``record-count``. Never deletes.

    Same population as dest COUNT(*) (``len`` of this listing). Catalog
    ``scan().count()`` / metadata ``record-count`` never close. Missing
    table is ``[]``. Incomplete / unreadable snapshot is ``None``.
    """
    rows = _iceberg_snapshot_rows(cfg, schema=schema, table_name=table_name, cols=cols)
    if rows is None:
        return None
    out: list[tuple[Any, ...]] = []
    width = len(cols)
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        tup = tuple(row.get(c) for c in cols)
        if len(tup) != width or any(v is None for v in tup):
            continue
        out.append(tup)
    return out


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
    """Catalog SKU → dest-engine family. ``amazon_s3`` counts as ``s3``."""
    key = str(db_type or "").strip().lower()
    if key in {
        "adls",
        "azure_blob_storage",
        "azure_data_lake",
        "azure_data_lake_storage",
    }:
        return "adls"
    if key in {"s3", "amazon_s3"}:
        return "s3"
    if key == "gcs":
        return "gcs"
    return key


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
    """Dest-engine artifact COUNT of GET bodies. Missing object is 0.

    Same format machine as local ``count_artifact_rows``. A truncated or
    unparseable part is unmeasured — never JSON-fallback empty, never the
    sum of a prefix. Writer PUT rowcount is not this proof.
    """
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
    total = 0
    for obj_key, body in payloads:
        n = _count_artifact_payload(body, name=str(obj_key))
        if n is None:
            logger.info(
                "object-store dest COUNT unmeasured for %s/%s", bucket, obj_key
            )
            return None
        total += n
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
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return "excel"
    if name.endswith(".avro"):
        return "avro"
    if name.endswith(".orc"):
        return "orc"
    if name.endswith(".xml"):
        return "xml"
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


def _count_parquet_bytes(content: bytes) -> int | None:
    import io

    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    try:
        metadata = pq.ParquetFile(io.BytesIO(content)).metadata
        if metadata is None:
            return None
        return int(metadata.num_rows)
    except Exception as exc:
        logger.info("parquet artifact count unavailable: %s", exc)
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


def _count_orc_bytes(content: bytes) -> int | None:
    import io

    try:
        from pyarrow import orc
    except ImportError:
        return None
    try:
        reader = orc.ORCFile(io.BytesIO(content))
        n = getattr(reader, "nrows", None)
        if n is None:
            n = reader.read().num_rows
        return int(n)
    except Exception as exc:
        logger.info("orc artifact count unavailable: %s", exc)
        return None


def _count_orc_path(path: Path) -> int | None:
    """ORC footer stripe cardinality — dest-engine COUNT of this file.

    Same honesty as Parquet ``metadata.num_rows``. Never a warehouse
    catalog estimate. Missing pyarrow stays unmeasured, not zero.
    """
    try:
        from pyarrow import orc
    except ImportError:
        return None
    try:
        reader = orc.ORCFile(str(path))
        n = getattr(reader, "nrows", None)
        if n is None:
            n = reader.read().num_rows
        return int(n)
    except Exception as exc:
        logger.info("orc artifact count unavailable at %s: %s", path, exc)
        return None


def _count_excel_bytes(content: bytes) -> int | None:
    """Value-bearing rows. Used-range / ``max_row`` is not dest population."""
    try:
        from services.excel_parser import count_excel_rows

        return int(count_excel_rows(content))
    except Exception as exc:
        logger.info("excel artifact count unavailable: %s", exc)
        return None


def _count_avro_bytes(content: bytes) -> int | None:
    """Streamed record COUNT. Never materialize; never the ingest 100k cap."""
    import io

    try:
        import fastavro
    except ImportError:
        return None
    try:
        return sum(1 for _ in fastavro.reader(io.BytesIO(content)))
    except Exception as exc:
        logger.info("avro artifact count unavailable: %s", exc)
        return None


def _count_artifact_kind(kind: str, content: bytes) -> int | None:
    """Record COUNT for already-decoded artifact bytes. Unknown kind is None."""
    if kind not in _ARTIFACT_FORMATS:
        return None
    if kind == "parquet":
        return _count_parquet_bytes(content)
    if kind == "orc":
        return _count_orc_bytes(content)
    try:
        if kind in {"csv", "tsv"}:
            from services.csv_profiler import count_csv_rows

            return int(count_csv_rows(content))
        if kind == "jsonl":
            return _count_jsonl_bytes(content)
        if kind == "json":
            return _count_json_bytes(content)
        if kind == "excel":
            return _count_excel_bytes(content)
        if kind == "avro":
            return _count_avro_bytes(content)
        if kind == "xml":
            from services.file_parser import count_xml_records

            n = count_xml_records(content)
            return None if n is None else int(n)
    except Exception as exc:
        logger.info("artifact count failed for kind %s: %s", kind, exc)
        return None
    return None


def _count_artifact_payload(
    content: bytes,
    *,
    name: str,
    fmt: str | None = None,
) -> int | None:
    """Dest-engine COUNT of an object-store GET body. Same machine as a local file.

    Gzip keys decompress first. Unparseable / unsupported / missing parser
    stay unmeasured — never JSON-fallback empty (that is dest=0).
    """
    label = str(name or "")
    body = content
    if label.lower().endswith(".gz"):
        try:
            body = gzip.decompress(content)
        except Exception as exc:
            logger.info("artifact gzip decode failed for %s: %s", label, exc)
            return None
        label = label[: -len(".gz")]
    kind = _infer_artifact_format(Path(label), fmt)
    return _count_artifact_kind(kind, body)


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
    Empty but well-formed artifacts are measured zero. Excel counts rows
    that carry values, not the worksheet used range. XML counts the unique
    repeating record-path, not ingest ``max_rows``.
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
    if kind == "orc":
        return _count_orc_path(artifact)
    content = _read_artifact_bytes(artifact)
    if content is None:
        return None
    return _count_artifact_kind(kind, content)


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

    Gate-8 ``target_rows`` on a vector dest is embedding cardinality (chunks)
    when a SQL engine can COUNT(*) them. That figure must not survive as
    dest population — 2 documents / 5 chunks would close overwrite as a
    surplus. Cell fidelity of opaque embeddings stays with the caller
    (``skipped_readback`` / ``migration_proven=false``). This only owns
    identity cardinality.

    Engines in ``VECTOR_IDENTITY_ENGINES`` (pgvector, Milvus, Qdrant,
    Pinecone, Weaviate) run dest-engine DISTINCT ``source_id``. Physical
    ``rowCount`` / ``vectorCount`` / Aggregate ``meta.count`` is not
    identity and is left as diagnostic ``vector_rows``.
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


def _scd2_current_predicate(dialect: str, quoted_column: str) -> str:
    """Same rule as merge/expire: BIT/NUMBER(1) ``= 1``, BOOLEAN ``IS TRUE``."""
    from services.scd2_engine import scd2_is_current_predicate

    return scd2_is_current_predicate(dialect, quoted_column)


def _sqlite_column_names(conn: Any, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()  # nosec B608
    names: set[str] = set()
    for row in rows:
        name = row[1] if not isinstance(row, Mapping) else row.get("name")
        if name:
            names.add(str(name).lower())
    return names


def count_scd2_current(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """``COUNT(*) WHERE is_current`` — current identities, not history versions.

    Missing table is 0 (create-on-first-write). A live table without
    ``is_current`` cannot prove current identities — return ``None``,
    never fall back to physical history ``COUNT(*)``.
    """
    pop = count_scd2_populations(
        db_type, cfg, schema=schema, table_name=table_name
    )
    if pop is None:
        return None
    return int(pop[CURRENT_ROWS_KEY])


def count_scd2_populations(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> dict[str, int] | None:
    """Current and history populations from one dest-engine session.

    ``None`` means current is unmeasurable (unsupported engine, missing
    ``is_current``, or unreachable dest). Missing table is measured zero
    on both axes. Oracle / SQL Server (and catalog SKUs) use the warehouse
    dest-engine session; Snowflake / BigQuery / DuckDB / Databricks use
    the same ``COUNT(*)`` machine (BOOLEAN ``IS TRUE``), never catalog
    stats.
    """
    table = (table_name or "").strip()
    if not table:
        return None
    kind = str(db_type or "").strip().lower()
    try:
        if kind == "sqlite":
            return _sqlite_scd2_populations(cfg, table_name=table)
        if kind in {"postgresql", "redshift"}:
            return _pg_scd2_populations(cfg, schema=schema, table_name=table)
        if kind in {"mysql", "mariadb"}:
            return _mysql_scd2_populations(cfg, table_name=table)
        from services.dialect_profiles import warehouse_sql_quote_dialect

        dialect = warehouse_sql_quote_dialect(kind)
        if dialect:
            return _warehouse_sql_scd2_populations(
                kind, cfg, schema=schema, table_name=table, dialect=dialect
            )
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("SCD2 current-row count failed: %s", exc)
        return None
    return None


def _sqlite_scd2_populations(
    cfg: dict[str, Any],
    *,
    table_name: str,
) -> dict[str, int] | None:
    import sqlite3

    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
    from services.scd2_engine import IS_CURRENT_COLUMN

    database = str(cfg.get("database") or "")
    if not database:
        return None
    with sqlite3.connect(database) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not exists:
            return {CURRENT_ROWS_KEY: 0, HISTORY_ROWS_KEY: 0}
        if IS_CURRENT_COLUMN.lower() not in _sqlite_column_names(conn, table_name):
            return None
        table_ref = quote_table_ref(table_name, dialect="sqlite")
        current_q = quote_sql_identifier(IS_CURRENT_COLUMN)
        current = _count_where(
            conn, table_ref, _scd2_current_predicate("sqlite", current_q)
        )
        history = _count(conn, table_ref)
        return {CURRENT_ROWS_KEY: current, HISTORY_ROWS_KEY: history}


def _pg_scd2_populations(
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> dict[str, int] | None:
    from connectors.postgresql_conn import get_connection
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
    from services.scd2_engine import IS_CURRENT_COLUMN

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
                return {CURRENT_ROWS_KEY: 0, HISTORY_ROWS_KEY: 0}
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "AND column_name = %s",
                (sch, table_name, IS_CURRENT_COLUMN),
            )
            if cur.fetchone() is None:
                return None
        table_ref = quote_table_ref(table_name, sch, dialect="postgresql")
        current_q = quote_sql_identifier(IS_CURRENT_COLUMN)
        current = _count_where(
            conn, table_ref, _scd2_current_predicate("postgresql", current_q)
        )
        history = _count(conn, table_ref)
        return {CURRENT_ROWS_KEY: current, HISTORY_ROWS_KEY: history}
    finally:
        conn.close()


def _mysql_scd2_populations(
    cfg: dict[str, Any],
    *,
    table_name: str,
) -> dict[str, int] | None:
    from connectors.mysql_conn import get_connection
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
    from services.scd2_engine import IS_CURRENT_COLUMN

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
                (table_name,),
            )
            row = cur.fetchone()
            if not row or not int(row[0]):
                return {CURRENT_ROWS_KEY: 0, HISTORY_ROWS_KEY: 0}
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = %s "
                "AND column_name = %s",
                (table_name, IS_CURRENT_COLUMN),
            )
            if cur.fetchone() is None:
                return None
        table_ref = quote_table_ref(table_name, dialect="mysql")
        current_q = quote_sql_identifier(IS_CURRENT_COLUMN, "`")
        current = _count_where(
            conn, table_ref, _scd2_current_predicate("mysql", current_q)
        )
        history = _count(conn, table_ref)
        return {CURRENT_ROWS_KEY: current, HISTORY_ROWS_KEY: history}
    finally:
        conn.close()


def stamp_scd2_census(
    recon: Mapping[str, Any],
    dest_cfg: Mapping[str, Any] | None,
    *,
    schema: str,
    table_name: str,
    dest_engine: str,
) -> dict[str, Any]:
    """Stamp COUNT(*) WHERE is_current. Never history COUNT(*) or writer ack.

    Gate-8 ``target_rows`` on SCD2 is the writer's ``_active_checksum``
    ``active_rows`` (versions read during merge). That figure must not
    survive as dest population — physical history COUNT(*) would close
    overwrite as a surplus after the first attribute change. Cell
    fidelity of current versions stays with the caller. This only owns
    current-row cardinality.

    Vector destinations own identity COUNT(DISTINCT source_id), not this
    temporal current census. ``is_current`` is not a tombstone.
    """
    out = dict(recon)
    engine = str(dest_engine or "").strip().lower()
    if engine in _VECTOR_IDENTITY_ENGINES:
        return out
    if not dest_cfg or not str(table_name or "").strip():
        return out
    pop = count_scd2_populations(
        engine or str(dict(dest_cfg).get("type") or dict(dest_cfg).get("db_type") or ""),
        dict(dest_cfg),
        schema=str(schema or ""),
        table_name=str(table_name or ""),
    )
    if pop is None:
        out[DEST_COUNT_SOURCE_KEY] = "skipped_current_readback"
        return out
    current = int(pop[CURRENT_ROWS_KEY])
    history = int(pop[HISTORY_ROWS_KEY])
    out[CURRENT_ROWS_KEY] = current
    out[HISTORY_ROWS_KEY] = history
    out[DEST_COUNT_SOURCE_KEY] = DEST_COUNT_CURRENT
    nested = dict(out.get("scd2") or {}) if isinstance(out.get("scd2"), dict) else {}
    nested["current_rows"] = current
    nested["history_rows"] = history
    out["scd2"] = nested
    return out
