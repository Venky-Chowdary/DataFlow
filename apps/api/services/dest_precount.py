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
(``count_xml_records`` StAX), never ``parse_xml`` ingest ``max_rows`` and
never a whole-document-as-one fallback. Ambiguous sibling collections
stay unmeasured. Empty ``<records/>`` is 0. Missing parser is
unmeasured, not dest=0. Local XML is counted from the path; gzip XML
streams (never a decompressed slurp). JSON dest population is the unique
array-of-object (``count_json_records`` ijson StAX), never
``json.loads`` of the whole export, never ingest single-object-as-one
or preferred-wrapper ranking. Empty ``[]`` is 0. Scalar arrays stay
unmeasured, not dest=N. Local JSON is counted from the path; gzip JSON
streams. JSONL dest population is one object per
non-blank line (``count_jsonl_records`` line stream), never
``decode`` + ``splitlines`` of the whole export, never ingest
``parse_jsonl`` (raises, materializes). Empty file / only blank lines
is 0. A scalar, array, or malformed line stays unmeasured — never
COUNT of a prefix. Local JSONL is counted from the path; gzip JSONL
streams. NDJSON aliases onto JSONL. CSV/TSV dest
population is RFC 4180 ``csv.reader`` rows after the header
(``count_csv_rows``), never ``wc -l``, never spreadsheet ``,,,,``
blank lines. Quoted embedded newlines are one record. Local CSV/TSV
is counted from the path; gzip CSV/TSV streams. Excel / Parquet / ORC gzip
stream-decompress into one rewindable image (``GzipFile`` +
``rewindable_byte_source``). Footer/workbook parsers need seek; Hadoop
GzipCodec is not splittable — this is not a fake sequential COUNT.
Never ``gzip.decompress(source.read())`` (compressed + decompressed copies).
Avro is a sequential object-container (header + blocks), not a footer
format — local and GET gzip Avro stream through ``GzipFile`` like JSONL.
Object-store GET of CSV/JSON/JSONL/XML/Avro streams the HTTP body
(gzip through ``GzipFile(fileobj=StreamingBody)``).
Uncompressed Parquet/ORC dest COUNT Range-GETs the footer (Hadoop
``FSDataInputStream`` / Spark footer read) — never a spool of the
object. Gzip Parquet/ORC/Excel stay ``GzipFile`` + spool (codec is
not splittable). COUNT does not ``Body.read()`` the object, does not
hold every part in RAM, and does not ``gzip.decompress`` a second copy.
Gate-8 checksum still walks data pages via sequential GET. Gate-8 cell
checksum of those same GET streams is ``checksum_object_store`` — never
``json.loads`` fallback empty (gzip CSV / Parquet as UTF-8 JSON garbage
was dest=0). JSON root array, JSONL objects, CSV RFC 4180 dicts, streamed
Avro records, and Parquet/ORC/Excel value walks feed ``canonical_checksum_from_iter``.
JSON unique-path cell dicts (root array or wrapped ``{\"records\":[]}``)
and XML unique-path cell dicts are a second StAX pass of the COUNT path
(one-shot GET is spooled once). Empty well-formed is ``(0, "")``. Dest sample
of those GET streams is ``sample_object_store`` / ``sample_artifact_records``
— never JSON-fallback ``[]`` (that greens a lost write). SFTP dest COUNT
and Gate-8 checksum walk the same artifact machine via ``open_sftp_binary``
(never ``fh.read()`` of the remote file). Missing SFTP object is 0.

Lakehouse and object-store destinations already have dest-*after* read-back
(Iceberg scan, S3/GCS/ADLS GET). Dest-*before* must use the same COUNT so
append delta and first-write overwrite (missing table/object = 0) can close.
Writer ``Table.upsert`` / PUT rowcount is not that proof. Object-store dest
COUNT is the same artifact machine as local file export (Excel value rows,
streamed Avro, Parquet/ORC footer, XML unique record-path, JSON unique
array-of-object, JSONL object-per-line, CSV/TSV RFC 4180 records). A JSON-parse fallback that yields ``[]``
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
BigQuery / DuckDB / Databricks / Redshift use dest-engine ``COUNT(*)`` —
never ``INFORMATION_SCHEMA`` / ``__TABLES__`` / ``SVV_TABLE_INFO.tbl_rows``
(unvacuumed ghosts; Spectrum has no tbl_rows). Redshift is PG-wire, not
PG-catalog (no ``to_regclass``). ClickHouse dest COUNT is
``COUNT(*) FROM table FINAL`` (same helper the writer uses for
ReplacingMergeTree) — never ``system.tables.total_rows``. ClickHouse
leftover MERGE lists ``SELECT pk FROM table FINAL``, then
``ALTER TABLE … DELETE`` + ``SYSTEM WAIT MUTATIONS`` so FINAL can see
the delete. Composite key hits use portable AND/OR equality, not row-value
``IN`` (Oracle 19c
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
it never deletes them itself. Iceberg dest COUNT is dest-engine
population of current-snapshot data files (Parquet footer / JSONL
stream), never metadata ``record-count`` / ``scan().count()`` /
``len(to_pylist())``. Catalog ``s3://`` / ``gs://`` / ``abfss://`` / ``hdfs://``
data files Range-GET the footer through the object-store kernel
(WebHDFS only when ``webhdfs.endpoint`` is set; RPC ``:8020`` is not
HTTP). A snapshot file that 404s is unmeasured, not dest=0. Key list and leftover
MERGE project PK columns from those same snapshot files — never
``scan().to_arrow()`` of the table. ``row_conservation.apply_inferred_leftover_deletes``
applies the anti-join only when the source census is complete overwrite
(SQL and Iceberg). Incremental CDC must not call that apply. Mirror
already applies inferred soft-deletes on full re-sync. Iceberg v2 MoR
(position/equality) and v3 deletion vectors (Puffin
``deletion-vector-v1``) are dest − applied deletes. Missing
``content_offset`` / CRC / cardinality stays unmeasured. The identity
is still ``leftover = D \\ S``.

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
``is_current`` column is unmeasured, never current=0. Warehouse BOOLEAN
engines (Snowflake / BigQuery / DuckDB / Databricks / Redshift) use
``IS TRUE``.

``None`` means the count is unavailable (unsupported engine, missing table,
unreachable destination, or an unreadable/unsupported artifact); callers
must degrade assurance rather than assume zero.
"""

from __future__ import annotations

import gzip
import io
import logging
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from services.value_serializer import is_null_evidence, present_cell_text

if TYPE_CHECKING:
    from src.transfer.models import EndpointConfig

logger = logging.getLogger(__name__)

__all__ = [
    "PRECOUNT_KEY",
    "count_dialect",
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
# Streaming Gate-8 passes records=[] so leftover MERGE cannot rebuild S
# from the in-memory snapshot. The stream stamps unique source PK tuples
# here (bounded) for D \ S. Resume / overflow / duplicate PK leave it unset.
OVERWRITE_SOURCE_KEYS_KEY = "overwrite_source_keys"
_KEYSET_CENSUS_MAX = 20_000
# Same bound as dest key listing: a prefix DISTINCT is a lie.
_IDENTITY_SCAN_MAX = _KEYSET_CENSUS_MAX

_ARTIFACT_FORMATS = frozenset({
    "csv", "tsv", "json", "jsonl", "parquet", "excel", "avro", "orc", "xml",
    "yaml",
})
_STREAMING_COUNT_KINDS = frozenset({"csv", "tsv", "json", "jsonl", "xml", "avro", "yaml"})
_BYTE_IMAGE_KINDS = frozenset({"parquet", "orc", "excel"})


class UnmeasuredArtifact(Exception):
    """Dest population cannot be checksummed. Never hash a prefix or JSON ``[]``.

    Gate-8 ``json.loads`` fallback empty is dest=0 — the same hole COUNT
    already refuses. Poison JSONL and ambiguous sibling collections stay
    unmeasured. JSON and XML unique-path cell dicts are a second StAX pass.
    """


_ARTIFACT_SPOOL_MAX = 8 * 1024 * 1024


def source_can_rewind(source: Any) -> bool:
    """Whether a second pass can ``seek(0)`` after consuming the stream.

    CPython ``GzipFile.seekable()`` is True even when the compressed
    ``fileobj`` cannot rewind after EOF (a one-shot HTTP GET). Rewind
    capability is the byte container, not the codec wrapper: local gzip
    wrapping a file seeks; gzip wrapping a StreamingBody must spool.
    """
    inner = source.fileobj if isinstance(source, gzip.GzipFile) else source
    try:
        return bool(inner.seekable())
    except Exception:
        return False


def _spool_byte_source(source: Any) -> tuple[Any, Any]:
    """Copy a forward-only stream into a seekable image (RAM until 8 MiB).

    Hadoop's local two-pass: one sequential read, then cheap ``seek``.
    Chunked ``read(1 MiB)`` — never unsized ``Body.read()`` of a GET.
    """
    spool = tempfile.SpooledTemporaryFile(max_size=_ARTIFACT_SPOOL_MAX)
    try:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            if not isinstance(chunk, (bytes, bytearray)):
                raise TypeError("artifact stream must yield bytes")
            spool.write(chunk)
        spool.seek(0)
    except Exception:
        spool.close()
        raise
    return spool, spool.close


def rewindable_byte_source(source: Any) -> tuple[Any, Any]:
    """Rewindable uncompressed byte container. One-shot GET is spooled.

    Unique-path identity is known only after the document ends. Pass 1
    discovers; pass 2 emits. Parquet/ORC footers seek to the end;
    Excel is a zip workbook. ``GzipFile.seek`` is decompress-from-start
    (and CPython reports seekable even when the compressed GET cannot
    rewind). A gzip wrapper is always spooled once — the spool is the
    byte container. Uncompressed seekable handles (``BytesIO``, local
    files) rewind in place. JSON/XML Gate-8 and footer/workbook COUNT
    share this kernel. Never ``gzip.decompress(source.read())``.
    """
    if isinstance(source, gzip.GzipFile) or not source_can_rewind(source):
        return _spool_byte_source(source)
    try:
        source.seek(0)
        return source, None
    except (OSError, AttributeError, io.UnsupportedOperation, ValueError):
        return _spool_byte_source(source)


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

    Callers that resolved the *capability* family hand in ``generic_sql`` for
    every engine behind the shared SQLAlchemy writer. That name matches no
    branch below, so the count came back unknowable while the write itself
    worked; normalising here keeps one owner for the engine identity.
    """
    table = (table_name or "").strip()
    if not table:
        return None
    if db_type == "generic_sql":
        db_type = count_dialect(str(cfg.get("type") or db_type))
    try:
        from connectors.sql_identifiers import quote_table_ref

        if db_type == "sqlite":
            import sqlite3

            database = str(cfg.get("database") or "")
            if not database:
                return None
            with closing(sqlite3.connect(database)) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not exists:
                    return 0
                return _count(conn, quote_table_ref(table, dialect="sqlite"))

        if db_type == "postgresql":
            from connectors.postgresql_conn import get_connection

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

        if db_type == "sftp":
            return _sftp_row_count(cfg, table_name=table)

        if db_type == "redis":
            return _redis_prefix_row_count(cfg, prefix=table)

        if db_type == "dynamodb":
            return _dynamodb_item_count(cfg, table=table)

        if db_type in {"elasticsearch", "opensearch"}:
            return _search_index_doc_count(cfg, index=table)

        if db_type == "clickhouse":
            return _clickhouse_row_count(cfg, schema=schema, table_name=table)

        # Every SQLAlchemy engine that is not one of the named dialects above
        # arrives here as ``generic_sql`` (DuckDB, Trino, RisingWave, …). Having
        # no branch meant dest-before was unknowable for all of them, so an
        # append delta and a quiet incremental poll could not be proven and
        # correct runs were reported unproven — while dest-*after* counted fine
        # through the same connection.
        if db_type == "generic_sql":
            from connectors.generic_sql import count_table

            return count_table(cfg, table)

        from services.dialect_profiles import warehouse_sql_quote_dialect

        dialect = warehouse_sql_quote_dialect(db_type)
        # Snowflake and BigQuery answer through the driver the writer and the
        # Gate-8 read-back already use. The SQLAlchemy route needs
        # `snowflake-sqlalchemy` / `sqlalchemy-bigquery`, which the product does
        # not ship: dest-*after* counted fine through the native client while
        # dest-*before* came back unknowable, and a correct append was failed
        # for having no delta to prove.
        if dialect == "snowflake":
            return _snowflake_row_count(cfg, schema=schema, table_name=table)

        if dialect == "bigquery":
            return _bigquery_row_count(cfg, schema=schema, table_name=table)

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
    unique = _unique_key_tuples(keys or [], len(cols))
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
    if _object_store_kind(db_type) in {"s3", "gcs", "adls"}:
        try:
            from connectors.object_store_leftover import object_store_key_hits

            return object_store_key_hits(
                db_type, cfg, table_name=table, cols=cols, keys=unique
            )
        except Exception as exc:
            logger.warning("Object-store dest key census failed: %s", exc)
            return None
    if db_type == "snowflake":
        try:
            return _snowflake_key_hits(
                cfg, schema=schema, table_name=table, cols=cols, keys=unique
            )
        except Exception as exc:
            logger.warning("Snowflake dest key census failed: %s", exc)
            return None
    if db_type == "bigquery":
        try:
            return _bigquery_key_hits(
                cfg, schema=schema, table_name=table, cols=cols, keys=unique
            )
        except Exception as exc:
            logger.warning("BigQuery dest key census failed: %s", exc)
            return None
    if db_type == "redis":
        try:
            return _redis_key_hits(cfg, prefix=table, keys=unique)
        except Exception as exc:
            logger.warning("Redis dest key census failed: %s", exc)
            return None
    if db_type == "dynamodb":
        try:
            return _dynamodb_key_hits(
                cfg, table_name=table, cols=cols, keys=unique
            )
        except Exception as exc:
            logger.warning("DynamoDB dest key census failed: %s", exc)
            return None
    if db_type in {"elasticsearch", "opensearch"}:
        try:
            return _elasticsearch_key_hits(
                cfg, index=table, cols=cols, keys=unique
            )
        except Exception as exc:
            logger.warning("Elasticsearch dest key census failed: %s", exc)
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
    seen: set[tuple[str, ...]] = set()
    for raw in keys or []:
        tup = tuple(raw)
        if len(tup) != width:
            continue
        norm = _norm_dest_key(tup)
        if norm is None or norm in seen:
            continue
        seen.add(norm)
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
    seen: set[tuple[str, ...]] = set()
    for rec in records:
        if not isinstance(rec, Mapping):
            return None
        row: list[Any] = []
        for field, target in zip(source_fields, cols):
            raw = rec.get(field)
            if raw is None and field != target:
                raw = rec.get(target)
            if is_null_evidence(raw):
                return None
            row.append(raw)
        tup = tuple(row)
        norm = _norm_dest_key(tup)
        if norm is None or norm in seen:
            return None
        seen.add(norm)
        tuples.append(tup)
    if len(tuples) != len(records):
        return None
    return tuples


def matrix_to_key_tuples(
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]] | None,
    mappings: Sequence[Mapping[str, Any]] | None,
    key_columns: Sequence[str],
) -> list[tuple[Any, ...]] | None:
    """PK tuples from a writer matrix page. Empty page is ``[]``, not unusable."""
    if not rows:
        return []
    records = [dict(zip(headers, row)) for row in rows]
    return records_to_key_tuples(records, key_columns, mappings)


class OverwriteSourceKeySet:
    """Complete unique ``S`` across stream pages for leftover MERGE.

    Streaming reconcile has ``records=[]``. Collecting keys during the
    already-running source scan is the Debezium snapshot-completion analogue:
    a second source read after write would not be the same snapshot.
    Duplicate PKs, a missing PK cell, or a census past
    ``_KEYSET_CENSUS_MAX`` stay unusable — leftover MERGE then stays
    ``None`` rather than deleting from a prefix of S.
    """

    def __init__(self, width: int) -> None:
        self._width = int(width)
        self._seen: dict[tuple[Any, ...], None] = {}
        self._failed = False

    def observe_tuples(self, keys: Sequence[tuple[Any, ...]] | None) -> None:
        if self._failed:
            return
        if keys is None:
            self._failed = True
            return
        from services.row_conservation import coerce_pk_part

        for raw in keys:
            tup = tuple(raw)
            if len(tup) != self._width or any(is_null_evidence(v) for v in tup):
                self._failed = True
                return
            coerced = tuple(coerce_pk_part(p) for p in tup)
            if coerced in self._seen or len(self._seen) >= _KEYSET_CENSUS_MAX:
                self._failed = True
                return
            self._seen[coerced] = None

    def export(self) -> list[tuple[Any, ...]] | None:
        if self._failed or not self._seen:
            return None
        return list(self._seen.keys())


def begin_overwrite_source_keys(
    sync_mode: str,
    pk_cols: Sequence[str] | None,
    *,
    resumed: bool = False,
) -> OverwriteSourceKeySet | None:
    from services.sync_cursor import is_overwrite_sync

    cols = [str(c).strip() for c in (pk_cols or []) if str(c).strip()]
    if resumed or not cols or not is_overwrite_sync(sync_mode):
        return None
    return OverwriteSourceKeySet(len(cols))


def stamp_overwrite_source_keys(
    dest_summary: dict[str, Any] | None,
    acc: OverwriteSourceKeySet | None,
) -> None:
    if acc is None or not isinstance(dest_summary, dict):
        return
    keys = acc.export()
    if keys is not None:
        dest_summary[OVERWRITE_SOURCE_KEYS_KEY] = keys


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
        elif _object_store_kind(db_type) in {"s3", "gcs", "adls"}:
            from connectors.object_store_leftover import object_store_key_list

            rows = object_store_key_list(
                db_type, cfg, table_name=table, cols=cols
            )
        elif db_type == "snowflake":
            rows = _snowflake_key_list(
                cfg, schema=schema, table_name=table, cols=cols
            )
        elif db_type == "bigquery":
            rows = _bigquery_key_list(
                cfg, schema=schema, table_name=table, cols=cols
            )
        elif db_type == "clickhouse":
            rows = _clickhouse_key_list(
                cfg, schema=schema, table_name=table, cols=cols
            )
        elif db_type == "mongodb":
            rows = _mongodb_key_list(
                cfg, schema=schema, table_name=table, cols=cols
            )
        elif db_type == "redis":
            rows = _redis_key_list(cfg, prefix=table, cols=cols)
        elif db_type in {"elasticsearch", "opensearch"}:
            rows = _elasticsearch_key_list(
                cfg, index=table, cols=cols, dest_n=dest_n
            )
        elif db_type == "dynamodb":
            rows = _dynamodb_key_list(cfg, table_name=table, cols=cols)
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
    ``TABLE_OR_VIEW_NOT_FOUND``. Redshift relation/schema missing, never
    column-missing (SCD2 unmeasured). Never ``SVV_TABLE_INFO``.
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
        return "not found: table" in combined or "table not found" in combined
    if dialect == "duckdb":
        return "catalog error" in combined and "does not exist" in combined
    if dialect == "databricks":
        return "table_or_view_not_found" in combined or "table or view not found" in combined
    if dialect == "redshift":
        if "column" in combined and "does not exist" in combined:
            return False
        return ("relation" in combined or "schema" in combined) and "does not exist" in combined
    if dialect == "clickhouse":
        if "unknown_database" in combined or "code: 81" in combined:
            return False
        return (
            "unknown_table" in combined
            or "code: 60" in combined
            or ("table" in combined and ("doesn't exist" in combined or "does not exist" in combined))
        )
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
    missing column. Databricks unresolved column. Redshift column-missing.
    Never current=0.
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
    if dialect == "redshift":
        return "column" in combined and "does not exist" in combined
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


def _snowflake_row_count(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """Dest-engine ``COUNT(*)`` through the native Snowflake driver.

    The same connection, warehouse selection and stored-name resolution the
    Gate-8 read-back uses, so dest-before and dest-after count the same object:
    a table created quoted-lowercase is not the folded upper-case name, and
    counting the folded name would report a missing table as 0 against a
    populated one. A table that genuinely does not exist is 0.
    """
    from connectors.snowflake_conn import (
        _snowflake_object_missing,
        get_connection,
        normalize_account,
        resolve_snowflake_table_name,
        snowflake_qualified_table,
    )
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    sch = str(schema or cfg.get("schema") or "").strip() or "PUBLIC"
    warehouse = str(cfg.get("warehouse") or "")
    conn = None
    try:
        conn = get_connection(
            account=normalize_account(str(cfg.get("host") or "")),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            database=str(cfg.get("database") or ""),
            schema=sch,
            warehouse=warehouse,
            connection_string=str(cfg.get("connection_string") or ""),
            role=str(cfg.get("role") or ""),
            private_key=str(cfg.get("private_key") or ""),
            private_key_passphrase=str(cfg.get("private_key_passphrase") or ""),
        )
        with conn.cursor() as cur:
            if warehouse:
                try:
                    wh = require_safe_identifier(warehouse, preserve_case=True)
                    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")
                except Exception as exc:
                    logger.warning("Snowflake USE WAREHOUSE failed: %s", exc)
            resolved = resolve_snowflake_table_name(cur, sch, table_name)
            if resolved is None:
                return 0
            qualified = snowflake_qualified_table(sch, resolved)
            cur.execute(f"SELECT COUNT(*) FROM {qualified}")  # nosec B608
            row = cur.fetchone()
        return int(row[0]) if row else 0
    except Exception as exc:
        if _snowflake_object_missing(exc):
            return 0
        logger.warning("Snowflake dest COUNT(*) failed: %s", exc)
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # pragma: no cover - close-time failure
                logger.debug("Snowflake close failed: %s", exc)


def _bigquery_row_count(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """Dest-engine ``COUNT(*)`` through the native BigQuery client.

    A query, not ``Table.num_rows``: table metadata lags the streaming buffer,
    and a stale estimate cannot prove an append delta. Missing table is 0.
    """
    from google.api_core.exceptions import NotFound

    from connectors.bigquery_conn import get_client

    project = str(cfg.get("database") or cfg.get("project_id") or "")
    dataset = str(schema or cfg.get("schema") or cfg.get("dataset") or "")
    if not project or not dataset:
        return None
    try:
        client = get_client(
            project_id=project,
            credentials_path=str(cfg.get("connection_string") or ""),
            service_account=str(cfg.get("service_account") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
        )
        table_id = f"`{project}`.`{dataset}`.`{table_name}`"
        rows = _bigquery_run_query(client, f"SELECT COUNT(*) AS n FROM {table_id}")  # nosec B608
        return int(rows[0][0]) if rows else 0
    except NotFound:
        return 0
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, "bigquery"):
            return 0
        logger.warning("BigQuery dest COUNT(*) failed: %s", exc)
        return None


def _bigquery_no_retry():
    from connectors.google_emulator import google_emulator_retry

    return google_emulator_retry()


def _bigquery_run_job(client: Any, sql: str) -> Any:
    """One dest-engine job. Missing-table 500s must not retry-sleep.

    goccy/bigquery-emulator answers ``Table not found`` as InternalServerError
    500. The google client default retry would sleep until pytest/operator
    timeout instead of treating dest-missing as 0.
    """
    no_retry = _bigquery_no_retry()
    job = client.query(sql, retry=no_retry, timeout=8.0, job_retry=None)
    job.result(retry=no_retry, timeout=8.0, job_retry=None)
    return job


def _bigquery_run_query(client: Any, sql: str) -> list[Any]:
    return list(_bigquery_run_job(client, sql))


def _snowflake_key_list(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """Dest-engine PK tuples through the native Snowflake driver.

    Same connection and stored-name resolution as ``COUNT(*)``. Never
    ``INFORMATION_SCHEMA`` row estimates. Missing table is ``[]``.
    """
    from connectors.snowflake_conn import (
        _snowflake_object_missing,
        get_connection,
        normalize_account,
        resolve_snowflake_table_name,
        snowflake_qualified_table,
    )
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    sch = str(schema or cfg.get("schema") or "").strip() or "PUBLIC"
    warehouse = str(cfg.get("warehouse") or "")
    conn = None
    try:
        conn = get_connection(
            account=normalize_account(str(cfg.get("host") or "")),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            database=str(cfg.get("database") or ""),
            schema=sch,
            warehouse=warehouse,
            connection_string=str(cfg.get("connection_string") or ""),
            role=str(cfg.get("role") or ""),
            private_key=str(cfg.get("private_key") or ""),
            private_key_passphrase=str(cfg.get("private_key_passphrase") or ""),
        )
        with conn.cursor() as cur:
            if warehouse:
                try:
                    wh = require_safe_identifier(warehouse, preserve_case=True)
                    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")
                except Exception as exc:
                    logger.warning("Snowflake USE WAREHOUSE failed: %s", exc)
            resolved = resolve_snowflake_table_name(cur, sch, table_name)
            if resolved is None:
                return []
            qualified = snowflake_qualified_table(sch, resolved)
            col_sql = ", ".join(quote_sql_identifier(c) for c in cols)
            cur.execute(f"SELECT {col_sql} FROM {qualified}")  # nosec B608
            fetched = cur.fetchall() or []
    except Exception as exc:
        if _snowflake_object_missing(exc):
            return []
        logger.warning("Snowflake dest key list failed: %s", exc)
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.debug("Snowflake close failed: %s", exc)
    width = len(cols)
    out: list[tuple[Any, ...]] = []
    for row in fetched:
        tup = tuple(row[:width])
        if len(tup) != width or any(is_null_evidence(v) for v in tup):
            continue
        out.append(tup)
    return out


def _snowflake_key_hits(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    listed = _snowflake_key_list(cfg, schema=schema, table_name=table_name, cols=cols)
    if listed is None:
        return None
    wanted = {norm for key in keys if (norm := _norm_dest_key(key)) is not None}
    return sum(1 for tup in listed if _norm_dest_key(tup) in wanted)


def _snowflake_delete_keys(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: Sequence[str],
) -> int:
    """Hard-DELETE leftover PKs through the native Snowflake driver."""
    from connectors.snowflake_conn import (
        get_connection,
        normalize_account,
        resolve_snowflake_table_name,
        snowflake_qualified_table,
    )
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier
    from services.row_conservation import parse_delete_keys

    leftover = parse_delete_keys(list(keys), len(cols))
    if not leftover:
        return 0
    sch = str(schema or cfg.get("schema") or "").strip() or "PUBLIC"
    warehouse = str(cfg.get("warehouse") or "")
    conn = get_connection(
        account=normalize_account(str(cfg.get("host") or "")),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        database=str(cfg.get("database") or ""),
        schema=sch,
        warehouse=warehouse,
        connection_string=str(cfg.get("connection_string") or ""),
        role=str(cfg.get("role") or ""),
        private_key=str(cfg.get("private_key") or ""),
        private_key_passphrase=str(cfg.get("private_key_passphrase") or ""),
    )
    try:
        with conn.cursor() as cur:
            if warehouse:
                try:
                    wh = require_safe_identifier(warehouse, preserve_case=True)
                    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")
                except Exception as exc:
                    logger.warning("Snowflake USE WAREHOUSE failed: %s", exc)
            resolved = resolve_snowflake_table_name(cur, sch, table_name)
            if resolved is None:
                return 0
            qualified = snowflake_qualified_table(sch, resolved)
            qcols = [quote_sql_identifier(c) for c in cols]
            deleted = 0
            for tup in leftover:
                clause = " AND ".join(f"{q} = %s" for q in qcols)
                cur.execute(f"DELETE FROM {qualified} WHERE {clause}", list(tup))  # nosec B608
                n = cur.rowcount
                deleted += 1 if n is None or int(n) < 0 else int(n)
            conn.commit()
        return deleted
    finally:
        conn.close()


def _bigquery_key_list(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """Dest-engine PK tuples through the native BigQuery client.

    A query, not ``Table.num_rows`` / catalog stats. Missing table is ``[]``.
    """
    from google.api_core.exceptions import NotFound

    from connectors.bigquery_conn import get_client
    from connectors.sql_identifiers import quote_sql_identifier

    project = str(cfg.get("database") or cfg.get("project_id") or "")
    dataset = str(schema or cfg.get("schema") or cfg.get("dataset") or "")
    if not project or not dataset:
        return None
    try:
        client = get_client(
            project_id=project,
            credentials_path=str(cfg.get("connection_string") or ""),
            service_account=str(cfg.get("service_account") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
        )
        table_id = f"`{project}`.`{dataset}`.`{table_name}`"
        col_sql = ", ".join(quote_sql_identifier(c, "`") for c in cols)
        fetched = _bigquery_run_query(client, f"SELECT {col_sql} FROM {table_id}")  # nosec B608
    except NotFound:
        return []
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, "bigquery"):
            return []
        logger.warning("BigQuery dest key list failed: %s", exc)
        return None
    width = len(cols)
    out: list[tuple[Any, ...]] = []
    for row in fetched:
        try:
            tup = tuple(row[c] for c in cols)
        except Exception:
            tup = tuple(row[i] for i in range(width))
        if len(tup) != width or any(is_null_evidence(v) for v in tup):
            continue
        out.append(tup)
    return out


def _bigquery_key_hits(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    listed = _bigquery_key_list(cfg, schema=schema, table_name=table_name, cols=cols)
    if listed is None:
        return None
    wanted = {norm for key in keys if (norm := _norm_dest_key(key)) is not None}
    return sum(1 for tup in listed if _norm_dest_key(tup) in wanted)


def _bigquery_delete_keys(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: Sequence[str],
) -> int:
    """Hard-DELETE leftover PKs through the native BigQuery client."""
    from connectors.bigquery_conn import get_client
    from connectors.sql_identifiers import quote_sql_identifier
    from services.row_conservation import parse_delete_keys

    leftover = parse_delete_keys(list(keys), len(cols))
    if not leftover:
        return 0
    project = str(cfg.get("database") or cfg.get("project_id") or "")
    dataset = str(schema or cfg.get("schema") or cfg.get("dataset") or "")
    if not project or not dataset:
        raise RuntimeError("BigQuery leftover MERGE needs project and dataset")
    client = get_client(
        project_id=project,
        credentials_path=str(cfg.get("connection_string") or ""),
        service_account=str(cfg.get("service_account") or ""),
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 0),
    )
    table_id = f"`{project}`.`{dataset}`.`{table_name}`"
    qcols = [quote_sql_identifier(c, "`") for c in cols]
    deleted = 0
    for tup in leftover:
        clause = " AND ".join(
            f"CAST({q} AS STRING) = '{str(part).replace(chr(39), chr(39) + chr(39))}'"
            for q, part in zip(qcols, tup)
        )
        job = _bigquery_run_job(client, f"DELETE FROM {table_id} WHERE {clause}")  # nosec B608
        n = getattr(job, "num_dml_affected_rows", None)
        deleted += 1 if n is None or int(n) < 0 else int(n)
    return deleted


def _clickhouse_row_count(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """Visible MergeTree population: ``COUNT(*) FROM table FINAL``.

    ``system.tables.total_rows`` is parts metadata. ReplacingMergeTree
    without FINAL overcounts at-least-once INSERT versions. This is dest
    This is dest COUNT. Leftover MERGE lists the same FINAL population
    then ALTER DELETE + ``SYSTEM WAIT MUTATIONS``.
    """
    import sqlalchemy as sa
    from connectors.generic_sql import clickhouse_final_table_sql
    from connectors.sql_identifiers import quote_table_ref

    sch = str(schema or cfg.get("schema") or "").strip() or None
    table_ref = clickhouse_final_table_sql(
        quote_table_ref(table_name, sch, dialect="clickhouse")
    )
    try:
        with _warehouse_sql_engine("clickhouse", cfg) as engine:
            with engine.connect() as conn:
                n = conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
        return int(n or 0)
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, "clickhouse"):
            return 0
        logger.warning("ClickHouse dest COUNT(*) FINAL failed: %s", exc)
        return None


def _clickhouse_key_list(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """PK tuples of ``COUNT(*) FROM table FINAL`` — leftover MERGE listing.

    SELECT without FINAL overcounts ReplacingMergeTree versions and would
    disagree with dest COUNT, so leftover MERGE would refuse (unique ≠ n).
    Mutations stay unapplied until ``SYSTEM WAIT MUTATIONS`` in the deleter.
    """
    import sqlalchemy as sa
    from connectors.generic_sql import clickhouse_final_table_sql
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref

    sch = str(schema or cfg.get("schema") or "").strip() or None
    table_ref = clickhouse_final_table_sql(
        quote_table_ref(table_name, sch, dialect="clickhouse")
    )
    col_sql = ", ".join(quote_sql_identifier(c, "`") for c in cols)
    sql = f"SELECT {col_sql} FROM {table_ref}"  # nosec B608
    try:
        with _warehouse_sql_engine("clickhouse", cfg) as engine:
            with engine.connect() as conn:
                rows = conn.execute(sa.text(sql)).fetchall() or []
    except Exception as exc:
        if _is_missing_warehouse_relation(exc, "clickhouse"):
            return []
        logger.warning("ClickHouse dest key list FINAL failed: %s", exc)
        return None
    out: list[tuple[Any, ...]] = []
    width = len(cols)
    for row in rows:
        tup = tuple(row[:width])
        if len(tup) != width or any(v is None for v in tup):
            continue
        out.append(tup)
    return out


def _mongodb_key_list(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """Collection PK tuples. Exact find projection — never estimatedCount."""
    from pymongo import MongoClient

    from src.transfer.adapters import mongodb_connection_string

    database = str(cfg.get("database") or "")
    if not database or not table_name:
        return None
    client: MongoClient = MongoClient(
        mongodb_connection_string(dict(cfg)), serverSelectionTimeoutMS=5000
    )
    try:
        coll = client[database][table_name]
        projection = {c: 1 for c in cols}
        if "_id" not in projection:
            projection["_id"] = 0
        out: list[tuple[Any, ...]] = []
        width = len(cols)
        for doc in coll.find({}, projection):
            if not isinstance(doc, Mapping):
                continue
            tup = tuple(doc.get(c) for c in cols)
            if len(tup) != width or any(v is None for v in tup):
                continue
            out.append(tup)
        return out
    except Exception as exc:
        logger.warning("Mongo dest key list failed: %s", exc)
        return None
    finally:
        client.close()


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
                from services.dest_key_typing import (
                    coerce_key_tuples,
                    key_column_types_sqlalchemy,
                )

                keys, dropped = coerce_key_tuples(
                    keys,
                    cols,
                    key_column_types_sqlalchemy(
                        conn, schema=schema, table_name=table_name
                    ),
                )
                if dropped:
                    logger.info(
                        "dest key census: %d key(s) not representable in %s key column type(s)",
                        dropped,
                        dialect,
                    )
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
        with closing(sqlite3.connect(database)) as conn:
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
        with closing(sqlite3.connect(database)) as conn:
            total = _sum_distinct_hits(
                conn, table_ref, col_sql, cols, keys, ph, dialect=dialect, schema=schema,
                table_name=table_name,
            )
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
            return _sum_distinct_hits(
                conn, table_ref, col_sql, cols, keys, ph, dialect=dialect, schema=schema,
                table_name=table_name,
            )
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
            return _sum_distinct_hits(
                conn, table_ref, col_sql, cols, keys, ph, dialect=dialect, schema=schema,
                table_name=table_name,
            )
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
    *,
    dialect: str = "",
    schema: str = "",
    table_name: str = "",
) -> int:
    from services.dest_key_typing import coerce_key_tuples, key_column_types_dbapi

    col_types = key_column_types_dbapi(
        conn,
        dialect=dialect,
        schema=schema,
        table_name=table_name,
        placeholder=ph,
    )
    keys, dropped = coerce_key_tuples(keys, cols, col_types)
    if dropped:
        # Values the destination column cannot represent are misses, not errors.
        logger.info(
            "dest key census: %d key(s) not representable in %s key column type(s)",
            dropped,
            dialect or "destination",
        )
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


def _iceberg_dest_layout(endpoint: dict[str, Any]) -> str | None:
    """catalog vs filesystem for dest COUNT / key list.

    WRITE uses ``resolve_iceberg_write_path`` and fail-closes when the
    catalog driver is missing — that must not invent a local warehouse.
    COUNT still inspects a catalog snapshot when ``load_catalog`` is
    available (including test fakes). Missing driver then fails inside
    ``load_catalog`` → unmeasured, not a silent filesystem tree.
    """
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    try:
        parsed = parse_iceberg_catalog_config(endpoint)
    except Exception:
        return None
    catalog_type = str(parsed.get("catalog_type") or "filesystem").lower()
    if catalog_type == "filesystem":
        return "filesystem"
    # REST / Glue / Hive / Hadoop / SQL: leftover MERGE lists the catalog
    # snapshot. Hadoop is not a silent filesystem tree — that invented dest
    # COUNT while writes fail-closed (pyiceberg 0.11 has no HadoopCatalog).
    return "catalog"


def _iceberg_row_count(
    cfg: dict[str, Any], *, schema: str, table_name: str
) -> int | None:
    """Dest COUNT of current-snapshot data files. Never ``scan().count()``.

    Spark ``COUNT(*)`` on Iceberg uses manifest ``record-count`` and bails
    to a full row scan when delete files exist. Manifest ``record-count``
    is writer-stamped — the same honesty hole as ``sys.partitions``. Dest
    COUNT opens each live data file and uses the dest-engine population
    already proven for artifacts (Parquet footer ``num_rows``, JSONL
    object-per-line). Catalog object-store URIs use the same Range-GET
    footer kernel as S3/GCS/ADLS dest.     A listed data-file that is gone
    is unmeasured, not dest=0. Filesystem MoR applies Iceberg v2 position
    and equality deletes (unique ``(file_path, pos)``; equality AND plus
    ``data_seq < delete_seq``). V3 deletion vectors apply the same
    unique in-range pos kernel from the Puffin roaring blob. ``data_footer
    − delete_record_count`` is never dest (Iceberg #14864). Missing
    ``content_offset`` / unreadable puffin stays unmeasured. Missing
    delete file is unmeasured. Missing table is 0.
    Unreadable snapshot is ``None``. Key list / leftover MERGE project PK
    columns from the same snapshot population as COUNT. Never
    ``scan().to_arrow()``.
    """
    endpoint = _iceberg_endpoint(cfg, table_name, schema)
    layout = _iceberg_dest_layout(endpoint)
    if layout is None:
        return None
    if layout == "catalog":
        return _iceberg_catalog_file_count(endpoint)
    return _iceberg_filesystem_file_count(cfg, schema=schema, table_name=table_name)


def _iceberg_data_warehouse(endpoint: dict[str, Any], parsed: dict[str, Any]) -> str:
    """Warehouse URI for joining relative snapshot paths.

    SqlCatalog ``properties.warehouse`` may be a local Path invented from
    ``s3://`` by ``_warehouse_root``. Join relative ``file_path`` values
    against the endpoint warehouse URI instead, so COUNT can Range-GET.
    """
    from services.object_streaming import parse_object_store_uri

    for raw in (
        str(endpoint.get("warehouse") or "").strip(),
        str(endpoint.get("database") or "").strip(),
        str(parsed.get("warehouse") or "").strip(),
    ):
        if not raw:
            continue
        lower = raw.lower()
        if lower.startswith(
            (
                "s3://",
                "s3a://",
                "s3n://",
                "gs://",
                "gcs://",
                "abfs://",
                "abfss://",
                "wasb://",
                "wasbs://",
                "hdfs://",
                "webhdfs://",
                "swebhdfs://",
            )
        ):
            return raw.rstrip("/")
        if parse_object_store_uri(raw) is not None:
            return raw.rstrip("/")
        if raw.startswith("file:") or "://" not in raw:
            return raw
    return str(parsed.get("warehouse") or "").strip()


def _count_iceberg_data_file(path: Path) -> int | None:
    """Dest-engine population of one snapshot data file. Never manifest record-count."""
    return count_artifact_rows(path)


def _resolve_iceberg_data_uri(uri: str, warehouse: str) -> str:
    """Join a relative snapshot path onto the warehouse URI. Absolute URIs stay."""
    raw = str(uri or "").strip()
    if not raw:
        return ""
    if "://" in raw or raw.startswith("file:"):
        return raw
    if raw.startswith("/") or raw.startswith("\\\\"):
        return raw
    if len(raw) >= 3 and raw[1] == ":" and raw[0].isalpha():
        return raw
    root = str(warehouse or "").strip().rstrip("/")
    if not root:
        return raw
    return f"{root}/{raw.lstrip('/')}"


def _iceberg_object_store_cfg(
    endpoint: dict[str, Any], location: Any
) -> dict[str, Any]:
    """Map Iceberg catalog extra (``s3.*`` / ``gcs.*`` / ``adls.*``) onto GET cfg."""
    extra = endpoint.get("extra")
    extra = extra if isinstance(extra, dict) else {}
    cfg = dict(endpoint)
    kind = str(getattr(location, "kind", "") or "")
    if kind == "s3":
        cfg["username"] = str(
            extra.get("s3.access-key-id")
            or extra.get("s3.access-key")
            or extra.get("client.access-key-id")
            or cfg.get("username")
            or ""
        ).strip()
        cfg["password"] = str(
            extra.get("s3.secret-access-key")
            or extra.get("s3.secret-key")
            or extra.get("client.secret-access-key")
            or cfg.get("password")
            or ""
        ).strip()
        cfg["host"] = str(
            extra.get("s3.region")
            or extra.get("client.region")
            or extra.get("region")
            or cfg.get("host")
            or "us-east-1"
        ).strip()
        endpoint_url = str(
            extra.get("s3.endpoint") or extra.get("s3.endpoint-url") or ""
        ).strip()
        if endpoint_url.startswith(("http://", "https://")):
            cfg["endpoint_url"] = endpoint_url.rstrip("/")
            cfg["connection_string"] = cfg["endpoint_url"]
        else:
            cs = str(cfg.get("connection_string") or "")
            if not cs.startswith(("http://", "https://")):
                cfg["connection_string"] = ""
        path_style = extra.get("s3.path-style-access")
        if path_style in (True, "true", "True", 1, "1"):
            cfg["path_style"] = True
        return cfg
    if kind == "gcs":
        project = str(
            extra.get("gcs.project-id") or extra.get("gcs.project") or cfg.get("host") or ""
        ).strip()
        if project:
            cfg["host"] = project
        creds = str(
            extra.get("gcs.credentials")
            or extra.get("gcs.oauth2.token")
            or cfg.get("service_account")
            or cfg.get("password")
            or ""
        ).strip()
        if creds:
            cfg["service_account"] = creds
        return cfg
    if kind == "adls":
        account = str(
            extra.get("adls.account-name")
            or extra.get("azure.account-name")
            or getattr(location, "account", "")
            or cfg.get("account_name")
            or cfg.get("username")
            or ""
        ).strip()
        if account:
            cfg["username"] = account
            cfg["account_name"] = account
        key = str(
            extra.get("adls.account-key")
            or extra.get("azure.account-key")
            or cfg.get("account_key")
            or cfg.get("password")
            or ""
        ).strip()
        if key:
            cfg["password"] = key
            cfg["account_key"] = key
        conn = str(
            extra.get("adls.connection-string") or extra.get("azure.connection-string") or ""
        ).strip()
        if conn:
            cfg["connection_string"] = conn
        elif not str(cfg.get("connection_string") or "").startswith(("http://", "https://")):
            if "AccountName" not in str(cfg.get("connection_string") or ""):
                cfg["connection_string"] = ""
        return cfg
    if kind == "hdfs":
        endpoint = str(
            extra.get("webhdfs.endpoint")
            or extra.get("hdfs.webhdfs")
            or extra.get("hdfs.webhdfs.endpoint")
            or extra.get("webhdfs_endpoint")
            or cfg.get("webhdfs_endpoint")
            or ""
        ).strip()
        if endpoint:
            cfg["webhdfs_endpoint"] = endpoint.rstrip("/")
        user = str(
            extra.get("webhdfs.user")
            or extra.get("hdfs.user")
            or extra.get("webhdfs_user")
            or cfg.get("username")
            or ""
        ).strip()
        if user:
            cfg["webhdfs_user"] = user
        cfg["hdfs_scheme"] = str(getattr(location, "account", "") or "")
        return cfg
    return cfg


def _count_iceberg_data_uri(
    uri: str, *, endpoint: dict[str, Any], warehouse: str = ""
) -> int | None:
    """Dest-engine COUNT of one snapshot data-file URI. Missing remote is unmeasured."""
    from services.object_streaming import parse_object_store_uri

    local = _iceberg_local_path(str(uri or ""), warehouse=warehouse)
    if local is not None:
        return _count_iceberg_data_file(local)
    resolved = _resolve_iceberg_data_uri(uri, warehouse)
    if resolved != str(uri or "").strip():
        local = _iceberg_local_path(resolved, warehouse=warehouse)
        if local is not None:
            return _count_iceberg_data_file(local)
    loc = parse_object_store_uri(resolved)
    if loc is None:
        logger.info("iceberg catalog data-file not a local path or object URI: %s", uri)
        return None
    store_cfg = _iceberg_object_store_cfg(endpoint, loc)
    return _count_object_store_key(
        loc.kind, store_cfg, loc.bucket, loc.key, missing=None
    )


def _iceberg_local_path(uri: str, *, warehouse: str = "") -> Path | None:
    from connectors.iceberg_catalog import local_path_from_location

    raw = str(uri or "").strip()
    if not raw:
        return None
    candidates = [local_path_from_location(raw)]
    root = str(warehouse or "").strip()
    if root:
        candidates.append(local_path_from_location(root) / raw)
    for path in candidates:
        if path.is_file():
            return path
    return None


def _iceberg_filesystem_file_count(
    cfg: dict[str, Any], *, schema: str, table_name: str
) -> int | None:
    from connectors.iceberg_writer import (
        _load_metadata,
        _resolve_iceberg_table_dir,
        snapshot_data_files,
        snapshot_has_delete_files,
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
    try:
        files = snapshot_data_files(table_dir, current_meta)
    except ValueError as exc:
        logger.info("iceberg snapshot data files unreadable: %s", exc)
        return None
    if snapshot_has_delete_files(current_meta):
        from connectors.iceberg_mor import filesystem_mor_count

        return filesystem_mor_count(
            table_dir,
            current_meta,
            files,
            count_data_file=_mor_count_local,
            project_file=_mor_project_local,
        )
    total = 0
    for _rel, path in files:
        n = _count_iceberg_data_file(path)
        if n is None:
            return None
        total += n
    return total


def _iceberg_catalog_snapshot(
    endpoint: dict[str, Any],
) -> tuple[list[str], dict[str, Any] | None] | None:
    """Current snapshot data URIs plus optional MoR metadata.

    ``None`` is unmeasured. ``([], None)`` is empty or missing table.
    Delete files without a readable ``file_path`` column stay unmeasured
    (catalog fake that only stamps ``num_rows``). Remote delete parquet
    is applied only after the caller resolves local paths; inspect
    equality deletes without ``entries()`` sequence numbers stay
    unmeasured in ``iceberg_mor``. Never ``scan().to_arrow()``.
    """
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config
    from connectors.iceberg_mor import inspect_delete_refs, inspect_sequence_by_path

    parsed = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = parsed["namespace"] + (parsed["table_name"],)
    try:
        tbl = catalog.load_table(identifier)
    except Exception as exc:
        if _iceberg_missing_table(exc):
            return [], None
        raise
    try:
        inspect = tbl.inspect
        deletes = inspect.delete_files()
        data_files = inspect.data_files()
    except Exception as exc:
        logger.info("iceberg catalog file listing failed: %s", exc)
        return None
    delete_refs = inspect_delete_refs(deletes)
    if delete_refs is None:
        logger.info("iceberg catalog delete files present; dest COUNT unmeasured")
        return None
    n_files = int(getattr(data_files, "num_rows", 0) or 0)
    if n_files == 0:
        return [], None
    try:
        uris = [str(p) for p in data_files.column("file_path").to_pylist()]
    except Exception as exc:
        logger.info("iceberg catalog file_path listing failed: %s", exc)
        return None
    if not delete_refs:
        return uris, None
    seq_by_path: dict[str, int] = {}
    try:
        seq_by_path = inspect_sequence_by_path(inspect.entries())
    except Exception:
        seq_by_path = {}
    schema_fields: list[dict[str, Any]] = []
    try:
        schema = tbl.schema()
        schema_fields = [
            {"id": int(getattr(f, "field_id", 0) or 0), "name": str(getattr(f, "name", "") or "")}
            for f in (getattr(schema, "fields", None) or [])
            if getattr(f, "name", None)
        ]
    except Exception:
        schema_fields = []
    data_meta = []
    for uri in uris:
        ref: dict[str, Any] = {"path": uri}
        if uri in seq_by_path:
            ref["sequence-number"] = seq_by_path[uri]
        data_meta.append(ref)
    for ref in delete_refs:
        path = str(ref.get("path") or "")
        if path in seq_by_path:
            ref["sequence-number"] = seq_by_path[path]
    meta = {
        "data-files": data_meta,
        "delete-files": delete_refs,
        "schema": {"fields": schema_fields},
        "schemas": [{"fields": schema_fields}],
    }
    return uris, meta


def _mor_count_local(_rel: str, path: Path | None) -> int | None:
    if path is None:
        return None
    return _count_iceberg_data_file(path)


def _mor_project_local(
    _rel: str, path: Path | None, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    if path is None:
        return None
    return _project_iceberg_local(path, cols)


def _mor_count_catalog(
    rel: str,
    path: Path | None,
    *,
    endpoint: dict[str, Any],
    warehouse: str,
) -> int | None:
    if path is not None:
        return _count_iceberg_data_file(path)
    return _count_iceberg_data_uri(rel, endpoint=endpoint, warehouse=warehouse)


def _mor_project_catalog(
    rel: str,
    path: Path | None,
    cols: Sequence[str],
    *,
    endpoint: dict[str, Any],
    warehouse: str,
) -> list[dict[str, Any]] | None:
    if path is not None:
        return _project_iceberg_local(path, cols)
    return _iceberg_project_data_uri(
        rel, endpoint=endpoint, warehouse=warehouse, cols=cols
    )


def _iceberg_catalog_data_files(
    uris: Sequence[str], *, warehouse: str
) -> list[tuple[str, Path | None]]:
    files: list[tuple[str, Path | None]] = []
    for uri in uris:
        local = _iceberg_local_path(str(uri), warehouse=warehouse)
        if local is None:
            resolved = _resolve_iceberg_data_uri(str(uri), warehouse)
            if resolved != str(uri):
                local = _iceberg_local_path(resolved, warehouse=warehouse)
        files.append((str(uri), local))
    return files


def _iceberg_delete_spool_suffix(uri: str) -> str:
    lowered = str(uri or "").lower()
    if lowered.endswith(".puffin.gz"):
        return ".puffin.gz"
    if lowered.endswith(".puffin"):
        return ".puffin"
    return ".parquet"


def _spool_iceberg_delete_range(
    uri: str,
    *,
    endpoint: dict[str, Any],
    warehouse: str,
    offset: int,
    size: int,
) -> Path | None:
    """Range-GET one deletion-vector blob. Never unsized Body.read() of the puffin."""
    from services.object_streaming import (
        parse_object_store_uri,
        range_get_object_bytes,
    )

    raw = str(uri or "").strip()
    resolved = _resolve_iceberg_data_uri(raw, warehouse)
    loc = parse_object_store_uri(resolved)
    if loc is None:
        logger.info("iceberg catalog deletion-vector not an object URI: %s", uri)
        return None
    store_cfg = _iceberg_object_store_cfg(endpoint, loc)
    try:
        blob = range_get_object_bytes(
            loc.kind, store_cfg, loc.bucket, loc.key, int(offset), int(size)
        )
    except Exception as exc:
        logger.info("iceberg catalog deletion-vector Range GET failed: %s", exc)
        return None
    if not blob or len(blob) != int(size):
        logger.info("iceberg catalog deletion-vector Range GET short read: %s", uri)
        return None
    tmp_path: Path | None = None
    try:
        tmp = tempfile.NamedTemporaryFile(
            prefix="df-iceberg-del-",
            suffix=_iceberg_delete_spool_suffix(raw),
            delete=False,
        )
        tmp_path = Path(tmp.name)
        tmp.write(blob)
        tmp.flush()
        tmp.close()
        return tmp_path
    except Exception as exc:
        logger.info("iceberg catalog deletion-vector spool failed: %s", exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
        return None


def _spool_iceberg_delete_uri(
    uri: str, *, endpoint: dict[str, Any], warehouse: str
) -> Path | None:
    """Local path of a delete parquet/puffin. Object-store URIs GET into a temp file."""
    from services.object_streaming import (
        open_object_store_binary,
        parse_object_store_uri,
    )

    raw = str(uri or "").strip()
    local = _iceberg_local_path(raw, warehouse=warehouse)
    if local is not None:
        return local
    resolved = _resolve_iceberg_data_uri(raw, warehouse)
    if resolved != raw:
        local = _iceberg_local_path(resolved, warehouse=warehouse)
        if local is not None:
            return local
    loc = parse_object_store_uri(resolved)
    if loc is None:
        logger.info("iceberg catalog delete file not local or object URI: %s", uri)
        return None
    store_cfg = _iceberg_object_store_cfg(endpoint, loc)
    opened = open_object_store_binary(loc.kind, store_cfg, loc.bucket, loc.key)
    if opened is False or opened is None:
        logger.info("iceberg catalog delete file GET unmeasured: %s", uri)
        return None
    handle, closer = opened
    tmp_path: Path | None = None
    try:
        tmp = tempfile.NamedTemporaryFile(
            prefix="df-iceberg-del-",
            suffix=_iceberg_delete_spool_suffix(raw),
            delete=False,
        )
        tmp_path = Path(tmp.name)
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            tmp.write(chunk)
        tmp.flush()
        tmp.close()
        return tmp_path
    except Exception as exc:
        logger.info("iceberg catalog delete file spool failed: %s", exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
        return None
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _prepare_catalog_mor(
    uris: Sequence[str],
    mor_meta: dict[str, Any],
    *,
    endpoint: dict[str, Any],
    warehouse: str,
) -> tuple[list[tuple[str, Path | None]], dict[str, Any], list[Path]] | None:
    """Resolve data URIs and spool remote delete parquet. Temps must be unlinked."""
    files = _iceberg_catalog_data_files(uris, warehouse=warehouse)
    rewritten = dict(mor_meta)
    temps: list[Path] = []
    refs: list[dict[str, Any]] = []
    from connectors.iceberg_deletion_vector import (
        IcebergDeletionVectorError,
        deletion_vector_byte_range,
    )
    from connectors.iceberg_mor import is_deletion_vector_ref

    for ref in list(mor_meta.get("delete-files") or []):
        if not isinstance(ref, dict):
            return None
        raw = str(ref.get("path") or ref.get("file_path") or "").strip()
        is_dv = is_deletion_vector_ref(ref, raw)
        byte_range = None
        if is_dv:
            try:
                byte_range = deletion_vector_byte_range(ref)
            except IcebergDeletionVectorError:
                return None
        local = _iceberg_local_path(raw, warehouse=warehouse)
        if local is None:
            resolved = _resolve_iceberg_data_uri(raw, warehouse)
            if resolved != raw:
                local = _iceberg_local_path(resolved, warehouse=warehouse)
        if local is not None:
            cloned = dict(ref)
            cloned["path"] = str(local)
            refs.append(cloned)
            continue
        if is_dv and byte_range is not None:
            path = _spool_iceberg_delete_range(
                raw,
                endpoint=endpoint,
                warehouse=warehouse,
                offset=byte_range[0],
                size=byte_range[1],
            )
            if path is None:
                for tmp in temps:
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass
                return None
            cloned = dict(ref)
            cloned["path"] = str(path)
            cloned["content_offset"] = 0
            cloned["content-offset"] = 0
            cloned["content_size_in_bytes"] = byte_range[1]
            refs.append(cloned)
            temps.append(path)
            continue
        path = _spool_iceberg_delete_uri(raw, endpoint=endpoint, warehouse=warehouse)
        if path is None:
            for tmp in temps:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
            return None
        cloned = dict(ref)
        cloned["path"] = str(path)
        refs.append(cloned)
        if path.name.startswith("df-iceberg-del-"):
            temps.append(path)
    rewritten["delete-files"] = refs
    return files, rewritten, temps


def _parquet_column_names(pf: Any) -> set[str]:
    schema = getattr(pf, "schema_arrow", None) or getattr(pf, "schema", None)
    names = getattr(schema, "names", None) or ()
    return {str(n) for n in names}


def _resolve_projected_names(
    available: set[str], wanted: Sequence[str]
) -> list[tuple[str, str]] | None:
    """Map requested PK names onto file schema names.

    Composite leftover MERGE needs every PK part. A partial projection
    (one of two columns, or a case-folded collision) is unmeasured —
    not a truncated key list that looks like dest has no leftovers.
    """
    if not wanted:
        return []
    lower: dict[str, str] = {}
    for name in available:
        lower.setdefault(name.lower(), name)
    mapping: list[tuple[str, str]] = []
    used: set[str] = set()
    for want in wanted:
        actual: str | None = want if want in available else lower.get(want.lower())
        if actual is None or actual in used:
            return None
        mapping.append((str(want), actual))
        used.add(actual)
    return mapping


def _project_records(
    records: list[dict[str, Any]], wanted: Sequence[str]
) -> list[dict[str, Any]] | None:
    out: list[dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            return None
        resolved = _resolve_projected_names({str(k) for k in rec}, wanted)
        if resolved is None:
            return None
        out.append({want: rec.get(actual) for want, actual in resolved})
    return out


def _project_parquet_columns(source: Any, cols: Sequence[str]) -> list[dict[str, Any]] | None:
    """PK (or requested) columns of one Parquet image. Never the full table scan."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    wanted = [str(c) for c in cols if str(c).strip()]
    if not wanted:
        return []
    try:
        pf = pq.ParquetFile(_seek_start(source))
        resolved = _resolve_projected_names(_parquet_column_names(pf), wanted)
        if resolved is None:
            return None
        table = pf.read(columns=[actual for _, actual in resolved])
    except UnmeasuredArtifact:
        return None
    except Exception as exc:
        logger.info("iceberg parquet PK projection failed: %s", exc)
        return None
    pylist = table.to_pylist()
    out: list[dict[str, Any]] = []
    for rec in pylist:
        if not isinstance(rec, dict):
            return None
        out.append({want: rec.get(actual) for want, actual in resolved})
    return out


def _project_orc_columns(source: Any, cols: Sequence[str]) -> list[dict[str, Any]] | None:
    try:
        from pyarrow import orc
    except ImportError:
        return None
    wanted = [str(c) for c in cols if str(c).strip()]
    if not wanted:
        return []
    try:
        reader = orc.ORCFile(_seek_start(source))
        names = set(str(n) for n in (getattr(reader.schema, "names", None) or ()))
        resolved = _resolve_projected_names(names, wanted)
        if resolved is None:
            return None
        table = reader.read(columns=[actual for _, actual in resolved])
    except UnmeasuredArtifact:
        return None
    except Exception as exc:
        logger.info("iceberg ORC PK projection failed: %s", exc)
        return None
    pylist = table.to_pylist()
    out: list[dict[str, Any]] = []
    for rec in pylist:
        if not isinstance(rec, dict):
            return None
        out.append({want: rec.get(actual) for want, actual in resolved})
    return out


def _project_iceberg_handle(
    source: Any, *, name: str, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    """Project requested columns from one snapshot data file. None if unreadable."""
    wanted = [str(c) for c in cols if str(c).strip()]
    kind, handle, gz_close = _artifact_stream_open(source, name=name)
    try:
        if kind in {"parquet", "orc"}:
            image, spool_close = (
                rewindable_byte_source(handle) if gz_close is not None else (handle, None)
            )
            try:
                if kind == "parquet":
                    return _project_parquet_columns(image, wanted)
                return _project_orc_columns(image, wanted)
            finally:
                if spool_close is not None:
                    try:
                        spool_close()
                    except Exception:
                        pass
        if kind in {"jsonl", "json"}:
            records: list[dict[str, Any]] = []
            for rec in _iter_streaming_kind(kind, handle, name=name):
                if not isinstance(rec, dict):
                    return None
                records.append(rec)
            return _project_records(records, wanted)
        logger.info("iceberg snapshot file format unprojected for keys: %s", name)
        return None
    except UnmeasuredArtifact as exc:
        logger.info("iceberg snapshot PK projection unmeasured (%s): %s", name, exc)
        return None
    except Exception as exc:
        logger.info("iceberg snapshot PK projection failed (%s): %s", name, exc)
        return None
    finally:
        if gz_close is not None:
            try:
                gz_close()
            except Exception:
                pass


def _project_iceberg_local(
    file_path: Path, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    """Project columns from one local snapshot data file."""
    handle, closer = open_artifact_binary(file_path)
    try:
        return _project_iceberg_handle(handle, name=file_path.name, cols=cols)
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _iceberg_project_data_uri(
    uri: str, *, endpoint: dict[str, Any], warehouse: str, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    """PK columns of one catalog data-file URI. Missing remote is unmeasured."""
    from services.object_streaming import (
        open_object_store_binary,
        open_object_store_seekable,
        parse_object_store_uri,
    )

    raw = str(uri or "").strip()
    local = _iceberg_local_path(raw, warehouse=warehouse)
    resolved = _resolve_iceberg_data_uri(raw, warehouse)
    if local is None and resolved != raw:
        local = _iceberg_local_path(resolved, warehouse=warehouse)
    if local is not None:
        handle, closer = open_artifact_binary(local)
        try:
            return _project_iceberg_handle(handle, name=local.name, cols=cols)
        finally:
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass
    loc = parse_object_store_uri(resolved)
    if loc is None:
        logger.info("iceberg catalog data-file not a local path or object URI: %s", uri)
        return None
    store_cfg = _iceberg_object_store_cfg(endpoint, loc)
    footer_kind = _object_store_footer_kind(loc.key)
    opened: Any
    if footer_kind is not None:
        opened = open_object_store_seekable(loc.kind, store_cfg, loc.bucket, loc.key)
        if opened is False:
            return None
        if opened is not None:
            stream, closer = opened
            try:
                return _project_iceberg_handle(stream, name=loc.key, cols=cols)
            finally:
                if closer is not None:
                    try:
                        closer()
                    except Exception:
                        pass
    opened = open_object_store_binary(loc.kind, store_cfg, loc.bucket, loc.key)
    if opened is False or opened is None:
        return None
    stream, closer = opened
    try:
        return _project_iceberg_handle(stream, name=loc.key, cols=cols)
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _iceberg_catalog_file_count(endpoint: dict[str, Any]) -> int | None:
    """Sql/REST catalog COUNT via live data-file footers. Never ``scan().count()``.

    Local ``file:`` URIs footer from disk. ``s3://`` / ``gs://`` / ``abfss://``
    / ``hdfs://`` (WebHDFS when ``webhdfs.endpoint`` is set) use the
    object-store Range kernel. Missing remote files are unmeasured.
    ``hdfs://`` without a WebHDFS HTTP endpoint stays unmeasured.
    """
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    snap = _iceberg_catalog_snapshot(endpoint)
    if snap is None:
        return None
    paths, mor_meta = snap
    if not paths:
        return 0
    parsed = parse_iceberg_catalog_config(endpoint)
    warehouse = _iceberg_data_warehouse(endpoint, parsed)
    if mor_meta:
        from connectors.iceberg_mor import filesystem_mor_count

        prepared = _prepare_catalog_mor(
            paths, mor_meta, endpoint=endpoint, warehouse=warehouse
        )
        if prepared is None:
            return None
        files, meta, temps = prepared
        try:
            root = Path(str(warehouse or "/"))
            return filesystem_mor_count(
                root,
                meta,
                files,
                count_data_file=lambda rel, path: _mor_count_catalog(
                    rel, path, endpoint=endpoint, warehouse=warehouse
                ),
                project_file=lambda rel, path, cols: _mor_project_catalog(
                    rel, path, cols, endpoint=endpoint, warehouse=warehouse
                ),
            )
        finally:
            for tmp in temps:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
    total = 0
    for uri in paths:
        n = _count_iceberg_data_uri(uri, endpoint=endpoint, warehouse=warehouse)
        if n is None:
            return None
        total += n
    return total


def _iceberg_snapshot_rows(
    cfg: dict[str, Any], *, schema: str, table_name: str, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    """Current snapshot PK (or requested) columns. Key list and leftover MERGE.

    Same data-file population as dest COUNT, including Iceberg v2/v3 MoR.
    Catalog path projects columns from live files — never
    ``scan().to_arrow()``. A missing snapshot file is unmeasured
    (``None``), not dest=0. Missing table is ``[]``. Metadata
    ``record-count`` is never dest population.
    """
    wanted = [str(c) for c in cols if str(c).strip()]
    endpoint = _iceberg_endpoint(cfg, table_name, schema)
    layout = _iceberg_dest_layout(endpoint)
    if layout is None:
        return None
    if layout == "catalog":
        from connectors.iceberg_catalog import parse_iceberg_catalog_config

        snap = _iceberg_catalog_snapshot(endpoint)
        if snap is None:
            return None
        uris, mor_meta = snap
        if not uris:
            return []
        parsed = parse_iceberg_catalog_config(endpoint)
        warehouse = _iceberg_data_warehouse(endpoint, parsed)
        if mor_meta:
            from connectors.iceberg_mor import filesystem_mor_snapshot_rows

            prepared = _prepare_catalog_mor(
                uris, mor_meta, endpoint=endpoint, warehouse=warehouse
            )
            if prepared is None:
                return None
            files, meta, temps = prepared
            try:
                root = Path(str(warehouse or "/"))
                return filesystem_mor_snapshot_rows(
                    root,
                    meta,
                    files,
                    cols=wanted,
                    project_file=lambda rel, path, cols: _mor_project_catalog(
                        rel, path, cols, endpoint=endpoint, warehouse=warehouse
                    ),
                )
            finally:
                for tmp in temps:
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass
        rows: list[dict[str, Any]] = []
        for uri in uris:
            part = _iceberg_project_data_uri(
                uri, endpoint=endpoint, warehouse=warehouse, cols=wanted
            )
            if part is None:
                return None
            rows.extend(part)
        return rows
    from connectors.iceberg_writer import (
        _load_metadata,
        _resolve_iceberg_table_dir,
        snapshot_data_files,
        snapshot_has_delete_files,
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
    try:
        files = snapshot_data_files(table_dir, current_meta)
    except ValueError as exc:
        logger.info("iceberg snapshot data files unreadable: %s", exc)
        return None
    if snapshot_has_delete_files(current_meta):
        from connectors.iceberg_mor import filesystem_mor_snapshot_rows

        return filesystem_mor_snapshot_rows(
            table_dir,
            current_meta,
            files,
            cols=wanted,
            project_file=_mor_project_local,
        )
    rows = []
    for _rel, file_path in files:
        part = _project_iceberg_local(file_path, wanted)
        if part is None:
            return None
        rows.extend(part)
    return rows


def _norm_dest_key(values: Sequence[Any]) -> tuple[str, ...] | None:
    """Comparable dest key — JSONL strings and catalog ints must hit the same PK."""
    out: list[str] = []
    for value in values:
        key = present_cell_text(value)
        if key is None:
            return None
        out.append(key)
    return tuple(out)


def _row_values_for_cols(
    row: Mapping[str, Any], cols: Sequence[str]
) -> tuple[Any, ...] | None:
    """PK tuple from a projected row. Case-insensitive; incomplete identity is skip."""
    lower: dict[str, Any] | None = None
    parts: list[Any] = []
    for col in cols:
        if col in row:
            val = row[col]
        else:
            if lower is None:
                lower = {str(k).lower(): v for k, v in row.items()}
            val = lower.get(col.lower())
        if is_null_evidence(val):
            return None
        parts.append(val)
    return tuple(parts)


def _iceberg_key_list(
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """Current-snapshot PK tuples. Never metadata ``record-count``.

    Same population as dest COUNT(*) (``len`` of this listing), including
    Iceberg v2 MoR. Catalog ``scan().count()`` / ``scan().to_arrow()`` /
    metadata ``record-count`` never close. Missing table is ``[]``.
    Incomplete / unreadable snapshot is ``None``.
    """
    rows = _iceberg_snapshot_rows(cfg, schema=schema, table_name=table_name, cols=cols)
    if rows is None:
        return None
    out: list[tuple[Any, ...]] = []
    width = len(cols)
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        tup = _row_values_for_cols(row, cols)
        if tup is None or len(tup) != width:
            continue
        out.append(tup)
    return out


def iceberg_target_sample(
    cfg: Mapping[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: Sequence[str] | None,
    limit: int | None = 50,
    sort_key: str | None = None,
    key_values: Sequence[Any] | None = None,
) -> list[dict[str, Any]] | None:
    """Gate-8 sample from current-snapshot data files — never ``scan().to_arrow()``.

    Same files dest COUNT / leftover MERGE list. Unreadable snapshot is
    ``None`` (caller raises ``TargetSampleUnavailable``). Missing table is
    ``[]``. Filesystem MoR uses the same surviving-row population as COUNT.
    """
    cols = [str(c).strip() for c in (columns or []) if str(c).strip() and c != "*"]
    key_col = str(sort_key or "").strip()
    if key_col and key_col not in cols:
        cols = [key_col] + cols
    if not cols:
        return None
    rows = _iceberg_snapshot_rows(
        dict(cfg), schema=schema, table_name=table_name, cols=cols
    )
    if rows is None:
        return None
    if key_values and key_col:
        wanted = {
            key for k in key_values if (key := present_cell_text(k)) is not None
        }
        rows = [
            r for r in rows if present_cell_text(r.get(key_col)) in wanted
        ]
    if limit is not None and int(limit) > 0:
        rows = rows[: int(limit)]
    return list(rows)


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
        if not isinstance(row, Mapping):
            continue
        tup = _row_values_for_cols(row, cols)
        if tup is None:
            continue
        norm = _norm_dest_key(tup)
        if norm is not None and norm in wanted:
            seen.add(norm)
    return len(seen)


def _object_store_kind(db_type: str) -> str:
    """Catalog SKU → dest-engine family. ``amazon_s3`` / ``minio`` count as ``s3``."""
    from services.dialect_profiles import normalize_driver

    key = normalize_driver(db_type) or str(db_type or "").strip().lower()
    if key in {"s3", "amazon_s3", "aws_s3", "minio", "s3_compatible"}:
        return "s3"
    if key in {"gcs", "google_cloud_storage"}:
        return "gcs"
    if key in {
        "adls",
        "azure_blob_storage",
        "azure_data_lake",
        "azure_data_lake_storage",
    }:
        return "adls"
    return key


def _object_store_list_keys(
    kind: str, cfg: dict[str, Any], bucket: str, key: str
) -> list[str] | None:
    """Object keys for dest COUNT. ``[]`` is missing (measured zero). ``None`` is unknowable.

    Listing is not GET. COUNT opens one key at a time so a multi-part
    export is never held as N payloads in RAM.
    """
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
    return list(object_store_read_keys(base, listed))


def _object_store_footer_kind(obj_key: str) -> str | None:
    """Uncompressed Parquet/ORC — Range-GET the footer. Gzip is not this path."""
    name = str(obj_key or "")
    if name.lower().endswith(".gz"):
        return None
    kind = _infer_artifact_format(Path(name), None)
    if kind in {"parquet", "orc"}:
        return kind
    return None


def _count_object_store_key(
    store: str,
    cfg: dict[str, Any],
    bucket: str,
    obj_key: str,
    *,
    missing: int | None = 0,
) -> int | None:
    """COUNT of one listed key. Object-store dest missing is 0.

    Iceberg snapshot files that 404 are unmeasured (``missing=None``) —
    dest=0 would close overwrite on a truncated lakehouse snapshot.
    Uncompressed Parquet/ORC prefer ``open_object_store_seekable`` (footer
    Range GET). HEAD/Range setup failure falls back to sequential GET +
    spool — still correct. Gzip / Excel / CSV / JSON / Avro stay sequential.
    Gate-8 checksum never calls this.
    """
    from services.object_streaming import (
        open_object_store_binary,
        open_object_store_seekable,
    )

    footer_kind = _object_store_footer_kind(obj_key)
    if footer_kind is not None:
        opened = open_object_store_seekable(store, cfg, bucket, str(obj_key))
        if opened is False:
            return missing
        if opened is not None:
            stream, closer = opened
            try:
                if footer_kind == "parquet":
                    return _count_parquet_handle(stream)
                return _count_orc_handle(stream)
            except Exception as exc:
                logger.info(
                    "object-store Range footer COUNT failed for %s/%s: %s",
                    bucket,
                    obj_key,
                    exc,
                )
                return None
            finally:
                if closer is not None:
                    try:
                        closer()
                    except Exception:
                        pass
        # Range unavailable — sequential spool is still dest-engine COUNT.
    opened = open_object_store_binary(store, cfg, bucket, str(obj_key))
    if opened is False:
        return missing
    if opened is None:
        return None
    stream, closer = opened
    try:
        return _count_artifact_stream(stream, name=str(obj_key))
    except Exception as exc:
        logger.info(
            "object-store dest COUNT failed for %s/%s: %s", bucket, obj_key, exc
        )
        return None
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _object_store_row_count(
    db_type: str, cfg: dict[str, Any], *, table_name: str
) -> int | None:
    """Dest-engine artifact COUNT of GET streams. Missing object is 0.

    Same format machine as local ``count_artifact_rows``. One key is
    opened, counted, and closed before the next — never a list of GET
    bodies. Uncompressed Parquet/ORC Range-GET the footer; gzip and
    sequential kinds walk ``open_object_store_binary``. A truncated or
    unparseable part is unmeasured — never JSON-fallback empty, never
    the sum of a prefix. Writer PUT rowcount is not this proof.
    """
    bucket = str(cfg.get("database") or "").strip()
    key = str(table_name or "").strip()
    if not bucket or not key:
        return None
    kind = _object_store_kind(db_type)
    keys = _object_store_list_keys(kind, cfg, bucket, key)
    if keys is None:
        return None
    if not keys:
        return 0
    total = 0
    for obj_key in keys:
        n = _count_object_store_key(kind, cfg, bucket, str(obj_key))
        if n is None:
            logger.info(
                "object-store dest COUNT unmeasured for %s/%s", bucket, obj_key
            )
            return None
        total += n
    return total


def _redis_prefix_row_count(cfg: dict[str, Any], *, prefix: str) -> int | None:
    """Keys under ``prefix:*``, the cardinality the writer addresses.

    Redis is key-addressed, so the destination row count is the number of keys
    the writer's prefix owns — the same population Gate-8 reads back. Without
    this number an append or a quiet incremental poll had no pre-write count to
    subtract, and the reconcile refused to prove a correct run. An empty or
    absent prefix is 0 (a known-empty destination is a proof); an unreachable
    server stays ``None`` rather than substituting writer acknowledgement.
    """
    from connectors.redis_reader import _redis_client

    client = _redis_client(cfg)
    pattern = f"{prefix}:*" if prefix else "*"
    # SCAN guarantees each key at least once, not exactly once (a rehash during
    # the walk repeats slots), so the keys are de-duplicated before counting —
    # an inflated pre-count would understate the delta of the next append.
    seen: set[str] = set()
    cursor = 0
    while True:
        cursor, batch = client.scan(cursor=cursor, match=pattern, count=500)
        for raw in batch:
            seen.add(raw.decode() if isinstance(raw, bytes) else str(raw))
        if cursor == 0:
            return len(seen)


def _redis_key_hits(
    cfg: dict[str, Any],
    *,
    prefix: str,
    keys: list[tuple[Any, ...]],
) -> int | None:
    """How many of these identities Redis already holds under ``prefix:*``.

    Redis ``SET`` replaces the value at the key, so a re-written identity does
    not move the key count. The census needs the split (new keys vs replaced
    keys) to close, and it is only honest if the probed key is the exact key
    the writer addresses: ``connectors.redis_reader.redis_key_for`` on the
    ``|``-joined identity, which is the same rule
    ``redis_writer._resolve_redis_key_id`` writes with.
    """
    from connectors.redis_reader import _redis_client, redis_key_for
    from services.value_serializer import present_cell_text

    if not prefix:
        return None
    client = _redis_client(cfg)
    hits = 0
    chunk: list[str] = []
    for tup in keys:
        identity = "|".join(present_cell_text(part) or "" for part in tup)
        chunk.append(redis_key_for(prefix, identity))
        if len(chunk) >= 500:
            hits += _redis_exists_count(client, chunk)
            chunk = []
    if chunk:
        hits += _redis_exists_count(client, chunk)
    return hits


def _redis_exists_count(client: Any, keys: list[str]) -> int:
    pipe = client.pipeline()
    for key in keys:
        pipe.exists(key)
    return sum(1 for landed in pipe.execute() if landed)


def _dynamodb_item_count(cfg: dict[str, Any], *, table: str) -> int | None:
    """Exact item COUNT of a DynamoDB table, paged through ``Scan Select=COUNT``.

    ``DescribeTable.ItemCount`` is refreshed roughly every six hours, so it is a
    stale estimate and cannot answer "what did the destination hold *before*
    this write" — an append into Dynamo had no pre-count and the delta stayed
    unproven. This is the dest-engine analogue of ``COUNT(*)``. A missing table
    is 0 (create-on-first-write is a known-empty destination); an unnamed table
    or an unreachable endpoint stays ``None`` rather than substituting writer
    acknowledgement.
    """
    from botocore.exceptions import ClientError

    from connectors.aws_common import boto3_client

    table = (table or "").strip()
    if not table:
        return None
    client = boto3_client("dynamodb", cfg)
    total = 0
    start_key: dict[str, Any] | None = None
    while True:
        kwargs: dict[str, Any] = {"TableName": table, "Select": "COUNT"}
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        try:
            resp = client.scan(**kwargs)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return 0
            raise
        total += int(resp.get("Count") or 0)
        start_key = resp.get("LastEvaluatedKey") or None
        if not start_key:
            return total


def _dynamodb_key_hits(
    cfg: dict[str, Any],
    *,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    """How many of these keys DynamoDB already holds (``BatchGetItem``).

    ``PutItem`` replaces the item at the key, so a re-written key does not move
    the item count. The probe is only a proof when the census key columns are
    the table's own ``KeySchema``; anything else would compare a non-identity
    and is left unmeasured instead.
    """
    from botocore.exceptions import ClientError

    from connectors.aws_common import boto3_client
    from connectors.dynamodb_reader import describe_key_schema
    from connectors.dynamodb_writer import _coerce_dynamo_cell, _to_attr

    table = (table_name or "").strip()
    if not table or not cols:
        return None
    try:
        schema = describe_key_schema(cfg, table)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return 0
        raise
    key_names = [str(item.get("name") or "") for item in schema]
    if sorted(name.lower() for name in key_names) != sorted(
        col.lower() for col in cols
    ):
        return None
    # The key attribute must be serialized as the type the table declared: a
    # numeric HASH key probed as ``S`` matches nothing and would report every
    # replaced key as a new insert.
    declared = {
        str(item.get("name") or "").lower(): str(item.get("attr_type") or "VARCHAR")
        for item in schema
    }
    # The census has to build the key exactly the way the writer built it, or
    # ``BatchGetItem`` answers ``Type mismatch for key`` (a numeric-looking id
    # under an ``S`` key serializes as ``N``) and the whole probe reports
    # unmeasured. ``_coerce_dynamo_cell`` is that one canonical step.
    key_letters = {
        name: {"VARCHAR": "S", "DECIMAL": "N", "BINARY": "B"}.get(declared_type, "S")
        for name, declared_type in declared.items()
    }
    client = boto3_client("dynamodb", cfg)
    hits = 0
    pending: list[dict[str, Any]] = []
    for tup in keys:
        item = {
            name: _to_attr(
                _coerce_dynamo_cell(
                    value,
                    col=name,
                    logical_type=declared.get(name.lower(), "VARCHAR"),
                    key_types={name: key_letters.get(name.lower(), "S")},
                ),
                declared.get(name.lower(), "VARCHAR"),
            )
            for name, value in zip(cols, tup)
        }
        pending.append(item)
        if len(pending) >= 100:
            landed = _dynamodb_batch_get_count(client, table, pending, key_names)
            if landed is None:
                return None
            hits += landed
            pending = []
    if pending:
        landed = _dynamodb_batch_get_count(client, table, pending, key_names)
        if landed is None:
            return None
        hits += landed
    return hits


def _dynamodb_batch_get_count(
    client: Any,
    table: str,
    items: list[dict[str, Any]],
    key_names: list[str],
) -> int | None:
    """``BatchGetItem`` hit count. Unprocessed keys stay unmeasured, not 0."""
    request = {table: {"Keys": items, "ConsistentRead": True}}
    found = 0
    attempts = 0
    while request:
        resp = client.batch_get_item(RequestItems=request)
        found += len(resp.get("Responses", {}).get(table) or [])
        unprocessed = resp.get("UnprocessedKeys") or {}
        request = unprocessed if unprocessed.get(table) else {}
        attempts += 1
        if request and attempts >= 5:
            return None
    del key_names
    return found


def _search_index_doc_count(cfg: dict[str, Any], *, index: str) -> int | None:
    """Documents in the index through the cluster's own ``_count``.

    An absent index is 0 rather than unknowable, for the same reason a missing
    table is: the writer creates it, and refusing to count it left append and
    quiet-incremental runs without a pre-write number to subtract.
    """
    from connectors.elasticsearch_reader import _client

    client = _client(cfg)
    try:
        if not client.indices.exists(index=index):
            return 0
        client.indices.refresh(index=index)
        return int(client.count(index=index).get("count") or 0)
    finally:
        client.close()


def _redis_key_list(
    cfg: dict[str, Any],
    *,
    prefix: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """PK tuples from JSON docs under ``prefix:*``. Non-JSON keys stay unlisted."""
    from connectors.redis_reader import (
        _redis_client,
        load_redis_json_doc,
        resolve_key_pattern,
        scan_all_keys,
    )

    if not prefix or not cols:
        return None
    client = _redis_client(cfg)
    redis_keys = scan_all_keys(client, resolve_key_pattern(prefix))
    rows: list[tuple[Any, ...]] = []
    for key in redis_keys:
        doc = load_redis_json_doc(client.get(key))
        if not isinstance(doc, dict):
            return None
        tup = tuple(doc.get(col) for col in cols)
        if any(part is None for part in tup):
            return None
        rows.append(tup)
    return rows


def _redis_delete_keys(
    cfg: Mapping[str, Any],
    *,
    prefix: str,
    cols: list[str],
    keys: Sequence[str],
) -> int:
    """DEL leftover identities at the same ``prefix:identity`` the writer uses."""
    from connectors.redis_reader import _redis_client, redis_key_for
    from services.row_conservation import parse_delete_keys
    from services.value_serializer import present_cell_text

    leftover = parse_delete_keys(list(keys), len(cols))
    if not leftover or not prefix:
        return 0
    client = _redis_client(dict(cfg))
    to_del: list[str] = []
    for tup in leftover:
        identity = "|".join(present_cell_text(part) or "" for part in tup)
        to_del.append(redis_key_for(prefix, identity))
    if not to_del:
        return 0
    return int(client.delete(*to_del) or 0)


def _elasticsearch_doc_id(tup: tuple[Any, ...]) -> str:
    from services.value_serializer import present_cell_text

    parts = [present_cell_text(part) or "" for part in tup]
    return "|".join(parts) if len(parts) > 1 else (parts[0] if parts else "")


def _elasticsearch_key_list(
    cfg: dict[str, Any],
    *,
    index: str,
    cols: list[str],
    dest_n: int,
) -> list[tuple[Any, ...]] | None:
    """PK tuples from the index ``_source`` (or ``_id`` when that is the PK)."""
    from connectors.elasticsearch_reader import _client

    if not index or not cols:
        return None
    client = _client(cfg)
    try:
        if not client.indices.exists(index=index):
            return []
        client.indices.refresh(index=index)
        size = max(int(dest_n or 0), 0)
        if size <= 0:
            return []
        resp = client.search(
            index=index,
            body={
                "query": {"match_all": {}},
                "_source": cols,
                "size": size,
            },
        )
        hits = (resp.get("hits") or {}).get("hits") or []
        rows: list[tuple[Any, ...]] = []
        for hit in hits:
            src = dict(hit.get("_source") or {})
            if "_id" not in src:
                src["_id"] = hit.get("_id")
            tup = tuple(src.get(col, hit.get("_id") if col.lower() in {"id", "_id"} else None) for col in cols)
            if any(part is None for part in tup):
                return None
            rows.append(tup)
        return rows
    finally:
        client.close()


def _elasticsearch_key_hits(
    cfg: dict[str, Any],
    *,
    index: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    """How many leftover identities the index already holds (``mget`` by ``_id``)."""
    from connectors.elasticsearch_reader import _client

    if not index or not cols:
        return None
    client = _client(cfg)
    try:
        if not client.indices.exists(index=index):
            return 0
        ids = [_elasticsearch_doc_id(tup) for tup in keys if _elasticsearch_doc_id(tup)]
        if not ids:
            return 0
        resp = client.mget(index=index, ids=ids)
        return sum(1 for doc in (resp.get("docs") or []) if doc.get("found"))
    finally:
        client.close()


def _elasticsearch_delete_keys(
    cfg: Mapping[str, Any],
    *,
    index: str,
    cols: list[str],
    keys: Sequence[str],
) -> int:
    """Delete leftover documents by the same ``_id`` the writer upserts."""
    from elasticsearch.helpers import bulk

    from connectors.elasticsearch_reader import _client
    from services.row_conservation import parse_delete_keys

    leftover = parse_delete_keys(list(keys), len(cols))
    if not leftover or not index:
        return 0
    client = _client(dict(cfg))
    try:
        if not client.indices.exists(index=index):
            return 0
        actions = [
            {"_op_type": "delete", "_index": index, "_id": _elasticsearch_doc_id(tup)}
            for tup in leftover
            if _elasticsearch_doc_id(tup)
        ]
        if not actions:
            return 0
        deleted, errors = bulk(client, actions, raise_on_error=False, refresh=True)
        if errors:
            raise RuntimeError(f"elasticsearch leftover DELETE failed: {errors[:3]}")
        return int(deleted or 0)
    finally:
        client.close()


def _dynamodb_key_list(
    cfg: dict[str, Any],
    *,
    table_name: str,
    cols: list[str],
) -> list[tuple[Any, ...]] | None:
    """HASH/RANGE tuples from ``Scan`` — KeySchema only, never a non-identity attr."""
    from botocore.exceptions import ClientError

    from connectors.aws_common import boto3_client
    from connectors.dynamodb_reader import _item_to_record, describe_key_schema

    table = (table_name or "").strip()
    if not table or not cols:
        return None
    try:
        schema = describe_key_schema(cfg, table)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return []
        raise
    key_names = [str(item.get("name") or "") for item in schema]
    if sorted(name.lower() for name in key_names) != sorted(col.lower() for col in cols):
        return None
    name_by_lower = {name.lower(): name for name in key_names}
    ordered = [name_by_lower[col.lower()] for col in cols]
    client = boto3_client("dynamodb", cfg)
    expr_names = {f"#k{i}": name for i, name in enumerate(ordered)}
    projection = ",".join(expr_names)
    rows: list[tuple[Any, ...]] = []
    start_key: dict[str, Any] | None = None
    while True:
        kwargs: dict[str, Any] = {
            "TableName": table,
            "ProjectionExpression": projection,
            "ExpressionAttributeNames": expr_names,
            "ConsistentRead": True,
        }
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        try:
            resp = client.scan(**kwargs)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return []
            raise
        for item in resp.get("Items") or []:
            rec = _item_to_record(item)
            tup = tuple(rec.get(name) for name in ordered)
            if any(part is None for part in tup):
                return None
            rows.append(tup)
        start_key = resp.get("LastEvaluatedKey") or None
        if not start_key:
            return rows


def _dynamodb_delete_keys(
    cfg: Mapping[str, Any],
    *,
    table_name: str,
    cols: list[str],
    keys: Sequence[str],
) -> int:
    """DeleteItem leftover HASH/RANGE keys using the writer's AttributeValue encode."""
    from botocore.exceptions import ClientError

    from connectors.aws_common import boto3_client
    from connectors.dynamodb_reader import describe_key_schema
    from connectors.dynamodb_writer import _coerce_dynamo_cell, _to_attr
    from services.row_conservation import parse_delete_keys

    leftover = parse_delete_keys(list(keys), len(cols))
    table = (table_name or "").strip()
    if not leftover or not table or not cols:
        return 0
    try:
        schema = describe_key_schema(dict(cfg), table)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return 0
        raise
    key_names = [str(item.get("name") or "") for item in schema]
    if sorted(name.lower() for name in key_names) != sorted(col.lower() for col in cols):
        return 0
    declared = {
        str(item.get("name") or "").lower(): str(item.get("attr_type") or "VARCHAR")
        for item in schema
    }
    key_letters = {
        name: {"VARCHAR": "S", "DECIMAL": "N", "BINARY": "B"}.get(declared_type, "S")
        for name, declared_type in declared.items()
    }
    client = boto3_client("dynamodb", dict(cfg))
    deleted = 0
    for tup in leftover:
        item = {
            name: _to_attr(
                _coerce_dynamo_cell(
                    value,
                    col=name,
                    logical_type=declared.get(name.lower(), "VARCHAR"),
                    key_types={name: key_letters.get(name.lower(), "S")},
                ),
                declared.get(name.lower(), "VARCHAR"),
            )
            for name, value in zip(cols, tup)
        }
        try:
            client.delete_item(TableName=table, Key=item)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                continue
            raise
        deleted += 1
    return deleted


def _sftp_row_count(cfg: dict[str, Any], *, table_name: str) -> int | None:
    """Dest-engine artifact COUNT of an SFTP GET stream. Missing file is 0.

    Same format machine as object-store / local ``count_artifact_rows``.
    Never ``fh.read()`` of the remote file. Writer PUT rowcount is not
    this proof. Unparseable stays unmeasured.
    """
    from services.object_streaming import open_sftp_binary

    merged = dict(cfg)
    if table_name:
        merged["table"] = table_name
    opened = open_sftp_binary(merged)
    if opened is False:
        return 0
    if opened is None:
        return None
    stream, closer = opened
    name = str(table_name or merged.get("table") or merged.get("path") or "")
    try:
        return _count_artifact_stream(stream, name=name)
    except Exception as exc:
        logger.info("SFTP dest COUNT failed for %s: %s", name, exc)
        return None
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _seek_start(source: Any) -> Any:
    """Byte 0 of an uncompressed image. Footer/workbook parsers seek."""
    if isinstance(source, (bytes, bytearray)):
        return io.BytesIO(bytes(source))
    try:
        source.seek(0)
    except Exception as exc:
        raise UnmeasuredArtifact("byte_image_not_seekable") from exc
    return source


def _iter_parquet_records(source: Any) -> Any:
    """Cell values of one Parquet object. Footer ``num_rows`` is COUNT, not this."""
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise UnmeasuredArtifact("parquet_checksum_needs_pyarrow") from exc
    try:
        table = pq.read_table(_seek_start(source))
    except UnmeasuredArtifact:
        raise
    except Exception as exc:
        raise UnmeasuredArtifact("parquet_unparseable") from exc
    for batch in table.to_batches():
        for rec in batch.to_pylist():
            if not isinstance(rec, dict):
                raise UnmeasuredArtifact("parquet_non_record")
            yield rec


def _iter_orc_records(source: Any) -> Any:
    """Cell values of one ORC object. Footer ``nrows`` is COUNT, not this."""
    try:
        from pyarrow import orc
    except ImportError as exc:
        raise UnmeasuredArtifact("orc_checksum_needs_pyarrow") from exc
    try:
        table = orc.ORCFile(_seek_start(source)).read()
    except UnmeasuredArtifact:
        raise
    except Exception as exc:
        raise UnmeasuredArtifact("orc_unparseable") from exc
    for batch in table.to_batches():
        for rec in batch.to_pylist():
            if not isinstance(rec, dict):
                raise UnmeasuredArtifact("orc_non_record")
            yield rec


def _iter_excel_records(source: Any) -> Any:
    """Value-bearing Excel dicts. Used-range / ``max_row`` is not dest."""
    try:
        from services.excel_parser import iter_excel_dicts
    except ImportError as exc:
        raise UnmeasuredArtifact("excel_checksum_needs_parser") from exc
    try:
        yield from iter_excel_dicts(source)
    except UnmeasuredArtifact:
        raise
    except Exception as exc:
        raise UnmeasuredArtifact("excel_unparseable") from exc


def iter_avro_dicts(content: bytes | str | Path | Any) -> Any:
    """Same sequential Avro population as dest COUNT, as dicts for Gate-8.

    Apache Avro object-container files are a header plus data blocks with
    sync markers — a forward-only record stream, not a footer ``nrows``.
    ``fastavro.reader`` walks a file-like (local path, GET body, gzip
    wrapping a StreamingBody). Never ``gzip.decompress`` a second copy,
    never ingest ``parse_avro``'s row cap. A non-dict datum is unmeasured
    (tabular dest is records). Empty well-formed yields nothing.
    """
    try:
        import fastavro
    except ImportError as exc:
        raise UnmeasuredArtifact("avro_checksum_needs_fastavro") from exc
    closer = None
    try:
        source, closer = artifact_byte_source(content)
        for rec in fastavro.reader(source):
            if not isinstance(rec, dict):
                raise UnmeasuredArtifact("avro_non_record")
            yield rec
    except UnmeasuredArtifact:
        raise
    except Exception as exc:
        raise UnmeasuredArtifact("avro_unparseable") from exc
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def count_avro_records(content: bytes | str | Path | Any) -> int | None:
    """Dest-engine record COUNT of tabular Avro. Never ingest ``max_rows``.

    Population is one dict per object-container record. Empty header-only
    is 0. Missing parser / malformed / non-record stay unmeasured, not
    dest=0. Path and GET streams; gzip is ``GzipFile``, not a decompressed
    slurp. COUNT is ``sum`` of ``iter_avro_dicts``.
    """
    try:
        return sum(1 for _ in iter_avro_dicts(content))
    except UnmeasuredArtifact:
        return None
    except Exception as exc:
        logger.info("avro artifact count unavailable: %s", exc)
        return None


def _iter_streaming_kind(kind: str, source: Any, *, name: str) -> Any:
    """CSV/JSON/JSONL/XML/Avro records from a forward-only GET."""
    if kind in {"csv", "tsv"}:
        from services.csv_profiler import iter_csv_dicts

        yield from iter_csv_dicts(source)
        return
    if kind == "jsonl":
        from services.file_parser import iter_jsonl_dicts

        yield from iter_jsonl_dicts(source)
        return
    if kind == "json":
        from services.json_tabular import iter_json_dicts

        yield from iter_json_dicts(source)
        return
    if kind == "xml":
        from services.file_parser import iter_xml_dicts

        yield from iter_xml_dicts(source)
        return
    if kind == "avro":
        yield from iter_avro_dicts(source)
        return
    if kind == "yaml":
        from services.yaml_tabular import iter_yaml_dicts

        yield from iter_yaml_dicts(source)
        return
    raise UnmeasuredArtifact(f"{kind}_checksum_unmeasured:{name}")


def _iter_byte_image_kind(kind: str, source: Any, *, name: str) -> Any:
    """Parquet/ORC/Excel value walk of one seekable uncompressed image.

    Gzip is already a ``GzipFile`` on entry; ``rewindable_byte_source``
    spools it once so footer seeks are not decompress-from-start.
    Never ``gzip.decompress(source.read())``. Never JSON ``[]``.
    """
    image, closer = rewindable_byte_source(source)
    try:
        if kind == "parquet":
            yield from _iter_parquet_records(image)
            return
        if kind == "orc":
            yield from _iter_orc_records(image)
            return
        if kind == "excel":
            yield from _iter_excel_records(image)
            return
        raise UnmeasuredArtifact(f"{kind}_checksum_unmeasured:{name}")
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _artifact_stream_open(
    source: Any,
    *,
    name: str,
    fmt: str | None = None,
) -> tuple[str, Any, Any]:
    """Kind + handle for one GET. Gzip is ``GzipFile``, never ``source.read()``.

    Returns ``(kind, handle, gzip_closer)``. Streaming kinds walk the
    handle forward-only. Footer/workbook kinds spool via
    ``rewindable_byte_source``.
    """
    label = str(name or "")
    compressed = label.lower().endswith(".gz")
    if compressed:
        label = label[: -len(".gz")]
    kind = _infer_artifact_format(Path(label), fmt)
    if not compressed:
        return kind, source, None
    try:
        stream = gzip.GzipFile(fileobj=source, mode="rb")
    except Exception as exc:
        raise UnmeasuredArtifact(f"gzip_stream_failed:{name}") from exc
    return kind, stream, stream.close


def _iter_artifact_records(
    source: Any,
    *,
    name: str,
    fmt: str | None = None,
) -> Any:
    """Dest-engine records of an object-store GET. Same gzip machine as COUNT.

    CSV/JSON/JSONL/XML/Avro (including gzip) walk ``source`` forward-only.
    Parquet/ORC/Excel gzip stream-decompress into one rewindable image
    (footer / workbook parsers). JSON unique-path cell dicts (root array
    or wrapped) and XML unique-path cell dicts are a second StAX pass of
    the COUNT path (one-shot GET is spooled once). Never ``json.loads``
    fallback empty. Never ``gzip.decompress(source.read())``.
    """
    kind, handle, gz_close = _artifact_stream_open(source, name=name, fmt=fmt)
    try:
        if kind in _STREAMING_COUNT_KINDS:
            yield from _iter_streaming_kind(kind, handle, name=name)
            return
        if kind in _BYTE_IMAGE_KINDS:
            yield from _iter_byte_image_kind(kind, handle, name=name)
            return
        raise UnmeasuredArtifact(f"{kind or 'unknown'}_checksum_unmeasured:{name}")
    finally:
        if gz_close is not None:
            try:
                gz_close()
            except Exception:
                pass


def _checksum_records(
    records: Any,
    *,
    columns: list[str] | None = None,
    limit: int = 0,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Hash a dest-engine record iterator. Empty ``(0, "")``. Unmeasured ``(-1, "")``."""
    from services.reconciliation import canonical_checksum_from_iter

    n = 0

    def counted() -> Any:
        nonlocal n
        for rec in records:
            n += 1
            yield rec

    try:
        digest = canonical_checksum_from_iter(
            counted(),
            columns,
            limit=limit,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
    except UnmeasuredArtifact as exc:
        logger.info("artifact Gate-8 checksum unmeasured: %s", exc)
        return -1, ""
    except Exception as exc:
        logger.info("artifact Gate-8 checksum failed: %s", exc)
        return -1, ""
    if n == 0:
        return 0, ""
    return n, digest


def checksum_artifact_stream(
    source: Any,
    *,
    name: str,
    columns: list[str] | None = None,
    limit: int = 0,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Gate-8 cell checksum of one artifact stream. Never JSON-fallback empty."""
    return _checksum_records(
        _iter_artifact_records(source, name=name),
        columns=columns,
        limit=limit,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )


def sample_artifact_records(
    source: Any,
    *,
    name: str,
    limit: int = 50,
    sort_key: str = "",
    keys: Any = None,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Bounded dest-engine sample of an artifact stream.

    ``UnmeasuredArtifact`` propagates — never ``[]`` for gzip CSV / Parquet
    as UTF-8 JSON garbage (that greens a lost write). Well-formed empty
    is ``[]``.
    """
    wanted = (
        {key for k in keys if (key := present_cell_text(k)) is not None}
        if keys
        else set()
    )
    lim = max(1, int(limit or 50))
    projection = None if not columns or columns == ["*"] else list(columns)
    out: list[dict[str, Any]] = []
    for rec in _iter_artifact_records(source, name=name):
        if wanted and sort_key and present_cell_text(rec.get(sort_key)) not in wanted:
            continue
        if projection is not None:
            rec = {k: rec.get(k) for k in projection}
        out.append(rec)
        if len(out) >= lim and not wanted:
            break
    return out[:lim]


def sample_object_store(
    db_type: str,
    cfg: dict[str, Any],
    *,
    table_name: str,
    limit: int = 50,
    sort_key: str = "",
    keys: Any = None,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Bounded dest sample of object-store GET streams. Never JSON-fallback empty.

    Same keys and GET handles as dest COUNT / Gate-8 checksum. Unparseable
    raises ``UnmeasuredArtifact`` (callers map that to sample-unavailable).
    Missing listing is ``[]``.
    """
    from services.object_streaming import open_object_store_binary

    bucket = str(cfg.get("database") or "").strip()
    key = str(table_name or "").strip()
    if not bucket or not key:
        raise UnmeasuredArtifact("object_store_sample_missing_bucket_or_key")
    kind = _object_store_kind(db_type)
    listed = _object_store_list_keys(kind, cfg, bucket, key)
    if listed is None:
        raise UnmeasuredArtifact("object_store_list_unknowable")
    if not listed:
        return []
    wanted = (
        {key for k in keys if (key := present_cell_text(k)) is not None}
        if keys
        else set()
    )
    lim = max(1, int(limit or 50))
    out: list[dict[str, Any]] = []
    for obj_key in listed:
        opened = open_object_store_binary(kind, cfg, bucket, str(obj_key))
        if opened is False:
            continue
        if opened is None:
            raise UnmeasuredArtifact("object_store_get_unknowable")
        stream, closer = opened
        try:
            need = lim if wanted else max(1, lim - len(out))
            out.extend(
                sample_artifact_records(
                    stream,
                    name=str(obj_key),
                    limit=need,
                    sort_key=sort_key,
                    keys=keys,
                    columns=columns,
                )
            )
            if len(out) >= lim and not wanted:
                return out[:lim]
        finally:
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass
    return out[:lim]


def checksum_object_store(
    db_type: str,
    cfg: dict[str, Any],
    *,
    table_name: str,
    columns: list[str] | None = None,
    limit: int = 0,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Gate-8 cell checksum of object-store GET streams. Never JSON-fallback empty.

    Same keys and GET handles as dest COUNT. One key is opened, walked,
    and closed before the next — never a list of GET bodies. Gzip CSV as
    UTF-8 JSON garbage is not dest=0. Unparseable / ambiguous JSON / XML /
    Excel / ORC cell walk is ``(-1, "")``. Empty well-formed is
    ``(0, "")``. Cardinality remains dest COUNT; this digest is cell
    fidelity of the records we could walk. CDC stays at-least-once upsert.
    """
    from services.object_streaming import open_object_store_binary

    bucket = str(cfg.get("database") or "").strip()
    key = str(table_name or "").strip()
    if not bucket or not key:
        return -1, ""
    kind = _object_store_kind(db_type)
    keys = _object_store_list_keys(kind, cfg, bucket, key)
    if keys is None:
        return -1, ""
    if not keys:
        return 0, ""

    def records() -> Any:
        for obj_key in keys:
            opened = open_object_store_binary(kind, cfg, bucket, str(obj_key))
            if opened is False:
                continue
            if opened is None:
                raise UnmeasuredArtifact("object_store_get_unknowable")
            stream, closer = opened
            try:
                yield from _iter_artifact_records(stream, name=str(obj_key))
            finally:
                if closer is not None:
                    try:
                        closer()
                    except Exception:
                        pass

    return _checksum_records(
        records(),
        columns=columns,
        limit=limit,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )


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


def count_dialect(raw_type: str) -> str:
    """The engine identity this module must COUNT against.

    ``resolve_driver_type`` answers a *capability* question and folds every SQL
    engine that shares the SQLAlchemy writer into ``generic_sql``. That family
    has no COUNT: asked for DuckDB it returned ``None``, so append delta and
    keyed conservation were permanently unprovable for every engine behind the
    generic writer while the writer itself worked. The dialect owner
    (``dialect_profiles.normalize_driver``) keeps the concrete engine and still
    folds hosted SKUs (``azure_sql`` → ``sqlserver``), so it is the right
    resolver once the family says "generic SQL".
    """
    from services.dialect_profiles import normalize_driver
    from src.transfer.connector_capabilities import resolve_driver_type

    family = resolve_driver_type(raw_type)
    if family != "generic_sql":
        return family
    return normalize_driver(raw_type) or family


def precount_destination(
    endpoint: EndpointConfig, cfg: dict[str, Any]
) -> int | None:
    """Pre-write count for a resolved destination endpoint.

    Resolves the driver, schema and table exactly the way the writer will, so
    the delta is measured against the object the rows actually land in.
    """
    from src.transfer.adapters import resolve_dest_table

    db_type = count_dialect(str(cfg.get("type") or endpoint.format or ""))
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

    try:
        cfg = resolve_connector_config(endpoint)
        db_type = count_dialect(str(cfg.get("type") or endpoint.format or ""))
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
    if name.endswith(".yaml") or name.endswith(".yml"):
        return "yaml"
    return ""


def open_artifact_binary(path: Path) -> tuple[Any, Any]:
    """Byte source for dest COUNT. ``*.gz`` is a gzip stream, never a slurp.

    Caller closes via the returned closer. CSV COUNT sniffs a prefix from
    this handle and continues via prefix-then-rest — one gzip open, not a
    second. Object-store GET gzip wraps the compressed body in
    ``GzipFile``; CSV COUNT does not ``seek(0)`` that stream.
    """
    if path.name.lower().endswith(".gz"):
        handle = gzip.open(path, "rb")
        return handle, handle.close
    handle = path.open("rb")
    return handle, handle.close


def artifact_byte_source(content: bytes | str | Path | Any) -> tuple[Any, Any]:
    """Path / bytes / str / readable stream → a binary handle for dest COUNT.

    ``Path`` (including ``*.gz``) uses ``open_artifact_binary``. Bytes and
    str stay in RAM. A readable stream is returned as-is (object-store GET,
    one-shot gzip). JSON, XML, and Avro share this opener — not three copies.
    """
    if isinstance(content, Path):
        return open_artifact_binary(content)
    if isinstance(content, bytes):
        return io.BytesIO(content), None
    if isinstance(content, str):
        return io.BytesIO(content.encode("utf-8")), None
    if hasattr(content, "read"):
        return content, None
    raise TypeError("artifact COUNT expects bytes, str, Path, or a readable stream")


def _count_parquet_handle(source: Any) -> int | None:
    """Footer ``num_rows`` of a seekable uncompressed Parquet image."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    try:
        metadata = pq.ParquetFile(_seek_start(source)).metadata
        if metadata is None:
            return None
        return int(metadata.num_rows)
    except UnmeasuredArtifact:
        return None
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


def _count_orc_handle(source: Any) -> int | None:
    """Footer ``nrows`` of a seekable uncompressed ORC image."""
    try:
        from pyarrow import orc
    except ImportError:
        return None
    try:
        reader = orc.ORCFile(_seek_start(source))
        n = getattr(reader, "nrows", None)
        if n is None:
            n = reader.read().num_rows
        return int(n)
    except UnmeasuredArtifact:
        return None
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


def _count_excel_handle(source: Any) -> int | None:
    """Value-bearing rows. Used-range / ``max_row`` is not dest population."""
    try:
        from services.excel_parser import count_excel_rows

        return int(count_excel_rows(source))
    except Exception as exc:
        logger.info("excel artifact count unavailable: %s", exc)
        return None


def _count_streaming_kind(kind: str, source: Any) -> int | None:
    """CSV/TSV/JSON/JSONL/XML/Avro COUNT from Path, bytes, str, or a readable stream."""
    if kind in {"csv", "tsv"}:
        from services.csv_profiler import count_csv_rows

        n = count_csv_rows(source)
        return None if n is None else int(n)
    if kind == "jsonl":
        from services.file_parser import count_jsonl_records

        n = count_jsonl_records(source)
        return None if n is None else int(n)
    if kind == "json":
        from services.json_tabular import count_json_records

        n = count_json_records(source)
        return None if n is None else int(n)
    if kind == "xml":
        from services.file_parser import count_xml_records

        n = count_xml_records(source)
        return None if n is None else int(n)
    if kind == "avro":
        n = count_avro_records(source)
        return None if n is None else int(n)
    if kind == "yaml":
        from services.yaml_tabular import count_yaml_records

        n = count_yaml_records(source)
        return None if n is None else int(n)
    return None


def _count_byte_image_kind(kind: str, source: Any) -> int | None:
    """Parquet/ORC/Excel COUNT of a (possibly gzip) stream via one rewindable image."""
    try:
        image, closer = rewindable_byte_source(source)
    except Exception as exc:
        logger.info("byte-image spool failed for kind %s: %s", kind, exc)
        return None
    try:
        if kind == "parquet":
            return _count_parquet_handle(image)
        if kind == "orc":
            return _count_orc_handle(image)
        if kind == "excel":
            return _count_excel_handle(image)
        return None
    except Exception as exc:
        logger.info("artifact count failed for kind %s: %s", kind, exc)
        return None
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _count_artifact_kind(kind: str, content: bytes) -> int | None:
    """Record COUNT for already-decoded artifact bytes. Unknown kind is None."""
    if kind not in _ARTIFACT_FORMATS:
        return None
    try:
        if kind in _STREAMING_COUNT_KINDS:
            return _count_streaming_kind(kind, content)
        if kind in _BYTE_IMAGE_KINDS:
            return _count_byte_image_kind(kind, io.BytesIO(content))
    except Exception as exc:
        logger.info("artifact count failed for kind %s: %s", kind, exc)
        return None
    return None


def _count_artifact_stream(
    source: Any,
    *,
    name: str,
    fmt: str | None = None,
) -> int | None:
    """Dest-engine COUNT of an object-store GET stream. Same machine as a local file.

    CSV/JSON/JSONL/XML/Avro (including gzip) walk ``source`` forward-only —
    ``GzipFile(fileobj=StreamingBody)``, never ``Body.read()`` of the object
    and never ``gzip.decompress`` of a second copy. CSV encoding sniff is
    prefix-then-rest (no ``seek(0)``). Excel/Parquet/ORC gzip stream-decompress
    into one rewindable image (footer / workbook). Uncompressed object-store
    Parquet/ORC COUNT uses ``open_object_store_seekable`` before this
    helper. Unparseable / unsupported / missing parser stay unmeasured —
    never JSON-fallback empty (that is dest=0).
    """
    try:
        kind, handle, gz_close = _artifact_stream_open(source, name=name, fmt=fmt)
    except UnmeasuredArtifact as exc:
        logger.info("artifact stream open failed for %s: %s", name, exc)
        return None
    try:
        if kind in _STREAMING_COUNT_KINDS:
            return _count_streaming_kind(kind, handle)
        if kind in _BYTE_IMAGE_KINDS:
            return _count_byte_image_kind(kind, handle)
        return None
    except Exception as exc:
        logger.info("artifact stream count failed for %s: %s", name, exc)
        return None
    finally:
        if gz_close is not None:
            try:
                gz_close()
            except Exception:
                pass


def _count_artifact_payload(
    content: bytes,
    *,
    name: str,
    fmt: str | None = None,
) -> int | None:
    """In-RAM GET body. Streaming GET uses ``_count_artifact_stream``."""
    return _count_artifact_stream(io.BytesIO(content), name=name, fmt=fmt)


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
    repeating record-path from disk (StAX), not ingest ``max_rows``. JSON
    counts the unique array-of-object from disk (ijson StAX), not
    ``json.loads`` of the whole export. JSONL counts one object per
    non-blank line from disk, not ``decode`` + ``splitlines`` of the
    whole export. CSV/TSV counts RFC 4180 records from disk, not
    ``wc -l`` and not a slurp of the whole file. Local CSV/JSON/JSONL/XML/Avro
    gzip streams. Local Excel/Parquet/ORC gzip stream-decompress into one
    rewindable image (never ``read_bytes`` of the compressed export).
    Uncompressed Parquet/ORC still read the footer from the path.
    Object-store GET of CSV/JSON/JSONL/XML/Avro streams the HTTP body through
    ``GzipFile`` when gzip; Excel/Parquet/ORC GET gzip uses the same
    ``GzipFile`` + spool kernel. Uncompressed object-store Parquet/ORC
    Range-GET the footer instead of spooling the object.
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
    gzipped = artifact.name.lower().endswith(".gz")
    if kind == "parquet" and not gzipped:
        return _count_parquet_path(artifact)
    if kind == "orc" and not gzipped:
        return _count_orc_path(artifact)
    if kind in _STREAMING_COUNT_KINDS:
        try:
            n = _count_streaming_kind(kind, artifact)
            return None if n is None else int(n)
        except Exception as exc:
            logger.info("artifact count unavailable at %s: %s", artifact, exc)
            return None
    if kind in _BYTE_IMAGE_KINDS:
        closer = None
        try:
            source, closer = open_artifact_binary(artifact)
            n = _count_byte_image_kind(kind, source)
            return None if n is None else int(n)
        except Exception as exc:
            logger.info("artifact count unavailable at %s: %s", artifact, exc)
            return None
        finally:
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass
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
    dest-engine session; Snowflake / BigQuery / DuckDB / Databricks /
    Redshift use the same ``COUNT(*)`` machine (BOOLEAN ``IS TRUE``),
    never catalog stats / ``to_regclass``.
    """
    table = (table_name or "").strip()
    if not table:
        return None
    kind = str(db_type or "").strip().lower()
    try:
        if kind == "sqlite":
            return _sqlite_scd2_populations(cfg, table_name=table)
        if kind == "postgresql":
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
    with closing(sqlite3.connect(database)) as conn:
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
