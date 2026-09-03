"""Choosing the server-to-server COPY path, and shaping what it reports.

Split out of ``src.transfer.stream`` (a module at its size budget). The decision
is deliberately conservative: it returns ``None`` for every route it cannot
prove identical, because a route this path cannot verify belongs on the row
path, which knows how to reconcile the differences it refuses to guess at.
"""

from __future__ import annotations

import logging
from typing import Any

from services.checkpoint_service import Checkpoint

from .models import EndpointConfig

logger = logging.getLogger(__name__)


def _try_copy_fast_path(
    *,
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    src_type: str,
    dest_type: str,
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    effective_sync: str,
    incremental: bool,
    source_filter: dict[str, Any] | None,
    limit: int,
    checkpoint: Checkpoint | None,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Move the whole table server-to-server, or return ``None`` to stream rows.

    PostgreSQL→PostgreSQL: binary COPY when types are identical. Append and
    overwrite both qualify; a non-empty destination on append stays on the
    row path. Proof is the mapped-column digest plus dest ``COUNT(*)``.

    PostgreSQL→MySQL identity append/overwrite: text COPY + STRICT
    ``LOAD DATA LOCAL INFILE`` when every mapping is a no-op carry and every
    type is LOAD-DATA-safe. Proof is dest ``COUNT(*)`` vs the source snapshot.

    MySQL→PostgreSQL identity append/overwrite: unbuffered SELECT + FIFO TSV
    into ``COPY FROM STDIN``. One InnoDB consistent snapshot. Proof is dest
    ``COUNT(*)`` vs that snapshot.

    MySQL→MySQL identity append/overwrite: same-instance ``INSERT SELECT``
    under a consistent snapshot, or cross-host STRICT ``LOAD DATA LOCAL
    INFILE``. Proof is dest ``COUNT(*)`` vs that snapshot.

    PostgreSQL→SQL Server identity append/overwrite: text COPY decoded
    into pyodbc ``fast_executemany`` batches. Not BCP / ``BULK INSERT``
    CSV (quoted empty string collapses to NULL on this engine). Proof
    is dest ``COUNT(*)`` vs the source snapshot.

    SQL Server→PostgreSQL identity append/overwrite: HOLDLOCK SELECT
    encoded as COPY text into ``COPY FROM STDIN``. Proof is dest
    ``COUNT(*)`` vs that snapshot.

    SQL Server→SQL Server identity append/overwrite: same-instance
    ``INSERT SELECT`` (SNAPSHOT when the database allows it, else
    ``HOLDLOCK, TABLOCK``). Proof is dest ``COUNT(*)`` vs that snapshot.
    Cross-host declines to the row path (no BCP yet).

    Oracle→Oracle identity append/overwrite: same-instance ``INSERT
    SELECT`` after ``LOCK TABLE src IN SHARE MODE``. Proof is dest
    ``COUNT(*)`` vs that snapshot. Cross-host declines to the row path
    (no Data Pump / DB link yet).

    PostgreSQL→Oracle identity append/overwrite: text COPY decoded
    into ``oracledb.executemany`` batches. Oracle VARCHAR2 stores
    ``''`` as NULL (engine law, counted in
    ``empty_string_as_null_cells``). Proof is dest ``COUNT(*)`` vs the
    source snapshot.

    Oracle→PostgreSQL identity append/overwrite: SHARE-lock SELECT
    encoded as COPY text into ``COPY FROM STDIN``. Proof is dest
    ``COUNT(*)`` vs that snapshot.

    MySQL→SQL Server identity append/overwrite: consistent-snapshot
    SELECT bound with pyodbc ``fast_executemany``. Not BCP / CSV
    ``BULK INSERT``. Proof is dest ``COUNT(*)`` vs that snapshot.

    SQL Server→MySQL identity append/overwrite: HOLDLOCK SELECT encoded
    as LOAD DATA TSV into a tempfile, then STRICT ``LOAD DATA LOCAL
    INFILE`` (no pyodbc FIFO). Proof is dest ``COUNT(*)`` vs that
    snapshot.

    MySQL→Oracle identity append/overwrite: consistent-snapshot SELECT
    bound with ``oracledb.executemany``. Oracle VARCHAR2 stores ``''``
    as NULL (engine law, counted in ``empty_string_as_null_cells``).
    Proof is dest ``COUNT(*)`` vs the source snapshot.

    Oracle→MySQL identity append/overwrite: SHARE-lock SELECT encoded
    as LOAD DATA TSV into a tempfile, then STRICT ``LOAD DATA LOCAL
    INFILE``. Proof is dest ``COUNT(*)`` vs that snapshot.

    SQL Server→Oracle identity append/overwrite: HOLDLOCK SELECT bound
    with ``oracledb.executemany``. Oracle VARCHAR2 stores ``''`` as
    NULL (engine law, counted in ``empty_string_as_null_cells``). Proof
    is dest ``COUNT(*)`` vs the source snapshot.

    Oracle→SQL Server identity append/overwrite: SHARE-lock SELECT
    bound with pyodbc ``fast_executemany``. Not BCP / CSV
    ``BULK INSERT``. Proof is dest ``COUNT(*)`` vs that snapshot.

    PostgreSQL→Iceberg identity append/overwrite: COPY CSV into one
    Arrow table and one catalog snapshot commit (CoW append, or
    snapshot replace on overwrite). Dest COUNT is file footers, never
    ``scan().count()``. Occupied dest with a different COUNT declines
    (leftover MERGE stays on the row path). Filesystem CoW declines.

    Iceberg→PostgreSQL identity append/overwrite: current-snapshot
    Parquet files encoded as COPY text into ``COPY FROM STDIN``. Source
    COUNT is file footers, never ``scan().count()``. Occupied dest with
    a different COUNT declines. MoR snapshots decline.

    Iceberg→MySQL identity append/overwrite: current-snapshot Parquet
    files encoded as LOAD DATA TSV into a tempfile, then STRICT
    ``LOAD DATA LOCAL INFILE``. Source COUNT is file footers, never
    ``scan().count()``. Occupied dest with a different COUNT declines.
    MoR snapshots decline.

    Iceberg→SQL Server identity append/overwrite: current-snapshot
    Parquet files bound with pyodbc ``fast_executemany``. Not BCP /
    ``BULK INSERT`` CSV. Source COUNT is file footers, never
    ``scan().count()``. Occupied dest with a different COUNT declines.
    MoR snapshots decline.

    SQL Server→MongoDB identity append/overwrite: HOLDLOCK SELECT bound
    with ``insert_many``. Not ``mongoimport`` / BCP. Dest COUNT is
    ``count_documents({})``, never ``estimatedDocumentCount``. Empty dest
    is insert, not upsert. Occupied dest with a different COUNT declines.
    DATE is BSON Date at UTC midnight. DATETIME / DATETIME2 decline.

    MongoDB→SQL Server identity append/overwrite: replica-set snapshot
    ``find()`` bound with pyodbc ``fast_executemany``. Not BCP / CSV
    ``BULK INSERT``. Source COUNT is ``count_documents`` in that
    snapshot. Occupied dest with a different COUNT declines.

    Oracle→MongoDB identity append/overwrite: SHARE-lock SELECT bound
    with ``insert_many``. Not ``mongoimport`` / sqlldr. Dest COUNT is
    ``count_documents({})``, never ``estimatedDocumentCount``. Empty dest
    is insert, not upsert. Occupied dest with a different COUNT declines.
    DATE at midnight is BSON Date at UTC midnight. TIMESTAMP declines.
    VARCHAR2 ``''`` is NULL (engine law) and lands as BSON null.

    MongoDB→Oracle identity append/overwrite: replica-set snapshot
    ``find()`` bound with ``oracledb.executemany``. Not sqlldr / Data
    Pump. VARCHAR2 stores ``''`` as NULL (engine law, counted in
    ``empty_string_as_null_cells``). Source COUNT is ``count_documents``
    in that snapshot. Occupied dest with a different COUNT declines.
    Nested documents decline.

    MongoDB→MongoDB identity append/overwrite: replica-set snapshot
    ``find()`` bound with ``insert_many`` on a separate dest session
    (not ``$out`` in a transaction). Nested BSON is identity-safe.
    Same collection declines. Dest COUNT is ``count_documents({})``.
    Empty dest is insert, not upsert.

    Iceberg→MongoDB identity append/overwrite: current-snapshot Parquet
    files bound with ``insert_many``. Not ``mongoimport`` / ``scan().count()``.
    Dest COUNT is ``count_documents({})``. DATE is BSON Date at UTC
    midnight. TIMESTAMP declines. Empty dest is insert, not upsert /
    ``MERGE INTO``. Occupied dest with a different COUNT declines. MoR
    snapshots decline.

    Iceberg→Iceberg identity append/overwrite: current-snapshot Parquet
    files committed as a dest CoW snapshot (append or overwrite). Dest
    writes new data files — source files are not shared. Same table
    declines. Source and dest COUNT are file footers, never
    ``scan().count()``. Empty dest is CoW append, not ``MERGE INTO``.
    Occupied dest with a different COUNT declines. MoR snapshots
    decline. Nested list/map/struct decline.

    SQLite→SQLite identity append/overwrite: ``ATTACH DATABASE`` then
    ``INSERT INTO dest SELECT … FROM src``. Dest ``COUNT(*)`` is the
    proof. Same file + same table declines. Empty dest is INSERT SELECT,
    not upsert. Occupied dest with a different COUNT declines.
    ``:memory:`` declines. Not ``.dump`` / ``.import``.

    PostgreSQL→SQLite identity append/overwrite: text COPY decoded into
    ``executemany``. DATE lands as SQLite TEXT (ISO calendar day —
    SQLite has no DATE affinity). TIMESTAMP / BYTEA / JSONB decline.
    Dest ``COUNT(*)`` is the proof.

    SQLite→PostgreSQL identity append/overwrite: ``SELECT`` encoded as
    COPY text into ``COPY FROM STDIN``. DATE/BOOLEAN/BLOB decline
    (SQLite affinity would invent a PostgreSQL type). Dest ``COUNT(*)``
    is the proof.

    S3→S3 identity append/overwrite: server-side ``CopyObject`` /
    ``UploadPartCopy``. Dest COUNT is object-store artifact COUNT (GET
    streams / Parquet footers), never ListObjects length, never writer
    PUT ack. Same endpoint+bucket+key declines. Cross-endpoint declines.
    Empty dest is CopyObject, not GET+PUT / ``aws s3 cp`` / ``aws s3
    sync``. Occupied dest with a different COUNT declines.

    PostgreSQL→S3 identity append/overwrite: text COPY CSV (HEADER) into
    a tempfile, then ``upload_file``. Dest key must be ``.csv`` /
    ``.tsv``. Dest COUNT is artifact COUNT of that CSV (header skipped).
    JSON dest keys decline (row path keeps JSON export).

    S3→PostgreSQL identity append/overwrite: GET CSV/TSV into
    ``COPY FROM STDIN`` (HEADER). JSON/JSONL/Parquet decline. Dest
    ``COUNT(*)`` is the proof.

    MySQL→S3 identity append/overwrite: consistent-snapshot SELECT into
    a CSV tempfile (HEADER, ``\\N`` = NULL), then ``upload_file``. Dest
    key must be ``.csv`` / ``.tsv``. Dest COUNT is artifact COUNT of
    that CSV (header skipped). JSON dest keys decline.

    S3→MySQL identity append/overwrite: GET CSV/TSV into STRICT
    ``LOAD DATA LOCAL INFILE`` (HEADER skipped). JSON/JSONL/Parquet
    decline. Dest ``COUNT(*)`` runs before commit.

    SQLite→S3 identity append/overwrite: ``BEGIN`` + ``SELECT`` into a
    CSV tempfile (HEADER, ``\\N`` = NULL), then ``upload_file``. Dest
    key must be ``.csv`` / ``.tsv``. Dest COUNT is artifact COUNT of
    that CSV (header skipped). DATE affinity is allowed (stored TEXT).
    JSON dest keys decline. ``:memory:`` / BLOB decline. Not ``.dump``
    / ``aws s3 cp``.

    S3→SQLite identity append/overwrite: GET CSV/TSV into
    ``executemany`` INSERT. JSON/JSONL/Parquet decline. Dest
    ``COUNT(*)`` runs before commit. Not sqlite3 ``.import``.
    ``:memory:`` / BLOB decline.

    S3→Iceberg identity append/overwrite: GET CSV/TSV re-encoded into
    one Arrow table and one catalog snapshot. Source COUNT is object-
    store artifact COUNT (header skipped), never ListObjects length.
    Dest COUNT is file footers, never ``scan().count()``. JSON/JSONL/
    Parquet decline. Occupied dest with a different COUNT declines.
    Empty dest is CoW snapshot append, not ``MERGE INTO``. Filesystem
    CoW declines. Not ``aws s3 cp``.

    Iceberg→S3 identity append/overwrite: current-snapshot Parquet
    encoded as CSV (HEADER, ``\\N`` = NULL), then ``upload_file``.
    Source COUNT is file footers, never ``scan().count()``. Dest COUNT
    is artifact COUNT (header skipped), never PUT ack. Dest key must
    be ``.csv`` / ``.tsv``. Nested list/map/struct / MoR / binary /
    uuid / timestamptz decline. JSON dest keys decline. Occupied dest
    with a different COUNT declines. Empty dest is PUT, not ``MERGE
    INTO`` / ``aws s3 cp``.

    MongoDB→S3 identity append/overwrite: replica-set snapshot
    ``find()`` into a CSV tempfile (HEADER, ``\\N`` = NULL), then
    ``upload_file``. Source COUNT is ``count_documents({})``, never
    ``estimatedDocumentCount``. Dest key must be ``.csv`` / ``.tsv``.
    Dest COUNT is artifact COUNT (header skipped). Nested documents
    decline. JSON dest keys decline. Not ``mongoexport`` / ``aws s3 cp``.

    S3→MongoDB identity append/overwrite: GET CSV/TSV into
    ``insert_many``. Dest COUNT is ``count_documents({})``.
    JSON/JSONL/Parquet decline. Empty dest is insert, not upsert /
    ``mongoimport``. DATE CSV cells become BSON Date at UTC midnight.

    SQLite→MongoDB identity append/overwrite: ``BEGIN`` + ``SELECT``
    ``fetchmany`` into ``insert_many``. Dest COUNT is
    ``count_documents({})``, never ``estimatedDocumentCount``. DATE ISO
    text or a calendar day becomes BSON Date at UTC midnight when the
    mapping/pragma is DATE; TEXT ISO stays a string (identity of SQLite
    TEXT storage). DATETIME / TIMESTAMP / BLOB decline. Occupied dest
    with a different COUNT declines. Empty dest is insert, not upsert /
    ``mongoimport`` / sqlite3 ``.dump``. ``:memory:`` declines. ``_id``
    is not invented from row bytes.

    MongoDB→SQLite identity append/overwrite: replica-set snapshot
    ``find()`` bound with ``executemany`` INSERT. Source COUNT is
    ``count_documents`` in that snapshot. Dest ``COUNT(*)`` runs
    **before commit**. Nested documents / binary decline. DATE lands as
    SQLite TEXT (ISO calendar day — SQLite has no DATE affinity).
    Occupied dest with a different COUNT declines. Empty dest is INSERT,
    not upsert / sqlite3 ``.import`` / ``mongoexport``. ``:memory:`` /
    BLOB dest DDL decline.

    SQLite→MySQL identity append/overwrite: ``BEGIN`` + ``SELECT``
    encoded as LOAD DATA TSV into a tempfile, then STRICT
    ``LOAD DATA LOCAL INFILE``. Dest ``COUNT(*)`` runs **before commit**.
    DATE ISO/calendar day loads as MySQL DATE when mapped DATE; TEXT ISO
    stays a string. DATETIME / TIMESTAMP / BLOB / JSON decline. Occupied
    dest with a different COUNT declines. Empty dest is LOAD DATA, not
    upsert / sqlite3 ``.dump`` / sqlldr. ``:memory:`` declines.

    MySQL→SQLite identity append/overwrite: consistent-snapshot SELECT
    bound with ``executemany`` INSERT. Dest ``COUNT(*)`` runs **before
    commit**. DATE/DATETIME land as SQLite TEXT (no DATE affinity).
    TIMESTAMP / BLOB / JSON decline. Occupied dest with a different
    COUNT declines. Empty dest is INSERT, not upsert / sqlite3
    ``.import`` / mysqldump. ``:memory:`` declines.

    SQLite→Iceberg identity append/overwrite: ``BEGIN`` + ``SELECT``
    encoded as CSV into one Arrow table and one catalog snapshot. Dest
    COUNT is file footers, never ``scan().count()``. DATE ISO/calendar
    day is COPY-safe; DATETIME / TIMESTAMP / BLOB / JSON decline.
    Occupied dest with a different COUNT declines. Empty dest is CoW
    snapshot append, not ``MERGE INTO``. ``:memory:`` / filesystem CoW
    decline.

    Iceberg→SQLite identity append/overwrite: current-snapshot Parquet
    bound with ``executemany`` INSERT. Source COUNT is file footers,
    never ``scan().count()``. Dest ``COUNT(*)`` runs **before commit**.
    DATE/DATETIME-NTZ land as SQLite TEXT (no DATE affinity). Nested
    list/map/struct / MoR / binary / uuid / timestamptz decline.
    Occupied dest with a different COUNT declines. Empty dest is INSERT,
    not upsert / sqlite3 ``.import`` / ``MERGE INTO``. ``:memory:`` /
    BLOB dest DDL decline.

    SQLite→SQL Server identity append/overwrite: ``BEGIN`` + ``SELECT``
    bound with pyodbc ``fast_executemany``. Dest ``COUNT(*)`` is the
    proof. DATE ISO/calendar day binds as SQL Server DATE when mapped
    DATE; TEXT ISO stays a string. DATETIME / TIMESTAMP / BLOB / JSON
    decline. Occupied dest with a different COUNT declines. Empty dest
    is INSERT, not upsert / BCP / ``BULK INSERT`` CSV / sqlite3
    ``.dump``. ``:memory:`` declines.

    SQL Server→SQLite identity append/overwrite: HOLDLOCK ``SELECT``
    bound with ``executemany`` INSERT. Dest ``COUNT(*)`` runs **before
    commit**. DATE/DATETIME-NTZ land as SQLite TEXT (no DATE affinity).
    DATETIMEOFFSET / varbinary / xml / rowversion decline. Occupied dest
    with a different COUNT declines. Empty dest is INSERT, not upsert /
    sqlite3 ``.import`` / BCP. ``:memory:`` / BLOB dest DDL decline.

    SQLite→Oracle identity append/overwrite: ``BEGIN`` + ``SELECT``
    bound with ``oracledb.executemany``. Dest ``COUNT(*)`` is the proof.
    DATE ISO/calendar day binds as Oracle DATE when mapped DATE; TEXT
    ISO stays a string. DATETIME / TIMESTAMP / BLOB / JSON decline.
    VARCHAR2 stores ``''`` as NULL (engine law, counted in
    ``empty_string_as_null_cells``). Occupied dest with a different
    COUNT declines. Empty dest is INSERT, not upsert / sqlldr / Data
    Pump / sqlite3 ``.dump``. ``:memory:`` declines.

    Oracle→SQLite identity append/overwrite: SHARE-lock ``SELECT``
    bound with ``executemany`` INSERT. Dest ``COUNT(*)`` runs **before
    commit**. DATE/DATETIME-NTZ land as SQLite TEXT (no DATE affinity).
    Oracle VARCHAR2 empty strings already arrive as ``None`` (engine
    law) — SQLite stores NULL. BLOB/RAW/XMLTYPE decline. Occupied dest
    with a different COUNT declines. Empty dest is INSERT, not upsert /
    sqlite3 ``.import`` / sqlldr. ``:memory:`` / BLOB dest DDL decline.

    MongoDB→Iceberg identity append/overwrite: replica-set snapshot
    ``find()`` encoded as CSV into one Arrow table and one catalog
    snapshot. Source COUNT is ``count_documents``. Dest COUNT is file
    footers, never ``scan().count()``. Nested documents decline. Empty
    dest is CoW snapshot append, not ``MERGE INTO``.

    Iceberg→Oracle identity append/overwrite: current-snapshot Parquet
    files bound with ``oracledb.executemany``. Not sqlldr / Data Pump.
    VARCHAR2 stores ``''`` as NULL (engine law, counted in
    ``empty_string_as_null_cells``). Source COUNT is file footers, never
    ``scan().count()``. Occupied dest with a different COUNT declines.
    MoR snapshots decline.

    MySQL→Iceberg identity append/overwrite: consistent-snapshot SELECT
    encoded as CSV into one Arrow table and one catalog snapshot. Dest
    COUNT is file footers, never ``scan().count()``. Occupied dest with
    a different COUNT declines. Empty dest is CoW snapshot append, not
    ``MERGE INTO``.

    SQL Server→Iceberg identity append/overwrite: HOLDLOCK SELECT
    encoded as CSV into one Arrow table and one catalog snapshot. Dest
    COUNT is file footers, never ``scan().count()``. Occupied dest with
    a different COUNT declines. Empty dest is CoW snapshot append, not
    ``MERGE INTO``.

    Oracle→Iceberg identity append/overwrite: SHARE-lock SELECT encoded
    as CSV into one Arrow table and one catalog snapshot. Dest COUNT is
    file footers, never ``scan().count()``. VARCHAR2 ``''`` → NULL is
    source-side engine law (Iceberg string can store ``''``, Oracle
    never emits it). Occupied dest with a different COUNT declines.
    Empty dest is CoW snapshot append, not ``MERGE INTO``.

    PostgreSQL→MongoDB identity append/overwrite: text COPY decoded into
    ``insert_many`` batches. Not ``mongoimport``. Dest COUNT is
    ``count_documents({})``, never ``estimatedDocumentCount``. Empty dest
    is insert, not upsert. Occupied dest with a different COUNT declines.
    DATE is BSON Date at UTC midnight (Mongo has no date-only type).
    ``_id`` is not invented from row bytes.

    MongoDB→PostgreSQL identity append/overwrite: replica-set snapshot
    ``find()`` encoded as COPY text into ``COPY FROM STDIN``. Source COUNT
    is ``count_documents`` in that snapshot. Occupied dest with a
    different COUNT declines. Nested documents decline.

    MySQL→MongoDB identity append/overwrite: consistent-snapshot SELECT
    bound with ``insert_many``. Not ``mongoimport``. Dest COUNT is
    ``count_documents({})``, never ``estimatedDocumentCount``. Empty dest
    is insert, not upsert. Occupied dest with a different COUNT declines.
    DATE is BSON Date at UTC midnight. ``_id`` is not invented from row
    bytes. DATETIME / TIME decline.

    MongoDB→MySQL identity append/overwrite: replica-set snapshot
    ``find()`` encoded as LOAD DATA TSV into a tempfile, then STRICT
    ``LOAD DATA LOCAL INFILE``. Source COUNT is ``count_documents`` in
    that snapshot. Occupied dest with a different COUNT declines.
    Nested documents decline.

    Returning ``None`` rather than raising is deliberate — every route this
    cannot prove belongs on the row path, which knows how to reconcile the
    differences this one refuses to guess at.
    """
    from services.procedure_source import is_callable_source
    from services.sync_cursor import is_append_sync, is_overwrite_sync

    # Studio may set source.table to the procedure stream name (e.g. get_orders).
    # COPY of a colliding real table would move the wrong population — refuse.
    if is_callable_source(source) or is_callable_source(src_cfg):
        logger.info("COPY fast path declined: callable source is a result set, not a table")
        return None

    if (
        incremental
        or source_filter
        or limit
        or (checkpoint and getattr(checkpoint, "chunk_index", 0) > 0)
    ):
        return None

    source_table = source.table or source.collection or ""
    from .stream import _source_name, resolve_dest_table

    dest_table = resolve_dest_table(dest_type, destination, _source_name(source))
    if not source_table or not dest_table:
        return None

    src_n = (src_type or "").strip().lower()
    dest_n = (dest_type or "").strip().lower()
    if src_n in {"postgresql", "postgres"} and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_fast = _try_pg_mysql_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_fast is not None:
            return mysql_fast
        return None

    from services.copy_sqlserver_sqlserver import sqlserver_family_name

    if (
        src_n in {"postgresql", "postgres"}
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_ss = _try_pg_sqlserver_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_ss is not None:
            return pg_ss
        return None

    from services.copy_oracle_oracle import oracle_family_name
    from services.copy_pg_mongo import mongo_family_name
    from services.copy_s3_common import s3_family_name

    if (
        src_n in {"postgresql", "postgres"}
        and oracle_family_name(dest_n) == "oracle"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_ora = _try_pg_oracle_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_ora is not None:
            return pg_ora
        return None

    if src_n in {"postgresql", "postgres"} and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_ice = _try_pg_iceberg_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_ice is not None:
            return pg_ice
        return None

    if src_n in {"postgresql", "postgres"} and mongo_family_name(dest_n) == "mongodb":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_mongo = _try_pg_mongo_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_mongo is not None:
            return pg_mongo
        return None

    if src_n in {"postgresql", "postgres"} and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_sqlite = _try_pg_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_sqlite is not None:
            return pg_sqlite
        return None

    if src_n in {"postgresql", "postgres"} and s3_family_name(dest_n) == "s3":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_s3 = _try_pg_s3_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_s3 is not None:
            return pg_s3
        return None

    if src_n in {"iceberg", "apache_iceberg"} and dest_n in {"postgresql", "postgres"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_pg = _try_iceberg_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_pg is not None:
            return ice_pg
        return None

    if src_n in {"iceberg", "apache_iceberg"} and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_mysql = _try_iceberg_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_mysql is not None:
            return ice_mysql
        return None

    if src_n in {"iceberg", "apache_iceberg"} and sqlserver_family_name(dest_n) == "sqlserver":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_ss = _try_iceberg_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_ss is not None:
            return ice_ss
        return None

    if src_n in {"iceberg", "apache_iceberg"} and oracle_family_name(dest_n) == "oracle":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_ora = _try_iceberg_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_ora is not None:
            return ice_ora
        return None

    if src_n in {"iceberg", "apache_iceberg"} and mongo_family_name(dest_n) == "mongodb":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_mongo = _try_iceberg_mongo_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_mongo is not None:
            return ice_mongo
        return None

    if src_n in {"iceberg", "apache_iceberg"} and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_sqlite = _try_iceberg_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_sqlite is not None:
            return ice_sqlite
        return None

    if src_n in {"iceberg", "apache_iceberg"} and s3_family_name(dest_n) == "s3":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_s3 = _try_iceberg_s3_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_s3 is not None:
            return ice_s3
        return None

    if src_n in {"iceberg", "apache_iceberg"} and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_ice = _try_iceberg_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_ice is not None:
            return ice_ice
        return None

    if src_n == "sqlite" and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_sqlite = _try_sqlite_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_sqlite is not None:
            return sqlite_sqlite
        return None

    if src_n == "sqlite" and dest_n in {"postgresql", "postgres"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_pg = _try_sqlite_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_pg is not None:
            return sqlite_pg
        return None

    if src_n == "sqlite" and s3_family_name(dest_n) == "s3":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_s3 = _try_sqlite_s3_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_s3 is not None:
            return sqlite_s3
        return None

    if src_n == "sqlite" and mongo_family_name(dest_n) == "mongodb":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_mongo = _try_sqlite_mongo_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_mongo is not None:
            return sqlite_mongo
        return None

    if src_n == "sqlite" and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_mysql = _try_sqlite_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_mysql is not None:
            return sqlite_mysql
        return None

    if src_n == "sqlite" and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_ice = _try_sqlite_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_ice is not None:
            return sqlite_ice
        return None

    if src_n == "sqlite" and sqlserver_family_name(dest_n) == "sqlserver":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_ss = _try_sqlite_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_ss is not None:
            return sqlite_ss
        return None

    if src_n == "sqlite" and oracle_family_name(dest_n) == "oracle":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        sqlite_ora = _try_sqlite_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if sqlite_ora is not None:
            return sqlite_ora
        return None

    if s3_family_name(src_n) == "s3" and s3_family_name(dest_n) == "s3":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        s3_s3 = _try_s3_s3_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if s3_s3 is not None:
            return s3_s3
        return None

    if s3_family_name(src_n) == "s3" and dest_n in {"postgresql", "postgres"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        s3_pg = _try_s3_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if s3_pg is not None:
            return s3_pg
        return None

    if s3_family_name(src_n) == "s3" and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        s3_mysql = _try_s3_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if s3_mysql is not None:
            return s3_mysql
        return None

    if s3_family_name(src_n) == "s3" and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        s3_sqlite = _try_s3_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if s3_sqlite is not None:
            return s3_sqlite
        return None

    if s3_family_name(src_n) == "s3" and mongo_family_name(dest_n) == "mongodb":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        s3_mongo = _try_s3_mongo_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if s3_mongo is not None:
            return s3_mongo
        return None

    if s3_family_name(src_n) == "s3" and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        s3_ice = _try_s3_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if s3_ice is not None:
            return s3_ice
        return None

    if mongo_family_name(src_n) == "mongodb" and mongo_family_name(dest_n) == "mongodb":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_mongo = _try_mongo_mongo_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_mongo is not None:
            return mongo_mongo
        return None

    if mongo_family_name(src_n) == "mongodb" and dest_n in {"postgresql", "postgres"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_pg = _try_mongo_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_pg is not None:
            return mongo_pg
        return None

    if mongo_family_name(src_n) == "mongodb" and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_mysql = _try_mongo_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_mysql is not None:
            return mongo_mysql
        return None

    if mongo_family_name(src_n) == "mongodb" and sqlserver_family_name(dest_n) == "sqlserver":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_ss = _try_mongo_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_ss is not None:
            return mongo_ss
        return None

    if mongo_family_name(src_n) == "mongodb" and oracle_family_name(dest_n) == "oracle":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_ora = _try_mongo_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_ora is not None:
            return mongo_ora
        return None

    if mongo_family_name(src_n) == "mongodb" and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_ice = _try_mongo_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_ice is not None:
            return mongo_ice
        return None

    if mongo_family_name(src_n) == "mongodb" and s3_family_name(dest_n) == "s3":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_s3 = _try_mongo_s3_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_s3 is not None:
            return mongo_s3
        return None

    if mongo_family_name(src_n) == "mongodb" and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mongo_sqlite = _try_mongo_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mongo_sqlite is not None:
            return mongo_sqlite
        return None

    if src_n in {"mysql", "mariadb"} and dest_n in {"postgresql", "postgres"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_fast = _try_mysql_pg_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_fast is not None:
            return pg_fast
        return None

    if src_n in {"mysql", "mariadb"} and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_mysql = _try_mysql_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_mysql is not None:
            return mysql_mysql
        return None

    if (
        src_n in {"mysql", "mariadb"}
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_ss = _try_mysql_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_ss is not None:
            return mysql_ss
        return None

    if (
        src_n in {"mysql", "mariadb"}
        and oracle_family_name(dest_n) == "oracle"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_ora = _try_mysql_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_ora is not None:
            return mysql_ora
        return None

    if src_n in {"mysql", "mariadb"} and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_ice = _try_mysql_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_ice is not None:
            return mysql_ice
        return None

    if src_n in {"mysql", "mariadb"} and mongo_family_name(dest_n) == "mongodb":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_mongo = _try_mysql_mongo_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_mongo is not None:
            return mysql_mongo
        return None

    if src_n in {"mysql", "mariadb"} and s3_family_name(dest_n) == "s3":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_s3 = _try_mysql_s3_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_s3 is not None:
            return mysql_s3
        return None

    if src_n in {"mysql", "mariadb"} and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_sqlite = _try_mysql_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_sqlite is not None:
            return mysql_sqlite
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_fast = _try_sqlserver_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_fast is not None:
            return ss_fast
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and dest_n in {"postgresql", "postgres"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_pg = _try_sqlserver_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_pg is not None:
            return ss_pg
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and dest_n in {"mysql", "mariadb"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_mysql = _try_sqlserver_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_mysql is not None:
            return ss_mysql
        return None

    if sqlserver_family_name(src_n) == "sqlserver" and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_sqlite = _try_sqlserver_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_sqlite is not None:
            return ss_sqlite
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and oracle_family_name(dest_n) == "oracle"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_ora = _try_sqlserver_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_ora is not None:
            return ss_ora
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and dest_n in {"iceberg", "apache_iceberg"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_ice = _try_sqlserver_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_ice is not None:
            return ss_ice
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and mongo_family_name(dest_n) == "mongodb"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_mongo = _try_sqlserver_mongo_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_mongo is not None:
            return ss_mongo
        return None

    if oracle_family_name(src_n) == "oracle" and oracle_family_name(dest_n) == "oracle":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_fast = _try_oracle_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_fast is not None:
            return ora_fast
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and dest_n in {"postgresql", "postgres"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_pg = _try_oracle_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_pg is not None:
            return ora_pg
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and dest_n in {"mysql", "mariadb"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_mysql = _try_oracle_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_mysql is not None:
            return ora_mysql
        return None

    if oracle_family_name(src_n) == "oracle" and dest_n == "sqlite":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_sqlite = _try_oracle_sqlite_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_sqlite is not None:
            return ora_sqlite
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_ss = _try_oracle_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_ss is not None:
            return ora_ss
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and dest_n in {"iceberg", "apache_iceberg"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_ice = _try_oracle_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_ice is not None:
            return ora_ice
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and mongo_family_name(dest_n) == "mongodb"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_mongo = _try_oracle_mongo_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_mongo is not None:
            return ora_mongo
        return None

    if not (
        is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)
    ):
        return None

    from services.engine_checksum import comparable_column_pairs, engines_comparable

    if not engines_comparable(src_type, dest_type):
        return None

    from services.copy_fast_path import (
        FastPathUnavailable,
        copy_between_postgres,
        source_column_types,
    )

    # Both sides are described by the source catalog: the destination is created
    # from the source's own declarations, so "identical" is true by construction
    # rather than by comparing two independently resolved spellings.
    try:
        conn = _pg_connect_for_probe(src_cfg)
    except Exception as exc:
        logger.info("COPY fast path declined (source probe): %s", exc)
        return None
    try:
        with conn.cursor() as cur:
            declared = source_column_types(
                cur,
                source.schema or "public",
                source_table,
                [str(m.get("source") or "") for m in mappings if m.get("source")],
            )
    except Exception as exc:
        logger.info("COPY fast path declined (source catalog): %s", exc)
        return None
    finally:
        try:
            conn.close()
        except Exception:  # nosec B110 — probe connection only
            pass

    pairs = comparable_column_pairs(mappings, declared, declared, engine=src_type)
    if not pairs:
        return None
    # ``comparable_column_pairs`` compared the source against itself above, which
    # proves the mapping is a plain carry but not that the *destination* agrees.
    # The destination is created from these same declarations, so it does.
    try:
        result = copy_between_postgres(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=destination.schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            replace_destination=is_overwrite_sync(effective_sync),
        )
    except FastPathUnavailable as exc:
        logger.info("COPY fast path declined: %s", exc)
        return None
    except Exception as exc:
        # A refusal here means the copy ran and did not verify, which is a real
        # finding — never silently retry it on the row path and report success.
        logger.warning("COPY fast path failed: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_binary_server_to_server",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "engine_source_checksum": result.source_checksum,
        "engine_target_checksum": result.target_checksum,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": effective_sync,
        # The digest is only comparable because it was read in the same snapshot
        # as the rows, so the snapshot claim travels with the result.
        "source_snapshot": dict(result.source_snapshot or {}),
        # Secondary indexes reproduced after the load — carried, not dropped, so
        # the destination enforces the same rules and reads at the same cost.
        "indexes_carried": list(result.indexes_carried or ()),
        "copy_split": (result.source_snapshot or {}).get("copy_split") or "binary",
        "shard_mode": (result.source_snapshot or {}).get("shard_mode") or "serial",
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
        "proof_scope": result.proof_scope,
    }
    proof_line = (
        "Proof: mapped-column checksum inside the source snapshot; "
        "destination COUNT(*) equals that snapshot."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY {source_table} → {dest_table} "
        f"({result.source_rows:,} rows, binary, server-to-server)",
        proof_line,
    ]
    if result.indexes_carried:
        ddl_log.append(
            f"Carried {len(result.indexes_carried)} secondary index(es) "
            f"after load: {', '.join(result.indexes_carried)}"
        )
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_mysql_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→MySQL: COPY text + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import (
        copy_postgres_to_mysql,
        mapping_is_plain_carry,
        pg_type_is_load_safe,
    )

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→MySQL COPY declined: %s type %s is not LOAD DATA safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared))

    try:
        result = copy_postgres_to_mysql(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_to_mysql_load_data",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": "full_refresh_append" if not replace_destination else "full_refresh_overwrite",
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    workers = int((result.source_snapshot or {}).get("copy_workers") or 1)
    shard_mode = (result.source_snapshot or {}).get("shard_mode") or "ctid"
    copy_split = (result.source_snapshot or {}).get("copy_split") or shard_mode
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if shard_mode == "pk" and dest_summary.get("partition_proof"):
        skipped = int(dest_summary.get("partitions_skipped") or 0)
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, text COPY + STRICT LOAD DATA, "
        f"{workers} worker(s), copy_split={copy_split}, proof={shard_mode})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_sqlserver_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→SQL Server: COPY text + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
    from services.copy_pg_sqlserver import copy_postgres_to_sqlserver
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→SQL Server COPY declined: %s type %s is not COPY-text safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_postgres_to_sqlserver(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_to_sqlserver_fast_executemany",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY PostgreSQL {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, COPY text + fast_executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_oracle_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→Oracle: COPY text + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
    from services.copy_pg_oracle import copy_postgres_to_oracle
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→Oracle COPY declined: %s type %s is not COPY-text safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_postgres_to_oracle(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_to_oracle_executemany",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = (
        "Proof: destination COUNT(*) equals source snapshot count. "
        "Oracle VARCHAR2 stores empty string as NULL (engine law)."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range. "
            "Oracle VARCHAR2 stores empty string as NULL (engine law)."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY PostgreSQL {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, COPY text + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_iceberg_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→Iceberg: COPY CSV + one snapshot. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_iceberg import copy_postgres_to_iceberg
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_postgres_to_iceberg(
            source_cfg=src_cfg,
            source_schema=source.schema or src_cfg.get("schema") or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_csv_pg_to_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals source snapshot count. "
        "Not scan().count(). Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, COPY CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_mongo_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→Mongo: COPY text + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mongo import copy_postgres_to_mongo, pg_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not pg_mongo_type_is_copy_safe(declared):
            logger.info(
                "PG→Mongo COPY declined: %s type %s is not Mongo COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "TEXT")

    try:
        result = copy_postgres_to_mongo(
            source_cfg=src_cfg,
            source_schema=source.schema or src_cfg.get("schema") or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_write": snapshot.get("mongo_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals source snapshot count. "
        "Not estimatedDocumentCount. Empty dest is insert_many, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, COPY text + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→SQLite: COPY text + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_pg_sqlite import copy_postgres_to_sqlite, pg_sqlite_type_is_copy_safe
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PostgreSQL→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            pg_sqlite_type_is_copy_safe(declared) or sqlite_type_is_copy_safe(declared)
        ):
            logger.info(
                "PostgreSQL→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(declared or "TEXT")

    try:
        result = copy_postgres_to_sqlite(
            source_cfg=src_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("PostgreSQL→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PostgreSQL→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_executemany_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals source snapshot COUNT(*). "
        "Not .import. Empty dest is insert, not upsert. DATE lands as TEXT."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY PostgreSQL {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, COPY text + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_s3_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→S3: COPY CSV + upload_file. Dest artifact COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_pg_s3 import copy_postgres_to_s3, pg_s3_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PostgreSQL→S3 COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    s3_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not pg_s3_type_is_copy_safe(declared):
            logger.info(
                "PostgreSQL→S3 COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        s3_ddls.append(declared or "TEXT")

    try:
        result = copy_postgres_to_s3(
            source_cfg=src_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            s3_ddls=s3_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("PostgreSQL→S3 COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PostgreSQL→S3 COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": snapshot.get("s3_key") or dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_csv_pg_upload_s3",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_write": snapshot.get("s3_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("s3_write") or "insert"
    proof_line = (
        "Proof: S3 dest artifact COUNT equals source snapshot COUNT(*). "
        "Not aws s3 cp. Empty dest is PUT, not upsert. CSV HEADER is not a dest row."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY PostgreSQL {source_table} → S3 {dest_table} "
        f"({result.source_rows:,} rows, COPY CSV + {write} upload, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→PG: snapshot Parquet + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import copy_iceberg_to_postgres, iceberg_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            pg_type_is_load_safe(declared) or iceberg_type_is_copy_safe(declared)
        ):
            logger.info(
                "Iceberg→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_iceberg_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Iceberg source footer COUNT. "
        "Not scan().count()."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + COPY FROM STDIN, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→MySQL: snapshot Parquet + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_mysql import copy_iceberg_to_mysql
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mysql_type_is_copy_safe(declared) or iceberg_type_is_copy_safe(declared)
        ):
            logger.info(
                "Iceberg→MySQL COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_iceberg_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Iceberg source footer COUNT. "
        "Not scan().count()."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + STRICT LOAD DATA, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→SQL Server: snapshot Parquet + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_iceberg_sqlserver import copy_iceberg_to_sqlserver
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            sqlserver_type_is_copy_safe(declared) or iceberg_type_is_copy_safe(declared)
        ):
            logger.info(
                "Iceberg→SQL Server COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_iceberg_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_fast_executemany_sqlserver",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Iceberg source footer COUNT. "
        "Not scan().count()."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + fast_executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→Oracle: snapshot Parquet + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_oracle import copy_iceberg_to_oracle
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            iceberg_type_is_copy_safe(declared) or oracle_type_is_copy_safe(declared)
        ):
            logger.info(
                "Iceberg→Oracle COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_iceberg_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_executemany_oracle",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Iceberg source footer COUNT. "
        "Not scan().count()."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    empty_cells = int(dest_summary.get("empty_string_as_null_cells") or 0)
    if empty_cells:
        proof_line += (
            f" Oracle VARCHAR2 stored {empty_cells} empty string(s) as NULL "
            "(engine law, not a row drop)."
        )
    ddl_log = [
        f"COPY Iceberg {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_mongo_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→Mongo: snapshot Parquet + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_mongo import (
        copy_iceberg_to_mongo,
        iceberg_mongo_type_is_copy_safe,
    )
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not iceberg_mongo_type_is_copy_safe(declared):
            logger.info(
                "Iceberg→Mongo COPY declined: %s type %s is not Mongo COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "string")

    try:
        result = copy_iceberg_to_mongo(
            source_cfg=src_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "mongo_write": snapshot.get("mongo_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals Iceberg source footer COUNT. "
        "Not scan().count(). Not estimatedDocumentCount. Empty dest is insert_many, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→SQLite: snapshot Parquet + executemany. Dest COUNT(*) before commit is the proof."""
    from connectors.sqlite_writer import sqlite_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_iceberg_sqlite import copy_iceberg_to_sqlite
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not iceberg_type_is_copy_safe(declared):
            logger.info(
                "Iceberg→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = sqlite_type(declared) if declared else "TEXT"
        if not sqlite_type_is_copy_safe(dest_ddl):
            logger.info(
                "Iceberg→SQLite COPY declined: dest %s type %s is not SQLite COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(dest_ddl)

    try:
        result = copy_iceberg_to_sqlite(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_executemany_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals Iceberg source footer COUNT. "
        "Not scan().count(). Empty dest is INSERT, not upsert / .import / MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_s3_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→S3: snapshot Parquet + CSV upload. Dest artifact COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_iceberg_s3 import copy_iceberg_to_s3
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→S3 COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    s3_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not iceberg_type_is_copy_safe(declared):
            logger.info(
                "Iceberg→S3 COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        s3_ddls.append(declared or "string")

    try:
        result = copy_iceberg_to_s3(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            s3_ddls=s3_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→S3 COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→S3 COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_upload_s3",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "s3_write": snapshot.get("s3_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("s3_write") or "insert"
    proof_line = (
        "Proof: S3 dest artifact COUNT equals Iceberg source footer COUNT. "
        "Not scan().count() / ListObjects / PUT ack. Empty dest is upload, not aws s3 cp / MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → S3 {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + {write} CSV upload, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→Iceberg: snapshot Parquet + CoW snapshot. Dest footer COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_iceberg import copy_iceberg_to_iceberg
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not iceberg_type_is_copy_safe(declared):
            logger.info(
                "Iceberg→Iceberg COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_iceberg_to_iceberg(
            source_cfg=src_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            dest_schema=dest_schema,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_snapshot_parquet_cow_iceberg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest footer COUNT equals Iceberg source footer COUNT. "
        "Not scan().count(). Not MERGE INTO. Empty dest is CoW append. "
        "Source files are not shared with dest."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→SQLite: ATTACH + INSERT SELECT. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_common import sqlite_type_is_copy_safe
    from services.copy_sqlite_sqlite import copy_sqlite_to_sqlite

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_type_is_copy_safe(declared):
            logger.info(
                "SQLite→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(declared or "TEXT")

    try:
        result = copy_sqlite_to_sqlite(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "attach_insert_select_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals source COUNT(*). "
        "Not .dump / .import. Empty dest is INSERT SELECT, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQLite {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, ATTACH + {write} INSERT SELECT, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→PG: SELECT + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_common import sqlite_pg_type_is_copy_safe
    from services.copy_sqlite_pg import copy_sqlite_to_postgres

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_pg_type_is_copy_safe(declared):
            logger.info(
                "SQLite→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_sqlite_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema,
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlite_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: PostgreSQL dest COUNT(*) equals SQLite source COUNT(*). "
        "Not .dump. Empty dest is COPY FROM STDIN, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQLite {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + COPY FROM STDIN, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_s3_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→S3: SELECT CSV + upload_file. Dest artifact COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_s3 import copy_sqlite_to_s3, sqlite_s3_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→S3 COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    s3_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_s3_type_is_copy_safe(declared):
            logger.info(
                "SQLite→S3 COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        s3_ddls.append(declared or "TEXT")

    try:
        result = copy_sqlite_to_s3(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            s3_ddls=s3_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→S3 COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→S3 COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": snapshot.get("s3_key") or dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlite_upload_s3",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "s3_write": snapshot.get("s3_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("s3_write") or "insert"
    proof_line = (
        "Proof: S3 dest artifact COUNT equals SQLite source COUNT(*). "
        "Not aws s3 cp / .dump. Empty dest is PUT, not upsert. CSV HEADER is not a dest row."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQLite {source_table} → S3 {dest_table} "
        f"({result.source_rows:,} rows, SELECT CSV + {write} upload, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_mongo_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→Mongo: SELECT fetchmany + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_mongo import copy_sqlite_to_mongo, sqlite_mongo_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_mongo_type_is_copy_safe(declared):
            logger.info(
                "SQLite→Mongo COPY declined: %s type %s is not Mongo COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "TEXT")

    try:
        result = copy_sqlite_to_mongo(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlite_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "mongo_write": snapshot.get("mongo_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals SQLite source COUNT(*). "
        "Not estimatedDocumentCount. Not mongoimport / .dump. Empty dest is insert_many, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQLite {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, SELECT + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→MySQL: SELECT TSV + STRICT LOAD DATA. Dest COUNT(*) before commit is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_mysql import copy_sqlite_to_mysql, sqlite_mysql_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_mysql_type_is_copy_safe(declared):
            logger.info(
                "SQLite→MySQL COPY declined: %s type %s is not MySQL COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = mysql_type(declared) if declared else "TEXT"
        if not mysql_type_is_copy_safe(dest_ddl):
            logger.info(
                "SQLite→MySQL COPY declined: dest %s type %s is not LOAD DATA safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(dest_ddl)

    try:
        result = copy_sqlite_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlite_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "load_data": snapshot.get("load_data"),
        "mysql_write": snapshot.get("mysql_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mysql_write") or "insert"
    proof_line = (
        "Proof: MySQL dest COUNT(*) equals SQLite source COUNT(*). "
        "Not .dump / sqlldr. Empty dest is STRICT LOAD DATA, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQLite {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT TSV + {write} LOAD DATA, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→Iceberg: SELECT CSV + snapshot. Dest footer COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_iceberg import (
        copy_sqlite_to_iceberg,
        sqlite_iceberg_type_is_copy_safe,
    )
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_iceberg_type_is_copy_safe(declared):
            logger.info(
                "SQLite→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        iceberg_ddl = ddl_type("iceberg", declared) if declared else "string"
        if iceberg_ddl and not iceberg_type_is_copy_safe(iceberg_ddl):
            logger.info(
                "SQLite→Iceberg COPY declined: dest %s type %s is not Iceberg COPY-safe",
                target_col,
                iceberg_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(iceberg_ddl)

    try:
        result = copy_sqlite_to_iceberg(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlite_csv_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals SQLite source COUNT(*). "
        "Not scan().count(). Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQLite {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, SELECT CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→SQL Server: SELECT + fast_executemany. Dest COUNT(*) is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe
    from services.copy_sqlite_sqlserver import (
        copy_sqlite_to_sqlserver,
        sqlite_sqlserver_type_is_copy_safe,
    )
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQLite→SQL Server COPY declined: %s type %s is not SQL Server COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        if dest_ddl and not sqlserver_type_is_copy_safe(dest_ddl):
            logger.info(
                "SQLite→SQL Server COPY declined: dest %s type %s is not COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(dest_ddl)

    try:
        result = copy_sqlite_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlite_fast_executemany_sqlserver",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "sqlserver_write": snapshot.get("sqlserver_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlserver_write") or "insert"
    proof_line = (
        "Proof: SQL Server dest COUNT(*) equals SQLite source COUNT(*). "
        "Not BCP / BULK INSERT CSV. Empty dest is INSERT, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQLite {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, SELECT + {write} fast_executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlite_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQLite→Oracle: SELECT + executemany. Dest COUNT(*) is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_oracle import (
        copy_sqlite_to_oracle,
        sqlite_declared_to_oracle_ddl,
        sqlite_oracle_type_is_copy_safe,
    )

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQLite→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_oracle_type_is_copy_safe(declared):
            logger.info(
                "SQLite→Oracle COPY declined: %s type %s is not Oracle COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = sqlite_declared_to_oracle_ddl(declared)
        if dest_ddl and not oracle_type_is_copy_safe(dest_ddl):
            logger.info(
                "SQLite→Oracle COPY declined: dest %s type %s is not COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(dest_ddl)

    try:
        result = copy_sqlite_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQLite→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQLite→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlite_executemany_oracle",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlite_read": snapshot.get("sqlite_read"),
        "oracle_write": snapshot.get("oracle_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("oracle_write") or "insert"
    proof_line = (
        "Proof: Oracle dest COUNT(*) equals SQLite source COUNT(*). "
        "Not sqlldr / Data Pump / .dump. Empty dest is INSERT, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    empty_cells = int(dest_summary.get("empty_string_as_null_cells") or 0)
    if empty_cells:
        proof_line += (
            f" Oracle VARCHAR2 stored {empty_cells} empty string(s) as NULL "
            "(engine law, not a row drop)."
        )
    ddl_log = [
        f"COPY SQLite {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, SELECT + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_s3_s3_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity S3→S3: CopyObject. Dest artifact COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_s3_s3 import copy_s3_to_s3

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("S3→S3 COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    s3_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        pairs.append((source_col, target_col))
        s3_ddls.append(declared or "string")

    try:
        result = copy_s3_to_s3(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            s3_ddls=s3_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("S3→S3 COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("S3→S3 COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": snapshot.get("s3_key") or dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_object_s3_s3",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_read": snapshot.get("s3_read"),
        "s3_write": snapshot.get("s3_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("s3_write") or "insert"
    proof_line = (
        "Proof: S3 dest artifact COUNT equals source artifact COUNT. "
        "Not aws s3 cp / aws s3 sync / GET+PUT. Empty dest is CopyObject."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY S3 {source_table} → S3 {dest_table} "
        f"({result.source_rows:,} rows, CopyObject {write}, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_s3_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity S3→PG: GET CSV + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
    from services.copy_s3_pg import copy_s3_to_postgres

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("S3→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not pg_type_is_load_safe(declared):
            logger.info(
                "S3→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_s3_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema,
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("S3→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("S3→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "get_csv_s3_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_read": snapshot.get("s3_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: PostgreSQL dest COUNT(*) equals S3 source artifact COUNT. "
        "Not aws s3 cp. Empty dest is COPY FROM STDIN, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY S3 {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, GET CSV + COPY FROM STDIN, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_s3_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity S3→MySQL: GET CSV + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_s3_mysql import copy_s3_to_mysql

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("S3→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not mysql_type_is_copy_safe(declared):
            logger.info(
                "S3→MySQL COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_s3_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("S3→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("S3→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "get_csv_s3_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_read": snapshot.get("s3_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: MySQL dest COUNT(*) equals S3 source artifact COUNT. "
        "Not aws s3 cp. Empty dest is STRICT LOAD DATA, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY S3 {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, GET CSV + STRICT LOAD DATA, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_s3_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity S3→SQLite: GET CSV + executemany. Dest COUNT(*) is the proof."""
    from connectors.sqlite_writer import sqlite_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_s3_sqlite import copy_s3_to_sqlite
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("S3→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlite_type_is_copy_safe(declared):
            logger.info(
                "S3→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(sqlite_type(declared) if declared else "TEXT")

    try:
        result = copy_s3_to_sqlite(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("S3→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("S3→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "get_csv_s3_executemany_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_read": snapshot.get("s3_read"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals S3 source artifact COUNT. "
        "Not aws s3 cp / .import. Empty dest is executemany insert, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY S3 {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, GET CSV + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_s3_mongo_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity S3→Mongo: GET CSV + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_pg import mongo_type_is_copy_safe
    from services.copy_pg_mongo import pg_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_s3_mongo import copy_s3_to_mongo

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("S3→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_type_is_copy_safe(declared) or pg_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "S3→Mongo COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "TEXT")

    try:
        result = copy_s3_to_mongo(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("S3→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("S3→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "get_csv_s3_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_read": snapshot.get("s3_read"),
        "mongo_write": snapshot.get("mongo_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals S3 source artifact COUNT. "
        "Not estimatedDocumentCount. Not aws s3 cp / mongoimport. Empty dest is insert, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY S3 {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, GET CSV + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_s3_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity S3→Iceberg: GET CSV + snapshot. Dest footer COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_s3_iceberg import copy_s3_to_iceberg
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("S3→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        dest_ddl = ddl_type("iceberg", declared) if declared else "string"
        if declared and not iceberg_type_is_copy_safe(declared) and not iceberg_type_is_copy_safe(dest_ddl):
            logger.info(
                "S3→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        if dest_ddl and not iceberg_type_is_copy_safe(dest_ddl):
            logger.info(
                "S3→Iceberg COPY declined: dest %s type %s is not Iceberg COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(dest_ddl)

    try:
        result = copy_s3_to_iceberg(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("S3→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("S3→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "get_csv_s3_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_read": snapshot.get("s3_read"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals S3 source artifact COUNT. "
        "Not scan().count() / ListObjects. Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY S3 {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, GET CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→PG: snapshot find + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_pg import copy_mongo_to_postgres, mongo_type_is_copy_safe
    from services.copy_pg_mongo import pg_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_type_is_copy_safe(declared) or pg_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "Mongo→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_mongo_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema,
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "mongo_snapshot_find_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Mongo source snapshot count_documents. "
        "Not estimatedDocumentCount."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MongoDB {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, snapshot find + COPY FROM STDIN, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→MySQL: snapshot find + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_mysql import copy_mongo_to_mysql
    from services.copy_mongo_pg import mongo_type_is_copy_safe
    from services.copy_mysql_mongo import mysql_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_type_is_copy_safe(declared) or mysql_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "Mongo→MySQL COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_mongo_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "mongo_snapshot_find_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Mongo source snapshot count_documents. "
        "Not estimatedDocumentCount."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MongoDB {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, snapshot find + STRICT LOAD DATA, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→SQL Server: snapshot find + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_pg import mongo_type_is_copy_safe
    from services.copy_mongo_sqlserver import copy_mongo_to_sqlserver
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_mongo import sqlserver_mongo_type_is_copy_safe
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_type_is_copy_safe(declared) or sqlserver_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "Mongo→SQL Server COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_mongo_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "dbo",
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "mongo_snapshot_find_fast_executemany_sqlserver",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Mongo source snapshot count_documents. "
        "Not estimatedDocumentCount. Not BCP / BULK INSERT CSV."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MongoDB {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, snapshot find + fast_executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→Oracle: snapshot find + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_oracle import copy_mongo_to_oracle
    from services.copy_mongo_pg import mongo_type_is_copy_safe
    from services.copy_oracle_mongo import oracle_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_type_is_copy_safe(declared) or oracle_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "Mongo→Oracle COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_mongo_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "DATAFLOW",
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "mongo_snapshot_find_executemany_oracle",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Mongo source snapshot count_documents. "
        "Not estimatedDocumentCount. Not sqlldr / Data Pump."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    empty_cells = int(dest_summary.get("empty_string_as_null_cells") or 0)
    if empty_cells:
        proof_line += (
            f" Oracle VARCHAR2 stored {empty_cells:,} empty-string cell(s) as NULL "
            "(engine law, not a row drop)."
        )
    ddl_log = [
        f"COPY MongoDB {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, snapshot find + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_mongo_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→Mongo: snapshot find + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_mongo import copy_mongo_to_mongo, mongo_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not mongo_mongo_type_is_copy_safe(declared):
            logger.info(
                "Mongo→Mongo COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "string")

    try:
        result = copy_mongo_to_mongo(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "mongo_snapshot_find_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "mongo_write": snapshot.get("mongo_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals source snapshot count_documents. "
        "Not estimatedDocumentCount. Empty dest is insert_many, not upsert / $out."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MongoDB {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, snapshot find + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→Iceberg: snapshot find + CSV + snapshot. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_mongo import iceberg_mongo_type_is_copy_safe
    from services.copy_mongo_iceberg import copy_mongo_to_iceberg
    from services.copy_mongo_pg import mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_type_is_copy_safe(declared) or iceberg_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "Mongo→Iceberg COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_mongo_to_iceberg(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "mongo_snapshot_find_csv_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest footer COUNT equals Mongo source snapshot count_documents. "
        "Not scan().count(). Not estimatedDocumentCount. Empty dest is CoW append, not MERGE."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MongoDB {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, snapshot find + CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_s3_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→S3: snapshot find CSV + upload_file. Dest artifact COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_s3 import copy_mongo_to_s3, mongo_s3_type_is_copy_safe
    from services.copy_pg_mongo import pg_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→S3 COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    s3_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_s3_type_is_copy_safe(declared) or pg_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "Mongo→S3 COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        s3_ddls.append(declared or "TEXT")

    try:
        result = copy_mongo_to_s3(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            s3_ddls=s3_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→S3 COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→S3 COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": snapshot.get("s3_key") or dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "snapshot_find_mongo_upload_s3",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "s3_write": snapshot.get("s3_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("s3_write") or "insert"
    proof_line = (
        "Proof: S3 dest artifact COUNT equals Mongo source snapshot count_documents. "
        "Not estimatedDocumentCount. Not mongoexport / aws s3 cp. Empty dest is PUT, not upsert. "
        "CSV HEADER is not a dest row."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MongoDB {source_table} → S3 {dest_table} "
        f"({result.source_rows:,} rows, snapshot find CSV + {write} upload, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mongo_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Mongo→SQLite: snapshot find + executemany. Dest COUNT(*) before commit is the proof."""
    from connectors.sqlite_writer import sqlite_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_pg import mongo_type_is_copy_safe
    from services.copy_mongo_sqlite import copy_mongo_to_sqlite
    from services.copy_pg_mongo import pg_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Mongo→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            mongo_type_is_copy_safe(declared) or pg_mongo_type_is_copy_safe(declared)
        ):
            logger.info(
                "Mongo→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = sqlite_type(declared) if declared else "TEXT"
        if not sqlite_type_is_copy_safe(dest_ddl):
            logger.info(
                "Mongo→SQLite COPY declined: dest %s type %s is not SQLite COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(dest_ddl)

    try:
        result = copy_mongo_to_sqlite(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Mongo→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Mongo→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "snapshot_find_mongo_executemany_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_read": snapshot.get("mongo_read"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals Mongo source snapshot count_documents. "
        "Not estimatedDocumentCount. Not mongoexport / .import. Empty dest is executemany insert, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MongoDB {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, snapshot find + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→PG: SELECT + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_pg import (
        copy_sqlserver_to_postgres,
        sqlserver_type_is_copy_safe,
    )

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_sqlserver_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "sqlserver_isolation": (result.source_snapshot or {}).get("sqlserver_isolation"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    isolation = dest_summary.get("sqlserver_isolation") or "holdlock"
    ddl_log = [
        f"COPY SQL Server {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + COPY FROM STDIN, {isolation})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_pg_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→PG: SELECT + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_pg import copy_mysql_to_postgres, mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared))

    try:
        result = copy_mysql_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_mysql_to_pg_stdin",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": "full_refresh_append" if not replace_destination else "full_refresh_overwrite",
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "tsv_encoder": (result.source_snapshot or {}).get("tsv_encoder"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
    ddl_log = [
        f"COPY MySQL {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + COPY FROM STDIN)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→MySQL: INSERT SELECT or STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_mysql import copy_mysql_to_mysql
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→MySQL copy declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_mysql_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→MySQL copy declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→MySQL copy failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    split = str((result.source_snapshot or {}).get("copy_split") or "")
    load_method = (
        "insert_select_mysql_same_instance"
        if split == "insert_select"
        else "copy_text_mysql_to_mysql_load_data"
    )
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": load_method,
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": "full_refresh_append" if not replace_destination else "full_refresh_overwrite",
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
    how = (
        "INSERT SELECT (same instance)"
        if split == "insert_select"
        else "SELECT + STRICT LOAD DATA"
    )
    ddl_log = [
        f"COPY MySQL {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, {how})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→SQL Server: SELECT + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_mysql_sqlserver import copy_mysql_to_sqlserver
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→SQL Server COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_mysql_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_fast_executemany_sqlserver",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY MySQL {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, SELECT + fast_executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→MySQL: SELECT + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_mysql import copy_sqlserver_to_mysql
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→MySQL COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_sqlserver_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlserver_isolation": snapshot.get("sqlserver_isolation"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY SQL Server {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + STRICT LOAD DATA, tempfile)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→SQLite: HOLDLOCK SELECT + executemany. Dest COUNT(*) before commit is the proof."""
    from connectors.sqlite_writer import sqlite_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe
    from services.copy_sqlserver_sqlite import copy_sqlserver_to_sqlite
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = sqlite_type(declared) if declared else "TEXT"
        if not sqlite_type_is_copy_safe(dest_ddl):
            logger.info(
                "SQL Server→SQLite COPY declined: dest %s type %s is not SQLite COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(dest_ddl)

    try:
        result = copy_sqlserver_to_sqlite(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_executemany_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlserver_read": snapshot.get("sqlserver_read"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals SQL Server source snapshot COUNT. "
        "Not BCP / .import. Empty dest is INSERT, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQL Server {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, HOLDLOCK SELECT + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→Oracle: SELECT + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_oracle import copy_mysql_to_oracle
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→Oracle COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_mysql_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_executemany_oracle",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    empty_cells = int(dest_summary.get("empty_string_as_null_cells") or 0)
    if empty_cells:
        proof_line += (
            f" Oracle VARCHAR2 stored {empty_cells} empty string(s) as NULL "
            "(engine law, not a row drop)."
        )
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY MySQL {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, SELECT + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→Iceberg: SELECT + CSV + snapshot. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_iceberg import copy_mysql_to_iceberg
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_mysql_to_iceberg(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_csv_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals source snapshot count. "
        "Not scan().count(). Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MySQL {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, SELECT + CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_mongo_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→Mongo: SELECT + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_mongo import copy_mysql_to_mongo, mysql_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not mysql_mongo_type_is_copy_safe(declared):
            logger.info(
                "MySQL→Mongo COPY declined: %s type %s is not Mongo COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "TEXT")

    try:
        result = copy_mysql_to_mongo(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_write": snapshot.get("mongo_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals source snapshot count. "
        "Not estimatedDocumentCount. Empty dest is insert_many, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MySQL {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, SELECT + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_s3_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→S3: SELECT CSV + upload_file. Dest artifact COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_s3 import copy_mysql_to_s3, mysql_s3_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→S3 COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    s3_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not mysql_s3_type_is_copy_safe(declared):
            logger.info(
                "MySQL→S3 COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        s3_ddls.append(declared or "TEXT")

    try:
        result = copy_mysql_to_s3(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            s3_ddls=s3_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→S3 COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→S3 COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": snapshot.get("s3_key") or dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_upload_s3",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "s3_write": snapshot.get("s3_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("s3_write") or "insert"
    proof_line = (
        "Proof: S3 dest artifact COUNT equals source snapshot COUNT(*). "
        "Not aws s3 cp. Empty dest is PUT, not upsert. CSV HEADER is not a dest row."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MySQL {source_table} → S3 {dest_table} "
        f"({result.source_rows:,} rows, SELECT CSV + {write} upload, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→SQLite: snapshot SELECT + executemany. Dest COUNT(*) before commit is the proof."""
    from connectors.sqlite_writer import sqlite_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_mysql_sqlite import copy_mysql_to_sqlite
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = sqlite_type(declared) if declared else "TEXT"
        if not sqlite_type_is_copy_safe(dest_ddl):
            logger.info(
                "MySQL→SQLite COPY declined: dest %s type %s is not SQLite COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(dest_ddl)

    try:
        result = copy_mysql_to_sqlite(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_executemany_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mysql_read": snapshot.get("mysql_read"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals MySQL source snapshot COUNT(*). "
        "Not mysqldump / .import. Empty dest is executemany insert, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MySQL {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, snapshot SELECT + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_mongo_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→Mongo: SELECT + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_mongo import (
        copy_sqlserver_to_mongo,
        sqlserver_mongo_type_is_copy_safe,
    )

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not sqlserver_mongo_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→Mongo COPY declined: %s type %s is not Mongo COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "NVARCHAR(MAX)")

    try:
        result = copy_sqlserver_to_mongo(
            source_cfg=src_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_write": snapshot.get("mongo_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals source snapshot count. "
        "Not estimatedDocumentCount. Empty dest is insert_many, not upsert. Not BCP."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQL Server {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, SELECT + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→Iceberg: SELECT + CSV + snapshot. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_iceberg import copy_sqlserver_to_iceberg
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_sqlserver_to_iceberg(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_csv_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals source snapshot count. "
        "Not scan().count(). Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY SQL Server {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, SELECT + CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→Iceberg: SELECT + CSV + snapshot. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import iceberg_type_is_copy_safe
    from services.copy_oracle_iceberg import copy_oracle_to_iceberg
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            oracle_type_is_copy_safe(declared) or iceberg_type_is_copy_safe(declared)
        ):
            logger.info(
                "Oracle→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_oracle_to_iceberg(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_csv_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals source snapshot count. "
        "Not scan().count(). Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Oracle {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, SELECT + CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_mongo_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→Mongo: SELECT + insert_many. Dest count_documents is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_mongo import copy_oracle_to_mongo, oracle_mongo_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→Mongo COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mongo_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not oracle_mongo_type_is_copy_safe(declared):
            logger.info(
                "Oracle→Mongo COPY declined: %s type %s is not Mongo COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mongo_ddls.append(declared or "VARCHAR2(4000)")

    try:
        result = copy_oracle_to_mongo(
            source_cfg=src_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mongo_ddls=mongo_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→Mongo COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→Mongo COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_insert_many_mongo",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "mongo_write": snapshot.get("mongo_write"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("mongo_write") or "insert"
    proof_line = (
        "Proof: Mongo dest count_documents equals source snapshot count. "
        "Not estimatedDocumentCount. Empty dest is insert_many, not upsert. Not sqlldr."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Oracle {source_table} → MongoDB {dest_table} "
        f"({result.source_rows:,} rows, SHARE-lock SELECT + {write} insert_many, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→MySQL: SELECT + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_mysql import copy_oracle_to_mysql
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not oracle_type_is_copy_safe(declared):
            logger.info(
                "Oracle→MySQL COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_oracle_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY Oracle {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + STRICT LOAD DATA, tempfile, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_sqlite_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→SQLite: SHARE-lock SELECT + executemany. Dest COUNT(*) before commit is the proof."""
    from connectors.sqlite_writer import sqlite_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_oracle_sqlite import copy_oracle_to_sqlite
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→SQLite COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlite_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not oracle_type_is_copy_safe(declared):
            logger.info(
                "Oracle→SQLite COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        dest_ddl = sqlite_type(declared) if declared else "TEXT"
        if not sqlite_type_is_copy_safe(dest_ddl):
            logger.info(
                "Oracle→SQLite COPY declined: dest %s type %s is not SQLite COPY-safe",
                target_col,
                dest_ddl,
            )
            return None
        pairs.append((source_col, target_col))
        sqlite_ddls.append(dest_ddl)

    try:
        result = copy_oracle_to_sqlite(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→SQLite COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→SQLite COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_executemany_sqlite",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "oracle_read": snapshot.get("oracle_read"),
        "sqlite_write": snapshot.get("sqlite_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("sqlite_write") or "insert"
    proof_line = (
        "Proof: SQLite dest COUNT(*) equals Oracle source snapshot COUNT. "
        "Not sqlldr / .import. Empty dest is INSERT, not upsert."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Oracle {source_table} → SQLite {dest_table} "
        f"({result.source_rows:,} rows, SHARE-lock SELECT + {write} executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→Oracle: SELECT + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_oracle import copy_sqlserver_to_oracle
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→Oracle COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_sqlserver_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_executemany_oracle",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlserver_isolation": snapshot.get("sqlserver_isolation"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    empty_cells = int(dest_summary.get("empty_string_as_null_cells") or 0)
    if empty_cells:
        proof_line += (
            f" Oracle VARCHAR2 stored {empty_cells} empty string(s) as NULL "
            "(engine law, not a row drop)."
        )
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY SQL Server {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, SELECT + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→SQL Server: SELECT + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_oracle_sqlserver import copy_oracle_to_sqlserver
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not oracle_type_is_copy_safe(declared):
            logger.info(
                "Oracle→SQL Server COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_oracle_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_fast_executemany_sqlserver",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY Oracle {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, SELECT + fast_executemany, "
        f"copy_split={split}, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→SQL Server: INSERT SELECT. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_sqlserver import copy_sqlserver_to_sqlserver
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→SQL Server copy declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_sqlserver_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→SQL Server copy declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→SQL Server copy failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "insert_select_sqlserver_same_instance",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "sqlserver_isolation": (result.source_snapshot or {}).get("sqlserver_isolation"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        skipped = int(dest_summary.get("partitions_skipped") or 0)
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    isolation = dest_summary.get("sqlserver_isolation") or "holdlock"
    ddl_log = [
        f"COPY SQL Server {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, INSERT SELECT, {isolation})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→Oracle: INSERT SELECT. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_oracle import copy_oracle_to_oracle
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→Oracle copy declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_oracle_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→Oracle copy declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→Oracle copy failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "insert_select_oracle_same_instance",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "oracle_lock": (result.source_snapshot or {}).get("oracle_lock"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        skipped = int(dest_summary.get("partitions_skipped") or 0)
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY Oracle {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, INSERT SELECT, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→PG: SELECT + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_pg import copy_oracle_to_postgres, oracle_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not oracle_type_is_copy_safe(declared):
            logger.info(
                "Oracle→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_oracle_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY Oracle {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + COPY FROM STDIN, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _pg_connect_for_probe(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection

    return get_connection(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )
