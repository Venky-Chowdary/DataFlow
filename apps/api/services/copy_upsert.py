"""Identity COPY upsert: staging bulk load + dest MERGE, dest COUNT independently reread.

Append/overwrite COPY proves dest COUNT(*) equals the source snapshot. Upsert
cannot use that proof: occupied dest may already hold keys the snapshot does
not touch. The conservation that does not lie:

1. Staging is empty, then filled by the same identity COPY path.
2. Staging COUNT(*) equals the source snapshot.
3. INSERT ... ON DUPLICATE / ON CONFLICT applies those rows onto dest.
4. Dest PK ⋈ staging PK COUNT equals the staging COUNT (every staged key
   is present on dest after the merge).
5. Dest COUNT(*) is independently reread and recorded — never required to
   equal the source.

CDC / mirror / SCD2 stay on the row path. This module is ``upsert`` only.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable

logger = logging.getLogger(__name__)

STAGING_PREFIX = "_df_stg_"
UPSERT_PROOF_SCOPE = "staging_count_equals_source_and_dest_pk_join_equals_staging"


def staging_table_name(dest_table: str, *, max_len: int = 64) -> str:
    """Deterministic staging ident. MySQL's identifier cap is 64."""
    raw = f"{STAGING_PREFIX}{dest_table}"
    if len(raw) <= max_len:
        return raw
    digest = hashlib.sha1(dest_table.encode("utf-8")).hexdigest()[:8]
    keep = max(1, max_len - len(STAGING_PREFIX) - 1 - len(digest))
    return f"{STAGING_PREFIX}{dest_table[:keep]}_{digest}"


def mysql_upsert_from_staging_sql(
    dest_q: str,
    staging_q: str,
    columns: list[str],
    pk_col: str,
    quote,
) -> str:
    cols = ", ".join(quote(c) for c in columns)
    updates = ", ".join(
        f"{quote(c)}=VALUES({quote(c)})"
        for c in columns
        if c.lower() != pk_col.lower()
    )
    if not updates:
        return (
            f"INSERT IGNORE INTO {dest_q} ({cols}) "
            f"SELECT {cols} FROM {staging_q}"
        )
    return (
        f"INSERT INTO {dest_q} ({cols}) SELECT {cols} FROM {staging_q} "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )


def pg_upsert_from_staging_sql(
    dest_ref: str,
    staging_ref: str,
    columns: list[str],
    pk_col: str,
    quote,
) -> str:
    cols = ", ".join(quote(c) for c in columns)
    updates = ", ".join(
        f"{quote(c)}=EXCLUDED.{quote(c)}"
        for c in columns
        if c.lower() != pk_col.lower()
    )
    conflict = quote(pk_col)
    if not updates:
        return (
            f"INSERT INTO {dest_ref} ({cols}) SELECT {cols} FROM {staging_ref} "
            f"ON CONFLICT ({conflict}) DO NOTHING"
        )
    return (
        f"INSERT INTO {dest_ref} ({cols}) SELECT {cols} FROM {staging_ref} "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    )


def sqlite_upsert_from_staging_sql(
    dest_q: str,
    staging_q: str,
    columns: list[str],
    pk_col: str,
    quote,
) -> str:
    cols = ", ".join(quote(c) for c in columns)
    updates = ", ".join(
        f"{quote(c)}=excluded.{quote(c)}"
        for c in columns
        if c.lower() != pk_col.lower()
    )
    conflict = quote(pk_col)
    if not updates:
        return (
            f"INSERT INTO {dest_q} ({cols}) SELECT {cols} FROM {staging_q} WHERE true "
            f"ON CONFLICT ({conflict}) DO NOTHING"
        )
    return (
        f"INSERT INTO {dest_q} ({cols}) SELECT {cols} FROM {staging_q} WHERE true "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    )


def pk_join_count_sql(dest_q: str, staging_q: str, pk_ident: str) -> str:
    return (
        f"SELECT COUNT(*) FROM {dest_q} d "
        f"INNER JOIN {staging_q} s ON d.{pk_ident} = s.{pk_ident}"
    )


def _result_with_upsert_proof(
    result: FastPathResult,
    *,
    join_count: int,
    dest_count: int,
    staging_table: str,
    dest_table: str,
) -> FastPathResult:
    if join_count != int(result.source_rows):
        raise ValueError(
            "COPY upsert refused: dest PK ⋈ staging COUNT "
            f"{join_count} != source snapshot {result.source_rows}"
        )
    snapshot = dict(result.source_snapshot or {})
    snapshot["upsert"] = True
    snapshot["staging_table"] = staging_table
    snapshot["dest_table"] = dest_table
    snapshot["dest_count"] = dest_count
    snapshot["pk_join_count"] = join_count
    proof = f"pk_join_count:{join_count}"
    return FastPathResult(
        rows_copied=result.source_rows,
        source_rows=result.source_rows,
        source_checksum=proof,
        target_rows=join_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        indexes_carried=result.indexes_carried,
        proof_scope=UPSERT_PROOF_SCOPE,
    )


def copy_postgres_to_mysql_upsert(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
) -> FastPathResult:
    """LOAD DATA into staging, then INSERT ... ON DUPLICATE KEY UPDATE dest."""
    from services.copy_fast_path import source_table_shape
    from services.copy_pg_mysql import (
        _mysql_connect,
        _mysql_create_sql,
        _mysql_ident,
        _pg_connect,
        copy_postgres_to_mysql,
        mapped_single_pk,
    )

    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_conn = _pg_connect(source_cfg)
    try:
        source_conn.autocommit = True
        with source_conn.cursor() as src_cur:
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
        pk_map = mapped_single_pk(list(shape.primary_key or []), pairs)
        if pk_map is None:
            raise FastPathUnavailable(
                "COPY upsert requires exactly one mapped primary key"
            )
        _src_pk, dest_pk = pk_map
    finally:
        source_conn.close()

    dest_q = _mysql_ident(dest_table)
    staging = staging_table_name(dest_table)
    staging_q = _mysql_ident(staging)
    pk_ident = _mysql_ident(dest_pk)
    dest_conn = _mysql_connect(dest_cfg)
    created_dest = False
    try:
        with dest_conn.cursor() as dst_cur:
            dst_cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
                (dest_table,),
            )
            if dst_cur.fetchone() is None:
                create_sql = _mysql_create_sql(
                    dest_table, pairs, mysql_ddls, [dest_pk]
                )
                dst_cur.execute(create_sql)  # nosec B608
                dest_conn.commit()
                created_dest = True
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql upsert dest probe close skipped", exc_info=True)

    dest_conn = None
    try:
        result = copy_postgres_to_mysql(
            source_cfg=source_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=staging,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=True,
        )
        # COPY created/dropped staging on a different session. Reopen so this
        # transaction does not see MySQL 1412 (table definition changed).
        dest_conn = _mysql_connect(dest_cfg)
        with dest_conn.cursor() as dst_cur:
            dst_cur.execute(
                mysql_upsert_from_staging_sql(
                    dest_q, staging_q, target_cols, dest_pk, _mysql_ident
                )
            )
            dst_cur.execute(pk_join_count_sql(dest_q, staging_q, pk_ident))
            join_count = int(dst_cur.fetchone()[0])
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
            dest_count = int(dst_cur.fetchone()[0])
            dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
            dest_conn.commit()
        return _result_with_upsert_proof(
            result,
            join_count=join_count,
            dest_count=dest_count,
            staging_table=staging,
            dest_table=dest_table,
        )
    except Exception:
        cleanup = dest_conn or _mysql_connect(dest_cfg)
        try:
            with cleanup.cursor() as dst_cur:
                dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
                if created_dest:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            cleanup.commit()
        except Exception:
            logger.debug("mysql upsert cleanup skipped", exc_info=True)
        if cleanup is not dest_conn:
            try:
                cleanup.close()
            except Exception:
                logger.debug("mysql upsert cleanup close skipped", exc_info=True)
        raise
    finally:
        if dest_conn is not None:
            try:
                dest_conn.close()
            except Exception:
                logger.debug("mysql upsert dest close skipped", exc_info=True)


def copy_between_postgres_upsert(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
) -> FastPathResult:
    """Binary COPY into UNLOGGED staging, then INSERT ... ON CONFLICT dest."""
    from services.copy_fast_path import (
        _connect,
        _quote,
        _table_ref,
        copy_between_postgres,
        create_destination_like_source,
        source_table_shape,
    )
    from services.copy_pg_mysql import mapped_single_pk as _mapped_pk

    if not pairs:
        raise FastPathUnavailable("no comparable columns")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_conn = _connect(source_cfg)
    dest_conn = _connect(dest_cfg)
    dest_ref = _table_ref(dest_schema, dest_table)
    staging = staging_table_name(dest_table)
    staging_ref = _table_ref(dest_schema, staging)
    created_dest = False
    try:
        source_conn.autocommit = True
        dest_conn.autocommit = True
        with source_conn.cursor() as src_cur, dest_conn.cursor() as dst_cur:
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
            pk_map = _mapped_pk(list(shape.primary_key or []), pairs)
            if pk_map is None:
                raise FastPathUnavailable(
                    "COPY upsert requires exactly one mapped primary key"
                )
            _src_pk, dest_pk = pk_map
            dst_cur.execute(
                "SELECT to_regclass(%s)",
                (f"{dest_schema or 'public'}.{dest_table}",),
            )
            if dst_cur.fetchone()[0] is None:
                create_destination_like_source(
                    dst_cur, dest_schema, dest_table, pairs, shape
                )
                created_dest = True
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("pg upsert source probe close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("pg upsert dest probe close skipped", exc_info=True)

    result = copy_between_postgres(
        source_cfg=source_cfg,
        source_schema=source_schema,
        source_table=source_table,
        dest_cfg=dest_cfg,
        dest_schema=dest_schema,
        dest_table=staging,
        pairs=pairs,
        replace_destination=True,
    )

    dest_conn = _connect(dest_cfg)
    try:
        dest_conn.autocommit = False
        with dest_conn.cursor() as dst_cur:
            dst_cur.execute(
                pg_upsert_from_staging_sql(
                    dest_ref, staging_ref, target_cols, dest_pk, _quote
                )
            )
            pk_ident = _quote(dest_pk)
            dst_cur.execute(pk_join_count_sql(dest_ref, staging_ref, pk_ident))
            join_count = int(dst_cur.fetchone()[0])
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count = int(dst_cur.fetchone()[0])
            dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
            dest_conn.commit()
        return _result_with_upsert_proof(
            result,
            join_count=join_count,
            dest_count=dest_count,
            staging_table=staging,
            dest_table=dest_table,
        )
    except Exception:
        dest_conn.rollback()
        if created_dest:
            try:
                dest_conn.autocommit = True
                with dest_conn.cursor() as dst_cur:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                    dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
            except Exception:
                logger.debug("pg upsert dest cleanup skipped", exc_info=True)
        else:
            try:
                dest_conn.autocommit = True
                with dest_conn.cursor() as dst_cur:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
            except Exception:
                logger.debug("pg upsert staging cleanup skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("pg upsert dest close skipped", exc_info=True)
