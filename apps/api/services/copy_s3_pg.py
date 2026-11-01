"""S3 CSV GET → PostgreSQL COPY FROM STDIN (cross-engine bulk).

Source COUNT is object-store artifact COUNT of the CSV (header skipped),
never ListObjects length. Payload is one GET into a tempfile, then
``COPY … FROM STDIN`` CSV HEADER. Dest ``COUNT(*)`` must equal that
source COUNT. Empty dest COPYs once. Occupied dest whose COUNT already
equals the source COUNT is skip-complete. Occupied dest with a different
COUNT declines. JSON/JSONL/Parquet stay on the row path (CSV is the
COPY-native wire). This is **not** ``aws s3 cp``.

Declines (row path keeps quarantine): transforms that change values,
non-CSV source, public proxy, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote
from services.copy_fast_path import _table_ref as _pg_table_ref
from services.copy_mysql_pg import _pg_connect, _pg_create_sql
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_dest_count,
    s3_ext,
    s3_file_has_legacy_null,
    s3_list_keys,
)
from services.copy_sql_pairs import pg_ident as _pg_ident

logger = logging.getLogger(__name__)


def s3_pg_copy_enabled() -> bool:
    raw = (getenv_brand("S3_PG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _census_csv_cells(path: str, ext: str, key: str, pg_ddls: list[str]) -> None:
    """Decline before COPY when any cell is one the row path would not load as-is.

    ``COPY`` parses the file all-or-nothing, so one ``2024-02-30`` (or a quoted
    ``""`` in a DATE column) failed the whole run with nothing quarantined. The
    local-file COPY already grades every cell (``_write_mapped_csv``); this is
    the same rule - the shared blank-cell bind, then ``text_cell_copy_safe`` -
    on the object GET. A decline hands the file to the row path, which holds
    out exactly the unfit rows.
    """
    from services.copy_fast_path import text_cell_copy_safe
    from services.csv_codec import iter_csv_records
    try:
        from transfer.file_stream import file_empty_cell_bind
    except ImportError:  # pragma: no cover - api root on PYTHONPATH
        from src.transfer.file_stream import file_empty_cell_bind

    with open(path, "r", encoding="utf-8", newline="") as handle:
        rows = iter_csv_records(handle, "\t" if ext == "tsv" else ",")
        next(rows, None)  # HEADER true
        for row_no, row in enumerate(rows, start=1):
            for col, ddl in enumerate(pg_ddls):
                cell = row[col] if col < len(row) else None
                # A quoted "" is '' to COPY but NULL to the row path on a
                # typed carrier (the shared blank-cell bind): COPY would
                # store or reject a value the row path never writes.
                blank_differs = cell == "" and file_empty_cell_bind("", ddl) is None
                if blank_differs or not text_cell_copy_safe(
                    cell, ddl, physical=ddl, dest_db="postgresql"
                ):
                    raise FastPathUnavailable(
                        f"{key!r} column {col + 1} cell {row[col]!r} (data row {row_no}) "
                        f"is not {ddl} COPY-safe; the row path owns its validation "
                        "and quarantine"
                    )


def copy_s3_to_postgres(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """GET S3 CSV into PostgreSQL COPY FROM STDIN. Dest COUNT(*) is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_pg_copy_enabled():
        raise FastPathUnavailable("S3→PostgreSQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: COPY FROM STDIN not assumed")

    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if s3_ext(key) not in {"csv", "tsv"}:
            raise FastPathUnavailable(
                f"source object {key!r} is not CSV/TSV (S3→PG COPY wire)"
            )

    source_count = s3_dest_count(source_cfg, source_table)
    target_cols = [p[1] for p in pairs]
    dest_schema_n = dest_schema or dest_cfg.get("schema") or "public"
    dest_ref = _pg_table_ref(dest_schema_n, dest_table)

    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    tmp_paths: list[str] = []
    try:
        dst_cur = dest_conn.cursor()
        dst_cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s LIMIT 1",
            (dest_schema_n, dest_table),
        )
        exists = dst_cur.fetchone() is not None
        dest_occupied = False
        swap_at_end = False
        swap_schema = dest_schema_n
        swap_dest_ref = dest_ref
        swap_dest_table = dest_table
        if replace_destination and exists:
            # C03/X-OW2 — stage the overwrite, rename-swap inside the run
            # transaction: the old dest stays visible until COMMIT and a
            # failed copy rolls back to it instead of leaving nothing.
            from services.dest_overwrite_swap import (
                overwrite_trash_name,
                staged_overwrite_name,
            )

            stage_table = staged_overwrite_name(dest_table)
            swap_trash_table = overwrite_trash_name(dest_table)
            dest_ref = _pg_table_ref(swap_schema, stage_table)
            dest_table = stage_table
            swap_at_end = True
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied PostgreSQL dest stays on the row path "
                    "(identity COPY would duplicate)"
                )
        else:
            dst_cur.execute(
                _pg_create_sql(dest_schema_n, dest_table, pairs, pg_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        col_list = ", ".join(_pg_ident(c) for c in target_cols)
        client = s3_client(source_cfg)
        bucket = s3_bucket(source_cfg)
        for src_key in src_keys:
            ext = s3_ext(src_key)
            delim = "E'\\t'" if ext == "tsv" else "','"
            fd, tmp_path = tempfile.mkstemp(prefix="df-s3-pg-", suffix=f".{ext}")
            os.close(fd)
            tmp_paths.append(tmp_path)
            client.download_file(bucket, src_key, tmp_path)
            if s3_file_has_legacy_null(tmp_path):
                raise FastPathUnavailable(
                    f"source object {src_key!r} spells NULL as \\N; the row "
                    "path's CSV codec reads it (COPY's CSV NULL is the empty field)"
                )
            _census_csv_cells(tmp_path, ext, src_key, pg_ddls)
            # C04 is PostgreSQL's own CSV rule: NULL '' makes an unquoted empty
            # field NULL and a quoted "" the empty string. NULL '\N' landed
            # every NULL as '' under a dest_count-only proof (CR-D125).
            copy_sql = (
                f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
                f"(FORMAT csv, HEADER true, DELIMITER {delim}, NULL '')"
            )
            with open(tmp_path, "r", encoding="utf-8") as handle:
                dst_cur.copy_expert(copy_sql, handle)
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            dest_conn.rollback()
            raise ValueError(
                "S3→PG COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        if swap_at_end:
            from services.dest_overwrite_swap import pg_swap_staged_dest

            pg_swap_staged_dest(
                dst_cur,
                schema=swap_schema,
                dest_table=swap_dest_table,
                staging_table=dest_table,
                trash_table=swap_trash_table,
                quote=_pg_ident,
            )
            dest_ref = swap_dest_ref
            dest_table = swap_dest_table
        dest_conn.commit()
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "object",
                "s3_read": "get_csv",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("PostgreSQL dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("PG dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        for path in tmp_paths:
            try:
                os.unlink(path)
            except OSError:
                logger.debug("S3→PG tempfile unlink skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("PostgreSQL dest close skipped", exc_info=True)
