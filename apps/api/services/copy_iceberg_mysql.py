"""Iceberg snapshot Parquet → MySQL STRICT LOAD DATA (cross-engine bulk).

The reverse of ``copy_mysql_iceberg``. Source COUNT is Iceberg file
footers via ``destination_row_count`` / ``iceberg_mor`` — never
``scan().count()``. Payload is current-snapshot data files read as
Arrow (no ``scan().to_arrow()``). Each cell is encoded as LOAD DATA
TSV into a tempfile, then STRICT ``LOAD DATA LOCAL INFILE``. Dest
``COUNT(*)`` must equal that footer COUNT.

Empty dest loads the snapshot once. Occupied dest whose ``COUNT(*)``
already equals the source footer COUNT is skip-complete (COUNT only).
Occupied dest with a different COUNT declines — leftover MERGE / upsert
stays on the row path. Iceberg MoR (delete files) declines. Filesystem
CoW declines. This is **not** BCP / ``sqlldr``.

Reuses ``_arrow_from_iceberg_files`` from ``copy_iceberg_pg`` and the
canonical LOAD DATA encoder / session checks.

Declines (row path keeps quarantine): transforms that change values,
binary/uuid/timestamptz/list/map/struct, MoR snapshots, public proxy,
occupied dest with dest COUNT ≠ source, LOAD DATA ineligible sessions.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_iceberg_pg import (
    _ARROW_BATCH,
    _arrow_from_iceberg_files,
    _iceberg_source_count,
    iceberg_type_is_copy_safe,
)
from services.copy_mysql_mysql import fast_load_data_text_value
from services.copy_mysql_pg import _mysql_connect, _mysql_ident
from services.copy_pg_iceberg import iceberg_copy_endpoint
from services.copy_pg_mysql import _mysql_create_sql, mapping_is_plain_carry

logger = logging.getLogger(__name__)


def iceberg_mysql_copy_enabled() -> bool:
    raw = (getenv_brand("ICEBERG_MYSQL_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _mysql_table_exists(cur: Any, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    return int(cur.fetchone()[0]) > 0


def _arrow_to_load_data_tsv(table: Any, path: str) -> None:
    encode = fast_load_data_text_value
    join = "\t".join
    with open(path, "wb", buffering=1 << 20) as writer:
        for batch in table.to_batches(max_chunksize=_ARROW_BATCH):
            cols = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
            if not cols:
                continue
            payload = "\n".join(join(encode(v) for v in row) for row in zip(*cols))
            if payload:
                writer.write((payload + "\n").encode("utf-8"))


def _load_tsv_into_mysql(
    dest_conn: Any,
    dst_cur: Any,
    *,
    path: str,
    table_q: str,
    columns: list[str],
) -> None:
    from connectors.mysql_load_data import (
        blocking_load_data_warnings,
        build_load_data_sql,
        mysql_load_data_session_ready,
        quote_load_data_path,
    )

    ready, why = mysql_load_data_session_ready(dst_cur, dest_conn)
    if not ready:
        raise FastPathUnavailable(why)
    load_sql = build_load_data_sql(
        table_q=table_q,
        columns=columns,
        infile_sql=quote_load_data_path(path),
    )
    dst_cur.execute(load_sql)
    dst_cur.execute("SHOW WARNINGS")
    blocked = blocking_load_data_warnings(list(dst_cur.fetchall() or []))
    if blocked:
        raise FastPathUnavailable(f"LOAD DATA warnings: {blocked[0]}")


def copy_iceberg_to_mysql(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """LOAD Iceberg snapshot files into MySQL. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not iceberg_mysql_copy_enabled():
        raise FastPathUnavailable("Iceberg→MySQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        source_cfg.get("connection_string") or source_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pyarrow/pyiceberg required for Iceberg COPY: {exc}") from exc

    endpoint = iceberg_copy_endpoint(source_cfg, source_table, source_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dest_q = _mysql_ident(dest_table)

    dest_conn = _mysql_connect(dest_cfg)
    created_here = False
    tmp_path = ""
    dst_cur = dest_conn.cursor()
    try:
        source_count = _iceberg_source_count(endpoint)

        exists = _mysql_table_exists(dst_cur, dest_table)
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied and dest_count_before == source_count:
                proof = f"dest_count:{dest_count_before}"
                return FastPathResult(
                    rows_copied=source_count,
                    source_rows=source_count,
                    source_checksum=proof,
                    target_rows=dest_count_before,
                    target_checksum=proof,
                    source_snapshot={
                        "copy_workers": 1,
                        "copy_split": "skip",
                        "copy_partitions": 1,
                        "partitions_skipped": 1,
                        "partitions_loaded": 0,
                        "shard_mode": "table",
                        "iceberg_read": "skip",
                        "load_data": "skip",
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied MySQL dest stays on the row path "
                    "(Iceberg source has no PK-range skip on this path)"
                )
        else:
            dst_cur.execute(_mysql_create_sql(dest_table, pairs, mysql_ddls, []))
            dest_conn.commit()
            created_here = True

        pa_table = _arrow_from_iceberg_files(endpoint, source_cols)
        if len(pa_table) != source_count:
            raise ValueError(
                "Iceberg→MySQL COPY refused: Arrow rows "
                f"{len(pa_table)} != source footer COUNT {source_count}"
            )
        pa_table = pa_table.rename_columns(target_cols)

        handle, tmp_path = tempfile.mkstemp(prefix="df_iceberg_mysql_", suffix=".tsv")
        os.close(handle)
        _arrow_to_load_data_tsv(pa_table, tmp_path)
        _load_tsv_into_mysql(
            dest_conn,
            dst_cur,
            path=tmp_path,
            table_q=dest_q,
            columns=target_cols,
        )
        dest_conn.commit()

        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            raise ValueError(
                "Iceberg→MySQL COPY refused: dest COUNT(*) "
                f"{dest_count} != source footer COUNT {source_count}"
            )
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
                "shard_mode": "table",
                "iceberg_read": "snapshot_parquet",
                "load_data": "tempfile",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("MySQL dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("Iceberg MySQL COPY tempfile unlink skipped", exc_info=True)
        try:
            dst_cur.close()
        except Exception:
            logger.debug("MySQL dest cursor close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("MySQL dest close skipped", exc_info=True)
