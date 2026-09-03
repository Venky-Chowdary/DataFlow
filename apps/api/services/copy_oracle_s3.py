"""Oracle SHARE-lock SELECT CSV → S3 PUT (cross-engine bulk).

Oracle has no ``COPY TO STDOUT`` and this host has no client ``sqlldr``
/ Data Pump. One ``LOCK TABLE src IN SHARE MODE`` transaction streams
``SELECT`` into a CSV tempfile (HEADER, ``\\N`` = NULL, ``""`` = empty
string), then ``upload_file``. Dest COUNT is object-store artifact COUNT
of that CSV (header skipped) — never writer PUT ack, never ListObjects
length. Empty dest is PUT, **not** sqlldr / Data Pump / ``aws s3 cp``.
Occupied dest whose COUNT already equals the source snapshot is
skip-complete. Occupied dest with a different COUNT declines. Dest key
must be ``.csv`` / ``.tsv``. BLOB/RAW/XMLTYPE/CLOB/JSON decline.
Occupancy is counted **before** delete.

Oracle ``VARCHAR2`` stores ``''`` as ``NULL`` (engine law). Source cells
that were originally empty strings therefore arrive here as ``None`` and
CSV as ``\\N``. That is not a row drop.

Declines (row path keeps quarantine): transforms that change values,
BLOB/RAW/XMLTYPE/CLOB/JSON, public proxy, occupied dest with dest COUNT
≠ source, non-CSV dest key.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _ora_table_pk_and_types,
    _oracle_connect,
    _schema_of as _ora_schema_of,
    _table_ref as _ora_table_ref,
    oracle_cfg_is_public_proxy,
)
from services.copy_oracle_pg import (
    _select_sql,
    _tune_fetch,
    oracle_type_is_copy_safe,
)
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_delete_keys,
    s3_dest_count,
    s3_ensure_bucket,
    s3_ext,
    s3_iter_fetchmany,
    s3_list_keys,
    s3_write_delimited,
    skip_complete_s3,
)

logger = logging.getLogger(__name__)

_FETCH_BATCH = 8192


def oracle_s3_copy_enabled() -> bool:
    raw = (getenv_brand("ORACLE_S3_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _s3_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "endpoint_url", "dsn")
    )


def oracle_value_to_s3(value: Any) -> Any:
    """Bind an Oracle Python value for the S3 CSV wire.

    DATE / DATETIME-NTZ at midnight land as a calendar day so the CSV
    cell is ISO date, not ``YYYY-MM-DD 00:00:00``.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise FastPathUnavailable(
                "timestamptz Oracle value is not S3 COPY-safe"
            )
        if value.hour or value.minute or value.second or value.microsecond:
            return value.replace(tzinfo=None)
        return date(value.year, value.month, value.day)
    if isinstance(value, date):
        return value
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Oracle field is not S3 COPY-safe")
    return value


def _oracle_s3_rows(cursor: Any):
    for row in s3_iter_fetchmany(cursor, _FETCH_BATCH):
        yield tuple(oracle_value_to_s3(v) for v in row)


def copy_oracle_to_s3(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    s3_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """SELECT CSV from Oracle into one S3 object. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(s3_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not oracle_s3_copy_enabled():
        raise FastPathUnavailable("Oracle→S3 COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    if oracle_cfg_is_public_proxy(source_cfg) or _s3_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: S3 bulk copy not assumed")

    ext = s3_ext(dest_table)
    if ext not in {"csv", "tsv"}:
        raise FastPathUnavailable("Oracle→S3 COPY writes CSV/TSV")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = _ora_schema_of(source_cfg, source_schema)
    source_ref = _ora_table_ref(src_schema, source_table)
    delim = "\t" if ext == "tsv" else ","

    source_conn = _oracle_connect(source_cfg)
    created_here = False
    tmp_path = ""
    src_cur = source_conn.cursor()
    try:
        _pk_cols, live = _ora_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not oracle_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not S3 COPY-safe"
                )
        src_cur.execute(f"LOCK TABLE {source_ref} IN SHARE MODE")  # nosec B608
        source_count = _ora_count(src_cur, source_ref)
        select_sql = _select_sql(source_ref, source_cols, "")
        src_cur.close()
        src_cur = None  # type: ignore[assignment]

        s3_ensure_bucket(dest_cfg)
        dest_count_before = s3_dest_count(dest_cfg, dest_table)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                try:
                    source_conn.rollback()
                except Exception:
                    logger.debug("Oracle source rollback on skip skipped", exc_info=True)
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "oracle_lock": "share",
                        "oracle_read": "skip",
                        "s3_write": "skip",
                    },
                )
            raise FastPathUnavailable(
                "append into occupied S3 dest stays on the row path "
                "(identity COPY would duplicate)"
            )

        if replace_destination:
            s3_delete_keys(dest_cfg, s3_list_keys(dest_cfg, dest_table))
        created_here = dest_count_before == 0 or replace_destination

        fd, tmp_path = tempfile.mkstemp(prefix="df-oracle-s3-", suffix=f".{ext}")
        os.close(fd)
        cur = source_conn.cursor()
        try:
            _tune_fetch(cur)
            cur.execute(select_sql)
            csv_rows = s3_write_delimited(
                tmp_path, target_cols, _oracle_s3_rows(cur), delim
            )
        finally:
            try:
                cur.close()
            except Exception:
                logger.debug("Oracle stream cursor close skipped", exc_info=True)
        if csv_rows != source_count:
            raise ValueError(
                "Oracle→S3 COPY refused: CSV rows "
                f"{csv_rows} != source snapshot {source_count}"
            )
        client = s3_client(dest_cfg)
        client.upload_file(tmp_path, s3_bucket(dest_cfg), dest_table)
        dest_count = s3_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "Oracle→S3 COPY refused: dest COUNT "
                f"{dest_count} != source snapshot {source_count}"
            )
        try:
            source_conn.commit()
        except Exception:
            logger.debug("Oracle source commit skipped", exc_info=True)
        s3_write = "overwrite" if replace_destination and dest_occupied else "insert"
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "oracle_lock": "share",
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "object",
                "oracle_read": "share_select",
                "s3_write": s3_write,
                "s3_key": dest_table,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            source_conn.rollback()
        except Exception:
            logger.debug("Oracle source rollback skipped", exc_info=True)
        if created_here:
            try:
                s3_delete_keys(dest_cfg, [dest_table] + s3_list_keys(dest_cfg, dest_table))
            except Exception:
                logger.debug("S3 dest delete after copy failure skipped", exc_info=True)
        raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("Oracle→S3 tempfile unlink skipped", exc_info=True)
        if src_cur is not None:
            try:
                src_cur.close()
            except Exception:
                logger.debug("Oracle source cursor close skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("Oracle source close skipped", exc_info=True)
