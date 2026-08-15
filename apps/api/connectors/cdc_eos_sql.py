"""Dest-owned CDC exactly-once apply — SQLite transactional watermark.

Apply + ``_df_cdc_eos_watermarks`` share one ``BEGIN IMMEDIATE`` transaction.
Other SQL engines are classified transactional but stay fail-closed until a
shared-connection writer is wired (see ``EOS_TXN_WIRED_DESTS``).
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

from connectors.lsn_guards import DF_LSN_COL
from connectors.sqlite_common import sqlite_file_path
from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier
from services.cdc_exactly_once import (
    ALGORITHM,
    WATERMARK_TABLE,
    EosApplyResult,
    ExactlyOnceRouteError,
    already_committed,
    eos_stream_key,
    require_batch_lsn,
)
from services.cdc_engine import ChangeBatch

_WM_DDL = f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
  stream_key TEXT PRIMARY KEY,
  committed_lsn TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  committed_at TEXT NOT NULL,
  dest_object TEXT,
  epoch INTEGER NOT NULL DEFAULT 1
)
"""


def _sqlite_path(dest_cfg: dict[str, Any]) -> str:
    path = sqlite_file_path(
        str(dest_cfg.get("database") or ""),
        str(dest_cfg.get("connection_string") or ""),
        str(dest_cfg.get("host") or ""),
    )
    if not path:
        raise ExactlyOnceRouteError(
            "exactly_once SQLite destination path is required "
            "(database or connection_string).",
            reason="exactly_once_sqlite_path_missing",
        )
    return path


def _ensure_dest_table(
    cur: sqlite3.Cursor,
    table_name: str,
    columns: list[str],
    pk_cols: list[str],
) -> None:
    require_safe_identifier(table_name)
    table_q = quote_sql_identifier(table_name)
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    exists = cur.fetchone() is not None
    if not exists:
        col_sql = []
        for col in columns:
            require_safe_identifier(col)
            suffix = " PRIMARY KEY" if pk_cols == [col] else ""
            col_sql.append(f"{quote_sql_identifier(col)} TEXT{suffix}")
        if len(pk_cols) > 1:
            pk_sql = ", ".join(quote_sql_identifier(c) for c in pk_cols)
            col_sql.append(f"PRIMARY KEY ({pk_sql})")
        cur.execute(f"CREATE TABLE {table_q} ({', '.join(col_sql)})")  # nosec B608
        return
    cur.execute(f"PRAGMA table_info({table_q})")  # nosec B608
    present = {str(row[1]) for row in cur.fetchall() if row[1]}
    for col in columns:
        if col in present:
            continue
        require_safe_identifier(col)
        cur.execute(
            f"ALTER TABLE {table_q} ADD COLUMN {quote_sql_identifier(col)} TEXT"  # nosec B608
        )


def _read_watermark(cur: sqlite3.Cursor, stream_key: str) -> tuple[str | None, int]:
    cur.execute(
        f"SELECT committed_lsn, epoch FROM {WATERMARK_TABLE} WHERE stream_key = ?",
        (stream_key,),
    )
    row = cur.fetchone()
    if not row:
        return None, 0
    return str(row[0] or "") or None, int(row[1] or 0)


def _write_watermark(
    cur: sqlite3.Cursor,
    *,
    stream_key: str,
    lsn: str,
    batch_id: str,
    dest_object: str,
    epoch: int,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    cur.execute(
        f"""
        INSERT INTO {WATERMARK_TABLE}
          (stream_key, committed_lsn, batch_id, committed_at, dest_object, epoch)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(stream_key) DO UPDATE SET
          committed_lsn = excluded.committed_lsn,
          batch_id = excluded.batch_id,
          committed_at = excluded.committed_at,
          dest_object = excluded.dest_object,
          epoch = excluded.epoch
        """,
        (stream_key, lsn, batch_id, now, dest_object, epoch),
    )


def _delete_on_cursor(
    cur: sqlite3.Cursor,
    table_name: str,
    pk_cols: list[str],
    keys: list[str],
    incoming_lsn: str,
) -> int:
    from services.cdc_effectively_once import should_apply_pk_delete
    from services.cdc_snapshot_window import _pk_row_dict

    if not keys or not pk_cols:
        return 0
    table_q = quote_sql_identifier(table_name)
    deleted = 0
    for key in keys:
        parts = _pk_row_dict(pk_cols, key) if len(pk_cols) > 1 else {pk_cols[0]: key}
        where = " AND ".join(f"{quote_sql_identifier(c)} = ?" for c in pk_cols)
        binds = [parts[c] for c in pk_cols]
        select_cols = ", ".join(quote_sql_identifier(c) for c in pk_cols)
        lsn_q = quote_sql_identifier(DF_LSN_COL)
        cur.execute(
            f"SELECT {select_cols}, {lsn_q} FROM {table_q} WHERE {where}",  # nosec B608
            binds,
        )
        row = cur.fetchone()
        prior = row[-1] if row else None
        if not should_apply_pk_delete(
            existing_lsn=prior, incoming_lsn=incoming_lsn
        ).applied:
            continue
        cur.execute(f"DELETE FROM {table_q} WHERE {where}", binds)  # nosec B608
        deleted += int(cur.rowcount or 0)
    return deleted


def apply_change_batch_exactly_once(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    change: ChangeBatch,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    headers: list[str],
    pk_target_cols: list[str],
    cursor_key: str = "",
    stream_name: str = "",
    crash_after: str | None = None,
) -> tuple[int, str, dict[str, Any], int]:
    """Apply one CDC batch under dest-owned watermark EOS.

    Returns ``(rows_written, checksum, dest_summary, deleted)`` to match
    ``_apply_change_batch``.
    """
    dest = (dest_type or "").strip().lower()
    if dest != "sqlite":
        raise ExactlyOnceRouteError(
            f"exactly_once dest-owned watermark apply is wired for sqlite only "
            f"(got {dest_type!r}). Refuse inventing EOS on an unwired writer.",
            reason="exactly_once_dest_txn_not_wired",
        )
    incoming = require_batch_lsn(change.resume_token)
    stream_key = eos_stream_key(
        dest_type=dest,
        dest_database=str(dest_cfg.get("database") or ""),
        dest_object=dest_table,
        cursor_key=cursor_key,
        stream_name=stream_name,
    )
    batch_id = f"eos-{uuid.uuid4().hex[:12]}"
    result = _apply_eos_sqlite(
        dest_cfg=dest_cfg,
        dest_table=dest_table,
        change=change,
        mappings=mappings,
        column_types=column_types,
        headers=headers,
        pk_target_cols=pk_target_cols,
        stream_key=stream_key,
        incoming_lsn=incoming,
        batch_id=batch_id,
        crash_after=crash_after,
    )
    return (
        result.rows_written,
        "",
        result.to_dest_summary(),
        result.deleted,
    )


def _apply_eos_sqlite(
    *,
    dest_cfg: dict[str, Any],
    dest_table: str,
    change: ChangeBatch,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    headers: list[str],
    pk_target_cols: list[str],
    stream_key: str,
    incoming_lsn: str,
    batch_id: str,
    crash_after: str | None = None,
) -> EosApplyResult:
    from connectors.sqlite_writer import _sqlite_upsert_batch
    from connectors.writer_common import resolve_target_columns

    mappings = list(mappings)
    column_types = dict(column_types)
    if not any(m.get("source") == DF_LSN_COL for m in mappings):
        mappings.append(
            {"source": DF_LSN_COL, "target": DF_LSN_COL, "confidence": 1.0}
        )
    column_types.setdefault(DF_LSN_COL, "string")
    target_cols, _logical = resolve_target_columns(
        mappings,
        column_types,
        preserve_case=True,
        table_exists=None,
        dest_db="sqlite",
    )
    if DF_LSN_COL not in target_cols:
        target_cols = list(target_cols) + [DF_LSN_COL]
    if not pk_target_cols:
        raise ExactlyOnceRouteError(
            "exactly_once SQLite apply requires destination primary-key columns.",
            reason="exactly_once_requires_primary_key",
        )
    _ = headers  # caller headers; dest columns come from mappings

    path = _sqlite_path(dest_cfg)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            cur.execute(_WM_DDL)
            _ensure_dest_table(cur, dest_table, target_cols, pk_target_cols)
            dest_lsn, epoch = _read_watermark(cur, stream_key)
            if already_committed(incoming_lsn, dest_lsn):
                conn.execute("COMMIT")
                return EosApplyResult(
                    status="already_committed",
                    committed_lsn=dest_lsn,
                    batch_id=batch_id,
                    epoch=epoch,
                    already_committed=True,
                )

            rows_written = 0
            records = list(change.inserts or []) + list(change.updates or [])
            if records:
                src_to_tgt = {
                    str(m.get("source") or ""): str(
                        m.get("target") or m.get("source") or ""
                    )
                    for m in mappings
                    if m.get("source")
                }
                tgt_to_src = {t: s for s, t in src_to_tgt.items() if t}
                tuples: list[tuple[Any, ...]] = []
                for rec in records:
                    stamped = dict(rec)
                    stamped[DF_LSN_COL] = incoming_lsn
                    values: list[Any] = []
                    for tgt in target_cols:
                        src = tgt_to_src.get(tgt, tgt)
                        if tgt in stamped:
                            values.append(stamped.get(tgt))
                        else:
                            values.append(stamped.get(src))
                    tuples.append(tuple(values))
                written, _skipped = _sqlite_upsert_batch(
                    cur,
                    dest_table,
                    target_cols,
                    tuples,
                    pk_target_cols,
                    schema=None,
                )
                rows_written += int(written or 0)

            deleted = 0
            if change.deletes:
                deleted = _delete_on_cursor(
                    cur, dest_table, pk_target_cols, list(change.deletes), incoming_lsn
                )

            if crash_after == "after_apply_before_watermark":
                conn.execute("ROLLBACK")
                from services.cdc_exactly_once import EosCrash

                raise EosCrash(crash_after)

            new_epoch = epoch + 1
            _write_watermark(
                cur,
                stream_key=stream_key,
                lsn=incoming_lsn,
                batch_id=batch_id,
                dest_object=dest_table,
                epoch=new_epoch,
            )
            if crash_after == "after_watermark_before_commit":
                conn.execute("ROLLBACK")
                from services.cdc_exactly_once import EosCrash

                raise EosCrash(crash_after)
            conn.execute("COMMIT")
            return EosApplyResult(
                status="applied" if (rows_written or deleted or records) else "empty",
                rows_written=rows_written,
                deleted=deleted,
                committed_lsn=incoming_lsn,
                batch_id=batch_id,
                epoch=new_epoch,
            )
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
    finally:
        conn.close()


def dest_watermark_lsn(dest_cfg: dict[str, Any], stream_key: str) -> str | None:
    """Read dest-owned watermark (recovery / proofs)."""
    path = _sqlite_path(dest_cfg)
    conn = sqlite3.connect(path, timeout=8)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (WATERMARK_TABLE,),
        )
        if cur.fetchone() is None:
            return None
        lsn, _epoch = _read_watermark(cur, stream_key)
        return lsn
    finally:
        conn.close()


def dest_engine_count(dest_cfg: dict[str, Any], table_name: str) -> int:
    """Dest-engine COUNT — never writer-stamped row counts."""
    path = _sqlite_path(dest_cfg)
    require_safe_identifier(table_name)
    conn = sqlite3.connect(path, timeout=8)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        )
        if cur.fetchone() is None:
            return 0
        cur.execute(
            f"SELECT COUNT(*) FROM {quote_sql_identifier(table_name)}"  # nosec B608
        )
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        conn.close()


# Imported by proofs — keep algorithm name on the connector surface.
EOS_ALGORITHM = ALGORITHM
