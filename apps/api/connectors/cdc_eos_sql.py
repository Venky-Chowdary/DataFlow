"""Dest-owned CDC exactly-once apply.

SQLite uses a native ``BEGIN IMMEDIATE`` path. Other transactional SQL
engines use :mod:`connectors.cdc_eos_sa` (one dest transaction).
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
    DestWmView,
    EosApplyResult,
    EosBundleResult,
    EosBundleStream,
    EosOpenResult,
    ExactlyOnceRouteError,
    assert_bundle_members_reached,
    batch_apply_checksum,
    combine_change_batch,
    decide_from_view,
    encode_resume_blob,
    extract_snapshot_window_id,
    incoming_pk_keys,
    load_reduce_into_dest,
    planned_apply_seq,
    verify_dest_commit,
    eos_stream_key,
    extract_cdc_phase,
    is_incremental_snapshot_token,
    next_dest_window_id,
    next_handoff_phase,
    plan_open_session,
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
  epoch INTEGER NOT NULL DEFAULT 1,
  fence_epoch INTEGER NOT NULL DEFAULT 0,
  prev_lsn TEXT,
  phase TEXT,
  apply_checksum TEXT,
  resume_blob TEXT,
  apply_seq INTEGER NOT NULL DEFAULT 0,
  window_id TEXT
)
"""

_WM_ALTERS = (
    "ALTER TABLE {table} ADD COLUMN fence_epoch INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE {table} ADD COLUMN prev_lsn TEXT",
    "ALTER TABLE {table} ADD COLUMN phase TEXT",
    "ALTER TABLE {table} ADD COLUMN apply_checksum TEXT",
    "ALTER TABLE {table} ADD COLUMN resume_blob TEXT",
    "ALTER TABLE {table} ADD COLUMN apply_seq INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE {table} ADD COLUMN window_id TEXT",
)


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


def _ensure_wm_table(cur: sqlite3.Cursor) -> None:
    cur.execute(_WM_DDL)
    for stmt in _WM_ALTERS:
        try:
            cur.execute(stmt.format(table=WATERMARK_TABLE))
        except Exception:
            pass


def _read_watermark(cur: sqlite3.Cursor, stream_key: str) -> DestWmView:
    try:
        cur.execute(
            f"SELECT committed_lsn, epoch, fence_epoch, phase, apply_checksum, "
            f"resume_blob, apply_seq, window_id "
            f"FROM {WATERMARK_TABLE} WHERE stream_key = ?",
            (stream_key,),
        )
    except Exception:
        cur.execute(
            f"SELECT committed_lsn, epoch FROM {WATERMARK_TABLE} WHERE stream_key = ?",
            (stream_key,),
        )
    row = cur.fetchone()
    if not row:
        return DestWmView()
    return DestWmView(
        committed_lsn=str(row[0] or "") or None,
        epoch=int(row[1] or 0),
        fence_epoch=int(row[2] or 0) if len(row) > 2 else 0,
        phase=str(row[3] or "") if len(row) > 3 else "",
        apply_checksum=str(row[4] or "") if len(row) > 4 else "",
        resume_blob=str(row[5] or "") if len(row) > 5 else "",
        apply_seq=int(row[6] or 0) if len(row) > 6 else 0,
        window_id=str(row[7] or "") if len(row) > 7 else "",
    )


def _write_watermark(
    cur: sqlite3.Cursor,
    *,
    stream_key: str,
    lsn: str,
    batch_id: str,
    dest_object: str,
    epoch: int,
    fence_epoch: int = 0,
    prev_lsn: str | None = None,
    phase: str = "streaming",
    apply_checksum: str = "",
    resume_blob: str = "",
    apply_seq: int = 0,
    window_id: str = "",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    cur.execute(
        f"""
        INSERT INTO {WATERMARK_TABLE}
          (stream_key, committed_lsn, batch_id, committed_at, dest_object, epoch,
           fence_epoch, prev_lsn, phase, apply_checksum, resume_blob, apply_seq,
           window_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stream_key) DO UPDATE SET
          committed_lsn = excluded.committed_lsn,
          batch_id = excluded.batch_id,
          committed_at = excluded.committed_at,
          dest_object = excluded.dest_object,
          epoch = excluded.epoch,
          fence_epoch = excluded.fence_epoch,
          prev_lsn = excluded.prev_lsn,
          phase = excluded.phase,
          apply_checksum = excluded.apply_checksum,
          resume_blob = excluded.resume_blob,
          apply_seq = excluded.apply_seq,
          window_id = excluded.window_id
        """,
        (
            stream_key,
            lsn,
            batch_id,
            now,
            dest_object,
            epoch,
            fence_epoch,
            prev_lsn,
            phase,
            apply_checksum,
            resume_blob,
            apply_seq,
            window_id,
        ),
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


def _prepare_sqlite_targets(
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    pk_target_cols: list[str],
) -> tuple[list[dict[str, Any]], dict[str, str], list[str], dict[str, str]]:
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
    src_to_tgt = {
        str(m.get("source") or ""): str(m.get("target") or m.get("source") or "")
        for m in mappings
        if m.get("source")
    }
    tgt_to_src = {t: s for s, t in src_to_tgt.items() if t}
    return mappings, column_types, target_cols, tgt_to_src


def _sqlite_load_dest_rows(
    cur: sqlite3.Cursor,
    table_name: str,
    pk_cols: list[str],
    keys: list[str],
    columns: list[str],
) -> dict[str, dict[str, Any]]:
    """Estuary Load — dest documents for batch PKs on the apply cursor."""
    from services.cdc_snapshot_window import _pk_row_dict, _pk_value

    out: dict[str, dict[str, Any]] = {}
    if not keys or not pk_cols or not columns:
        return out
    table_q = quote_sql_identifier(table_name)
    col_sql = ", ".join(quote_sql_identifier(c) for c in columns)
    for key in keys:
        parts = _pk_row_dict(pk_cols, key) if len(pk_cols) > 1 else {pk_cols[0]: key}
        where = " AND ".join(f"{quote_sql_identifier(c)} = ?" for c in pk_cols)
        binds = [parts[c] for c in pk_cols]
        cur.execute(
            f"SELECT {col_sql} FROM {table_q} WHERE {where}",  # nosec B608
            binds,
        )
        row = cur.fetchone()
        if not row:
            continue
        rec = {columns[i]: row[i] for i in range(len(columns))}
        pk = _pk_value(rec, pk_cols)
        if pk:
            out[str(pk)] = rec
    return out


def _sqlite_apply_member(
    cur: sqlite3.Cursor,
    *,
    dest_table: str,
    change: ChangeBatch,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    pk_target_cols: list[str],
    stream_key: str,
    incoming_lsn: str,
    batch_id: str,
    writer_fence: int = 0,
    crash_after: str | None = None,
) -> EosApplyResult:
    """Apply one stream on an open dest cursor — no BEGIN/COMMIT (bundle-safe)."""
    from connectors.sqlite_writer import _sqlite_upsert_batch

    mappings, column_types, target_cols, tgt_to_src = _prepare_sqlite_targets(
        mappings, column_types, pk_target_cols
    )
    change = combine_change_batch(change, pk_cols=pk_target_cols)
    incoming_phase = extract_cdc_phase(change.resume_token)
    incoming_checksum = batch_apply_checksum(
        change, incoming_lsn=incoming_lsn, pk_cols=pk_target_cols
    )
    _ensure_wm_table(cur)
    _ensure_dest_table(cur, dest_table, target_cols, pk_target_cols)
    dest = _read_watermark(cur, stream_key)
    action, fence = decide_from_view(
        incoming_lsn=incoming_lsn,
        dest=dest,
        incoming_fence=writer_fence,
        incoming_phase=incoming_phase,
        incoming_checksum=incoming_checksum,
        incremental_snapshot=is_incremental_snapshot_token(change.resume_token),
        change=change,
    )
    phase = next_handoff_phase(incoming_phase, dest.phase or None)
    resume_blob = encode_resume_blob(change.resume_token)
    window_id = next_dest_window_id(
        extract_snapshot_window_id(change.resume_token), dest.window_id
    )
    if action in {"already_committed", "stream_wins_skip", "window_closed_skip"}:
        return EosApplyResult(
            status=action,
            committed_lsn=dest.committed_lsn,
            batch_id=batch_id,
            epoch=dest.epoch,
            already_committed=True,
            fence_epoch=fence,
            phase=dest.phase or "streaming",
            apply_checksum=dest.apply_checksum,
            apply_seq=dest.apply_seq,
            window_id=dest.window_id,
        )
    dest_seq = planned_apply_seq(dest.apply_seq)
    if action == "handoff_phase":
        new_epoch = dest.epoch + 1
        _write_watermark(
            cur,
            stream_key=stream_key,
            lsn=dest.committed_lsn or incoming_lsn,
            batch_id=batch_id,
            dest_object=dest_table,
            epoch=new_epoch,
            fence_epoch=fence,
            prev_lsn=dest.committed_lsn,
            phase="streaming",
            apply_checksum=incoming_checksum or dest.apply_checksum,
            resume_blob=resume_blob or dest.resume_blob,
            apply_seq=dest_seq,
            window_id=window_id,
        )
        return EosApplyResult(
            status="handoff_phase",
            committed_lsn=dest.committed_lsn or incoming_lsn,
            batch_id=batch_id,
            epoch=new_epoch,
            already_committed=True,
            fence_epoch=fence,
            phase="streaming",
            apply_checksum=incoming_checksum or dest.apply_checksum,
            apply_seq=dest_seq,
            window_id=window_id,
        )

    rows_written = 0
    records = list(change.inserts or []) + list(change.updates or [])
    if records:
        dest_docs = _sqlite_load_dest_rows(
            cur,
            dest_table,
            pk_target_cols,
            incoming_pk_keys(records, pk_target_cols),
            target_cols,
        )
        records = load_reduce_into_dest(
            incoming_rows=records,
            dest_rows=dest_docs,
            pk_cols=pk_target_cols,
            incoming_lsn=incoming_lsn,
        )
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
        from services.cdc_exactly_once import EosCrash

        raise EosCrash(crash_after)

    new_epoch = dest.epoch + 1
    _write_watermark(
        cur,
        stream_key=stream_key,
        lsn=incoming_lsn,
        batch_id=batch_id,
        dest_object=dest_table,
        epoch=new_epoch,
        fence_epoch=fence,
        prev_lsn=dest.committed_lsn,
        phase=phase,
        apply_checksum=incoming_checksum,
        resume_blob=resume_blob,
        apply_seq=dest_seq,
        window_id=window_id,
    )
    if crash_after == "after_watermark_before_commit":
        from services.cdc_exactly_once import EosCrash

        raise EosCrash(crash_after)
    return EosApplyResult(
        status="applied" if (rows_written or deleted or records) else "empty",
        rows_written=rows_written,
        deleted=deleted,
        committed_lsn=incoming_lsn,
        batch_id=batch_id,
        epoch=new_epoch,
        fence_epoch=fence,
        phase=phase,
        apply_checksum=incoming_checksum,
        apply_seq=dest_seq,
        window_id=window_id,
    )


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
    writer_fence: int = 0,
) -> tuple[int, str, dict[str, Any], int]:
    """Apply one CDC batch under dest-owned watermark EOS.

    Returns ``(rows_written, checksum, dest_summary, deleted)`` to match
    ``_apply_change_batch``.
    """
    dest = (dest_type or "").strip().lower().replace("-", "_")
    incoming = require_batch_lsn(change.resume_token)
    change = combine_change_batch(change, pk_cols=pk_target_cols)
    stream_key = eos_stream_key(
        dest_type=dest,
        dest_database=str(dest_cfg.get("database") or ""),
        dest_object=dest_table,
        cursor_key=cursor_key,
        stream_name=stream_name,
    )
    batch_id = f"eos-{uuid.uuid4().hex[:12]}"
    if dest != "sqlite":
        from services.cdc_exactly_once import (
            EOS_TRANSACTIONAL_DESTS,
            EOS_TXN_WIRED_DESTS,
            REASON_DEST_NOT_TXN,
            REASON_DEST_NOT_WIRED,
        )

        if dest not in EOS_TXN_WIRED_DESTS:
            reason = (
                REASON_DEST_NOT_TXN
                if dest not in EOS_TRANSACTIONAL_DESTS
                else REASON_DEST_NOT_WIRED
            )
            raise ExactlyOnceRouteError(
                f"exactly_once dest-owned watermark apply is not wired for "
                f"{dest_type!r}. Refuse inventing EOS on an unwired writer.",
                reason=reason,
            )
        from connectors.cdc_eos_sa import apply_eos_sqlalchemy

        result = apply_eos_sqlalchemy(
            dest_type=dest,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            change=change,
            mappings=mappings,
            column_types=column_types,
            pk_target_cols=pk_target_cols,
            stream_key=stream_key,
            incoming_lsn=incoming,
            batch_id=batch_id,
            crash_after=crash_after,
            writer_fence=writer_fence,
        )
        return (
            result.rows_written,
            "",
            result.to_dest_summary(),
            result.deleted,
        )
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
        writer_fence=writer_fence,
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
    writer_fence: int = 0,
) -> EosApplyResult:
    _ = headers  # caller headers; dest columns come from mappings
    path = _sqlite_path(dest_cfg)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            result = _sqlite_apply_member(
                cur,
                dest_table=dest_table,
                change=change,
                mappings=mappings,
                column_types=column_types,
                pk_target_cols=pk_target_cols,
                stream_key=stream_key,
                incoming_lsn=incoming_lsn,
                batch_id=batch_id,
                writer_fence=writer_fence,
                crash_after=crash_after,
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        if result.status in {"applied", "empty"} and result.committed_lsn:
            verify_dest_commit(
                dest=dest_watermark_view(dest_cfg, stream_key),
                expected_lsn=result.committed_lsn,
                expected_fence=result.fence_epoch,
                expected_seq=result.apply_seq,
            )
        return result
    finally:
        conn.close()


def apply_eos_bundle(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    streams: list[EosBundleStream],
    incoming_lsn: str = "",
    bundle_key: str = "",
    writer_fence: int = 0,
    crash_after: str | None = None,
) -> EosBundleResult:
    """N streams + one shared LSN in one dest transaction.

    Crash before COMMIT rolls back every member. Source ack happens after.
    """
    dest = (dest_type or "").strip().lower().replace("-", "_")
    if dest != "sqlite":
        from connectors.cdc_eos_sa import apply_eos_sa_bundle

        return apply_eos_sa_bundle(
            dest_type=dest,
            dest_cfg=dest_cfg,
            streams=streams,
            incoming_lsn=incoming_lsn,
            bundle_key=bundle_key,
            writer_fence=writer_fence,
            crash_after=crash_after,
        )
    if not incoming_lsn:
        if streams:
            incoming_lsn = require_batch_lsn(streams[0].change.resume_token)
        else:
            incoming_lsn = ""
    if not streams and not (bundle_key and incoming_lsn):
        return EosBundleResult(committed_lsn=incoming_lsn or None)
    path = _sqlite_path(dest_cfg)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    members: list[EosApplyResult] = []
    member_keys: list[str] = []
    try:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            _ensure_wm_table(cur)
            ordered = sorted(streams, key=lambda s: s.stream_key)
            for stream in ordered:
                member_lsn = incoming_lsn
                try:
                    member_lsn = require_batch_lsn(stream.change.resume_token)
                except ExactlyOnceRouteError:
                    if not incoming_lsn:
                        raise
                member_keys.append(stream.stream_key)
                members.append(
                    _sqlite_apply_member(
                        cur,
                        dest_table=stream.dest_table,
                        change=stream.change,
                        mappings=stream.mappings,
                        column_types=stream.column_types,
                        pk_target_cols=stream.pk_target_cols,
                        stream_key=stream.stream_key,
                        incoming_lsn=member_lsn,
                        batch_id=f"eos-b-{uuid.uuid4().hex[:12]}",
                        writer_fence=writer_fence,
                    )
                )
            if incoming_lsn and members:
                assert_bundle_members_reached(
                    [m.committed_lsn for m in members], incoming_lsn
                )
            if bundle_key and incoming_lsn:
                dest_wm = _read_watermark(cur, bundle_key)
                action, fence = decide_from_view(
                    incoming_lsn=incoming_lsn,
                    dest=dest_wm,
                    incoming_fence=writer_fence,
                )
                if action != "already_committed":
                    _write_watermark(
                        cur,
                        stream_key=bundle_key,
                        lsn=incoming_lsn,
                        batch_id=f"eos-bundle-{uuid.uuid4().hex[:10]}",
                        dest_object="*",
                        epoch=dest_wm.epoch + 1,
                        fence_epoch=fence,
                        prev_lsn=dest_wm.committed_lsn,
                        phase="streaming",
                    )
            if crash_after in {
                "after_apply_before_watermark",
                "after_watermark_before_commit",
            }:
                from services.cdc_exactly_once import EosCrash

                raise EosCrash(crash_after)
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
    finally:
        conn.close()
    if incoming_lsn:
        for key, member in zip(member_keys, members):
            if member.status in {"applied", "empty"} and member.committed_lsn:
                verify_dest_commit(
                    dest=dest_watermark_view(dest_cfg, key),
                    expected_lsn=member.committed_lsn,
                    expected_fence=member.fence_epoch,
                    expected_seq=member.apply_seq,
                )
        if bundle_key:
            verify_dest_commit(
                dest=dest_watermark_view(dest_cfg, bundle_key),
                expected_lsn=incoming_lsn,
                expected_fence=max(
                    (m.fence_epoch for m in members), default=writer_fence
                ),
            )
    rows = sum(m.rows_written for m in members)
    deleted = sum(m.deleted for m in members)
    fence = max((m.fence_epoch for m in members), default=writer_fence)
    return EosBundleResult(
        members=members,
        committed_lsn=incoming_lsn or None,
        already_committed=bool(members) and all(m.already_committed for m in members),
        rows_written=rows,
        deleted=deleted,
        fence_epoch=fence,
        bundle_key=bundle_key,
    )


def open_eos_session(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    stream_key: str,
    incoming_fence: int = 0,
    job_resume: Any = None,
) -> EosOpenResult:
    """Estuary Open: raise dest fence with no data; return dest resume blob."""
    dest = (dest_type or "").strip().lower().replace("-", "_")
    if dest != "sqlite":
        from connectors.cdc_eos_sa import open_eos_sa_session

        return open_eos_sa_session(
            dest_type=dest,
            dest_cfg=dest_cfg,
            stream_key=stream_key,
            incoming_fence=incoming_fence,
            job_resume=job_resume,
        )
    path = _sqlite_path(dest_cfg)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            _ensure_wm_table(cur)
            view = _read_watermark(cur, stream_key)
            opened = plan_open_session(
                dest=view, incoming_fence=incoming_fence, job_resume=job_resume
            )
            if opened.fence_raised and view.committed_lsn:
                _write_watermark(
                    cur,
                    stream_key=stream_key,
                    lsn=view.committed_lsn,
                    batch_id="eos-open",
                    dest_object="",
                    epoch=view.epoch,
                    fence_epoch=opened.fence_epoch,
                    prev_lsn=view.committed_lsn,
                    phase=view.phase or "streaming",
                    apply_checksum=view.apply_checksum,
                    resume_blob=view.resume_blob,
                    apply_seq=view.apply_seq,
                    window_id=view.window_id,
                )
            conn.execute("COMMIT")
            return opened
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
        return _read_watermark(cur, stream_key).committed_lsn
    finally:
        conn.close()


def dest_watermark_view(dest_cfg: dict[str, Any], stream_key: str) -> DestWmView:
    """Read dest-owned watermark fields (phase / checksum proofs)."""
    path = _sqlite_path(dest_cfg)
    conn = sqlite3.connect(path, timeout=8)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (WATERMARK_TABLE,),
        )
        if cur.fetchone() is None:
            return DestWmView()
        return _read_watermark(cur, stream_key)
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


def read_route_dest_lsn(
    dest_type: str,
    dest_cfg: dict[str, Any],
    stream_key: str,
) -> str | None:
    """Dest-authoritative watermark read (resume Open)."""
    dest = (dest_type or "").strip().lower().replace("-", "_")
    if dest == "sqlite":
        try:
            return dest_watermark_lsn(dest_cfg, stream_key)
        except Exception:
            return None
    from services.cdc_exactly_once import EOS_TXN_WIRED_DESTS

    if dest not in EOS_TXN_WIRED_DESTS:
        return None
    try:
        from connectors.cdc_eos_sa import sa_dest_watermark_lsn

        return sa_dest_watermark_lsn(dest_cfg, stream_key, dest)
    except Exception:
        return None


def read_route_dest_resume(
    dest_type: str,
    dest_cfg: dict[str, Any],
    stream_key: str,
) -> Any:
    """Dest-stored resume blob (Estuary Opened checkpoint)."""
    dest = (dest_type or "").strip().lower().replace("-", "_")
    if dest == "sqlite":
        try:
            return dest_watermark_view(dest_cfg, stream_key).resume_blob
        except Exception:
            return ""
    from services.cdc_exactly_once import EOS_TXN_WIRED_DESTS

    if dest not in EOS_TXN_WIRED_DESTS:
        return ""
    try:
        from connectors.cdc_eos_sa import sa_dest_resume_blob

        return sa_dest_resume_blob(dest_cfg, stream_key, dest)
    except Exception:
        return ""


# Imported by proofs — keep algorithm name on the connector surface.
EOS_ALGORITHM = ALGORITHM
