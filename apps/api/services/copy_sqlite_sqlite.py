"""SQLite → SQLite ATTACH + INSERT SELECT (identity bulk).

Same-file copy declines. Dest COUNT is ``SELECT COUNT(*)``. Empty dest
is ``INSERT INTO dest SELECT … FROM src`` after ``ATTACH DATABASE``.
Python never formats a row. Occupied dest whose COUNT already equals
the source COUNT is skip-complete. Occupied dest with a different COUNT
declines. ``:memory:`` declines. This is **not** ``.dump`` / ``.import``.

Declines (row path keeps quarantine): transforms that change values,
BLOB, copy onto the same file, occupied dest with dest COUNT ≠ source, and a
source cell the destination carrier cannot hold (SQLite does not enforce its
declared types, so the census is measured on the stored values).
"""

from __future__ import annotations

import logging
from typing import Any

import connectors.sqlite_writer as sqlite_writer
from services.brand_env import getenv_brand
from services.copy_fast_path import (
    CopyLedgerKey,
    FastPathResult,
    FastPathUnavailable,
    settle_fast_path_create_on,
)
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_sqlite_common import (
    sqlite_connect,
    sqlite_create_sql,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_same_file,
    sqlite_source_carrier_census,
    sqlite_table_exists,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)


def sqlite_sqlite_copy_enabled() -> bool:
    raw = (getenv_brand("SQLITE_SQLITE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _attach_snapshot_meta(src_path: str, *, same_file: bool) -> dict[str, Any]:
    """Snapshot evidence for an ATTACH + INSERT SELECT.

    The source is read and the destination written inside the one
    ``BEGIN IMMEDIATE`` transaction on the destination connection; SQLite
    holds the attached source's read lock from the first SELECT until COMMIT,
    so COUNT, INSERT SELECT and the dest COUNT all describe the same source
    state — the same guarantee ``begin_sqlite_snapshot`` gives the row path.
    """
    return {
        "engine": "sqlite",
        "isolation": "immediate_transaction",
        "guarantee": "sqlite_transaction_snapshot",
        "snapshot_lsn": "",
        "export_snapshot": "",
        "path": str(src_path),
        "note": (
            "Source COUNT, INSERT SELECT and dest COUNT ran in one SQLite "
            "transaction" + (" on the shared file." if same_file else " over ATTACH.")
        ),
    }


def copy_sqlite_to_sqlite(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    source_where: str = "",
    ledger: CopyLedgerKey | None = None,
) -> FastPathResult:
    """ATTACH source and INSERT SELECT into dest. Dest COUNT(*) is the proof.

    ``source_where`` is a pre-quoted SQL fragment (incremental cursor predicate).
    When set, COUNT and INSERT SELECT use that filter, dest-occupied skip is
    disabled, and the load is a single shard.

    ``ledger`` arms the SQLite writer's same-transaction chunk ledger for the
    one shard this COPY commits: a retry of the same job reads the shard back
    from the ledger and returns the committed count instead of landing it twice.
    """
    del source_schema
    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlite_sqlite_copy_enabled():
        raise FastPathUnavailable("SQLite→SQLite COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    if sqlite_same_file(source_cfg, dest_cfg) and source_table.strip().lower() == dest_table.strip().lower():
        raise FastPathUnavailable("SQLite COPY onto the same table stays on the row path")

    src_path = sqlite_resolved_path(source_cfg)
    dest_cols = [p[1] for p in pairs]
    source_cols = [p[0] for p in pairs]
    # Always schema-qualified: once ``srcdb`` is attached, an unqualified
    # ``"t"`` that no longer exists in ``main`` resolves to ``srcdb."t"`` —
    # the operator's source table.
    dest_ref = f"main.{sqlite_ident(dest_table)}"
    same_file = sqlite_same_file(source_cfg, dest_cfg)
    dest_col_sql = ", ".join(sqlite_ident(c) for c in dest_cols)
    src_col_sql = ", ".join(sqlite_ident(c) for c in source_cols)
    attached = False
    conn = sqlite_connect(dest_cfg)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if ledger is not None:
            ledger_cur = conn.cursor()
            sqlite_writer.ensure_raw_write_ledger(ledger_cur, dialect="sqlite")
            already = sqlite_writer.raw_chunk_rows_written(
                ledger_cur,
                dialect="sqlite",
                job_id=ledger.job_id,
                batch_key=ledger.batch_key,
                chunk_idx=ledger.chunk_idx,
            )
            if already is not None and sqlite_table_exists(conn, dest_table):
                dest_count = int(
                    conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
                )
                conn.commit()
                proof = f"dest_count:{dest_count}"
                return FastPathResult(
                    rows_copied=already,
                    source_rows=already,
                    source_checksum=proof,
                    target_rows=dest_count,
                    target_checksum=proof,
                    source_snapshot={
                        "engine": "sqlite",
                        "copy_workers": 1,
                        "copy_split": "serial",
                        "copy_partitions": 1,
                        "partitions_skipped": 1,
                        "partitions_loaded": 0,
                        "shard_mode": "table",
                        "ledger_chunks_skipped": 1,
                        "sqlite_write": "ledger_skip",
                    },
                    proof_scope="write_ledger_shard_already_committed",
                )
        if same_file:
            src_ref = sqlite_ident(source_table)
            live = sqlite_pragma_types(conn, source_table)
        else:
            conn.execute("ATTACH DATABASE ? AS srcdb", (src_path,))
            attached = True
            src_ref = f"srcdb.{sqlite_ident(source_table)}"
            rows = conn.execute(
                f"PRAGMA srcdb.table_info({sqlite_ident(source_table)})"
            ).fetchall()
            live = {str(r[1]): str(r[2] or "TEXT") for r in rows}
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower())
            if declared is None:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not sqlite_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not SQLite COPY-safe"
                )
        cursor_where = (source_where or "").strip()
        where_sql = f" WHERE {cursor_where}" if cursor_where else ""
        sqlite_source_carrier_census(conn, src_ref, source_cols, sqlite_ddls, where_sql)
        source_count = int(
            conn.execute(f"SELECT COUNT(*) FROM {src_ref}{where_sql}").fetchone()[0]  # nosec B608
        )
        exists = sqlite_table_exists(conn, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = int(
                conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
            )
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if cursor_where:
                raise FastPathUnavailable(
                    "filtered COPY into occupied dest stays on the incremental staging path"
                )
            raise FastPathUnavailable(
                "append into occupied SQLite dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            conn.execute(f"DROP TABLE {dest_ref}")  # nosec B608
            exists = False
        if not exists:
            conn.execute(sqlite_create_sql(dest_table, pairs, sqlite_ddls))
            settle_fast_path_create_on(conn, dest_dialect="sqlite", dest_table=dest_table)

        sqlite_write = "overwrite" if replace_destination and dest_occupied else "insert"
        conn.execute(
            f"INSERT INTO {dest_ref} ({dest_col_sql}) "  # nosec B608
            f"SELECT {src_col_sql} FROM {src_ref}{where_sql}"
        )
        dest_count = int(
            conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count:
            raise ValueError(
                "SQLite→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        if ledger is not None:
            sqlite_writer.mark_raw_chunk_committed(
                conn.cursor(),
                dialect="sqlite",
                job_id=ledger.job_id,
                batch_key=ledger.batch_key,
                chunk_idx=ledger.chunk_idx,
                rows_written=dest_count,
                row_start=0,
                row_end=max(0, dest_count - 1),
                attempt=1,
            )
        conn.commit()
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                **_attach_snapshot_meta(src_path, same_file=same_file),
                "copy_workers": 1,
                "copy_split": "cursor" if cursor_where else "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "cursor" if cursor_where else "table",
                "source_where": bool(cursor_where),
                "sqlite_read": "attach_select" if not same_file else "same_file_select",
                "sqlite_write": sqlite_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        # CREATE, INSERT SELECT and the ledger mark all ride the one
        # BEGIN IMMEDIATE transaction; rollback is the whole cleanup. No DDL
        # runs here — a post-rollback DROP has nothing of ours left to drop.
        try:
            conn.rollback()
        except Exception:
            logger.debug("SQLite dest rollback skipped", exc_info=True)
        raise
    finally:
        if attached:
            try:
                conn.execute("DETACH DATABASE srcdb")
            except Exception:
                logger.debug("SQLite DETACH skipped", exc_info=True)
        try:
            conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)
