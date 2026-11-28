"""D437 - confirmed CDC drift repair inside the destination's exactly-once transaction.

``repair_cdc_drift`` re-copies the drifted keys with the committed position
stamped as ``_df_lsn`` and refuses any row a newer change already moved
(an *inclusive* compare-and-set). On SQLite, PostgreSQL and MySQL the
at-least-once writer pushes that guard into its upsert SQL. SQL Server,
Oracle and generic SQL writers have no such guard, so the repair was refused
there after the preview had already offered it, and the route stayed red
with no way back (QA round 73, PostgreSQL CDC into SQL Server).

These destinations already apply every CDC batch through the dest-owned
watermark protocol (``connectors.cdc_eos_sa``). The repair now uses the same
protocol: one destination transaction per key that locks the stream's
watermark row (serialising with any apply that could still run), checks the
repair's lease fence against it, reads the row's ``_df_lsn`` and writes the
source image only when that LSN is not newer than the committed position.
The watermark itself is never moved: this is state convergence, not replay.
"""

from __future__ import annotations

from typing import Any

from connectors.lsn_guards import DF_LSN_COL, compare_lsn, lsn_family

#: Driver types repaired through the watermark transaction. Each one has a live
#: proof in CI (cdc-sqlserver, oracle-live) or locally (generic SQL on SQLite).
EOS_TRANSACTION_REPAIR_DESTS = frozenset({
    "sqlserver",
    "mssql",
    "azure_sql",
    "azure_sql_database",
    "amazon_rds_sql_server",
    "oracle",
    "oracle_db",
    "oracle_autonomous_warehouse",
    "generic_sql",
})


def repair_overwrite_allowed(existing_lsn: Any, committed_lsn: str) -> bool:
    """Inclusive compare-and-set: overwrite unless a newer change holds the row.

    An empty destination LSN is a row no change stamped (or no row at all).
    Stamps of different families are not comparable, so they are refused
    rather than guessed (``compare_lsn`` returns 0 for them).
    """
    if existing_lsn is None or not str(existing_lsn).strip():
        return True
    if lsn_family(existing_lsn) != lsn_family(committed_lsn):
        return False
    return compare_lsn(committed_lsn, existing_lsn) >= 0


def repair_row_in_eos_transaction(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    stream_key: str,
    row: dict[str, Any],
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    pk_targets: list[str],
    committed_lsn: str,
    writer_fence: int,
) -> str:
    """Re-copy one source row under the stream's watermark lock.

    Returns ``"repaired"`` or ``"refused_newer_event"``. A stale lease
    generation raises ``ExactlyOnceRouteError`` and writes nothing.
    """
    from sqlalchemy import text

    from connectors import cdc_eos_sa as sa
    from connectors.generic_sql import _engine
    from services.cdc_exactly_once import assert_writer_fence
    from services.engine_pool import release_engine

    dialect = sa.normalize_eos_dialect(dest_type, dest_cfg)
    cfg = dict(dest_cfg)
    cfg.setdefault("type", dialect if dialect != "generic_sql" else (dest_cfg.get("type") or dest_type))
    engine = _engine(cfg)
    wm_token = sa._enter_wm_scope(engine, dialect)
    try:
        shape_maps, shape_types, target_cols, logical_by = sa._eos_write_shape(
            dialect, mappings, column_types
        )
        tgt_to_src = {
            str(m.get("target") or m.get("source") or ""): str(m.get("source") or "")
            for m in shape_maps
            if m.get("source")
        }
        present = {str(k) for k in row}
        # A column the scan did not return is left as the destination holds it.
        absent = {
            c for c in target_cols
            if c != DF_LSN_COL and tgt_to_src.get(c, c) not in present and c not in present
        }
        values = sa._coerce_eos_row(
            sa._apply_mapped_transforms(
                {
                    c: v
                    for c, v in sa._row_values(row, target_cols, tgt_to_src, committed_lsn).items()
                    if c not in absent
                },
                shape_maps,
                shape_types,
            ),
            logical_by,
            dialect,
        )
        table_q = sa._q(dest_table, dialect)
        where = " AND ".join(f"{sa._q(c, dialect)} = :pk_{i}" for i, c in enumerate(pk_targets))
        binds = {f"pk_{i}": values.get(c) for i, c in enumerate(pk_targets)}

        def _work(conn: Any) -> str:
            # The same lock every batch apply takes first: no apply can commit
            # between this read and the write below.
            view = sa._lock_watermark(conn, dialect, stream_key)
            assert_writer_fence(writer_fence, view.fence_epoch)
            held = conn.execute(
                text(f"SELECT {sa._q(DF_LSN_COL, dialect)} FROM {table_q} WHERE {where}"),  # nosec B608
                binds,
            ).fetchone()
            if held is not None and not repair_overwrite_allowed(held[0], committed_lsn):
                return "refused_newer_event"
            full = {c: values.get(c) for c in target_cols}
            written = sa._upsert_row(
                conn, table_q, target_cols, pk_targets, full, dialect,
                absent=absent, allow_equal_lsn=True,
            )
            # The upsert re-checks the stamp itself and always sets _df_lsn, so
            # writing nothing means its guard refused: never claim a repair.
            return "repaired" if written else "refused_newer_event"

        return sa._in_dest_transaction(engine, _work)
    finally:
        sa._WM_TABLE.reset(wm_token)
        release_engine(engine)
