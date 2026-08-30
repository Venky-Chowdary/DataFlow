"""Batch advanced modes at 100K: incremental-deduped, SCD2, mirror, reverse ETL.

Each mode is defined by what the destination holds after the *second* and
*third* run, not by the first run's success:

``incremental_deduped``
    the same source twice must land N, not 2N, and a keyed re-read of changed
    rows must update in place rather than accumulate versions.
``scd2``
    an update must *close* the previous version (``valid_to`` set,
    ``is_current`` false) and leave exactly one current row per business key —
    a destination that merely appends a second row with both marked current has
    silently doubled every downstream join.
``mirror``
    a row deleted at the source must stop being current at the destination.
``reverse_etl``
    an operational-system write must be idempotent across runs.

Everything is read back on an independent driver connection.
"""

from __future__ import annotations

from typing import Any, Sequence

from tests.scale import live_engines as L
from tests.scale import stores
from tests.scale.matrix import Cell, Matrix, contract, fill, run_transfer

COLS = ["id", "region", "amount", "note", "updated_at"]
SHAPE = "id BIGINT PK, region TEXT, amount NUMERIC(12,2), note TEXT NULL, updated_at TIMESTAMP"

DELTA_ROWS = 2000
UPDATE_ROWS = 1000
DELETE_ROWS = 1000
LATE_ROWS = 500


def _run(
    src_dialect: str,
    src_obj: str,
    dst_dialect: str,
    dst_obj: str,
    *,
    mode: str,
    cursor_field: str = "",
    primary_key: str = "id",
    cols: Sequence[str] = tuple(COLS),
    job_id: str = "",
) -> tuple[Any, float, str]:
    return run_transfer(
        stores.endpoint(src_dialect, src_obj),
        stores.endpoint(dst_dialect, dst_obj),
        mode=mode,
        contracts=[
            contract(src_obj, mode, primary_key=primary_key, cursor_field=cursor_field)
        ],
        cols=cols,
        job_id=job_id,
        omit=stores.omitted_columns(src_dialect),
    )


def _cell(
    matrix: Matrix,
    route: str,
    mode: str,
    result: Any,
    elapsed: float,
    run_id: str,
    *,
    src_rows: int,
    dst_rows: int,
    src_checksum: str = "",
    dst_checksum: str = "",
    ok: bool,
    note: str,
    detail: dict[str, Any] | None = None,
) -> Cell:
    cell = Cell(route=route, mode=mode, schema_shape=SHAPE)
    fill(cell, result, elapsed, run_id)
    cell.source_rows = src_rows
    cell.dest_rows = dst_rows
    cell.source_checksum = src_checksum
    cell.dest_checksum = dst_checksum
    cell.detail = detail or {}
    if not result.success:
        cell.mark(False, str(result.error or "")[:240])
    else:
        cell.mark(ok, note)
    return matrix.add(cell)


# --------------------------------------------------------------------------
# incremental deduped: three runs, delta capture, late-arriving update
# --------------------------------------------------------------------------

def incremental_deduped_cells(matrix: Matrix, rows: int, dialect: str = "postgresql",
                              dest: str = "postgresql") -> None:
    route = f"{dialect}→{dest}"
    src, dst = f"sc_inc_{dialect[:2]}{dest[:2]}_s", f"sc_inc_{dialect[:2]}{dest[:2]}_d"
    job = f"scinc{dialect[:2]}{dest[:2]}"
    stores.drop(dialect, src)
    stores.drop(dest, dst)
    stores.seed(dialect, src, rows)

    def run() -> tuple[Any, float, str]:
        return _run(dialect, src, dest, dst, mode="incremental_deduped",
                    cursor_field="updated_at", job_id=job)

    result, elapsed, run_id = run()
    src_n = stores.count(dialect, src)
    dst_n = stores.count(dest, dst) if result.success else 0
    _cell(matrix, route, "incremental_deduped run 1 (snapshot)", result, elapsed, run_id,
          src_rows=src_n, dst_rows=dst_n,
          src_checksum=L.checksum(stores.projection(dialect, src, COLS)) if result.success else "",
          dst_checksum=L.checksum(stores.projection(dest, dst, COLS)) if result.success else "",
          ok=dst_n == src_n, note=f"landed {dst_n} of {src_n}")
    if not result.success:
        return

    # run 2 and run 3 with no source change: a keyed mode must be idempotent.
    for attempt in (2, 3):
        r, e, rid = run()
        dst_n2 = stores.count(dest, dst) if r.success else 0
        _cell(matrix, route, f"incremental_deduped run {attempt} (idempotent)", r, e, rid,
              src_rows=stores.count(dialect, src), dst_rows=dst_n2,
              src_checksum=L.checksum(stores.projection(dialect, src, COLS)),
              dst_checksum=L.checksum(stores.projection(dest, dst, COLS)) if r.success else "",
              ok=dst_n2 == dst_n,
              note="destination unchanged" if dst_n2 == dst_n
              else f"destination moved {dst_n} → {dst_n2} with no source change")

    # delta: new rows plus in-place updates whose cursor moves forward.
    stores.append(dialect, src, DELTA_ROWS, start=rows + 1)
    _bump_updated_at(dialect, src, UPDATE_ROWS, days=1)
    r, e, rid = run()
    src_n3 = stores.count(dialect, src)
    dst_n3 = stores.count(dest, dst) if r.success else 0
    src_ck = L.checksum(stores.projection(dialect, src, COLS))
    dst_ck = L.checksum(stores.projection(dest, dst, COLS)) if r.success else ""
    _cell(matrix, route, "incremental_deduped delta + dedupe", r, e, rid,
          src_rows=src_n3, dst_rows=dst_n3, src_checksum=src_ck, dst_checksum=dst_ck,
          ok=dst_n3 == src_n3 and src_ck == dst_ck,
          note=f"{DELTA_ROWS} inserts and {UPDATE_ROWS} updates merged in place"
          if dst_n3 == src_n3 and src_ck == dst_ck
          else f"dest={dst_n3} src={src_n3} checksum_match={src_ck == dst_ck}")

    # late-arriving update: the row changes but its cursor value lands *below*
    # the stored watermark. A cursor read cannot see it; only a log reader can.
    _bump_updated_at(dialect, src, LATE_ROWS, days=-30, note="late")
    r, e, rid = run()
    dst_ck4 = L.checksum(stores.projection(dest, dst, COLS)) if r.success else ""
    src_ck4 = L.checksum(stores.projection(dialect, src, COLS))
    captured = src_ck4 == dst_ck4
    _cell(matrix, route, "incremental_deduped late-arriving update", r, e, rid,
          src_rows=stores.count(dialect, src),
          dst_rows=stores.count(dest, dst) if r.success else 0,
          src_checksum=src_ck4, dst_checksum=dst_ck4, ok=captured,
          note="late update captured"
          if captured
          else f"{LATE_ROWS} rows changed below the watermark were not captured — "
               f"inherent to cursor-based incremental; the CDC route captures them",
          detail={"late_rows": LATE_ROWS, "captured": captured})


def _bump_updated_at(dialect: str, obj: str, rows: int, *, days: int,
                     note: str = "upd") -> None:
    if dialect == "postgresql":
        L.pg_exec(
            f'UPDATE public."{obj}" SET amount = amount + 1, note = %s, '
            f"updated_at = updated_at + make_interval(days => {days}) "
            f"WHERE id <= {rows}",
            (note,),
        )
    elif dialect == "mysql":
        L.mysql_exec(
            f"UPDATE `{obj}` SET amount = amount + 1, note = %s, "
            f"updated_at = DATE_ADD(updated_at, INTERVAL {days} DAY) "
            f"WHERE id <= {rows}",
            (note,),
        )
    else:
        client = L.mongo_client()
        try:
            client[L.MONGO_DB][obj].update_many(
                {"id": {"$lte": rows}}, {"$set": {"note": note}}
            )
        finally:
            client.close()


# --------------------------------------------------------------------------
# upsert-MERGE: composite keys and a null in the key
# --------------------------------------------------------------------------

def composite_key_cells(matrix: Matrix, rows: int) -> None:
    src, dst = "sc_comp_s", "sc_comp_d"
    cols = ["tenant_id", "id", "amount", "updated_at"]
    L.pg_drop(src)
    L.pg_drop(dst)
    L.pg_exec(
        f'CREATE TABLE public."{src}" (tenant_id TEXT, id BIGINT, '
        f"amount NUMERIC(12,2) NOT NULL, updated_at TIMESTAMP NOT NULL, "
        f"PRIMARY KEY (tenant_id, id))"
    )
    L.pg_exec(
        f'INSERT INTO public."{src}" (tenant_id, id, amount, updated_at) '
        f"SELECT 't' || (g % 5), g, (g % 100000)::numeric / 100, "
        f"TIMESTAMP '2024-01-01 00:00:00' + (g || ' seconds')::interval "
        f"FROM generate_series(1, {rows}) g"
    )

    def run() -> tuple[Any, float, str]:
        return run_transfer(
            stores.endpoint("postgresql", src),
            stores.endpoint("postgresql", dst),
            mode="upsert",
            contracts=[contract(src, "upsert", primary_key="tenant_id,id")],
            cols=cols,
            job_id="sccomp",
        )

    r1, e1, id1 = run()
    dst_n = L.pg_count(dst) if r1.success else 0
    _cell(matrix, "postgresql→postgresql", "upsert composite key run 1", r1, e1, id1,
          src_rows=L.pg_count(src), dst_rows=dst_n, ok=dst_n == rows,
          note=f"composite (tenant_id, id) landed {dst_n}")
    if not r1.success:
        return
    r2, e2, id2 = run()
    dst_n2 = L.pg_count(dst)
    src_ck = L.checksum(L.pg_projection(src, cols, order="id"))
    dst_ck = L.checksum(L.pg_projection(dst, cols, order="id"))
    _cell(matrix, "postgresql→postgresql", "upsert composite key run 2 (idempotent)",
          r2, e2, id2, src_rows=L.pg_count(src), dst_rows=dst_n2,
          src_checksum=src_ck, dst_checksum=dst_ck,
          ok=dst_n2 == rows and src_ck == dst_ck,
          note="no duplicate versions under composite key"
          if dst_n2 == rows else f"destination grew to {dst_n2}")

    # null in one key column: the row has no identity, so it must be refused or
    # quarantined. Writing it lands a row no later update can ever address.
    L.pg_exec(f'ALTER TABLE public."{src}" DROP CONSTRAINT "{src}_pkey"')
    L.pg_exec(
        f'INSERT INTO public."{src}" (tenant_id, id, amount, updated_at) '
        f"SELECT NULL, {rows} + g, 1.00, TIMESTAMP '2025-01-01 00:00:00' "
        f"FROM generate_series(1, 100) g"
    )
    r3, e3, id3 = run()
    dst_n3 = L.pg_count(dst)
    null_keys_landed = L.pg_count(dst, "tenant_id IS NULL")
    refused = not r3.success
    accounted = (r3.row_accounting or {}) if hasattr(r3, "row_accounting") else {}
    quarantined = int(accounted.get("quarantined") or 0)
    ok = refused or quarantined >= 100 or null_keys_landed == 0
    cell = Cell(route="postgresql→postgresql", mode="upsert null-in-key", schema_shape=SHAPE)
    fill(cell, r3, e3, id3)
    cell.source_rows = L.pg_count(src)
    cell.dest_rows = dst_n3
    cell.detail = {
        "null_key_rows_at_source": 100,
        "null_key_rows_landed": null_keys_landed,
        "run_refused": refused,
        "quarantined": quarantined,
    }
    cell.mark(
        ok,
        "null-keyed rows refused or quarantined, not written unaddressable"
        if ok
        else f"{null_keys_landed} rows with a NULL key component landed at the "
             f"destination with no addressable identity",
    )
    matrix.add(cell)


# --------------------------------------------------------------------------
# SCD2
# --------------------------------------------------------------------------

def scd2_cells(matrix: Matrix, rows: int) -> None:
    src, dst = "sc_scd_s", "sc_scd_d"
    L.pg_drop(src)
    L.pg_drop(dst)
    L.seed_pg_scale(src, rows)

    def run() -> tuple[Any, float, str]:
        return _run("postgresql", src, "postgresql", dst, mode="scd2", job_id="scscd2")

    r1, e1, id1 = run()
    total = L.pg_count(dst) if r1.success else 0
    _cell(matrix, "postgresql→postgresql", "scd2 run 1 (initial versions)", r1, e1, id1,
          src_rows=L.pg_count(src), dst_rows=total, ok=total == rows,
          note=f"one version per business key ({total})")
    if not r1.success:
        return

    cols = L.pg_columns(dst)
    current_col = next((c for c in cols if c.lower() in {"is_current", "_is_current"}), "")
    valid_to_col = next((c for c in cols if c.lower() in {"valid_to", "_valid_to"}), "")
    if not current_col or not valid_to_col:
        matrix.add(
            Cell(route="postgresql→postgresql", mode="scd2 version closure").mark(
                False,
                f"destination has no SCD2 bookkeeping columns: {sorted(cols)}",
            )
        )
        return

    L.pg_exec(
        f'UPDATE public."{src}" SET amount = amount + 7, note = \'scd2-upd\', '
        f"updated_at = updated_at + interval '1 day' WHERE id <= {UPDATE_ROWS}"
    )
    r2, e2, id2 = run()
    total2 = L.pg_count(dst)
    current = L.pg_count(dst, f'"{current_col}" IS TRUE')
    closed = L.pg_count(
        dst, f'"{current_col}" IS NOT TRUE AND "{valid_to_col}" IS NOT NULL'
    )
    open_closed = L.pg_count(
        dst, f'"{current_col}" IS NOT TRUE AND "{valid_to_col}" IS NULL'
    )
    dupe_current = L.pg_fetch(
        f'SELECT count(*) FROM (SELECT id FROM public."{dst}" '
        f'WHERE "{current_col}" IS TRUE GROUP BY id HAVING count(*) > 1) q'
    )[0][0]
    ok = (
        total2 == rows + UPDATE_ROWS
        and current == rows
        and closed == UPDATE_ROWS
        and open_closed == 0
        and int(dupe_current) == 0
    )
    cell = Cell(route="postgresql→postgresql", mode="scd2 version closure", schema_shape=SHAPE)
    fill(cell, r2, e2, id2)
    cell.source_rows = L.pg_count(src)
    cell.dest_rows = total2
    cell.detail = {
        "current_rows": current,
        "closed_rows": closed,
        "closed_without_valid_to": open_closed,
        "business_keys_with_two_current_rows": int(dupe_current),
        "current_col": current_col,
        "valid_to_col": valid_to_col,
    }
    cell.mark(
        ok,
        f"{UPDATE_ROWS} old versions closed (valid_to set, is_current false); "
        f"exactly one current row per key",
    )
    matrix.add(cell)

    # unchanged re-run: SCD2 must not mint a new version for an identical row.
    r3, e3, id3 = run()
    total3 = L.pg_count(dst)
    current3 = L.pg_count(dst, f'"{current_col}" IS TRUE')
    churn = Cell(route="postgresql→postgresql", mode="scd2 no churn on re-run",
                 schema_shape=SHAPE)
    fill(churn, r3, e3, id3)
    churn.source_rows = L.pg_count(src)
    churn.dest_rows = total3
    churn.detail = {"current_rows": current3, "versions_before": total2}
    churn.mark(
        total3 == total2 and current3 == current,
        "no new versions for unchanged rows"
        if total3 == total2
        else f"versions grew {total2} → {total3} with no source change",
    )
    matrix.add(churn)


# --------------------------------------------------------------------------
# mirror
# --------------------------------------------------------------------------

def mirror_cells(matrix: Matrix, rows: int, dest: str = "postgresql") -> None:
    route = f"postgresql→{dest}"
    src, dst = f"sc_mir_{dest[:2]}_s", f"sc_mir_{dest[:2]}_d"
    L.pg_drop(src)
    stores.drop(dest, dst)
    L.seed_pg_scale(src, rows)

    def run() -> tuple[Any, float, str]:
        return _run("postgresql", src, dest, dst, mode="mirror", job_id=f"scmir{dest[:2]}")

    r1, e1, id1 = run()
    dst_n = stores.count(dest, dst) if r1.success else 0
    _cell(matrix, route, "mirror run 1", r1, e1, id1, src_rows=L.pg_count(src),
          dst_rows=dst_n, ok=dst_n == rows, note=f"mirrored {dst_n}")
    if not r1.success:
        return

    lo, hi = 5001, 5000 + DELETE_ROWS
    L.pg_exec(f'DELETE FROM public."{src}" WHERE id BETWEEN {lo} AND {hi}')
    r2, e2, id2 = run()
    cols = stores.columns(dest, dst)
    soft = next((c for c in cols if c.lower() in {"_deleted", "is_deleted"}), "")
    if soft:
        active_clause = (
            f'COALESCE("{soft}", FALSE) = FALSE' if dest == "postgresql"
            else f"COALESCE(`{soft}`, 0) = 0"
        )
        active = stores.count(dest, dst, active_clause)
        survivors = stores.count(
            dest, dst,
            f"id BETWEEN {lo} AND {hi} AND {active_clause}",
        )
    else:
        active = stores.count(dest, dst)
        survivors = stores.count(dest, dst, f"id BETWEEN {lo} AND {hi}")
    src_n = L.pg_count(src)
    cell = Cell(route=route, mode="mirror source-delete propagation", schema_shape=SHAPE)
    fill(cell, r2, e2, id2)
    cell.source_rows = src_n
    cell.dest_rows = active
    cell.delete_capture = "yes" if survivors == 0 else "no"
    cell.detail = {
        "deleted_at_source": DELETE_ROWS,
        "still_active_at_destination": survivors,
        "soft_delete_column": soft or None,
    }
    if r2.success:
        cell.source_checksum = L.checksum(L.pg_projection(src, COLS))
        cell.mark(
            survivors == 0 and active == src_n,
            f"{DELETE_ROWS} source deletions removed at the destination"
            if survivors == 0 and active == src_n
            else f"{survivors} deleted keys still active; active={active} src={src_n}",
        )
    else:
        cell.mark(False, str(r2.error or "")[:240])
    matrix.add(cell)


# --------------------------------------------------------------------------
# reverse ETL
# --------------------------------------------------------------------------

def reverse_etl_cells(matrix: Matrix, rows: int) -> None:
    src, dst = "sc_retl_s", "sc_retl_d"
    L.pg_drop(src)
    L.mysql_drop(dst)
    L.seed_pg_scale(src, rows)

    def run() -> tuple[Any, float, str]:
        return _run("postgresql", src, "mysql", dst, mode="reverse_etl", job_id="scretl")

    r1, e1, id1 = run()
    dst_n = L.mysql_count(dst) if r1.success else 0
    _cell(matrix, "postgresql→mysql", "reverse_etl run 1", r1, e1, id1,
          src_rows=L.pg_count(src), dst_rows=dst_n, ok=dst_n == rows,
          note=f"activated {dst_n} rows into the operational store")
    if not r1.success:
        return
    r2, e2, id2 = run()
    dst_n2 = L.mysql_count(dst)
    _cell(matrix, "postgresql→mysql", "reverse_etl run 2 (idempotent)", r2, e2, id2,
          src_rows=L.pg_count(src), dst_rows=dst_n2, ok=dst_n2 == rows,
          note="no duplicate activation" if dst_n2 == rows
          else f"destination grew to {dst_n2}")


def batch_mode_cells(matrix: Matrix, rows: int) -> None:
    if not stores.reachable("postgresql"):
        matrix.add(Cell(route="postgresql", mode="batch modes").skip("PostgreSQL 5432 unreachable"))
        return
    incremental_deduped_cells(matrix, rows, "postgresql", "postgresql")
    if stores.reachable("mysql"):
        incremental_deduped_cells(matrix, rows, "mysql", "postgresql")
        incremental_deduped_cells(matrix, rows, "postgresql", "mysql")
    composite_key_cells(matrix, rows)
    scd2_cells(matrix, rows)
    mirror_cells(matrix, rows, "postgresql")
    if stores.reachable("mysql"):
        mirror_cells(matrix, rows, "mysql")
        reverse_etl_cells(matrix, rows)
