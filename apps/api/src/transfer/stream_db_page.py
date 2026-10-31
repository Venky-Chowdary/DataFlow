"""Per-page filtering and cursor advance of the streaming DB loop."""

from __future__ import annotations

from typing import Any

from connectors.writer_common import row_fingerprints
from services.keyset_pagination import header_column_index
from services.row_filter import apply_row_filter_to_matrix

from .stream_boundary import _boundary_append_dedupe
from .stream_row_accounting import (
    _mark_raw_page,
    _raw_page_cursor,
    _raw_page_marked,
    _raw_page_rows,
)
from . import stream as _stream


def _boundary_rows_already_landed(run, headers: list[str], rows: list[list[str]]) -> set[int]:
    """Indices of re-read tie-group rows the destination already holds."""
    cfg = run.cfg
    cur = run.cur
    from services.sync_cursor import requires_upsert
    if not (cfg._tie_dedupe and rows):
        return set()
    cidx = header_column_index(headers or [], cfg.cursor_source_col)
    if cidx is None:
        return set()
    try:
        from services.keyset_pagination import split_cursor_bookmark
        from services.target_sample import read_target_sample
        from services.value_serializer import present_cell_text

        from connectors.cursor_bind import cursor_instant_key

        cur_val, _ = split_cursor_bookmark(str(cfg.watermark), has_tiebreak=True)
        # A zone-bearing tie group is one instant written with any offset:
        # ``02:45-08:00`` and ``16:15+05:30`` are both at a ``10:45Z`` watermark.
        wm_instant = cursor_instant_key(str(cur_val))

        def _at_watermark(cell: Any) -> bool:
            text = present_cell_text(cell)
            if text == str(cur_val):
                return True
            return wm_instant is not None and cursor_instant_key(text) == wm_instant

        boundary_idx = [
            i for i, row in enumerate(rows) if cidx < len(row) and _at_watermark(row[cidx])
        ]
        if not boundary_idx:
            return set()
        all_cols = list(cfg.target_cols or [])
        cursor_dest_col = str(
            next(
                (
                    m.get("target")
                    for m in cfg.mappings
                    if m.get("source") == cfg.cursor_source_col
                ),
                cfg.cursor_source_col,
            )
        )
        match_cols = all_cols
        if (
            not requires_upsert(cfg.effective_sync)
            and cfg.pk_target_cols
            and all(c in all_cols for c in cfg.pk_target_cols)
        ):
            match_cols = [cursor_dest_col] + [
                c for c in cfg.pk_target_cols if c != cursor_dest_col
            ]
        if cur._boundary_fps_state["fps"] is None:
            # A run that ADDs a column (approved schema evolution) re-reads
            # tie peers whose new column the destination does not have yet.
            # Selecting it failed the read, which fails open: every peer was
            # delivered twice. Compare on the columns the table holds, and
            # keep a peer only when its new columns carry a value.
            landed = read_target_sample(
                cfg.dest_bind_dialect or cfg.dest_type,
                cfg.dest_cfg,
                schema=str(cfg.dest_cfg.get("schema") or "public"),
                table_name=cfg.dest_table,
                columns=None,
                limit=1,
                sort_key=cursor_dest_col,
                key_values=[cur_val],
            )
            live_cols = {str(k).lower() for k in (landed[0] if landed else {})}
            read_cols = [c for c in all_cols if c.lower() in live_cols] or all_cols
            cur._boundary_fps_state["new_cols"] = [
                c for c in all_cols if c.lower() not in live_cols
            ] if landed else []
            match_cols = [c for c in match_cols if c in read_cols] or match_cols
            cur._boundary_fps_state["match_cols"] = match_cols
            dest_rows = read_target_sample(
                cfg.dest_bind_dialect or cfg.dest_type,
                cfg.dest_cfg,
                schema=str(cfg.dest_cfg.get("schema") or "public"),
                table_name=cfg.dest_table,
                columns=read_cols,
                limit=10_000,
                sort_key=cursor_dest_col,
                key_values=[cur_val],
            )
            dest_tups = []
            for dr in dest_rows or []:
                low = {str(k).lower(): v for k, v in dr.items()}
                dest_tups.append(tuple(low.get(str(c).lower()) for c in match_cols))
            cur._boundary_fps_state["fps"] = {
                fp
                for _k, fp in row_fingerprints(
                    dest_tups,
                    match_cols,
                    dest_db_type=cfg.dest_bind_dialect,
                    dest_types=cfg.digest_dest_types,
                )
            }
        dest_fps = cur._boundary_fps_state["fps"]
        if not dest_fps:
            return set()
        match_cols = cur._boundary_fps_state["match_cols"]
        drop = _boundary_append_dedupe(
            list(headers or []),
            [rows[i] for i in boundary_idx],
            dest_fps,
            match_cols=match_cols,
            new_cols=cur._boundary_fps_state["new_cols"],
            mappings=cfg.mappings,
            target_cols=all_cols,
            column_types=cfg.column_types,
            stream_error_policy=cfg.stream_error_policy,
            digest_dest_types=cfg.digest_dest_types,
            dest_bind_dialect=cfg.dest_bind_dialect,
            # The write path's remap basis — the same one Gate-8's re-read uses.
            remap_dest_types=cfg.fingerprint_dest_types,
            destination_pk_columns=list(cfg.pk_target_cols or []),
        )
        if drop:
            _stream.logger.info(
                "Boundary re-read dedupe dropped %d already-present row(s) "
                "at watermark %r.",
                len(drop),
                str(cur_val),
            )
        return {boundary_idx[i] for i in drop}
    except Exception as exc:  # fail open — dedupe must never lose a row
        _stream.logger.warning("Boundary dedupe skipped (fail-open): %s", exc)
        return set()


def _rows_after_watermark_matrix(
    run,
    headers: list[str], rows: list[list[str]]
) -> tuple[list[list[str]], int]:
    """Bound a read page to this run's window past the watermark.

    The comparison goes through the same typed comparator the SQL readers
    bound with, so a Redis or Dynamo delta means the same thing the seed's
    ``WHERE cursor >= :watermark`` means — the tie group at the watermark
    included (D124). Returns the kept rows and how many carried no cursor
    value at all.
    """
    cfg = run.cfg
    from services.sync_cursor import row_in_run_window
    cursor_idx = header_column_index(headers, cfg.cursor_source_col)
    if cursor_idx is None:
        # The cursor column is not on the page: nothing here can be proven
        # historical, and dropping the page would lose rows.
        return list(rows), 0
    kept: list[list[str]] = []
    unbounded = 0
    for row in rows:
        verdict = row_in_run_window(
            {cfg.cursor_source_col: row[cursor_idx]}, cfg.cursor_source_col, cfg.watermark
        )
        if verdict is None:
            unbounded += 1
            continue
        if verdict:
            kept.append(row)
    return kept, unbounded


def _filter_batch(run, batch):
    """Source filter, then the approved recipe — once per page.

    The page carries its own pre-filter, pre-shape row count: the source
    OFFSET must advance by the rows the source handed over, not by the rows
    that survived a filter or a recipe, or every dropped row shifts the next
    page and the tail of the table is never read.

    That mark is also what makes this idempotent, and why both rewrites are
    recorded by one mark: they happen together, in this call, so the mark
    means "this page has already been rewritten". The first page passes
    through here twice (its DDL is committed before any worker starts), and a
    page already shaped is returned untouched: shaping it again would round a
    rounded value and count the page's effects into the run's ledger twice.
    """
    cfg = run.cfg
    from services.sync_cursor import max_cursor_value
    from services.keyset_pagination import max_keyset_bookmark
    if not batch:
        return batch
    if _raw_page_marked(batch):
        return batch
    raw_rows = len(batch.rows or [])
    if raw_rows and (
        cfg.source_filter
        or cfg.shape_runner is not None
        or cfg.client_cursor_bound
        or cfg._tie_dedupe
        or cfg.fk_orphan_guard is not None
    ):
        # Pagination bookmarks belong to the rows the source handed over. A
        # recipe that drops the page's highest key would otherwise bookmark a
        # lower one and the next read would hand those rows over again.
        order_cols = cfg.keyset_order_cols or (
            [cfg.keyset_col] + ([cfg.keyset_tiebreak] if cfg.keyset_tiebreak else [])
        )
        keyset_mark = max_keyset_bookmark(batch.rows, batch.headers, order_cols)
        if keyset_mark is None and header_column_index(
            batch.headers or [], cfg.keyset_col
        ) is not None:
            keyset_mark = max_cursor_value(
                batch.rows, batch.headers, cfg.keyset_col, cfg.keyset_tiebreak or None
            )
        cursor_mark = ""
        if cfg.incremental and cfg.cursor_source_col:
            cursor_mark = (
                max_cursor_value(
                    batch.rows,
                    batch.headers,
                    cfg.cursor_source_col,
                    cfg.cursor_pk_source or None,
                )
                or ""
            )
        if not _mark_raw_page(batch, raw_rows, cursor_mark, keyset_mark or ""):
            raise ValueError(
                "this source's read page cannot record the page as it was read, "
                "so a filter or a transform recipe cannot be proven to be applied "
                "to it exactly once, nor the next page proven to resume where "
                "this one ended — refusing rather than risk skipping source rows"
            )
    if cfg.client_cursor_bound and batch.rows:
        # The source could not push the cursor bound into its read call, so
        # it handed the whole keyspace over. Honouring the bound here is
        # what makes "incremental" mean incremental for it: without this the
        # second run re-offered every row and an append destination held 2N.
        before = len(batch.rows)
        kept, unbounded = _rows_after_watermark_matrix(run,
            batch.headers, batch.rows
        )
        if unbounded:
            # A row with no cursor value cannot be proven new or already at
            # rest. Skipping it loses data and sending it duplicates data,
            # so the run refuses instead of choosing silently.
            raise ValueError(
                f"{unbounded} row(s) carry no value for cursor "
                f"'{cfg.cursor_source_col}' — an incremental read of "
                f"{cfg.src_type} cannot prove whether they already landed. "
                "Fill the cursor column at the source, or run this sync as "
                "full refresh."
            )
        batch.rows = kept
        excluded = before - len(kept)
        if excluded:
            try:
                batch.raw_page_cursor_bounded = excluded
            except AttributeError as exc:
                raise ValueError(
                    "this source's read page cannot record how many rows the "
                    "incremental cursor bound excluded, so the run's source "
                    "population cannot be stated — refusing rather than "
                    "counting rows this run was never asked to move"
                ) from exc
    if cfg._tie_dedupe and batch.rows:
        # Tie peers an earlier run already delivered are outside this
        # run's scope, exactly like rows below the watermark.
        landed = _boundary_rows_already_landed(run, batch.headers, batch.rows)
        if landed:
            batch.rows = [r for i, r in enumerate(batch.rows) if i not in landed]
            try:
                batch.raw_page_cursor_bounded = (
                    int(getattr(batch, "raw_page_cursor_bounded", 0) or 0)
                    + len(landed)
                )
            except AttributeError as exc:
                raise ValueError(
                    "this source's read page cannot record the re-read tie "
                    "rows it dropped, so the run's source population cannot "
                    "be stated — refusing rather than miscounting them"
                ) from exc
    if cfg.source_filter and batch.rows:
        before = len(batch.rows)
        batch.rows = apply_row_filter_to_matrix(
            batch.headers, batch.rows, cfg.source_filter
        )
        removed = before - len(batch.rows)
        try:
            batch.raw_page_filtered = removed
        except AttributeError as exc:
            # Without this the removal would be charged to the recipe, and
            # proof would name the wrong authority for a missing row.
            raise ValueError(
                "this source's read page cannot record how many rows the "
                "declared source filter removed, so proof cannot say whether "
                "the filter or the transform recipe removed a row — refusing "
                "rather than attributing it to the wrong authority"
            ) from exc
    if cfg.fk_orphan_guard is not None and batch.rows:
        # fk__pg-maria/fk__pg-pg: a child row whose parent is absent at the
        # destination must quarantine, never land. The guard anti-joins
        # this page's FK tuples against the destination parent table.
        kept_rows, orphan_details = cfg.fk_orphan_guard.partition(
            list(batch.headers or []), list(batch.rows)
        )
        if (
            orphan_details
            and _stream._fk_orphans_fail_closed(cfg.validation_mode)
            and not getattr(cfg.fk_orphan_guard, "risk_acknowledged", False)
        ):
            raise ValueError(_stream._fk_orphan_violation_message(orphan_details))
        if orphan_details:
            batch.rows = kept_rows
            try:
                batch.fk_orphan_details = orphan_details
            except AttributeError as exc:
                raise ValueError(
                    "this source's read page cannot carry quarantined "
                    "orphan-FK details, so the run cannot prove the rows "
                    "were held out — refusing rather than dropping them"
                ) from exc
    if cfg.shape_runner is not None and batch.rows is not None:
        headers = list(batch.headers or [])
        if cfg.pin_ledger is not None:
            cfg.pin_ledger.add_page(headers, batch.rows)
        _shape_inputs_folded = {
            str(s).casefold() for s in (cfg.shape_inputs or [])
        }
        unknown = [
            h
            for h in headers
            if h and str(h).casefold() not in _shape_inputs_folded
        ]
        if unknown:
            # A column the recipe was never validated against cannot be
            # shaped, and writing it raw beside shaped columns would put two
            # different rules in one row. Fail closed, naming it.
            raise ValueError(
                f"source column(s) {', '.join(sorted(unknown))} appeared on the read "
                f"but the approved transform recipe ({cfg.shape_runner.recipe_hash}) was "
                "validated against a different column set — re-validate the recipe "
                "against the source as it is now"
            )
        batch.headers, batch.rows = cfg.shape_runner.matrix(headers, batch.rows)
        batch.raw_page_diverted = _persist_shape_diverts(run)
        # Rows still held by a buffering step are not removals — the
        # ledger subtracts them via this marker (DD-3).
        batch.raw_page_held = int(cfg.shape_runner.effect.rows_buffered or 0)
    return batch


def _persist_shape_diverts(run) -> int:
    """Persist this page's shape-diverted rows to the quarantine DLQ.

    A row a recipe step diverted (on_error=divert, divert_rows, lookup
    unmatched=divert) is excluded from the destination — quarantine is what
    makes it recoverable. Persisting per page keeps the pending list
    bounded and keeps quarantine durable across a mid-stream abort.
    """
    cfg = run.cfg
    tally = run.tally
    if cfg.shape_runner is None:
        return 0
    diverted = cfg.shape_runner.take_diverted()
    tally.shape_diverted_total[0] += len(diverted or [])
    # Keyed routed runs purge diverted keys from every route table —
    # a row that stopped matching any route must lose its stale copy
    # (P1-B). Records carry the shaped row; try the contract pk names
    # then the identity source names.
    for _d in diverted or []:
        _rec = getattr(_d, "record", None) or {}
        _key = None
        for _names in (cfg.pk_source_cols, cfg.pk_audit_cols):
            if _names and all(n in _rec for n in _names):
                _key = tuple(_rec[n] for n in _names)
                break
        if _key is not None and all(v is not None for v in _key):
            tally.shape_divert_key_tuples.append(_key)
    if not diverted or not cfg.job_id:
        return len(diverted or [])
    from services.quarantine_dlq import persist_rejected_rows

    details = [
        {
            "row": d.record,
            "reason": (
                f"transform divert — step {d.step} '{d.op}': {d.reason}"
            ),
            "failure_reason": d.reason,
            "transform_attempted": f"shape step {d.step} '{d.op}'",
            "shape_step": d.step,
            "shape_op": d.op,
            "source_row_index": d.row_index + 1,
            **(
                {
                    "dedupe_key": list(d.dedupe_key),
                    "winner_row": d.winner_row_index + 1,
                }
                if getattr(d, "dedupe_key", None) is not None
                and getattr(d, "winner_row_index", None) is not None
                else {}
            ),
        }
        for d in diverted
    ]
    persist_rejected_rows(
        job_id=str(cfg.job_id),
        rejected_details=details,
        source="shape_divert",
        connector=str(
            getattr(cfg.destination, "format", None)
            or getattr(cfg.destination, "kind", None)
            or ""
        ),
    )
    return len(diverted)


def _page_cursor_max(run, batch) -> str:
    """The page's highest cursor value, as the source handed it over.

    A row the recipe removed was still read, so the watermark must move past
    it — otherwise the next incremental run reads it again to remove it again,
    for ever.
    """
    cfg = run.cfg
    from services.sync_cursor import max_cursor_value
    return _raw_page_cursor(batch) or max_cursor_value(
        batch.rows, batch.headers, cfg.cursor_source_col, cfg.cursor_pk_source or None
    )


def _advance_fetch_cursor(run, page: Any) -> None:
    """Move the read edge to the page's high-water mark.

    The seed page is taken whole: an inclusive tie-group read can end
    *below* the stored ``(cursor, last key)``, and keeping the stored mark
    then would skip the tie rows between the page edge and it.
    """
    cfg = run.cfg
    cur = run.cur
    from services.sync_cursor import compare_cursor_values
    if not (cfg.incremental and cfg.cursor_source_col and _raw_page_rows(page)):
        return
    batch_max = _page_cursor_max(run, page)
    if batch_max and (
        cur.fetch_from_seed
        or cur.fetch_cursor is None
        or compare_cursor_values(batch_max, cur.fetch_cursor) > 0
    ):
        cur.fetch_cursor = batch_max
        cur.fetch_from_seed = False


def _prepare_batch(run, idx: int, raw_batch: Any) -> Any:
    cur = run.cur
    prepared = _filter_batch(run, raw_batch)
    _advance_fetch_cursor(run, prepared)
    cur.fetch_offset += _raw_page_rows(prepared)
    return prepared
