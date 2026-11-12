"""Commit-safe window of a streaming incremental run (audit C1).

The decision lives in :mod:`services.commit_safe_window`. This module binds it
to one run: it plans the window once (before anything is read), attaches it to
the read scope, de-duplicates the rows the window re-reads against the
destination's pre-run state, and persists the watermark so that it never moves
backwards and carries the commit-order bound only once the rows are at rest.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

from services.commit_safe_window import (
    EVIDENCE_KEY,
    HORIZON_KEY,
    STRATEGY_PG,
    CommitSafeWindow,
    capture_pg_bound,
    commit_safety_strategy,
    plan_commit_safe_window,
    probe_pg_late_rows,
)
from services.keyset_pagination import header_column_index

logger = logging.getLogger(__name__)

DISABLE_ENV = "DATAFLOW_COMMIT_SAFE_WINDOW"
_LOOKUP_CHUNK = 500


def _enabled() -> bool:
    from services.brand_env import getenv_brand_bool

    return getenv_brand_bool(DISABLE_ENV, True)


def _cursor_type(cfg: Any, scope: Any, catalog_types: dict[str, Any] | None) -> str:
    col = cfg.cursor_source_col
    return str(
        (cfg.schema or {}).get(col)
        or (catalog_types or {}).get(col)
        or getattr(scope, "watermark_cursor_type", "")
        or ""
    )


def route_replay_key(cfg: Any, catalog: tuple | None) -> tuple[tuple[str, ...], bool]:
    """Source columns a re-read row is recognised by, and whether they are a whole key.

    An upsert collapses on its key. An append needs the row's own identity:
    the declared key, the catalog primary key or non-null unique key the
    cursor's tie-break comes from, the engine's intrinsic row id, or the cursor
    itself when the catalog proves it unique. Always the *whole* key: two rows
    of a composite key can share the cursor value and the first key column
    (audit review C1), and only the full tuple tells them apart.

    ``complete`` is False when the columns are not proven to be a unique key
    (a tie-break with no key evidence). Such a key may still look rows up, but
    it cannot prove a row delivered: every row the source reports behind the
    watermark then counts as undelivered, so the route re-reads, never skips.
    """
    from services.keyset_pagination import (
        cursor_unique_evidence,
        intrinsic_tiebreak_column,
        safe_keyset_unique_columns,
    )
    from services.sync_cursor import requires_upsert

    cursor = str(cfg.cursor_source_col or "")
    declared = tuple(str(c) for c in (cfg.pk_source_cols or []) if c)
    if requires_upsert(cfg.effective_sync):
        return (declared, True) if declared and cfg.pk_target_cols else ((), False)
    if declared:
        return declared, True
    _types, nullable, keys = catalog or ({}, {}, {})
    keys = keys or {}
    catalog_pk = tuple(str(c) for c in (keys.get("primary_key_columns") or []) if c)
    tie = str(getattr(cfg, "cursor_pk_source", "") or "")
    if tie:
        if tie in catalog_pk:
            return catalog_pk, True
        for uk in keys.get("unique_keys") or []:
            cols = list((uk.get("columns") if isinstance(uk, dict) else uk) or [])
            if tie in cols and safe_keyset_unique_columns([uk], cols, nullable or {}) == cols:
                return tuple(str(c) for c in cols), True
        if intrinsic_tiebreak_column(str(getattr(cfg, "src_read_type", "") or ""), cursor) == tie:
            return (tie,), True
        return (tie,), False
    if cursor_unique_evidence(
        cursor,
        primary_key_columns=list(catalog_pk),
        unique_keys=list(keys.get("unique_keys") or []),
        nullable=nullable or {},
    ):
        return (cursor,), True
    return (), False


def route_replay_identity(cfg: Any, catalog: tuple | None) -> str:
    """The key column re-read rows are looked up by at the destination, or ``""``.

    One column of :func:`route_replay_key` — the cursor's tie-break when it is
    part of the key, else the first key column that is not the cursor, so a
    coarse cursor does not fetch every row of its value. Matching always uses
    the whole key.
    """
    key, _complete = route_replay_key(cfg, catalog)
    if not key:
        return ""
    tie = str(getattr(cfg, "cursor_pk_source", "") or "")
    if tie in key:
        return tie
    return next((c for c in key if c != cfg.cursor_source_col), key[0])


def bind_commit_window(run: Any, scope: Any, catalog: tuple | None, contract: Any, table: str) -> Any:
    """Plan this run's window once and return the scope that reads through it."""
    cfg = run.cfg
    cfg.route_watermark = scope.watermark
    if table and not getattr(cfg, "text_cursor_judged", False):
        cfg.text_cursor_judged = True
        refuse_unorderable_text_cursor(cfg, scope, catalog, table)
    if getattr(cfg, "census_plan", None) is None:
        from services.incremental_census import census_schedule

        every = int(getattr(contract, "key_census_every", 0) or 0)
        cfg.census_plan = (every, *census_schedule(every, scope.runs_since_key_census))
    window = getattr(cfg, "commit_window", None)
    if window is None and _enabled() and table and scope.cursor_key:
        try:
            window = _plan(cfg, scope, catalog, contract, table)
        except Exception as exc:  # noqa: BLE001 — never weaker than the read it replaces
            logger.warning("commit-safe window unavailable (%s): reading from the watermark", exc)
            window = None
        cfg.commit_window = window
    cfg.replay_through = window.replay_through if window is not None else ""
    if window is None:
        return scope
    return dataclasses.replace(scope, commit_safe=window)


def refuse_unorderable_text_cursor(cfg: Any, scope: Any, catalog: tuple | None, table: str) -> None:
    """Refuse a text cursor the source orders differently from the sync (audit C3/H4b).

    The source orders a text column as text. Numbers of more than one width and
    timestamps in more than one offset sort differently as text, so rows land
    on the wrong side of the watermark and are skipped while the run reports
    success. Judged on the live source before anything is read or written; a
    source that cannot be probed is read as before.
    """
    from connectors.cursor_bind import declared_type_is_zone_bearing
    from services.cursor_order import is_text_declared, probe_text_cursor

    # The sync's type for the column is inferred; the source's ordering is not.
    # SQLite compares stored text as text whatever the column is declared, so
    # it is always asked; elsewhere only a text-typed cursor is.
    typed = _cursor_type(cfg, scope, (catalog or ({},))[0])
    if not (is_text_declared(typed) or (cfg.src_read_type or "").lower() == "sqlite"):
        return
    try:
        from services.null_cursor_probe import _resolve_source_cfg
        from services.source_duplicate_probe import is_sqlish_source

        src_cfg, db_type = _resolve_source_cfg("", dict(cfg.src_cfg or {}), None)
        if not src_cfg or not is_sqlish_source(db_type):
            return
        reason = probe_text_cursor(
            src_cfg, table, cfg.cursor_source_col,
            utc_watermark=declared_type_is_zone_bearing(typed),
        )
    except Exception as exc:  # noqa: BLE001 — unmeasured, not refused
        logger.warning("text cursor ordering probe skipped: %s", exc)
        return
    if reason:
        raise ValueError(reason)


def _plan(cfg: Any, scope: Any, catalog: tuple | None, contract: Any, table: str) -> CommitSafeWindow | None:
    cursor_type = _cursor_type(cfg, scope, (catalog or ({},))[0])
    identity = route_replay_identity(cfg, catalog)
    cfg.replay_identity = identity
    cfg.replay_key, cfg.replay_key_complete = route_replay_key(cfg, catalog)
    src_cfg = cfg.src_cfg
    schema = str(src_cfg.get("schema") or "public")
    is_pg = commit_safety_strategy(cfg.src_read_type) == STRATEGY_PG
    return plan_commit_safe_window(
        source_type=cfg.src_read_type,
        sync_mode=cfg.effective_sync,
        cursor_column=cfg.cursor_source_col,
        cursor_type=cursor_type,
        stored_watermark=scope.watermark,
        metadata={HORIZON_KEY: dict(scope.commit_horizon or {})},
        can_replay=bool(identity),
        contract=contract.overlap_declaration() if contract is not None else {},
        capture_bound=(lambda: capture_pg_bound(src_cfg)) if is_pg else None,
        probe_late=(lambda **kw: _probe_undelivered(cfg, schema, table, **kw))
        if is_pg
        else None,
    )


def _probe_undelivered(cfg: Any, schema: str, table: str, **kw: Any) -> Any:
    """Rows committed behind the watermark that the destination does not hold.

    The source reports every row behind the watermark written since the stored
    bound — late commits, and rows merely edited in place. The second kind is
    already at the destination under the same cursor value and the same whole
    key, and re-reading from it would re-read everything after it on every run
    that follows an edit. So the reported rows are looked up at the destination
    by key and only the ones it lacks set the re-read seed. Unchecked (too
    many, the destination unreadable, or no whole unique key to prove a row
    delivered) they all count, which is the safe side.
    """
    key = tuple(getattr(cfg, "replay_key", ()) or ())
    complete = bool(getattr(cfg, "replay_key_complete", False))
    late = probe_pg_late_rows(
        cfg.src_cfg, schema=schema, table=table, cursor_column=cfg.cursor_source_col,
        identity_columns=key if complete else (), **kw,
    )
    if not late.count or len(late.candidates) != late.count:
        return late
    try:
        missing = _not_at_destination(cfg, table, key, list(late.candidates))
    except Exception as exc:  # noqa: BLE001 — unchecked rows all count
        logger.warning("commit-safe window: destination lookup skipped: %s", exc)
        return late
    return dataclasses.replace(
        late, undelivered=len(missing), min_cursor=missing[0][0] if missing else None
    )


def _not_at_destination(
    cfg: Any, table: str, key: tuple[str, ...], candidates: list[tuple[str, tuple[str, ...]]]
) -> list[tuple[str, tuple[str, ...]]]:
    """``candidates`` (cursor, whole key) with no such row version at the destination."""
    from services.sync_cursor import compare_cursor_values
    from services.target_sample import read_target_sample
    from services.value_serializer import present_cell_text

    from .adapters import resolve_dest_table

    cursor_dest = _map_target(cfg.cursor_source_col, cfg.mappings)
    key_dest = [_map_target(k, cfg.mappings) for k in key]
    lookup = str(getattr(cfg, "replay_identity", "") or "") or key[0]
    at = key.index(lookup) if lookup in key else 0
    dest_table = resolve_dest_table(cfg.dest_type, cfg.destination, table)
    held: dict[tuple[str, ...], list[str]] = {}
    lookups = list(dict.fromkeys(k[at] for _c, k in candidates))
    for start in range(0, len(lookups), _LOOKUP_CHUNK):
        chunk = lookups[start : start + _LOOKUP_CHUNK]
        for row in read_target_sample(
            getattr(cfg, "dest_bind_dialect", None) or cfg.dest_type,
            cfg.dest_cfg,
            schema=str(cfg.dest_cfg.get("schema") or "public"),
            table_name=dest_table,
            columns=list(dict.fromkeys([*key_dest, cursor_dest])),
            limit=max(10_000, 20 * len(chunk)),
            sort_key=key_dest[at],
            key_values=chunk,
        ) or []:
            low = {str(k).lower(): v for k, v in row.items()}
            ident = tuple(present_cell_text(low.get(c.lower())) for c in key_dest)
            mark = present_cell_text(low.get(cursor_dest.lower()))
            if mark is not None and all(p is not None for p in ident):
                held.setdefault(ident, []).append(mark)
    return [
        (cursor, ident)
        for cursor, ident in candidates
        if not any(compare_cursor_values(cursor, mark) == 0 for mark in held.get(tuple(ident), []))
    ]


def copy_declined_for_replay(cfg: Any) -> bool:
    """A run that re-reads behind the watermark needs a path that recognises re-reads.

    The row path does (:func:`replay_rows_already_landed`), and so does the
    identity incremental COPY of the SQL core, which prunes its staging table
    (``copy_incremental.prune_replayed_staging``). Any other engine-side copy
    is declined for such a run: it would deliver the re-read rows again.
    """
    from services.copy_incremental import identity_incremental_route

    return bool(getattr(cfg, "replay_through", "")) and not identity_incremental_route(
        cfg.src_read_type, cfg.dest_type
    )


# --------------------------------------------------------------------------
# Watermark persistence
# --------------------------------------------------------------------------


def _metadata(cfg: Any, horizon: dict[str, Any] | None) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "job_id": cfg.job_id,
        "sync_mode": cfg.effective_sync,
        # A watermark is a value of one column; record which one so a later
        # run on a different cursor cannot inherit it.
        "cursor_column": cfg.cursor_source_col,
        # The column a composite watermark is decodable on (QA ACC-02).
        "tiebreak_column": cfg.cursor_pk_source,
        "cursor_type": str((cfg.schema or {}).get(cfg.cursor_source_col) or ""),
    }
    if horizon:
        meta[HORIZON_KEY] = dict(horizon)
    every, _due, counter = getattr(cfg, "census_plan", None) or (0, False, 0)
    if every:
        meta["runs_since_key_census"] = counter
    return meta


def _census_due(run: Any) -> bool:
    """Ask Gate-8 for the key census when this run of the route is due one."""
    every, due, _counter = getattr(run.cfg, "census_plan", None) or (0, False, 0)
    if due:
        from services.incremental_census import request_census

        request_census(run.tally.dest_summary, cursor_column=run.cfg.cursor_source_col, every=every)
    return bool(every)


def _never_behind(cfg: Any, candidate: str | None) -> str | None:
    """``candidate``, or the route's stored watermark when that is further on.

    A run that re-reads behind the watermark pages through values below it; the
    stored mark must not follow them down, or a crash mid-run would make the
    next run treat rows already delivered as new.
    """
    from services.sync_cursor import advance_stored_cursor

    stored = getattr(cfg, "route_watermark", None)
    # Only a run that re-reads pages below its stored mark. Every other run
    # stores what it reached, as before — a connector's opaque state is not a
    # value this comparator may rank.
    if stored in (None, "") or not getattr(cfg, "replay_through", ""):
        return candidate
    moved, advanced = advance_stored_cursor(str(stored), candidate)
    return moved if advanced else str(stored)


def _restore_point(cfg: Any, cursor_key: str, before: Any) -> dict[str, Any]:
    point: dict[str, Any] = {"cursor_key": cursor_key, "watermark": before}
    window = getattr(cfg, "commit_window", None)
    if window is not None and window.bound:
        # Batch undo removes this run's rows: the bound they were read under
        # goes with them, or the next run would not look for them again.
        point[HORIZON_KEY] = dict(window.previous_bound or {})
    return point


def persist_batch_watermark(run: Any, cursor_key: str, running_cursor: Any, before: Any) -> None:
    """ACC-05 — advance the route watermark with a committed batch.

    The bound stays the previous run's until the whole read is at rest: a run
    that dies half-way must be asked again from where the last complete read
    stood.
    """
    from services.sync_cursor import set_watermark

    cfg = run.cfg
    window = getattr(cfg, "commit_window", None)
    # A first read has no earlier bound: its own (captured before it read
    # anything) already holds for every row at or below a committed batch.
    horizon = (window.previous_bound or window.bound) if window is not None else None
    set_watermark(
        cursor_key, _never_behind(cfg, running_cursor), metadata=_metadata(cfg, horizon)
    )
    # D110 — batch undo restores the watermark to this pre-run value when it
    # removes the rows this advance stood for.
    run.tally.dest_summary.setdefault("watermark_restore_point", _restore_point(cfg, cursor_key, before))


def persist_final_watermark(run: Any, cursor_key: str, running_cursor: Any, before: Any) -> None:
    """Store the run's high mark and, with it, the bound the run read under."""
    from services.sync_cursor import set_watermark

    cfg = run.cfg
    window = getattr(cfg, "commit_window", None)
    stamp_window_evidence(run)
    if not (cfg.incremental and cursor_key):
        return
    stored = getattr(cfg, "route_watermark", None)
    final = _never_behind(cfg, running_cursor) if running_cursor else stored
    if final in (None, ""):
        return
    horizon = window.metadata().get(HORIZON_KEY) if window is not None else None
    advanced = bool(running_cursor) and running_cursor != cfg.watermark and final != stored
    counted = _census_due(run)
    if not advanced and not counted and not (
        horizon and horizon != dict(window.previous_bound or {})
    ):
        return
    set_watermark(cursor_key, str(final), metadata=_metadata(cfg, horizon))
    run.tally.dest_summary.setdefault("watermark_restore_point", _restore_point(cfg, cursor_key, before))


def persist_copy_watermark(run: Any, cursor_key: str, high: str | None, before: Any) -> str | None:
    """The COPY fast path's watermark write; returns the value now stored."""
    from services.sync_cursor import set_watermark

    cfg = run.cfg
    window = getattr(cfg, "commit_window", None)
    stamp_window_evidence(run)
    stored = getattr(cfg, "route_watermark", None)
    final = _never_behind(cfg, high) if high else stored
    if final in (None, "") or not cursor_key:
        return None
    horizon = window.metadata().get(HORIZON_KEY) if window is not None else None
    counted = _census_due(run)
    if not high and not counted and not (
        horizon and horizon != dict(window.previous_bound or {})
    ):
        return str(final)
    set_watermark(cursor_key, str(final), metadata=_metadata(cfg, horizon))
    if high:
        run.tally.dest_summary.setdefault(
            "watermark_restore_point", _restore_point(cfg, cursor_key, before)
        )
    return str(final)


def stamp_window_evidence(run: Any) -> None:
    """Publish the window on the run summary — the job's cursor evidence."""
    window = getattr(run.cfg, "commit_window", None)
    summary = getattr(run.tally, "dest_summary", None)
    if window is None or not isinstance(summary, dict):
        return
    state = getattr(run.cur, "_boundary_fps_state", None) or {}
    copied = summary.get("source_snapshot") if isinstance(summary.get("source_snapshot"), dict) else {}
    replayed = landed = None
    if window.replays:
        # Row path: counted by the page filter. COPY: counted on staging.
        replayed = state.get("replayed", copied.get("replay_rows_read", 0))
        landed = state.get("replay_landed", copied.get("replay_rows_already_landed", 0))
    summary[EVIDENCE_KEY] = window.evidence(rows_replayed=replayed, rows_already_landed=landed)


# --------------------------------------------------------------------------
# Re-read de-duplication
# --------------------------------------------------------------------------


def _map_target(source_col: str, mappings: list[dict[str, Any]] | None) -> str:
    for m in mappings or []:
        if isinstance(m, dict) and m.get("source") == source_col:
            return str(m.get("target") or source_col)
    return source_col


def replay_rows_already_landed(
    run: Any, headers: list[str], rows: list[list[str]], *, verify: bool = False
) -> set[int]:
    """Indices of re-read rows the destination held before this run.

    Rows at or below the stored watermark are re-reads. Each is looked up at
    the destination by its identity column — once, before this run writes it,
    so the verify pass judges against the same pre-run state — and dropped when
    the destination already holds it.
    Fail-open like the tie-group dedupe: a duplicate is caught by Gate-8's
    window check, a silently dropped row is not.
    """
    from connectors.writer_common import row_fingerprints
    from services.sync_cursor import compare_cursor_values, requires_upsert
    from services.target_sample import read_target_sample
    from services.value_serializer import present_cell_text

    from .stream_boundary import _boundary_append_dedupe

    cfg, cur = run.cfg, run.cur
    through = str(getattr(cfg, "replay_through", "") or "")
    identity = str(getattr(cfg, "replay_identity", "") or "")
    state = cur._boundary_fps_state
    cidx = header_column_index(headers or [], cfg.cursor_source_col)
    kidx = header_column_index(headers or [], identity) if identity else None
    if not (through and rows) or cidx is None or kidx is None:
        return set()
    try:
        replay_idx = []
        for i, row in enumerate(rows):
            text = present_cell_text(row[cidx]) if cidx < len(row) else None
            if text is not None and compare_cursor_values(text, through) <= 0:
                replay_idx.append(i)
        if not replay_idx:
            return set()
        all_cols = list(cfg.target_cols or [])
        cursor_dest = _map_target(cfg.cursor_source_col, cfg.mappings)
        key_dest = _map_target(identity, cfg.mappings)
        dialect = cfg.dest_bind_dialect or cfg.dest_type
        schema = str(cfg.dest_cfg.get("schema") or "public")
        if state.get("replay_cols") is None:
            probe = read_target_sample(
                dialect, cfg.dest_cfg, schema=schema, table_name=cfg.dest_table,
                columns=None, limit=1,
            )
            live = {str(k).lower() for k in (probe[0] if probe else {})}
            read_cols = [c for c in all_cols if c.lower() in live] or all_cols
            # Below the stored mark a re-read row is recognised as a row
            # *version*: its cursor value and its identity. Content is not part
            # of it — a row edited in place without moving its cursor is
            # out-of-window drift (D188), exactly as before the window re-read
            # it. The tie group *at* the mark keeps the rule it always had
            # (D124): (cursor, key) for a keyed append, every column otherwise,
            # so an upsert still rewrites a peer whose content changed.
            whole_key = [_map_target(k, cfg.mappings) for k in getattr(cfg, "replay_key", ()) or ()]
            version = (
                list(
                    dict.fromkeys(
                        c
                        for c in [cursor_dest, key_dest, *whole_key, *(cfg.pk_target_cols or [])]
                        if c in read_cols
                    )
                )
                # Without a proven unique key a row is its whole content: two
                # rows that share (cursor, part of a key) are never one row.
                if getattr(cfg, "replay_key_complete", True)
                else read_cols
            )
            keyed_append = bool(
                not requires_upsert(cfg.effective_sync)
                and cfg.pk_target_cols
                and all(c in read_cols for c in cfg.pk_target_cols)
            )
            state.update(
                replay_cols=read_cols,
                replay_match=version,
                replay_tie_match=version if keyed_append else read_cols,
                replay_seen=set(),
                replay_tie_fps=set(),
                # Columns this run adds: an upsert keeps a tie peer that carries
                # a value there (it rewrites the key in place) — D335.
                new_cols=[c for c in all_cols if c.lower() not in live]
                if probe and requires_upsert(cfg.effective_sync)
                else [],
            )
            state["fps"] = state.get("fps") or set()
        read_cols, match_cols = state["replay_cols"], state["replay_match"]
        tie_cols = state["replay_tie_match"]
        seen: set = state["replay_seen"]
        wanted = []
        for i in replay_idx:
            key = present_cell_text(rows[i][kidx]) if kidx < len(rows[i]) else None
            if key is not None and key not in seen:
                seen.add(key)
                wanted.append(key)
        for start in range(0, len(wanted), _LOOKUP_CHUNK):
            chunk = wanted[start : start + _LOOKUP_CHUNK]
            dest_rows = read_target_sample(
                dialect, cfg.dest_cfg, schema=schema, table_name=cfg.dest_table,
                columns=read_cols, limit=max(10_000, 20 * len(chunk)),
                sort_key=key_dest, key_values=chunk,
            )
            lowered = [{str(k).lower(): v for k, v in dr.items()} for dr in dest_rows or []]
            for cols, bucket in ((match_cols, state["fps"]), (tie_cols, state["replay_tie_fps"])):
                tups = [tuple(low.get(str(c).lower()) for c in cols) for low in lowered]
                bucket.update(
                    fp
                    for _k, fp in row_fingerprints(
                        tups, cols,
                        dest_db_type=cfg.dest_bind_dialect, dest_types=cfg.digest_dest_types,
                    )
                )
        tie_idx = [
            i for i in replay_idx
            if compare_cursor_values(present_cell_text(rows[i][cidx]), through) == 0
        ]
        tie_set = set(tie_idx)
        below_idx = [i for i in replay_idx if i not in tie_set]
        landed: set[int] = set()
        for group, cols, fps, added in (
            (below_idx, match_cols, state["fps"], []),
            (tie_idx, tie_cols, state["replay_tie_fps"], state.get("new_cols") or []),
        ):
            if not (group and fps):
                continue
            drop = _boundary_append_dedupe(
                list(headers or []),
                [rows[i] for i in group],
                fps,
                match_cols=cols,
                new_cols=added,
                mappings=cfg.mappings,
                target_cols=all_cols,
                column_types=cfg.column_types,
                stream_error_policy=cfg.stream_error_policy,
                digest_dest_types=cfg.digest_dest_types,
                dest_bind_dialect=cfg.dest_bind_dialect,
                remap_dest_types=cfg.fingerprint_dest_types,
                destination_pk_columns=list(cfg.pk_target_cols or []),
            )
            landed.update(group[i] for i in drop)
        if not verify:
            # The window's own re-read: rows below the mark (the tie group at
            # it was always re-read).
            state["replayed"] = int(state.get("replayed") or 0) + len(below_idx)
            state["replay_landed"] = int(state.get("replay_landed") or 0) + sum(
                1 for i in below_idx if i in landed
            )
        peers = [rows[i] for i in tie_idx if i in landed]
        if peers and cfg.pk_target_cols and not requires_upsert(cfg.effective_sync):
            # Tie peers at the watermark an earlier run delivered: inside this
            # run's inclusive window but not its write (D335) — named so the
            # ladder grades neither side of them.
            from .stream_db_page import _record_landed_boundary_keys

            _record_landed_boundary_keys(run, headers, peers)
        return landed
    except Exception as exc:  # noqa: BLE001 — fail open: dedupe must never lose a row
        logger.warning("Re-read window dedupe skipped (fail-open): %s", exc)
        return set()
