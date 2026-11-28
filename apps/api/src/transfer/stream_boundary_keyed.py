"""Append boundary dedupe and its post-write check (D335).

The inclusive run seed (``cursor >= watermark``) re-reads the tie group at the
stored watermark - the watermark row IS read again - and an append must drop
the peers the destination already holds and append the rest. The old lookup
asked ``cursor IN (:watermark_text)``: Oracle's NLS format rejected the ISO
``T`` (ORA-01861), SQL Server's DATETIME rounded to 1/300 s, a TEXT cursor
matched one spelling; a miss or an error failed *open* and the watermark row
was appended again under a green "Append delta verified".

The destination is now read at its own precision (``stream_boundary_read``):

* keyed: a peer is landed when a stored row has its key at the stored instant;
* keyless: when a stored row at that instant equals it on every column the
  table holds (one stored copy absorbs one re-read peer);
* a transient read failure stops the run at the tie group
  (:class:`BoundaryHeldBack`): the tie group is read first, so nothing past the
  watermark is written and the stored watermark stays where it is. Strict:
  the job fails; balanced: it completes with warnings, 0 rows, the hold-back
  reason. The next run retries the same tie group;
* a structural one (no such column, key not on the page, a cursor that cannot
  be compared) fails with the remedy (:data:`STRUCTURAL_REMEDY`) unless the
  route carries the audited acknowledgement (``services.boundary_ack_store``),
  in which case the peers are appended and the acceptance is recorded.

After the write, Gate-8 re-reads the boundary the same way and fails when a
watermark row is at the destination more often than the source holds it.
"""

from __future__ import annotations

import json
from typing import Any

from connectors.writer_common import row_fingerprints

from .stream_boundary_read import (
    STRUCTURAL_REMEDY,
    BoundaryStructural,
    BoundaryUnmeasured,
    carrier_values,
    instant,
    live_columns,
    read_candidates,
    same_cursor,
)

__all__ = [
    "BOUNDARY_TIE_KEY", "BoundaryHeldBack", "BoundaryRefused", "BoundaryUnmeasured",
    "append_boundary_rows_landed", "apply_boundary_duplicate_gate", "boundary_duplicates",
    "carrier_values", "held_back_message", "instant", "read_boundary_candidates", "same_cursor",
]

#: dest_summary key: the tie group this run re-read at the watermark.
BOUNDARY_TIE_KEY = "incremental_boundary_tie"


class BoundaryHeldBack(ValueError):
    """The run stops before writing a tie group the destination did not answer for.

    ``completes_with_warnings`` is set for a non-strict run (job status
    ``completed_with_warnings``, 0 rows); a strict run fails. Either way the
    stored watermark does not move, so the next run retries the tie group.
    """

    completes_with_warnings = False


class BoundaryRefused(ValueError):
    """The destination can never answer as configured: the remedy is named."""


def read_boundary_candidates(**kwargs: Any) -> list[dict[str, Any]]:
    """Every destination row stored at the watermark (see ``stream_boundary_read``)."""
    return list(read_candidates(**kwargs))


def held_back_message(n: int, watermark: Any, reason: str) -> str:
    return (
        f"{n} boundary row(s) held back; the next run will retry them. The destination "
        f"could not be read to tell whether the rows at the stored watermark {watermark} "
        f"were already there ({reason}), so this run stopped before writing them, wrote "
        "nothing past the watermark, and keeps its cursor at that watermark."
    )


def _refused_message(reason: str) -> str:
    return (
        "The watermark's rows cannot be checked against this destination for "
        f"duplicates ({reason}); every run would stop the same way. " + STRUCTURAL_REMEDY
    )


def _accepted(destination: Any, table: str, schema: str = "") -> dict[str, str] | None:
    """The route's recorded acknowledgement (``services.boundary_ack_store``).

    Only one recorded through the audited API counts; ``accept_boundary_duplicates``
    typed into the destination's options is ignored.
    """
    from services.boundary_ack_store import lookup

    schema = str(getattr(destination, "schema", "") or schema or "")
    ack = lookup(str(getattr(destination, "connector_id", "") or ""), schema, str(table or ""))
    if not ack or not str(ack.get("actor") or "").strip():
        return None
    return {"actor": str(ack["actor"]), "reason": str(ack.get("reason") or ""), "at": str(ack.get("at") or "")}


#: Consecutive held-back runs after which the route stops retrying (review r4).
HOLDBACK_LIMIT = 3


def _holdbacks(run: Any, *, failed: bool) -> int:
    """Count consecutive hold-backs on the route's stored watermark record.

    ``failed`` adds one; a measured boundary resets it. The watermark value
    itself is written back unchanged.
    """
    key = str(getattr(run.cfg, "boundary_route_key", "") or "")
    if not key:
        return 1 if failed else 0
    from services import sync_cursor as sc

    watermark, meta = sc.get_watermark_record(key)
    if watermark is None:
        return 1 if failed else 0
    before = int(meta.get("boundary_holdbacks") or 0)
    after = before + 1 if failed else 0
    if after != before:
        sc.set_watermark(key, watermark, metadata={**meta, "boundary_holdbacks": after})
    return after


def _fp(values: tuple, cols: list[str], dialect: str, types: dict) -> str:
    if not cols:
        return ""
    return next(
        fp for _k, fp in row_fingerprints([values], list(cols), dest_db_type=dialect, dest_types=types)
    )


def _key_token(key: dict[str, Any]) -> str:
    return json.dumps({str(k): (None if v is None else str(v)) for k, v in key.items()}, sort_keys=True)


def source_key_texts(cfg: Any, headers: list[str], rows: list[list[Any]]) -> list[dict[str, Any]] | None:
    """``{target key column: source text}`` per row, or None when a key is not on the page."""
    from services.keyset_pagination import header_column_index
    from services.value_serializer import SQL_NULL_SENTINEL, present_cell_text

    to_source = {
        str(m.get("target") or ""): str(m.get("source") or "")
        for m in cfg.mappings or []
        if isinstance(m, dict) and m.get("target") and m.get("source")
    }
    pos = {}
    for col in cfg.pk_target_cols or []:
        idx = header_column_index(headers or [], to_source.get(col, col))
        if idx is None:
            return None
        pos[col] = idx
    out = []
    for row in rows:
        key = {}
        for c, i in pos.items():
            text = present_cell_text(row[i]) if i < len(row) else None
            key[c] = None if text in (None, SQL_NULL_SENTINEL) else text
        out.append(key)
    return out


def _summary(run: Any) -> dict[str, Any] | None:
    summary = getattr(run.tally, "dest_summary", None)
    return summary if isinstance(summary, dict) else None


def append_boundary_rows_landed(
    run: Any, headers: list[str], rows: list[list[Any]], *, cursor_dest_col: str,
    watermark: str, keyed: bool, verify: bool = False,
) -> set[int]:
    """Indices of the tie peers ``rows`` the destination already holds.

    Raises :class:`BoundaryHeldBack` (transient) or :class:`BoundaryRefused`
    (structural, unacknowledged) instead of guessing.
    """
    state = run.cur._boundary_fps_state
    if state.get("fps") is None:
        state["fps"] = set()  # the re-read digest asks the same question of its page
    try:
        landed = _landed(run, headers, rows, cursor_dest_col=cursor_dest_col,
                         watermark=watermark, keyed=keyed, record=not verify)
        if not verify:
            _holdbacks(run, failed=False)
        return landed
    except BoundaryStructural as exc:
        ack = _accepted(
            getattr(run.cfg, "destination", None), run.cfg.dest_table, str(run.cfg.dest_cfg.get("schema") or "")
        )
        summary = _summary(run)
        if ack is None:
            from .stream_boundary_recipe import refusal_text

            raise BoundaryRefused(refusal_text(exc)) from exc
        if summary is not None:
            summary.setdefault(BOUNDARY_TIE_KEY, {})["accepted"] = {**ack, "why": str(exc)}
        import logging

        logging.getLogger(__name__).warning(
            "Boundary dedupe not possible (%s); duplicates accepted by %s: %s",
            exc, ack["actor"], ack["reason"],
        )
        return set()
    except Exception as exc:  # noqa: BLE001 - transient: hold the tie group back
        reason = str(exc) if isinstance(exc, BoundaryUnmeasured) else (
            f"the destination read failed ({type(exc).__name__})"
        )
        summary = _summary(run)
        if verify and ((summary or {}).get(BOUNDARY_TIE_KEY) or {}).get("accepted"):
            return set()  # the write pass proceeded under the acknowledgement
        count = _holdbacks(run, failed=True) if not verify else 0
        if summary is not None:
            summary.setdefault(BOUNDARY_TIE_KEY, {})["held_back"] = {
                "rows": len(rows), "reason": reason, "watermark": str(watermark), "consecutive": count,
            }
        if count >= HOLDBACK_LIMIT:
            # A deterministic failure (an unindexed cursor timing out) would
            # hold back forever: from the third run on it is structural - and
            # the acknowledgement the refusal offers then lets the run proceed.
            ack = _accepted(
                getattr(run.cfg, "destination", None), run.cfg.dest_table,
                str(run.cfg.dest_cfg.get("schema") or ""),
            )
            if ack is not None:
                if summary is not None:
                    summary.setdefault(BOUNDARY_TIE_KEY, {})["accepted"] = {
                        **ack, "why": f"the watermark check did not answer on {count} consecutive runs ({reason})",
                    }
                return set()
            raise BoundaryRefused(
                f"The destination did not answer the watermark check on {count} consecutive "
                f"runs ({reason}). Add an index on {cursor_dest_col} (or declare a key) so "
                "the check finishes, or accept boundary duplicates for this route."
            ) from exc
        raise BoundaryHeldBack(held_back_message(len(rows), watermark, reason)) from exc


def _landed(run, headers, rows, *, cursor_dest_col, watermark, keyed, record):
    from .stream_boundary import remap_boundary_rows

    cfg, state = run.cfg, run.cur._boundary_fps_state
    dialect = cfg.dest_bind_dialect or cfg.dest_type
    types = dict(cfg.digest_dest_types or {})
    targets = list(cfg.target_cols or [])
    # Keyless peers are compared on every column, so as the recipe wrote them.
    from .stream_boundary_recipe import recipe_touches

    mapped = remap_boundary_rows(
        cfg, headers, rows,
        shape=not keyed and recipe_touches(getattr(cfg, "shape_runner", None), cfg.mappings, targets),
    )
    summary = _summary(run)
    tie = None
    if summary is not None and record:
        tie = summary.setdefault(BOUNDARY_TIE_KEY, {})
        tie.update(cursor_column=cursor_dest_col, watermark=str(watermark), types=types,
                   mode="keyed" if keyed else "keyless", table=str(cfg.dest_table or ""))
    if keyed:
        return _keyed_landed(run, headers, rows, mapped, tie, dialect=dialect, types=types,
                             cursor_dest_col=cursor_dest_col, watermark=watermark)
    if "keyless_pool" not in state:
        from .stream_boundary_recipe import run_stamped_targets

        # A run-stamped column holds the earlier run's start on the stored copy
        # and this run's start on the probe: never part of the match (D359).
        stamped = {c.lower() for c in run_stamped_targets(getattr(cfg, "shape_runner", None), cfg.mappings)}
        live = live_columns(dialect=dialect, dest_cfg=cfg.dest_cfg, table=cfg.dest_table)
        stored = read_candidates(
            dialect=dialect, dest_cfg=cfg.dest_cfg, table=cfg.dest_table,
            cursor_col=cursor_dest_col, watermark=str(watermark), key_cols=[], keys=None,
            columns=targets, ddl=str(types.get(cursor_dest_col) or ""),
        )
        pool: dict[str, int] = {}
        cols: list[str] | None = None
        for r in stored:
            if cols is None:
                have = live if live is not None else set(r.keys())
                cols = [c for c in targets if c.lower() in have and c != cursor_dest_col
                        and c.lower() not in stamped]
            k = _fp(tuple(r.get(c.lower()) for c in cols), cols, dialect, types)
            pool[k] = pool.get(k, 0) + 1
        if cols is None:
            have = live if live is not None else set()
            cols = [c for c in targets if c.lower() in have and c != cursor_dest_col
                    and c.lower() not in stamped]
        if stamped and not cols:
            from .stream_boundary_recipe import RecipeBoundaryStructural

            raise RecipeBoundaryStructural(
                "Every column this route writes besides the cursor holds a value its recipe "
                "stamps on every run (run_timestamp, surrogate_key), so a re-read row at the "
                "stored watermark cannot be told apart from its stored copy. Declare a key on "
                "this route, add a source column to it, run it as full_refresh_overwrite, or "
                "record the route's acknowledgement 'accept possible boundary duplicates'."
            )
        state["keyless_pool"], state["keyless_cols"] = pool, cols
    pool = dict(state["keyless_pool"])  # the verify pass asks the same question
    cols = state["keyless_cols"]
    pos = [targets.index(c) for c in cols]
    landed: set[int] = set()
    src_counts: dict[str, int] = {}
    for i, row in enumerate(mapped):
        if row is None:
            continue
        rfp = _fp(tuple(row[p] for p in pos), cols, dialect, types)
        src_counts[rfp] = src_counts.get(rfp, 0) + 1
        if pool.get(rfp, 0) > 0:
            pool[rfp] -= 1  # one stored copy absorbs one re-read peer
            landed.add(i)
    if tie is not None:
        tie["match_columns"] = cols
        counts = tie.setdefault("row_counts", {})
        for k, n in src_counts.items():
            counts[k] = counts.get(k, 0) + n
    return landed


def _keyed_landed(run, headers, rows, mapped, tie, *, dialect, types, cursor_dest_col, watermark):
    cfg, state = run.cfg, run.cur._boundary_fps_state
    key_cols = list(cfg.pk_target_cols)
    keys = source_key_texts(cfg, headers, rows)
    if keys is None:
        raise BoundaryStructural("a key column is not on the read page")
    cache: dict[str, int] = state.setdefault("keyed_dest", {})
    todo = [k for k in keys if _key_token(k) not in cache]
    if todo:
        by_fp: dict[str, int] = {}
        for r in read_candidates(
            dialect=dialect, dest_cfg=cfg.dest_cfg, table=cfg.dest_table,
            cursor_col=cursor_dest_col, watermark=str(watermark), key_cols=key_cols,
            keys=todo, columns=[cursor_dest_col, *key_cols], ddl=str(types.get(cursor_dest_col) or ""),
        ):
            kfp = _fp(tuple(r.get(c.lower()) for c in key_cols), key_cols, dialect, types)
            by_fp[kfp] = by_fp.get(kfp, 0) + 1
        for k in todo:
            cache[_key_token(k)] = by_fp.get(
                _fp(tuple(k.get(c) for c in key_cols), key_cols, dialect, types), 0
            )
    landed: set[int] = set()
    for i, row in enumerate(mapped):
        copies = cache[_key_token(keys[i])]
        if row is not None and copies:
            landed.add(i)
        if tie is not None:
            tie["key_columns"] = key_cols
            if keys[i] not in tie.setdefault("keys", []):
                tie["keys"].append(keys[i])
            tie.setdefault("pre_copies", {})[_key_token(keys[i])] = copies
    return landed


# --- the post-write check (Gate-8) ---------------------------------------------


def accepted_window_duplicates(report: dict[str, Any], dups: dict[str, Any], dest_summary: Any) -> dict[str, Any] | None:
    """The window-duplicate finding of a route that accepted boundary duplicates.

    ``None`` (the caller fails the run as always) unless the run recorded the
    route's audited acknowledgement because its boundary could not be checked.
    """
    tie = (dest_summary or {}).get(BOUNDARY_TIE_KEY) if isinstance(dest_summary, dict) else None
    ack = (tie or {}).get("accepted") if isinstance(tie, dict) else None
    if not dups or not isinstance(ack, dict):
        return None
    out = dict(report)
    out["window_duplicates"] = {**dups, "accepted": dict(ack)}
    out["migration_proven"] = False
    note = (
        f"{int(dups.get('rows') or 0)} row(s) at the watermark were appended again; possible "
        f"boundary duplicates are accepted for this route by {ack.get('actor')} ({ack.get('reason')})."
    )
    out["message"] = f"{str(out.get('message') or '').strip()} {note}".strip()
    return out


def boundary_duplicates(*, dest_summary: Any, endpoint: Any, mappings: list[dict] | None = None) -> list[dict[str, Any]]:
    """Tie-group rows the destination holds more often than the source does."""
    from .adapters import resolve_connector_config
    from .connector_capabilities import resolve_driver_type

    tie = dest_summary.get(BOUNDARY_TIE_KEY) if isinstance(dest_summary, dict) else None
    if not isinstance(tie, dict) or not tie.get("mode"):
        return []
    dialect = resolve_driver_type(getattr(endpoint, "format", "") or "")
    dest_cfg = resolve_connector_config(endpoint)
    # A document store's endpoint names its target as ``collection`` (a Mongo
    # schedule leaves ``table`` empty): reading collection "" failed every
    # post-write count (InvalidName -> TargetSampleUnavailable).
    table = str(
        tie.get("table") or getattr(endpoint, "table", "") or getattr(endpoint, "collection", "")
        or dest_summary.get("table") or ""
    )
    types = dict(tie.get("types") or {})
    cursor_col = str(tie.get("cursor_column") or "")
    watermark = str(tie.get("watermark"))
    if tie["mode"] == "keyed":
        key_cols = list(tie.get("key_columns") or [])
        keys = list(tie.get("keys") or [])
        if not keys:
            return []
        wanted = {_fp(tuple(k.get(c) for c in key_cols), key_cols, dialect, types): k for k in keys}
        counts: dict[str, int] = {}
        for r in read_candidates(
            dialect=dialect, dest_cfg=dest_cfg, table=table, cursor_col=cursor_col,
            watermark=watermark, key_cols=key_cols, keys=keys, columns=[cursor_col, *key_cols],
            ddl=str(types.get(cursor_col) or ""),
        ):
            kfp = _fp(tuple(r.get(c.lower()) for c in key_cols), key_cols, dialect, types)
            if kfp in wanted:
                counts[kfp] = counts.get(kfp, 0) + 1
        pre = tie.get("pre_copies") or {}
        return [
            {"key": wanted[k], "copies": n, "expected": 1,
             "copies_before_run": pre.get(_key_token(wanted[k])), "cursor": watermark}
            for k, n in counts.items()
            if n > 1
        ]
    cols = list(tie.get("match_columns") or [])
    counts = {}
    for r in read_candidates(
        dialect=dialect, dest_cfg=dest_cfg, table=table, cursor_col=cursor_col,
        watermark=watermark, key_cols=[], keys=None, columns=[cursor_col, *cols],
        ddl=str(types.get(cursor_col) or ""),
    ):
        rfp = _fp(tuple(r.get(c.lower()) for c in cols), cols, dialect, types)
        counts[rfp] = counts.get(rfp, 0) + 1
    want = tie.get("row_counts") or {}
    return [
        {"key": {"row": k[:12] or "-"}, "copies": n, "expected": int(want[k]), "cursor": watermark}
        for k, n in counts.items()
        if k in want and n > int(want[k])
    ]


def apply_boundary_duplicate_gate(recon: Any, *, validation_mode: str = "strict", **kwargs: Any) -> Any:
    """Fail Gate-8 - loudly - when a watermark row is at the destination twice.

    A post-write read that fails is "not proven": strict fails, balanced is a
    warning (never verified). An audited acceptance is said, not failed.
    """
    if not isinstance(recon, dict):
        return recon
    tie = (kwargs.get("dest_summary") or {}).get(BOUNDARY_TIE_KEY) or {}
    if tie.get("accepted"):
        return _accepted_verdict(recon, tie, kwargs)
    try:
        dups = boundary_duplicates(**kwargs)
    except Exception as exc:  # noqa: BLE001 - unmeasured is said, and never green
        reason = str(exc) if isinstance(exc, BoundaryUnmeasured) else (
            f"the destination read failed ({type(exc).__name__})"
        )
        _unmeasured(recon, reason, validation_mode)
        return _no_green_count_sentence(recon)
    recon["boundary_duplicates"] = dups
    if dups:
        named = "; ".join(
            ", ".join(f"{c}={v}" for c, v in (d.get("key") or {}).items())
            + f" ({d.get('copies')} copies at {d.get('cursor')}, source holds {d.get('expected')}"
            + (f"; {d.get('copies_before_run')} already before this run"
               if (d.get("copies_before_run") or 0) > 1 else "")
            + ")"
            for d in dups[:8]
        )
        earlier = all((d.get("copies_before_run") or 0) >= int(d.get("copies") or 0) for d in dups)
        sentence = (
            f"Append delta NOT verified: {len(dups)} row(s) at the stored watermark are at the "
            "destination more often than the source holds them - the watermark row was "
            + ("appended again by an earlier run" if earlier else "appended again")
            + f" ({named}). The destination is not exactly-once."
        )
        recon["passed"] = False
        recon["migration_proven"] = False
        recon["verification_verdict"] = "failed"
        recon["error"] = sentence
        recon["message"] = f"{sentence} {str(recon.get('message') or '').strip()}".strip()
    return _no_green_count_sentence(recon)


def _accepted_verdict(recon: dict[str, Any], tie: dict[str, Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    """An acknowledged route: never green - at most "explained", with the count.

    The duplicates are still measured (from the window check, else a key-free
    re-read of the watermark) so the job says how many this run added.
    """
    ack = tie["accepted"]
    count: int | None = None
    window = recon.get("window_duplicates")
    if isinstance(window, dict) and window.get("accepted"):
        count = int(window.get("rows") or 0)
    else:
        try:
            count = sum(int(d["copies"]) - int(d["expected"]) for d in boundary_duplicates(**kwargs))
        except Exception:  # noqa: BLE001 - an unmeasured count is said as such
            count = None
    shown = "not measured" if count is None else str(count)
    ack["duplicates"] = count
    recon["boundary_check"] = {"status": "accepted", **ack}
    recon["migration_proven"] = False
    if recon.get("passed") is not False and recon.get("verification_verdict") not in ("failed",):
        recon["verification_verdict"] = "explained"
    note = (
        f"Boundary duplicates accepted by {ack.get('actor')}: {shown} this run "
        f"({ack.get('reason')}; {ack.get('why')})."
    )
    recon["message"] = f"{str(recon.get('message') or '').strip()} {note}".strip()
    if "Append delta verified" in recon["message"]:
        recon["message"] = recon["message"].replace(
            "Append delta verified", "Append row count matched (not a verdict)"
        )
    return recon


def _unmeasured(recon: dict[str, Any], reason: str, validation_mode: str) -> None:
    """The post-write read failed: strict fails, balanced warns - never verified."""
    from services.validation_mode_contract import normalize_validation_mode

    note = (
        "Exactly-once at the stored watermark is NOT proven: the destination could not be "
        f"read back after the write to count the watermark's rows ({reason})."
    )
    recon["boundary_check"] = {"status": "unmeasured", "reason": reason}
    recon["migration_proven"] = False
    if normalize_validation_mode(validation_mode) in {"strict", "maximum"}:
        recon["passed"] = False
        recon["verification_verdict"] = "failed"
        recon["error"] = note
    elif recon.get("verification_verdict") in (None, "verified"):
        recon["verification_verdict"] = "partial"
    recon["message"] = f"{note} {str(recon.get('message') or '').strip()}".strip()


def _no_green_count_sentence(recon: dict[str, Any]) -> dict[str, Any]:
    """A failed run, or one whose boundary check was not measured, never reads
    "Append delta verified". A clean run's message is left exactly as it was."""
    message = str(recon.get("message") or "")
    unproven = recon.get("passed") is False or (
        (recon.get("boundary_check") or {}).get("status") == "unmeasured"
    )
    if unproven and "Append delta verified" in message:
        recon["message"] = message.replace(
            "Append delta verified", "Append row count matched (not a verdict)"
        )
    return recon
