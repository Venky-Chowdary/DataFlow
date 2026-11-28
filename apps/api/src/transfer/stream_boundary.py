"""Boundary-tie dedupe for the inclusive incremental run seed.

Split out of ``stream`` (module size budget); ``stream`` re-exports it.
"""

from __future__ import annotations

from typing import Any

from connectors.writer_common import map_rows_for_fingerprint, row_fingerprints


def _boundary_append_dedupe(
    headers: list[str],
    boundary_rows: list[list[str]],
    dest_fps: set,
    *,
    match_cols: list[str],
    new_cols: list[str] | None = None,
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict | None,
    stream_error_policy: str | None,
    digest_dest_types: dict | None,
    dest_bind_dialect: str,
    remap_dest_types: dict | None = None,
    destination_pk_columns: list[str] | None = None,
    shape_runner: Any = None,
) -> set[int]:
    """Indices of re-read boundary rows whose ``match_cols`` the destination holds.

    D60/D98/D124 — the inclusive run seed (``cursor >= watermark``) re-reads
    the whole tie group. Upsert overwrites the re-read rows by key; an append
    drops the ones already delivered. ``dest_fps`` fingerprints the
    destination's boundary rows on ``match_cols`` — (cursor, key) for a keyed
    route, every column for an unkeyed one.

    ``new_cols`` are columns this run adds: the destination has no history for
    them, so a peer that carries a value there is new information and is kept.
    Only an upsert passes them (it rewrites the key in place); an append
    passes none, since keeping such a peer appends the same row twice (D335).

    The peers are remapped exactly as the writer maps them — ``remap_dest_types``
    is the write path's type basis (``fingerprint_dest_types``) — and hashed on
    ``digest_dest_types``, the carrier the destination read-back hashes on.
    Remapping on the carrier types quarantined every tz-aware peer bound for an
    NTZ column in this remap alone (the writer converts it), so the drop failed
    open and every peer was appended again on each run (round 5, D203 prov.).

    Fail-open by construction: a fingerprint that does not match, or any
    missing fingerprint keeps the row — a duplicate is caught by Gate-8's
    window check, a silently dropped row is not. A peer the remap itself holds
    out is kept (the writer quarantines it again); it no longer switches the
    drop off for its landed neighbours.

    ``shape_runner`` is the run's approved recipe: the peers are compared as it
    wrote them (:func:`shape_boundary_peers`, D359).
    """
    if shape_runner is not None:
        headers, shaped = shape_boundary_peers(shape_runner, headers, boundary_rows)
        live_idx = [i for i, r in enumerate(shaped) if r is not None]
        inner = _boundary_append_dedupe(
            headers, [shaped[i] for i in live_idx], dest_fps, match_cols=match_cols,
            new_cols=new_cols, mappings=mappings, target_cols=target_cols,
            column_types=column_types, stream_error_policy=stream_error_policy,
            digest_dest_types=digest_dest_types, dest_bind_dialect=dest_bind_dialect,
            remap_dest_types=remap_dest_types, destination_pk_columns=destination_pk_columns,
        )
        return {live_idx[j] for j in inner}
    remap_kw = dict(
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=stream_error_policy,
        dest_types=remap_dest_types if remap_dest_types is not None else digest_dest_types,
        dest_kind=dest_bind_dialect,
        destination_pk_columns=destination_pk_columns or None,
    )
    mapped, _rejected = map_rows_for_fingerprint(
        headers=list(headers or []), data_rows=boundary_rows, **remap_kw
    )
    if len(mapped) != len(boundary_rows):
        aligned: list = []
        for row in boundary_rows:
            one, _held = map_rows_for_fingerprint(
                headers=list(headers or []), data_rows=[row], **remap_kw
            )
            aligned.append(one[0] if len(one) == 1 else None)
        mapped = aligned
    try:
        positions = [target_cols.index(c) for c in match_cols]
    except ValueError:
        return set()
    live = [i for i, row in enumerate(mapped) if row is not None]
    projected = [tuple(mapped[i][p] for p in positions) for i in live]
    fps = [
        fp
        for _k, fp in row_fingerprints(
            projected,
            list(match_cols),
            dest_db_type=dest_bind_dialect,
            dest_types=digest_dest_types,
        )
    ]
    if len(fps) != len(live):
        return set()
    landed = {live[j] for j, fp in enumerate(fps) if fp in dest_fps}
    if new_cols:
        from services.value_serializer import is_missing_sentinel, is_reader_null_cell

        new_pos = [target_cols.index(c) for c in new_cols if c in target_cols]
        landed = {
            i
            for i in landed
            if all(
                mapped[i][p] is None
                or is_missing_sentinel(mapped[i][p])
                or is_reader_null_cell(mapped[i][p])
                for p in new_pos
            )
        }
    return landed


def shape_boundary_peers(
    shape_runner: Any, headers: list[str], rows: list[list[Any]]
) -> tuple[list[str], list[list[Any] | None]]:
    """The re-read peers as the write sees them: after the run's approved recipe.

    The tie dedupe runs on the page the source handed over, before the recipe
    shapes it; the destination holds the recipe's output. Compared raw, a
    transformed column (``name`` trimmed and lowered) never matched its stored
    copy, so on a keyless append every watermark row was appended again under a
    green run, and the post-write duplicate check counted the same raw rows and
    found nothing (D359). Each peer is shaped on a fork of the run's runner, so
    the run's own effect ledger is untouched.

    The result is aligned with ``rows``: ``None`` where the recipe removes the
    row (the write drops it too, so it is never "already landed"). A recipe
    that turns one row into several, or holds rows until end-of-stream, has no
    one-to-one image to compare: that is structural, and is refused by name.
    """
    if shape_runner is None or not rows:
        return list(headers or []), list(rows)
    from .stream_boundary_read import BoundaryStructural

    probe = shape_runner.fork()
    out_headers = list(headers or [])
    shaped: list[list[Any] | None] = []
    for row in rows:
        held = int(probe.effect.rows_buffered or 0)
        out_headers, got = probe.matrix(list(headers or []), [row])
        if len(got) > 1 or (not got and int(probe.effect.rows_buffered or 0) > held):
            raise BoundaryStructural(
                f"the approved transform recipe ({shape_runner.recipe_hash}) does not turn "
                "each source row into exactly one written row, so a re-read watermark row "
                "cannot be compared with its stored copy"
            )
        shaped.append(got[0] if got else None)
    return out_headers, shaped


def remap_boundary_rows(
    cfg, headers: list[str], rows: list[list[str]], *, shape: bool = False
) -> list:
    """Re-read peers mapped as the writer maps them; ``None`` where the remap holds one out.

    ``shape`` first applies the run's recipe (:func:`shape_boundary_peers`), so
    every column is compared as it was written.
    """
    if shape:
        headers, shaped = shape_boundary_peers(getattr(cfg, "shape_runner", None), headers, rows)
        live = [i for i, r in enumerate(shaped) if r is not None]
        mapped = remap_boundary_rows(cfg, headers, [shaped[i] for i in live])
        out_aligned: list = [None] * len(rows)
        for j, i in enumerate(live):
            out_aligned[i] = mapped[j]
        return out_aligned
    remap_kw = dict(
        mappings=cfg.mappings,
        target_cols=list(cfg.target_cols or []),
        column_types=cfg.column_types,
        error_policy=cfg.stream_error_policy,
        dest_types=(
            cfg.fingerprint_dest_types
            if cfg.fingerprint_dest_types is not None
            else cfg.digest_dest_types
        ),
        dest_kind=cfg.dest_bind_dialect,
        destination_pk_columns=list(cfg.pk_target_cols or []) or None,
    )
    out: list = []
    for row in rows:
        one, _held = map_rows_for_fingerprint(
            headers=list(headers or []), data_rows=[row], **remap_kw
        )
        out.append(one[0] if len(one) == 1 else None)
    return out
