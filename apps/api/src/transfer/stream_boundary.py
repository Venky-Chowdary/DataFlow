"""Boundary-tie dedupe for the inclusive incremental run seed.

Split out of ``stream`` (module size budget); ``stream`` re-exports it.
"""

from __future__ import annotations

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
) -> set[int]:
    """Indices of re-read boundary rows whose ``match_cols`` the destination holds.

    D60/D98/D124 — the inclusive run seed (``cursor >= watermark``) re-reads
    the whole tie group. Upsert overwrites the re-read rows by key; an append
    drops the ones already delivered. ``dest_fps`` fingerprints the
    destination's boundary rows on ``match_cols`` — (cursor, key) for a keyed
    route, every column for an unkeyed one.

    ``new_cols`` are columns this run adds: the destination has no history for
    them, so a peer that carries a value there is new information and is kept.

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
    """
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
