"""Restrict a destination read-back to the columns the mapping writes.

Gate-8 compares a destination digest against a source digest computed over the
mapped target columns. The read-back, however, selects the whole destination
row, so a column no mapping writes into — an extra nullable column, a default,
a generated column — entered the destination digest and not the source one.
Identical data then hashed differently and the job was marked failed *after*
the rows had committed: a false failure sitting on top of a real write.

Projection is refused when the read-back does not return every mapped column:
a missing destination column is a genuine proof failure, and hiding it behind
a narrower digest would be the silent-drop failure this product exists to
prevent.
"""

from __future__ import annotations

from typing import Any, Iterable, Iterator

from services.value_serializer import payload_cell_wire


def _wire_rows(rows: Iterable[Any]) -> Iterator[Any]:
    """Stored cells → the transfer wire the Gate-8 fingerprint decodes.

    The destination holds text, never wire; a stored ``__DF_ESC__x`` read
    back bare would decode to ``x`` against a faithful source (D121).
    """
    for row in rows:
        if isinstance(row, dict):
            yield {k: payload_cell_wire(v) for k, v in row.items()}
        else:
            yield tuple(payload_cell_wire(v) for v in row)


def project_readback(
    names: list[str] | None,
    target_columns: list[str] | None,
    rows: Iterable[Any],
) -> tuple[list[str], Iterable[Any]]:
    """Return ``(columns, rows)`` narrowed to ``target_columns`` when possible.

    Rows are put on the transfer wire (``_wire_rows``).
    """
    rows = _wire_rows(rows)
    cols = [str(c) for c in (names or []) if str(c).strip()]
    wanted = [str(c) for c in (target_columns or []) if str(c).strip()]
    if not cols:
        return (wanted, rows)
    if not wanted or len(wanted) >= len(cols):
        return (cols, rows)

    index: dict[str, int] = {}
    for i, name in enumerate(cols):
        index.setdefault(name.strip().lower(), i)
    positions: list[int] = []
    for name in wanted:
        pos = index.get(name.strip().lower())
        if pos is None:
            # Mapped column absent from the destination read-back — keep the
            # full row so the digest mismatch is reported, not hidden.
            return (cols, rows)
        positions.append(pos)

    resolved = [cols[p] for p in positions]

    def _projected() -> Iterator[Any]:
        for row in rows:
            if isinstance(row, dict):
                yield {name: row.get(name) for name in resolved}
            else:
                seq = list(row)
                yield tuple(seq[p] if p < len(seq) else None for p in positions)

    return (resolved, _projected())
