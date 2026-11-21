"""Isolating the bad rows of a failed write chunk by splitting it, in row order.

When a chunk insert fails with a row-level data error under a quarantine
policy, the writers used to retry the whole chunk one row at a time: one
statement (and on MySQL one commit) per row, so a single bad value in a
10,000-row chunk cost 10,000 round trips. ``salvage_in_order`` instead tries
the two halves as batches, splits only a half that fails, and hands only a
single row to the writer's existing per-row salvage — the same code that
records the quarantine detail and the reason.

What stays exactly as before:

* Rows are visited in their original order, and every batch attempt is
  all-or-nothing (a savepoint on PostgreSQL, a rolled-back transaction on
  MySQL), so a row is only ever written by a batch that wrote all of it. An
  insert's outcome depends on the row and on the rows already written before
  it, so the rows that land and the rows held out — and the order their
  quarantine details are recorded in — are the ones the row-by-row salvage
  produced.
* Every held-out row still comes from a single-row attempt, so its reason
  text is that row's own driver error, never a neighbour's.

With ``k`` bad rows in a chunk of ``n`` the salvage makes about
``2k·log2(n/k)`` batch attempts instead of ``n`` single-row ones.
"""

from __future__ import annotations

from typing import Callable


def salvage_in_order(
    count: int,
    *,
    attempt_range: Callable[[int, int], bool],
    salvage_row: Callable[[int], None],
) -> None:
    """Write rows ``[0, count)`` in order after their chunk failed as a whole.

    ``attempt_range(lo, hi)`` writes rows ``lo..hi-1`` as one all-or-nothing
    batch and returns ``True`` when they landed, ``False`` (with nothing
    written) when the batch failed; it raises for an error that must stop
    the salvage (a lost connection or a lost savepoint). ``salvage_row(i)`` is
    the writer's per-row salvage for one row.
    """
    if count <= 0:
        return
    if count == 1:
        salvage_row(0)
        return
    # The chunk itself already failed: start from its halves. Ranges are popped
    # left-first, so rows are written (or held out) in their original order.
    mid = count // 2
    stack: list[tuple[int, int]] = [(mid, count), (0, mid)]
    while stack:
        lo, hi = stack.pop()
        if hi - lo == 1:
            salvage_row(lo)
            continue
        if attempt_range(lo, hi):
            continue
        mid = (lo + hi) // 2
        stack.append((mid, hi))
        stack.append((lo, mid))
