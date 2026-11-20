"""Cancel inside a single-transaction row salvage (D299).

PostgreSQL salvages a failed chunk row by row under SAVEPOINTs and commits
once when the chunk ends. The chunk-boundary cancel probe runs before the
chunk, so a cancel pressed during a long salvage (three round trips per row
on a remote server) let every salvaged row of the chunk commit after the job
already said cancelled.

:class:`SalvageCancelGuard` polls the job's cancel flag before each salvaged
row, at most once per ``interval`` seconds so a fast local salvage does not
pay a job-store read per row. On a cancel it discards the uncommitted chunk
(to the overwrite chunk savepoint when one transaction holds the whole
overwrite, otherwise a full rollback) and re-raises: nothing of the chunk
lands after the cancel, and the committed count is the rows before it.
"""

from __future__ import annotations

import time
from typing import Any

from services.error_handling import TransferCancelled


class SalvageCancelGuard:
    def __init__(self, conn: Any, cur: Any, *, interval: float = 0.5) -> None:
        self._conn = conn
        self._cur = cur
        self._interval = interval
        self._next = 0.0

    def check(self, rows_committed: int, *, held_overwrite: bool) -> None:
        """Raise ``TransferCancelled`` after discarding the open chunk."""
        now = time.monotonic()
        if now < self._next:
            return
        self._next = now + self._interval
        from services.cancel_check import poll_cancel

        try:
            poll_cancel(rows_committed)
        except TransferCancelled:
            if held_overwrite:
                self._cur.execute("ROLLBACK TO SAVEPOINT df_ow_chunk")
            else:
                self._conn.rollback()
            raise
