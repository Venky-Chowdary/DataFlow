"""Bound a whole-read batch transfer to its incremental delta.

The streaming paths bound their reads by the route's watermark; the batch path
(used whenever the destination is a file export, and for any small read the
engine materialises) did not. An ``incremental_append`` there re-read the whole
source every run and appended it again — the second run of a 200-row export left
450 rows, a duplication the run reported as success.

Same resolver, same cursor key, same refusals as every other path — a second
implementation of cursor arithmetic would be a second, divergent truth.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any

from services.sync_cursor import (
    IncrementalReadScope,
    compare_cursor_values,
    max_cursor_value,
    records_after_watermark,
    resolve_incremental_read_scope,
    set_watermark,
)

logger = logging.getLogger(__name__)


@dataclass
class BatchIncrementalBound:
    """The delta bound of one batch run, and the mark to persist on success."""

    scope: IncrementalReadScope
    high_mark: str | None = field(default=None)
    #: ``False`` for a snapshot-comparing mode (SCD2): the cursor still moves,
    #: but the read is not narrowed. See :func:`bind_batch_incremental`.
    narrows_read: bool = True

    @property
    def active(self) -> bool:
        return bool(self.scope.cursor_column and self.scope.cursor_key)

    def bound(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """The rows past the watermark; raises when a row cannot be judged."""
        if not self.active:
            return records
        delta, unbounded = records_after_watermark(
            records,
            self.scope.cursor_column,
            self.scope.watermark,
            primary_key=self.scope.primary_key,
        )
        if unbounded:
            # A row with no cursor value can be proven neither new nor already
            # landed. Skipping loses data, sending duplicates it — refuse.
            raise ValueError(
                f"{unbounded} row(s) carry no value for cursor "
                f"'{self.scope.cursor_column}' — an incremental read cannot "
                "prove whether they already landed. Fill the cursor column at "
                "the source, or run this sync as full refresh."
            )
        headers = [c for c in (self.scope.cursor_column, self.scope.primary_key) if c]
        mark = max_cursor_value(
            [[str(r.get(c, "")) for c in headers] for r in delta],
            headers,
            self.scope.cursor_column,
            self.scope.primary_key or None,
        )
        if mark and (
            self.high_mark is None or compare_cursor_values(mark, self.high_mark) > 0
        ):
            self.high_mark = mark
        return list(delta) if self.narrows_read else list(records)

    def commit(self) -> None:
        """Persist the watermark — only after the rows are proven at rest."""
        if not self.active or not self.high_mark:
            return
        set_watermark(
            self.scope.cursor_key,
            self.high_mark,
            metadata={"cursor_column": self.scope.cursor_column},
        )


def _dest_identity(destination: Any) -> tuple[str, str, str]:
    """(type, database, object) naming a destination for the cursor key."""
    if getattr(destination, "kind", "") == "file_export":
        path = str(getattr(destination, "output_path", "") or "")
        return (
            "file_export",
            "",
            os.path.basename(path)
            or getattr(destination, "table", "")
            or getattr(destination, "collection", "")
            or "export",
        )
    return (
        str(getattr(destination, "format", "") or "").lower(),
        str(getattr(destination, "database", "") or ""),
        str(getattr(destination, "table", "") or "")
        or str(getattr(destination, "collection", "") or ""),
    )


def bind_transfer_request(request: Any, source_format: str) -> BatchIncrementalBound:
    """Cursor bound for the buffered path, resolved exactly as streaming does."""
    dest_type, dest_database, dest_object = _dest_identity(request.destination)
    return bind_batch_incremental(
        sync_mode=str(getattr(request, "sync_mode", "") or ""),
        stream_contracts=list(getattr(request, "stream_contracts", None) or []),
        source_type=(source_format or "").lower(),
        source_database=str(getattr(request.source, "database", "") or ""),
        source_object=(
            str(getattr(request.source, "table", "") or "")
            or str(getattr(request.source, "collection", "") or "")
            or str(getattr(request, "source_filename", "") or "")
        ),
        dest_type=dest_type,
        dest_database=dest_database,
        dest_object=dest_object,
    )


def bind_batch_incremental(
    *,
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None,
    source_type: str,
    source_database: str,
    source_object: str,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    dest_rows: int | None = None,
) -> BatchIncrementalBound:
    """Resolve this route's cursor state, refusing states it cannot honour."""
    scope = resolve_incremental_read_scope(
        sync_mode=sync_mode,
        stream_contracts=stream_contracts,
        source_type=source_type,
        source_database=source_database,
        source_object=source_object,
        dest_type=dest_type,
        dest_database=dest_database,
        dest_object=dest_object,
    )
    if scope.cursor_column_changed:
        from services.preflight_cursor_gate import cursor_identity_issue

        raise ValueError(cursor_identity_issue(scope))
    if scope.bounded and dest_rows is not None:
        from services.preflight_cursor_gate import cursor_destination_reset_issue

        reset_issue = cursor_destination_reset_issue(scope, dest_rows)
        if reset_issue:
            raise ValueError(reset_issue)
    # SCD2 compares a whole source snapshot against the destination's current
    # versions: unchanged rows produce no new version, so a full read is
    # idempotent, and the current census is what Gate-8 can prove. Narrowing the
    # read to the cursor delta left the buffered path measuring one changed row
    # against a 200-row current population — Validate cleared and Run failed its
    # own row-count proof, while the streaming SQL path (which snapshots the
    # whole source into staging) passed the same case. One read scope per mode.
    from services.sync_cursor import normalize_sync_mode

    narrows = normalize_sync_mode(sync_mode, default="") != "scd2"
    return BatchIncrementalBound(scope=scope, narrows_read=narrows)
