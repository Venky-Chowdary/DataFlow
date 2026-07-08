"""Shared dev-only stub write path — never used unless DATAFLOW_ALLOW_STUB_WRITES=1."""

from __future__ import annotations

from typing import Callable

from connectors.writer_common import CHUNK_SIZE, row_checksum


def simulate_stub_write(
    *,
    data_rows: list[list[str]],
    table_name: str,
    target_schema: str,
    on_checkpoint: Callable[[int, int, int], None] | None = None,
) -> tuple[int, str, int]:
    rows = len(data_rows)
    chunks = max(1, (rows + CHUNK_SIZE - 1) // CHUNK_SIZE)
    if on_checkpoint:
        for c in range(1, chunks + 1):
            on_checkpoint(c, chunks, min(c * CHUNK_SIZE, rows))
    checksum = row_checksum([tuple(r) for r in data_rows])
    return rows, checksum, chunks
