"""Throttled progress callbacks for million-row transfers."""

from __future__ import annotations

import time
from typing import Callable


class ThrottledCheckpoint:
    """Limit MongoDB job status writes during batched transfers."""

    def __init__(
        self,
        callback: Callable[[int, int, int], None],
        *,
        min_interval_sec: float = 2.0,
    ) -> None:
        self._callback = callback
        self._min_interval = min_interval_sec
        self._last_at = 0.0

    def __call__(self, chunk: int, chunks: int, rows: int) -> None:
        now = time.time()
        if chunk <= 1 or chunk >= chunks or now - self._last_at >= self._min_interval:
            self._last_at = now
            self._callback(chunk, chunks, rows)
