"""Bounded, ordered parallel chunk processing for streaming transfers.

The ``ChunkDispatcher`` lets the caller feed chunks from the main thread and
receive results in ascending index order. This keeps per-chunk state (offsets,
cursors, keyset bookmarks) under the caller's control while reads, writes,
and CPU work overlap.

``OrderedChunkRunner`` is a convenience subclass that consumes a complete
``(idx, item)`` iterable on a background reader thread. Use it when the source
iterator is stateless and cheap; for stateful streams, use ``ChunkDispatcher``
directly and drive the input from the main thread.

This is intentionally generic: it does not know about transfer semantics; the
caller supplies a ``process(idx, item)`` function.
"""

from __future__ import annotations

import concurrent.futures
import os
from services.brand_env import getenv_brand
import queue
import threading
from collections.abc import Callable, Iterable, Iterator
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")

# Phase F6 — library default follows stream (env unset → 4, not 1).
DEFAULT_WORKERS = int(getenv_brand("PARALLEL_WORKERS", "4") or "4")
DEFAULT_PREFETCH = int(getenv_brand("PARALLEL_QUEUE", str(max(DEFAULT_WORKERS * 2, 4))))


class ChunkAborted(Exception):
    """A queued chunk was dropped before it began because the run was aborted.

    Distinct from a chunk *failure*: nothing was written, so the caller can stop
    cleanly at the last committed offset rather than treating it as an error to
    retry.
    """


class ChunkDispatcher:
    """Dispatch chunks to a thread pool and receive results in index order.

    The caller feeds chunks via :meth:`submit` and drives the source iterator
    itself, which keeps any per-chunk mutable state (offsets, cursors, keyset
    bookmarks) under the caller's control. Completed chunks are buffered and
    returned in ascending index order so checkpoints and accumulators can be
    updated sequentially while reads/writes overlap.

    A bounded ``max_inflight`` cap is enforced in :meth:`submit` so a fast source
    reader cannot outrun slow destination writers and exhaust memory.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        max_inflight: int | None = None,
    ) -> None:
        self.max_workers = max_workers if max_workers is not None else DEFAULT_WORKERS
        self.max_inflight = max_inflight if max_inflight is not None else max(self.max_workers * 2, 4)
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._pending: dict[concurrent.futures.Future, int] = {}
        self._buffer: dict[int, R] = {}
        self._next_yield: int | None = None
        self._closed = False
        self._aborted = threading.Event()

    def __enter__(self) -> "ChunkDispatcher":
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self._closed = False
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def abort(self) -> None:
        """Stop queued chunks from being written.

        ``Future.cancel()`` alone is not enough: it is a no-op once a future has
        started, and ``shutdown(wait=True)`` then blocks until every started
        chunk finishes. With up to ``max_inflight`` chunks in flight, a
        cancellation therefore still committed a batch of chunks *past* the
        persisted checkpoint, and a later resume rewrote that region — duplicate
        rows in insert mode.

        Setting this flag makes every not-yet-started chunk raise
        :class:`ChunkAborted` instead of writing. A chunk already mid-write
        cannot be stopped from here and is allowed to finish so it commits
        atomically rather than half-applied.
        """
        self._aborted.set()

    @property
    def aborted(self) -> bool:
        return self._aborted.is_set()

    def _guarded(self, process: Callable[[int, T], R]) -> Callable[[int, T], R]:
        """Wrap ``process`` so an aborted run refuses to write queued chunks."""

        def _run(idx: int, item: T) -> R:
            if self._aborted.is_set():
                raise ChunkAborted(f"chunk {idx} dropped before write (run aborted)")
            return process(idx, item)

        return _run

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._executor:
            for future in self._pending:
                future.cancel()
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None

    def _wait_for_room(self) -> None:
        """Block until at least one in-flight chunk has completed."""
        if self._executor is None:
            raise RuntimeError("Use ChunkDispatcher as a context manager (with ...)")
        while len(self._pending) >= self.max_inflight:
            done, _ = concurrent.futures.wait(
                self._pending.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                idx = self._pending.pop(future)
                self._buffer[idx] = future.result()

    def submit(self, idx: int, item: T, process: Callable[[int, T], R]) -> None:
        """Submit a chunk to the worker pool, blocking if the in-flight cap is reached."""
        if self._executor is None:
            raise RuntimeError("Use ChunkDispatcher as a context manager (with ...)")
        self._wait_for_room()
        future = self._executor.submit(self._guarded(process), idx, item)
        self._pending[future] = idx
        if self._next_yield is None:
            self._next_yield = idx

    def _drain_completed(self) -> None:
        if not self._pending:
            return
        done, _ = concurrent.futures.wait(
            self._pending.keys(),
            timeout=0.05,
            return_when=concurrent.futures.FIRST_COMPLETED,
        )
        for future in done:
            idx = self._pending.pop(future)
            self._buffer[idx] = future.result()

    def ready(self) -> Iterator[tuple[int, R]]:
        """Yield all consecutive, in-order results that are ready right now."""
        self._drain_completed()
        while self._next_yield is not None and self._next_yield in self._buffer:
            yield self._next_yield, self._buffer.pop(self._next_yield)
            self._next_yield += 1

    def results(self) -> Iterator[tuple[int, R]]:
        """Yield the remaining results in order, blocking until all are done."""
        if self._executor is None:
            raise RuntimeError("Use ChunkDispatcher as a context manager (with ...)")
        while self._pending or self._buffer:
            done, _ = concurrent.futures.wait(
                self._pending.keys(),
                timeout=0.1,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                idx = self._pending.pop(future)
                self._buffer[idx] = future.result()
            while self._next_yield is not None and self._next_yield in self._buffer:
                yield self._next_yield, self._buffer.pop(self._next_yield)
                self._next_yield += 1
            # Safety: if there is no more work in flight and the next expected
            # index is not present, the stream has become inconsistent; stop
            # rather than spinning forever.
            if not self._pending and self._buffer:
                if self._next_yield is None or min(self._buffer.keys()) != self._next_yield:
                    raise RuntimeError(
                        f"Parallel chunk ordering broken: expecting {self._next_yield}, "
                        f"buffer has {sorted(self._buffer.keys())[:5]}"
                    )


class OrderedChunkRunner(ChunkDispatcher):
    """Run a per-chunk function in parallel while returning results in order.

    This subclass consumes a complete ``(idx, item)`` iterable on a background
    reader thread. Use it when the source iterator is stateless and cheap; for
    stateful streams, use :class:`ChunkDispatcher` directly and drive the input
    from the main thread.

    - ``max_workers`` controls how many chunks are processed concurrently.
    - ``max_prefetch`` controls how many input items the reader thread buffers
      ahead of the workers, bounding memory usage.
    - If ``process`` raises, iteration stops and the exception is re-raised from
      the consumer thread. Any running workers are cancelled/shutdown.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        max_prefetch: int | None = None,
    ) -> None:
        super().__init__(max_workers=max_workers)
        self.max_prefetch = max_prefetch if max_prefetch is not None else DEFAULT_PREFETCH
        self._prefetch: queue.Queue[tuple[int, T] | None] = queue.Queue(maxsize=self.max_prefetch)

    def run(
        self,
        iterable: Iterable[tuple[int, T]],
        process: Callable[[int, T], R],
    ) -> Iterator[tuple[int, R]]:
        """Yield ``(idx, result)`` in index order while processing chunks in parallel.

        Keeps up to ``max_workers`` chunks in flight. After the pool is full we
        block on the next completed chunk, yield consecutive results, and then
        refill. This avoids per-submission polling delays that would otherwise
        dominate small, fast batches.
        """
        if self._executor is None:
            raise RuntimeError("Use OrderedChunkRunner as a context manager (with ...)")

        def _reader() -> None:
            try:
                for idx, item in iterable:
                    self._prefetch.put((idx, item))
            finally:
                # Sentinel tells the dispatcher the input stream is done.
                self._prefetch.put(None)

        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()

        try:
            input_exhausted = False
            while True:
                # Refill the worker pool from the bounded prefetch queue.
                while len(self._pending) < self.max_workers and not input_exhausted:
                    try:
                        msg = self._prefetch.get(timeout=0.05)
                    except queue.Empty:
                        break
                    if msg is None:
                        input_exhausted = True
                        break
                    idx, item = msg
                    self.submit(idx, item, process)

                if not self._pending:
                    if input_exhausted:
                        # No work in flight and the reader has sent the sentinel.
                        break
                    # The reader is still producing items but none are buffered.
                    # Block on the prefetch queue instead of spinning with a short
                    # timeout; this prevents dropping remaining batches on slow CI.
                    msg = self._prefetch.get()
                    if msg is None:
                        input_exhausted = True
                        break
                    idx, item = msg
                    self.submit(idx, item, process)
                    continue

                # Block until at least one worker finishes, then yield all
                # consecutive in-order results that are ready.
                done, _ = concurrent.futures.wait(
                    self._pending.keys(),
                    timeout=0.2,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    idx = self._pending.pop(future)
                    self._buffer[idx] = future.result()
                while self._next_yield is not None and self._next_yield in self._buffer:
                    yield self._next_yield, self._buffer.pop(self._next_yield)
                    self._next_yield += 1

                # Safety: if the input is exhausted and no work is in flight,
                # but we are still missing the next index, something is wrong.
                if input_exhausted and not self._pending and self._buffer:
                    raise RuntimeError(
                        f"Parallel chunk ordering broken: expecting {self._next_yield}, "
                        f"buffer has {sorted(self._buffer.keys())[:5]}"
                    )
        finally:
            # Drain any stragglers and clean up.
            while self._pending or self._buffer:
                done, _ = concurrent.futures.wait(
                    self._pending.keys(),
                    timeout=0.2,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    idx = self._pending.pop(future)
                    self._buffer[idx] = future.result()
                while self._next_yield is not None and self._next_yield in self._buffer:
                    yield self._next_yield, self._buffer.pop(self._next_yield)
                    self._next_yield += 1
            if self._executor:
                for future in self._pending:
                    future.cancel()
                self._executor.shutdown(wait=True, cancel_futures=True)
                self._executor = None
            reader_thread.join(timeout=5)
