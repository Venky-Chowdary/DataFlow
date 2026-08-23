"""A reader that dies mid-stream must not look like a stream that ended.

The ordered runner reads its input on a background thread, so an exception
raised by the source iterator — a decode failure deep in a file, or a shaping
step refusing a row — has to be carried back to the consumer. If it is not, the
consumer sees the sentinel, stops, and reports the rows it managed to write as a
completed transfer: a silently truncated load.
"""

from __future__ import annotations

import pytest

from services.parallel_chunks import OrderedChunkRunner


class ReaderFailed(RuntimeError):
    """Stands in for a source read or shaping refusal."""


def _batches(fail_at: int, total: int = 6):
    for idx in range(total):
        if idx == fail_at:
            raise ReaderFailed(f"source read failed on batch {idx}")
        yield idx, idx * 10


def test_a_reader_exception_reaches_the_consumer():
    with OrderedChunkRunner(max_workers=2, max_prefetch=2) as runner:
        with pytest.raises(ReaderFailed):
            for _idx, _result in runner.run(_batches(fail_at=3), lambda i, v: v + 1):
                pass


def test_batches_the_reader_handed_over_are_still_yielded_in_order():
    """Accounting stays intact: what was read is seen, then the read fails.

    The consumer's ledger must be able to say how many rows it actually took
    before the failure, so the batches already handed over are not thrown away
    — the run still fails.
    """
    seen: list[tuple[int, int]] = []
    with OrderedChunkRunner(max_workers=2, max_prefetch=2) as runner:
        with pytest.raises(ReaderFailed):
            for idx, result in runner.run(_batches(fail_at=4), lambda i, v: v + 1):
                seen.append((idx, result))

    assert [idx for idx, _ in seen] == sorted(idx for idx, _ in seen)
    assert seen == [(idx, idx * 10 + 1) for idx, _ in seen]
    assert all(idx < 4 for idx, _ in seen)


def test_a_clean_stream_still_completes_in_order():
    with OrderedChunkRunner(max_workers=3, max_prefetch=2) as runner:
        out = list(runner.run(_batches(fail_at=-1), lambda i, v: v + 1))
    assert out == [(idx, idx * 10 + 1) for idx in range(6)]


def test_a_processing_exception_still_propagates():
    """The pre-existing contract: a failing worker stops iteration."""

    def _boom(idx: int, value: int) -> int:
        if idx == 2:
            raise ReaderFailed("chunk processing failed")
        return value

    with OrderedChunkRunner(max_workers=1, max_prefetch=2) as runner:
        with pytest.raises(ReaderFailed):
            for _idx, _result in runner.run(_batches(fail_at=-1), _boom):
                pass
