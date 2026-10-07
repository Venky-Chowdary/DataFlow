"""A one-connection destination write must not cross into the chunk pool.

psycopg2 and the Snowflake connector are bound to the thread that opened
them. ChunkDispatcher always uses a pool, so a serial 100k load committed
the first pages and then waited forever inside the next COPY.
"""

from __future__ import annotations

import threading

import pytest

from services.parallel_chunks import drive_ordered_batches, shares_one_destination_connection


def test_one_worker_is_a_shared_connection():
    assert shares_one_destination_connection(1) is True
    assert shares_one_destination_connection(0) is True
    assert shares_one_destination_connection(4) is False


def _pull(pages: list[str | None]):
    state = {"i": 0}

    def fetch_next(_prev):
        item = pages[state["i"]]
        state["i"] += 1
        return item

    return fetch_next


def test_serial_batches_stay_on_the_caller_and_skip_the_pool(monkeypatch):
    import concurrent.futures

    caller = threading.get_ident()
    seen: list[int] = []
    opened: list[int] = []
    real = concurrent.futures.ThreadPoolExecutor

    def _open(*args, **kwargs):
        opened.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", _open)

    def process(idx: int, item: str) -> str:
        seen.append(threading.get_ident())
        assert idx
        return item

    applied: list[tuple[int, str]] = []
    drive_ordered_batches(
        max_workers=1,
        initial="already-committed",
        fetch_next=_pull(["p1", "p2", None]),
        prepare=lambda idx, batch: batch,
        process=process,
        apply_result=lambda idx, result: applied.append((idx, result)),
        start_idx=2,
    )
    assert applied == [(2, "p1"), (3, "p2")]
    assert seen == [caller, caller]
    assert opened == []


def test_a_serial_failure_stops_before_the_next_page():
    def process(_idx: int, item: str) -> str:
        if item == "p2":
            raise RuntimeError("copy stalled")
        return item

    applied: list[str] = []
    with pytest.raises(RuntimeError, match="copy stalled"):
        drive_ordered_batches(
            max_workers=1,
            initial="already-committed",
            fetch_next=_pull(["p1", "p2", "p3", None]),
            prepare=lambda idx, batch: batch,
            process=process,
            apply_result=lambda _idx, result: applied.append(result),
            start_idx=1,
        )
    assert applied == ["p1"]


def test_parallel_batches_still_run_off_the_caller_in_order():
    caller = threading.get_ident()
    threads: list[int] = []

    def process(_idx: int, item: str) -> str:
        threads.append(threading.get_ident())
        return item + "!"

    applied: list[tuple[int, str]] = []
    drive_ordered_batches(
        max_workers=2,
        initial="already-committed",
        fetch_next=_pull(["a", "b", "c", None]),
        prepare=lambda idx, batch: batch,
        process=process,
        apply_result=lambda idx, result: applied.append((idx, result)),
        start_idx=1,
    )
    assert applied == [(1, "a!"), (2, "b!"), (3, "c!")]
    assert threads
    assert any(thread != caller for thread in threads)
