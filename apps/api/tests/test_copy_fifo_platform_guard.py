"""The COPY routes hand rows over a path: a named pipe, or a spill file.

``os.mkfifo`` is POSIX-only, and the FIFO routes (PG→MySQL, MySQL→PG,
cross-instance MySQL→MySQL) used to decline on a host without it — a
Windows-hosted DataFlow lost the server-side copy entirely and fell back to
the per-row writer. The payload now spills to a real file there and is read
once the producer finishes: the same bytes in the same order, sequential
instead of overlapped.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest  # noqa: E402

from services.copy_fast_path import (  # noqa: E402
    copy_spill_dir,
    fifo_streaming_supported,
    stream_between_cursors,
)


def test_fifo_support_follows_the_platform() -> None:
    assert fifo_streaming_supported() is hasattr(os, "mkfifo")


def _payload(count: int) -> bytes:
    return b"".join(f"{i}\tvalue-{i}\n".encode() for i in range(count))


def test_handoff_delivers_every_byte_in_order() -> None:
    """Whichever handoff the host has, the consumer reads what was written."""
    written = _payload(5000)
    read: list[bytes] = []

    def producer(path: str) -> None:
        with open(path, "wb") as writer:
            writer.write(written)

    def consumer(path: str) -> None:
        with open(path, "rb") as reader:
            read.append(reader.read())

    mode = stream_between_cursors(
        prefix="df_test_handoff_", producer=producer, consumer=consumer
    )
    assert mode == ("fifo" if fifo_streaming_supported() else "spill")
    assert read == [written]


def test_pipeless_host_spills_and_still_delivers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(os, "mkfifo", raising=False)
    written = _payload(1000)
    read: list[bytes] = []
    seen_paths: list[str] = []

    def producer(path: str) -> None:
        seen_paths.append(path)
        with open(path, "wb") as writer:
            writer.write(written)

    def consumer(path: str) -> None:
        seen_paths.append(path)
        with open(path, "rb") as reader:
            read.append(reader.read())

    assert (
        stream_between_cursors(
            prefix="df_test_spill_", producer=producer, consumer=consumer
        )
        == "spill"
    )
    assert read == [written]
    assert seen_paths[0] == seen_paths[1]
    # A whole table was on disk: leaving it there fills the volume on the next run.
    assert not os.path.exists(seen_paths[0])
    assert not os.path.exists(os.path.dirname(seen_paths[0]))


def test_spill_directory_is_operator_chosen(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """The spill can be moved off the system volume for a large migration."""
    monkeypatch.delattr(os, "mkfifo", raising=False)
    monkeypatch.setenv("DATAFLOW_COPY_SPILL_DIR", str(tmp_path))
    assert copy_spill_dir() == str(tmp_path)
    where: list[str] = []

    def producer(path: str) -> None:
        where.append(path)
        with open(path, "wb") as writer:
            writer.write(b"1\tx\n")

    def consumer(path: str) -> None:
        with open(path, "rb") as reader:
            assert reader.read() == b"1\tx\n"

    stream_between_cursors(
        prefix="df_test_spilldir_", producer=producer, consumer=consumer
    )
    assert os.path.dirname(os.path.dirname(where[0])) == str(tmp_path)


def test_a_producer_failure_reaches_the_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source read that dies must fail the copy, not load a truncated table."""
    monkeypatch.delattr(os, "mkfifo", raising=False)
    consumed: list[str] = []

    def producer(_path: str) -> None:
        raise RuntimeError("source read died")

    def consumer(path: str) -> None:
        consumed.append(path)

    with pytest.raises(RuntimeError, match="source read died"):
        stream_between_cursors(
            prefix="df_test_prodfail_", producer=producer, consumer=consumer
        )
    # Spilling is sequential, so a dead producer never reaches the load.
    assert consumed == []


def test_a_consumer_failure_reaches_the_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(os, "mkfifo", raising=False)

    def producer(path: str) -> None:
        with open(path, "wb") as writer:
            writer.write(b"1\tx\n")

    def consumer(_path: str) -> None:
        raise RuntimeError("LOAD DATA died")

    with pytest.raises(RuntimeError, match="LOAD DATA died"):
        stream_between_cursors(
            prefix="df_test_consfail_", producer=producer, consumer=consumer
        )


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="host has no os.mkfifo")
def test_named_pipe_producer_failure_reaches_the_caller() -> None:
    """On a FIFO the producer runs in a thread; its exception is still raised."""

    def producer(path: str) -> None:
        with open(path, "wb"):
            pass
        raise RuntimeError("source read died")

    def consumer(path: str) -> None:
        with open(path, "rb") as reader:
            reader.read()

    with pytest.raises(RuntimeError, match="source read died"):
        stream_between_cursors(
            prefix="df_test_fifofail_", producer=producer, consumer=consumer
        )


def test_same_instance_mysql_insert_select_needs_no_pipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """INSERT…SELECT never opens a handoff at all."""
    import services.copy_mysql_mysql as mod

    monkeypatch.delattr(os, "mkfifo", raising=False)
    monkeypatch.setattr(mod, "mysql_mysql_insert_select_enabled", lambda: True)
    reached: list[str] = []

    def _connect(cfg: dict[str, object]) -> object:
        reached.append("connect")
        raise RuntimeError("stop here")

    monkeypatch.setattr(mod, "_mysql_connect", _connect)
    with pytest.raises(RuntimeError, match="stop here"):
        mod.copy_mysql_to_mysql(
            source_cfg={"host": "127.0.0.1", "port": 3306, "database": "a"},
            source_table="t",
            dest_cfg={"host": "127.0.0.1", "port": 3306, "database": "b"},
            dest_table="t",
            pairs=[("id", "id")],
            mysql_ddls=["BIGINT"],
            replace_destination=True,
        )
    assert reached == ["connect"]
