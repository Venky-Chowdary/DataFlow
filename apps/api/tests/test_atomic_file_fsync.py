"""write_json_atomic is durable: data fsynced before the rename, the directory after.

Without the fsyncs a power loss after os.replace can leave the target empty or
partial (the rename reached the journal, the data blocks did not), which is how
a JSON store such as transfer_plans.json comes back unreadable.
"""

from __future__ import annotations

import json
import os
import stat

from services import atomic_file


def test_the_temp_file_is_fsynced_before_the_rename_and_the_directory_after(tmp_path, monkeypatch):
    events: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace

    def recording_fsync(fd):
        events.append("fsync-dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "fsync-file")
        return real_fsync(fd)

    def recording_replace(src, dst):
        events.append("replace")
        return real_replace(src, dst)

    monkeypatch.setattr(atomic_file.os, "fsync", recording_fsync)
    monkeypatch.setattr(atomic_file.os, "replace", recording_replace)
    target = tmp_path / "store.json"
    atomic_file.write_json_atomic(target, {"plans": [1, 2, 3]})

    assert events == ["fsync-file", "replace", "fsync-dir"], events
    assert json.loads(target.read_text()) == {"plans": [1, 2, 3]}


def test_a_directory_that_refuses_fsync_does_not_fail_the_write(tmp_path, monkeypatch):
    real_fsync = os.fsync

    def no_dir_fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(22, "Invalid argument")  # some filesystems
        return real_fsync(fd)

    monkeypatch.setattr(atomic_file.os, "fsync", no_dir_fsync)
    target = tmp_path / "store.json"
    atomic_file.write_json_atomic(target, {"ok": True})
    assert json.loads(target.read_text()) == {"ok": True}
    assert not [p for p in tmp_path.iterdir() if p.name != "store.json"], "no temp file left"


def test_fsyncs_run_outside_the_process_wide_write_lock(tmp_path, monkeypatch):
    """A slow disk flush must not serialize every JSON store in the process."""
    held: list[bool] = []
    real_fsync = os.fsync

    def recording_fsync(fd):
        held.append(atomic_file._WRITE_LOCK.locked())
        return real_fsync(fd)

    monkeypatch.setattr(atomic_file.os, "fsync", recording_fsync)
    atomic_file.write_json_atomic(tmp_path / "store.json", {"a": 1})
    assert held == [False, False], held


def test_macos_full_fsync_is_used_where_available(tmp_path, monkeypatch):
    calls: list[int] = []

    class FakeFcntl:
        F_FULLFSYNC = 51

        @staticmethod
        def fcntl(fd, op, *args):
            calls.append(op)
            return 0

    monkeypatch.setattr(atomic_file, "_fcntl", FakeFcntl)
    atomic_file.write_json_atomic(tmp_path / "store.json", {"a": 1})
    assert calls == [51, 51], "data file and directory both get F_FULLFSYNC"


def test_a_directory_fsync_io_error_is_a_warning(tmp_path, monkeypatch, caplog):
    import logging

    real_fsync = os.fsync

    def eio_on_dir(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(5, "Input/output error")
        return real_fsync(fd)

    monkeypatch.setattr(atomic_file.os, "fsync", eio_on_dir)
    caplog.set_level(logging.DEBUG, logger=atomic_file.logger.name)
    atomic_file.write_json_atomic(tmp_path / "store.json", {"a": 1})
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("fsync" in r.getMessage() for r in warnings), [r.getMessage() for r in caplog.records]


def test_the_directory_fsync_can_be_batched(tmp_path, monkeypatch):
    kinds: list[str] = []
    real_fsync = os.fsync

    def recording_fsync(fd):
        kinds.append("dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        return real_fsync(fd)

    monkeypatch.setattr(atomic_file.os, "fsync", recording_fsync)
    for i in range(3):
        atomic_file.write_json_atomic(tmp_path / f"p{i}.json", {"i": i}, sync_directory=False)
    atomic_file.fsync_directory(tmp_path)
    assert kinds == ["file", "file", "file", "dir"]
