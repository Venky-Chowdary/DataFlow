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
