"""REL-3: a file export that fails mid-write leaves no partial file and says why plainly.

A full disk left ``export_<job>.csv`` truncated mid-row at a predictable
name, and the error was ``[Errno 27] File too large``.
"""

from __future__ import annotations

import errno
import os
from types import SimpleNamespace

import pytest


@pytest.fixture
def exports(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", "on")
    monkeypatch.setattr("services.export_object_store.exports_use_object_store", lambda: False)
    monkeypatch.setattr("services.export_object_store.job_export_workspace", lambda job_id: "")
    return tmp_path / "exports"


def _full_disk_after(monkeypatch, limit: int):
    """Writes past ``limit`` bytes fail like RLIMIT_FSIZE / ENOSPC."""
    import builtins

    real_open = builtins.open

    class Capped:
        def __init__(self, fh):
            self._fh, self._n = fh, 0

        def write(self, data):
            if self._n + len(data) > limit:
                self._fh.write(data[: max(0, limit - self._n)])
                raise OSError(errno.EFBIG, "File too large")
            self._n += len(data)
            return self._fh.write(data)

        def __getattr__(self, name):
            return getattr(self._fh, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self._fh.close()

    def capped_open(path, mode="r", *a, **k):
        fh = real_open(path, mode, *a, **k)
        return Capped(fh) if ("w" in mode or "a" in mode) and "export" in str(path) else fh

    monkeypatch.setattr(builtins, "open", capped_open)


def test_a_failed_export_write_keeps_no_partial_file(exports, monkeypatch):
    from services.file_export_append import land_export_bytes

    data = b"id,v\n" + b"".join(f"{i},{'x' * 40}\n".encode() for i in range(5000))
    _full_disk_after(monkeypatch, 4096)
    dest = SimpleNamespace(format="csv", output_path="")
    with pytest.raises(ValueError) as caught:
        land_export_bytes(dest, data, job_id="w1secjob", sync_mode="full_refresh_overwrite",
                          export_name="export_w1secjob.csv")
    message = str(caught.value)
    assert "Errno" not in message and "disk is full" in message and "No partial file" in message
    left = [p for p in exports.rglob("*") if p.is_file()]
    assert left == [], left


def test_an_append_that_fails_restores_the_file(exports, monkeypatch):
    from services.file_export_append import land_export_bytes

    dest = SimpleNamespace(format="csv", output_path="w1sec_append.csv")
    land_export_bytes(dest, b"id\n1\n", job_id="j1", sync_mode="full_refresh_append", export_name="x.csv")
    target = next(p for p in exports.rglob("w1sec_append.csv"))
    before = target.read_bytes()
    _full_disk_after(monkeypatch, 2)
    with pytest.raises(ValueError):
        land_export_bytes(dest, b"id\n" + b"2\n" * 5000, job_id="j2", sync_mode="full_refresh_append",
                          export_name="x.csv")
    assert target.read_bytes() == before
    assert os.path.getsize(target) == len(before)
