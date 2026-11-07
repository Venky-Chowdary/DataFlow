"""D244 — an SFTP introspect listing streamed every file in the directory.

``list_files`` (names only, used by introspect/existence) went through
``list_file_stats``, which sha256-hashes each file. Over a ~300 ms link a 68 KB
CSV preflight spent 14 of its 40 SFTP requests downloading the file to learn its
name; a directory of large files would be downloaded in full on every probe.
The processed-file ledger still gets its sha256.
"""
from __future__ import annotations

import hashlib
import os

import paramiko
import pytest

from tests.sftp_test_server import start_sftp_server


@pytest.fixture()
def server(tmp_path):
    root = str(tmp_path)
    blobs = {
        "a.csv": b"id,name\n1,x\n",
        "b.csv": b"id,name\n" + b"2,y\n" * 200_000,
        "c.json": b'{"id": 3}',
    }
    for name, body in blobs.items():
        with open(os.path.join(root, name), "wb") as fh:
            fh.write(body)
    details, runner = start_sftp_server(root)
    try:
        yield details, blobs
    finally:
        runner.stop()


def _cfg(details):
    cfg = dict(details.endpoint_config("/"))
    cfg.pop("type", None)
    return cfg


def _count_opens(monkeypatch):
    opened: list[str] = []
    real = paramiko.SFTPClient.open

    def spy(self, filename, *args, **kwargs):
        opened.append(str(filename))
        return real(self, filename, *args, **kwargs)

    monkeypatch.setattr(paramiko.SFTPClient, "open", spy)
    return opened


def test_name_listing_opens_no_file(server, monkeypatch):
    from connectors.sftp_reader import list_files

    details, blobs = server
    opened = _count_opens(monkeypatch)
    names = list_files(cfg=_cfg(details), directory="/")
    assert sorted(names) == sorted(blobs)
    assert opened == [], f"name listing opened {opened}"


def test_glob_name_listing_opens_no_file(server, monkeypatch):
    from connectors.sftp_reader import list_files

    details, _ = server
    opened = _count_opens(monkeypatch)
    assert sorted(list_files(cfg=_cfg(details), directory="/", pattern="*.csv")) == [
        "a.csv",
        "b.csv",
    ]
    assert opened == []


def test_processed_file_identity_keeps_sha256(server):
    from connectors.sftp_reader import list_file_stats

    details, blobs = server
    rows = {r["name"]: r for r in list_file_stats(cfg=_cfg(details), directory="/")}
    assert set(rows) == set(blobs)
    for name, body in blobs.items():
        assert rows[name]["checksum"] == hashlib.sha256(body).hexdigest()
        assert rows[name]["size"] == len(body)
