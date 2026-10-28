"""Candidate item 54 — a SQLite destination file must not live off the volume.

``QA_E2E_sqlite_dst`` listed 0 tables after a production restart: with no
``DATAFLOW_SQLITE_ROOT`` the path was used as given, so a relative name landed
in the container's working directory and ``/tmp/...`` in the container's
temp — both gone on the next restart. In production the default root is now a
directory on the persistent data volume, and the API says so at startup when
that volume is not mounted.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from connectors.sqlite_common import sqlite_file_path


@pytest.fixture()
def production(tmp_path, monkeypatch):
    for name in ("DATAFLOW_SQLITE_ROOT", "DATAWRAP_SQLITE_ROOT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.delenv("DATAWRAP_ENV", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    return tmp_path / "data" / "sqlite"


def test_production_relative_path_lands_on_the_data_volume(production):
    resolved = sqlite_file_path("QA_E2E_sqlite_dst.db", "", "")
    assert Path(resolved) == production.resolve() / "QA_E2E_sqlite_dst.db"
    assert production.is_dir()


def test_production_absolute_path_under_the_root_is_accepted(production):
    target = production / "nested" / "dst.db"
    assert Path(sqlite_file_path(str(target), "", "")) == target.resolve()


@pytest.mark.parametrize("path", ["/tmp/QA_RT5.db", "../escape.db", "sqlite:////var/tmp/x.db"])
def test_production_path_off_the_volume_is_refused_with_guidance(production, path):
    with pytest.raises(ValueError, match="persistent data volume"):
        sqlite_file_path(path, "", "")


def test_explicit_root_wins_and_anchors_relative_paths(production, tmp_path, monkeypatch):
    root = tmp_path / "custom"
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(root))
    assert Path(sqlite_file_path("a.db", "", "")) == root.resolve() / "a.db"
    with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
        sqlite_file_path(str(production / "a.db"), "", "")


def test_development_paths_are_untouched(tmp_path, monkeypatch):
    for name in ("DATAFLOW_SQLITE_ROOT", "DATAWRAP_SQLITE_ROOT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    assert sqlite_file_path("local.db", "", "") == "local.db"
    assert sqlite_file_path("/tmp/x.db", "", "") == "/tmp/x.db"
    assert sqlite_file_path(":memory:", "", "") == ":memory:"


def test_startup_warns_when_the_data_dir_is_not_a_mounted_volume(tmp_path, monkeypatch):
    from services import platform_config

    monkeypatch.delenv("DATAWRAP_ENV", raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(platform_config, "_on_root_filesystem", lambda _p: True)
    warnings = platform_config.persistent_storage_warnings()
    assert len(warnings) == 1
    assert "volume" in warnings[0] and "lost" in warnings[0]

    monkeypatch.setattr(platform_config, "_on_root_filesystem", lambda _p: False)
    assert platform_config.persistent_storage_warnings() == []

    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setattr(platform_config, "_on_root_filesystem", lambda _p: True)
    assert platform_config.persistent_storage_warnings() == []
