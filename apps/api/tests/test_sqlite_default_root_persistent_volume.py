"""Candidate item 54 — a SQLite destination file must not live off the volume.

``QA_E2E_sqlite_dst`` listed 0 tables after a production restart: with no
``DATAFLOW_SQLITE_ROOT`` the path was used as given, so a relative name landed
in the container's working directory and ``/tmp/...`` in the container's
temp — both gone on the next restart. With
``DATAFLOW_CONFINE_LOCAL_FILES=1`` the production default root is a directory on
the persistent data volume; with the flag off (the default) the path is used as
given and the would-be refusal is audited. The API says so at startup when the
volume is not mounted.
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
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "1")
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


def test_flag_off_keeps_today_behaviour_and_audits_what_it_would_refuse(
    production, monkeypatch, tmp_path
):
    monkeypatch.delenv("DATAFLOW_CONFINE_LOCAL_FILES")
    from services import would_apply_audit
    import services.audit_log as audit_log

    would_apply_audit.reset_dedupe()
    events: list[dict] = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw))
    assert sqlite_file_path("relative.db", "", "") == "relative.db"
    off_volume = str(tmp_path / "elsewhere" / "x.db")
    assert sqlite_file_path(off_volume, "", "") == off_volume
    on_volume = str(production / "ok.db")
    assert sqlite_file_path(on_volume, "", "") == on_volume
    assert [e["action"] for e in events] == ["local_file.would_confine"] * 2
    assert {e["details"]["kind"] for e in events} == {"sqlite"}
    assert events[1]["details"]["path"] == str(Path(off_volume).resolve())
    assert events[1]["details"]["would_be_root"] == str(production.resolve())
    assert events[1]["details"]["flag"] == "DATAFLOW_CONFINE_LOCAL_FILES"


def test_flag_off_still_refuses_service_data_internals_and_honours_explicit_roots(
    production, monkeypatch, tmp_path
):
    monkeypatch.delenv("DATAFLOW_CONFINE_LOCAL_FILES")
    data = production.parent
    with pytest.raises(ValueError, match="service data directory"):
        sqlite_file_path(str(data / "proof_ledger.db"), "", "")
    with pytest.raises(ValueError, match="service data directory"):
        sqlite_file_path(str(tmp_path / "x" / ".." / "data" / "jobs.db"), "", "")
    root = tmp_path / "custom"
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(root))
    with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
        sqlite_file_path(str(tmp_path / "outside.db"), "", "")
