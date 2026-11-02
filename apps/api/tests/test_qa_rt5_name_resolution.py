"""RT5-01 / RT5-02 / R2-110 / R2-111 — name lookup must not guess.

A deleted or mistyped connector name used to stage deletion of a different
live connector (shared tokens ``qa`` + ``rt5``). A schedule substring used to
run a different pipeline, and two pipelines were allowed to share a name.
"""

from __future__ import annotations

import pytest

from src.ai.copilot.schema_tools import AmbiguousConnectorError, _pick_connector


def _isolate_connectors(tmp_path, monkeypatch):
    import services.connector_store as connectors

    monkeypatch.setattr(connectors, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connectors, "_use_mongo", lambda: False)
    monkeypatch.setattr(connectors, "_resolve_backend", lambda: "file")
    monkeypatch.setattr(connectors, "_backend_choice", "file")


def _isolate_schedules(tmp_path, monkeypatch):
    import services.schedule_store as schedules

    monkeypatch.setattr(schedules, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedules, "_mongo_backend", lambda: None)


def test_identifier_typo_does_not_prefix_match():
    pool = [{"name": "QA_RT5_sqlite", "type": "sqlite", "id": "live"}]
    with pytest.raises(AmbiguousConnectorError) as exc:
        _pick_connector("QA_RT5_sqlit", pool)
    assert "no connector matched" in exc.value.message.lower()
    assert "QA_RT5_sqlite" in exc.value.message


def test_old_name_does_not_resolve_to_renamed_connector():
    pool = [{"name": "QA_RT5_sqlite_renamed", "type": "sqlite", "id": "live"}]
    with pytest.raises(AmbiguousConnectorError) as exc:
        _pick_connector("QA_RT5_sqlite", pool)
    assert "no connector matched" in exc.value.message.lower()


def test_deleted_name_does_not_token_match_a_live_connector():
    """RT5-01: ``QA_RT5_delme`` shared ``qa``/``rt5`` with the surviving connector."""
    pool = [
        {"name": "QA_RT5_sqlite_renamed", "type": "sqlite", "id": "dddf41c3"},
    ]
    with pytest.raises(AmbiguousConnectorError) as exc:
        _pick_connector("QA_RT5_delme", pool)
    assert "no connector matched" in exc.value.message.lower()
    assert "dddf41c3" not in exc.value.message


def test_exact_name_still_resolves_case_insensitively():
    pool = [{"name": "QA_RT5_sqlite", "type": "sqlite", "id": "live"}]
    assert _pick_connector("qa_rt5_sqlite", pool)["id"] == "live"


def test_destructive_dialect_word_does_not_select_an_instance():
    pool = [{"name": "Prod Postgres", "type": "postgresql", "id": "pg"}]
    with pytest.raises(AmbiguousConnectorError):
        _pick_connector("postgres", pool, exact_name=True)
    # Copilot phrase resolution is unchanged when the tool is not destructive.
    assert _pick_connector("postgres", pool)["id"] == "pg"


def test_duplicate_exact_names_are_an_error():
    pool = [
        {"name": "Orders", "type": "sqlite", "id": "a"},
        {"name": "orders", "type": "postgresql", "id": "b"},
    ]
    with pytest.raises(AmbiguousConnectorError) as exc:
        _pick_connector("Orders", pool)
    assert "a" in exc.value.message and "b" in exc.value.message


def test_delete_connector_refuses_a_deleted_name(tmp_path, monkeypatch):
    _isolate_connectors(tmp_path, monkeypatch)
    from services.connector_store import create_connector
    from src.ai.copilot.lifecycle_tools import delete_connector

    create_connector({
        "name": "QA_RT5_sqlite_renamed",
        "type": "sqlite",
        "connection_string": "sqlite:///tmp/qa.db",
    })
    result = delete_connector(name="QA_RT5_delme")
    assert result.success is False
    assert "no connector matched" in (result.error or "").lower()
    # The saved list may name the survivor. The ack must not stage its deletion.
    assert result.output is None
    assert "delete connector" not in (result.error or "").lower()


def test_schedule_substring_does_not_resolve(tmp_path, monkeypatch):
    _isolate_schedules(tmp_path, monkeypatch)
    from services.schedule_store import create_schedule
    from src.ai.copilot.tools import DataPilotTools

    create_schedule({
        "name": "QA_RT5_sched_nightly",
        "source_connector_id": "s",
        "source_table": "t",
        "dest_connector_id": "d",
        "dest_table": "t",
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })
    executor = DataPilotTools.__new__(DataPilotTools)
    sched, clarify = executor._resolve_schedule(name="QA_RT5_sched")
    assert sched is None
    assert clarify is None
    hit, clarify = executor._resolve_schedule(name="qa_rt5_sched_nightly")
    assert hit is not None
    assert clarify is None


def test_duplicate_schedule_names_rejected(tmp_path, monkeypatch):
    _isolate_schedules(tmp_path, monkeypatch)
    from services.schedule_store import create_schedule, update_schedule

    first = create_schedule({
        "name": "Nightly",
        "source_connector_id": "s1",
        "source_table": "a",
        "dest_connector_id": "d1",
        "dest_table": "a",
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })
    with pytest.raises(ValueError, match="already exists"):
        create_schedule({
            "name": "nightly",
            "source_connector_id": "s2",
            "source_table": "b",
            "dest_connector_id": "d2",
            "dest_table": "b",
            "interval": "hourly",
            "mappings": [{"source": "id", "target": "id"}],
        })
    other = create_schedule({
        "name": "Hourly",
        "source_connector_id": "s2",
        "source_table": "b",
        "dest_connector_id": "d2",
        "dest_table": "b",
        "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    })
    with pytest.raises(ValueError, match="already exists"):
        update_schedule(other.id, {"name": "Nightly"})
    assert update_schedule(first.id, {"name": "Nightly"}) is not None


def test_ambiguous_schedule_name_asks_for_an_id(tmp_path, monkeypatch):
    _isolate_schedules(tmp_path, monkeypatch)
    from services.schedule_store import _load_all, _save_all, create_schedule
    from src.ai.copilot.tools import DataPilotTools

    create_schedule({
        "name": "Nightly",
        "source_connector_id": "s1",
        "source_table": "a",
        "dest_connector_id": "d1",
        "dest_table": "a",
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })
    # A row written before the uniqueness guard still has to fail closed.
    rows = _load_all()
    twin = rows[0]
    from services.schedule_store import PipelineSchedule

    clone = PipelineSchedule.from_dict({**twin.to_dict(), "id": "sched-twin"})
    rows.append(clone)
    _save_all(rows)
    executor = DataPilotTools.__new__(DataPilotTools)
    sched, clarify = executor._resolve_schedule(name="Nightly")
    assert sched is None
    assert clarify and "sched-twin" in clarify and twin.id in clarify
