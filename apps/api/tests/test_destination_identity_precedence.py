"""Destination path precedence: a Studio field must not silently redirect a write.

Regression for the reported defect — a saved SQLite connector pointing at
``exports/smoke.db`` had its path overridden by the Studio ``Database`` field, so
the transfer landed in ``dataflow_test`` and the connector's file stayed empty.
"""

from __future__ import annotations

import pytest

from services.destination_identity import (
    DestinationIdentity,
    resolve_destination_database,
)


def test_saved_connector_wins_when_studio_is_blank() -> None:
    got = resolve_destination_database(
        saved_database="exports/smoke.db", requested_database="", db_type="sqlite"
    )
    assert got == DestinationIdentity(
        database="exports/smoke.db", authority="saved_connector"
    )


def test_file_backed_conflict_keeps_the_connector_path_and_reports_it() -> None:
    got = resolve_destination_database(
        saved_database="exports/smoke.db",
        requested_database="dataflow_test",
        db_type="sqlite",
    )
    assert got.database == "exports/smoke.db"
    assert got.authority == "saved_connector"
    assert got.conflict is True
    assert got.ignored_value == "dataflow_test"
    assert "dataflow_test" in got.note and "smoke.db" in got.note


def test_file_backed_override_is_honoured_only_when_acknowledged() -> None:
    got = resolve_destination_database(
        saved_database="exports/smoke.db",
        requested_database="dataflow_test",
        db_type="sqlite",
        override_acknowledged=True,
    )
    assert got.database == "dataflow_test"
    assert got.authority == "studio_override"
    assert got.conflict is True
    assert got.ignored_value == "exports/smoke.db"


def test_equivalent_file_paths_are_not_a_conflict() -> None:
    got = resolve_destination_database(
        saved_database="exports/smoke.db",
        requested_database="./exports/smoke.db",
        db_type="sqlite",
    )
    assert got.conflict is False
    assert got.authority == "saved_connector"


@pytest.mark.parametrize("db_type", ["postgresql", "mysql", "mongodb"])
def test_server_engines_allow_an_explicit_database_choice(db_type: str) -> None:
    got = resolve_destination_database(
        saved_database="analytics",
        requested_database="staging",
        db_type=db_type,
    )
    assert got.database == "staging"
    assert got.authority == "studio_override"
    assert got.conflict is True
    assert got.ignored_value == "analytics"


def test_server_engine_case_only_difference_is_not_an_override() -> None:
    got = resolve_destination_database(
        saved_database="Analytics", requested_database="analytics", db_type="postgresql"
    )
    assert got.authority == "saved_connector"
    assert got.conflict is False


def test_no_saved_connector_uses_the_inline_value() -> None:
    got = resolve_destination_database(
        saved_database=None, requested_database="dataflow_test", db_type="sqlite"
    )
    assert got == DestinationIdentity(
        database="dataflow_test", authority="studio_inline"
    )


def test_empty_saved_value_falls_back_to_inline() -> None:
    got = resolve_destination_database(
        saved_database="", requested_database="dataflow_test", db_type="postgresql"
    )
    assert got.database == "dataflow_test"
    assert got.authority == "studio_inline"
    assert got.conflict is False


def _saved_sqlite(monkeypatch) -> None:
    from transfer import adapters

    saved = {
        "host": "",
        "port": 0,
        "database": "exports/smoke.db",
        "username": "",
        "password": "",
        "type": "sqlite",
    }
    monkeypatch.setattr(
        adapters,
        "_lookup_saved_connector",
        lambda connector_id, workspace_id=None: saved,
    )


def test_execution_path_keeps_the_saved_sqlite_file(monkeypatch) -> None:
    """The engine — not just Validate — must resolve to the connector's file."""
    from transfer.adapters import resolve_connector_config
    from transfer.models import EndpointConfig

    _saved_sqlite(monkeypatch)
    cfg = resolve_connector_config(
        EndpointConfig(
            format="sqlite",
            connector_id="conn-sqlite",
            database="dataflow_test",
            table="portfolios",
        )
    )
    assert cfg["database"] == "exports/smoke.db"
    assert cfg["destination_identity"]["conflict"] is True
    assert cfg["destination_identity"]["ignored_value"] == "dataflow_test"


def test_execution_path_honours_an_acknowledged_override(monkeypatch) -> None:
    from transfer.adapters import resolve_connector_config
    from transfer.models import EndpointConfig

    _saved_sqlite(monkeypatch)
    cfg = resolve_connector_config(
        EndpointConfig(
            format="sqlite",
            connector_id="conn-sqlite",
            database="dataflow_test",
            table="portfolios",
            extra={"destination_override_acknowledged": True},
        )
    )
    assert cfg["database"] == "dataflow_test"
    assert cfg["destination_identity"]["authority"] == "studio_override"


def test_identity_is_serialisable_for_the_decision_artifact() -> None:
    payload = resolve_destination_database(
        saved_database="exports/smoke.db",
        requested_database="dataflow_test",
        db_type="sqlite",
    ).as_dict()
    assert payload["authority"] == "saved_connector"
    assert payload["conflict"] is True
    assert payload["database"] == "exports/smoke.db"
