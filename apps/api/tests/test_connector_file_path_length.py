"""A file engine's "database" is a file path, not an identifier.

PR 403 (review 3, H3) keeps SQLite and DuckDB files in the workspace's own
area of the volume, ``<volume>/_workspaces/<workspace id>/``. That prefix
alone is about 50 characters, so the 128-character identifier cap refused
ordinary paths ("Database is longer than 128 characters"). File engines get
a path-sized bound; every other engine keeps the identifier bound.
"""
from __future__ import annotations

import pytest

from services.connector_request_limits import (
    IDENT_MAX,
    PATH_MAX,
    connector_request_errors,
)

_WORKSPACE_PATH = (
    "/var/lib/datawrap/data/sqlite/_workspaces/"
    "0f3c9b1e-6a2d-4c8e-9b7a-1d2e3f4a5b6c/finance/2026/q3/ledger_reconciliation_snapshot_eu_west.db"
)


@pytest.mark.parametrize("db_type", ["sqlite", "duckdb", "SQLite"])
def test_a_workspace_volume_path_is_accepted_for_a_file_engine(db_type):
    assert len(_WORKSPACE_PATH) > IDENT_MAX
    assert connector_request_errors(db_type=db_type, database=_WORKSPACE_PATH) == ""


@pytest.mark.parametrize("db_type", ["sqlite", "duckdb"])
def test_a_file_path_is_still_bounded(db_type):
    assert connector_request_errors(db_type=db_type, database="/" + "a" * PATH_MAX) == (
        f"Database is longer than {PATH_MAX} characters."
    )


@pytest.mark.parametrize("db_type", ["postgresql", "mysql", "", "supabase"])
def test_a_network_engine_database_keeps_the_identifier_bound(db_type):
    assert connector_request_errors(db_type=db_type, database="d" * (IDENT_MAX + 1)) == (
        f"Database is longer than {IDENT_MAX} characters."
    )
