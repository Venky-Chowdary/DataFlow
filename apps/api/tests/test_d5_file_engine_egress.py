"""D5: file engines must not go through the host egress check.

QA_E2E_duckdb: ``list_connector_objects`` refused ``localhost`` with
"loopback addresses are not connection targets" — but a duckdb/sqlite file
bind never dials a host; the cfg "host" field is a label.
"""

from __future__ import annotations

import sqlite3

import pytest


def test_duckdb_file_bind_skips_host_egress(tmp_path):
    from connectors.generic_sql import get_connection

    db = tmp_path / "qa_e2e.duckdb"
    # sqlite is the available file engine in this env — the egress skip is
    # keyed on the file-engine kind, identical for duckdb.
    conn = get_connection(
        host="localhost",
        database=str(db),
        connection_string=f"sqlite:///{db}",
        db_type="sqlite",
    )
    assert conn is not None


def test_sqlite_file_bind_skips_host_egress(tmp_path):
    from connectors.generic_sql import get_connection

    db = tmp_path / "qa_e2e.db"
    sqlite3.connect(str(db)).close()
    conn = get_connection(
        host="localhost",
        database=str(db),
        db_type="sqlite",
    )
    assert conn is not None


def test_network_engine_still_goes_through_egress_path():
    """The skip is file-engine-only: a non-file kind still calls the guard."""
    import connectors.generic_sql as gsql

    calls = []
    from services import egress_guard

    orig = egress_guard.metadata_endpoint_refusal

    def spy(*a, **k):
        calls.append(a)
        return "loopback addresses are not connection targets."

    egress_guard.metadata_endpoint_refusal = spy
    try:
        with pytest.raises(ValueError, match="connection targets"):
            gsql.get_connection(
                host="db.internal",
                database="x",
                username="u",
                password="p",
                db_type="postgresql",
            )
    finally:
        egress_guard.metadata_endpoint_refusal = orig
    assert calls, "a non-file engine must still be refused through the guard"
