"""M21 — introspect_connector_schema on SQLite must report declared types.

Repro: ``data_type: ""`` for every column of ``hq1608_w``. The sqlite
introspect path carries ``declared_type``/``native_type`` from
PRAGMA table_info, but the tool layer only looked at ``data_type``/
``column_type`` keys — so the field was always empty.
"""

from __future__ import annotations

import sqlite3

import pytest


def test_introspect_sqlite_reports_declared_types(tmp_path):
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.schema_tools import introspect_connector_schema

    db = tmp_path / "hq1608.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE hq1608_w (c126 TEXT, flag BOOLEAN, amount DECIMAL(8,4))"
    )
    conn.execute("INSERT INTO hq1608_w VALUES ('12.3456', 1, 3.5)")
    conn.commit()
    conn.close()

    saved = create_connector(
        {
            "name": "QA_E2E_sqlite_dst_m21",
            "type": "sqlite",
            "database": str(db),
        }
    )
    try:
        result = introspect_connector_schema(
            connector_id=saved.id, table="hq1608_w"
        )
    finally:
        delete_connector(saved.id)

    assert result.success, result.error
    cols = {c["name"]: c for c in result.output["columns"]}
    # declared DDL from PRAGMA table_info — not "" and not a value guess
    assert cols["c126"]["data_type"].upper() == "TEXT"
    assert cols["flag"]["data_type"].upper() == "BOOLEAN"
    assert cols["amount"]["data_type"].upper() == "DECIMAL(8,4)"
