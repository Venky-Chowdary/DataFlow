"""Destination introspect must not steal columns from another DB/schema.

Regression: MySQL host had ``users`` in a sibling database while ``railway.users``
did not exist — Studio showed "Existing table detected" + foreign columns.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.transfer.endpoint_intelligence import _attach_db_sample
from src.transfer.models import EndpointConfig


def test_mysql_destination_does_not_cross_database_heal_users():
    """Missing railway.users must stay create-new even if another DB has users."""
    from services.schema_introspect import _introspect_mysql

    cur = MagicMock()
    # 1) list tables in railway (no users)
    # 2) columns for railway.users (exact) → empty
    # 3) columns LOWER match → empty
    # Cross-DB search must NOT run when strict_namespace=True.
    cur.fetchall.side_effect = [
        [("orders",), ("jobs",), ("sessions",), ("events",), ("audit_log",)],
        [],
        [],
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch("connectors.mysql_conn.get_connection", return_value=conn):
        out = _introspect_mysql(
            host="h",
            port=3306,
            database="railway",
            username="u",
            password="p",
            ssl=False,
            table="users",
            strict_namespace=True,
        )

    assert out["ok"] is True
    assert out["columns"] == []
    assert "users" not in [t.lower() for t in out.get("tables") or []]
    # No 4th fetchall = cross-DB search skipped
    assert cur.fetchall.call_count == 3


def test_mysql_source_still_cross_database_recovers():
    from services.schema_introspect import _introspect_mysql

    cur = MagicMock()
    cur.fetchall.side_effect = [
        [("orders",)],  # railway list
        [],  # exact columns miss
        [],  # lower columns miss
        [("other_app", "users")],  # cross-DB hit
        [("id", "int", "NO"), ("email", "varchar(255)", "YES")],
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch("connectors.mysql_conn.get_connection", return_value=conn):
        with patch("services.schema_introspect._refine_columns_by_samples", side_effect=lambda *a, **k: a[1]):
            out = _introspect_mysql(
                host="h",
                port=3306,
                database="railway",
                username="u",
                password="p",
                ssl=False,
                table="users",
                strict_namespace=False,
            )

    assert out["ok"] is True
    assert [c["name"] for c in out["columns"]] == ["id", "email"]
    assert out.get("schema") == "other_app"


def test_attach_db_sample_destination_purpose_passes_strict_namespace():
    out: dict = {
        "kind": "database",
        "format": "mysql",
        "connected": True,
        "objects": [{"name": "orders", "type": "table"}],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "MySQL connected",
    }
    endpoint = EndpointConfig(
        kind="database",
        format="mysql",
        database="railway",
        table="users",
        extra={"introspect_purpose": "destination"},
    )
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "mysql", "database": "railway", "host": "h", "port": 3306},
    ), patch(
        "src.transfer.endpoint_intelligence._introspect_table_schema",
        return_value={},
    ) as intro:
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is False
    assert out["columns"] == []
    assert intro.called
    assert intro.call_args.kwargs.get("strict_namespace") is True
