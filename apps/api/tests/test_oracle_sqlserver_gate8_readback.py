"""Gate-8 independent read-back: Oracle verify + SQL Server/Oracle samples."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_verify_target_routes_oracle_to_verify_oracle_table():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_oracle_table",
        return_value=(2, "abc"),
    ) as mock_v:
        count, chk = verify_target(
            "oracle",
            {
                "host": "db.example",
                "port": 1521,
                "database": "ORCL",
                "username": "app",
                "password": "x",
                "connection_string": "",
            },
            schema="APP",
            table_name="ORDERS",
            fallback_rows=0,
            fallback_checksum="",
            dest_types={"NOTE": "VARCHAR2(100)"},
        )
    assert count == 2
    assert chk == "abc"
    mock_v.assert_called_once()
    assert mock_v.call_args.kwargs["table_name"] == "ORDERS"
    assert mock_v.call_args.kwargs["dest_types"] == {"NOTE": "VARCHAR2(100)"}


def test_verify_target_generic_sql_oracle_url_routes():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_oracle_table",
        return_value=(1, "chk"),
    ) as mock_v:
        count, chk = verify_target(
            "generic_sql",
            {"connection_string": "oracle+oracledb://u:p@h:1521/?service_name=X"},
            schema="",
            table_name="T",
            fallback_rows=99,
            fallback_checksum="fallback",
        )
    assert (count, chk) == (1, "chk")
    mock_v.assert_called_once()


def test_oracle_checksum_equates_empty_string_and_null():
    """Write-location fingerprints: Oracle NULL read-back matches source ''."""
    from services.reconciliation import canonical_checksum_from_iter

    src = [("1", ""), ("2", "hi")]
    dest = [("1", None), ("2", "hi")]
    cols = ["id", "note"]
    assert canonical_checksum_from_iter(
        src, cols, dest_db_type="oracle", dest_types={"note": "VARCHAR2(50)"}
    ) == canonical_checksum_from_iter(
        dest, cols, dest_db_type="oracle", dest_types={"note": "VARCHAR2(50)"}
    )
    # Postgres must keep them distinct — different checksums.
    assert canonical_checksum_from_iter(
        src, cols, dest_db_type="postgresql"
    ) != canonical_checksum_from_iter(dest, cols, dest_db_type="postgresql")


def test_read_target_sample_sqlserver_keyed(monkeypatch):
    pymssql = pytest.importorskip("pymssql")

    from services import reconciliation as rec

    rows = [(1, "a"), (2, "b")]
    cur = MagicMock()
    cur.description = [("id",), ("name",)]
    cur.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cur

    monkeypatch.setattr(pymssql, "connect", lambda **_kw: conn)

    out = rec.read_target_sample(
        "sqlserver",
        {
            "host": "127.0.0.1",
            "port": 1433,
            "database": "dataflow",
            "username": "sa",
            "password": "x",
        },
        schema="dbo",
        table_name="items",
        columns=["id", "name"],
        limit=10,
        sort_key="id",
        key_values=[1, 2],
    )
    assert out == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    sql = cur.execute.call_args[0][0]
    assert "TOP (10)" in sql
    assert "IN" in sql.upper()


def test_read_target_sample_oracle_fetch_first():
    from services import reconciliation as rec

    result = MagicMock()
    result.keys.return_value = ["id", "note"]
    result.fetchall.return_value = [(1, None), (2, "x")]

    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = SimpleNamespace(
        execute=MagicMock(return_value=result)
    )
    conn_cm.__exit__.return_value = None
    engine = SimpleNamespace(connect=MagicMock(return_value=conn_cm))

    with patch(
        "connectors.generic_sql.get_sqlalchemy_engine",
        return_value=engine,
    ):
        out = rec.read_target_sample(
            "oracle",
            {
                "host": "ora",
                "port": 1521,
                "database": "ORCL",
                "username": "app",
                "password": "x",
            },
            schema="APP",
            table_name="NOTES",
            columns=["id", "note"],
            limit=5,
            sort_key="id",
            key_values=[1, 2],
        )
    assert out == [{"id": 1, "note": None}, {"id": 2, "note": "x"}]
    sql = conn_cm.__enter__.return_value.execute.call_args[0][0].text
    assert "FETCH FIRST" in sql.upper()
