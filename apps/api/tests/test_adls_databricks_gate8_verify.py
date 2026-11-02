"""Gate-8 verify_target for ADLS + Databricks (independent read-back)."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_verify_target_routes_adls():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_adls_blob",
        return_value=(3, "adls-chk"),
    ) as mock_v:
        count, chk = verify_target(
            "adls",
            {
                "database": "lake",
                "host": "acct.blob.core.windows.net",
                "connection_string": "",
            },
            schema="lake",
            table_name="out/data.json",
            fallback_rows=0,
            fallback_checksum="",
        )
    assert (count, chk) == (3, "adls-chk")
    mock_v.assert_called_once()
    assert mock_v.call_args.kwargs["container"] == "lake"
    assert mock_v.call_args.kwargs["key"] == "out/data.json"


def test_verify_target_routes_azure_blob_alias():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_adls_blob",
        return_value=(1, "x"),
    ) as mock_v:
        verify_target(
            "azure_blob_storage",
            {"database": "c"},
            schema="",
            table_name="k.json",
            fallback_rows=-1,
            fallback_checksum="",
        )
    mock_v.assert_called_once()


def test_verify_target_routes_databricks():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_databricks_table",
        return_value=(10, "dbx"),
    ) as mock_v:
        count, chk = verify_target(
            "databricks",
            {
                "host": "adb.azuredatabricks.net",
                "database": "main",
                "http_path": "/sql/1.0/warehouses/abc",
            },
            schema="default",
            table_name="orders",
            fallback_rows=0,
            fallback_checksum="",
            dest_types={"id": "BIGINT"},
        )
    assert (count, chk) == (10, "dbx")
    assert mock_v.call_args.kwargs["table_name"] == "orders"
    assert mock_v.call_args.kwargs["dest_types"] == {"id": "BIGINT"}


def test_verify_adls_blob_parses_json(monkeypatch):
    """Gate-8 ADLS JSON is the GET stream walk, not download_blob().readall()."""
    import io

    from services.reconciliation import verify_adls_blob

    body = b'[{"id": 1, "n": "a"}, {"id": 2, "n": "b"}]'
    monkeypatch.setattr(
        "services.dest_precount._object_store_list_keys",
        lambda *_a, **_k: ["data.json"],
    )
    monkeypatch.setattr(
        "services.object_streaming.open_object_store_binary",
        lambda *_a, **_k: (io.BytesIO(body), None),
    )
    count, chk = verify_adls_blob(
        container="lake",
        key="data.json",
        target_columns=["id", "n"],
    )
    assert count == 2
    assert chk


def test_verify_databricks_table_sqlalchemy_path():
    from services.reconciliation import verify_databricks_table

    count_result = MagicMock()
    count_result.scalar.return_value = 2
    select_result = MagicMock()
    select_result.keys.return_value = ["id", "amount"]
    select_result.__iter__ = lambda self: iter([(1, "10.0"), (2, "20.5")])

    conn = MagicMock()
    conn.execute.side_effect = [count_result, select_result]
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = None
    engine = SimpleNamespace(connect=MagicMock(return_value=conn_cm))

    with patch(
        "connectors.generic_sql.get_sqlalchemy_engine",
        return_value=engine,
    ):
        count, chk = verify_databricks_table(
            host="adb",
            port=443,
            database="main",
            username="t",
            password="x",
            connection_string="",
            schema="default",
            table_name="t",
            target_columns=["id", "amount"],
        )
    assert count == 2
    assert chk
