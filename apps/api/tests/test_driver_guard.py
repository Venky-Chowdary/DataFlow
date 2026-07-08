"""Writer fail-closed policy when drivers are missing."""

import os

from connectors.mysql_writer import write_mapped_rows


def test_mysql_writer_fails_without_driver_or_stub_flag(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "pymysql":
            raise ImportError("forced missing driver")
        return real_import(name, *args, **kwargs)

    monkeypatch.delenv("DATAFLOW_ALLOW_STUB_WRITES", raising=False)
    monkeypatch.setattr(builtins, "__import__", mock_import)

    result = write_mapped_rows(
        host="localhost",
        port=3306,
        database="app_db",
        username="root",
        password="pass",
        schema="",
        connection_string="",
        ssl=False,
        table_name="df_orders_test",
        headers=["AMT"],
        data_rows=[["1.00"]],
        mappings=[{"source": "AMT", "target": "amount"}],
        column_types={"AMT": "DECIMAL"},
    )
    assert not result.ok
    assert result.driver == "none"
    assert "pymysql" in (result.error or "")
