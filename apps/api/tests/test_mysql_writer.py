"""MySQL writer tests (stub mode without live server)."""

import builtins

from connectors.mysql_writer import write_mapped_rows


def test_mysql_writer_stub(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "1")
    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "pymysql":
            raise ImportError("forced stub for test")
        return real_import(name, *args, **kwargs)

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
        headers=["AMT", "CUST_ID"],
        data_rows=[["100.00", "C1"], ["200.00", "C2"]],
        mappings=[
            {"source": "AMT", "target": "payment_amount"},
            {"source": "CUST_ID", "target": "customer_id"},
        ],
        column_types={"AMT": "DECIMAL", "CUST_ID": "TEXT"},
    )
    assert result.ok
    assert result.rows_written == 2
    assert result.driver in {"stub", "pymysql"}
