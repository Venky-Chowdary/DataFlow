"""Snowflake writer tests (stub mode without live warehouse)."""

from connectors.snowflake_writer import write_mapped_rows


def test_snowflake_writer_stub():
    result = write_mapped_rows(
        host="xy12345.us-east-1",
        port=443,
        database="ANALYTICS",
        username="user",
        password="pass",
        schema="PUBLIC",
        connection_string="",
        ssl=True,
        warehouse="COMPUTE_WH",
        table_name="df_payments_test",
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
    assert result.driver in {"stub", "snowflake-connector-python"}
