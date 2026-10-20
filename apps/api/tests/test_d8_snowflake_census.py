"""D8: Snowflake destinations get a key-census driver.

Culprit keys could never be named on Snowflake because the census connection
factory had no snowflake branch — the engine fell through to None and the
report carried "no key-census driver".
"""

from unittest.mock import MagicMock, patch

from src.transfer.reconcile_step import _culprit_census_conn, _culprit_table_ref


def test_snowflake_census_conn_constructs_with_endpoint_config():
    cfg = {
        "account": "xy12345.us-east-1",
        "username": "qa",
        "password": "pw",
        "database": "DATAFLOW",
        "schema": "STAGE",
        "warehouse": "COMPUTE_WH",
        "role": "QA_ROLE",
    }
    fake = MagicMock(name="snowflake_conn")
    with patch(
        "connectors.snowflake_conn.get_connection", return_value=fake
    ) as get_conn:
        eng, conn = _culprit_census_conn("snowflake", cfg)
    assert eng == "snowflake"
    assert conn is fake
    kwargs = get_conn.call_args.kwargs
    assert kwargs["account"] == "xy12345.us-east-1"
    assert kwargs["database"] == "DATAFLOW"
    assert kwargs["schema"] == "STAGE"
    assert kwargs["warehouse"] == "COMPUTE_WH"
    assert kwargs["role"] == "QA_ROLE"


def test_snowflake_census_defaults_schema_public():
    cfg = {"account": "a", "username": "u", "password": "p", "database": "D"}
    with patch("connectors.snowflake_conn.get_connection", return_value=MagicMock()) as get_conn:
        eng, _ = _culprit_census_conn("snowflake", cfg)
    assert eng == "snowflake"
    assert get_conn.call_args.kwargs["schema"] == "PUBLIC"


def test_snowflake_table_ref_is_qualified():
    ref = _culprit_table_ref(
        "snowflake", {"database": "D", "schema": "S"}, "ORDERS"
    )
    assert "ORDERS" in ref
    assert "S" in ref  # schema-qualified so the census reads the right table
