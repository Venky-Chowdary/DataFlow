from __future__ import annotations

from unittest.mock import patch

from connectors.sqlserver_cdc_native import SqlServerNativeCdc
from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc


CFG = {
    "host": "sql.example",
    "port": 1433,
    "database": "cdc",
    "username": "reader",
    "password": "not-logged",
    "multi_subnet_failover": True,
    "application_intent": "ReadOnly",
}


def _assert_connection_options_reach_get_connection(reader) -> None:
    with patch("connectors.generic_sql.get_connection") as get_connection:
        reader._conn()

    kwargs = get_connection.call_args.kwargs
    assert kwargs["multi_subnet_failover"] is True
    assert kwargs["application_intent"] == "ReadOnly"


def test_native_cdc_forwards_ag_listener_connection_options() -> None:
    reader = SqlServerNativeCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
    )

    _assert_connection_options_reach_get_connection(reader)


def test_change_tracking_forwards_ag_listener_connection_options() -> None:
    reader = SqlServerChangeTrackingCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
    )

    _assert_connection_options_reach_get_connection(reader)
