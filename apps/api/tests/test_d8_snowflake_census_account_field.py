"""D8: the Snowflake key-census connection honours the ``account`` field.

Request-shape and connector configs carry the locator in ``account``; the
census read only ``host`` and connected with an empty account, so culprit
keys could not be named. Same ``account or host`` convention as
``preflight_destination`` / ``dest_precount``.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.transfer.reconcile_step import _culprit_census_conn


@pytest.mark.parametrize(
    ("cfg", "expected"),
    [
        ({"account": "xy12345.us-east-1"}, "xy12345.us-east-1"),
        ({"host": "https://xy12345.us-east-1.snowflakecomputing.com"}, "xy12345.us-east-1"),
        ({"account": "xy12345.us-east-1", "host": "ignored.example"}, "xy12345.us-east-1"),
    ],
)
def test_census_account_comes_from_account_then_host(cfg, expected):
    base = {"username": "qa", "password": "pw", "database": "D", "warehouse": "W"}
    with patch("connectors.snowflake_conn.get_connection", return_value=MagicMock()) as get_conn:
        eng, _conn = _culprit_census_conn("snowflake", {**base, **cfg})
    assert eng == "snowflake"
    assert get_conn.call_args.kwargs["account"] == expected
