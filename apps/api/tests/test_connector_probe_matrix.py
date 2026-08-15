"""Shared connector probe matrix — fail-closed, no crash, honest humanize.

Live green login for every catalog tile is not claimed here. This proves the
shared Test path: every registry driver returns a tuple, empty secrets fail
closed, humanize is idempotent, and file Test does not claim Connected
without a path.
"""

from __future__ import annotations

import pytest

from connectors.snowflake_conn import SNOWFLAKE_BAD_PASSWORD_MSG
from services.connector_auth import SALESFORCE_PLACEHOLDER_HOST_MSG, validate_probe_auth
from src.transfer.connector_capabilities import file_source_types
from src.transfer.connector_registry import (
    CONNECTOR_MODULES,
    humanize_connection_error,
    probe_file_source,
    run_probe,
)

_DRIVERS = sorted(CONNECTOR_MODULES.keys())
_FILE_TYPES = sorted(file_source_types())
_HUMANIZE_DRIVERS = (
    "postgresql",
    "mysql",
    "mongodb",
    "snowflake",
    "salesforce",
    "s3",
    "bigquery",
    "redis",
)


@pytest.mark.parametrize("driver", _DRIVERS)
def test_run_probe_never_raises(driver: str):
    ok, message = run_probe(
        driver,
        {
            "host": "invalid.example",
            "port": 1,
            "database": "test",
            "username": "u",
            "password": "p",
        },
    )
    assert isinstance(ok, bool)
    assert isinstance(message, str)
    assert message.strip()
    assert "takes 0 positional arguments" not in message
    assert "ModuleNotFoundError" not in message
    assert "No module named" not in message


@pytest.mark.parametrize("driver", _DRIVERS)
def test_empty_user_pass_fails_closed(driver: str):
    err = validate_probe_auth(driver=driver, auth_mode="user_pass")
    assert isinstance(err, str) and err.strip()


@pytest.mark.parametrize("driver", _HUMANIZE_DRIVERS)
def test_humanize_is_idempotent_for_auth_families(driver: str):
    raw = "password authentication failed for user \"app\""
    first = humanize_connection_error(driver, raw)
    second = humanize_connection_error(driver, first)
    assert first == second
    assert first.strip()


@pytest.mark.parametrize("fmt", _FILE_TYPES)
def test_file_source_test_does_not_claim_connected_without_a_path(fmt: str):
    ok, message = probe_file_source(fmt, "")
    assert ok is False
    assert "catalog support is not a successful connection" in message.lower()


def test_file_source_remote_uri_is_not_connected():
    ok, message = probe_file_source("csv", "s3://bucket/data.csv")
    assert ok is False
    assert "does not download" in message.lower()


def test_salesforce_placeholder_host_is_rejected():
    assert validate_probe_auth(
        driver="salesforce",
        auth_mode="api_key",
        host="https://yourorg.my.salesforce.com",
        api_key="00Dxx0000000000",
    ) == SALESFORCE_PLACEHOLDER_HOST_MSG


def test_snowflake_250001_humanize_is_idempotent():
    raw = (
        "250001 (08001): Failed to connect to DB: tmjdswz-kz40681.snowflakecomputing.com:443. "
        "Incorrect username or password was specified."
    )
    first = humanize_connection_error("snowflake", raw)
    second = humanize_connection_error("snowflake", first)
    assert first == SNOWFLAKE_BAD_PASSWORD_MSG
    assert second == first
    assert "refused this login. Snowflake rejected" not in first
