"""Shared connector probe matrix — fail-closed, no crash, honest humanize.

Live green login for every catalog tile is not claimed here. This proves the
shared Test path: every registry driver returns a tuple, empty secrets do not
reach a positional-connect TypeError, and Snowflake 250001 is not re-wrapped
as MFA.
"""

from __future__ import annotations

import pytest

from connectors.snowflake_conn import SNOWFLAKE_BAD_PASSWORD_MSG
from services.connector_auth import validate_probe_auth
from src.transfer.connector_registry import CONNECTOR_MODULES, humanize_connection_error, run_probe

_DRIVERS = sorted(CONNECTOR_MODULES.keys())


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
