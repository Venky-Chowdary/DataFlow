"""Engine login role vs topology — Snowflake must never receive role=BOTH."""

from __future__ import annotations

from connectors.snowflake_conn import classify_snowflake_connect_error, normalize_account
from services.connector_auth import engine_login_role
from services.connector_probe import probe_cfg_from_saved
from services.connector_store import SavedConnector
from src.transfer.connector_registry import humanize_connection_error


def test_engine_login_role_drops_topology_tokens():
    assert engine_login_role("both") == ""
    assert engine_login_role("source", "destination") == ""
    assert engine_login_role("both", "ACCOUNTADMIN") == "ACCOUNTADMIN"
    assert engine_login_role("", "SYSADMIN") == "SYSADMIN"
    assert engine_login_role(None, "  ") == ""


def test_probe_cfg_from_saved_does_not_send_topology_as_snowflake_role():
    conn = SavedConnector(
        id="sf1",
        name="Snowflake",
        type="snowflake",
        role="both",
        host="xy12345.us-east-1.snowflakecomputing.com",
        port=443,
        database="ANALYTICS",
        username="svc_dataflow",
        password="correct-password",
        warehouse="COMPUTE_WH",
        auth_role="",
    )
    cfg = probe_cfg_from_saved(conn)
    assert cfg["role"] == ""
    assert cfg["auth_role"] == ""
    assert cfg["password"] == "correct-password"
    assert cfg["warehouse"] == "COMPUTE_WH"


def test_probe_cfg_from_saved_keeps_real_snowflake_role():
    conn = SavedConnector(
        id="sf2",
        name="Snowflake",
        type="snowflake",
        role="both",
        host="myorg-acct",
        username="svc",
        password="x",
        auth_role="SYSADMIN",
    )
    cfg = probe_cfg_from_saved(conn)
    assert cfg["role"] == "SYSADMIN"
    assert cfg["auth_role"] == "SYSADMIN"


def test_normalize_account_strips_url_and_privatelink():
    assert normalize_account("https://xy12345.us-east-1.snowflakecomputing.com") == "xy12345.us-east-1"
    assert normalize_account("xy12345.us-east-1.snowflakecomputing.com:443/console") == "xy12345.us-east-1"
    assert normalize_account("myorg-acct.privatelink.snowflakecomputing.com") == "myorg-acct"
    assert normalize_account("myorg-acct") == "myorg-acct"
    assert normalize_account("") == ""


def test_invalid_role_is_not_called_a_bad_password():
    raw = (
        "251005: Role 'BOTH' specified in the connect string is not granted to this user. "
        "Contact your local system administrator, or attempt to login with another role, e.g. PUBLIC."
    )
    msg = humanize_connection_error("snowflake", raw)
    assert "password" not in msg.lower()
    assert "role" in msg.lower()
    assert classify_snowflake_connect_error(raw)


def test_mfa_is_honest_not_wrong_password():
    raw = "390195: Multi-factor authentication is required for this user."
    msg = humanize_connection_error("snowflake", raw)
    assert "mfa" in msg.lower() or "key-pair" in msg.lower()


def test_real_bad_password_stays_auth():
    raw = "250001 (08001): Failed to connect to DB. Incorrect username or password was specified."
    msg = humanize_connection_error("snowflake", raw)
    assert "username or password" in msg.lower() or "rejected" in msg.lower()
