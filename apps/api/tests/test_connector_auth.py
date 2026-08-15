"""Engine login role vs topology — Snowflake must never receive role=BOTH."""

from __future__ import annotations

from connectors.snowflake_conn import (
    SNOWFLAKE_HOST_ONLY_URL_MSG,
    classify_snowflake_connect_error,
    normalize_account,
    parse_snowflake_url,
    snowflake_connect_kwargs,
)
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


def test_parse_browser_host_is_account_only():
    parsed = parse_snowflake_url("https://bq73198.snowflakecomputing.com")
    assert parsed == {"account": "bq73198"}


def test_parse_sqlalchemy_url_keeps_at_in_password():
    parsed = parse_snowflake_url(
        "snowflake://VENKY170259:venkatesh@170259@bq73198/EMPLOYEE_DB/PUBLIC"
        "?warehouse=COMPUTE_WH&role=ACCOUNTADMIN"
    )
    assert parsed["account"] == "bq73198"
    assert parsed["user"] == "VENKY170259"
    assert parsed["password"] == "venkatesh@170259"
    assert parsed["database"] == "EMPLOYEE_DB"
    assert parsed["schema"] == "PUBLIC"
    assert parsed["warehouse"] == "COMPUTE_WH"
    assert parsed["role"] == "ACCOUNTADMIN"


def test_parse_url_encoded_at_in_password():
    parsed = parse_snowflake_url(
        "snowflake://svc:p%40ss%40word@myorg-acct/ANALYTICS/PUBLIC"
    )
    assert parsed["account"] == "myorg-acct"
    assert parsed["user"] == "svc"
    assert parsed["password"] == "p@ss@word"


def test_parse_jdbc_query_params():
    parsed = parse_snowflake_url(
        "jdbc:snowflake://xy12345.us-east-1.snowflakecomputing.com/"
        "?user=SVC&password=secret&db=SALES&schema=PUBLIC&warehouse=COMPUTE_WH&role=SYSADMIN"
    )
    assert parsed["account"] == "xy12345.us-east-1"
    assert parsed["user"] == "SVC"
    assert parsed["password"] == "secret"
    assert parsed["database"] == "SALES"
    assert parsed["schema"] == "PUBLIC"
    assert parsed["warehouse"] == "COMPUTE_WH"
    assert parsed["role"] == "SYSADMIN"


def test_connect_kwargs_never_include_raw_url():
    kwargs = snowflake_connect_kwargs(
        account="account.snowflakecomputing.com",
        username="",
        password="",
        database="",
        schema="PUBLIC",
        warehouse="",
        connection_string=(
            "snowflake://VENKY170259:venkatesh@170259@bq73198/EMPLOYEE_DB/PUBLIC"
            "?warehouse=COMPUTE_WH&role=ACCOUNTADMIN"
        ),
    )
    assert kwargs["account"] == "bq73198"
    assert kwargs["user"] == "VENKY170259"
    assert kwargs["password"] == "venkatesh@170259"
    assert kwargs["database"] == "EMPLOYEE_DB"
    assert kwargs["schema"] == "PUBLIC"
    assert kwargs["warehouse"] == "COMPUTE_WH"
    assert kwargs["role"] == "ACCOUNTADMIN"
    assert "connection_string" not in kwargs


def test_browser_host_url_fails_before_connect():
    try:
        snowflake_connect_kwargs(
            connection_string="https://bq73198.snowflakecomputing.com"
        )
    except ValueError as exc:
        assert str(exc) == SNOWFLAKE_HOST_ONLY_URL_MSG
    else:
        raise AssertionError("expected host-only URL to fail before connect")


def test_get_connection_never_passes_url_positionally(monkeypatch):
    import pytest

    snowflake = pytest.importorskip("snowflake.connector")
    seen: dict = {}

    def fake_connect(*args, **kwargs):
        assert args == ()
        seen.update(kwargs)

        class _Conn:
            def close(self) -> None:
                return None

        return _Conn()

    monkeypatch.setattr(snowflake, "connect", fake_connect)
    from connectors.snowflake_conn import get_connection

    get_connection(
        account="account.snowflakecomputing.com",
        username="",
        password="",
        database="",
        schema="",
        warehouse="",
        connection_string=(
            "snowflake://VENKY170259:venkatesh@170259@bq73198/EMPLOYEE_DB/PUBLIC"
            "?warehouse=COMPUTE_WH&role=ACCOUNTADMIN"
        ),
    )
    assert seen["account"] == "bq73198"
    assert seen["user"] == "VENKY170259"
    assert seen["password"] == "venkatesh@170259"
    assert seen["role"] == "ACCOUNTADMIN"


def test_positional_init_error_is_classified():
    raw = "SnowflakeConnection.__init__() takes 0 positional arguments but 1 was given"
    msg = classify_snowflake_connect_error(raw)
    assert msg == SNOWFLAKE_HOST_ONLY_URL_MSG
    human = humanize_connection_error("snowflake", raw)
    assert "password" not in human.lower()
    assert "account host" in human.lower() or "login" in human.lower()
