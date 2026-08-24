"""Engine login role vs topology — Snowflake must never receive role=BOTH."""

from __future__ import annotations

from connectors.snowflake_conn import (
    SNOWFLAKE_ACCOUNT_NOT_FOUND_MSG,
    SNOWFLAKE_BAD_PASSWORD_MSG,
    SNOWFLAKE_HOST_ONLY_URL_MSG,
    SNOWFLAKE_PLACEHOLDER_HOST_MSG,
    classify_snowflake_connect_error,
    is_placeholder_snowflake_account,
    normalize_account,
    parse_snowflake_url,
    snowflake_connect_kwargs,
)
from services.connector_auth import engine_login_role, infer_auth_mode, validate_probe_auth
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
    raw = (
        "250001 (08001): Failed to connect to DB: tmjdswz-kz40681.snowflakecomputing.com:443. "
        "Incorrect username or password was specified."
    )
    msg = humanize_connection_error("snowflake", raw)
    assert msg == SNOWFLAKE_BAD_PASSWORD_MSG
    assert humanize_connection_error("snowflake", msg) == msg
    assert "snowflake refused this login. snowflake rejected" not in msg.lower()
    assert "250001" in msg


def test_http_404_login_request_is_account_host_not_password():
    """Live bq73198.snowflakecomputing.com returns this — not a bad password."""
    raw = (
        "290404 (08001): 404 Not Found: post "
        "bq73198.snowflakecomputing.com:443/session/v1/login-request"
    )
    classified = classify_snowflake_connect_error(raw)
    assert classified == SNOWFLAKE_ACCOUNT_NOT_FOUND_MSG
    msg = humanize_connection_error("snowflake", raw)
    assert msg == SNOWFLAKE_ACCOUNT_NOT_FOUND_MSG
    assert "authentication failed" not in msg.lower()
    assert "check account name, username, password" not in msg.lower()
    assert "org-account" in msg.lower()


def test_unclassified_snowflake_auth_keeps_raw_driver_text():
    raw = "Authentication token has expired for user VENKY170259"
    msg = humanize_connection_error("snowflake", raw)
    assert "Authentication token has expired" in msg
    assert msg != (
        "Authentication failed. Check account name, username, password, role, "
        "and that the account is active."
    )


def test_password_policy_is_not_wrong_password():
    raw = "394504: PASSWORD authentication is not allowed by the authentication policy."
    msg = humanize_connection_error("snowflake", raw)
    assert "programmatic access token" in msg.lower() or "key-pair" in msg.lower()
    assert "incorrect username" not in msg.lower()


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
    import connectors.snowflake_conn as sc

    seen: dict = {}

    class _FakeMod:
        @staticmethod
        def connect(*args, **kwargs):
            assert args == ()
            seen.update(kwargs)

            class _Conn:
                def close(self) -> None:
                    return None

            return _Conn()

    monkeypatch.setattr(sc, "_snowflake_connector_module", lambda: _FakeMod)
    sc.get_connection(
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


def test_get_connection_rejects_browser_host_before_driver():
    from connectors.snowflake_conn import get_connection

    try:
        get_connection(
            account="",
            username="",
            password="",
            database="",
            schema="",
            warehouse="",
            connection_string="https://bq73198.snowflakecomputing.com",
        )
    except ValueError as exc:
        assert str(exc) == SNOWFLAKE_HOST_ONLY_URL_MSG
    else:
        raise AssertionError("expected host-only URL to fail before the driver loads")


def test_positional_init_error_is_classified():
    raw = "SnowflakeConnection.__init__() takes 0 positional arguments but 1 was given"
    msg = classify_snowflake_connect_error(raw)
    assert msg == SNOWFLAKE_HOST_ONLY_URL_MSG
    human = humanize_connection_error("snowflake", raw)
    assert "authentication failed" not in human.lower()
    assert "incorrect username" not in human.lower()
    assert "account host" in human.lower()


def test_pat_and_key_pair_do_not_fall_through():
    assert validate_probe_auth(
        driver="snowflake",
        auth_mode="pat",
        host="bq73198",
        username="SVC",
        password="",
    ) == "Username and programmatic access token are required."
    assert validate_probe_auth(
        driver="snowflake",
        auth_mode="pat",
        host="bq73198",
        username="SVC",
        password="token-value",
    ) is None
    assert validate_probe_auth(
        driver="postgresql",
        auth_mode="pat",
        host="db.example",
        port=5432,
        username="u",
        password="p",
    ) == "Programmatic access tokens are a Snowflake authentication mode."
    assert validate_probe_auth(
        driver="snowflake",
        auth_mode="key_pair",
        host="bq73198",
        username="SVC",
        private_key="",
    ) == "Username and PKCS#8 private key are required for Snowflake key-pair."
    assert validate_probe_auth(
        driver="snowflake",
        auth_mode="key_pair",
        host="bq73198",
        username="SVC",
        private_key="-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----",
    ) is None


def test_unknown_auth_mode_is_rejected():
    assert validate_probe_auth(
        driver="postgresql",
        auth_mode="oauth_magic",
        host="db.example",
        port=5432,
        username="u",
        password="p",
    ) == "Unknown authentication mode 'oauth_magic'."


def test_placeholder_account_host_is_rejected_before_driver():
    assert is_placeholder_snowflake_account("account.snowflakecomputing.com")
    assert is_placeholder_snowflake_account("https://account.snowflakecomputing.com")
    assert not is_placeholder_snowflake_account("tmjdswz-kz40681")
    assert validate_probe_auth(
        driver="snowflake",
        auth_mode="user_pass",
        host="account.snowflakecomputing.com",
        username="VENKATESH1117",
        password="secret",
    ) == SNOWFLAKE_PLACEHOLDER_HOST_MSG
    try:
        snowflake_connect_kwargs(
            account="account.snowflakecomputing.com",
            username="VENKATESH1117",
            password="secret",
        )
    except ValueError as exc:
        assert str(exc) == SNOWFLAKE_PLACEHOLDER_HOST_MSG
    else:
        raise AssertionError("expected placeholder host to fail before connect")


def test_infer_auth_mode_from_private_key():
    assert infer_auth_mode(private_key="-----BEGIN PRIVATE KEY-----", driver="snowflake") == "key_pair"
    assert infer_auth_mode(connection_string="postgresql://u:p@h/db") == "connection_string"
    assert infer_auth_mode(username="u", password="p") == "user_pass"


def test_user_pass_still_requires_host_and_secret():
    assert validate_probe_auth(
        driver="postgresql",
        auth_mode="user_pass",
        host="",
        port=5432,
        username="u",
        password="p",
    ) == "Host is required for username & password authentication."
    assert validate_probe_auth(
        driver="postgresql",
        auth_mode="user_pass",
        host="db.example",
        port=5432,
        username="u",
        password="p",
    ) is None


def test_snowflake_session_kwargs_threads_key_pair_and_drops_topology():
    from services.connector_auth import snowflake_session_kwargs

    out = snowflake_session_kwargs(
        {
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----",
            "role": "both",
            "auth_role": "SYSADMIN",
        }
    )
    assert out["private_key"].startswith("-----BEGIN PRIVATE KEY-----")
    assert out["role"] == "SYSADMIN"

    nested = snowflake_session_kwargs(
        {"extra": {"private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----"}}
    )
    assert nested["private_key"].startswith("-----BEGIN PRIVATE KEY-----")
    assert snowflake_session_kwargs({"role": "both"})["role"] == ""
