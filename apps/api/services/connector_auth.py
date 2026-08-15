"""Engine login role vs topology role — never send source/destination/both to a driver."""

from __future__ import annotations

from typing import Any, Mapping

# SavedConnector.role is inventory topology, not a warehouse/login role.
TOPOLOGY_ROLES = frozenset({"source", "destination", "both", "src", "dest", "any"})

_PATH_BASED = frozenset({"sqlite", "duckdb"})
_NO_PORT = frozenset({
    "bigquery", "snowflake", "s3", "dynamodb", "gcs", "adls", "elasticsearch",
})
_NO_USERPASS = frozenset({
    "sqlite", "duckdb", "bigquery", "s3", "dynamodb", "gcs", "adls",
})
_KNOWN_AUTH_MODES = frozenset({
    "user_pass",
    "connection_string",
    "file_path",
    "service_account",
    "api_key",
    "aws_keys",
    "key_pair",
    "pat",
})


SALESFORCE_PLACEHOLDER_HOST_MSG = (
    "That Salesforce host is a form placeholder, not your org. "
    "Paste your My Domain instance URL (https://<org>.my.salesforce.com) — "
    "not yourorg.my.salesforce.com and not login.salesforce.com."
)

_SALESFORCE_PLACEHOLDER_HOSTS = frozenset({
    "yourorg.my.salesforce.com",
    "login.salesforce.com",
})


def _salesforce_placeholder_host(host: str) -> str | None:
    raw = (host or "").strip().lower().rstrip("/")
    raw = raw.removeprefix("https://").removeprefix("http://")
    if raw in _SALESFORCE_PLACEHOLDER_HOSTS:
        return SALESFORCE_PLACEHOLDER_HOST_MSG
    return None


def _snowflake_placeholder_host(host: str) -> str | None:
    from connectors.snowflake_conn import (
        SNOWFLAKE_PLACEHOLDER_HOST_MSG,
        is_placeholder_snowflake_account,
    )

    if is_placeholder_snowflake_account(host):
        return SNOWFLAKE_PLACEHOLDER_HOST_MSG
    return None


def infer_auth_mode(
    *,
    auth_mode: str = "",
    connection_string: str = "",
    service_account: str = "",
    api_key: str = "",
    username: str = "",
    password: str = "",
    private_key: str = "",
    driver: str = "",
) -> str:
    """Resolve the operator auth mode. Never invent a mode the driver cannot use."""
    mode = (auth_mode or "").strip().lower()
    if mode:
        return mode
    if (private_key or "").strip() and (driver or "").lower() in ("snowflake", "sftp"):
        return "key_pair"
    if (connection_string or "").strip():
        return "connection_string"
    if (service_account or "").strip():
        return "service_account"
    if (api_key or "").strip():
        return "api_key"
    if (username or "").strip() or (password or "").strip():
        return "user_pass"
    return "user_pass"


def validate_probe_auth(
    *,
    driver: str,
    auth_mode: str = "",
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    service_account: str = "",
    api_key: str = "",
    private_key: str = "",
) -> str | None:
    """Fail-fast required fields per auth mode. Return operator copy or None.

    ``pat`` and ``key_pair`` must not fall through — an empty token/key used to
    reach the driver and surface as a cryptic TypeError or bad-password guess.
    """
    driver = (driver or "").strip().lower()
    mode = infer_auth_mode(
        auth_mode=auth_mode,
        connection_string=connection_string,
        service_account=service_account,
        api_key=api_key,
        username=username,
        password=password,
        private_key=private_key,
        driver=driver,
    )
    if mode not in _KNOWN_AUTH_MODES:
        return f"Unknown authentication mode '{mode}'."

    host = (host or "").strip()
    database = (database or "").strip()
    username = (username or "").strip()
    password = password or ""
    connection_string = (connection_string or "").strip()
    service_account = (service_account or "").strip()
    api_key = (api_key or "").strip()
    private_key = (private_key or "").strip()

    if mode in ("connection_string", "file_path"):
        if not connection_string:
            return "Connection string is required."
        return None

    if mode == "service_account":
        if not service_account:
            return "Service account JSON or file path is required."
        if not database:
            return "Project / bucket / database is required for service account authentication."
        return None

    if mode == "api_key":
        if not api_key:
            return "API key is required."
        if not host:
            return "Host is required for API key authentication."
        if driver == "salesforce":
            placeholder = _salesforce_placeholder_host(host)
            if placeholder:
                return placeholder
        return None

    if mode == "aws_keys":
        if not host and not database:
            return "Region / endpoint and bucket / table are required for AWS authentication."
        if not username or not password.strip():
            return "Access key ID and secret access key are required for AWS authentication."
        return None

    if mode == "key_pair":
        if driver == "snowflake":
            placeholder = _snowflake_placeholder_host(host)
            if placeholder:
                return placeholder
            if not host:
                return "Account host is required for Snowflake key-pair."
            if not username or not private_key:
                return "Username and PKCS#8 private key are required for Snowflake key-pair."
            return None
        if driver == "sftp":
            if not host:
                return "Host is required for SFTP key-pair."
            if not private_key:
                return "Private key is required for SFTP key-pair."
            return None
        return "Key-pair authentication is not supported for this connector."

    if mode == "pat":
        if driver != "snowflake":
            return "Programmatic access tokens are a Snowflake authentication mode."
        placeholder = _snowflake_placeholder_host(host)
        if placeholder:
            return placeholder
        if not host:
            return "Account host is required."
        if not username or not password.strip():
            return "Username and programmatic access token are required."
        return None

    if mode == "user_pass":
        if driver == "snowflake":
            placeholder = _snowflake_placeholder_host(host)
            if placeholder:
                return placeholder
        if driver == "salesforce":
            placeholder = _salesforce_placeholder_host(host)
            if placeholder:
                return placeholder
        path_based = driver in _PATH_BASED
        has_path = bool(host or database)
        if path_based and not has_path:
            return "File path or database name is required for SQLite/DuckDB."
        if not host and not path_based:
            return "Host is required for username & password authentication."
        if not path_based and driver not in _NO_PORT and not int(port or 0):
            return "Port is required for username & password authentication."
        if driver not in _NO_USERPASS:
            if driver == "snowflake" and private_key:
                if not username or not private_key:
                    return "Username and PKCS#8 private key are required for Snowflake key-pair."
                return None
            if not username or not password.strip():
                return "Username and password are required."
        return None

    return f"Unknown authentication mode '{mode}'."


def engine_login_role(*candidates: str | None) -> str:
    """Return the first real engine/warehouse role. Topology tokens are ignored.

    Snowflake / Redshift / Databricks login ``role`` must never fall back to
    SavedConnector.role (``both``). That produces ``Role 'BOTH' is not granted``
    which operators read as a failed password.
    """
    for raw in candidates:
        value = str(raw or "").strip()
        if not value:
            continue
        if value.lower() in TOPOLOGY_ROLES:
            continue
        return value
    return ""


def snowflake_session_kwargs(
    cfg: Mapping[str, Any] | None = None, **overrides: Any
) -> dict[str, Any]:
    """Auth extras every Snowflake connect/read/write/introspect path must pass.

    Key-pair and login role used to stop at Test — Map and transfer then failed
    mid-extract with a password error. One helper so a new call site cannot
    drop ``private_key`` again.
    """
    src: dict[str, Any] = dict(cfg or {})
    src.update({k: v for k, v in overrides.items() if v is not None})
    extra_nested = src.get("extra") if isinstance(src.get("extra"), dict) else {}
    pk = str(src.get("private_key") or extra_nested.get("private_key") or "").strip()
    role = engine_login_role(
        src.get("auth_role"),
        src.get("role"),
        extra_nested.get("auth_role") if extra_nested else None,
        extra_nested.get("role") if extra_nested else None,
    )
    return {"private_key": pk, "role": role}
