"""Snowflake connection helper."""

from __future__ import annotations

import os
import sys
from typing import Any


def normalize_account(host: str) -> str:
    host = host.strip()
    if not host:
        return ""
    if ".snowflakecomputing.com" in host:
        return host.split(".snowflakecomputing.com")[0]
    return host


def _is_local_account(account: str) -> bool:
    return account.lower() in ("local", "localhost", "fakesnow")


def _fakesnow_db_path() -> str:
    path = os.environ.get("FAKESNOW_DB_PATH", "/tmp/fakesnow_data")
    os.makedirs(path, exist_ok=True)
    return path


def get_connection(
    *,
    account: str,
    username: str,
    password: str,
    database: str,
    schema: str,
    warehouse: str,
    connection_string: str,
    role: str = "",
) -> Any:
    try:
        import snowflake.connector
    except ImportError as exc:
        from connectors.driver_guard import require_driver
        raise RuntimeError(require_driver("snowflake.connector", "snowflake-connector-python")) from exc

    if connection_string.strip():
        return snowflake.connector.connect(connection_string, login_timeout=10)

    kwargs: dict[str, Any] = {
        "account": normalize_account(account),
        "user": username,
        "password": password,
        "login_timeout": 10,
    }
    if database:
        kwargs["database"] = database
    if schema:
        kwargs["schema"] = schema
    if warehouse:
        kwargs["warehouse"] = warehouse
    if role:
        kwargs["role"] = role

    # Use fakesnow for local/emulator testing; it patches snowflake.connector.connect
    # and persists databases to disk so read-after-write works across connections.
    if _is_local_account(kwargs["account"]):
        import unittest.mock

        import fakesnow

        # fakesnow cannot be nested, so reuse an active patch in this thread.
        already_patched = isinstance(snowflake.connector.connect, unittest.mock.MagicMock)
        patch_cm = None
        if not already_patched:
            patch_cm = fakesnow.patch(
                db_path=_fakesnow_db_path(),
                nop_regexes=[r"^USE WAREHOUSE"],
            )
            patch_cm.__enter__()

        try:
            conn = snowflake.connector.connect(**kwargs)
        except Exception:
            # Ensure the fakesnow patch is undone if connection fails, otherwise
            # every subsequent test that patches the connector will error out.
            if patch_cm is not None:
                patch_cm.__exit__(*sys.exc_info())
            raise
        orig_close = conn.close

        def _close() -> None:
            try:
                orig_close()
            finally:
                if patch_cm is not None:
                    patch_cm.__exit__(None, None, None)

        conn.close = _close  # type: ignore[assignment]
        return conn

    return snowflake.connector.connect(**kwargs)
