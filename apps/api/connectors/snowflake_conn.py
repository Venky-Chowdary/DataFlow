"""Snowflake connection helper."""

from __future__ import annotations

import os
import sys
import threading
import unittest.mock
from typing import Any


# fakesnow patches snowflake.connector.connect globally; keep a process-wide
# refcount so multiple nested get_connection() calls (e.g. count + read) can
# share one patch and the last close tears it down.  This prevents the "already
# patched" leaks that break downstream tests.
_fakesnow_lock = threading.Lock()
_fakesnow_refcount = 0
_fakesnow_patch_cm: Any | None = None


def _fakesnow_exit_patch() -> None:
    global _fakesnow_refcount, _fakesnow_patch_cm
    with _fakesnow_lock:
        _fakesnow_refcount -= 1
        if _fakesnow_refcount <= 0 and _fakesnow_patch_cm is not None:
            _fakesnow_patch_cm.__exit__(None, None, None)
            _fakesnow_patch_cm = None
            _fakesnow_refcount = 0


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
        import fakesnow

        global _fakesnow_refcount, _fakesnow_patch_cm

        product_managed = False
        with _fakesnow_lock:
            already_patched = isinstance(snowflake.connector.connect, unittest.mock.MagicMock)
            if _fakesnow_refcount > 0:
                # Product already owns the active patch; just share it.
                _fakesnow_refcount += 1
                product_managed = True
            elif not already_patched:
                # No existing patch — install one and own it.
                _fakesnow_patch_cm = fakesnow.patch(
                    db_path=_fakesnow_db_path(),
                    nop_regexes=[r"^USE WAREHOUSE"],
                )
                _fakesnow_patch_cm.__enter__()
                _fakesnow_refcount = 1
                product_managed = True
            else:
                # A test/framework already patched the connector; use it but do
                # not manage its lifecycle.
                product_managed = False

        try:
            conn = snowflake.connector.connect(**kwargs)
        except Exception:
            # If we installed a patch for this connect attempt, roll it back so a
            # failed local connection cannot leak the patch into later tests.
            if product_managed:
                with _fakesnow_lock:
                    _fakesnow_refcount -= 1
                    if _fakesnow_refcount <= 0 and _fakesnow_patch_cm is not None:
                        _fakesnow_patch_cm.__exit__(*sys.exc_info())
                        _fakesnow_patch_cm = None
                        _fakesnow_refcount = 0
            raise
        orig_close = conn.close

        def _close() -> None:
            try:
                orig_close()
            finally:
                if product_managed:
                    _fakesnow_exit_patch()

        conn.close = _close  # type: ignore[assignment]
        return conn

    return snowflake.connector.connect(**kwargs)
