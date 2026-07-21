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


def resolve_snowflake_table_name(cur: Any, schema: str, table: str) -> str | None:
    """Return the exact ``TABLE_NAME`` as stored, or ``None`` if not visible.

    DataFlow historically created quoted lowercase tables via
    ``sanitize_identifier`` + ``"name"`` quoting (e.g. ``"csvtestfile"``), while
    readers fold unquoted-style names to ``CSVTESTFILE``. Preview then fails with
    ``002003 Object 'DATAFLOW.PUBLIC.CSVTESTFILE' does not exist`` even though the
    lowercase table exists and information_schema can see it.
    """
    from connectors.sql_identifiers import snowflake_fold_identifier

    schema_f = snowflake_fold_identifier((schema or "PUBLIC").strip() or "PUBLIC")
    raw = (table or "").strip()
    if not raw:
        raise ValueError("Snowflake table name is empty")

    candidates: list[str] = []
    for c in (snowflake_fold_identifier(raw), raw, raw.upper(), raw.lower()):
        if c and c not in candidates:
            candidates.append(c)

    for cand in candidates:
        try:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE UPPER(table_schema) = UPPER(%s)
                  AND table_name = %s
                  AND table_type = 'BASE TABLE'
                LIMIT 1
                """,
                (schema_f, cand),
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception:
            continue

    try:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE UPPER(table_schema) = UPPER(%s)
              AND UPPER(table_name) = UPPER(%s)
              AND table_type = 'BASE TABLE'
            LIMIT 1
            """,
            (schema_f, raw),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass

    return None


def resolve_or_fold_snowflake_table(cur: Any, schema: str, table: str) -> str:
    """Resolve stored table name, or Snowflake-fold for a not-yet-created table."""
    from connectors.sql_identifiers import snowflake_fold_identifier

    found = resolve_snowflake_table_name(cur, schema, table)
    if found:
        return found
    return snowflake_fold_identifier((table or "").strip())


def snowflake_qualified_table(schema: str, table: str) -> str:
    """Quote schema.table using the exact stored/folded names (no second fold)."""
    from connectors.sql_identifiers import quote_sql_identifier, snowflake_fold_identifier

    sch = snowflake_fold_identifier((schema or "PUBLIC").strip() or "PUBLIC")
    # ``table`` must already be the resolved information_schema name, or a
    # folded name for a table that does not exist yet.
    return f"{quote_sql_identifier(sch)}.{quote_sql_identifier(table)}"


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
            connect_mod = getattr(snowflake.connector.connect, "__module__", "") or ""
            if not already_patched and connect_mod.startswith("fakesnow"):
                already_patched = True
            if _fakesnow_refcount > 0:
                # Product already owns the active patch; just share it.
                _fakesnow_refcount += 1
                product_managed = True
            elif not already_patched:
                # No existing patch — install one and own it.
                try:
                    _fakesnow_patch_cm = fakesnow.patch(
                        db_path=_fakesnow_db_path(),
                        nop_regexes=[r"^USE WAREHOUSE"],
                    )
                    _fakesnow_patch_cm.__enter__()
                    _fakesnow_refcount = 1
                    product_managed = True
                except (AssertionError, RuntimeError) as exc:
                    # Nested fakesnow.patch() raises when a test already patched.
                    if "already patched" not in str(exc).lower():
                        raise
                    product_managed = False
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
