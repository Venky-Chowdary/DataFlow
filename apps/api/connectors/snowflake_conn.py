"""Snowflake connection helper."""

from __future__ import annotations

import logging
import os
from services.brand_env import getenv_brand
import sys
import threading
import unittest.mock
from typing import Any

# fakesnow patches snowflake.connector.connect globally; keep a process-wide
# refcount so multiple nested get_connection() calls (e.g. count + read) can
# share one patch and the last close tears it down.  This prevents the "already
# patched" leaks that break downstream tests.
_fakesnow_lock = threading.Lock()

logger = logging.getLogger(__name__)
_fakesnow_refcount = 0
_fakesnow_patch_cm: Any | None = None


def _fakesnow_exit_patch() -> None:
    global _fakesnow_refcount, _fakesnow_patch_cm
    with _fakesnow_lock:
        _fakesnow_refcount -= 1
        # Keep the fakesnow mock active for the rest of the process when requested
        # (test suites verify by issuing their own snowflake.connector.connect calls).
        if getenv_brand("FAKESNOW_KEEP_PATCH") == "1":
            if _fakesnow_refcount < 0:
                _fakesnow_refcount = 0
            return
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

    Datawrap historically created quoted lowercase tables via
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
        except Exception as exc:
            logger.debug("Candidate table resolution failed for %r: %s", cand, exc)
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
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)

    return None


def resolve_or_fold_snowflake_table(cur: Any, schema: str, table: str) -> str:
    """Resolve stored table name, or Snowflake-fold for a not-yet-created table."""
    from connectors.sql_identifiers import snowflake_fold_identifier

    found = resolve_snowflake_table_name(cur, schema, table)
    if found:
        return found
    return snowflake_fold_identifier((table or "").strip())


_SF_COLUMN_PROJECTIONS: tuple[str, ...] = (
    (
        "column_name, data_type, is_nullable, character_maximum_length, "
        "numeric_precision, numeric_scale, datetime_precision"
    ),
    (
        "column_name, data_type, is_nullable, character_maximum_length, "
        "numeric_precision, numeric_scale"
    ),
    "column_name, data_type, is_nullable",
)


def snowflake_physical_column_rows(
    cur: Any, schema: str, table: str
) -> list[tuple[Any, ...]]:
    """Physical column metadata as ``(name, type, nullable, len, p, s, dt_p)``.

    Single introspection SSOT for the Snowflake reader, the writer bind overlay
    and destination schema discovery, because they must never disagree about
    what the destination physically holds.

    Catalogs differ in which optional INFORMATION_SCHEMA columns they expose
    (a role without full projection rights, a Snowflake-compatible engine
    without ``DATETIME_PRECISION``). One missing optional column used to fail
    the whole SELECT, so a table whose DDL was perfectly readable reported *no
    physical metadata* and the writer fail-closed on every row. Degrade the
    projection instead, then fall back to ``DESC TABLE`` — which returns the
    fully qualified type text (``NUMBER(38,10)``, ``TIMESTAMP_NTZ(9)``). Every
    rung reads the catalog; nothing here infers a type from data.
    """
    for projection in _SF_COLUMN_PROJECTIONS:
        try:
            cur.execute(
                f"SELECT {projection} FROM information_schema.columns "
                "WHERE UPPER(table_schema) = UPPER(%s) "
                "AND UPPER(table_name) = UPPER(%s) ORDER BY ordinal_position",
                (schema, table),
            )
            rows = [tuple(r) for r in (cur.fetchall() or [])]
        except Exception as exc:
            logger.debug(
                "snowflake information_schema projection failed (%s): %s",
                projection.split(",")[-1].strip(),
                exc,
                exc_info=exc,
            )
            continue
        if rows:
            return [r + (None,) * (7 - len(r)) for r in rows]
    return _snowflake_desc_column_rows(cur, schema, table)


def _snowflake_desc_column_rows(
    cur: Any, schema: str, table: str
) -> list[tuple[Any, ...]]:
    """``DESC TABLE`` rows shaped like the INFORMATION_SCHEMA projection."""
    try:
        cur.execute(f"DESC TABLE {snowflake_qualified_table(schema, table)}")
        rows = list(cur.fetchall() or [])
    except Exception as exc:
        logger.debug("snowflake DESC TABLE failed: %s", exc, exc_info=exc)
        return []
    out: list[tuple[Any, ...]] = []
    for row in rows:
        if len(row) < 2:
            continue
        kind = str(row[2]).upper() if len(row) > 2 and row[2] is not None else "COLUMN"
        if kind != "COLUMN":
            continue
        name = str(row[0] or "")
        ddl = str(row[1] or "").strip()
        if not name or not ddl:
            continue
        nullable = "YES"
        if len(row) > 3 and row[3] is not None:
            nullable = "YES" if str(row[3]).upper().startswith("Y") else "NO"
        # DESC carries the width inside the type text, so the typmod columns
        # stay None rather than being invented as zero.
        out.append((name, ddl, nullable, None, None, None, None))
    return out


def snowflake_qualified_table(schema: str, table: str) -> str:
    """Quote schema.table using the exact stored/folded names (no second fold)."""
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        snowflake_fold_identifier,
    )

    sch = snowflake_fold_identifier((schema or "PUBLIC").strip() or "PUBLIC")
    # ``table`` must already be the resolved information_schema name, or a
    # folded name for a table that does not exist yet.
    return f"{quote_sql_identifier(sch)}.{quote_sql_identifier(table)}"


def _fakesnow_db_path() -> str:
    from services.platform_config import data_dir

    path = os.environ.get("FAKESNOW_DB_PATH") or str(data_dir() / "fakesnow_data")
    os.makedirs(path, exist_ok=True)
    return path


def _is_fakesnow_catalog_error(exc: BaseException) -> bool:
    """DuckDB catalog written by a different duckdb/fakesnow version, or corrupt file.

    Field-id deserialize failures and "not a valid DuckDB database" both block
    local Snowflake emulator routes until the on-disk store is rebuilt.
    """
    msg = str(exc).lower()
    return (
        "serialization error" in msg
        or "failed to deserialize" in msg
        or "field id" in msg
        or "not a valid duckdb" in msg
        or "not a valid database" in msg
    )


def _reset_fakesnow_catalog(db_path: str) -> None:
    """Drop incompatible/corrupt fakesnow DuckDB files so the emulator can recreate them."""
    from pathlib import Path

    root = Path(db_path)
    if not root.exists():
        return
    removed = 0
    for pattern in ("*.db", "*.db.wal", "*.duckdb", "*.duckdb.wal"):
        for path in root.glob(pattern):
            try:
                path.unlink()
                removed += 1
            except OSError as exc:
                logger.warning("Could not remove fakesnow catalog file %s: %s", path, exc)
    if removed:
        logger.warning(
            "Reset fakesnow catalog at %s (%d file(s)) after DuckDB version/corruption error",
            db_path,
            removed,
        )


def _fakesnow_rollback_product_patch() -> None:
    global _fakesnow_refcount, _fakesnow_patch_cm
    with _fakesnow_lock:
        _fakesnow_refcount -= 1
        if _fakesnow_refcount <= 0 and _fakesnow_patch_cm is not None:
            try:
                _fakesnow_patch_cm.__exit__(*sys.exc_info())
            except Exception as exc:
                logger.debug("fakesnow patch exit during rollback: %s", exc)
            _fakesnow_patch_cm = None
            _fakesnow_refcount = 0


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

        db_path = _fakesnow_db_path()
        catalog_retry_done = False

        while True:
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
                            db_path=db_path,
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
            except Exception as exc:
                # If we installed a patch for this connect attempt, roll it back so a
                # failed local connection cannot leak the patch into later tests.
                if product_managed:
                    _fakesnow_rollback_product_patch()
                if (
                    product_managed
                    and not catalog_retry_done
                    and _is_fakesnow_catalog_error(exc)
                ):
                    catalog_retry_done = True
                    _reset_fakesnow_catalog(db_path)
                    continue
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
