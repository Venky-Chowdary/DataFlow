"""Snowflake connection helper."""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import unittest.mock
from typing import Any
from urllib.parse import parse_qs, unquote

from services.brand_env import getenv_brand

_SF_HOST_SUFFIXES = (
    ".privatelink.snowflakecomputing.com",
    ".snowflakecomputing.com",
)

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
    """Account locator / org-account for snowflake.connector — not a URL.

    Accepts pasted browser hosts:
    ``https://xy12345.us-east-1.snowflakecomputing.com``,
    ``org-account.privatelink.snowflakecomputing.com:443/console``.
    """
    raw = (host or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"^https?://", "", raw, flags=re.I)
    raw = raw.split("/")[0].split("?")[0]
    if "@" in raw:
        raw = raw.rsplit("@", 1)[-1]
    if raw.count(":") == 1 and not raw.startswith("["):
        host_part, port = raw.rsplit(":", 1)
        if port.isdigit():
            raw = host_part
    raw = raw.strip().rstrip(".")
    lower = raw.lower()
    for suffix in _SF_HOST_SUFFIXES:
        if lower.endswith(suffix):
            return raw[: -len(suffix)]
    return raw


# Operator-facing copy when they paste a browser host instead of a login URL.
SNOWFLAKE_HOST_ONLY_URL_MSG = (
    "That is a Snowflake account host, not a login. "
    "Use snowflake://user:password@account/DATABASE/SCHEMA?warehouse=COMPUTE_WH "
    "or switch to Username & password and enter the account host, user, and password. "
    "If the password contains @, encode it as %40."
)

SNOWFLAKE_MISSING_ACCOUNT_MSG = (
    "Snowflake account is required. Paste snowflake://user:password@account/... "
    "or the account host (org-account or locator.region)."
)

SNOWFLAKE_MISSING_USER_MSG = (
    "Snowflake username is required. A browser URL "
    "(https://….snowflakecomputing.com) is the account host, not a login."
)

SNOWFLAKE_MISSING_SECRET_MSG = (
    "Provide a password or a PKCS#8 private key. Browser account URLs do not "
    "include credentials."
)

# Official driver: 290404 (08001) when POST /session/v1/login-request is 404.
# That is an account-identifier miss — Snowflake never checked the password.
# Preferred identifier is org-account (docs: admin-account-identifier).
SNOWFLAKE_ACCOUNT_NOT_FOUND_MSG = (
    "Snowflake account host was not found (HTTP 404 / 290404). "
    "Username and password were not checked — that host is not a login endpoint. "
    "In Snowsight, open the account menu and copy the account identifier "
    "(preferred: org-account such as myorg-acctname). "
    "Locator-only hosts like xy12345.snowflakecomputing.com return 404 when the "
    "account is not in the default region or the locator is wrong. "
    "You can also use locator.region or locator.region.cloud."
)


def parse_snowflake_url(raw: str) -> dict[str, str]:
    """Parse operator-pasted Snowflake URLs into connector kwargs.

    ``snowflake.connector.connect`` is keyword-only. Passing a SQLAlchemy or
    browser URL as a positional argument raises
    ``SnowflakeConnection.__init__() takes 0 positional arguments but 1 was given``.

    Accepted:
    - ``snowflake://user:pass@account/db/schema?warehouse=&role=``
    - ``snowflake://user:pass@account.snowflakecomputing.com/db/schema?...``
    - ``jdbc:snowflake://account.snowflakecomputing.com/?user=&password=&db=``
    - ``https://account.snowflakecomputing.com`` (account only — caller must supply user/secret)
    - host-only ``account.snowflakecomputing.com``

    The last ``@`` separates account from userinfo so a password may contain ``@``.
    """
    text = (raw or "").strip()
    if not text:
        return {}
    if text.lower().startswith("jdbc:"):
        text = text[5:].lstrip()

    query = ""
    if "?" in text:
        text, query = text.split("?", 1)

    if "://" in text:
        _scheme, rest = text.split("://", 1)
    else:
        rest = text

    userinfo = ""
    if "@" in rest:
        userinfo, rest = rest.rsplit("@", 1)

    path_parts = [part for part in rest.split("/") if part]
    account_raw = unquote(path_parts[0]) if path_parts else ""
    database = unquote(path_parts[1]) if len(path_parts) > 1 else ""
    schema = unquote(path_parts[2]) if len(path_parts) > 2 else ""

    user = ""
    password = ""
    if userinfo:
        if ":" in userinfo:
            user, password = userinfo.split(":", 1)
            user = unquote(user)
            password = unquote(password)
        else:
            user = unquote(userinfo)

    qs = parse_qs(query, keep_blank_values=True)

    def q(*names: str) -> str:
        for name in names:
            for key in (name, name.lower(), name.upper()):
                vals = qs.get(key)
                if vals and str(vals[0]).strip():
                    return unquote(str(vals[0]))
        return ""

    out: dict[str, str] = {}
    account = normalize_account(account_raw or q("account"))
    if account:
        out["account"] = account
    user = user or q("user", "username")
    password = password or q("password", "passwd", "pwd")
    database = database or q("db", "database")
    schema = schema or q("schema")
    warehouse = q("warehouse", "wh")
    role = q("role")
    if user:
        out["user"] = user
    if password:
        out["password"] = password
    if database:
        out["database"] = database
    if schema:
        out["schema"] = schema
    if warehouse:
        out["warehouse"] = warehouse
    if role:
        out["role"] = role
    return out


def snowflake_connect_kwargs(
    *,
    account: str = "",
    username: str = "",
    password: str = "",
    database: str = "",
    schema: str = "",
    warehouse: str = "",
    connection_string: str = "",
    role: str = "",
    private_key: str = "",
    private_key_passphrase: str = "",
) -> dict[str, Any]:
    """Keyword args for ``snowflake.connector.connect`` — never a positional URL.

    URL fields win when present; discrete form fields fill gaps. Topology tokens
    such as ``both`` are dropped from ``role``.
    """
    from services.connector_auth import engine_login_role

    parsed = parse_snowflake_url(connection_string) if (connection_string or "").strip() else {}
    merged_account = parsed.get("account") or account
    merged_user = parsed.get("user") or username
    merged_password = parsed.get("password") or password
    merged_database = parsed.get("database") or database
    merged_schema = parsed.get("schema") or schema
    merged_warehouse = parsed.get("warehouse") or warehouse
    login_role = engine_login_role(parsed.get("role"), role)
    pem = (private_key or "").strip()

    if not normalize_account(merged_account):
        raise ValueError(SNOWFLAKE_MISSING_ACCOUNT_MSG)
    if not (merged_user or "").strip():
        if parsed.get("account") and not parsed.get("user"):
            raise ValueError(SNOWFLAKE_HOST_ONLY_URL_MSG)
        raise ValueError(SNOWFLAKE_MISSING_USER_MSG)
    if not pem and not (merged_password or "").strip():
        if parsed.get("account") and not parsed.get("password"):
            raise ValueError(SNOWFLAKE_HOST_ONLY_URL_MSG)
        raise ValueError(SNOWFLAKE_MISSING_SECRET_MSG)

    kwargs: dict[str, Any] = {
        "account": normalize_account(merged_account),
        "user": merged_user,
        "login_timeout": 10,
    }
    if pem:
        kwargs["private_key"] = load_snowflake_private_key(pem, private_key_passphrase)
    elif merged_password:
        kwargs["password"] = merged_password
    if merged_database:
        kwargs["database"] = merged_database
    if merged_schema:
        kwargs["schema"] = merged_schema
    if merged_warehouse:
        kwargs["warehouse"] = merged_warehouse
    if login_role:
        kwargs["role"] = login_role
    return kwargs


def load_snowflake_private_key(pem: str, passphrase: str = "") -> bytes:
    """PKCS#8 DER bytes for snowflake.connector ``private_key=``."""
    blob = (pem or "").strip().encode("utf-8")
    if not blob:
        raise ValueError("Snowflake private key is empty")
    try:
        from cryptography.hazmat.primitives import serialization
    except ImportError as exc:
        raise RuntimeError(
            "cryptography is required for Snowflake key-pair authentication"
        ) from exc
    password = passphrase.encode("utf-8") if passphrase.strip() else None
    key = serialization.load_pem_private_key(blob, password=password)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def classify_snowflake_connect_error(raw: str) -> str | None:
    """Honest operator copy — do not call an invalid role a bad password."""
    text = (raw or "").lower()
    if not text:
        return None
    # 404 / 290404 / 513 on login-request is a missing account host, not auth.
    # The path contains "login" and used to be humanized as a bad password.
    if re.search(r"290404|\b513\b", text) or (
        "login-request" in text and re.search(r"\b404\b|not found", text)
    ) or re.search(r"verify the account name|account name is correct", text):
        return SNOWFLAKE_ACCOUNT_NOT_FOUND_MSG
    if re.search(r"network policy|not allowed to access|390403|390422", text):
        return (
            "Snowflake blocked this IP (network policy). Allow the DataFlow egress "
            "address or ask your Snowflake admin to update the policy."
        )
    if re.search(
        r"password.{0,40}(not allowed|disabled|not enabled|deprecated)|"
        r"single-factor password|authentication policy|394400|394504|"
        r"authentication_method",
        text,
    ):
        return (
            "Snowflake refused password-only login (authentication policy / MFA "
            "rollout). Use Programmatic access token or Key-pair (JWT). "
            "Password-only Test cannot complete MFA."
        )
    if re.search(r"mfa|duo|ext_auth|390195|394508|multi-factor", text):
        return (
            "Snowflake requires MFA or key-pair for this user. Password-only login "
            "is refused. Use Programmatic access token or Key-pair on Connectors."
        )
    if re.search(r"jwt|private.?key|390144|invalid token", text):
        return (
            "Snowflake key-pair authentication failed. Check the username, account "
            "host, and that the public key is assigned (ALTER USER … SET RSA_PUBLIC_KEY)."
        )
    if re.search(
        r"role .+ (does not exist|not granted|not authorized)|unknown role|"
        r"invalid role|251006|390201|specified in the connect string",
        text,
    ):
        return (
            "Snowflake role is invalid or not granted to this user. Leave Role blank "
            "to use the user's default role, or enter a role the user can assume."
        )
    if re.search(r"warehouse .+ (does not exist|not authorized)|invalid warehouse|000606", text):
        return (
            "Snowflake warehouse is invalid or not granted. Check the warehouse name "
            "and USAGE privilege."
        )
    if re.search(
        r"database .+ (does not exist|not authorized)|schema .+ (does not exist|not authorized)",
        text,
    ):
        return (
            "Snowflake database or schema was not found or is not granted. Check names "
            "and privileges."
        )
    if re.search(
        r"250001|incorrect username|incorrect password|invalid username or password",
        text,
    ):
        return (
            "Snowflake rejected the username or password. Check the account host "
            "(org-account or locator.region), username, and password."
        )
    if re.search(
        r"takes 0 positional arguments|snowflakeconnection\.__init__",
        text,
    ):
        return SNOWFLAKE_HOST_ONLY_URL_MSG
    return None


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


def _snowflake_connector_module() -> Any:
    try:
        import snowflake.connector
    except ImportError as exc:
        from connectors.driver_guard import require_driver
        raise RuntimeError(require_driver("snowflake.connector", "snowflake-connector-python")) from exc
    return snowflake.connector


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
    private_key: str = "",
    private_key_passphrase: str = "",
) -> Any:
    kwargs = snowflake_connect_kwargs(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
        private_key=private_key,
        private_key_passphrase=private_key_passphrase,
    )

    snowflake_connector = _snowflake_connector_module()

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
                already_patched = isinstance(snowflake_connector.connect, unittest.mock.MagicMock)
                connect_mod = getattr(snowflake_connector.connect, "__module__", "") or ""
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
                conn = snowflake_connector.connect(**kwargs)
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

    return snowflake_connector.connect(**kwargs)
