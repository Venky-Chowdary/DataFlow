"""Shared helpers for SQLite reader/writer."""

from __future__ import annotations

import os
import sqlite3
from decimal import Decimal
from pathlib import Path

from services.brand_env import getenv_brand


def sqlite_decimal_bind_text(value: Decimal) -> str:
    """SQLite has no Decimal affinity — bind dest-canonical text.

    Same policy as ``sqlite_writer._to_sqlite_value``: exact scale-preserving
    text, never IEEE float, never silent NULL for non-finite values.
    """
    from services.value_serializer import safe_decimal_text

    if not isinstance(value, Decimal):
        raise TypeError(f"sqlite_decimal_bind_text expects Decimal, got {type(value)!r}")
    text = safe_decimal_text(value)
    if text is None:
        raise ValueError(
            f"SQLite refused non-finite Decimal {value!r} "
            "(refuse silent NULL / float invent)"
        )
    return text


def register_sqlite_decimal_adapter() -> None:
    """Teach sqlite3 (and SQLAlchemy-sqlite) to bind Decimal as dest-canonical text.

    ``apply_transform(..., "decimal")`` returns Decimal. SQLAlchemy SCD2 inserts
    skip ``_to_sqlite_value`` and would otherwise raise ProgrammingError or,
    if the dialect coerced, invent IEEE float.
    """
    sqlite3.register_adapter(Decimal, sqlite_decimal_bind_text)


register_sqlite_decimal_adapter()


SQLITE_BUSY_TIMEOUT_MS = 30_000


def tune_sqlite_connection(dbapi_conn: sqlite3.Connection) -> None:
    """Configure a SQLite connection for a concurrent reader + writer.

    SCD2/mirror stream a staging SELECT on one connection while merging into
    the target on another, and both live in the same database file. Under
    the default rollback journal the open reader blocks the writer's commit
    and sqlite3 raises ``database is locked`` after its 5s default. WAL lets
    the writer commit beside the reader; the busy timeout covers the brief
    checkpoint/DDL contention that remains.
    """
    cur = dbapi_conn.cursor()
    try:
        cur.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        try:
            cur.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            # Read-only / network filesystems refuse WAL; the timeout still applies.
            pass
    finally:
        cur.close()


def require_sqlite_source_file(path: str) -> None:
    """Refuse a source open that would create an empty database.

    ``sqlite3.connect`` creates a missing file. A worker on another instance
    then saw ``/tmp/QA_RT5.db`` as 0 tables and the original data was gone
    (R3-H3). Destination writers may still create a new file.
    """
    if not path or path == ":memory:":
        return
    if Path(path).is_file():
        return
    raise ValueError(
        f"SQLite file {path} is not on this server. "
        "A path under /tmp exists only on the instance that created it. "
        "Put the database on storage this server can read and point the connector there."
    )


def _configured_roots(*env_names: str) -> list[Path]:
    """Allowlisted directories from the first non-empty ``env_names`` value.

    Several roots may be listed, separated by ``os.pathsep``.
    """
    for name in env_names:
        raw = (getenv_brand(name) or "").strip()
        if raw:
            return [
                Path(part).expanduser().resolve(strict=False)
                for part in raw.split(os.pathsep)
                if part.strip()
            ]
    return []


def _is_within(child: Path, parent: Path) -> bool:
    child_s = os.path.normcase(str(child))
    parent_s = os.path.normcase(str(parent))
    try:
        return os.path.commonpath([child_s, parent_s]) == parent_s
    except ValueError:  # different drives on Windows
        return False


def _service_state_dirs() -> list[Path]:
    from services import platform_config

    return [platform_config.data_dir().resolve(strict=False)]


def confine_local_database_path(
    path: str, *, label: str, root_envs: tuple[str, ...]
) -> str:
    """Refuse an embedded-database file path the workspace must not open.

    Connector ``database`` fields are operator input, so the API process never
    opens an arbitrary host path for them:

    * the service's own state directory (``platform_config.data_dir()``) is
      always refused;
    * when one of ``root_envs`` is set, the resolved path (symlinks followed)
      must sit under one of its ``os.pathsep``-separated roots;
    * when none is set, production refuses every file path (fail closed) and
      development keeps the raw path for local UX.
    """
    if not path or path == ":memory:":
        return path
    if "\x00" in path:
        raise ValueError(f"Invalid {label} path")
    try:
        resolved = Path(path).expanduser().resolve(strict=False)
        roots = _configured_roots(*root_envs)
        state_dirs = _service_state_dirs()
    except OSError as exc:
        raise ValueError(f"Invalid {label} path: {exc}") from exc
    env_hint = f"DATAFLOW_{root_envs[0]}"
    for state_dir in state_dirs:
        if _is_within(resolved, state_dir):
            raise ValueError(
                f"{label} path is inside the Datawrap service data directory and cannot be "
                f"opened as a connector; place the file under {env_hint}"
            )
    if roots:
        if not any(_is_within(resolved, root) for root in roots):
            shown = os.pathsep.join(str(r) for r in roots)
            raise ValueError(f"{label} path must be under {env_hint} ({shown})")
        return str(resolved)
    from services.platform_config import is_production

    if is_production():
        raise ValueError(
            f"{label} file connectors are disabled until {env_hint} is set to the "
            "directory that holds workspace database files"
        )
    return path


def confine_sqlite_path(path: str) -> str:
    return confine_local_database_path(path, label="SQLite", root_envs=("SQLITE_ROOT",))


def confine_duckdb_path(path: str) -> str:
    """DuckDB uses ``DATAFLOW_DUCKDB_ROOT``, falling back to ``DATAFLOW_SQLITE_ROOT``."""
    lowered = (path or "").lower()
    if lowered.startswith(("md:", "motherduck:")):
        return path
    return confine_local_database_path(
        path, label="DuckDB", root_envs=("DUCKDB_ROOT", "SQLITE_ROOT")
    )


def sqlite_file_path(database: str, connection_string: str, host: str) -> str:
    """Resolve the filesystem path to a SQLite database.

    Prefers the raw ``database`` path, then ``connection_string``.  If the value
    looks like a SQLAlchemy ``sqlite://`` URL, strip the scheme so ``sqlite3``
    receives a filesystem path.

    The result is confined by :func:`confine_sqlite_path` (``DATAFLOW_SQLITE_ROOT``
    allowlist, service data directory refused, production fails closed).
    """
    path = (database or connection_string or "").strip()
    if not path:
        return ""
    if path == ":memory:" or path.lower().startswith("sqlite://:memory:"):
        return ":memory:"
    if path.startswith("sqlite://"):
        path = path[len("sqlite://"):]
        if path.startswith("//"):
            path = path[1:]  # sqlite:////abs/path -> /abs/path
        elif path.startswith("/"):
            path = path[1:]  # sqlite:///relative -> relative
    if "\x00" in path:
        raise ValueError("Invalid SQLite path")
    return confine_sqlite_path(path)


def duckdb_file_path(database: str, connection_string: str, host: str = "") -> str:
    """Resolve a filesystem path (or ``:memory:``) from DuckDB endpoint fields.

    SQLAlchemy URLs: ``duckdb:///rel``, ``duckdb:////abs``, ``duckdb:///:memory:``.
    MotherDuck ``md:`` / ``motherduck:`` tokens are returned unchanged so callers
    can refuse a local-file open instead of treating the token as a filename.
    """
    raw = (database or connection_string or host or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered.startswith("md:") or lowered.startswith("motherduck:"):
        return raw
    for prefix in ("duckdb:///", "duckdb://", "duckdb:"):
        if lowered.startswith(prefix):
            raw = raw[len(prefix) :]
            lowered = raw.lower()
            break
    if lowered.startswith("//"):
        raw = raw.lstrip("/")
        lowered = raw.lower()
    if not raw or raw == ":memory:" or lowered.endswith(":memory:"):
        return ":memory:" if raw else ""
    if "\x00" in raw:
        raise ValueError("Invalid DuckDB path")
    return confine_duckdb_path(raw)
