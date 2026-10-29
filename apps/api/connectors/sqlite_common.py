"""Shared helpers for SQLite reader/writer."""

from __future__ import annotations

import os
import sqlite3
from decimal import Decimal
from pathlib import Path

from services.brand_env import getenv_brand
from services.local_file_policy import (
    audit_would_confine,
    confine_local_files,
    implicit_root,
    is_within,
    volume_root,
)


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


def _explicit_root(kind: str, confine: bool) -> str:
    """The operator-set root for ``kind`` (``sqlite`` | ``duckdb``), or ``""``.

    ``DATAFLOW_DUCKDB_ROOT`` is DuckDB's own. ``DATAFLOW_SQLITE_ROOT`` also covers
    DuckDB only when ``confine`` (``DATAFLOW_CONFINE_LOCAL_FILES``) says so.
    """
    if kind == "duckdb":
        own = (getenv_brand("DUCKDB_ROOT") or "").strip()
        if own or not confine:
            return own
    return (getenv_brand("SQLITE_ROOT") or "").strip()


def _runtime_root(kind: str) -> tuple[str, bool]:
    explicit = _explicit_root(kind, confine_local_files())
    if explicit:
        return explicit, True
    root = implicit_root(kind)
    return (str(root), False) if root else ("", False)


def sqlite_root() -> tuple[str, bool]:
    """``(root, explicit)`` — where SQLite files must live, or ``("", False)``.

    ``DATAFLOW_SQLITE_ROOT`` may list several directories separated by
    ``os.pathsep`` (a relative path is anchored at the first).

    ``DATAFLOW_SQLITE_ROOT`` wins. With ``DATAFLOW_CONFINE_LOCAL_FILES`` on,
    production otherwise confines SQLite to a directory on the persistent data
    volume: a relative name used to land in the container's working directory
    and ``/tmp`` in its temp, so a destination file (and the tables DataFlow
    created in it) vanished on the next restart (item 54). With the flag off
    (the default) and no explicit root, any path is kept; see
    :mod:`services.local_file_policy`.
    """
    return _runtime_root("sqlite")


def duckdb_root() -> tuple[str, bool]:
    """``(root, explicit)`` for local DuckDB files, like :func:`sqlite_root`.

    ``DATAFLOW_DUCKDB_ROOT`` wins. With ``DATAFLOW_CONFINE_LOCAL_FILES`` on,
    ``DATAFLOW_SQLITE_ROOT`` then applies to DuckDB too and production
    otherwise confines it to ``<data_dir>/duckdb``. With the flag off (the
    default, as before) DuckDB is unconfined unless ``DATAFLOW_DUCKDB_ROOT`` is set.
    """
    return _runtime_root("duckdb")


def proposed_root(kind: str) -> tuple[str, bool]:
    """The root ``DATAFLOW_CONFINE_LOCAL_FILES=1`` would enforce in production.

    Pure (creates nothing): the audit script's reference for "inside the root".
    """
    explicit = _explicit_root(kind, True)
    if explicit:
        return explicit, True
    return str(volume_root(kind)), False


def _confine_embedded_path(
    path: str, root: str, explicit: bool, *, label: str, env_name: str, volume_subdir: str
) -> str:
    """Resolve an embedded-database file path and keep it under ``root``.

    Symlinks and ``..`` are followed before the check; a relative path is
    anchored at the root. The service's own state directory
    (``platform_config.data_dir()``) is refused, except its ``sqlite`` /
    ``duckdb`` volume sub-directory, so a connector can never open the job
    store, ledgers or secrets (always on). Without a root any other path is
    returned unchanged; in production with ``DATAFLOW_CONFINE_LOCAL_FILES`` off,
    one outside the volume sub-directory is first recorded as
    ``local_file.would_confine``.
    """
    from services.platform_config import data_dir

    try:
        roots = [
            Path(part).expanduser().resolve(strict=False)
            for part in (root or "").split(os.pathsep)
            if part.strip()
        ]
        candidate = Path(path).expanduser()
        if roots and not candidate.is_absolute():
            candidate = roots[0] / candidate
        resolved = candidate.resolve(strict=False)
        state_dir = data_dir().resolve(strict=False)
    except OSError as exc:
        raise ValueError(f"Invalid {label} path: {exc}") from exc
    if roots and not any(is_within(resolved, r) for r in roots):
        shown = os.pathsep.join(str(r) for r in roots)
        if explicit:
            raise ValueError(f"{label} path must be under {env_name} ({shown})")
        raise ValueError(
            f"{label} path must be under {shown}, on the persistent data "
            "volume: a file anywhere else is lost when the service restarts. "
            f"Use a path under it, or set {env_name} to another mounted volume."
        )
    volume_dir = state_dir / volume_subdir
    if is_within(resolved, state_dir) and not is_within(resolved, volume_dir):
        raise ValueError(
            f"{label} path is inside the Datawrap service data directory and cannot be "
            f"opened as a connector; place the file under {volume_dir} "
            f"or set {env_name}"
        )
    if not roots:
        audit_would_confine(volume_subdir, resolved)
        return path
    return str(resolved)


def confine_sqlite_path(path: str) -> str:
    """Confine a plain SQLite file path (no URL scheme) like :func:`sqlite_file_path`."""
    if not path or path == ":memory:":
        return path
    if "\x00" in path:
        raise ValueError("Invalid SQLite path")
    root, explicit = sqlite_root()
    return _confine_embedded_path(
        path, root, explicit,
        label="SQLite", env_name="DATAFLOW_SQLITE_ROOT", volume_subdir="sqlite",
    )


def confine_duckdb_path(path: str) -> str:
    """Confine a plain DuckDB file path; ``md:`` / ``:memory:`` pass through."""
    lowered = (path or "").lower()
    if not path or path == ":memory:" or lowered.startswith(("md:", "motherduck:")):
        return path
    if "\x00" in path:
        raise ValueError("Invalid DuckDB path")
    root, explicit = duckdb_root()
    return _confine_embedded_path(
        path, root, explicit,
        label="DuckDB", env_name="DATAFLOW_DUCKDB_ROOT", volume_subdir="duckdb",
    )


def sqlite_unconfined_path(database: str, connection_string: str) -> str:
    """The SQLite file named by ``database`` / ``connection_string``, before any root.

    Prefers the raw ``database`` path, then ``connection_string``.  If the value
    looks like a SQLAlchemy ``sqlite://`` URL, strip the scheme so ``sqlite3``
    receives a filesystem path. ``""`` and ``":memory:"`` pass through.
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
    return path


def sqlite_file_path(database: str, connection_string: str, host: str) -> str:
    """Resolve the filesystem path to a SQLite database.

    Resolved paths must stay under :func:`sqlite_root` when there is one
    (blocks ``..`` / absolute escapes outside the allowlist); a relative path
    is anchored at that root.
    """
    return confine_sqlite_path(sqlite_unconfined_path(database, connection_string))


def duckdb_unconfined_path(database: str, connection_string: str, host: str = "") -> str:
    """The DuckDB file (or ``:memory:`` / ``md:`` token) named by the endpoint fields.

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
    return raw


def duckdb_file_path(database: str, connection_string: str, host: str = "") -> str:
    """Resolve a filesystem path (or ``:memory:``) from DuckDB endpoint fields."""
    return confine_duckdb_path(duckdb_unconfined_path(database, connection_string, host))
