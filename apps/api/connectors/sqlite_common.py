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
    confinement_refusal,
    connector_file_areas,
    connector_may_open_upload_path,
    implicit_root,
    is_within,
    upload_refusal,
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

#: Zone-bearing tokens DataFlow declares on SQLite columns it creates. SQLite
#: stores their values as ISO-8601 text.
SQLITE_TEXT_CARRIER_TOKENS = ("TIMESTAMPTZ", "TIMETZ")


def register_sqlite_text_carriers() -> None:
    """Reflect DataFlow's zone-bearing SQLite tokens as text, under their own name.

    SQLAlchemy does not know ``TIMESTAMPTZ`` on SQLite and falls back to the
    NUMERIC affinity, whose Decimal result processor raised "must be real
    number, not str" on the ISO text in every row: every cursor (incremental)
    read out of a table DataFlow created failed (D97). The values are text, so
    they read, bind and compare as text; DDL still spells the declared token.
    """
    try:
        import sqlalchemy as sa
        from sqlalchemy.dialects.sqlite import base as sqlite_base
        from sqlalchemy.ext.compiler import compiles
    except ImportError:  # pragma: no cover - SQLAlchemy is a hard dependency
        return
    for token in SQLITE_TEXT_CARRIER_TOKENS:
        if token in sqlite_base.ischema_names:
            continue
        carrier = type(f"SQLiteTextCarrier_{token}", (sa.types.TEXT,), {})
        compiles(carrier)(lambda _type, _compiler, _token=token, **_kw: _token)
        sqlite_base.ischema_names[token] = carrier


register_sqlite_text_carriers()


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
    (the default outside production) and no explicit root, any path is kept;
    see :mod:`services.local_file_policy`.
    """
    return _runtime_root("sqlite")


def duckdb_root() -> tuple[str, bool]:
    """``(root, explicit)`` for local DuckDB files, like :func:`sqlite_root`.

    ``DATAFLOW_DUCKDB_ROOT`` wins. With ``DATAFLOW_CONFINE_LOCAL_FILES`` on,
    ``DATAFLOW_SQLITE_ROOT`` then applies to DuckDB too and production
    otherwise confines it to ``<data_dir>/duckdb``. With the flag off (the
    default outside production) DuckDB is unconfined unless ``DATAFLOW_DUCKDB_ROOT`` is set.
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
    store, ledgers or secrets (always on). So is the exports tree
    (``services.export_paths.exports_dir``), except the caller's own export
    area (``connector_may_open_export_path``): every workspace's export files
    live there, so a path into another workspace's folder is refused (security
    review, always on), while a SQLite export the platform writes still opens. Without a root any other path is
    returned unchanged; in production with ``DATAFLOW_CONFINE_LOCAL_FILES`` off,
    one outside the volume sub-directory is first recorded as
    ``local_file.would_confine``.
    """
    from services.export_paths import connector_may_open_export_path, exports_dir
    from services.platform_config import data_dir

    try:
        roots = [
            Path(part).expanduser().resolve(strict=False)
            for part in (root or "").split(os.pathsep)
            if part.strip()
        ]
        if roots and not explicit:  # the production default: upload, export and declared areas too
            roots += [area.expanduser().resolve(strict=False) for area in connector_file_areas()]
        candidate = Path(path).expanduser()
        if roots and not candidate.is_absolute():
            candidate = roots[0] / candidate
        resolved = candidate.resolve(strict=False)
        state_dir = data_dir().resolve(strict=False)
        exports = exports_dir(create=False)
    except OSError as exc:
        raise ValueError(f"Invalid {label} path: {exc}") from exc
    # No message names a host path the request did not carry (security follow-up 1).
    if roots and not any(is_within(resolved, r) for r in roots):
        if explicit:
            raise ValueError(f"{label} path must be under {env_name}, the folder the operator set for {label} files")
        raise confinement_refusal(label, env_name)
    volume_dir = state_dir / volume_subdir
    if is_within(resolved, state_dir) and not is_within(resolved, volume_dir):
        raise ValueError(
            f"{label} path is inside the Datawrap service data directory and cannot be "
            f"opened as a connector; place the file in the data folder's {volume_subdir} "
            f"area or set {env_name}"
        )
    remapped = False
    if is_within(resolved, volume_dir):
        # One area per workspace, bound like the exports tree (review round 3, H3).
        from services.embedded_volume import volume_open_path

        opened = volume_open_path(resolved, volume_dir, kind=volume_subdir, label=label)
        remapped, resolved = opened != resolved, opened
    if is_within(resolved, exports) and not connector_may_open_export_path(resolved):
        raise ValueError(
            f"{label} path is inside the Datawrap export folder, outside this "
            "connector's own workspace area, and cannot be opened as a connector; "
            f"place the file elsewhere or set {env_name}"
        )
    if not connector_may_open_upload_path(resolved):
        raise upload_refusal(label)
    if not roots:
        audit_would_confine(volume_subdir, resolved)
        return str(resolved) if remapped else path
    return str(resolved)


# SQLite URI-filename controls (https://sqlite.org/uri.html). With them a
# value names a file other than the one the path checks resolve (``file:``
# is read as a relative name under the working directory, while the driver
# opens the absolute path it carries), or changes how the file is opened
# (``vfs``, ``immutable``, ``nolock``). A connector never needs them, so they
# are refused rather than parsed (security review round 2, C1).
URI_FILENAME_PARAMS = frozenset({"uri", "mode", "vfs", "immutable", "nolock"})


#: DuckDB configuration keys a connector never sets. duckdb-sqlalchemy merges
#: URL query keys into the DuckDB config after ``connect_args``, so with one of
#: these in the URL a connector could switch the playground's
#: ``enable_external_access = false`` back on, allow host paths or load
#: extensions (security review round 3, H7).
_DUCKDB_CONFIG_KEYS = frozenset({"enable_external_access", "lock_configuration"})
_DUCKDB_CONFIG_PREFIXES = ("allowed_", "allow_", "autoinstall", "autoload")


def is_duckdb_config_key(key: object) -> bool:
    """Whether a URL query key is a DuckDB configuration key a connector never sets."""
    name = str(key or "").strip().lower()
    return name in _DUCKDB_CONFIG_KEYS or name.startswith(_DUCKDB_CONFIG_PREFIXES) or "extension" in name


def refuse_duckdb_config_query(query: object) -> None:
    """Refuse DuckDB configuration keys in a URL query (a mapping or ``a=b&c=d`` text)."""
    from urllib.parse import parse_qsl, unquote

    if isinstance(query, str):
        keys = [k for k, _v in parse_qsl(query, keep_blank_values=True)]
    else:
        keys = list(dict(query or {}).keys())
    found = sorted({unquote(str(k)) for k in keys if is_duckdb_config_key(unquote(str(k)))})
    if found:
        raise ValueError(
            "DuckDB configuration options are not accepted in a connector URL "
            f"({', '.join(found)}); Datawrap sets them itself"
        )


def _uri_refusal(label: str) -> ValueError:
    return ValueError(
        f"{label} URI filenames are not accepted for a connector (a value that "
        "starts with 'file:', or a uri, mode, vfs, immutable or nolock URL "
        "parameter); give a plain file path"
    )


def refuse_uri_filename(value: str, *, label: str = "SQLite") -> None:
    """Refuse a database value that SQLite or DuckDB could read as a URI filename."""
    if str(value or "").lstrip().lower().startswith("file:"):
        raise _uri_refusal(label)


def refuse_uri_query(query: object, *, label: str = "SQLite") -> None:
    """Refuse URI-filename parameters in a URL query (a mapping or ``a=b&c=d`` text)."""
    from urllib.parse import parse_qsl

    if isinstance(query, str):
        keys = [k for k, _v in parse_qsl(query, keep_blank_values=True)]
    else:
        keys = list(dict(query or {}).keys())
    if any(str(k).strip().lower() in URI_FILENAME_PARAMS for k in keys):
        raise _uri_refusal(label)


def _refuse_uri_in_path(path: str, *, label: str) -> None:
    refuse_uri_filename(path, label=label)
    if "?" in path:
        refuse_uri_query(path.split("?", 1)[1], label=label)
        if label == "DuckDB":
            refuse_duckdb_config_query(path.split("?", 1)[1])


def sqlite_readonly_uri(path: str) -> str:
    """A read-only SQLite URI for ``path`` whose ``?`` / ``#`` / ``%`` are escaped.

    ``f"file:{path}?mode=ro"`` let a file name carry its own URI parameters,
    or end the path early at ``?`` / ``#`` so SQLite opened a different file
    from the one the caller checked.
    """
    return Path(os.path.abspath(path)).as_uri() + "?mode=ro"


def confine_sqlite_path(path: str) -> str:
    """Confine a plain SQLite file path (no URL scheme) like :func:`sqlite_file_path`."""
    if not path or path == ":memory:":
        return path
    if "\x00" in path:
        raise ValueError("Invalid SQLite path")
    _refuse_uri_in_path(path, label="SQLite")
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
    _refuse_uri_in_path(path, label="DuckDB")
    root, explicit = duckdb_root()
    return _confine_embedded_path(
        path, root, explicit,
        label="DuckDB", env_name="DATAFLOW_DUCKDB_ROOT", volume_subdir="duckdb",
    )


def confine_embedded_url(text: str) -> None:
    """Confine the file a SQLAlchemy ``sqlite`` / ``duckdb`` URL opens; other URLs pass.

    ``file:`` databases and ``uri`` / ``mode`` / ``vfs`` parameters make the
    driver open a file other than ``url.database``, so they are refused
    before the path itself is confined (security review round 2, C1).
    """
    import sqlalchemy as sa

    lowered = text.lower()
    if lowered.startswith("sqlite"):
        confine, label = confine_sqlite_path, "SQLite"
    elif lowered.startswith("duckdb"):
        confine, label = confine_duckdb_path, "DuckDB"
    else:
        return
    try:
        parsed = sa.engine.url.make_url(text)
    except (sa.exc.ArgumentError, ValueError) as exc:
        raise ValueError(f"Invalid embedded database URL: {exc}") from exc
    refuse_uri_query(parsed.query, label=label)
    if label == "DuckDB":
        refuse_duckdb_config_query(parsed.query)
    database = parsed.database or ""
    if database and database != ":memory:":
        confine(database)


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
    _refuse_uri_in_path(path, label="SQLite")
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
    _refuse_uri_in_path(raw, label="DuckDB")
    return raw


def duckdb_file_path(database: str, connection_string: str, host: str = "") -> str:
    """Resolve a filesystem path (or ``:memory:``) from DuckDB endpoint fields."""
    return confine_duckdb_path(duckdb_unconfined_path(database, connection_string, host))
