"""Shared helpers for SQLite reader/writer."""

from __future__ import annotations

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


def sqlite_file_path(database: str, connection_string: str, host: str) -> str:
    """Resolve the filesystem path to a SQLite database.

    Prefers the raw ``database`` path, then ``connection_string``.  If the value
    looks like a SQLAlchemy ``sqlite://`` URL, strip the scheme so ``sqlite3``
    receives a filesystem path.

    When ``DATAFLOW_SQLITE_ROOT`` is set, resolved paths must stay under that
    directory (blocks ``..`` / absolute escapes outside the allowlist).
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
    root = (getenv_brand("SQLITE_ROOT") or "").strip()
    if not root:
        return path
    try:
        resolved = Path(path).expanduser().resolve(strict=False)
        root_resolved = Path(root).expanduser().resolve(strict=False)
    except OSError as exc:
        raise ValueError(f"Invalid SQLite path: {exc}") from exc
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(
            f"SQLite path must be under DATAFLOW_SQLITE_ROOT ({root_resolved})"
        ) from exc
    return str(resolved)
