"""Shared helpers for SQLite reader/writer."""

from __future__ import annotations

import os
from pathlib import Path


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
    root = (os.environ.get("DATAFLOW_SQLITE_ROOT") or "").strip()
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
