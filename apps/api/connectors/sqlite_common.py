"""Shared helpers for SQLite reader/writer."""

from __future__ import annotations


def sqlite_file_path(database: str, connection_string: str, host: str) -> str:
    """Resolve the filesystem path to a SQLite database.

    Prefers the raw ``database`` path, then ``connection_string``.  If the value
    looks like a SQLAlchemy ``sqlite://`` URL, strip the scheme so ``sqlite3``
    receives a filesystem path.
    """
    path = database or connection_string or host or ""
    if path.startswith("sqlite://"):
        path = path[len("sqlite://"):]
        if path.startswith("//"):
            path = path[1:]  # sqlite:////abs/path -> /abs/path
        elif path.startswith("/"):
            path = path[1:]  # sqlite:///relative -> relative
    return path
