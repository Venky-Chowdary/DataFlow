"""Source engine identity for the duration of one transfer.

Create-new type invention has to know *which engine the values came from*, not
only the source type name. ``VARCHAR(64)`` means "any code point" on PostgreSQL
and "one code page byte" on SQL Server, so inventing a SQL Server ``VARCHAR``
destination for a PostgreSQL source silently rewrites ``中`` to ``?`` — a live
matrix read-back caught exactly that on ``postgresql->mssql``.

Writers are reached through two call surfaces (batch adapters and the streaming
path) with dozens of per-engine call sites, so the identity is bound once at the
transfer boundary and read by the single invent authority, mirroring
``auto_create_lifecycle.bind_auto_create_job``.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

_SOURCE_ENGINE: ContextVar[str] = ContextVar("df_source_engine", default="")


@contextmanager
def bind_source_engine(source_db_type: str) -> Iterator[None]:
    """Bind the source engine id (``postgresql``, ``mssql``, ``csv``, …)."""
    token = _SOURCE_ENGINE.set(str(source_db_type or "").strip().lower())
    try:
        yield
    finally:
        _SOURCE_ENGINE.reset(token)


def active_source_engine() -> str:
    """Source engine id bound for this transfer, or ``""`` when unknown.

    Empty means "unknown", never "not Unicode": callers must keep their
    conservative default rather than inventing a code-page carrier.
    """
    return _SOURCE_ENGINE.get()
