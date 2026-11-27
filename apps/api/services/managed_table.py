"""Whether a destination table was created by Datawrap.

The ownership comment (``datawrap:managed`` or the legacy ``dataflow:managed``)
is the signal. On a table Datawrap created, a Map stamp that only echoes the
live column type is the catalog, not a ceiling the operator drew, so a
same-family source widening is proposed as a widen (DW-D334); it is applied
under the schema policy like any widen (propagate, or manual_review with the
acknowledgement). On anyone else's table the live type stays the ceiling
(D161). A probe that cannot read the comment answers false.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Mapping

_OWNED: ContextVar[Callable[[], bool] | None] = ContextVar(
    "datawrap_destination_owned", default=None
)


@contextmanager
def destination_ownership(resolve: Callable[[], bool]) -> Iterator[None]:
    """Answer :func:`destination_is_owned` with ``resolve()`` (asked once, lazily)."""
    answer: list[bool] = []

    def _once() -> bool:
        if not answer:
            try:
                answer.append(bool(resolve()))
            except Exception:  # noqa: BLE001 - unreadable ownership: not owned
                answer.append(False)
        return answer[0]

    token = _OWNED.set(_once)
    try:
        yield
    finally:
        _OWNED.reset(token)


def destination_is_owned() -> bool:
    """Whether the destination being planned is a table Datawrap created."""
    resolve = _OWNED.get()
    return bool(resolve and resolve())


def with_preflight_destination_ownership(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Run a preflight with its destination's ownership answerable (probed on demand)."""

    @functools.wraps(fn)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        config = kwargs.get("destination_config")

        def _resolve() -> bool:
            return owned_for_evolution(
                str(kwargs.get("destination_db_type") or ""), config,
                schema=str((config or {}).get("schema") or ""),
                table=str(kwargs.get("destination_table") or ""),
                table_exists=kwargs.get("destination_table_exists"),
            )

        with destination_ownership(_resolve):
            return fn(*args, **kwargs)

    return wrapped


def owned_for_evolution(
    db_type: str,
    config: Mapping[str, Any] | None,
    *,
    schema: str = "",
    table: str = "",
    table_exists: bool | None = None,
    dest_recreated: bool = False,
) -> bool:
    """Whether preflight may treat this destination as Datawrap-owned.

    One call so the preflight module does not grow a probe of its own.
    """
    if table_exists is not True or dest_recreated or not config or not str(table or "").strip():
        return False
    try:
        return destination_table_is_managed(
            db_type, config, schema=schema, table=table
        )
    except Exception:
        return False


def destination_table_is_managed(
    db_type: str,
    config: Mapping[str, Any] | None,
    *,
    schema: str = "",
    table: str = "",
) -> bool:
    comment = read_table_comment(db_type, config, schema=schema, table=table)
    if not comment:
        return False
    from services.brand import is_own

    return is_own("managed_table_comment", comment)


def read_table_comment(
    db_type: str,
    config: Mapping[str, Any] | None,
    *,
    schema: str = "",
    table: str = "",
) -> str:
    name = str(table or "").strip()
    if not name or not config:
        return ""
    kind = (db_type or "").strip().lower()
    try:
        if kind in {"postgresql", "postgres"} or kind.startswith("postgres"):
            return _pg_comment(config, schema or str(config.get("schema") or "public"), name)
        if kind in {"mysql", "mariadb", "tidb"}:
            return _mysql_comment(config, name)
    except Exception:
        return ""
    return ""


def _pg_comment(config: Mapping[str, Any], schema: str, table: str) -> str:
    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=str(config.get("host") or ""),
        port=int(config.get("port") or 5432),
        database=str(config.get("database") or config.get("dbname") or ""),
        username=str(config.get("username") or config.get("user") or ""),
        password=str(config.get("password") or ""),
        connection_string=str(config.get("connection_string") or ""),
        ssl=bool(config.get("ssl")),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT obj_description(c.oid) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s",
                (schema or "public", table),
            )
            row = cur.fetchone()
            return str(row[0] or "") if row else ""
    finally:
        conn.close()


def _mysql_comment(config: Mapping[str, Any], table: str) -> str:
    from connectors.mysql_conn import get_connection

    conn = get_connection(
        host=str(config.get("host") or ""),
        port=int(config.get("port") or 3306),
        database=str(config.get("database") or ""),
        username=str(config.get("username") or ""),
        password=str(config.get("password") or ""),
        connection_string=str(config.get("connection_string") or ""),
        ssl=bool(config.get("ssl")),
        purpose="probe",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_comment FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
            row = cur.fetchone()
            return str(row[0] or "") if row else ""
    finally:
        conn.close()
