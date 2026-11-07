"""One resolver for the live services a test suite may talk to.

Every live matrix used to carry its own copy of "where is Postgres and who am
I", each with a different prefix (``P2_PG_*``, ``P3_PG_*``, ``ITEM1_PG_*``) and
each defaulting to ``postgres``/``admin``. Two things went wrong with that:

* CI's ``api-and-web`` job exports the standard libpq variables (``PGHOST``,
  ``PGUSER``, ``PGPASSWORD``, ``PGDATABASE``) — the spelling every Postgres tool
  on earth understands — which none of those copies read. So the tests
  authenticated as ``postgres``/``admin`` against a server that only knows
  ``dataflow``, and 20+ live cases failed on ``password authentication failed``
  while a perfectly good Postgres was running.
* Liveness was a bare TCP connect. A socket opening proves a server listens, not
  that our credentials work, so "reachable" was declared and the test then died
  at connect time. A live matrix must either run or say honestly that it cannot.

So: one resolution order, and a probe that authenticates.
"""

from __future__ import annotations

import os
import socket
from typing import Any

# Resolution order per field, first non-empty wins:
#   1. the caller's own prefix (P4_PG_USER)   — lets one matrix target its own server
#   2. the shared prefix (P2_PG_USER)         — historical spelling, still honored
#   3. the tool-standard env (PGUSER)         — what CI and psql/pg_dump export
#   4. the local compose default
_PG_STANDARD = {
    "host": "PGHOST",
    "port": "PGPORT",
    "database": "PGDATABASE",
    "username": "PGUSER",
    "password": "PGPASSWORD",
}
_PG_DEFAULTS = {
    "host": "127.0.0.1",
    "port": "5432",
    "database": "postgres",
    "username": "postgres",
    "password": "admin",
}

_MYSQL_STANDARD = {
    "host": "MYSQL_HOST",
    "port": "MYSQL_PORT",
    "database": "MYSQL_DATABASE",
    "username": "MYSQL_USER",
    "password": "MYSQL_PASSWORD",
}
_MYSQL_DEFAULTS = {
    "host": "127.0.0.1",
    "port": "3306",
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
}

_FIELD_SUFFIX = {
    "host": "HOST",
    "port": "PORT",
    "database": "DB",
    "username": "USER",
    "password": "PASSWORD",
}


def _resolve(
    field: str,
    *,
    engine: str,
    prefix: str,
    standard: dict[str, str],
    defaults: dict[str, str],
) -> str:
    suffix = _FIELD_SUFFIX[field]
    names = []
    if prefix:
        names.append(f"{prefix}_{engine}_{suffix}")
    names.append(f"P2_{engine}_{suffix}")
    names.append(standard[field])
    for name in names:
        value = os.environ.get(name, "")
        if value:
            return value
    return defaults[field]


def pg_creds(prefix: str = "") -> dict[str, Any]:
    """Postgres connection fields for a live test, honoring libpq env vars."""
    resolved = {
        field: _resolve(
            field,
            engine="PG",
            prefix=prefix,
            standard=_PG_STANDARD,
            defaults=_PG_DEFAULTS,
        )
        for field in _FIELD_SUFFIX
    }
    resolved["port"] = int(resolved["port"])
    return resolved


def mysql_creds(prefix: str = "") -> dict[str, Any]:
    """MySQL connection fields for a live test, honoring MYSQL_* env vars."""
    resolved = {
        field: _resolve(
            field,
            engine="MYSQL",
            prefix=prefix,
            standard=_MYSQL_STANDARD,
            defaults=_MYSQL_DEFAULTS,
        )
        for field in _FIELD_SUFFIX
    }
    resolved["port"] = int(resolved["port"])
    return resolved


_pg_probe_cache: dict[tuple[str, int, str, str, str], bool] = {}
_mysql_probe_cache: dict[tuple[str, int, str, str, str], bool] = {}


def _cache_key(creds: dict[str, Any]) -> tuple[str, int, str, str, str]:
    return (
        str(creds["host"]),
        int(creds["port"]),
        str(creds["database"]),
        str(creds["username"]),
        str(creds["password"]),
    )


def pg_up(prefix: str = "") -> bool:
    """True only when we can authenticate — a listening port is not enough."""
    creds = pg_creds(prefix)
    key = _cache_key(creds)
    cached = _pg_probe_cache.get(key)
    if cached is not None:
        return cached
    ok = False
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
            connect_timeout=3,
        )
        conn.close()
        ok = True
    except Exception:
        ok = False
    _pg_probe_cache[key] = ok
    return ok


def mysql_up(prefix: str = "") -> bool:
    """True only when we can authenticate against the resolved MySQL."""
    creds = mysql_creds(prefix)
    key = _cache_key(creds)
    cached = _mysql_probe_cache.get(key)
    if cached is not None:
        return cached
    ok = False
    try:
        import pymysql

        conn = pymysql.connect(
            host=creds["host"],
            port=creds["port"],
            user=creds["username"],
            password=creds["password"],
            database=creds["database"],
            connect_timeout=3,
        )
        conn.close()
        ok = True
    except Exception:
        ok = False
    _mysql_probe_cache[key] = ok
    return ok


def mongo_up(uri: str = "") -> bool:
    """Mongo has no credential handshake here, so a socket is the whole probe."""
    target = uri or os.environ.get("P2_MONGO_URI", "mongodb://127.0.0.1:27017")
    host, _, port_text = target.rsplit("/", 1)[-1].partition(":")
    if target.startswith("mongodb://"):
        authority = target[len("mongodb://") :].split("/", 1)[0]
        host, _, port_text = authority.partition(":")
    try:
        with socket.create_connection((host or "127.0.0.1", int(port_text or 27017)), timeout=0.4):
            return True
    except (OSError, ValueError):
        return False
