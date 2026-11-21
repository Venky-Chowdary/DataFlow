"""Live-engine connection settings shared by the Parity at Scale / CE matrix tests.

Every engine is read from the environment names CI already exports
(``.github/ci-live-engines.vars`` and the ``parity-ce-matrix`` job):

* PostgreSQL: ``DATAFLOW_PG_TEST_DSN``
* MySQL: ``DATAFLOW_TEST_MYSQL_{HOST,PORT,DB,USER,PASSWORD}``
* MariaDB (its own leg): ``DATAFLOW_TEST_MARIADB_{HOST,PORT,DB,USER,PASSWORD}``
* SQL Server: ``DATAFLOW_TEST_MSSQL_{HOST,PORT,DB,USER,PASSWORD}``

``available(engine)`` says whether the engine is configured *and* answers on
its port; a test skips with a message naming the engine otherwise, so the
strict CI skip checker fails a job that should have had it.
"""

from __future__ import annotations

import os
import socket
from typing import Any
from urllib.parse import urlparse


def pg_cfg() -> dict[str, Any]:
    u = urlparse(os.environ.get("DATAFLOW_PG_TEST_DSN")
                 or "postgresql://dataflow:dataflow@127.0.0.1:5432/dataflow")
    return {"host": u.hostname or "127.0.0.1", "port": u.port or 5432,
            "database": (u.path or "/dataflow").lstrip("/"), "username": u.username or "dataflow",
            "password": u.password or ""}


def _env_cfg(prefix: str, default_port: int) -> dict[str, Any] | None:
    host = os.environ.get(f"{prefix}_HOST")
    if not host:
        return None
    return {"host": host, "port": int(os.environ.get(f"{prefix}_PORT") or default_port),
            "database": os.environ.get(f"{prefix}_DB") or "dataflow",
            "username": os.environ.get(f"{prefix}_USER") or "dataflow",
            "password": os.environ.get(f"{prefix}_PASSWORD") or ""}


def cfg(engine: str) -> dict[str, Any] | None:
    if engine == "postgresql":
        return pg_cfg()
    if engine == "mysql":
        return _env_cfg("DATAFLOW_TEST_MYSQL", 3306)
    if engine == "mariadb":
        return _env_cfg("DATAFLOW_TEST_MARIADB", 3306)
    if engine == "sqlserver":
        return _env_cfg("DATAFLOW_TEST_MSSQL", 1433)
    raise ValueError(engine)


def available(engine: str) -> bool:
    c = cfg(engine)
    if not c:
        return False
    try:
        with socket.create_connection((c["host"], int(c["port"])), timeout=1.5):
            return True
    except OSError:
        return False


def connect(engine: str):
    """A DB-API connection (autocommit off) to the configured engine."""
    c = cfg(engine)
    assert c, engine
    if engine == "postgresql":
        import psycopg2

        return psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                                user=c["username"], password=c["password"])
    if engine in {"mysql", "mariadb"}:
        import pymysql

        return pymysql.connect(host=c["host"], port=int(c["port"]), database=c["database"],
                               user=c["username"], password=c["password"], autocommit=False,
                               charset="utf8mb4")
    if engine == "sqlserver":
        import pymssql

        return pymssql.connect(server=c["host"], port=int(c["port"]), database=c["database"],
                               user=c["username"], password=c["password"], autocommit=False)
    raise ValueError(engine)


def writer_kwargs(engine: str, table: str) -> dict[str, Any]:
    """The connection keyword arguments a ``write_mapped_rows`` takes."""
    c = cfg(engine)
    assert c, engine
    schema = {"postgresql": "public", "sqlserver": "dbo"}.get(engine, "")
    return {"host": c["host"], "port": int(c["port"]), "database": c["database"],
            "username": c["username"], "password": c["password"], "schema": schema,
            "connection_string": "", "ssl": False, "table_name": table}


def writer(engine: str):
    if engine == "postgresql":
        from connectors.postgresql_writer import write_mapped_rows
    elif engine in {"mysql", "mariadb"}:
        from connectors.mysql_writer import write_mapped_rows
    elif engine == "sqlserver":
        from connectors.sqlserver_writer import write_mapped_rows
    else:
        raise ValueError(engine)
    return write_mapped_rows
