"""Independent destination read-back for route certification.

Reads the destination with the engine's *driver*, never through DataFlow's
readers, writers or reconcile code, so a writer acknowledgment or a Gate-8
verdict cannot vouch for itself. Returns ``None`` for destinations this module
cannot read independently; the cell then stays without
``independent_readback`` and grades ``partial``.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

READBACK_FORMATS = frozenset({"sqlite", "postgresql", "mysql", "mongodb", "sqlserver", "oracle"})


def _connect(destination: Any) -> tuple[Any, str] | None:
    """A native-driver connection to ``destination`` and its identifier quote."""
    fmt = destination.format
    if fmt == "sqlite":
        import sqlite3

        conn = sqlite3.connect(str(destination.database))
        quote = '"'
    elif fmt == "postgresql":
        import psycopg2

        conn = psycopg2.connect(
            host=destination.host,
            port=destination.port,
            dbname=destination.database,
            user=destination.username,
            password=destination.password,
        )
        quote = '"'
    elif fmt == "mysql":
        import pymysql

        conn = pymysql.connect(
            host=destination.host,
            port=int(destination.port),
            database=destination.database,
            user=destination.username,
            password=destination.password,
        )
        quote = "`"
    elif fmt == "sqlserver":
        import pymssql

        conn = pymssql.connect(
            server=destination.host,
            port=str(destination.port),
            database=destination.database,
            user=destination.username,
            password=destination.password,
        )
        quote = '"'
    elif fmt == "oracle":
        import oracledb

        # NUMBER arrives as float by default, which would blame the route
        # for the driver's rounding.
        oracledb.defaults.fetch_decimals = True
        # CLOB values are read after the connection closes; fetch them as text.
        oracledb.defaults.fetch_lobs = False
        conn = oracledb.connect(
            user=destination.username,
            password=destination.password,
            dsn=f"{destination.host}:{destination.port}/{destination.database}",
        )
        quote = '"'
    else:
        return None
    return conn, quote


def execute_sql(destination: Any, statements: list[str]) -> None:
    """Run DDL/DML on the destination with its own driver (fixture setup)."""
    opened = _connect(destination)
    assert opened is not None, f"no native driver for {destination.format}"
    conn, _quote = opened
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
        conn.commit()
    finally:
        conn.close()


def _rows_sql(destination: Any, columns: list[str]) -> list[dict[str, Any]] | None:
    fmt = destination.format
    table = str(destination.table)
    opened = _connect(destination)
    if opened is None:
        return None
    conn, quote = opened
    if fmt == "oracle":
        table, columns = _oracle_names(conn, table, columns)
    try:
        cur = conn.cursor()
        cols = ", ".join(f"{quote}{c}{quote}" for c in columns)
        qualified = f"{quote}{table}{quote}"
        if fmt in {"postgresql", "sqlserver"} and getattr(destination, "schema", ""):
            qualified = f'"{destination.schema}".{qualified}'
        cur.execute(f"SELECT {cols} FROM {qualified}")
        keys = [c.lower() for c in columns]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _oracle_names(conn: Any, table: str, columns: list[str]) -> tuple[str, list[str]]:
    """Oracle folds unquoted names to upper case; read the dictionary spelling."""
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM user_tables WHERE UPPER(table_name) = UPPER(:t)",
        t=table,
    )
    row = cur.fetchone()
    actual = row[0] if row else table
    cur.execute(
        "SELECT column_name FROM user_tab_columns WHERE table_name = :t", t=actual
    )
    by_upper = {name.upper(): name for (name,) in cur.fetchall()}
    return actual, [by_upper.get(c.upper(), c) for c in columns]


def _rows_mongo(destination: Any, columns: list[str], key_field: str) -> list[dict[str, Any]]:
    import pymongo

    client = pymongo.MongoClient(
        host=destination.host or "127.0.0.1",
        port=int(destination.port or 27017),
        serverSelectionTimeoutMS=3000,
        directConnection=True,
    )
    try:
        coll = client[destination.database][destination.table]
        out = []
        for doc in coll.find({}):
            out.append({c: doc.get(key_field if c == columns[0] else c) for c in columns})
        return out
    finally:
        client.close()


def read_back(
    destination: Any, columns: list[str], *, key_field: str = ""
) -> list[dict[str, Any]] | None:
    """Rows at rest in the destination, or ``None`` when not independently readable."""
    if getattr(destination, "kind", "") != "database":
        return None
    if destination.format not in READBACK_FORMATS:
        return None
    if destination.format == "mongodb":
        return _rows_mongo(destination, columns, key_field or columns[0])
    return _rows_sql(destination, columns)


def canonical(value: Any) -> Any:
    """Compare numbers by value and everything else by its text."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return ("t", value.replace(tzinfo=None).isoformat(sep=" "))
    if isinstance(value, date):
        return ("t", datetime(value.year, value.month, value.day).isoformat(sep=" "))  # noqa: DTZ001
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    text = str(value)
    try:
        return ("n", Decimal(text).normalize())
    except (InvalidOperation, ValueError):
        pass
    if len(text) >= 19 and text[4] == "-" and text[10] in " T":
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return ("t", parsed.replace(tzinfo=None).isoformat(sep=" "))
        except ValueError:
            pass
    return ("s", text)


def canonical_rows(rows: list[dict[str, Any]], columns: list[str]) -> list[tuple]:
    return sorted(tuple(canonical(r.get(c)) for c in columns) for r in rows)
