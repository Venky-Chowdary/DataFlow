"""Independent destination readers and 100K seeders for the scale harness.

The proof rule this module exists to enforce: a transfer's own
``records_transferred`` is the writer talking about itself. Every number the
matrix reports for a destination is read back here, on a driver connection the
engine never touched, as ``COUNT(*)`` plus a content checksum over the mapped
projection. A route that acknowledges 100,000 writes into a destination holding
99,998 rows must read as a failure, and it only can if the count comes from the
destination.

Checksums are normalized in Python rather than delegated to each engine's own
digest function, because the point is to compare *across* engines: PostgreSQL
``md5(string_agg(...))``, MySQL ``MD5(GROUP_CONCAT(...))`` and a Mongo
aggregation do not agree on decimal rendering, null spelling or sort collation,
so an engine-side digest can only ever prove a route against itself.
"""

from __future__ import annotations

import hashlib
import os
import socket
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable, Sequence

PG = dict(host="localhost", port=5432, database="dataflow",
          user="dataflow", password="dataflow")
MYSQL = dict(host="localhost", port=3306, database="dataflow",
             user="dataflow", password="dataflow")
MONGO_URI = "mongodb://localhost:27017/?directConnection=true"
MONGO_DB = "dataflow"

#: Rows per route. Track D's bar is 100,000; lower it only to iterate locally,
#: and any published number must name the row count it was measured at.
SCALE_ROWS = int(os.getenv("DATAFLOW_SCALE_ROWS", "100000"))
#: Deterministic seed clock — cursor watermarks must be reproducible per row.
SEED_EPOCH = datetime(2024, 1, 1, tzinfo=UTC)

#: Rows changed on the source after the snapshot, to be carried by the log
#: reader (CDC) or the second run (batch modes).
CHANGE_ROWS = int(os.getenv("DATAFLOW_SCALE_CHANGE_ROWS", "2000"))


def enabled() -> bool:
    return os.getenv("DATAFLOW_SCALE_MODES", "").strip().lower() in {"1", "true", "yes"}


def reachable(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


# --------------------------------------------------------------------------
# value normalization
# --------------------------------------------------------------------------

def norm(value: Any) -> str:
    """Render one cell so the same logical value hashes equally everywhere.

    Decimal / float collapse to a fixed 4-place decimal string: PostgreSQL
    ``NUMERIC(12,2)`` returns ``Decimal('12.30')``, MySQL the same value as
    ``Decimal('12.3')`` depending on driver settings, and Mongo as a float.
    Without this, an identical payload produces three different checksums and
    the harness reports content drift that does not exist.
    """
    if value is None:
        return "\x00"
    if isinstance(value, bool):
        return "1" if value else "0"
    if hasattr(value, "to_decimal"):  # bson.Decimal128
        value = value.to_decimal()
    if isinstance(value, (Decimal, float)):
        return f"{value:.4f}"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        # An aware value is an instant: compare it in UTC, never by dropping the
        # offset, or a +05:30 source would "match" a UTC destination.
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.replace(tzinfo=None, microsecond=0).isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


def checksum(rows: Iterable[Sequence[Any]]) -> str:
    """SHA-256 over ``|``-joined normalized cells, one row per line."""
    digest = hashlib.sha256()
    for row in rows:
        digest.update("|".join(norm(cell) for cell in row).encode())
        digest.update(b"\n")
    return digest.hexdigest()


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------

def pg_connect():
    import psycopg2

    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    return conn


def pg_exec(sql: str, params: tuple | None = None) -> None:
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    finally:
        conn.close()


def pg_fetch(sql: str, params: tuple | None = None) -> list[tuple]:
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


def pg_count(table: str, where: str = "") -> int:
    clause = f" WHERE {where}" if where else ""
    return int(pg_fetch(f'SELECT count(*) FROM public."{table}"{clause}')[0][0])


def pg_columns(table: str) -> set[str]:
    return {
        str(r[0]) for r in pg_fetch(
            """
            SELECT a.attname FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            """,
            (table,),
        )
    }


def pg_projection(table: str, cols: Sequence[str], *, order: str = "id",
                  where: str = "") -> list[tuple]:
    select = ", ".join(f'"{c}"' for c in cols)
    clause = f" WHERE {where}" if where else ""
    return pg_fetch(
        f'SELECT {select} FROM public."{table}"{clause} ORDER BY "{order}"'
    )


def pg_drop(table: str) -> None:
    try:
        pg_exec(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
    except Exception as exc:  # noqa: BLE001 — must not mask a cell result
        print(f"    pg drop {table} skipped: {exc}")


def seed_pg_scale(table: str, rows: int = SCALE_ROWS, *, start: int = 1,
                  tz_aware: bool = False) -> None:
    """``generate_series`` seed: 100K rows in well under a second.

    ``tz_aware`` makes ``updated_at`` a ``TIMESTAMPTZ``. A document store's date
    is an instant, so a zoneless SQL timestamp has no honest BSON value and the
    writer refuses it rather than inventing UTC; a route into MongoDB is seeded
    from the column type that actually carries an instant.
    """
    stamp = "TIMESTAMPTZ" if tz_aware else "TIMESTAMP"
    pg_exec(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
    pg_exec(
        f"""
        CREATE TABLE public."{table}" (
          id BIGINT PRIMARY KEY,
          region TEXT NOT NULL,
          amount NUMERIC(12,2) NOT NULL,
          note TEXT,
          updated_at {stamp} NOT NULL
        )
        """
    )
    pg_append_scale(table, rows, start=start, tz_aware=tz_aware)


def pg_append_scale(table: str, rows: int, *, start: int = 1,
                    tz_aware: bool = False) -> None:
    literal = (
        "TIMESTAMPTZ '2024-01-01 00:00:00+00'"
        if tz_aware
        else "TIMESTAMP '2024-01-01 00:00:00'"
    )
    pg_exec(
        f"""
        INSERT INTO public."{table}" (id, region, amount, note, updated_at)
        SELECT g,
               'r' || (g %% 7),
               (g %% 100000)::numeric / 100,
               CASE WHEN g %% 500 = 0 THEN NULL ELSE 'note ' || g END,
               {literal} + (g || ' seconds')::interval
        FROM generate_series(%s, %s) g
        """,
        (start, start + rows - 1),
    )


# --------------------------------------------------------------------------
# MySQL
# --------------------------------------------------------------------------

def mysql_connect(user: str = "dataflow", password: str = "dataflow"):
    import pymysql

    return pymysql.connect(
        host=MYSQL["host"], port=int(MYSQL["port"]), user=user, password=password,
        database=MYSQL["database"], autocommit=True,
    )


def mysql_exec(sql: str, params: tuple | None = None) -> None:
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    finally:
        conn.close()


def mysql_fetch(sql: str, params: tuple | None = None) -> list[tuple]:
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


def mysql_count(table: str, where: str = "") -> int:
    clause = f" WHERE {where}" if where else ""
    return int(mysql_fetch(f"SELECT count(*) FROM `{table}`{clause}")[0][0])


def mysql_columns(table: str) -> set[str]:
    return {str(r[0]) for r in mysql_fetch(f"SHOW COLUMNS FROM `{table}`")}


def mysql_projection(table: str, cols: Sequence[str], *, order: str = "id",
                     where: str = "") -> list[tuple]:
    select = ", ".join(f"`{c}`" for c in cols)
    clause = f" WHERE {where}" if where else ""
    return mysql_fetch(f"SELECT {select} FROM `{table}`{clause} ORDER BY `{order}`")


def mysql_drop(table: str) -> None:
    try:
        mysql_exec(f"DROP TABLE IF EXISTS `{table}`")
    except Exception as exc:  # noqa: BLE001 — must not mask a cell result
        print(f"    mysql drop {table} skipped: {exc}")


def seed_mysql_scale(table: str, rows: int = SCALE_ROWS, *, start: int = 1) -> None:
    """Recursive-CTE seed. MySQL caps recursion at 1000 by default, so raise
    ``cte_max_recursion_depth`` on the session rather than issuing 100K
    round-trip INSERTs (which takes minutes and dominates the measurement)."""
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"""
                CREATE TABLE `{table}` (
                  id BIGINT PRIMARY KEY,
                  region VARCHAR(16) NOT NULL,
                  amount DECIMAL(12,2) NOT NULL,
                  note VARCHAR(64) NULL,
                  updated_at DATETIME NOT NULL
                )
                """
            )
            _mysql_bulk_insert(cur, table, rows, start)
    finally:
        conn.close()


def mysql_append_scale(table: str, rows: int, *, start: int = 1) -> None:
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            _mysql_bulk_insert(cur, table, rows, start)
    finally:
        conn.close()


def _mysql_bulk_insert(cur: Any, table: str, rows: int, start: int) -> None:
    cur.execute("SET SESSION cte_max_recursion_depth = 2000000")
    cur.execute(
        f"""
        INSERT INTO `{table}` (id, region, amount, note, updated_at)
        WITH RECURSIVE seq(n) AS (
          SELECT %s UNION ALL SELECT n + 1 FROM seq WHERE n < %s
        )
        SELECT n,
               CONCAT('r', n %% 7),
               (n %% 100000) / 100,
               CASE WHEN n %% 500 = 0 THEN NULL ELSE CONCAT('note ', n) END,
               DATE_ADD('2024-01-01 00:00:00', INTERVAL n SECOND)
        FROM seq
        """,
        (start, start + rows - 1),
    )


# --------------------------------------------------------------------------
# MongoDB
# --------------------------------------------------------------------------

def mongo_client():
    from pymongo import MongoClient

    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)


def mongo_replica_set() -> str:
    client = mongo_client()
    try:
        return str(client.admin.command("hello").get("setName") or "")
    finally:
        client.close()


def mongo_count(collection: str, query: dict | None = None) -> int:
    client = mongo_client()
    try:
        return int(client[MONGO_DB][collection].count_documents(query or {}))
    finally:
        client.close()


def mongo_projection(collection: str, cols: Sequence[str], *,
                     order: str = "id") -> list[tuple]:
    client = mongo_client()
    try:
        cursor = client[MONGO_DB][collection].find(
            {}, {c: 1 for c in cols} | {"_id": 0}
        ).sort(order, 1)
        return [tuple(doc.get(c) for c in cols) for doc in cursor]
    finally:
        client.close()


def mongo_drop(collection: str) -> None:
    try:
        client = mongo_client()
        try:
            client[MONGO_DB][collection].drop()
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001 — must not mask a cell result
        print(f"    mongo drop {collection} skipped: {exc}")


def seed_mongo_scale(collection: str, rows: int = SCALE_ROWS, *,
                     start: int = 1) -> None:
    client = mongo_client()
    try:
        coll = client[MONGO_DB][collection]
        coll.drop()
        batch: list[dict[str, Any]] = []
        for i in range(start, start + rows):
            batch.append(mongo_doc(i))
            if len(batch) >= 10000:
                coll.insert_many(batch, ordered=False)
                batch = []
        if batch:
            coll.insert_many(batch, ordered=False)
        coll.create_index("id", unique=True)
        enable_mongo_pre_images(collection, client=client)
    finally:
        client.close()


def enable_mongo_pre_images(collection: str, *, client: Any = None) -> bool:
    """Turn on change-stream pre-images so deletes carry the business key.

    A Mongo delete event publishes ``documentKey`` (``_id``) only, so a pipeline
    keyed on ``id`` cannot name the deleted row without a pre-image. This is the
    operator-side ``collMod`` the connector's refusal asks for; it is applied
    here so the delete-capture cells measure the configured route.
    """
    own = client is None
    client = client or mongo_client()
    try:
        client[MONGO_DB].command(
            "collMod",
            collection,
            changeStreamPreAndPostImages={"enabled": True},
        )
        return True
    except Exception as exc:  # noqa: BLE001 — reported, never silently assumed
        print(f"    mongo pre-images not enabled on {collection}: {exc}")
        return False
    finally:
        if own:
            client.close()


def mongo_doc(i: int) -> dict[str, Any]:
    return {
        "id": i,
        "region": f"r{i % 7}",
        "amount": round((i % 100000) / 100, 2),
        "note": None if i % 500 == 0 else f"note {i}",
        "updated_at": SEED_EPOCH + timedelta(seconds=i),
    }


# --------------------------------------------------------------------------
# SQLite (destination for routes whose source is the log, not the table)
# --------------------------------------------------------------------------

def sqlite_count(path: str, table: str) -> int:
    import sqlite3

    con = sqlite3.connect(path)
    try:
        return int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
    finally:
        con.close()


def sqlite_projection(path: str, table: str, cols: Sequence[str], *,
                      order: str = "id") -> list[tuple]:
    import sqlite3

    con = sqlite3.connect(path)
    try:
        select = ", ".join(f'"{c}"' for c in cols)
        return list(
            con.execute(f'SELECT {select} FROM "{table}" ORDER BY "{order}"')
        )
    finally:
        con.close()
