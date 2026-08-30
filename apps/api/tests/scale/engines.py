"""Independent driver access for the five relational engines in the matrix.

Every number the matrix reports about a destination is read **here**, through a
driver connection the transfer engine never touched: ``COUNT(*)`` and a
row-by-row projection read feeding the fixture checksum. The writer's
acknowledgement is not evidence — a route that reported success while the
destination held fewer rows is exactly the failure this harness exists to catch.

Seeding also lives here so the source side of a route is created by the driver
rather than by the product, keeping the fixture independent of the code under
test.
"""

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
import struct
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from services.brand_env import getenv_brand_str
from src.transfer.models import EndpointConfig

from tests.scale.fixture import (
    COLUMNS_BY_NAME,
    Checksum,
    ddl_for,
    invented_ddl_for,
    rows,
)

BATCH = 5_000


def reachable(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


@dataclass
class Engine:
    """One relational engine, usable as source and as destination."""

    name: str
    label: str

    # ---- connection -----------------------------------------------------
    def available(self) -> tuple[bool, str]:
        raise NotImplementedError

    def connect(self) -> Any:
        raise NotImplementedError

    def endpoint(self, table: str) -> EndpointConfig:
        raise NotImplementedError

    # ---- SQL dialect ----------------------------------------------------
    def quote(self, ident: str) -> str:
        return f'"{self.stored(ident)}"'

    def stored(self, ident: str) -> str:
        """Spelling the catalog stores for ``ident`` on this engine.

        Oracle folds unquoted identifiers to upper case, so an Oracle table
        written by any ordinary tool holds ``ID``, not ``id``. The fixture
        speaks lower case everywhere; each engine translates to its own stored
        spelling so mappings name the columns the product will actually see.
        """
        return ident

    def qualified(self, table: str) -> str:
        return self.quote(table)

    def param(self, index: int, name: str) -> str:
        return "?"

    def bind(self, column: str, value: Any) -> Any:
        return value

    def autocommit_conn(self) -> Any:
        return self.connect()

    # ---- DDL / DML ------------------------------------------------------
    def create_table(
        self,
        table: str,
        columns: Sequence[str],
        *,
        narrow: str = "",
        extra_columns: str = "",
        keyless: bool = False,
        source_types: Mapping[str, str] | None = None,
        source_engine: str = "",
    ) -> None:
        if source_types is not None:
            body = invented_ddl_for(
                self.name,
                columns,
                source_types,
                quote=self.quote,
                keyless=keyless,
                source_engine=source_engine,
            )
        else:
            body = ddl_for(
                self.name, columns, narrow=narrow, quote=self.quote, keyless=keyless
            )
        if extra_columns:
            body = f"{body}, {extra_columns}"
        conn = self.autocommit_conn()
        try:
            cur = conn.cursor()
            self._drop_with(cur, table)
            cur.execute(f"CREATE TABLE {self.qualified(table)} ({body})")  # nosec B608
            self._commit(conn)
        finally:
            conn.close()

    def add_column(self, table: str, ddl: str, fill_sql: str) -> None:
        conn = self.autocommit_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"ALTER TABLE {self.qualified(table)} ADD {ddl}")  # nosec B608
            cur.execute(f"UPDATE {self.qualified(table)} SET {fill_sql}")  # nosec B608
            self._commit(conn)
        finally:
            conn.close()

    def _commit(self, conn: Any) -> None:
        try:
            conn.commit()
        except Exception:  # noqa: BLE001 — autocommit connections have no txn
            pass

    def _drop_with(self, cur: Any, table: str) -> None:
        cur.execute(f"DROP TABLE IF EXISTS {self.qualified(table)}")

    def drop(self, table: str) -> None:
        conn = self.autocommit_conn()
        try:
            cur = conn.cursor()
            try:
                self._drop_with(cur, table)
            except Exception:  # noqa: BLE001 — dropping a missing table is fine
                pass
            self._commit(conn)
        finally:
            conn.close()

    def seed(self, table: str, columns: Sequence[str], count: int, *, offset: int = 0) -> None:
        """Create and bulk-load the fixture through the raw driver."""
        self.create_table(table, columns)
        self.insert(table, columns, count, offset=offset)

    def insert(
        self, table: str, columns: Sequence[str], count: int, *, offset: int = 0
    ) -> None:
        col_sql = ", ".join(self.quote(c) for c in columns)
        params = ", ".join(self.param(i, c) for i, c in enumerate(columns))
        sql = f"INSERT INTO {self.qualified(table)} ({col_sql}) VALUES ({params})"  # nosec B608
        conn = self.connect()
        try:
            cur = conn.cursor()
            self._prepare_bulk(cur, columns)
            batch: list[tuple] = []
            for row in rows(count, columns, offset=offset):
                batch.append(tuple(self.bind(c, row[c]) for c in columns))
                if len(batch) >= BATCH:
                    cur.executemany(sql, batch)
                    batch.clear()
            if batch:
                cur.executemany(sql, batch)
            conn.commit()
        finally:
            conn.close()

    def _prepare_bulk(self, cur: Any, columns: Sequence[str]) -> None:
        return None

    # ---- independent proof ---------------------------------------------
    def count(self, table: str) -> int:
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {self.qualified(table)}")  # nosec B608
            return int(cur.fetchone()[0])
        finally:
            conn.close()

    def select_expr(self, column: str) -> str:
        return self.quote(column)

    def scan(self, table: str, columns: Sequence[str]) -> Iterator[dict[str, Any]]:
        col_sql = ", ".join(self.select_expr(c) for c in columns)
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT {col_sql} FROM {self.qualified(table)}")  # nosec B608
            while True:
                chunk = cur.fetchmany(BATCH)
                if not chunk:
                    break
                for record in chunk:
                    yield dict(zip(columns, [self.read(c, v) for c, v in zip(columns, record)]))
        finally:
            conn.close()

    def read(self, column: str, value: Any) -> Any:
        return value

    def checksum(self, table: str, columns: Sequence[str]) -> Checksum:
        chk = Checksum(columns=list(columns))
        for row in self.scan(table, columns):
            chk.add(row)
        return chk

    def table_types(self, table: str) -> dict[str, str]:
        """Live destination DDL, so 'created' claims are checked not assumed."""
        return {}

    def introspect_types(self, table: str) -> dict[str, str]:
        """Column types as the *product* reads them.

        The dest-exists-compatible destination has to be built from what the
        product sees on the source, not from the catalog spelling this harness
        would write: an Oracle ``DATE`` carries a time-of-day, so the product
        reads it as ``TIMESTAMP(0)`` and a hand-built PostgreSQL ``DATE``
        destination is a real truncation the engine is right to refuse.
        ``services.schema_introspect`` is that owner.
        """
        from services.schema_introspect import introspect_schema

        endpoint = self.endpoint(table)
        payload = introspect_schema(
            endpoint.format,
            host=endpoint.host,
            port=endpoint.port or 0,
            database=endpoint.database,
            username=endpoint.username,
            password=endpoint.password,
            schema=endpoint.schema,
            table=table,
            ssl=endpoint.ssl,
            **dict(endpoint.extra or {}),
        )
        return {
            str(column.get("name")): str(column.get("inferred_type") or "")
            for column in (payload.get("columns") or [])
            if column.get("name") and column.get("inferred_type")
        }


class PostgresEngine(Engine):
    def __init__(self) -> None:
        super().__init__(name="postgresql", label="PostgreSQL 16")
        self.host = getenv_brand_str("PG_HOST", "localhost")
        self.port = int(getenv_brand_str("PG_PORT", "5432"))
        self.database = getenv_brand_str("PG_DATABASE", "dataflow")
        self.user = getenv_brand_str("PG_USER", "dataflow")
        self.password = getenv_brand_str("PG_PASSWORD", "dataflow")

    def available(self) -> tuple[bool, str]:
        if not reachable(self.host, self.port):
            return False, f"PostgreSQL not listening on {self.host}:{self.port}"
        return True, ""

    def connect(self) -> Any:
        import psycopg2

        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def autocommit_conn(self) -> Any:
        conn = self.connect()
        conn.autocommit = True
        return conn

    def qualified(self, table: str) -> str:
        return f'public."{table}"'

    def param(self, index: int, name: str) -> str:
        return "%s"

    def bind(self, column: str, value: Any) -> Any:
        return value

    def read(self, column: str, value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return value

    def endpoint(self, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="postgresql",
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.user,
            password=self.password,
            schema="public",
            table=table,
        )

    def table_types(self, table: str) -> dict[str, str]:
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT column_name, data_type, numeric_precision, numeric_scale, "
                "character_maximum_length, is_nullable FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=%s",
                (table,),
            )
            return {
                r[0]: f"{r[1]}({r[2]},{r[3]})" if r[2] is not None else
                (f"{r[1]}({r[4]})" if r[4] else str(r[1]))
                for r in cur.fetchall()
            }
        finally:
            conn.close()


class MySQLEngine(Engine):
    def __init__(self) -> None:
        super().__init__(name="mysql", label="MySQL 8")
        self.host = getenv_brand_str("MYSQL_HOST", "127.0.0.1")
        self.port = int(getenv_brand_str("MYSQL_PORT", "3306"))
        self.database = getenv_brand_str("MYSQL_DATABASE", "dataflow")
        self.user = getenv_brand_str("MYSQL_USER", "dataflow")
        self.password = getenv_brand_str("MYSQL_PASSWORD", "dataflow")

    def available(self) -> tuple[bool, str]:
        if not reachable(self.host, self.port):
            return False, f"MySQL not listening on {self.host}:{self.port}"
        return True, ""

    def connect(self) -> Any:
        import pymysql

        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
        )

    def autocommit_conn(self) -> Any:
        import pymysql

        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
            autocommit=True,
        )

    def quote(self, ident: str) -> str:
        return f"`{ident}`"

    def param(self, index: int, name: str) -> str:
        return "%s"

    def bind(self, column: str, value: Any) -> Any:
        if isinstance(value, bool):
            return int(value)
        return value

    def endpoint(self, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="mysql",
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.user,
            password=self.password,
            schema=self.database,
            table=table,
        )

    def table_types(self, table: str) -> dict[str, str]:
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT column_name, column_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s",
                (self.database, table),
            )
            return {r[0]: str(r[1]) for r in cur.fetchall()}
        finally:
            conn.close()


def _decode_datetimeoffset(raw: bytes | None) -> datetime | None:
    if raw is None:
        return None
    year, month, day, hour, minute, second, nanos, off_h, off_m = struct.unpack(
        "<6hI2h", raw
    )
    return datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanos // 1000,
        timezone(timedelta(hours=off_h, minutes=off_m)),
    )


class SQLServerEngine(Engine):
    def __init__(self) -> None:
        super().__init__(name="sqlserver", label="SQL Server 2022")
        self.host = getenv_brand_str("SQLSERVER_HOST", "localhost")
        self.port = int(getenv_brand_str("SQLSERVER_PORT", "1433"))
        self.database = getenv_brand_str("SQLSERVER_DATABASE", "dataflow")
        self.user = getenv_brand_str("SQLSERVER_USER", "sa")
        self.password = getenv_brand_str("SQLSERVER_PASSWORD", "DataFlow_CDC_2022!")

    def available(self) -> tuple[bool, str]:
        if not reachable(self.host, self.port):
            return False, f"SQL Server not listening on {self.host}:{self.port}"
        try:
            import pyodbc  # noqa: F401
        except Exception as exc:  # noqa: BLE001
            return False, f"pyodbc unavailable: {exc}"
        try:
            self._ensure_database()
        except Exception as exc:  # noqa: BLE001
            return False, f"SQL Server database {self.database} unavailable: {exc}"
        return True, ""

    def _dsn(self, database: str) -> str:
        return (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={self.host},{self.port};DATABASE={database};"
            f"UID={self.user};PWD={self.password};TrustServerCertificate=yes;"
        )

    def _ensure_database(self) -> None:
        import pyodbc

        conn = pyodbc.connect(self._dsn("master"), autocommit=True, timeout=10)
        try:
            conn.execute(
                f"IF DB_ID('{self.database}') IS NULL CREATE DATABASE [{self.database}]"  # nosec B608
            )
        finally:
            conn.close()

    def connect(self) -> Any:
        import pyodbc

        conn = pyodbc.connect(self._dsn(self.database), timeout=30)
        # pyodbc has no built-in reader for DATETIMEOFFSET (-155); without this
        # converter the offset would come back as raw bytes and any tz proof
        # against SQL Server would be unreadable rather than measured.
        conn.add_output_converter(-155, _decode_datetimeoffset)
        return conn

    def autocommit_conn(self) -> Any:
        import pyodbc

        return pyodbc.connect(self._dsn(self.database), autocommit=True, timeout=30)

    def quote(self, ident: str) -> str:
        return f"[{ident}]"

    def qualified(self, table: str) -> str:
        return f"[dbo].[{table}]"

    def _drop_with(self, cur: Any, table: str) -> None:
        cur.execute(
            f"IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL DROP TABLE [dbo].[{table}]"  # nosec B608
        )

    def _prepare_bulk(self, cur: Any, columns: Sequence[str]) -> None:
        # pyodbc's fast_executemany path has no binding for DATETIMEOFFSET
        # (ODBC type -155), so tables carrying ts_tz seed row-at-a-time.
        cur.fast_executemany = "ts_tz" not in columns

    def bind(self, column: str, value: Any) -> Any:
        if isinstance(value, bool):
            return 1 if value else 0
        if column == "ts_tz" and isinstance(value, datetime):
            # pyodbc binds aware datetimes as SQL_TIMESTAMP and drops the offset;
            # the string form reaches DATETIMEOFFSET with the offset intact.
            return value.isoformat(sep=" ")
        return value

    def read(self, column: str, value: Any) -> Any:
        return value

    def endpoint(self, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="sqlserver",
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.user,
            password=self.password,
            schema="dbo",
            table=table,
            # ODBC Driver 18 encrypts and verifies by default, and the compose
            # container holds a self-signed certificate with no file to pin.
            # Declaring the trust explicitly is the audited exit added to
            # connectors.generic_sql; the driver default stays verify-or-fail.
            extra={"trust_server_certificate": True},
        )

    def table_types(self, table: str) -> dict[str, str]:
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT column_name, data_type, numeric_precision, numeric_scale, "
                "character_maximum_length, is_nullable FROM information_schema.columns "
                "WHERE table_schema='dbo' AND table_name=?",
                table,
            )
            return {
                r[0]: f"{r[1]}({r[2]},{r[3]})" if r[2] is not None else
                (f"{r[1]}({r[4]})" if r[4] else str(r[1]))
                for r in cur.fetchall()
            }
        finally:
            conn.close()


class SQLiteEngine(Engine):
    def __init__(self) -> None:
        super().__init__(name="sqlite", label="SQLite (file)")
        root = getenv_brand_str("SCALE_SQLITE_DIR", "") or "/tmp/dataflow-scale"
        Path(root).mkdir(parents=True, exist_ok=True)
        self.path = str(Path(root) / "scale_matrix.db")

    def available(self) -> tuple[bool, str]:
        return True, ""

    def connect(self) -> Any:
        import sqlite3

        conn = sqlite3.connect(self.path, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def param(self, index: int, name: str) -> str:
        return "?"

    def bind(self, column: str, value: Any) -> Any:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, Decimal):
            # SQLite NUMERIC affinity keeps the text when a binary float would
            # not round-trip, which is what preserves DECIMAL(20,9) here.
            return format(value, "f")
        if isinstance(value, (datetime, date)):
            return _iso(value)
        return value

    def endpoint(self, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="sqlite",
            database=self.path,
            table=table,
        )

    def table_types(self, table: str) -> dict[str, str]:
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(f'PRAGMA table_info("{table}")')
            return {r[1]: f"{r[2]}{'' if r[3] else ' NULL'}" for r in cur.fetchall()}
        finally:
            conn.close()


class OracleEngine(Engine):
    def __init__(self) -> None:
        super().__init__(name="oracle", label="Oracle Free 23ai")
        self.host = getenv_brand_str("ORACLE_HOST", "localhost")
        self.port = int(getenv_brand_str("ORACLE_PORT", "1521"))
        self.service = getenv_brand_str("ORACLE_SERVICE", "FREEPDB1")
        self.user = getenv_brand_str("ORACLE_USER", "dataflow")
        self.password = getenv_brand_str("ORACLE_PASSWORD", "dataflow")

    @property
    def schema(self) -> str:
        return self.user.upper()

    def stored(self, ident: str) -> str:
        return ident.upper()

    def available(self) -> tuple[bool, str]:
        if not reachable(self.host, self.port):
            return False, f"Oracle not listening on {self.host}:{self.port}"
        try:
            conn = self.connect()
            conn.close()
        except Exception as exc:  # noqa: BLE001
            return False, f"Oracle connect failed: {str(exc)[:200]}"
        return True, ""

    def connect(self) -> Any:
        import oracledb

        conn = oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=f"{self.host}:{self.port}/{self.service}",
        )
        # Per-connection, not oracledb.defaults: the product's own Oracle reads
        # must keep their normal fetch types while the harness reads exactly.
        conn.outputtypehandler = _oracle_exact_numbers
        return conn

    def select_expr(self, column: str) -> str:
        if column == "ts_tz":
            # Fetching TIMESTAMP WITH TIME ZONE through the driver drops the
            # offset here, so render it in SQL where it is unambiguous.
            # SYS_EXTRACT_UTC pins the instant for both carriers Oracle may hold
            # it in: TZH:TZM is not a legal TO_CHAR format for WITH LOCAL TIME
            # ZONE (ORA-01821), which is what a Postgres timestamptz lands as.
            return (
                f"TO_CHAR(SYS_EXTRACT_UTC({self.quote('ts_tz')}), "
                f"'{_ORA_UTC_FMT}') || '+00:00'"
            )
        return self.quote(column)

    def param(self, index: int, name: str) -> str:
        if name == "ts_tz":
            # Binding an aware datetime drops the offset (the wall clock lands
            # as +00:00), so hand Oracle the offset in text and let it parse.
            return f"TO_TIMESTAMP_TZ(:{index + 1}, '{_ORA_TSTZ_FMT}')"
        return f":{index + 1}"

    def _drop_with(self, cur: Any, table: str) -> None:
        try:
            cur.execute(f"DROP TABLE {self.qualified(table)} PURGE")
        except Exception as exc:  # noqa: BLE001 — ORA-00942 table does not exist
            if "ORA-00942" not in str(exc):
                raise

    def bind(self, column: str, value: Any) -> Any:
        if isinstance(value, bool):
            return 1 if value else 0
        if column == "ts_tz" and isinstance(value, datetime):
            return value.isoformat(timespec="microseconds")
        return value

    def _prepare_bulk(self, cur: Any, columns: Sequence[str]) -> None:
        import oracledb

        # Without an explicit type, oracledb binds a Python float as NUMBER,
        # which cannot hold 1.5e300 — BINARY_DOUBLE can. Aware datetimes need
        # TIMESTAMP_TZ or the offset is dropped on the way in.
        sizes: list[Any] = []
        for name in columns:
            if name == "amt_float":
                sizes.append(oracledb.DB_TYPE_BINARY_DOUBLE)
            elif name == "ts_naive":
                sizes.append(oracledb.DB_TYPE_TIMESTAMP)
            elif name == "payload_json":
                sizes.append(oracledb.DB_TYPE_CLOB)
            else:
                sizes.append(None)
        cur.setinputsizes(*sizes)

    def read(self, column: str, value: Any) -> Any:
        if hasattr(value, "read"):  # LOB
            return value.read()
        return value

    def endpoint(self, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="oracle",
            host=self.host,
            port=self.port,
            database=self.service,
            username=self.user,
            password=self.password,
            schema=self.schema,
            table=table,
        )

    def table_types(self, table: str) -> dict[str, str]:
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT column_name, data_type, data_precision, data_scale, "
                "char_length, nullable FROM all_tab_columns "
                "WHERE owner = :1 AND table_name = :2",
                [self.schema, self.stored(table)],
            )
            out = {}
            for name, dtype, prec, scale, char_len, nullable in cur.fetchall():
                if prec is not None:
                    out[name] = f"{dtype}({prec},{scale})"
                elif char_len:
                    out[name] = f"{dtype}({char_len})"
                else:
                    out[name] = str(dtype)
            return out
        finally:
            conn.close()


_ORA_TSTZ_FMT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'
_ORA_UTC_FMT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6'


def _oracle_exact_numbers(cursor: Any, metadata: Any) -> Any:
    """Fetch Oracle NUMBER as Decimal so DECIMAL(20,9) is not read as a float."""
    import oracledb

    if metadata.type_code is oracledb.DB_TYPE_NUMBER:
        return cursor.var(Decimal, arraysize=cursor.arraysize)
    return None


_REGISTRY: dict[str, Callable[[], Engine]] = {
    "postgresql": PostgresEngine,
    "mysql": MySQLEngine,
    "sqlserver": SQLServerEngine,
    "sqlite": SQLiteEngine,
    "oracle": OracleEngine,
}


def build_engines(names: Sequence[str] | None = None) -> dict[str, Engine]:
    wanted = list(names or _REGISTRY)
    return {name: _REGISTRY[name]() for name in wanted if name in _REGISTRY}


def live_engines(engines: dict[str, Engine]) -> tuple[dict[str, Engine], dict[str, str]]:
    """Split the fleet into engines that answered and skip reasons for the rest."""
    live: dict[str, Engine] = {}
    skipped: dict[str, str] = {}
    for name, engine in engines.items():
        ok, reason = engine.available()
        if ok:
            live[name] = engine
        else:
            skipped[name] = reason
    return live, skipped
