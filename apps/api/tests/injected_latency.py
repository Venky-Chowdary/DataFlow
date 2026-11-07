"""Realistic remote latency injected into local engines for deadline tests.

Real Snowflake logins take seconds, metadata queries 0.5-2s and a suspended
warehouse 5-10s to resume; a cloud PostgreSQL connect costs a TLS handshake
and a big table's COUNT(*) a full scan. The local engines answer in
milliseconds, so a deadline test against them proves nothing about the
45s budget. These helpers slow the real drivers down and count what each
endpoint was asked to do.

PostgreSQL scan latency is injected server-side (``pg_sleep`` before the
statement on the same session), so a ``statement_timeout`` cancels it the way
it would cancel a real long scan.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class Calls:
    counts: Counter = field(default_factory=Counter)
    lock: threading.Lock = field(default_factory=threading.Lock)
    #: Clear to stop injecting (e.g. while a job runs outside any deadline).
    active: threading.Event = field(default_factory=threading.Event)

    def __post_init__(self) -> None:
        self.active.set()

    def add(self, key: str) -> None:
        with self.lock:
            self.counts[key] += 1


def slow_postgres(
    monkeypatch,
    *,
    connect: float = 0.0,
    scan: Callable[[str], float] | None = None,
) -> Calls:
    """Every psycopg2 connect sleeps ``connect``; ``scan(sql)`` seconds of
    server-side work precede each COUNT/GROUP BY statement."""
    import psycopg2
    import psycopg2.extensions as ext

    calls = Calls()
    real_connect = psycopg2.connect
    cursor_classes: dict[type, type] = {}
    conn_classes: dict[type, type] = {}

    def counted_cursor(base: type) -> type:
        if base not in cursor_classes:

            def execute(self, sql, *a, **k):
                text = sql.decode() if isinstance(sql, bytes) else str(sql)
                calls.add("pg_statement")
                if scan is not None and "count(" in text.lower():
                    seconds = scan(text)
                    if seconds:
                        calls.add("pg_scan")
                        base.execute(self, f"SELECT pg_sleep({float(seconds)})")
                return base.execute(self, sql, *a, **k)

            cursor_classes[base] = type("Slow" + base.__name__, (base,), {"execute": execute})
        return cursor_classes[base]

    def conn_class(base: type) -> type:
        if base not in conn_classes:

            def cursor(self, *a, **k):
                factory = k.pop("cursor_factory", None) or self.cursor_factory or ext.cursor
                return base.cursor(self, *a, cursor_factory=counted_cursor(factory), **k)

            conn_classes[base] = type("Slow" + base.__name__, (base,), {"cursor": cursor})
        return conn_classes[base]

    def slow_connect(*args, **kwargs):
        calls.add("pg_connect")
        if connect:
            time.sleep(connect)
        kwargs["connection_factory"] = conn_class(kwargs.get("connection_factory") or ext.connection)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(psycopg2, "connect", slow_connect)
    return calls


def slow_snowflake(
    monkeypatch,
    *,
    login: float = 3.0,
    query: float = 1.0,
    warehouse_resume: float = 8.0,
) -> Calls:
    """fakesnow with a real Snowflake's costs: each login ``login`` seconds,
    each statement ``query`` seconds, and the first statement that needs
    compute pays ``warehouse_resume`` once (a suspended warehouse)."""
    import connectors.snowflake_conn as sc
    from fakesnow.cursor import FakeSnowflakeCursor

    calls = Calls()
    real_open = sc._open_connection
    real_exec = FakeSnowflakeCursor.execute
    resumed = threading.Event()

    def open_(**kwargs):
        calls.add("sf_login")
        if calls.active.is_set():
            time.sleep(login)
        return real_open(**kwargs)

    def execute(self, command, *a, **k):
        calls.add("sf_statement")
        if not calls.active.is_set():
            return real_exec(self, command, *a, **k)
        time.sleep(query)
        head = str(command).lstrip().upper()
        if not resumed.is_set() and not head.startswith(
            ("SHOW", "USE", "ALTER", "DESC", "SELECT CURRENT_")
        ):
            resumed.set()
            calls.add("sf_warehouse_resume")
            time.sleep(warehouse_resume)
        return real_exec(self, command, *a, **k)

    monkeypatch.setattr(sc, "_open_connection", open_)
    monkeypatch.setattr(FakeSnowflakeCursor, "execute", execute)
    return calls


def slow_destination_scans(
    monkeypatch,
    *,
    table: str,
    seconds_per_row: float,
    connect: float = 0.0,
) -> Calls:
    """A PostgreSQL destination whose full-table reads cost what a big one's do.

    A local table answers a sequential scan of 100k rows in milliseconds; a
    production destination holding hundreds of millions of rows, or a cold
    cloud volume, does not. Every statement that reads ``table`` is EXPLAINed
    on its own session first: when the plan reads the whole table (a Seq Scan
    on it that no LIMIT stops early), ``reltuples * seconds_per_row`` of server-side ``pg_sleep``
    precede it, so the statement timeout cancels it the way it would cancel a
    real long scan. An index lookup costs nothing extra. ``calls`` counts
    ``dest_seq_scan`` and ``dest_index_read``.
    """
    import json

    import psycopg2
    import psycopg2.extensions as ext

    calls = Calls()
    real_connect = psycopg2.connect
    needle = table.lower()
    cursor_classes: dict[type, type] = {}
    conn_classes: dict[type, type] = {}

    def _seq_scans(plan: dict, under_limit: bool = False) -> bool:
        # ``SELECT ... LIMIT n`` stops after n rows; with a filter it may read
        # the whole table looking for them, so that one still counts.
        if plan.get("Node Type") == "Seq Scan" and str(plan.get("Relation Name", "")).lower() == needle:
            return not under_limit or bool(plan.get("Filter"))
        limited = under_limit or plan.get("Node Type") == "Limit"
        return any(_seq_scans(child, limited) for child in plan.get("Plans") or [])

    def counted_cursor(base: type) -> type:
        if base not in cursor_classes:

            def execute(self, sql, *a, **k):
                text = sql.decode() if isinstance(sql, bytes) else str(sql)
                low = text.lower()
                if (
                    calls.active.is_set()
                    and needle in low
                    and low.lstrip().startswith("select")
                    and "pg_catalog" not in low
                    and "information_schema" not in low
                ):
                    base.execute(self, "EXPLAIN (FORMAT JSON) " + text, *a, **k)
                    raw = self.fetchone()[0]
                    plan = (json.loads(raw) if isinstance(raw, str) else raw)[0]["Plan"]
                    if _seq_scans(plan):
                        calls.add("dest_seq_scan")
                        base.execute(
                            self,
                            "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = %s::regclass",
                            (table,),
                        )
                        rows = int(self.fetchone()[0] or 0)
                        if rows * seconds_per_row:
                            base.execute(self, f"SELECT pg_sleep({rows * seconds_per_row:.3f})")
                    else:
                        calls.add("dest_index_read")
                return base.execute(self, sql, *a, **k)

            cursor_classes[base] = type("Scan" + base.__name__, (base,), {"execute": execute})
        return cursor_classes[base]

    def conn_class(base: type) -> type:
        if base not in conn_classes:

            def cursor(self, *a, **k):
                factory = k.pop("cursor_factory", None) or self.cursor_factory or ext.cursor
                return base.cursor(self, *a, cursor_factory=counted_cursor(factory), **k)

            conn_classes[base] = type("Scan" + base.__name__, (base,), {"cursor": cursor})
        return conn_classes[base]

    def slow_connect(*args, **kwargs):
        calls.add("pg_connect")
        if connect and calls.active.is_set():
            time.sleep(connect)
        kwargs["connection_factory"] = conn_class(kwargs.get("connection_factory") or ext.connection)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(psycopg2, "connect", slow_connect)
    return calls


def slow_mysql(monkeypatch, *, connect: float = 0.0, statement: float = 0.0) -> Calls:
    """Every pymysql connect sleeps ``connect`` and every statement ``statement``
    (a remote MySQL's TLS handshake and round trip)."""
    import pymysql.connections as pc
    import pymysql.cursors as pcur

    calls = Calls()
    real_connect = pc.Connection.connect
    real_execute = pcur.Cursor.execute

    def slow_connect(self, *a, **k):
        calls.add("my_connect")
        if connect and calls.active.is_set():
            time.sleep(connect)
        return real_connect(self, *a, **k)

    def slow_execute(self, query, args=None):
        calls.add("my_statement")
        if statement and calls.active.is_set():
            time.sleep(statement)
        return real_execute(self, query, args)

    monkeypatch.setattr(pc.Connection, "connect", slow_connect)
    monkeypatch.setattr(pcur.Cursor, "execute", slow_execute)
    return calls
