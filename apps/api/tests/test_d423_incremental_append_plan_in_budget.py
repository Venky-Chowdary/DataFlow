"""D423 — a PostgreSQL-source incremental_append plan must finish in budget.

QA rounds 61-75: PG -> SQL Server / PG -> PG incremental_append plans on a
30-row table were refused at the 45 s tool deadline ("step: running the
preflight checks ... one of them is slow or unreachable", or "source
unreachable/slow: <host>") while overwrite on the same pair planned in ~32 s.

Measured (scratch harness, PG -> PG plan_transfer, 30-row seed, every connect
and statement delayed 500 ms like a tunnelled host):

  before  overwrite 34 statements 17.8 s | incremental_append 63 statements 32.3 s
  after   overwrite 32 statements 16.8 s | incremental_append 41 statements 21.3 s

Root cause: the cursor checks (text cursor order, NULL cursor count) each
resolved the source object's identity again, each catalog question checked a
fresh pooled connection out (one ping round trip per call), and the text-cursor
check reflected the column a second time. Fixed by resolving an object once per
plan (the same plan scope ``introspect_schema`` already shares), asking every
catalog question of one identity on one connection, and reading the declared
type from that reflection. Every value check still runs on live rows.

AC-2: a check that holds the deadline names itself and its table, and says the
host answered, instead of calling a reachable host unreachable.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid

import pytest
import sqlalchemy as sa

from services.introspect_memo import introspect_memo_scope
from services.null_cursor_probe import build_cursor_value_gates


class _Scope:
    cursor_column = "updated_at"


class _Counter:
    """Pool checkouts (each one pings on a pre-pinged pool) and statements."""

    def __init__(self) -> None:
        self.checkouts = 0
        self.statements = 0
        self._lock = threading.Lock()

    def _checkout(self, *_a) -> None:
        with self._lock:
            self.checkouts += 1

    def _stmt(self, *_a, **_k) -> None:
        with self._lock:
            self.statements += 1

    def __enter__(self) -> "_Counter":
        sa.event.listen(sa.pool.Pool, "checkout", self._checkout)
        sa.event.listen(sa.engine.Engine, "before_cursor_execute", self._stmt)
        return self

    def __exit__(self, *_exc) -> None:
        sa.event.remove(sa.pool.Pool, "checkout", self._checkout)
        sa.event.remove(sa.engine.Engine, "before_cursor_execute", self._stmt)

    @property
    def round_trips(self) -> int:
        return self.checkouts + self.statements


def _sqlite_source(tmp_path, rows):
    db = tmp_path / f"d423_{uuid.uuid4().hex[:6]}.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE ia_src (id INTEGER PRIMARY KEY, note TEXT, updated_at TEXT)")
    conn.executemany("INSERT INTO ia_src VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return db, {"type": "sqlite", "database": str(db), "connection_string": f"sqlite:///{db}"}


def _gates(cfg, validation_mode="strict"):
    return build_cursor_value_gates(
        read_scope=_Scope(),
        sync_mode="incremental_append",
        validation_mode=validation_mode,
        column_nullability={"updated_at": False},
        column_types={"updated_at": "TIMESTAMP"},
        source_connector_id="",
        source_config=cfg,
        source_table="ia_src",
    )


_SEED = [(i, "x", f"2024-01-01T{i % 24:02d}:00:00") for i in range(1, 31)]


def test_cursor_checks_resolve_the_source_once_per_plan(tmp_path):
    _db, cfg = _sqlite_source(tmp_path, _SEED)
    _gates(cfg)  # warm the engine cache: only the checks' own work is counted
    with _Counter() as live:
        _gates(cfg)
    with introspect_memo_scope(), _Counter() as planned:
        gate, blockers = _gates(cfg)
    assert gate and gate["status"] == "pass" and not blockers
    # Before: the two checks resolved the object twice, one checkout per
    # catalog call (8 checkouts). Now: one identity read on one connection,
    # then each check's own value query.
    assert planned.checkouts <= 3, (planned.checkouts, planned.statements)
    assert live.checkouts <= 4, (live.checkouts, live.statements)


def test_value_checks_are_not_memoised_inside_a_plan(tmp_path):
    """Only catalog facts are shared; a NULL cursor row is still counted live."""
    db, cfg = _sqlite_source(tmp_path, _SEED)
    with introspect_memo_scope():
        gate, _ = _gates(cfg)
        assert gate["status"] == "pass"
        conn = sqlite3.connect(db)
        conn.execute("INSERT INTO ia_src VALUES (31, 'late', NULL)")
        conn.commit()
        conn.close()
        gate, blockers = _gates(cfg)
    assert gate["id"] == "g_nullable_cursor" and gate["status"] == "block"
    assert gate["details"]["null_count"] == 1
    assert blockers and blockers[0]["id"] == "g_nullable_cursor"


def test_text_cursor_order_guarantee_holds_inside_a_plan(tmp_path):
    """Mixed-width numbers ordered as text are still refused (C3/H4b)."""
    rows = [(i, "x", str(i)) for i in range(1, 31)]  # '10' sorts before '9'
    _db, cfg = _sqlite_source(tmp_path, rows)
    with introspect_memo_scope():
        gate, blockers = _gates(cfg)
    assert gate and gate["id"] == "g_cursor_order" and gate["status"] == "block"
    assert blockers


def test_a_slow_cursor_check_is_named_with_its_table_not_the_host(monkeypatch):
    """AC-2: the deadline names the step and table, and does not call the host unreachable."""
    from services import cursor_order
    from src.ai.copilot import tool_deadline as td

    def slow_probe(*_a, **_k):
        time.sleep(3.0)
        return ""

    monkeypatch.setattr(cursor_order, "probe_text_cursor", slow_probe)
    src = {"name": "pg", "type": "postgresql", "host": "pg.example.com", "port": 5432}
    dst = {"name": "ms", "type": "sqlserver", "host": "ms.example.com", "port": 1433}

    def work():
        td.note_step("running the preflight checks", src, role="source",
                     also=(("destination", dst),))
        return _gates(dict(src))

    message = td.run_with_deadline("plan_transfer", work, lambda m: m, seconds=1.0)
    assert isinstance(message, str), message
    assert "checking the order of cursor column updated_at on source table ia_src" in message
    assert "unreachable" not in message
    assert "pg.example.com:5432" in message


def _pg():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL is not reachable")
    return pg_creds()


def test_pg_incremental_append_plan_costs_few_more_round_trips_than_overwrite(monkeypatch, tmp_path):
    """Live PG -> PG plan_transfer: the cursor checks add a bounded number of
    round trips over overwrite (measured 29 extra before the fix, 9 after)."""
    creds = _pg()
    import psycopg2

    from services import connector_store
    from src.ai.copilot import tools as tools_mod
    from src.ai.copilot.transfer_tools import plan_transfer

    table = f"d423_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(host=creds["host"], port=int(creds["port"]), dbname=creds["database"],
                            user=creds["username"], password=creds["password"])
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{table}" (id int PRIMARY KEY, name text, '
                "note text, updated_at timestamp)"
            )
            cur.execute(
                f'INSERT INTO public."{table}" SELECT g, \'n\' || g, NULL, '
                "timestamp '2024-01-01' + g * interval '1 hour' FROM generate_series(1, 30) g"
            )
        monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
        monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
        monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
        monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16")
        connector_store._backend_choice = None
        tools_mod._tools = None
        for name in ("D423SRC", "D423DST"):
            connector_store.create_connector({
                "name": name, "type": "postgresql", "role": "both", "host": creds["host"],
                "port": int(creds["port"]), "database": creds["database"],
                "username": creds["username"], "password": creds["password"],
                "schema": "public", "workspace_id": "",
            })
        costs = {}
        for mode in ("full_refresh_overwrite", "incremental_append", "full_refresh_overwrite",
                     "incremental_append"):
            kw = {"cursor_column": "updated_at"} if mode == "incremental_append" else {}
            with _Counter() as count:
                res = plan_transfer(source_connector_name="D423SRC", source_table=table,
                                    dest_connector_name="D423DST", dest_table=f"{table}_{mode[:4]}",
                                    sync_mode=mode, **kw)
            assert res.success, res.error
            costs[mode] = count.round_trips  # the second (warm) plan of each mode wins
        extra = costs["incremental_append"] - costs["full_refresh_overwrite"]
        assert extra <= 14, costs
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            for suffix in ("full", "incr"):
                cur.execute(f'DROP TABLE IF EXISTS public."{table}_{suffix}"')
        conn.close()
