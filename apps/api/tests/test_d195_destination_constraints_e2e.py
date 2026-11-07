"""D195 / D112 / D25 — destination constraints survive and gate the run, end to end.

Real SQLite source -> live PostgreSQL / MySQL / SQL Server destination through
``UniversalTransferEngine().execute_tracked`` with preflight ON.

- D195: a full-refresh overwrite published a staging table built from
  DataFlow's own DDL (COPY routes) or renamed the live table aside and let the
  writer CREATE a new one (MySQL row path). CHECK, FOREIGN KEY, NOT NULL,
  DEFAULT, the unique index and the comment were gone after the first run.
- D112: preflight must read the destination's CHECK / NOT NULL / FK and refuse
  an offending row before the write, naming it. MySQL / SQL Server CHECK
  predicates were never evaluated (backtick / ``(0)`` forms), and a
  destination FK's parent keys were looked up in the *source*, so valid rows
  were refused ("no such table") and orphans were reported as a probe error.
- D25: a signed CAST_AND_CONTINUE must quarantine the one row whose NULL meets
  a NOT NULL column and load the rest; preflight refused the whole job.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_MSSQL_PW = os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD") or os.getenv("MSSQL_SA_PASSWORD") or ""


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


class _Engine:
    """One live destination: DDL, catalog snapshot and an endpoint."""

    name = ""

    def q(self, sql: str) -> list[tuple]:
        raise NotImplementedError

    def endpoint(self, table: str) -> EndpointConfig:
        raise NotImplementedError

    def create_parent(self, p: str) -> None:
        raise NotImplementedError

    def create_target(self, t: str, p: str) -> None:
        raise NotImplementedError

    def snapshot(self, t: str) -> Any:
        raise NotImplementedError

    def rows(self, t: str) -> list[tuple]:
        raise NotImplementedError

    def tables_like(self, prefix: str) -> list[str]:
        raise NotImplementedError

    def drop(self, name: str) -> None:
        raise NotImplementedError


class _Pg(_Engine):
    name = "postgresql"

    def __init__(self) -> None:
        import psycopg2

        self.conn = psycopg2.connect(
            host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
        )
        self.conn.autocommit = True

    def q(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []

    def endpoint(self, table):
        return EndpointConfig(
            kind="database", format="postgresql", table=table, schema="public",
            host="127.0.0.1", port=5432, database="dataflow", username="dataflow",
            password="dataflow",
        )

    def create_parent(self, p):
        self.q(f'CREATE TABLE public."{p}" (id BIGINT PRIMARY KEY)')
        self.q(f'INSERT INTO public."{p}" VALUES (1), (2)')

    def create_target(self, t, p):
        self.q(
            f'CREATE TABLE public."{t}" (id BIGINT PRIMARY KEY, '
            "code VARCHAR(20) NOT NULL, "
            f'amount BIGINT CONSTRAINT "{t}_amt" CHECK (amount >= 0), '
            "status VARCHAR(10) NOT NULL DEFAULT 'new', "
            f'parent_id BIGINT CONSTRAINT "{t}_fk" REFERENCES public."{p}"(id))'
        )
        self.q(f'CREATE UNIQUE INDEX "{t}_ux" ON public."{t}" (code)')
        self.q(f"COMMENT ON TABLE public.\"{t}\" IS 'keep me'")

    def snapshot(self, t):
        reg = f"'public.\"{t}\"'::regclass"
        return {
            "constraints": sorted(self.q(
                f"SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = {reg}"
            )),
            "indexes": sorted(self.q(f"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{t}'")),
            "columns": sorted(self.q(
                "SELECT column_name, is_nullable, column_default, data_type "
                f"FROM information_schema.columns WHERE table_name = '{t}'"
            )),
            "comment": self.q(f"SELECT obj_description({reg}, 'pg_class')"),
        }

    def rows(self, t):
        return sorted(self.q(f'SELECT id, code, amount, status, parent_id FROM public."{t}"'))

    def tables_like(self, prefix):
        return [r[0] for r in self.q(
            f"SELECT table_name FROM information_schema.tables WHERE table_name LIKE '{prefix}%'"
        )]

    def drop(self, name):
        self.q(f'DROP TABLE IF EXISTS public."{name}" CASCADE')


class _MySql(_Engine):
    name = "mysql"

    def __init__(self) -> None:
        import pymysql

        self.conn = pymysql.connect(
            host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
            database="dataflow", autocommit=True,
        )

    def q(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []

    def endpoint(self, table):
        return EndpointConfig(
            kind="database", format="mysql", table=table, host="127.0.0.1", port=3306,
            database="dataflow", username="dataflow", password="dataflow",
        )

    def create_parent(self, p):
        self.q(f"CREATE TABLE `{p}` (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
        self.q(f"INSERT INTO `{p}` VALUES (1), (2)")

    def create_target(self, t, p):
        self.q(
            f"CREATE TABLE `{t}` (id BIGINT PRIMARY KEY, code VARCHAR(20) NOT NULL, "
            "amount BIGINT, status VARCHAR(10) NOT NULL DEFAULT 'new', parent_id BIGINT, "
            f"CONSTRAINT `{t}_amt` CHECK (amount >= 0), "
            f"CONSTRAINT `{t}_fk` FOREIGN KEY (parent_id) REFERENCES `{p}` (id), "
            f"UNIQUE KEY `{t}_ux` (code)) ENGINE=InnoDB COMMENT='keep me'"
        )

    def snapshot(self, t):
        return self.q(f"SHOW CREATE TABLE `{t}`")[0][1]

    def rows(self, t):
        return sorted(self.q(f"SELECT id, code, amount, status, parent_id FROM `{t}`"))

    def tables_like(self, prefix):
        return [r[0] for r in self.q(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = 'dataflow' AND table_name LIKE '{prefix}%'"
        )]

    def drop(self, name):
        self.q("SET FOREIGN_KEY_CHECKS = 0")
        self.q(f"DROP TABLE IF EXISTS `{name}`")
        self.q("SET FOREIGN_KEY_CHECKS = 1")


class _MsSql(_Engine):
    name = "sqlserver"

    def __init__(self) -> None:
        import pyodbc

        self.conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
            f"UID=sa;PWD={_MSSQL_PW};TrustServerCertificate=yes",
            autocommit=True,
            timeout=10,
        )

    def q(self, sql):
        cur = self.conn.execute(sql)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []

    def endpoint(self, table):
        return EndpointConfig(
            kind="database", format="mssql", table=table, host="127.0.0.1", port=1433,
            database="dataflow", username="sa", password=_MSSQL_PW,
            extra={"trust_server_certificate": True},
        )

    def create_parent(self, p):
        self.q(f"CREATE TABLE [{p}] (id BIGINT PRIMARY KEY)")
        self.q(f"INSERT INTO [{p}] VALUES (1), (2)")

    def create_target(self, t, p):
        self.q(
            f"CREATE TABLE [{t}] (id BIGINT PRIMARY KEY, code VARCHAR(20) NOT NULL, "
            f"amount BIGINT CONSTRAINT [{t}_amt] CHECK (amount >= 0), "
            f"status VARCHAR(10) NOT NULL CONSTRAINT [{t}_df] DEFAULT 'new', "
            f"parent_id BIGINT CONSTRAINT [{t}_fk] REFERENCES [{p}](id))"
        )
        self.q(f"CREATE UNIQUE INDEX [{t}_ux] ON [{t}] (code)")

    def snapshot(self, t):
        oid = f"OBJECT_ID('{t}')"
        return {
            "check": sorted(self.q(f"SELECT name, definition FROM sys.check_constraints WHERE parent_object_id = {oid}")),
            "fk": sorted(self.q(f"SELECT name FROM sys.foreign_keys WHERE parent_object_id = {oid}")),
            "default": sorted(self.q(f"SELECT name, definition FROM sys.default_constraints WHERE parent_object_id = {oid}")),
            "index": sorted(self.q(
                f"SELECT name, is_unique, is_primary_key FROM sys.indexes WHERE object_id = {oid} AND name IS NOT NULL"
            )),
            "columns": sorted(self.q(f"SELECT name, is_nullable FROM sys.columns WHERE object_id = {oid}")),
        }

    def rows(self, t):
        return sorted(self.q(f"SELECT id, code, amount, status, parent_id FROM [{t}]"))

    def tables_like(self, prefix):
        return [r[0] for r in self.q(f"SELECT name FROM sys.tables WHERE name LIKE '{prefix}%'")]

    def drop(self, name):
        self.q(f"DROP TABLE IF EXISTS [{name}]")


_ENGINES: list[Any] = [
    pytest.param(_Pg, marks=pytest.mark.skipif(not _reachable(5432), reason="PostgreSQL not reachable"), id="postgresql"),
    pytest.param(_MySql, marks=pytest.mark.skipif(not _reachable(3306), reason="MySQL not reachable"), id="mysql"),
    pytest.param(
        _MsSql,
        marks=pytest.mark.skipif(
            not (_reachable(1433) and _MSSQL_PW), reason="SQL Server not reachable / no password"
        ),
        id="sqlserver",
    ),
]


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture(params=_ENGINES)
def dest(request):
    eng = request.param()
    prefix = "d195_" + uuid.uuid4().hex[:8]
    yield eng, prefix
    for name in sorted(eng.tables_like(prefix), key=lambda n: n.endswith("_p")):
        eng.drop(name)
    eng.conn.close()


def _source(tmp_path: Path, rows: list[tuple]) -> str:
    path = tmp_path / f"src_{uuid.uuid4().hex[:6]}.sqlite"
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT, amount INTEGER, "
            "status TEXT, parent_id INTEGER)"
        )
        conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?)", rows)
    conn.close()
    return str(path)


def _run(isolated, src: str, endpoint: EndpointConfig, *, sync_mode: str, mappings=None):
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=src, table="t"),
        destination=endpoint,
        sync_mode=sync_mode,
        validation_mode="strict",
        mappings=mappings or [],
    )
    job_id = "d195" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


_GOOD = [(1, "a", 10, "x", 1), (2, "b", 0, "y", 2), (3, "c", 7, "z", None)]


def test_full_refresh_overwrite_twice_keeps_the_constraint_set(tmp_path, isolated, dest):
    eng, t = dest
    p = t + "_p"
    eng.create_parent(p)
    eng.create_target(t, p)
    before = eng.snapshot(t)
    src = _source(tmp_path, _GOOD)
    for run in (1, 2):
        result = _run(isolated, src, eng.endpoint(t), sync_mode="overwrite")
        assert result.success, f"run {run}: {result.error}"
        assert eng.snapshot(t) == before, f"run {run} changed the constraint set"
        assert eng.rows(t) == _GOOD
        assert [n for n in eng.tables_like(t) if n not in {t, p}] == []


@pytest.mark.parametrize(
    ("bad_row", "names"),
    [
        ((2, "b", -5, "y", 2), ("CHECK", "_amt")),
        ((2, "b", 5, "y", 99), ("99",)),
        ((2, None, 5, "y", 2), ("NOT NULL", "code")),
    ],
    ids=["check", "foreign_key", "not_null"],
)
def test_a_row_the_destination_refuses_is_refused_at_preflight(tmp_path, isolated, dest, bad_row, names):
    eng, t = dest
    p = t + "_p"
    eng.create_parent(p)
    eng.create_target(t, p)
    src = _source(tmp_path, [_GOOD[0], bad_row, _GOOD[2]])
    result = _run(isolated, src, eng.endpoint(t), sync_mode="append")
    error = str(result.error or "")
    assert not result.success
    for name in names:
        assert name in error, error
    # Refused before any write, in DataFlow's words, not the driver's.
    lowered = error.lower()
    for raw in ("violates check constraint", "check constraint '", "3819", "547", "no such table"):
        assert raw not in lowered, error
    assert eng.rows(t) == []


def _signed_null_contract_maps() -> list[dict[str, Any]]:
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="code",
        source_type="TEXT",
        destination_type="VARCHAR(20)",
        approved_by="ops@dataflow.app",
        reason="D25: hold the NULL code row out, load the rest",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()
    ident = {"confidence": 0.99}
    return [
        {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "BIGINT", **ident},
        {
            "source": "code", "target": "code", "source_type": "TEXT", "target_type": "VARCHAR(20)",
            "risk_contract": contract, "risk_acknowledged": True, **ident,
        },
        {"source": "amount", "target": "amount", "source_type": "INTEGER", "target_type": "BIGINT", **ident},
        {"source": "status", "target": "status", "source_type": "TEXT", "target_type": "VARCHAR(10)", **ident},
        {"source": "parent_id", "target": "parent_id", "source_type": "INTEGER", "target_type": "BIGINT", **ident},
    ]


def _quarantined(eng: _Engine, t: str) -> int:
    names = [n for n in eng.tables_like(t) if "quarantine" in n.lower()]
    if not names:
        return 0
    if eng.name == "mysql":
        return int(eng.q(f"SELECT COUNT(*) FROM `{names[0]}`")[0][0])
    if eng.name == "sqlserver":
        return int(eng.q(f"SELECT COUNT(*) FROM [{names[0]}]")[0][0])
    return int(eng.q(f'SELECT COUNT(*) FROM public."{names[0]}"')[0][0])


@pytest.mark.parametrize("sync_mode", ["append", "overwrite"])
def test_signed_cast_and_continue_quarantines_the_null_row_and_loads_the_rest(
    tmp_path, isolated, dest, sync_mode
):
    eng, t = dest
    p = t + "_p"
    eng.create_parent(p)
    eng.create_target(t, p)
    before = eng.snapshot(t)
    rows = [_GOOD[0], (2, None, 5, "y", 2), _GOOD[2], (4, "", 1, "w", 1)]
    src = _source(tmp_path, rows)
    runs = 2 if sync_mode == "overwrite" else 1
    for run in range(runs):
        result = _run(
            isolated, src, eng.endpoint(t), sync_mode=sync_mode,
            mappings=_signed_null_contract_maps(),
        )
        assert result.success, f"run {run + 1}: {result.error}"
        # Only the NULL-code row is held out; the empty string is a value.
        assert eng.rows(t) == [_GOOD[0], _GOOD[2], (4, "", 1, "w", 1)]
        assert _quarantined(eng, t) >= 1
        assert eng.snapshot(t) == before
