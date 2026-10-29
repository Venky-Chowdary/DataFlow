"""D188 — a source edit that does not move the cursor must not fail later runs.

QA (production ac612203): scheduled SQL Server -> MySQL ``incremental_upsert``.
Run 1 matched 30/30. Someone then edited a source row without bumping the
cursor column. Runs 2 and 3 wrote every row their cursor window selected
correctly, yet both ended *failed* on that edited row — a row neither run was
meant to pick up. Every manual fix or backfill on the source therefore turned
every later run red even though the data each run wrote was right.

The contract this file pins, end to end on real engines through
``UniversalTransferEngine().execute_tracked``:

* a run verifies exactly the rows its cursor window selected;
* a source row outside the window that differs from the destination is
  reported as out-of-window drift, not as this run's failure;
* a real loss *inside* the window still fails the run.
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

import pytest


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


_MS = {
    "host": os.getenv("DATAFLOW_TEST_MSSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MSSQL_PORT", "1433")),
    "database": os.getenv("DATAFLOW_TEST_MSSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MSSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
}
_MY = {
    "host": os.getenv("DATAFLOW_TEST_MYSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MYSQL_PORT", "3306")),
    "database": os.getenv("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MYSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MYSQL_PASSWORD", ""),
}
_PG = {
    "host": os.getenv("PGHOST", ""),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", ""),
    "username": os.getenv("PGUSER", ""),
    "password": os.getenv("PGPASSWORD", ""),
}


def _up(cfg: dict) -> bool:
    return bool(cfg["host"] and cfg["password"] and _reachable(cfg["host"], cfg["port"]))


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


# ---------------------------------------------------------------- engines


class _SqlServer:
    fmt = "mssql"
    cfg = _MS
    extra = {"trust_server_certificate": True}

    def conn(self):
        import pyodbc

        return pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
            f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
            timeout=10,
            autocommit=True,
        )

    def q(self, t: str) -> str:
        return f"[{t}]"

    ph = "?"
    create = (
        "CREATE TABLE {t} (id INT PRIMARY KEY, name NVARCHAR(80) NULL, "
        "note NVARCHAR(80) NULL, updated_at DATETIME2(0) NOT NULL)"
    )

    def drop(self, t: str) -> str:
        return f"DROP TABLE IF EXISTS [{t}]"


class _MySql:
    fmt = "mysql"
    cfg = _MY
    extra: dict = {}

    def conn(self):
        import pymysql

        return pymysql.connect(
            host=_MY["host"], port=_MY["port"], user=_MY["username"],
            password=_MY["password"], database=_MY["database"], autocommit=True,
            charset="utf8mb4",
        )

    def q(self, t: str) -> str:
        return f"`{t}`"

    ph = "%s"
    create = (
        "CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(80) NULL, "
        "note VARCHAR(80) NULL, updated_at DATETIME NOT NULL)"
    )

    def drop(self, t: str) -> str:
        return f"DROP TABLE IF EXISTS `{t}`"


class _Postgres:
    fmt = "postgresql"
    cfg = _PG
    extra: dict = {}

    def conn(self):
        import psycopg2

        c = psycopg2.connect(
            host=_PG["host"], port=_PG["port"], user=_PG["username"],
            password=_PG["password"], dbname=_PG["database"],
        )
        c.autocommit = True
        return c

    def q(self, t: str) -> str:
        return f'"{t}"'

    ph = "%s"
    create = (
        "CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(80) NULL, "
        "note VARCHAR(80) NULL, updated_at TIMESTAMP NOT NULL)"
    )

    def drop(self, t: str) -> str:
        return f'DROP TABLE IF EXISTS "{t}"'


def _exec(eng, sql: str, rows=None):
    c = eng.conn()
    try:
        cur = c.cursor()
        if rows is None:
            cur.execute(sql)
        else:
            cur.executemany(sql, rows)
    finally:
        c.close()


def _fetch(eng, sql: str) -> list[tuple]:
    c = eng.conn()
    try:
        cur = c.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        c.close()


def _ts(i: int) -> str:
    # One distinct second per row; well inside every engine's DATETIME range.
    return f"2026-01-01 00:{i // 60:02d}:{i % 60:02d}"


def _seed(eng, table: str, lo: int, hi: int) -> None:
    rows = []
    for i in range(lo, hi + 1):
        # NULL, empty, whitespace and unicode ride the window on purpose.
        note = {0: None, 1: "", 2: "  ", 3: "naïve 中文 ✓"}.get(i % 7, f"note {i}")
        rows.append((i, f"name {i}", note, _ts(i)))
    ph = eng.ph
    _exec(
        eng,
        f"INSERT INTO {eng.q(table)} (id, name, note, updated_at) "  # nosec B608
        f"VALUES ({ph}, {ph}, {ph}, {ph})",
        rows,
    )


def _request(src, dst, table: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format=src.fmt, table=table, extra=dict(src.extra), **src.cfg
        ),
        destination=EndpointConfig(
            kind="database", format=dst.fmt, table=table, extra=dict(dst.extra), **dst.cfg
        ),
        sync_mode="incremental_upsert",
        stream_contracts=[
            {
                "name": table,
                "cursor_field": "updated_at",
                "sync_mode": "incremental_upsert",
                "primary_key": ["id"],
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(fake, src, dst, table: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d188" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(src, dst, table), job_id)


def _drift(result) -> dict:
    recon = result.reconciliation or {}
    return recon.get("out_of_window_drift") or {}


def _scenario(isolated, src, dst):
    table = "d188_" + uuid.uuid4().hex[:10]
    _exec(src, src.create.format(t=src.q(table)))
    try:
        _seed(src, table, 1, 30)
        first = _run(isolated, src, dst, table)
        assert first.success, first.error
        assert len(_fetch(dst, f"SELECT id FROM {dst.q(table)}")) == 30  # nosec B608
        assert not _drift(first)

        # A manual fix on the source that does NOT bump the cursor column.
        _exec(
            src,
            f"UPDATE {src.q(table)} SET name = {src.ph} WHERE id = 5",  # nosec B608
            [("hand-fixed 5",)],
        )

        _seed(src, table, 31, 33)
        second = _run(isolated, src, dst, table)
        assert second.success, (second.error, second.reconciliation)
        dst_rows = dict(
            _fetch(dst, f"SELECT id, name FROM {dst.q(table)}")  # nosec B608
        )
        assert len(dst_rows) == 33
        assert dst_rows[5] == "name 5", "row 5 is outside run 2's window"
        assert dst_rows[31] == "name 31"
        drift = _drift(second)
        assert drift.get("rows") == 1 and drift.get("changed") == 1, second.reconciliation
        assert drift.get("sample_keys") == ["5"], drift
        msg = str(second.reconciliation.get("message") or "")
        assert "Out-of-window drift: 1 row(s)" in msg and "key 5" in msg, msg

        _seed(src, table, 34, 36)
        third = _run(isolated, src, dst, table)
        assert third.success, (third.error, third.reconciliation)
        got = _fetch(dst, f"SELECT id, note FROM {dst.q(table)} ORDER BY id")  # nosec B608
        assert len(got) == 36
        # NULL / empty / whitespace / unicode in the window land exactly.
        notes = dict(got)
        assert notes[35] is None and notes[36] == "" and notes[30] == "  ", notes
        assert notes[31] == "naïve 中文 ✓", notes
        assert _drift(third).get("sample_keys") == ["5"], third.reconciliation
        return table
    except BaseException:
        _cleanup(src, dst, table)
        raise


def _cleanup(src, dst, table: str) -> None:
    _exec(src, src.drop(table))
    _exec(dst, dst.drop(table))


_PAIRS = {
    "sqlserver->mysql": (_SqlServer, _MySql, (_MS, _MY)),
    "postgres->mysql": (_Postgres, _MySql, (_PG, _MY)),
    "mysql->postgres": (_MySql, _Postgres, (_MY, _PG)),
    "sqlserver->postgres": (_SqlServer, _Postgres, (_MS, _PG)),
    "postgres->sqlserver": (_Postgres, _SqlServer, (_PG, _MS)),
    "mysql->sqlserver": (_MySql, _SqlServer, (_MY, _MS)),
}


def _pair(name: str):
    s, d, cfgs = _PAIRS[name]
    if not all(_up(c) for c in cfgs):
        pytest.skip(f"live {name} engines not reachable")
    return s(), d()


@pytest.mark.parametrize("pair", sorted(_PAIRS))
def test_edit_outside_the_window_is_drift_not_failure(isolated, pair):
    src, dst = _pair(pair)
    table = _scenario(isolated, src, dst)
    _cleanup(src, dst, table)


@pytest.mark.parametrize("pair", ["sqlserver->mysql", "postgres->mysql"])
def test_real_loss_inside_the_window_still_fails(isolated, monkeypatch, pair):
    """Drift reporting must not hide a row the window selected but never landed."""
    import src.transfer.engine as engine_mod

    src, dst = _pair(pair)
    table = _scenario(isolated, src, dst)
    try:
        _seed(src, table, 37, 40)
        real = engine_mod.run_reconciliation

        def _lose_then_verify(*args, **kwargs):
            # The run read and wrote id 38; it is gone when Gate-8 looks.
            _exec(dst, f"DELETE FROM {dst.q(table)} WHERE id = 38")  # nosec B608
            return real(*args, **kwargs)

        monkeypatch.setattr(engine_mod, "run_reconciliation", _lose_then_verify)
        fourth = _run(isolated, src, dst, table)
        assert not fourth.success, fourth.reconciliation
        assert "38" in str(fourth.error), fourth.error
    finally:
        _cleanup(src, dst, table)


@pytest.mark.parametrize("pair", ["sqlserver->mysql"])
def test_strict_flag_restores_fail_closed_on_drift(isolated, monkeypatch, pair):
    src, dst = _pair(pair)
    table = "d188_" + uuid.uuid4().hex[:10]
    _exec(src, src.create.format(t=src.q(table)))
    try:
        _seed(src, table, 1, 10)
        assert _run(isolated, src, dst, table).success
        _exec(src, f"UPDATE {src.q(table)} SET name = {src.ph} WHERE id = 2", [("x",)])  # nosec B608
        _seed(src, table, 11, 12)
        monkeypatch.setenv("DATAFLOW_INCREMENTAL_DRIFT_FAILS", "1")
        strict = _run(isolated, src, dst, table)
        assert not strict.success
        assert "Out-of-window drift" in str(strict.error), strict.error
    finally:
        _cleanup(src, dst, table)
