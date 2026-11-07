"""QA round 10: SQL Server DATETIME2(7) lost its seventh fractional digit on
every path, and verification could not see it.

pyodbc builds a Python ``datetime`` (six digits) from the ODBC timestamp
struct, whose fraction is in nanoseconds. The source digest read the column the
same way, so ``03:04:05.1234567`` landed as ``.1234560`` (SQL Server row path)
or ``.123456`` (SQLite) and fingerprinted equal to the source. Run 2 into the
SQLite TEXT column DataFlow created then read back as ``TIMESTAMP(6)`` and was
graded a lossy ``DATETIME2(7) -> TIMESTAMP(6)`` collapse.

Now (connectors/sqlserver_tick.py) the driver struct is decoded with its 100 ns
digit, carried to destinations that hold seven digits (SQL Server DATETIME2(7),
Oracle TIMESTAMP(7), text carriers), fingerprinted at seven digits on both sides, and disclosed as a
declared narrowing where the carrier keeps six (PostgreSQL / MySQL).

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job on real
SQL Server :1433 into SQL Server, SQLite, Oracle :1521, PostgreSQL :5432 and
MySQL :3306,
on the engine copy and on the row path, then run 2 into the table DataFlow
created.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

_ROWS = [
    (1, "2024-01-02 03:04:05.1234567"),
    (2, "2024-01-02 00:00:00.0000001"),
    (3, None),
    (4, "1999-12-31 23:59:59.9999999"),
    (5, "2024-06-30 12:00:00.5"),
    (6, "2024-06-30 12:00:00"),
]
_RUN2 = [(7, "2025-02-03 04:05:06.0000007")]
_ROW_PATH_FLAGS = (
    "SQLSERVER_SQLSERVER_INSERT_SELECT",
    "SQLSERVER_SQLITE_COPY",
    "SQLSERVER_PG_COPY",
    "SQLSERVER_MYSQL_COPY",
    "SQLSERVER_ORACLE_COPY",
)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    sqlite_root = tmp_path / "sqlite"
    sqlite_root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(sqlite_root))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        c.sqlite_db = str(sqlite_root / "r10dt2.db")  # type: ignore[attr-defined]
        yield c


def _seven(text: str | None) -> str | None:
    """``YYYY-MM-DD HH:MM:SS.fffffff`` — the value at SQL Server's precision."""
    if text is None:
        return None
    text = str(text).replace("T", " ", 1)
    head, _, frac = text.partition(".")
    return f"{head}.{(frac + '0000000')[:7]}"


def _six(text: str | None) -> str | None:
    """The same value at six digits (what a microsecond carrier keeps)."""
    seven = _seven(text)
    return None if seven is None else seven[:-1]


def _source_text(table: str) -> dict[int, str | None]:
    return {
        int(r[0]): r[1]
        for r in h.query("sqlserver", f"SELECT id, CONVERT(varchar(27), ts, 121) FROM [{table}]")  # nosec B608
    }


def _oracle():
    from tests import test_r8_d210_oracle_empty_string_e2e as o

    return o


def _stored(client, dest: str, table: str) -> dict[int, str | None]:
    if dest == "oracle":
        rows = _oracle()._ora(
            f"SELECT id, TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS.FF7') FROM \"{table}\""  # nosec B608
        )
        return {int(i): v for i, v in rows}
    if dest == "sqlite":
        with sqlite3.connect(client.sqlite_db) as conn:
            return {int(i): v for i, v in conn.execute(f'SELECT id, ts FROM "{table}"')}  # nosec B608
    if dest == "sqlserver":
        sql = f"SELECT id, CONVERT(varchar(27), ts, 121) FROM [{table}]"
    elif dest == "postgresql":
        sql = f"SELECT id, to_char(ts, 'YYYY-MM-DD HH24:MI:SS.US') FROM \"{table}\""
    else:
        sql = f"SELECT id, DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s.%f') FROM `{table}`"
    return {int(i): v for i, v in h.query(dest, sql)}  # nosec B608


def _insert(table: str, rows) -> None:
    for rid, text in rows:
        lit = "NULL" if text is None else f"'{text}'"
        h.execute("sqlserver", f"INSERT INTO [{table}] (id, ts) VALUES ({rid}, {lit})")  # nosec B608


def _connectors(client, dest: str, token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"r10dt2-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("sqlserver")})
    if dest == "sqlite":
        dcfg = {"type": "sqlite", "database": client.sqlite_db}
    elif dest == "oracle":
        dcfg = _oracle()._oracle_cfg()
    else:
        dcfg = h.dest_config(dest)
    dst = create_connector({"name": f"r10dt2-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **dcfg})
    return src, dst


def _verified(job: dict) -> dict:
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
    rec = job.get("reconciliation") or {}
    assert rec.get("verification_verdict") in (None, "verified"), rec
    return rec


def _ports(dest: str) -> None:
    need = {"sqlserver": [1433], "sqlite": [1433], "postgresql": [1433, 5432],
            "mysql": [1433, 3306], "oracle": [1433, 1521]}
    if not all(h.port_up(p) for p in need[dest]):
        pytest.skip(f"{dest} route needs ports {need[dest]}")


@pytest.mark.parametrize("path", ["engine_copy", "row_path"])
@pytest.mark.parametrize("dest", ["sqlserver", "sqlite", "oracle", "postgresql", "mysql"])
def test_datetime2_seventh_digit_is_carried_or_disclosed(client, monkeypatch, dest, path):
    _ports(dest)
    if path == "row_path":
        for flag in _ROW_PATH_FLAGS:
            monkeypatch.setenv(f"DATAFLOW_{flag}", "0")
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r10dt2_s_{token}", f"r10dt2_d_{token}"
    if dest == "oracle":
        dst_t = dst_t.upper()  # stored the way an Oracle user's DDL stores it
    try:
        h.execute("sqlserver", f"CREATE TABLE [{src_t}] (id INT PRIMARY KEY, ts DATETIME2(7) NULL)")
        _insert(src_t, _ROWS)
        src, dst = _connectors(client, dest, token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        }
        for run in (1, 2):
            job = h.stage_and_run(client, "start_transfer", args)
            rec = _verified(job)
            source = _source_text(src_t)
            got = _stored(client, dest, dst_t)
            if dest in {"sqlserver", "sqlite", "oracle"}:
                # Seven-digit carriers: every value exactly, 100 ns included.
                assert {k: _seven(v) for k, v in got.items()} == source, (run, got)
                assert not rec.get("carrier_rounded_columns"), rec
            else:
                # Microsecond carriers: six digits, and the report says the
                # seventh was not stored instead of claiming it.
                assert {k: _six(v) for k, v in got.items()} == {
                    k: _six(v) for k, v in source.items()
                }, (run, got)
                rounded = rec.get("carrier_rounded_columns") or []
                assert [c.get("column") for c in rounded] == ["ts"], rec
                assert rounded[0].get("kept_fractional_digits") == 6, rounded
                assert "declared narrowing" in str(rec.get("message") or ""), rec
            if run == 1:
                # Run 2 into the table DataFlow created, with a new 100 ns row.
                _insert(src_t, _RUN2)
    finally:
        h.drop_table("sqlserver", src_t)
        if dest == "oracle":
            _oracle()._drop_ora(dst_t)
        elif dest != "sqlite":
            h.drop_table(dest, dst_t)


def test_run2_into_sqlite_text_whose_rows_stop_at_six_digits(client):
    """Run 2 reads the TEXT column back as a text carrier, not TIMESTAMP(6),
    even when no sampled row spells a seventh digit."""
    _ports("sqlite")
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r10dt2_s_{token}", f"r10dt2_d_{token}"
    try:
        h.execute("sqlserver", f"CREATE TABLE [{src_t}] (id INT PRIMARY KEY, ts DATETIME2(7) NULL)")
        _insert(src_t, [(1, "2024-01-02 03:04:05.123456"), (2, None), (3, "2024-01-02 03:04:05")])
        src, dst = _connectors(client, "sqlite", token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        }
        _verified(h.stage_and_run(client, "start_transfer", args))
        _insert(src_t, _RUN2)
        _verified(h.stage_and_run(client, "start_transfer", args))
        got = _stored(client, "sqlite", dst_t)
        assert _seven(got[7]) == "2025-02-03 04:05:06.0000007", got
    finally:
        h.drop_table("sqlserver", src_t)


def test_verification_sees_a_dropped_seventh_digit(client, monkeypatch):
    """Gate-8 compares seven digits: a copy that writes six must not verify."""
    _ports("sqlite")
    try:
        import connectors.sqlserver_tick as tick_mod
    except ImportError:  # the base: every writer already spells six digits
        tick_mod = None
    if tick_mod is not None:
        def six_digit_text(value, sep="T"):
            return datetime.isoformat(tick_mod.drop_tick(value), sep=sep)

        monkeypatch.setattr(tick_mod, "temporal_iso_text", six_digit_text)
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r10dt2_s_{token}", f"r10dt2_d_{token}"
    try:
        h.execute("sqlserver", f"CREATE TABLE [{src_t}] (id INT PRIMARY KEY, ts DATETIME2(7) NULL)")
        _insert(src_t, _ROWS)
        src, dst = _connectors(client, "sqlite", token)
        ok, staged = h.mcp_call(client, "start_transfer", {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        })
        assert ok, staged
        resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "r10"})
        job = h.wait_job(str(resp.json().get("job_id") or ""), 300)
        rec = job.get("reconciliation") or {}
        assert job.get("status") != "completed" or rec.get("verification_verdict") == "failed", {
            "status": job.get("status"), "rec": rec,
        }
    finally:
        h.drop_table("sqlserver", src_t)


def test_fingerprint_compares_the_seventh_digit():
    from connectors.sqlserver_tick import with_tick
    from services.reconciliation import normalize_cell

    src = with_tick(datetime(2024, 1, 2, 3, 4, 5, 123456), 7)  # noqa: DTZ001
    for text in ("2024-01-02 03:04:05.1234567", "2024-01-02T03:04:05.1234567"):
        assert normalize_cell(text, ddl_type="TEXT", engine="sqlite") == normalize_cell(src)
        assert normalize_cell(text, ddl_type="DATETIME2(7)", engine="sqlserver") == normalize_cell(src)
    assert normalize_cell("2024-01-02 03:04:05.123456", ddl_type="TEXT", engine="sqlite") != normalize_cell(src)
    assert normalize_cell("2024-01-02 03:04:05.1234560", ddl_type="DATETIME2(7)", engine="sqlserver") != normalize_cell(src)
