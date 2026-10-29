"""X-35 / X-36 / X-37: MySQL -> PostgreSQL engine-copy route defects.

X-35: MySQL TIME is an interval (-838:59:59..+838:59:59); the PostgreSQL
``time`` carrier is a 0-24h wall clock. Feeding out-of-range rows into the
COPY fast path landed raw driver text (``-1 day, 23:00:00``) as an untyped
parse error and left the created table behind. The route now pre-scans and
declines to the row path, which emits a typed per-row finding.

X-36: a failed run on this route (bad write, or a Gate-8 dest tamper) must
drop the table this run created — the copy path now carries
``dest_table_existed_before`` into the summary and rolls back the aborted
transaction before the cleanup drop.

X-37: ``DATAFLOW_ENGINE_DIGEST=1`` was exported but the count-only scope
note still gated the engine-population digest off.

P3: the Gate-8 culprit-key census no longer requires a postgres-to-postgres
route — (pk, canonical row hash) is computed in Python for mysql/mssql/
duckdb/sqlite sides.

PG-dependent tests skip cleanly when 127.0.0.1:5432 dataflow creds are
rejected (local env); they run live on the loop's box.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

duckdb = pytest.importorskip("duckdb")

MYSQL = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}

PG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = "0" * 24
        self.jobs[job_id] = job_data
        return job_id

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields)
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def _mysql_conn():
    import pymysql

    try:
        return pymysql.connect(
            host=MYSQL["host"],
            port=MYSQL["port"],
            user=MYSQL["username"],
            password=MYSQL["password"],
            database=MYSQL["database"],
            connect_timeout=2,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"local MySQL (127.0.0.1:3306) unreachable: {exc}")


def _pg_conn():
    import psycopg2

    try:
        return psycopg2.connect(
            host=PG["host"],
            port=PG["port"],
            user=PG["username"],
            password=PG["password"],
            dbname=PG["database"],
            connect_timeout=2,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"local PostgreSQL (127.0.0.1:5432) unreachable: {exc}")


def _seed_time(table: str, rows: list[tuple]) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (id INT PRIMARY KEY, t TIME)"
            )
            cur.executemany(f"INSERT INTO `{table}` VALUES (%s,%s)", rows)
        conn.commit()
    finally:
        conn.close()


def _pg_drop_table(table: str) -> None:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.commit()
    finally:
        conn.close()


def _pg_table_exists(table: str) -> bool:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name=%s",
                (table,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _request(src_table: str, dest: dict, dest_table: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", table=src_table, **MYSQL
        ),
        destination=EndpointConfig(
            kind="database", table=dest_table, **dest
        ),
        sync_mode="full_refresh_overwrite",
        mappings=[
            {
                "source": c,
                "target": c,
                "confidence": 1.0,
                "user_override": True,
                "transform": "none",
            }
            for c in ["id", "t"]
        ],
        stream_contracts=[
            {
                "name": src_table,
                "primary_key": "id",
                "sync_mode": "full_refresh_overwrite",
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def _blob(res) -> str:
    return " ".join(
        str(p)
        for p in [
            res.error,
            (res.reconciliation or {}),
            getattr(res, "rejected_rows", None),
            getattr(res, "summary", None),
        ]
    )


def test_x35_time_out_of_range_scan_names_column():
    """The pre-scan must name the TIME column holding out-of-domain rows."""
    from services.copy_mysql_pg import mysql_time_column_out_of_range

    table = "x35_scan_src"
    _seed_time(
        table,
        [(1, "-01:00:00"), (2, "100:00:00"), (3, "12:00:00")],
    )
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            found = mysql_time_column_out_of_range(
                cur,
                f"`{table}`",
                [("id", "id"), ("t", "t")],
                ["INT", "time"],
                {"id": "INT", "t": "TIME"},
            )
        assert found == "t"
    finally:
        conn.close()


def test_x35_time_in_range_does_not_decline():
    """In-range TIME rows keep the fast path — the scan returns None."""
    from services.copy_mysql_pg import mysql_time_column_out_of_range

    table = "x35_ok_src"
    _seed_time(table, [(1, "01:23:45"), (2, "23:59:59"), (3, None)])
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            found = mysql_time_column_out_of_range(
                cur,
                f"`{table}`",
                [("id", "id"), ("t", "t")],
                ["INT", "time"],
                {"id": "INT", "t": "TIME"},
            )
        assert found is None
        # An interval/text dest carrier does not trigger the decline either.
        with conn.cursor() as cur2:
            found2 = mysql_time_column_out_of_range(
                cur2,
                f"`{table}`",
                [("id", "id"), ("t", "t")],
                ["INT", "text"],
                {"id": "INT", "t": "TIME"},
            )
        assert found2 is None
    finally:
        conn.close()


def test_x35_pg_copy_out_of_range_time_typed_finding_and_drop():
    """MySQL -> PG with out-of-range TIME: typed finding, no raw COPY error,
    and the table this run created is dropped."""
    dest_table = "x35_out"
    _pg_drop_table(dest_table)
    table = "x35_pg_src"
    _seed_time(
        table,
        [(1, "01:23:45"), (6, "-01:00:00"), (7, "100:00:00")],
    )
    res = UniversalTransferEngine().execute_tracked(
        _request(table, {"format": "postgresql", **PG}, dest_table), "0" * 24
    )
    assert not res.success, "out-of-range TIME must not land silently"
    blob = _blob(res)
    assert "invalid input syntax" not in blob, blob
    assert not _pg_table_exists(dest_table), (
        "the table this run created must be dropped on failure"
    )


def test_x36_pg_dest_tamper_drops_created_table(monkeypatch):
    """Tampered destination on MySQL -> PG: fail closed AND drop the table
    this run created — the same contract the DuckDB path already meets."""
    import src.transfer.reconcile_step as rs

    dest_table = "x36_out"
    _pg_drop_table(dest_table)
    table = "x36_pg_src"
    _seed_time(table, [(1, "01:23:45"), (2, "00:00:00"), (3, "09:05:00")])
    conn = _mysql_conn()
    conn.close()

    real = rs._run_reconciliation_with_rules

    def tampered(*a, **k):
        pc = _pg_conn()
        try:
            with pc.cursor() as cur:
                cur.execute(
                    f'UPDATE public."{dest_table}" SET t = \'01:23:46\' '
                    "WHERE id = 1"
                )
            pc.commit()
        finally:
            pc.close()
        return real(*a, **k)

    monkeypatch.setattr(rs, "_run_reconciliation_with_rules", tampered)
    res = UniversalTransferEngine().execute_tracked(
        _request(table, {"format": "postgresql", **PG}, dest_table), "0" * 24
    )
    assert not res.success, "dest tamper must fail the proof"
    assert not _pg_table_exists(dest_table), (
        "the table this run created must be dropped on reconcile failure"
    )
    recon = res.reconciliation or {}
    assert "postgres-to-postgres" not in str(
        recon.get("culprit_keys_unavailable_reason") or ""
    )


def test_x36_culprit_keys_named_on_duckdb(monkeypatch, tmp_path):
    """No-recipe MySQL -> DuckDB tamper: the cross-engine census names keys —
    the old message claimed a postgres-to-postgres route was required (P3)."""
    import src.transfer.reconcile_step as rs

    table = "x36_ddb_src"
    _seed_time(table, [(1, "01:23:45"), (2, "00:00:00"), (3, "09:05:00")])
    dst = tmp_path / "x36.duckdb"
    req = _request(
        table,
        {"format": "duckdb", "database": str(dst)},
        "out",
    )

    real = rs._run_reconciliation_with_rules

    def tampered(*a, **k):
        con = duckdb.connect(str(dst))
        con.execute("UPDATE out SET t = TIME '01:23:46' WHERE id = 1")
        con.close()
        return real(*a, **k)

    monkeypatch.setattr(rs, "_run_reconciliation_with_rules", tampered)
    res = UniversalTransferEngine().execute_tracked(req, "0" * 24)
    assert not res.success, "dest tamper must fail the proof"
    recon = res.reconciliation or {}
    assert "postgres-to-postgres" not in str(
        recon.get("culprit_keys_unavailable_reason") or ""
    ), recon.get("culprit_keys_unavailable_reason")
    keys = recon.get("culprit_keys") or {}
    named = str(keys)
    assert "1" in named or "t" in str(
        recon.get("culprit_columns") or ""
    ), f"culprit key 1 or column t must be named: {recon}"


def test_x37_engine_digest_flag_pg(monkeypatch):
    """DATAFLOW_ENGINE_DIGEST=1 on the engine-copy route must produce
    full_checksum, not count-only 'none' assurance."""
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    dest_table = "x37_out"
    _pg_drop_table(dest_table)
    table = "x37_pg_src"
    _seed_time(table, [(1, "01:23:45"), (2, "00:00:00"), (3, "09:05:00")])
    res = UniversalTransferEngine().execute_tracked(
        _request(table, {"format": "postgresql", **PG}, dest_table), "0" * 24
    )
    assert res.success, f"clean in-range TIME run must succeed: {res.error}"
    recon = res.reconciliation or {}
    blob = _blob(res)
    # With the digest flag on the verdict is full_checksum; the count-only
    # prompt to set the flag must not appear.
    assert "Set DATAFLOW_ENGINE_DIGEST" not in blob, blob
    assert str(recon.get("checksum_mode") or recon.get("assurance") or "") != "none", blob
    _pg_drop_table(dest_table)


def test_x38_timedelta_binds_as_time_on_sqlserver():
    """X-38: pymysql returns TIME as timedelta; the SQL Server bind path must
    hand the driver a datetime.time, not the raw timedelta that crashed
    pyodbc's describe ('Unknown object type datetime.timedelta')."""
    from datetime import time, timedelta

    from services.copy_pg_sqlserver import bind_row_for_sqlserver

    class _FakePyodbcCur:
        pass

    _FakePyodbcCur.__module__ = "pyodbc"
    cur = _FakePyodbcCur()
    td = timedelta(hours=1, minutes=23, seconds=45, microseconds=123456)
    bound = bind_row_for_sqlserver(cur, (1, td, None))
    assert bound[0] == 1 and bound[2] is None
    assert bound[1] == time(1, 23, 45, 123456), bound

    class _FakePymssqlCur:
        pass

    _FakePymssqlCur.__module__ = "pymssql"
    bound2 = bind_row_for_sqlserver(_FakePymssqlCur(), (td,))
    assert bound2[0] == time(1, 23, 45, 123456), bound2


def test_x38_coerce_temporal_accepts_timedelta_for_time():
    """timedelta -> TIME carrier on every SQL destination: in-domain values
    become datetime.time; out-of-domain stays for the typed finding."""
    from datetime import time, timedelta

    from connectors.sql_temporal import coerce_sql_temporal

    td = timedelta(hours=9, minutes=5)
    out = coerce_sql_temporal(td, "TIME(6)")
    assert out == time(9, 5), out
    # Out-of-domain does not invent a wall clock.
    for bad in (timedelta(days=1), -timedelta(hours=1)):
        stayed = coerce_sql_temporal(bad, "TIME")
        assert stayed is bad or not isinstance(stayed, time), stayed


def test_x38_mysql_time_to_sqlite(monkeypatch, tmp_path):
    """Live MySQL TIME -> SQLite: the timedelta reaches a text/affinity
    carrier cleanly and native read-back keeps the clock digits."""
    table = "x38_lite_src"
    _seed_time(table, [(1, "01:23:45"), (2, "09:05:00"), (3, None)])
    dst = tmp_path / "x38.duckdb"
    req = _request(
        table,
        {"format": "sqlite", "database": str(dst).replace(".duckdb", ".db")},
        "out",
    )
    res = UniversalTransferEngine().execute_tracked(req, "0" * 24)
    assert res.success, f"MySQL TIME -> SQLite must not crash: {res.error}"
    import sqlite3

    con = sqlite3.connect(str(dst).replace(".duckdb", ".db"))
    try:
        got = con.execute("SELECT id, t FROM out ORDER BY id").fetchall()
    finally:
        con.close()
    assert [r[0] for r in got] == [1, 2, 3]
    assert str(got[0][1]).startswith("01:23:45")
    assert str(got[1][1]).startswith("09:05:00")
    assert got[2][1] is None


def test_x37_residual_tamper_message_names_digest_mismatch(monkeypatch):
    """X-37 residual: with DATAFLOW_ENGINE_DIGEST=1 on, a dest tamper must
    say the in-engine digests mismatched — not tell the operator to set the
    flag they already set."""
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    import src.transfer.reconcile_step as rs

    dest_table = "x37r_out"
    _pg_drop_table(dest_table)
    table = "x37r_pg_src"
    _seed_time(table, [(1, "01:23:45"), (2, "00:00:00"), (3, "09:05:00")])

    real = rs._run_reconciliation_with_rules

    def tampered(*a, **k):
        pc = _pg_conn()
        try:
            with pc.cursor() as cur:
                cur.execute(
                    f'UPDATE public."{dest_table}" SET t = \'01:23:46\' '
                    "WHERE id = 1"
                )
            pc.commit()
        finally:
            pc.close()
        return real(*a, **k)

    monkeypatch.setattr(rs, "_run_reconciliation_with_rules", tampered)
    res = UniversalTransferEngine().execute_tracked(
        _request(table, {"format": "postgresql", **PG}, dest_table), "0" * 24
    )
    assert not res.success
    blob = _blob(res)
    assert "Set DATAFLOW_ENGINE_DIGEST" not in blob, blob
    assert "mismatch" in blob.lower(), blob
    # X-39: the digest promotion must not cut the ladder's L4/L5 localization
    # tail off the fail message, and must not leave the count-only sentence
    # next to the full_checksum verdict.
    assert "L4/L5 localized" in blob, blob
    assert "only proof the route produced" not in blob, blob
    _pg_drop_table(dest_table)


def test_x39_sqlite_dest_tamper_keeps_l45_localization(monkeypatch, tmp_path):
    """X-39 on a non-PG dest: engine-copy MySQL -> SQLite tamper must still
    localize row + column and say digests mismatched — never count-only."""
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    import sqlite3

    import src.transfer.reconcile_step as rs

    table = "x39_lite_src"
    _seed_time(table, [(1, "01:23:45"), (2, "09:05:00"), (3, "00:00:00")])
    dst = tmp_path / "x39.db"

    real = rs._run_reconciliation_with_rules

    def tampered(*a, **k):
        con = sqlite3.connect(str(dst))
        try:
            con.execute("UPDATE out SET t = '01:23:46' WHERE id = 1")
            con.commit()
        finally:
            con.close()
        return real(*a, **k)

    monkeypatch.setattr(rs, "_run_reconciliation_with_rules", tampered)
    res = UniversalTransferEngine().execute_tracked(
        _request(table, {"format": "sqlite", "database": str(dst)}, "out"),
        "0" * 24,
    )
    assert not res.success, "dest tamper must fail the proof"
    blob = _blob(res)
    assert "L4/L5 localized" in blob, blob
    assert "row id='1'" in blob or "row id=1" in blob, blob
    assert "'t'" in blob, blob
    assert "mismatch" in blob.lower(), blob
    assert "only proof the route produced" not in blob, blob
    assert "Set DATAFLOW_ENGINE_DIGEST" not in blob, blob


def test_undo_skips_delete_when_table_never_created(tmp_path, monkeypatch):
    """The undo path must not DELETE a table the run never created —
    SQL Server logged 'Invalid object name' 208 against the phantom ref
    (drop raised, then DELETE hit the same missing ref)."""
    import sqlite3

    import connectors.table_manager as tm
    import services.batch_undo as bu

    db = tmp_path / "undo.db"
    con = sqlite3.connect(str(db))
    con.close()

    def _drop_raises(db_type, cfg, table, schema=None):
        raise Exception(f"Invalid object name 'dbo.{table}' (208)")

    monkeypatch.setattr(tm, "drop_table", _drop_raises)
    deletes: list[str] = []
    monkeypatch.setattr(
        bu, "_delete_all_rows", lambda *a, **k: deletes.append(str(a)) or True
    )

    _Dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(db),
        table="never_made",
    )
    _Dest.extra = {"dest_table_existed_before": False}

    summary = {
        "dest_table_existed_before": False,
        "table": "never_made",
        "dest_rows_before": 0,
    }
    note = bu.undo_failed_batch_if_dest_was_empty(
        destination=_Dest, dest_summary=summary, sync_mode=""
    )
    assert not deletes, "no DELETE may run against a table that does not exist"
    assert summary.get("partial_batch_removed"), note
