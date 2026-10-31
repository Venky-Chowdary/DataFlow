"""QA D214: an append under a signed CAST_AND_CONTINUE contract refused the
whole write.

QA on 557f556c: with a signed quarantine contract the append still refused as
a strict error policy block and 0 rows landed. Related to D205 (its round-6 fix
is in the base).

Root cause: the SQL writers hold a bad cell out in two places. The type
matrix catches what Python can see and stamps the column's contract on the
holdout. What only the database can see (a 70,000 for a SQL Server TINYINT,
an emoji for a utf8mb3 column) surfaces as a data error on the batch INSERT;
the row-by-row salvage that quarantines the refused rows ran only under a
``quarantine`` job posture. Under ``strict`` the batch error was re-raised and
the signed contract never got a say.

Route: MCP ``start_transfer`` (``risk_acceptance`` signs CAST_AND_CONTINUE,
``validation_mode=strict``) -> confirm -> job, then the MCP
``replay_quarantine`` -> confirm after the operator widened the column. Bad
values sit inside the planner's sample (row 3) and beyond it (row N-1). Good
rows carry NULL, '', whitespace and unicode. Appends go into the operator's
existing, populated table.

PostgreSQL is not here, nor a table DataFlow created: every value class
that reaches such a column under a signed contract is held out by the type
matrix before the INSERT (probed: DOUBLE->INTEGER, TEXT->INTEGER,
TEXT->VARCHAR(n), NUMERIC(5,2), SMALLINT range, a created SQL Server SMALLINT),
and those already pass on 557f556c.

Live MySQL + SQL Server; skipped when absent.
"""
from __future__ import annotations

import os
import time
import uuid
from typing import Any

import pytest

N = 400
_PW = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD") or os.environ.get("MSSQL_SA_PASSWORD")


def _text(i: int) -> str | None:
    return {0: None, 1: "", 2: "  ", 3: "é漢✓"}.get(i % 7, f"t{i}")


def _mysql():
    pymysql = pytest.importorskip("pymysql")
    from tests.helpers.live_env import mysql_creds, mysql_up

    if not mysql_up():
        pytest.skip("MySQL not reachable")
    c = mysql_creds()
    return pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
        database=c["database"], autocommit=True, charset="utf8mb4",
    ), c


def _mssql():
    pyodbc = pytest.importorskip("pyodbc")
    if not _PW:
        pytest.skip("SQL Server not configured")
    host = os.environ.get("DATAFLOW_TEST_MSSQL_HOST") or "127.0.0.1"
    port = int(os.environ.get("DATAFLOW_TEST_MSSQL_PORT") or 1433)
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={host},{port};DATABASE=dataflow;UID=sa;PWD={_PW};TrustServerCertificate=yes",
            autocommit=True, timeout=10,
        )
    except pyodbc.Error as exc:
        pytest.skip(f"SQL Server not reachable: {exc}")
    return conn, {"host": host, "port": port}


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    c = pg_creds()
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                            user=c["username"], password=c["password"])
    conn.autocommit = True
    return conn, c


def _q(conn, sql: str, params: Any = None, many: bool = False) -> list[tuple] | None:
    cur = conn.cursor()
    try:
        if many:
            cur.executemany(sql, params)
        elif params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return [tuple(r) for r in cur.fetchall()] if cur.description else None
    finally:
        cur.close()


def _connector(kind: str, name: str, info: dict) -> Any:
    from services.connector_store import create_connector

    if kind == "postgresql":
        return create_connector({
            "name": name, "type": "postgresql", "host": info["host"], "port": info["port"],
            "database": info["database"], "username": info["username"],
            "password": info["password"], "schema": "public", "ssl": False,
        })
    if kind == "mysql":
        return create_connector({
            "name": name, "type": "mysql", "host": info["host"], "port": int(info["port"]),
            "database": info["database"], "username": info["username"],
            "password": info["password"], "ssl": False,
        })
    return create_connector({
        "name": name, "type": "sqlserver", "host": info["host"], "port": info["port"],
        "database": "dataflow", "username": "sa", "password": _PW, "schema": "dbo",
        "trust_server_certificate": True,
    })


def _status(job_id: str) -> dict:
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(tools.execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    return job


def _append(src: Any, src_table: str, dst: Any, dst_table: str, *, contract: bool = True) -> tuple[str, dict]:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    args: dict[str, Any] = {
        "source_connector_name": src.name, "source_table": src_table,
        "dest_connector_name": dst.name, "dest_table": dst_table,
        "sync_mode": "full_refresh_append", "validation_mode": "strict",
    }
    if contract:
        args["risk_acceptance"] = {
            "execution_policy": "CAST_AND_CONTINUE", "approved_by": "qa", "reason": "D214",
        }
    staged = DataPilotTools().execute("start_transfer", args)
    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "d214-test")["job_id"]
    return job_id, _status(job_id)


def _replay(job_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("replay_quarantine", {"job_id": job_id})
    assert staged.success, staged.error
    return spend_pilot_ack(staged.output["ack_id"], "d214-test")


def _assert_quarantined(job: dict, bad: set[int]) -> None:
    text = str(job.get("error") or job.get("message") or "")[:800]
    assert job.get("status") == "completed_with_quarantine", (job.get("status"), text)
    assert "blocks partial write" not in text, text
    rec = job.get("reconciliation") or (job.get("result") or {}).get("reconciliation") or {}
    if rec:
        assert rec.get("passed") is not False, rec.get("message")


def _assert_replay_closes(job_id: str, n_bad: int) -> None:
    from services.mongodb_service import get_mongodb_service
    from services.quarantine_dlq import job_quarantine_closure

    out = _replay(job_id)
    assert out["rows_written"] == n_bad and out["rejected"] == 0, out
    assert out["status"] == "completed", (out.get("reconciliation") or {}).get("message")
    assert out["quarantine_closure"]["verdict"] == "closed", out["quarantine_closure"]
    parent = get_mongodb_service().get_job(job_id)
    assert (job_quarantine_closure(parent) or {}).get("verdict") == "closed"


# --- SQL Server: integer ranges are only known to the database ------------

def _int_rows(first_id: int, big_inside: int, big_beyond: int) -> list[tuple]:
    rows = [(first_id + i, i % 200, _text(i)) for i in range(N)]
    rows[2] = (first_id + 2, big_inside, rows[2][2])  # inside the sample
    rows[N - 2] = (first_id + N - 2, big_beyond, rows[N - 2][2])  # beyond it
    return rows


@pytest.fixture
def mssql_route():
    from services.connector_store import delete_connector

    pg, pgc = _pg()
    ms, msc = _mssql()
    sfx = uuid.uuid4().hex[:6]
    t = {"src": f"d214s_{sfx}", "dst": f"d214d_{sfx}"}
    src_conn = _connector("postgresql", f"D214pg{sfx}", pgc)
    dst_conn = _connector("sqlserver", f"D214ms{sfx}", msc)
    try:
        yield {"pg": pg, "ms": ms, "src_conn": src_conn, "dst_conn": dst_conn, **t}
    finally:
        _q(pg, f'DROP TABLE IF EXISTS "{t["src"]}"')
        for name in (t["dst"], f"{t['dst']}_df_quarantine"):
            _q(ms, f"IF OBJECT_ID('dbo.{name}') IS NOT NULL DROP TABLE dbo.[{name}]")
        pg.close()
        ms.close()
        delete_connector(src_conn.id)
        delete_connector(dst_conn.id)


def _ms_rows(ms, table: str) -> list[tuple]:
    got = _q(ms, f"SELECT id, small, note FROM dbo.[{table}] ORDER BY id") or []
    return [(int(a), None if b is None else int(b), c) for a, b, c in got]


def _pg_source(pg, table: str, rows: list[tuple], small_type: str = "INT") -> None:
    _q(pg, f'CREATE TABLE "{table}" (id INT PRIMARY KEY, small {small_type}, note TEXT)')
    _q(pg, f'INSERT INTO "{table}" VALUES (%s,%s,%s)', rows, many=True)


def test_sqlserver_append_into_existing_table_quarantines_db_refusals(mssql_route):
    r = mssql_route
    pg, ms = r["pg"], r["ms"]
    rows = _int_rows(1, 70000, 30000)
    _pg_source(pg, r["src"], rows)
    _q(ms, f"CREATE TABLE dbo.[{r['dst']}] (id INT PRIMARY KEY, small TINYINT, note NVARCHAR(40))")
    _q(ms, f"INSERT INTO dbo.[{r['dst']}] VALUES (100000, 1, N'prior')")
    bad = {3, N - 1}

    job_id, job = _append(r["src_conn"], r["src"], r["dst_conn"], r["dst"])
    _assert_quarantined(job, bad)
    want = [row for row in rows if row[0] not in bad] + [(100000, 1, "prior")]
    assert _ms_rows(ms, r["dst"]) == sorted(want)

    _q(ms, f"ALTER TABLE dbo.[{r['dst']}] ALTER COLUMN small INT")
    _assert_replay_closes(job_id, len(bad))
    assert _ms_rows(ms, r["dst"]) == sorted(rows + [(100000, 1, "prior")])


# --- MySQL: a 3-byte utf8 column refuses 4-byte characters at the server ---

@pytest.fixture
def mysql_route():
    from services.connector_store import delete_connector

    pg, pgc = _pg()
    my, myc = _mysql()
    sfx = uuid.uuid4().hex[:6]
    t = {"src": f"d214s_{sfx}", "dst": f"d214d_{sfx}"}
    src_conn = _connector("postgresql", f"D214pg{sfx}", pgc)
    dst_conn = _connector("mysql", f"D214my{sfx}", myc)
    try:
        yield {"pg": pg, "my": my, "src_conn": src_conn, "dst_conn": dst_conn, **t}
    finally:
        _q(pg, f'DROP TABLE IF EXISTS "{t["src"]}"')
        for name in (t["dst"], f"{t['dst']}_df_quarantine"):
            _q(my, f"DROP TABLE IF EXISTS `{name}`")
        pg.close()
        my.close()
        delete_connector(src_conn.id)
        delete_connector(dst_conn.id)


def test_mysql_append_into_existing_utf8mb3_table_quarantines_db_refusals(mysql_route):
    r = mysql_route
    pg, my = r["pg"], r["my"]
    rows = [(i, i % 200, _text(i)) for i in range(1, N + 1)]
    rows[2] = (3, 2, "smile \U0001F600")  # inside the sample
    rows[N - 2] = (N - 1, 5, "\U0001F680 beyond")  # beyond it
    _pg_source(pg, r["src"], rows)
    _q(my, f"CREATE TABLE `{r['dst']}` (id INT PRIMARY KEY, small INT, "
           "note VARCHAR(40) CHARACTER SET utf8mb3) DEFAULT CHARSET=utf8mb3")
    _q(my, f"INSERT INTO `{r['dst']}` VALUES (100000, 1, 'prior')")
    bad = {3, N - 1}

    job_id, job = _append(r["src_conn"], r["src"], r["dst_conn"], r["dst"])
    _assert_quarantined(job, bad)
    got = _q(my, f"SELECT id, small, note FROM `{r['dst']}` ORDER BY id")
    assert got == sorted([row for row in rows if row[0] not in bad] + [(100000, 1, "prior")])

    _q(my, f"ALTER TABLE `{r['dst']}` CONVERT TO CHARACTER SET utf8mb4")
    _assert_replay_closes(job_id, len(bad))
    got = _q(my, f"SELECT id, small, note FROM `{r['dst']}` ORDER BY id")
    assert got == sorted(rows + [(100000, 1, "prior")])


def _signed(column: str, policy: str) -> dict:
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column=column, source_type="integer", destination_type="tinyint",
        approved_by="qa", reason="D214", execution_policy=policy, target=column,
        table="t",
    )
    return {"source": column, "target": column, "risk_contract": contract.to_dict()}


def test_the_contract_decides_a_database_refusal_and_nothing_else():
    """The rule under the e2e: which refused rows the signature covers."""
    from connectors.contract_salvage import contract_row_detail, salvage_policy

    maps = [_signed("small", "CAST_AND_CONTINUE"), {"source": "note", "target": "note"}]
    # The posture alone no longer refuses a Map that signs continue only.
    assert salvage_policy("fail", maps) == "quarantine"
    assert salvage_policy("quarantine", maps) == "quarantine"
    assert salvage_policy("fail", [{"source": "small", "target": "small"}]) == "fail"
    # A fail-closed signature anywhere keeps the batch error fatal.
    assert salvage_policy("fail", maps + [_signed("x", "FAIL_JOB")]) == "fail"

    signed = contract_row_detail({"row": 3, "column": "small", "policy": "fail"}, maps)
    assert (signed["policy"], signed["execution_policy"]) == ("quarantine", "CAST_AND_CONTINUE")
    # The driver did not name the column (SQL Server overflow): the job's signature.
    unnamed = contract_row_detail({"row": 3, "column": "*", "policy": "fail"}, maps)
    assert unnamed["execution_policy"] == "CAST_AND_CONTINUE"
    # A named column nobody signed keeps the strict posture.
    unsigned = {"row": 3, "column": "note", "policy": "fail"}
    assert contract_row_detail(dict(unsigned), maps) == unsigned
    # A quarantine posture is left exactly as it was.
    q = {"row": 3, "column": "small", "policy": "quarantine"}
    assert contract_row_detail(dict(q), maps) == q
