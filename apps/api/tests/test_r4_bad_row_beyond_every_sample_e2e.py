"""A bad value no sample can see: SFTP CSV, ~5,000 rows, the only non-numeric
``amount`` at row 4,800, into an EXISTING PostgreSQL / MySQL table through MCP
plan_transfer / start_transfer / confirm / job (real SFTP server, real DBs).

Every plan / Validate sample reads the head of the file, so the plan cannot
name the row. The contract under test is the write path's:

* never a wrong value: the bad text is not written as NULL, 0, or a truncated
  number, and every other row lands exactly;
* (a) strict, no contract: refused at start_transfer by G3f, which scans the
  whole population (not a sample) and names the value; nothing is written;
* (b) balanced, no contract: completed_with_quarantine, that one row held out;
* (c) a signed CAST_AND_CONTINUE / quarantine contract: completed_with_quarantine
  with exactly that row held out.
"""

from __future__ import annotations

import os
import time
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

ROWS = 5000
BAD = 4800
PRIOR = [(900001, "prior a", Decimal("1.50")), (900002, None, Decimal("2.00"))]


def _amount(i: int) -> str:
    return f"{i}.{i % 100:02d}"


def _csv() -> str:
    lines = ["id,name,amount"]
    for i in range(1, ROWS + 1):
        amount = "12.5x" if i == BAD else _amount(i)
        name = "" if i % 997 == 0 else f"name {i}"
        lines.append(f"{i},{name},{amount}")
    return "\n".join(lines) + "\n"


def _dest(engine: str):
    if engine == "postgresql":
        psycopg2 = pytest.importorskip("psycopg2")
        from conftest import pg_test_cfg

        cfg = pg_test_cfg()
        try:
            db = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                                  user=cfg["username"], password=cfg["password"], connect_timeout=3)
        except psycopg2.OperationalError as exc:
            pytest.skip(f"PostgreSQL not reachable: {exc}")
        db.autocommit = True
        return db, {**cfg, "schema": "public"}, (
            "CREATE TABLE public.{t} (id INTEGER PRIMARY KEY, name TEXT, amount NUMERIC(12,2))"
        )
    pymysql = pytest.importorskip("pymysql")
    cfg = {
        "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
        "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
        "username": os.environ.get("DATAFLOW_TEST_MYSQL_USER", "dataflow"),
        "password": os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD", "dataflow"),
    }
    try:
        db = pymysql.connect(host=cfg["host"], port=cfg["port"], database=cfg["database"],
                             user=cfg["username"], password=cfg["password"], autocommit=True,
                             connect_timeout=3)
    except pymysql.MySQLError as exc:
        pytest.skip(f"MySQL not reachable: {exc}")
    return db, cfg, "CREATE TABLE {t} (id INT PRIMARY KEY, name TEXT, amount DECIMAL(12,2))"


@pytest.fixture
def route(request, tmp_path):
    pytest.importorskip("paramiko")
    from services.connector_store import create_connector, delete_connector
    from tests.sftp_test_server import start_sftp_server

    engine = request.param
    db, cfg, ddl = _dest(engine)
    root = tmp_path / "sftp"
    root.mkdir()
    (root / "big.csv").write_text(_csv(), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    scfg = server.endpoint_config("/big.csv")
    sfx = uuid.uuid4().hex[:8]
    table = f"beyond_{sfx}"
    cur = db.cursor()
    cur.execute(ddl.format(t=table))
    for row in PRIOR:
        cur.execute(f"INSERT INTO {table} (id, name, amount) VALUES (%s, %s, %s)", row)
    src = create_connector({
        "name": f"sftp-{sfx}", "type": "sftp", "role": "source", "host": scfg["host"],
        "port": scfg["port"], "username": scfg["username"], "password": scfg["password"],
        "database": scfg["database"], "host_key": scfg["host_key"],
    })
    dst = create_connector({"name": f"{engine}-{sfx}", "type": engine, "role": "destination", **cfg})
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "db": db, "engine": engine}
    finally:
        runner.stop()
        delete_connector(src.id)
        delete_connector(dst.id)
        for t in (table, f"{table}_df_quarantine"):
            try:
                cur.execute(f"DROP TABLE IF EXISTS {t}")
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        db.close()


CONTRACT = {
    "approved_by": "qa", "reason": "hold the bad row out",
    "execution_policy": "CAST_AND_CONTINUE", "on_cast_failure": "quarantine",
}


def _start(route: dict, *, mode: str, contract: bool):
    from src.ai.copilot.tools import get_pilot_tools

    args = {
        "source_connector_id": route["src"], "source_table": "big.csv",
        "dest_connector_id": route["dst"], "dest_table": route["table"],
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
        "validation_mode": mode,
    }
    if contract:
        args["risk_acceptance"] = dict(CONTRACT)
    return get_pilot_tools().execute("start_transfer", args)


def _job(ack_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    job_id = spend_pilot_ack(ack_id, "beyond-sample-test")["job_id"]
    deadline = time.time() + 300
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _rows(route: dict) -> dict[int, tuple]:
    cur = route["db"].cursor()
    cur.execute(f"SELECT id, name, amount FROM {route['table']} ORDER BY id")
    return {int(r[0]): (r[1], r[2]) for r in cur.fetchall()}


def _assert_loaded_exactly(rows: dict[int, tuple]) -> None:
    # Overwrite: the prior rows are replaced; every good row lands exactly.
    assert BAD not in rows, ("the bad row was written", rows.get(BAD))
    assert sorted(rows) == [i for i in range(1, ROWS + 1) if i != BAD]
    for i, (name, amount) in rows.items():
        assert amount == Decimal(_amount(i)), (i, amount)
        assert name == (None if i % 997 == 0 else f"name {i}"), (i, name)


def _assert_quarantined_exactly_one(job: dict) -> None:
    assert job.get("status") == "completed_with_quarantine", job
    assert int(job.get("rejected_rows") or 0) == 1, job
    assert int(job.get("quarantine_row_count") or 0) == 1, job
    assert int(job.get("coerced_null_rows") or 0) == 0, job  # never NULL-ed
    assert int(job.get("records_processed") or 0) == ROWS - 1, job


@pytest.mark.parametrize("route", ["postgresql", "mysql"], indirect=True)
def test_strict_without_a_contract_is_refused_before_anything_runs(route):
    # Observed: G3f reads the whole population (not a sample) and names the
    # value, so strict refuses at start_transfer; nothing is written.
    staged = _start(route, mode="strict", contract=False)
    assert not staged.success
    assert "amount" in str(staged.error) and "12.5x" in str(staged.error), staged.error
    assert _rows(route) == {r[0]: (r[1], r[2]) for r in PRIOR}


@pytest.mark.parametrize("route", ["postgresql", "mysql"], indirect=True)
def test_balanced_without_a_contract_quarantines_only_that_row(route):
    staged = _start(route, mode="balanced", contract=False)
    assert staged.success, staged.error
    job = _job(staged.output["ack_id"])
    _assert_quarantined_exactly_one(job)
    _assert_loaded_exactly(_rows(route))


@pytest.mark.parametrize("route", ["postgresql", "mysql"], indirect=True)
def test_signed_contract_quarantines_exactly_that_row(route):
    staged = _start(route, mode="strict", contract=True)
    assert staged.success, staged.error
    job = _job(staged.output["ack_id"])
    _assert_quarantined_exactly_one(job)
    _assert_loaded_exactly(_rows(route))
