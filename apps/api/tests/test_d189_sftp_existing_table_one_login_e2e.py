"""D189 end to end (QA acceptance): an SFTP CSV with one bad row into an
EXISTING PostgreSQL / MySQL table, through MCP plan_transfer / start_transfer
/ confirm / job, with a real SFTP server's login cost.

QA (production ac612203): 5 of 5 start_transfer calls failed at the 45s
deadline ("source unreachable/slow: <SFTP host>") while plan_transfer passed,
and the SFTP server log showed 100+ connections. Measured here: every listing,
stat, spill download, preview and uniqueness probe opened its own SSH login,
10-16 per tool call and 14 per job. At 4s per login (a remote server's TCP +
key exchange + auth) a plan alone ran past 45s.

Now one authenticated SSH transport serves every consumer of an endpoint for
the length of a tool call (and an SFTP-only scope for the job). The test
counts logins per call.
"""

from __future__ import annotations

import os
import time
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

SFTP_LOGIN_SECONDS = 4.0
GOOD = 29


def _csv() -> str:
    lines = ["id,name,amount,joined"]
    special = {3: "", 5: '"  padded  "', 7: '"Zoë 東京 ✓"', 13: '""'}
    for i in range(1, GOOD + 1):
        lines.append(f"{i},{special.get(i, f'name {i}')},{i}.25,2024-01-{(i % 28) + 1:02d}")
    lines.append("30,bad row,not-a-number,2024-01-30")
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
            "CREATE TABLE public.{t} (id INTEGER PRIMARY KEY, name TEXT, amount NUMERIC(12,2), joined DATE)"
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
    return db, cfg, "CREATE TABLE {t} (id INT PRIMARY KEY, name TEXT, amount DECIMAL(12,2), joined DATE)"


@pytest.fixture
def route(request, tmp_path, monkeypatch):
    import paramiko

    from services import connector_executor
    from services.connector_store import create_connector, delete_connector
    from tests.sftp_test_server import start_sftp_server

    engine = request.param
    db, cfg, ddl = _dest(engine)
    root = tmp_path / "sftp"
    root.mkdir()
    (root / "people.csv").write_text(_csv(), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    scfg = server.endpoint_config("/people.csv")
    sfx = uuid.uuid4().hex[:8]
    table = f"d189e_{sfx}"
    cur = db.cursor()
    cur.execute(ddl.format(t=table))
    src = create_connector({
        "name": f"sftp-{sfx}", "type": "sftp", "role": "source", "host": scfg["host"],
        "port": scfg["port"], "username": scfg["username"], "password": scfg["password"],
        "database": scfg["database"], "host_key": scfg["host_key"],
    })
    dst = create_connector({"name": f"{engine}-{sfx}", "type": engine, "role": "destination", **cfg})
    logins: list[float] = []
    real_auth = paramiko.Transport.auth_password

    def slow_auth(self, *args, **kwargs):
        logins.append(time.monotonic())
        time.sleep(SFTP_LOGIN_SECONDS)
        return real_auth(self, *args, **kwargs)

    monkeypatch.setattr(paramiko.Transport, "auth_password", slow_auth)
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    connector_executor.reset_for_tests()
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "db": db, "logins": logins, "engine": engine}
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


def _call(tool: str, route: dict, *, contract: bool):
    from src.ai.copilot.tools import get_pilot_tools

    args = {
        "source_connector_id": route["src"], "source_table": "people.csv",
        "dest_connector_id": route["dst"], "dest_table": route["table"],
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
    }
    if contract:
        args["risk_acceptance"] = dict(CONTRACT)
    route["logins"].clear()
    started = time.monotonic()
    result = get_pilot_tools().execute(tool, args)
    return result, time.monotonic() - started, len(route["logins"])


def _job(ack_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    job_id = spend_pilot_ack(ack_id, "d189-test")["job_id"]
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _rows(route: dict) -> dict[int, tuple]:
    cur = route["db"].cursor()
    cur.execute(f"SELECT id, name, amount, joined FROM {route['table']} ORDER BY id")
    return {int(r[0]): tuple(r[1:]) for r in cur.fetchall()}


def _assert_good_rows(rows: dict[int, tuple], engine: str) -> None:
    assert sorted(rows) == list(range(1, GOOD + 1)), sorted(rows)
    assert 30 not in rows  # the bad row is held out
    assert rows[1][:2] == ("name 1", Decimal("1.25"))
    assert rows[3][0] is None  # unquoted empty -> NULL
    assert rows[13][0] == ""  # quoted "" -> empty string
    assert rows[5][0] == "  padded  "
    assert rows[7][0] == "Zoë 東京 ✓"
    for i in range(1, GOOD + 1):
        assert rows[i][1] == Decimal(f"{i}.25")
        assert str(rows[i][2]) == f"2024-01-{(i % 28) + 1:02d}"


@pytest.mark.parametrize("route", ["postgresql", "mysql"], indirect=True)
def test_csv_with_a_bad_row_into_an_existing_table_one_login_per_call(route):
    for run in (1, 2):  # the second run goes into the table the first one filled
        plan, plan_s, plan_logins = _call("plan_transfer", route, contract=True)
        assert plan.success, plan.error
        assert plan_s < 45, (run, plan_s)
        assert plan_logins == 1, (run, plan_logins)
        staged, start_s, start_logins = _call("start_transfer", route, contract=True)
        assert staged.success, staged.error
        assert start_s < 45, (run, start_s)
        assert start_logins == 1, (run, start_logins)
        route["logins"].clear()
        job = _job(staged.output["ack_id"])
        assert job.get("status") == "completed_with_quarantine", job
        # Base: 14 logins per job. The job shares one per endpoint too
        # (some reader paths run on threads of their own).
        assert len(route["logins"]) <= 4, len(route["logins"])
        _assert_good_rows(_rows(route), route["engine"])


@pytest.mark.parametrize("route", ["postgresql"], indirect=True)
def test_without_a_contract_the_bad_row_is_refused_within_budget(route):
    staged, start_s, logins = _call("start_transfer", route, contract=False)
    assert not staged.success
    assert start_s < 45 and logins == 1, (start_s, logins)
    assert "amount" in str(staged.error), staged.error
