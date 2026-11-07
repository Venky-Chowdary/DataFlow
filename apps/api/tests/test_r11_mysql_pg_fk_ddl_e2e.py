"""QA round 11 (build 9c75ada4), Addendum A — MySQL / MariaDB → PostgreSQL.

Item 2 — FK child tables. Rfam ``family`` / ``clan_membership`` could not be
copied on their own: the write-path FK orphan guard looked for the parent in
the *destination*, found nothing, and failed the run mid-write ("FK orphan
guard could not verify … refusing to write unverified child rows"), with no
override. Now the parent is probed once at plan time: absent → one warn
finding (G22), no guard, no constraint; present → the guard keeps orphans out,
sample orphans are a plan-time finding, ``fk_risk_acknowledged`` quarantines
them, and the route option ``foreign_keys: carry|guard|ignore`` decides.

Item 3 — DDL. ``int(10) unsigned`` / ``bigint(20)`` were pasted into the
PostgreSQL CREATE (syntax error), ``INT4 UNSIGNED`` fell through to TEXT, the
type system's own widenings (TINYINT UNSIGNED → SMALLINT, BIGINT UNSIGNED →
NUMERIC(20,0), YEAR → SMALLINT) were reported as fidelity collapse, BIT read as
base64, and a TINYINT(1) ``DEFAULT '0'`` became ``BOOLEAN DEFAULT 0`` — the
CREATE failed and the load died on "current transaction is aborted".

Every test drives the user's route — MCP ``start_transfer`` →
``/api/v1/copilot/confirm`` → job — on real MySQL 8 and MariaDB 11 into a real
PostgreSQL, strict validation, with NULL / '' / whitespace / unicode and max
values, then a second run into the table DataFlow created.
"""

from __future__ import annotations

import os
import socket
import time
import uuid
from decimal import Decimal

import pytest

pymysql = pytest.importorskip("pymysql")
psycopg2 = pytest.importorskip("psycopg2")

PG = {
    "host": os.environ.get("PGHOST") or "127.0.0.1",
    "port": int(os.environ.get("PGPORT") or 5432),
    "user": os.environ.get("PGUSER") or "dataflow",
    "password": os.environ.get("PGPASSWORD") or "dataflow",
    "dbname": os.environ.get("PGDATABASE") or "dataflow",
}


def _my_cfg(kind: str) -> dict:
    if kind == "mariadb":
        pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD")
        if not pw:
            pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_*)")
        return {
            "host": os.environ.get("DATAFLOW_TEST_MARIADB_HOST") or "127.0.0.1",
            "port": int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT") or 3307),
            "user": os.environ.get("DATAFLOW_TEST_MARIADB_USER") or "dataflow",
            "password": pw,
            "database": os.environ.get("DATAFLOW_TEST_MARIADB_DB") or "dataflow",
        }
    pw = os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD")
    if not pw:
        pytest.skip("MySQL not configured (DATAFLOW_TEST_MYSQL_*)")
    return {
        "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST") or "127.0.0.1",
        "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT") or 3306),
        "user": os.environ.get("DATAFLOW_TEST_MYSQL_USER") or "dataflow",
        "password": pw,
        "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB") or "dataflow",
    }


def _my(kind: str, *statements: str) -> None:
    cfg = _my_cfg(kind)
    try:
        conn = pymysql.connect(autocommit=True, connect_timeout=3, charset="utf8mb4", **cfg)
    except Exception as exc:  # environmental: server not running here
        pytest.skip(f"{kind} unavailable: {type(exc).__name__}")
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()


def _sql(statement: str, params=None) -> list[tuple]:
    try:
        socket.create_connection((PG["host"], PG["port"]), timeout=1).close()
        conn = psycopg2.connect(connect_timeout=3, **PG)
    except Exception as exc:  # environmental
        pytest.skip(f"PostgreSQL unavailable: {type(exc).__name__}")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


@pytest.fixture
def client(tmp_path, monkeypatch):
    for key, value in {
        "DATAFLOW_JOB_STORE": "memory",
        "DATAFLOW_DISABLE_OBJECT_STORE": "1",
        "DATAFLOW_PII_HASH_KEY": "test-pii-key",
        "DATAFLOW_EGRESS_ALLOWLIST": "127.0.0.0/8,::1/128",
        "DATAFLOW_PILOT_ACK_PATH": str(tmp_path / "acks.json"),
        "DATAFLOW_CONNECTOR_STORE": str(tmp_path / "connectors.json"),
        "DATAFLOW_CONNECTOR_STORE_BACKEND": "file",
        "DATAFLOW_PILOT_ENGINE": "local",
        "DATAFLOW_SEED_DEMO": "",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.ai.copilot import tools as tools_mod
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    monkeypatch.setattr(tools_mod, "_tools", None)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    with TestClient(app) as test_client:
        yield test_client


def _connector(kind: str, role: str) -> str:
    from services.connector_store import create_connector

    if kind in {"mysql", "mariadb"}:
        cfg = _my_cfg(kind)
        payload = {
            "host": cfg["host"], "port": cfg["port"], "database": cfg["database"],
            "username": cfg["user"], "password": cfg["password"],
        }
    else:
        payload = {
            "host": PG["host"], "port": PG["port"], "database": PG["dbname"],
            "username": PG["user"], "password": PG["password"], "schema": "public",
        }
    payload.update(
        name=f"r11-{kind}-{uuid.uuid4().hex[:6]}", type=kind, role=role, ssl=False,
        workspace_id="",
    )
    return str(create_connector(payload).id)


TERMINAL = {
    "completed", "completed_with_quarantine", "completed_with_warnings",
    "completed_with_contracted_holdouts", "failed", "partial", "cancelled",
}


def _wait(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in TERMINAL:
            return job
        time.sleep(0.25)
    return job


def _stage(**kwargs):
    from src.ai.copilot.transfer_tools import start_transfer

    kwargs.setdefault("validation_mode", "strict")
    kwargs.setdefault("sync_mode", "full_refresh_overwrite")
    return start_transfer(**kwargs)


def _run(client, expect: str = "completed", **kwargs) -> tuple[dict, dict]:
    """``start_transfer`` → ``/copilot/confirm`` → finished job."""
    staged = _stage(**kwargs)
    out = staged.output or {}
    pre = (out.get("plan") or {}).get("preflight") or out.get("preflight") or {}
    assert staged.success, {"error": staged.error, "blockers": pre.get("blockers")}
    confirmed = client.post(
        "/api/v1/copilot/confirm", json={"ack_id": out["ack_id"], "reason": "r11"}
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    job = _wait(str(body.get("job_id") or ""))
    assert job.get("status") == expect, {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
    }
    return out.get("plan") or {}, job


def _pg_fks(table: str) -> int:
    return int(
        _sql(
            "SELECT count(*) FROM pg_constraint WHERE contype = 'f' "
            "AND conrelid = %s::regclass",
            (f"public.{table}",),
        )[0][0]
    )


def _gate(pre: dict, gate_id: str) -> dict:
    return next((g for g in pre.get("gates") or [] if g.get("id") == gate_id), {})


KINDS = ["mysql", "mariadb"]


# ------------------------------------------------------------------ item 2


@pytest.fixture
def fk_schema(request):
    kind = request.param
    tag = uuid.uuid4().hex[:6]
    par, chi = f"r11par_{tag}", f"r11chi_{tag}"
    _my(
        kind,
        f"CREATE TABLE {par} (id INT PRIMARY KEY, n VARCHAR(10)) ENGINE=InnoDB",
        f"CREATE TABLE {chi} (id INT PRIMARY KEY, pid INT NOT NULL, v VARCHAR(20), "
        f"FOREIGN KEY (pid) REFERENCES {par}(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        f"INSERT INTO {par} VALUES (1, 'a'), (2, 'b')",
        f"INSERT INTO {chi} VALUES (1, 1, 'x'), (2, 2, NULL), (3, 1, ''), (4, 2, ' 日本 ')",
    )
    names = {"kind": kind, "par": par, "chi": chi, "dst": f"r11d_{tag}"}
    try:
        yield names
    finally:
        _my(kind, f"DROP TABLE IF EXISTS {chi}", f"DROP TABLE IF EXISTS {par}")
        _sql(f"DROP TABLE IF EXISTS {names['dst']}")
        _sql(f"DROP TABLE IF EXISTS {par}")


CHILD_ROWS = [(1, 1, "x"), (2, 2, None), (3, 1, ""), (4, 2, " 日本 ")]


@pytest.mark.parametrize("fk_schema", KINDS, indirect=True)
def test_child_table_alone_copies_with_plan_time_parent_finding(client, fk_schema):
    """QA's case: the parent is not at the destination. Plan says so; run lands."""
    src = _connector(fk_schema["kind"], "source")
    dst = _connector("postgresql", "destination")
    for _run_no in (1, 2):
        plan, job = _run(
            client, source_connector_id=src, source_table=fk_schema["chi"],
            dest_connector_id=dst, dest_table=fk_schema["dst"], primary_key="id",
        )
        pre = plan.get("preflight") or {}
        g22 = _gate(pre, "g22_dest_referential_integrity")
        assert g22.get("status") == "warn", g22
        assert fk_schema["par"] in g22.get("message", ""), g22
        assert "not at the destination" in g22.get("message", ""), g22
        got = _sql(f"SELECT id, pid, v FROM {fk_schema['dst']} ORDER BY id")
        assert got == CHILD_ROWS  # NULL stays NULL, '' stays '', unicode exact
        # No constraint to a parent that is not there.
        assert _pg_fks(fk_schema["dst"]) == 0


def _seed_dest_parent(table: str, ids: list[int]) -> None:
    _sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, n TEXT)")
    for i in ids:
        _sql(f"INSERT INTO {table} VALUES (%s, %s)", (i, f"p{i}"))


@pytest.mark.parametrize("fk_schema", KINDS, indirect=True)
def test_parent_present_carries_constraint_and_guard_option_does_not(client, fk_schema):
    _seed_dest_parent(fk_schema["par"], [1, 2])
    src = _connector(fk_schema["kind"], "source")
    dst = _connector("postgresql", "destination")
    _run(
        client, source_connector_id=src, source_table=fk_schema["chi"],
        dest_connector_id=dst, dest_table=fk_schema["dst"], primary_key="id",
    )
    assert _sql(f"SELECT id, pid, v FROM {fk_schema['dst']} ORDER BY id") == CHILD_ROWS
    assert _pg_fks(fk_schema["dst"]) == 1  # default 'carry'
    _sql(f"DROP TABLE {fk_schema['dst']}")
    _run(
        client, source_connector_id=src, source_table=fk_schema["chi"],
        dest_connector_id=dst, dest_table=fk_schema["dst"], primary_key="id",
        foreign_keys="guard",
    )
    assert _sql(f"SELECT id, pid, v FROM {fk_schema['dst']} ORDER BY id") == CHILD_ROWS
    assert _pg_fks(fk_schema["dst"]) == 0


@pytest.mark.parametrize("fk_schema", KINDS, indirect=True)
def test_orphans_are_one_plan_time_finding_ack_quarantines_ignore_copies(client, fk_schema):
    """Parent present but missing id 2: refused at plan time, not mid-run."""
    _seed_dest_parent(fk_schema["par"], [1])
    src = _connector(fk_schema["kind"], "source")
    dst = _connector("postgresql", "destination")
    common = dict(
        source_connector_id=src, source_table=fk_schema["chi"],
        dest_connector_id=dst, dest_table=fk_schema["dst"], primary_key="id",
    )
    staged = _stage(**common)
    assert not staged.success
    pre = ((staged.output or {}).get("plan") or {}).get("preflight") or (
        (staged.output or {}).get("preflight") or {}
    )
    blocker = next((b for b in pre.get("blockers") or [] if b.get("id") == "constraint_fk"), {})
    assert "reference a parent row the destination does not hold" in str(
        blocker.get("message")
    ), pre.get("blockers")
    # Refused before a row moved: nothing was created at the destination.
    assert _sql("SELECT to_regclass(%s)", (f"public.{fk_schema['dst']}",)) == [(None,)]

    # Acknowledged: the plan clears and the orphans are held out, not written.
    _plan, job = _run(
        client, expect="completed_with_quarantine", fk_risk_acknowledged=True, **common
    )
    assert _sql(f"SELECT id, pid, v FROM {fk_schema['dst']} ORDER BY id") == [
        (1, 1, "x"), (3, 1, ""),
    ]
    _sql(f"DROP TABLE {fk_schema['dst']}")

    # foreign_keys=ignore: copied as read, no guard, no constraint.
    for _run_no in (1, 2):
        _run(client, foreign_keys="ignore", **common)
        assert _sql(f"SELECT id, pid, v FROM {fk_schema['dst']} ORDER BY id") == CHILD_ROWS
        assert _pg_fks(fk_schema["dst"]) == 0


def test_unknown_foreign_keys_option_is_refused():
    from src.ai.copilot.transfer_tools import plan_transfer

    res = plan_transfer(source_table="t", foreign_keys="keep")
    assert not res.success
    assert "foreign_keys='keep'" in (res.error or "")


# ------------------------------------------------------------------ item 3

TYPES_DDL = (
    "CREATE TABLE {t} (id INT(10) UNSIGNED NOT NULL PRIMARY KEY, "
    "ti TINYINT(3) UNSIGNED, si SMALLINT(5) UNSIGNED, mi MEDIUMINT(8) UNSIGNED, "
    "ii INT(11) UNSIGNED, bi BIGINT(20) UNSIGNED, sti TINYINT(4), sbi BIGINT(20), "
    "e ENUM('a','b','','日本'), y YEAR(4), b1 BIT(1), b8 BIT(8), "
    "flag TINYINT(1) DEFAULT '0', v VARCHAR(40)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
)
TYPES_ROWS = [
    "(4294967295,255,65535,16777215,4294967295,18446744073709551615,-128,"
    "-9223372036854775808,'日本',2155,b'1',b'11111111',1,'日本 ✓')",
    "(1,0,0,0,0,0,127,9223372036854775807,'',1901,b'0',b'00000000',0,'')",
    "(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
    "(3,1,1,1,1,1,1,1,'a',2000,b'1',b'10101010',0,'   ')",
]
TYPES_EXPECTED = [
    (1, 0, 0, 0, 0, Decimal(0), 127, 9223372036854775807, "", 1901, False, "00000000", False, ""),
    (2, None, None, None, None, None, None, None, None, None, None, None, None, None),
    (3, 1, 1, 1, 1, Decimal(1), 1, 1, "a", 2000, True, "10101010", False, "   "),
    (
        4294967295, 255, 65535, 16777215, 4294967295, Decimal("18446744073709551615"),
        -128, -9223372036854775808, "日本", 2155, True, "11111111", True, "日本 ✓",
    ),
]


@pytest.fixture
def types_table(request):
    kind = request.param
    tag = uuid.uuid4().hex[:6]
    names = {"kind": kind, "src": f"r11typ_{tag}", "dst": f"r11typd_{tag}"}
    _my(kind, TYPES_DDL.format(t=names["src"]), f"INSERT INTO {names['src']} VALUES " + ",".join(TYPES_ROWS))
    try:
        yield names
    finally:
        _my(kind, f"DROP TABLE IF EXISTS {names['src']}")
        _sql(f"DROP TABLE IF EXISTS {names['dst']}")


def _pg_types(table: str) -> dict[str, str]:
    return {
        str(n): str(t)
        for n, t in _sql(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
            "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped",
            (f"public.{table}",),
        )
    }


@pytest.mark.parametrize("types_table", KINDS, indirect=True)
def test_unsigned_width_enum_year_bit_land_exact_run1_run2(client, types_table):
    src = _connector(types_table["kind"], "source")
    dst = _connector("postgresql", "destination")
    for _run_no in (1, 2):
        _run(
            client, source_connector_id=src, source_table=types_table["src"],
            dest_connector_id=dst, dest_table=types_table["dst"], primary_key="id",
        )
        types = _pg_types(types_table["dst"])
        # Every value fits; no MySQL token, no TEXT carrier for an integer.
        assert types["id"] == "bigint"
        assert types["ti"] == "smallint"
        assert types["bi"] == "numeric(20,0)"
        assert types["sti"] == "smallint" and types["sbi"] == "bigint"
        assert types["y"] in {"smallint", "integer", "bigint"}
        assert types["b1"] == "boolean" and types["b8"] == "bit(8)"
        got = _sql(
            f"SELECT id, ti, si, mi, ii, bi, sti, sbi, e::text, y, b1, b8::text, flag, v "
            f"FROM {types_table['dst']} ORDER BY id"
        )
        assert got == TYPES_EXPECTED


@pytest.fixture
def rfam_family(request):
    """The shape of Rfam ``family`` (QA's MariaDB table), parent included."""
    kind = request.param
    tag = uuid.uuid4().hex[:6]
    wiki, fam = f"r11wiki_{tag}", f"r11fam_{tag}"
    _my(
        kind,
        f"CREATE TABLE {wiki} (auto_wiki int(10) unsigned NOT NULL AUTO_INCREMENT, "
        "title varchar(150) NOT NULL DEFAULT '', PRIMARY KEY (auto_wiki)) ENGINE=InnoDB",
        f"CREATE TABLE {fam} (rfam_acc varchar(7) NOT NULL, auto_wiki int(10) unsigned NOT NULL, "
        "description varchar(75) DEFAULT NULL, gathering_cutoff double(5,2) DEFAULT NULL, "
        "num_seed bigint(20) DEFAULT NULL, ecmli_cal_db mediumint(9) DEFAULT '0', "
        "match_pair_node tinyint(1) DEFAULT '0', created datetime NOT NULL, "
        "PRIMARY KEY (rfam_acc), KEY (auto_wiki), "
        f"CONSTRAINT fk_{tag} FOREIGN KEY (auto_wiki) REFERENCES {wiki} (auto_wiki)) "
        "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        f"INSERT INTO {wiki} VALUES (1,'T1'),(2,'T2')",
        f"INSERT INTO {fam} VALUES ('RF00001',1,'5S ✓',38.5,4294967296,0,1,'2013-10-03 20:41:44'),"
        "('RF00002',2,NULL,NULL,NULL,NULL,NULL,'2013-10-03 20:41:44'),"
        "('RF00003',1,'',0,0,8388607,0,'2013-10-03 20:41:44'),"
        "('RF00004',2,'  ',-1.25,-1,-8388608,0,'2013-10-03 20:41:44')",
    )
    names = {"kind": kind, "wiki": wiki, "fam": fam, "dst": f"r11famd_{tag}"}
    try:
        yield names
    finally:
        _my(kind, f"DROP TABLE IF EXISTS {fam}", f"DROP TABLE IF EXISTS {wiki}")
        _sql(f"DROP TABLE IF EXISTS {names['dst']}")


@pytest.mark.parametrize("rfam_family", KINDS, indirect=True)
def test_rfam_family_shape_lands_without_transaction_abort(client, rfam_family):
    src = _connector(rfam_family["kind"], "source")
    dst = _connector("postgresql", "destination")
    for _run_no in (1, 2):
        _run(
            client, source_connector_id=src, source_table=rfam_family["fam"],
            dest_connector_id=dst, dest_table=rfam_family["dst"], primary_key="rfam_acc",
        )
        types = _pg_types(rfam_family["dst"])
        assert types["auto_wiki"] == "bigint"
        assert types["match_pair_node"] == "boolean"
        default = _sql(
            "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d JOIN pg_attribute a "
            "ON a.attrelid = d.adrelid AND a.attnum = d.adnum "
            "WHERE d.adrelid = %s::regclass AND a.attname = 'match_pair_node'",
            (f"public.{rfam_family['dst']}",),
        )
        assert default == [("false",)]
        got = _sql(
            f"SELECT rfam_acc, auto_wiki, description, num_seed, ecmli_cal_db, match_pair_node "
            f"FROM {rfam_family['dst']} ORDER BY rfam_acc"
        )
        assert got == [
            ("RF00001", 1, "5S ✓", 4294967296, 0, True),
            ("RF00002", 2, None, None, None, None),
            ("RF00003", 1, "", 0, 8388607, False),
            ("RF00004", 2, "  ", -1, -8388608, False),
        ]
