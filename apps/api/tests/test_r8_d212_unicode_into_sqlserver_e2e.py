"""D212 (HIGH) — Oracle -> new SQL Server table corrupted CJK/emoji text and went green.

QA (production 557f556c): an Oracle -> SQL Server transfer into a new table
created the text columns as code-page ``VARCHAR``; '東京' and 🚀 were stored
as '??' and the job still completed, because the route verified row counts
only.

Root causes pinned here:

* the create-new carrier: SQL Server ``VARCHAR`` is promoted to ``NVARCHAR``
  only when the source was *measured* Unicode; an Oracle ``VARCHAR2`` (and a
  MySQL/MariaDB column introspected without a charset) was "unmeasured" and
  kept the code page. Now every source text goes to ``NVARCHAR`` unless the
  source declares a single-byte charset;
* the verification gap: a fast-path load proved ``COUNT(*)`` only. Now a
  sampled cell-value read-back always runs where the destination is
  readable, so a lossy write fails the job instead of passing it.

Routes: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, against
real Oracle Free, MySQL 8, MariaDB, PostgreSQL and SQL Server 2022; new table
(run 1 creates, run 2 upserts into it) and an existing table. A deliberately
code-page ``VARCHAR`` destination must fail verification.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

# id, city, note. NULL and '' ride along; Oracle stores '' as NULL.
_ROWS = [
    (1, "東京", "日本語"),
    (2, "🚀 launch", "é combining"),
    (3, None, ""),
    (4, "plain ascii", "  "),
    (5, "Łódź 山田 🎉", "Grüße ß"),
]
_RUN2 = [
    (1, "東京 updated 🚀", "日本語"),
    (6, "新しい 行", "ño"),
]


# --------------------------------------------------------------------------- #
# Source engines
# --------------------------------------------------------------------------- #


def _oracle_cfg() -> dict[str, Any]:
    pw = os.environ.get("DATAFLOW_TEST_ORACLE_PASSWORD", "")
    if not pw or not h.port_up(1521):
        pytest.skip("Oracle not configured (DATAFLOW_TEST_ORACLE_PASSWORD, :1521)")
    pytest.importorskip("oracledb")
    svc = os.environ.get("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1")
    user = os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system")
    return dict(
        type="oracle", host=os.environ.get("DATAFLOW_TEST_ORACLE_HOST", "127.0.0.1"),
        port=1521, database=svc, service_name=svc, username=user, password=pw,
        schema=os.environ.get("DATAFLOW_TEST_ORACLE_SCHEMA", user.upper()),
    )


def _mariadb_cfg() -> dict[str, Any]:
    pytest.importorskip("pymysql")
    pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
    if not pw or not h.port_up(3307):
        pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_PASSWORD, :3307)")
    return dict(
        type="mariadb", host=os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"),
        port=3307, database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
        username=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "root"), password=pw,
    )


def _ora_exec(sql: str, params: Any = None) -> None:
    import oracledb

    cfg = _oracle_cfg()
    conn = oracledb.connect(user=cfg["username"], password=cfg["password"],
                            dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
    finally:
        conn.close()


def _maria_exec(sql: str, params: Any = None) -> None:
    import pymysql

    cfg = _mariadb_cfg()
    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                           password=cfg["password"], database=cfg["database"],
                           charset="utf8mb4", autocommit=True)
    try:
        conn.cursor().execute(sql, params)
    finally:
        conn.close()


def _mysql_exec(sql: str, params: Any = None) -> None:
    conn = h.mysql_conn()
    try:
        conn.cursor().execute(sql, params)
    finally:
        conn.close()


class _Source:
    """One source engine: create, insert, drop, connector config."""

    def __init__(self, kind: str):
        self.kind = kind

    def cfg(self) -> dict[str, Any]:
        if self.kind == "oracle":
            return _oracle_cfg()
        if self.kind == "mariadb":
            return _mariadb_cfg()
        return h.dest_config(self.kind)

    def table(self, token: str) -> str:
        name = f"d212_s_{token}"
        return name.upper() if self.kind == "oracle" else name

    def create(self, table: str, rows) -> None:
        if self.kind == "oracle":
            _ora_exec(f'CREATE TABLE "{table}" (ID NUMBER(10) PRIMARY KEY, '
                      "CITY VARCHAR2(100) NULL, NOTE VARCHAR2(100) NULL)")
        elif self.kind == "postgresql":
            h.execute("postgresql", f'CREATE TABLE "{table}" (id INT PRIMARY KEY, '
                      "city VARCHAR(100) NULL, note TEXT NULL)")
        else:
            ddl = (f"CREATE TABLE `{table}` (id INT PRIMARY KEY, city VARCHAR(100) NULL, "
                   "note TEXT NULL) DEFAULT CHARSET=utf8mb4")
            (_mysql_exec if self.kind == "mysql" else _maria_exec)(ddl)
        self.upsert(table, rows)

    def upsert(self, table: str, rows) -> None:
        for rid, city, note in rows:
            if self.kind == "oracle":
                _ora_exec(f'DELETE FROM "{table}" WHERE ID = :1', [rid])
                _ora_exec(f'INSERT INTO "{table}" (ID, CITY, NOTE) VALUES (:1, :2, :3)',
                          [rid, city, note])
            elif self.kind == "postgresql":
                conn = h.pg_conn()
                try:
                    cur = conn.cursor()
                    cur.execute(f'DELETE FROM "{table}" WHERE id = %s', (rid,))
                    cur.execute(f'INSERT INTO "{table}" VALUES (%s, %s, %s)', (rid, city, note))
                finally:
                    conn.close()
            else:
                ex = _mysql_exec if self.kind == "mysql" else _maria_exec
                ex(f"DELETE FROM `{table}` WHERE id = %s", (rid,))
                ex(f"INSERT INTO `{table}` VALUES (%s, %s, %s)", (rid, city, note))

    def drop(self, table: str) -> None:
        try:
            if self.kind == "oracle":
                _ora_exec(f'DROP TABLE "{table}" PURGE')
            elif self.kind == "postgresql":
                h.drop_table("postgresql", table)
            else:
                (_mysql_exec if self.kind == "mysql" else _maria_exec)(
                    f"DROP TABLE IF EXISTS `{table}`")
        except Exception:
            pass

    def expected(self, rows) -> dict[int, tuple]:
        out = {}
        for rid, city, note in rows:
            if self.kind == "oracle":  # Oracle source: '' is NULL already
                city = None if city == "" else city
                note = None if note == "" else note
            out[rid] = (city, note)
        return out


# --------------------------------------------------------------------------- #
# SQL Server destination
# --------------------------------------------------------------------------- #


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")  # no background training at shutdown
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _ss_columns(table: str) -> dict[str, str]:
    rows = h.query("sqlserver", "SELECT LOWER(COLUMN_NAME), DATA_TYPE FROM "
                   f"INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
    return {str(r[0]): str(r[1]).lower() for r in rows}


def _ss_image(table: str) -> dict[int, tuple]:
    cols = _ss_columns(table)
    assert cols, f"SQL Server table {table} missing"
    rows = h.query("sqlserver", f"SELECT id, city, note FROM [{table}]")  # nosec B608
    return {int(r[0]): (r[1], r[2]) for r in rows}


def _connectors(source: _Source, token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"d212-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **source.cfg()})
    dst = create_connector({"name": f"d212-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **h.dest_config("sqlserver")})
    return src, dst


def _stage(client, args: dict) -> dict:
    ok, staged = h.mcp_call(client, "start_transfer", args)
    if not ok and os.environ.get("D212_DEBUG"):
        _ok, plan = h.mcp_call(client, "plan_transfer", dict(args))
        import json as _json
        with open(os.environ["D212_DEBUG"], "a") as fh:
            fh.write(_json.dumps({"args": args, "blockers": h.blockers_of(plan),
                                  "mappings": (plan.get("plan") or plan).get("mappings")
                                  if isinstance(plan, dict) else plan},
                                 default=str, ensure_ascii=False)[:6000] + "\n")
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    return staged


_TERMINAL = {"completed", "completed_with_quarantine", "failed", "partial", "cancelled"}


def _wait_job(job_id: str, timeout_s: float = 240.0) -> dict:
    import time

    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in _TERMINAL:
            return job
        time.sleep(0.25)
    return job


def _confirm_job(client, ack_id: str) -> dict:
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": "d212"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    job = _wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status")) in _TERMINAL, {
        k: job.get(k) for k in ("status", "phase", "message", "error")}
    return job


def _value_proof(job: dict) -> dict:
    """What the job's verification compared: values, or COUNT(*) only."""
    recon = job.get("reconciliation") or {}
    vc = recon.get("value_compare") or {}
    digest = bool(recon.get("source_checksum")) and bool(recon.get("target_checksum"))
    return {
        "passed": recon.get("passed"),
        "value_compare_ran": vc.get("ran") is True,
        "digest_compared": digest,
        "scope": recon.get("checksum_scope"),
        "message": str(recon.get("message") or "")[:400],
    }


def _assert_value_verified(job: dict) -> None:
    """The job completed and its verification compared cell values, not only COUNT(*)."""
    proof = _value_proof(job)
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"), proof)
    assert proof["passed"] is True, proof
    assert proof["value_compare_ran"] or proof["digest_compared"], proof


def _assert_exact(image: dict[int, tuple], want: dict[int, tuple]) -> None:
    assert set(image) == set(want), (sorted(image), sorted(want))
    for key in sorted(want):
        assert image[key] == want[key], (key, image[key], want[key])


_SOURCES = ["oracle", "mysql", "mariadb", "postgresql"]


@pytest.mark.parametrize("kind", _SOURCES)
def test_new_sqlserver_table_keeps_unicode_on_run_one_and_two(client, kind):
    """QA's route: run 1 creates the table, run 2 overwrites the table DataFlow created."""
    source = _Source(kind)
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = source.table(token), f"d212_new_{kind}_{token}"
    try:
        source.create(src_t, _ROWS)
        src, dst = _connectors(source, token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        }
        j1 = _confirm_job(client, _stage(client, args)["ack_id"])
        _assert_value_verified(j1)
        cols = _ss_columns(dst_t)
        assert cols.get("city") in {"nvarchar", "nchar"}, cols
        assert cols.get("note") in {"nvarchar", "nchar"}, cols
        _assert_exact(_ss_image(dst_t), source.expected(_ROWS))

        source.upsert(src_t, _RUN2)
        j2 = _confirm_job(client, _stage(client, args)["ack_id"])
        _assert_value_verified(j2)
        merged = {r[0]: r for r in _ROWS}
        merged.update({r[0]: r for r in _RUN2})
        _assert_exact(_ss_image(dst_t), source.expected(merged.values()))
    finally:
        source.drop(src_t)
        h.drop_table("sqlserver", dst_t)


@pytest.mark.parametrize("kind", _SOURCES)
def test_existing_nvarchar_table_keeps_unicode(client, kind):
    source = _Source(kind)
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = source.table(token), f"d212_ex_{kind}_{token}"
    try:
        source.create(src_t, _ROWS)
        # The destination's equality matches the source's (MySQL/MariaDB
        # default collations are accent-insensitive), so only the charset
        # question is under test here.
        coll = (" COLLATE Latin1_General_100_CI_AI_SC" if kind in {"mysql", "mariadb"}
                else "")
        h.execute("sqlserver", f"CREATE TABLE [{dst_t}] (id BIGINT NOT NULL PRIMARY KEY, "
                  f"city NVARCHAR(100){coll} NULL, note NVARCHAR(MAX){coll} NULL)")
        src, dst = _connectors(source, token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_append",
        }
        j1 = _confirm_job(client, _stage(client, args)["ack_id"])
        _assert_value_verified(j1)
        _assert_exact(_ss_image(dst_t), source.expected(_ROWS))
    finally:
        source.drop(src_t)
        h.drop_table("sqlserver", dst_t)


@pytest.mark.parametrize("kind", ["oracle", "postgresql"])
def test_deliberate_varchar_destination_fails_verification(client, kind):
    """A code-page VARCHAR destination that turns '東京' into '??' is never green."""
    source = _Source(kind)
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = source.table(token), f"d212_vc_{kind}_{token}"
    try:
        source.create(src_t, _ROWS)
        h.execute("sqlserver", f"CREATE TABLE [{dst_t}] (id BIGINT NOT NULL PRIMARY KEY, "
                  "city VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, "
                  "note VARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL)")
        src, dst = _connectors(source, token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_append",
        }
        # 1) Unsigned: preflight refuses the code-page sink and names the columns.
        ok, staged = h.mcp_call(client, "start_transfer", dict(args))
        refusal = str(staged) + " ".join(h.blockers_of(staged) if ok else [])
        assert not ok or h.blockers_of(staged), staged
        assert "city" in refusal.lower(), refusal[:600]

        # 2) Signed CAST_AND_CONTINUE: the run goes ahead, and whatever the
        # writer does, a lossy write is never reported green.
        signed = dict(args, risk_acceptance={
            "approved_by": "qa", "reason": "d212 probe",
            "execution_policy": "CAST_AND_CONTINUE", "on_cast_failure": "quarantine",
        })
        staged = _stage(client, signed)
        job = _confirm_job(client, staged["ack_id"])
        image = _ss_image(dst_t)
        lossy = any("?" in str(v) for row in image.values() for v in row if v)
        proof = _value_proof(job)
        status = str(job.get("status"))
        if lossy:
            assert status != "completed", (status, proof, image)
            assert proof["passed"] is False, proof
        elif status == "completed":
            _assert_exact(image, source.expected(_ROWS))
        else:
            # Cells the code page cannot hold were held out, not rewritten.
            assert status == "completed_with_quarantine", (status, proof, image)
    finally:
        source.drop(src_t)
        h.drop_table("sqlserver", dst_t)


@pytest.mark.parametrize("kind", ["oracle", "postgresql", "mysql"])
def test_lossy_code_page_write_never_reports_green(client, kind, monkeypatch):
    """The verification gap on its own: a code-page carrier that rewrites text fails.

    The pre-fix carrier rule is put back (SQL Server VARCHAR for this source),
    so the engine copy really stores '東京' as '??' — exactly QA's run. The
    job must not complete green: verification compares cell values (or, where
    no value compare can run, refuses a count-only verdict over a code-page
    text column).
    """
    import services.type_system as ts

    # 557f556c's planning: this source's text is taken for a code page, so
    # the create-new carrier stays VARCHAR and nothing flags the sink.
    monkeypatch.setattr(ts, "source_text_declared_single_byte", lambda *_a, **_kw: True)
    source = _Source(kind)
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = source.table(token), f"d212_lossy_{kind}_{token}"
    try:
        source.create(src_t, _ROWS)
        src, dst = _connectors(source, token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        }
        ok, staged = h.mcp_call(client, "start_transfer", args)
        if not ok or h.blockers_of(staged):
            pytest.fail(f"expected the lossy carrier to be planned (QA's route): {staged}")
        job = _confirm_job(client, staged["ack_id"])
        cols = _ss_columns(dst_t)
        image = _ss_image(dst_t) if cols else {}
        lossy = any("?" in str(v) for row in image.values() for v in row if v)
        proof = _value_proof(job)
        if lossy or cols.get("city") == "varchar":
            assert str(job.get("status")) not in {"completed"}, (job.get("status"), proof, image)
            assert proof["passed"] is not True, proof
        else:
            _assert_exact(image, source.expected(_ROWS))
    finally:
        source.drop(src_t)
        h.drop_table("sqlserver", dst_t)
