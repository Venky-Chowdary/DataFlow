"""D210 (owner decision 4) — '' into Oracle is written as NULL, never a stale value.

QA (production 557f556c): PostgreSQL -> Oracle upsert, run 1 loads the table,
then a source cell changes to ``''`` and run 2 runs. The Oracle writer
"promoted" that row to a sparse write that *left the cell out* of the MERGE
SET, so the old value stayed in Oracle and Gate-8 failed.

Contract pinned here on QA's route (MCP ``start_transfer`` -> Confirm -> job,
REST ``/retry``), against real PostgreSQL and Oracle Free:

* a cell that changes to ``''`` is written as NULL together with the rest of
  the row (Oracle stores a zero-length VARCHAR2 as NULL); no stale value is
  ever kept — in upsert, overwrite and append;
* preflight warns once that Oracle stores ``''`` as NULL;
* the read-back / digest treat ``''`` and NULL as equal for the Oracle
  destination only, so the job completes green and the destination equals
  the source cell by cell with ``''`` expected as NULL;
* a retry re-reads the whole source, so a destination row tampered after the
  run is repaired;
* whitespace and unicode edits ride on every run and are kept exactly.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)


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


def _ora(sql: str, params: Any = None) -> list[tuple]:
    import oracledb

    oracledb.defaults.fetch_lobs = False  # CLOB cells come back as str
    cfg = _oracle_cfg()
    conn = oracledb.connect(
        user=cfg["username"], password=cfg["password"],
        dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _drop_ora(table: str) -> None:
    for name in {table, table.upper()}:
        try:
            _ora(f'DROP TABLE "{name}" PURGE')
        except Exception:
            pass


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


_RUN1 = [
    (1, "alpha", "keep-1"),
    (2, None, "keep-2"),
    (3, "  ", "keep-3"),
    (4, "Łukasz 山田 🎉", "keep-4"),
    (5, "five", ""),
    (6, "six", "keep-6"),
]
# Run 2 edits: a value -> '' (QA's case), '' -> value, whitespace, unicode, NULL.
_EDITS = {
    1: ("", "changed-1"),
    3: (" x ", "keep-3"),
    4: ("東京 é́ 🚀", "keep-4"),
    5: ("five", "now-set"),
    6: ("", ""),
}


def _lit(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def _create_source(table: str, rows) -> None:
    h.execute("postgresql", f'CREATE TABLE "{table}" (id INT PRIMARY KEY, note TEXT NULL, '
              "tag TEXT NULL, updated_at TIMESTAMP NOT NULL DEFAULT '2026-10-01 00:00:00')")
    for rid, note, tag in rows:
        h.execute("postgresql", f'INSERT INTO "{table}" (id, note, tag) VALUES '
                  f"({rid}, {_lit(note)}, {_lit(tag)})")


def _apply_edits(table: str) -> None:
    for rid, (note, tag) in _EDITS.items():
        h.execute("postgresql", f'UPDATE "{table}" SET note = {_lit(note)}, tag = {_lit(tag)}, '
                  f"updated_at = '2026-10-02 00:00:00' WHERE id = {rid}")


def _oracle_expected(rows) -> list[tuple]:
    """Source rows as Oracle stores them: '' is NULL, everything else exact."""
    return sorted(
        (rid, note if note != "" else None, tag if tag != "" else None)
        for rid, note, tag in rows
    )


def _source_rows(table: str) -> list[tuple]:
    rows = h.query("postgresql", f'SELECT id, note, tag FROM "{table}"')  # nosec B608
    return sorted((int(r[0]), r[1], r[2]) for r in rows)


def _ora_name(table: str) -> str:
    """The table's stored name (DataFlow may fold an unquoted name to upper case)."""
    found = _ora("SELECT table_name FROM user_tables WHERE UPPER(table_name) = :t",
                 {"t": table.upper()})
    assert len(found) == 1, (table, found)
    return str(found[0][0])


def _dest_rows(table: str) -> list[tuple]:
    table = _ora_name(table)
    cols = [
        str(r[0])
        for r in _ora(
            "SELECT column_name FROM user_tab_columns WHERE table_name = :t "
            "ORDER BY column_id", {"t": table},
        )
    ]
    assert cols, f"Oracle table {table} missing"
    lower = {c.lower(): c for c in cols}
    sel = ", ".join(f'"{lower[c]}"' for c in ("id", "note", "tag"))
    rows = _ora(f'SELECT {sel} FROM "{table}"')  # nosec B608
    return sorted(
        (int(r[0]) if isinstance(r[0], (Decimal, int, float)) else int(str(r[0])), r[1], r[2])
        for r in rows
    )


def _connectors(token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"d210-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    dst = create_connector({"name": f"d210-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **_oracle_cfg()})
    return src, dst


def _jid(job: dict) -> str:
    return str(job.get("job_id") or job.get("_id") or job.get("id") or "")


def _oracle_empty_warnings(payload: Any) -> list[str]:
    plan = payload.get("plan") or payload if isinstance(payload, dict) else {}
    pf = plan.get("preflight") or payload.get("preflight") or {}
    out = []
    for w in pf.get("warnings") or []:
        text = str(w.get("message") if isinstance(w, dict) else w)
        if "''" in text and "NULL" in text and "Oracle" in text:
            out.append(text)
    return out


def _run(client, args: dict, *, expect_warning: bool) -> dict:
    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    warnings = _oracle_empty_warnings(staged)
    if expect_warning:
        assert len(warnings) == 1, (warnings, (staged.get("preflight") or {}).get("warnings"))
    return h.confirm(client, staged["ack_id"], reason="d210")


def _assert_verified(job: dict) -> None:
    text = str(job).lower()
    assert "gate-8" not in str(job.get("error") or "").lower(), job.get("error")
    ver = job.get("verification") or job.get("verification_result") or {}
    if isinstance(ver, dict) and "passed" in ver:
        assert ver["passed"] is True, ver
    assert "stale" not in str(job.get("error") or ""), text[:400]


def test_upsert_empty_string_edit_writes_null_and_retry_repairs(client):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d210_s_{token}", f"d210_u_{token}"
    try:
        _create_source(src_t, _RUN1)
        src, dst = _connectors(token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "incremental_upsert", "primary_key": "id",
            "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
        }
        j1 = _run(client, args, expect_warning=True)
        _assert_verified(j1)
        assert _dest_rows(dst_t) == _oracle_expected(_source_rows(src_t))

        _apply_edits(src_t)
        j2 = _run(client, args, expect_warning=True)
        _assert_verified(j2)
        got = _dest_rows(dst_t)
        assert got == _oracle_expected(_source_rows(src_t)), got
        # QA's cell: row 1 note was 'alpha', is now '' -> NULL (never 'alpha').
        assert got[0] == (1, None, "changed-1"), got[0]

        # Tamper the destination, then retry: the retry re-reads the whole
        # source and repairs every caught row.
        name = _ora_name(dst_t)
        _ora(f'UPDATE "{name}" SET note = \'stale\' WHERE id IN (1, 6)')
        _ora(f'UPDATE "{name}" SET tag = \'stale\' WHERE id = 3')
        from services.mongodb_service import get_mongodb_service

        record = get_mongodb_service()._jobs[_jid(j2)]
        record["status"] = "failed"
        record["error"] = "d210 probe: attempt failed after commit"
        resp = client.post(f"/api/v1/connectors/jobs/{_jid(j2)}/retry")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["retry_decision"].get("read_scope") == "full_source", body
        j3 = h.wait_job(str(body["job_id"]))
        assert str(j3.get("status")) == "completed", (j3.get("status"), j3.get("error"))
        assert _dest_rows(dst_t) == _oracle_expected(_source_rows(src_t))
    finally:
        h.drop_table("postgresql", src_t)
        _drop_ora(dst_t)


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_overwrite_and_append_write_empty_as_null(client, mode):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d210_s_{token}", f"d210_{mode[:2]}_{token}"
    try:
        _create_source(src_t, _RUN1)
        src, dst = _connectors(token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite" if mode == "overwrite" else "full_refresh_append",
        }
        j1 = _run(client, args, expect_warning=True)
        _assert_verified(j1)
        assert _dest_rows(dst_t) == _oracle_expected(_source_rows(src_t))

        _apply_edits(src_t)
        if mode == "append":
            # Append adds rows: run 2 carries new keys (the edited values plus
            # one more row), so the key-declared table DataFlow created takes them.
            h.execute("postgresql", f'UPDATE "{src_t}" SET id = id + 100')
            h.execute("postgresql", f"INSERT INTO \"{src_t}\" (id, note, tag) VALUES (107, '', ' 🚀 ')")
        j2 = _run(client, args, expect_warning=True)
        _assert_verified(j2)
        expected = _oracle_expected(_source_rows(src_t))
        if mode == "append":
            expected = sorted(_oracle_expected(_RUN1) + expected)
        assert _dest_rows(dst_t) == expected
    finally:
        h.drop_table("postgresql", src_t)
        _drop_ora(dst_t)


def test_empty_equals_null_only_for_oracle_destinations():
    """The read-back relaxation is scoped: PG/MySQL/SQL Server keep ''!=NULL."""
    from services.oracle_empty_string import (
        empty_is_null_destination,
        normalize_oracle_empty,
    )

    assert empty_is_null_destination("oracle")
    assert empty_is_null_destination("oracle_autonomous")
    for other in ("postgresql", "mysql", "sqlserver", "mariadb", "snowflake", "", None):
        assert not empty_is_null_destination(other)
    assert normalize_oracle_empty("", "oracle") is None
    assert normalize_oracle_empty(" ", "oracle") == " "
    assert normalize_oracle_empty("", "postgresql") == ""


def test_overwrite_run_two_from_mysql_loads_the_staged_table(client):
    """Sibling: MySQL -> Oracle overwrite into the table DataFlow created.

    Every X -> Oracle engine copy built its INSERT before it switched to the
    staged table, so run 2 loaded the live table and the staged one stayed
    empty ("dest COUNT(*) 0 != source snapshot").
    """
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d210_m_{token}", f"d210_mo_{token}"
    conn = h.mysql_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE `{src_t}` (id INT PRIMARY KEY, note VARCHAR(60) NULL, "
                    "tag VARCHAR(60) NULL) DEFAULT CHARSET=utf8mb4")
        for rid, note, tag in _RUN1:
            cur.execute(f"INSERT INTO `{src_t}` VALUES (%s, %s, %s)", (rid, note, tag))
        src = create_connector({"name": f"d210-ms-{token}", "role": "source",
                                "workspace_id": "", "ssl": False, **h.dest_config("mysql")})
        dst = create_connector({"name": f"d210-md-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **_oracle_cfg()})
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        }
        _run(client, args, expect_warning=True)
        assert _dest_rows(dst_t) == _oracle_expected(_RUN1)
        for rid, (note, tag) in _EDITS.items():
            cur.execute(f"UPDATE `{src_t}` SET note = %s, tag = %s WHERE id = %s",
                        (note, tag, rid))
        cur.execute(f"SELECT id, note, tag FROM `{src_t}`")
        now = [(int(r[0]), r[1], r[2]) for r in cur.fetchall()]
        _run(client, args, expect_warning=True)
        assert _dest_rows(dst_t) == _oracle_expected(now)
    finally:
        try:
            conn.cursor().execute(f"DROP TABLE IF EXISTS `{src_t}`")
        finally:
            conn.close()
        _drop_ora(dst_t)
