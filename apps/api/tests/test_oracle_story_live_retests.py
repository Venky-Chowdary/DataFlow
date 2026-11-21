"""Live Oracle retests of stories that only had unit or code-level evidence.

ENGINE_READINESS listed these as "fixed, needs live retest on Oracle": no CI
job ran an Oracle Database until oracle-live. Each case runs the QA route
(MCP ``start_transfer`` -> Confirm -> job) against the real engines the job
starts, and checks the destination cell by cell.

* D382: a ``flag eq true`` row filter on an Oracle ``NUMBER(1)`` flag keeps
  exactly the rows whose flag is 1. ``0`` and NULL rows are not loaded.
* D295 (AC-3): an Oracle ``BINARY_DOUBLE`` ratio column into SQL Server on
  ``incremental_upsert`` strict is not taken for a card number: the plan has
  no PII blocker, nothing is masked or hashed, and every double lands
  exactly, on the first run and on the upsert run after a source change.
* D362: PostgreSQL ``text`` with a value longer than 4000 bytes and NULLs into
  a NEW Oracle table (a CLOB carrier): the NULL-marker probe on the CLOB does
  not fail the run (``ORA-00932`` in QA), NULL stays NULL, and the long value
  lands exactly.
* Oracle ``''`` into NOT NULL (owner decision): PostgreSQL ``note TEXT NOT
  NULL`` holding empty strings into a NEW Oracle table is blocked at Validate,
  naming the column and the count; the signed ``oracle_empty_as_null`` option
  creates that column NULL-able, the run completes, and Gate-8 reports the rows
  as explained by the contract. Into an EXISTING NOT NULL column it stays
  blocked whatever is approved, and the table is not altered.

Skipped when Oracle (or the peer) is not configured; the oracle-live CI job
configures all of them.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h
from tests.helpers.live_env import oracle_creds, oracle_dsn

pytestmark = pytest.mark.timeout(900)


def _oracle_cfg() -> dict[str, Any]:
    c = oracle_creds()
    if not (os.environ.get("DATAFLOW_ORACLE_PASSWORD") or os.environ.get("DATAFLOW_TEST_ORACLE_PASSWORD")):
        pytest.skip("Oracle not configured (DATAFLOW_ORACLE_PASSWORD)")
    if not h.port_up(c["port"], c["host"]):
        pytest.skip(f"Oracle not reachable on :{c['port']}")
    pytest.importorskip("oracledb")
    return dict(
        type="oracle", host=c["host"], port=c["port"], database=c["database"],
        service_name=c["database"], username=c["username"], password=c["password"],
        schema=c["schema"],
    )


def _ora(sql: str, params: Any = None) -> list[tuple]:
    import oracledb

    oracledb.defaults.fetch_lobs = False  # CLOB cells come back as str
    cfg = _oracle_cfg()
    conn = oracledb.connect(user=cfg["username"], password=cfg["password"], dsn=oracle_dsn(oracle_creds()))
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
            _ora(f'DROP TABLE "{name}" PURGE')  # nosec B608 - test-owned name
        except Exception:  # noqa: BLE001 - absent is fine
            pass


def _ora_name(table: str) -> str:
    found = _ora("SELECT table_name FROM user_tables WHERE UPPER(table_name) = :t", {"t": table.upper()})
    assert len(found) == 1, (table, found)
    return str(found[0][0])


def _ora_columns(stored: str) -> dict[str, tuple[str, str]]:
    return {
        str(r[0]).lower(): (str(r[0]), str(r[1]).upper())
        for r in _ora(
            "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :t", {"t": stored}
        )
    }


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _connector(role: str, token: str, cfg: dict[str, Any]):
    from services.connector_store import create_connector

    return create_connector(
        {"name": f"orast-{role[0]}-{token}", "role": role, "workspace_id": "", "ssl": False, **cfg}
    )


def _stage(client, args: dict) -> dict:
    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    return staged


# --------------------------------------------------------------------- D382

def test_d382_flag_eq_true_on_oracle_number1_keeps_only_the_flagged_rows(client):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"D382_S_{token}".upper(), f"d382_d_{token}"
    ora = _oracle_cfg()
    _drop_ora(src_t)
    try:
        _ora(f'CREATE TABLE "{src_t}" (ID NUMBER(10) PRIMARY KEY, FLAG NUMBER(1), NAME VARCHAR2(20))')
        for rid, flag in ((1, 1), (2, 0), (3, None), (4, 1), (5, 0)):
            _ora(f'INSERT INTO "{src_t}" VALUES (:1, :2, :3)', [rid, flag, f"n{rid}"])
        src = _connector("source", token, ora)
        dst = _connector("destination", token, h.dest_config("postgresql"))
        staged = _stage(client, {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
            "source_filter": {"column": "FLAG", "op": "eq", "value": True},
        })
        h.confirm(client, staged["ack_id"], reason="d382")
        cols = {str(r[0]).lower(): str(r[0]) for r in h.query(
            "postgresql",
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{dst_t}'",
        )}
        rows = h.query("postgresql", f'SELECT "{cols["id"]}", "{cols["flag"]}" FROM "{dst_t}" ORDER BY 1')
        assert [(int(r[0]), int(r[1])) for r in rows] == [(1, 1), (4, 1)], rows
    finally:
        _drop_ora(src_t)
        h.drop_table("postgresql", dst_t)


# --------------------------------------------------------------------- D295

_RATIOS = [(i, (i + 1) / 7) for i in range(1, 9)]


def _mssql_rows(table: str) -> list[tuple]:
    return sorted(
        (int(r[0]), float(r[1]))
        for r in h.query("sqlserver", f"SELECT [ID], [RATIO] FROM [dbo].[{table}]")  # nosec B608
    )


def test_d295_oracle_binary_double_ratios_into_sqlserver_upsert_are_not_card_numbers(client):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"D295_S_{token}".upper(), f"D295_D_{token}".upper()
    ora = _oracle_cfg()
    _drop_ora(src_t)
    h.drop_table("sqlserver", dst_t)
    try:
        _ora(
            f'CREATE TABLE "{src_t}" (ID NUMBER(10) PRIMARY KEY, RATIO BINARY_DOUBLE, '
            "UPDATED_AT TIMESTAMP(6) DEFAULT TIMESTAMP '2026-10-01 00:00:00' NOT NULL)"
        )
        for rid, ratio in _RATIOS:
            _ora(f'INSERT INTO "{src_t}" (ID, RATIO) VALUES (:1, :2)', [rid, ratio])
        src = _connector("source", token, ora)
        dst = _connector("destination", token, h.dest_config("sqlserver"))
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "incremental_upsert", "primary_key": "ID",
            "cursor_column": "UPDATED_AT", "cursor_semantics": "modification_timestamp",
            "validation_mode": "strict",
        }
        staged = _stage(client, args)
        # AC-3: the ratio is not classified as a card number on the plan.
        text = str(staged).lower()
        assert "pci_pan" not in text and "card number" not in text, text[:2000]
        h.confirm(client, staged["ack_id"], reason="d295")
        assert _mssql_rows(dst_t) == sorted(_RATIOS)

        # Upsert run: two ratios change, one row is new.
        _ora(f'UPDATE "{src_t}" SET RATIO = :1, UPDATED_AT = TIMESTAMP \'2026-10-02 00:00:00\' '
             "WHERE ID = 3", [22 / 7])
        _ora(f'INSERT INTO "{src_t}" (ID, RATIO, UPDATED_AT) VALUES '
             "(9, :1, TIMESTAMP '2026-10-02 00:00:00')", [10 / 7])
        expected = dict(_RATIOS)
        expected[3] = 22 / 7
        expected[9] = 10 / 7
        staged = _stage(client, args)
        h.confirm(client, staged["ack_id"], reason="d295-2")
        assert _mssql_rows(dst_t) == sorted(expected.items())
    finally:
        _drop_ora(src_t)
        h.drop_table("sqlserver", dst_t)


# --------------------------------------------------------------------- D362

_LONG = "Ł" + ("abcdefghij" * 500) + "東京"  # > 4000 bytes: a CLOB carrier in Oracle


def test_d362_pg_text_into_a_new_oracle_clob_keeps_nulls_and_long_values(client):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d362_s_{token}", f"d362_d_{token}"
    ora = _oracle_cfg()
    _drop_ora(dst_t)
    try:
        h.execute("postgresql", f'CREATE TABLE "{src_t}" (id INT PRIMARY KEY, note TEXT NULL)')
        rows = {1: "short", 2: None, 3: _LONG, 4: "  spaced  "}
        for rid, note in rows.items():
            lit = "NULL" if note is None else "'" + note.replace("'", "''") + "'"
            h.execute("postgresql", f'INSERT INTO "{src_t}" (id, note) VALUES ({rid}, {lit})')
        src = _connector("source", token, h.dest_config("postgresql"))
        dst = _connector("destination", token, ora)
        staged = _stage(client, {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        })
        h.confirm(client, staged["ack_id"], reason="d362")
        stored = _ora_name(dst_t)
        cols = _ora_columns(stored)
        got = {
            int(r[0]): r[1]
            for r in _ora(f'SELECT "{cols["id"][0]}", "{cols["note"][0]}" FROM "{stored}"')  # nosec B608
        }
        assert got == rows, {k: (v[:40] if isinstance(v, str) else v) for k, v in got.items()}
    finally:
        h.drop_table("postgresql", src_t)
        _drop_ora(dst_t)



# ------------------------------------------- Oracle '' into NOT NULL (owner decision)

def _seed_empty_notes(src_t: str) -> None:
    h.execute("postgresql", f'CREATE TABLE "{src_t}" (id INT PRIMARY KEY, note TEXT NOT NULL)')
    h.execute("postgresql", f"""INSERT INTO "{src_t}" (id, note) VALUES (1, ''), (2, 'x'), (3, '')""")


def _start(**kwargs):
    from src.ai.copilot.transfer_tools import start_transfer

    return start_transfer(validation_mode="strict", **kwargs)


def _blocker(staged) -> dict:
    from services import oracle_empty_not_null as oe

    pf = (staged.output or {}).get("preflight") or {}
    found = [b for b in pf.get("blockers") or [] if oe.GATE_ID in (b.get("id"), b.get("gate"))]
    assert staged.success is False and len(found) == 1, {"error": staged.error, "blockers": pf.get("blockers")}
    return found[0]


def test_oracle_empty_strings_into_a_new_not_null_column_block_then_approve(client):
    from services import oracle_empty_not_null as oe

    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"oe_s_{token}", f"oe_d_{token}"
    ora = _oracle_cfg()
    _drop_ora(dst_t)
    try:
        _seed_empty_notes(src_t)
        src = _connector("source", token, h.dest_config("postgresql"))
        dst = _connector("destination", token, ora)
        args = dict(source_connector_id=src.id, source_table=src_t, dest_connector_id=dst.id,
                    dest_table=dst_t, sync_mode="full_refresh_overwrite")
        blocker = _blocker(_start(**args))
        assert "note" in blocker["message"] and "2 empty string(s)" in blocker["message"], blocker
        assert "source population" in blocker["message"], blocker
        option = next(a for a in blocker["suggested_actions"] if a["id"] == oe.OPTION)
        assert "Create note as NULL-able" in option["label"]
        assert not _ora("SELECT 1 FROM user_tables WHERE UPPER(table_name) = :t", {"t": dst_t.upper()})

        staged = _start(**args, risk_acceptance={
            "option": oe.OPTION, "columns": ["note"], "approved_by": "qa@example.com",
            "reason": "an empty note is an absent note",
        })
        assert staged.success, {"error": staged.error}
        job = h.confirm(client, staged.output["ack_id"], reason="oe")
        stored = _ora_name(dst_t)
        cols = _ora_columns(stored)
        nullable = dict(_ora(
            "SELECT LOWER(column_name), nullable FROM user_tab_columns WHERE table_name = :t", {"t": stored}
        ))
        assert nullable["note"] == "Y" and nullable["id"] == "N", nullable
        got = dict(_ora(f'SELECT "{cols["id"][0]}", "{cols["note"][0]}" FROM "{stored}"'))  # nosec B608
        assert {int(k): v for k, v in got.items()} == {1: None, 2: "x", 3: None}, got
        recon = job.get("reconciliation") or {}
        (explained,) = [e for e in recon.get("explained_changes") or [] if e.get("kind") == oe.RISK_KIND]
        assert explained["rows"] == 2 and explained["approved_by"] == "qa@example.com", explained
        assert "explained by signed Risk Contract" in str(recon.get("message")), recon.get("message")
    finally:
        h.drop_table("postgresql", src_t)
        _drop_ora(dst_t)


def test_oracle_empty_strings_into_an_existing_not_null_column_stay_blocked(client):
    from services import oracle_empty_not_null as oe

    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"oe_s_{token}", f"OE_X_{token}".upper()
    ora = _oracle_cfg()
    _drop_ora(dst_t)
    try:
        _seed_empty_notes(src_t)
        _ora(f'CREATE TABLE "{dst_t}" (ID NUMBER(10) PRIMARY KEY, NOTE VARCHAR2(20) NOT NULL)')
        src = _connector("source", token, h.dest_config("postgresql"))
        dst = _connector("destination", token, ora)
        args = dict(source_connector_id=src.id, source_table=src_t, dest_connector_id=dst.id,
                    dest_table=dst_t, sync_mode="append")
        blocker = _blocker(_start(**args))
        assert "existing NOT NULL column" in blocker["message"] and "2 empty string(s)" in blocker["message"]
        assert oe.OPTION not in [a["id"] for a in blocker["suggested_actions"]]
        # Approving the option does not unlock an existing column.
        _blocker(_start(**args, risk_acceptance={
            "option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r",
        }))
        assert _ora(
            "SELECT nullable FROM user_tab_columns WHERE table_name = :t AND column_name = 'NOTE'", {"t": dst_t}
        ) == [("N",)]
        assert _ora(f'SELECT COUNT(*) FROM "{dst_t}"') == [(0,)]  # nosec B608
    finally:
        h.drop_table("postgresql", src_t)
        _drop_ora(dst_t)
