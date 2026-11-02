"""Engine-copy append: a key range is skipped only when it holds the source's keys.

Found while fixing QA D210: the X -> PostgreSQL / MySQL / SQL Server / Oracle
engine copies resume a crashed load by skipping a primary-key range whose
destination ``COUNT(*)`` equals the source range's count. Appending N *new*
key rows into a key-declared table that already held N rows matched on the
count (one unbounded range covers the whole table), every range was skipped,
nothing was written, and the run failed "N source row(s) are not in this
run's dest COUNT(*) growth (0)".

Contract, on the route a user drives (MCP ``start_transfer`` ->
``/api/v1/copilot/confirm`` -> job) against real engines:

* run 1 appends N rows into a new table; run 2 appends N rows with new keys;
  the destination holds all 2N rows exactly and the job completes;
* a genuine resume still skips: after run 1 into a 4-range plan, the rows of
  the last two ranges are removed (an attempt that finished ranges 1-2), the
  append runs again, the two complete ranges are skipped and only the
  missing ones load — the destination equals the source exactly.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)


# --------------------------------------------------------------------------- #
# Engines
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


def _ora(sql: str, params: Any = None, many: bool = False) -> list[tuple]:
    import oracledb

    cfg = _oracle_cfg()
    conn = oracledb.connect(user=cfg["username"], password=cfg["password"],
                            dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            if many:
                cur.executemany(sql, params)
                return []
            cur.execute(sql, params or [])
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _cfg(kind: str) -> dict[str, Any]:
    return _oracle_cfg() if kind == "oracle" else h.dest_config(kind)


def _ora_name(table: str) -> str:
    found = _ora("SELECT table_name FROM user_tables WHERE UPPER(table_name) = :t",
                 {"t": table.upper()})
    assert len(found) == 1, (table, found)
    return str(found[0][0])


def _create_source(kind: str, table: str, rows: list[tuple[int, str]]) -> None:
    if kind == "oracle":
        _ora(f'CREATE TABLE "{table}" (ID NUMBER(10) PRIMARY KEY, NOTE VARCHAR2(60))')
        _ora(f'INSERT INTO "{table}" (ID, NOTE) VALUES (:1, :2)', rows, many=True)
        return
    q = h.quote(kind, table)
    h.execute(kind, f"CREATE TABLE {q} (id INT NOT NULL PRIMARY KEY, note VARCHAR(60) NULL)")
    _insert_source(kind, table, rows)


def _insert_source(kind: str, table: str, rows: list[tuple[int, str]]) -> None:
    if kind == "oracle":
        _ora(f'INSERT INTO "{table}" (ID, NOTE) VALUES (:1, :2)', rows, many=True)
        return
    q = h.quote(kind, table)
    values = ", ".join(f"({i}, '{n}')" for i, n in rows)
    h.execute(kind, f"INSERT INTO {q} (id, note) VALUES {values}")  # nosec B608


def _replace_source(kind: str, table: str, rows: list[tuple[int, str]]) -> None:
    if kind == "oracle":
        _ora(f'DELETE FROM "{table}"')
    else:
        h.execute(kind, f"DELETE FROM {h.quote(kind, table)}")  # nosec B608
    _insert_source(kind, table, rows)


def _dest_rows(kind: str, table: str) -> list[tuple[int, str]]:
    if kind == "oracle":
        name = _ora_name(table)
        cols = {str(r[0]).lower(): str(r[0]) for r in _ora(
            "SELECT column_name FROM user_tab_columns WHERE table_name = :t", {"t": name})}
        rows = _ora(f'SELECT "{cols["id"]}", "{cols["note"]}" FROM "{name}"')  # nosec B608
    elif kind == "postgresql":
        cols = {str(r[0]).lower(): str(r[0]) for r in h.query(
            kind, "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table}'")}
        rows = h.query(kind, f'SELECT "{cols["id"]}", "{cols["note"]}" '
                       f"FROM {h.quote(kind, table)}")  # nosec B608
    else:
        rows = h.query(kind, f"SELECT id, note FROM {h.quote(kind, table)}")  # nosec B608
    return sorted((int(r[0]) if not isinstance(r[0], str) else int(r[0]), r[1]) for r in rows)


def _delete_dest_keys_from(kind: str, table: str, first: int) -> None:
    if kind == "oracle":
        _ora(f'DELETE FROM "{_ora_name(table)}" WHERE ID >= {int(first)}')  # nosec B608
    elif kind == "postgresql":
        h.execute(kind, f"DELETE FROM {h.quote(kind, table)} WHERE "
                  f'{"\"ID\"" if _pg_upper(table) else "id"} >= {int(first)}')
    else:
        h.execute(kind, f"DELETE FROM {h.quote(kind, table)} WHERE id >= {int(first)}")


def _pg_upper(table: str) -> bool:
    return bool(h.query("postgresql", "SELECT 1 FROM information_schema.columns "
                        f"WHERE table_name = '{table}' AND column_name = 'ID'"))


def _drop(kind: str, table: str) -> None:
    if kind == "oracle":
        for name in {table, table.upper()}:
            try:
                _ora(f'DROP TABLE "{name}" PURGE')
            except Exception:
                pass
    else:
        h.drop_table(kind, table)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _run(client, src_id: str, src_t: str, dst_id: str, dst_t: str) -> dict:
    ok, staged = h.mcp_call(client, "start_transfer", {
        "source_connector_id": src_id, "source_table": src_t,
        "dest_connector_id": dst_id, "dest_table": dst_t,
        "sync_mode": "full_refresh_append",
    })
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    resp = client.post("/api/v1/copilot/confirm",
                       json={"ack_id": staged["ack_id"], "reason": "append-fastpath"})
    assert resp.status_code == 200, resp.text
    return h.wait_job(str(resp.json().get("job_id") or ""), timeout_s=240)


def _summary(job: dict) -> dict:
    ds = job.get("dest_summary") or job.get("destination_summary") or {}
    return {
        "status": job.get("status"),
        "error": str(job.get("error") or job.get("message") or "")[:400],
        "load_method": ds.get("load_method") if isinstance(ds, dict) else None,
        "partitions_skipped": ds.get("partitions_skipped") if isinstance(ds, dict) else None,
    }


def _connectors(src_kind: str, dst_kind: str, token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"afp-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **_cfg(src_kind)})
    dst = create_connector({"name": f"afp-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **_cfg(dst_kind)})
    return src, dst


_PAIRS = [
    ("postgresql", "postgresql"),
    ("postgresql", "oracle"),
    ("postgresql", "sqlserver"),
    ("postgresql", "mysql"),
    ("oracle", "postgresql"),
    ("oracle", "sqlserver"),
    ("oracle", "mysql"),
    ("mysql", "postgresql"),
    ("mysql", "sqlserver"),
    ("mysql", "oracle"),
    ("sqlserver", "postgresql"),
    ("sqlserver", "oracle"),
]
_IDS = [f"{s}-{d}" for s, d in _PAIRS]


def _names(src_kind: str, dst_kind: str, token: str) -> tuple[str, str]:
    src_t = f"afp_s_{token}"
    dst_t = f"afp_d_{token}"
    return (src_t.upper() if src_kind == "oracle" else src_t), dst_t


@pytest.mark.parametrize(("src_kind", "dst_kind"), _PAIRS, ids=_IDS)
def test_append_of_n_new_keys_into_n_rows_is_written(client, src_kind, dst_kind):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = _names(src_kind, dst_kind, token)
    run1 = [(i, f"first {i}") for i in range(1, 7)]
    run2 = [(i, f"second {i}") for i in range(101, 107)]
    try:
        _create_source(src_kind, src_t, run1)
        src, dst = _connectors(src_kind, dst_kind, token)
        j1 = _run(client, src.id, src_t, dst.id, dst_t)
        assert str(j1.get("status")) == "completed", _summary(j1)
        assert _dest_rows(dst_kind, dst_t) == run1

        _replace_source(src_kind, src_t, run2)
        j2 = _run(client, src.id, src_t, dst.id, dst_t)
        assert str(j2.get("status")) == "completed", _summary(j2)
        assert _dest_rows(dst_kind, dst_t) == sorted(run1 + run2), _summary(j2)
    finally:
        _drop(src_kind, src_t)
        _drop(dst_kind, dst_t)


def _resume_copy(src_kind: str, dst_kind: str, src_t: str, dst_t: str, *, replace: bool):
    """One call of the engine copy itself — the API a crashed load is resumed through."""
    src_cfg, dst_cfg = _cfg(src_kind), _cfg(dst_kind)
    if src_kind == "oracle":
        pairs = [("ID", "id"), ("NOTE", "note")]
    else:
        pairs = [("id", "id"), ("note", "note")]
    if (src_kind, dst_kind) == ("postgresql", "oracle"):
        from services.copy_pg_oracle import copy_postgres_to_oracle

        return copy_postgres_to_oracle(
            source_cfg=src_cfg, source_schema="public", source_table=src_t,
            dest_cfg=dst_cfg, dest_table=dst_t, pairs=pairs,
            oracle_ddls=["NUMBER(10)", "VARCHAR2(60)"], replace_destination=replace)
    if (src_kind, dst_kind) == ("postgresql", "sqlserver"):
        from services.copy_pg_sqlserver import copy_postgres_to_sqlserver

        return copy_postgres_to_sqlserver(
            source_cfg=src_cfg, source_schema="public", source_table=src_t,
            dest_cfg=dst_cfg, dest_table=dst_t, pairs=pairs,
            sqlserver_ddls=["INT", "NVARCHAR(60)"], replace_destination=replace)
    if (src_kind, dst_kind) == ("oracle", "sqlserver"):
        from services.copy_oracle_sqlserver import copy_oracle_to_sqlserver

        return copy_oracle_to_sqlserver(
            source_cfg=src_cfg, source_table=src_t, dest_cfg=dst_cfg, dest_table=dst_t,
            pairs=pairs, sqlserver_ddls=["BIGINT", "NVARCHAR(60)"],
            replace_destination=replace)
    if (src_kind, dst_kind) == ("sqlserver", "postgresql"):
        from services.copy_sqlserver_pg import copy_sqlserver_to_postgres

        return copy_sqlserver_to_postgres(
            source_cfg=src_cfg, source_table=src_t, dest_cfg=dst_cfg, dest_schema="public",
            dest_table=dst_t, pairs=pairs, pg_ddls=["INTEGER", "VARCHAR(60)"],
            replace_destination=replace)
    if (src_kind, dst_kind) == ("oracle", "postgresql"):
        from services.copy_oracle_pg import copy_oracle_to_postgres

        return copy_oracle_to_postgres(
            source_cfg=src_cfg, source_table=src_t, dest_cfg=dst_cfg, dest_schema="public",
            dest_table=dst_t, pairs=pairs, pg_ddls=["BIGINT", "VARCHAR(60)"],
            replace_destination=replace)
    raise AssertionError((src_kind, dst_kind))


_RESUME = [("postgresql", "oracle"), ("postgresql", "sqlserver"), ("oracle", "sqlserver"),
           ("sqlserver", "postgresql"), ("oracle", "postgresql")]


@pytest.mark.parametrize(("src_kind", "dst_kind"), _RESUME,
                         ids=[f"{s}-{d}" for s, d in _RESUME])
def test_resume_of_a_partly_done_load_still_skips_complete_ranges(
    monkeypatch, src_kind, dst_kind
):
    """A crashed 4-range load: the finished ranges are skipped, the rest load."""
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = _names(src_kind, dst_kind, token)
    rows = [(i, f"row {i}") for i in range(1, 41)]
    try:
        _create_source(src_kind, src_t, rows)
        first = _resume_copy(src_kind, dst_kind, src_t, dst_t, replace=True)
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4, parts
        assert _dest_rows(dst_kind, dst_t) == rows
        # The attempt finished ranges 1-2 and crashed: ranges 3-4 are empty.
        cut = parts[2]["lo"]
        _delete_dest_keys_from(dst_kind, dst_t, int(cut))
        again = _resume_copy(src_kind, dst_kind, src_t, dst_t, replace=False)
        assert _dest_rows(dst_kind, dst_t) == rows
        snap = again.source_snapshot
        assert int(snap.get("partitions_skipped") or 0) == 2, snap
    finally:
        _drop(src_kind, src_t)
        _drop(dst_kind, dst_t)


def test_key_digest_is_engine_neutral_and_count_blind():
    from services.copy_range_proof import _digest

    assert _digest(iter([1, 2, 3])) == _digest(iter(["3", Decimal("2"), 1.0]))
    assert _digest(iter([1, 4, 6, 9])) != _digest(iter([1, 3, 7, 9]))  # same count/min/max/sum
    assert _digest(iter([1, 2])) != _digest(iter([101, 102]))
    assert _digest(iter(["007"])) != _digest(iter([7]))
