"""Datawrap's internal NULL marker (``__DF_SQL_NULL__``) never lands as destination data.

Live reproduction (real CDC apply, MySQL binlog → PostgreSQL): a table with no
primary key whose declared key ``sku`` holds a NULL. The snapshot held the row
out (a NULL key cannot be upserted), the drift repair then re-read the source
as wire cells, encoded them a second time, and re-applied the row: PostgreSQL
stored ``sku = '__DF_SQL_NULL__'`` and the run counted the row as rejected
twice. Now:

* the repair's source rows are marked wire cells, so a NULL stays NULL (and the
  held-out row is not re-applied and counted again);
* a writer refuses to bind a raw marker as text (fail-closed backstop);
* Gate-8 fails a destination that holds the marker text the source does not.
"""

from __future__ import annotations

import json
import uuid

import pytest

from services.null_marker_guard import NullMarkerLeak, refuse_unbound_marker
from services.value_serializer import (
    ESCAPED_WIRE_PREFIX,
    SQL_NULL_SENTINEL,
    WireCell,
    WireLiteral,
    cell_to_string,
)
from tests.c3_live_helpers import mysql_sql, pg_endpoint, pg_sql


# --------------------------------------------------------------------------
# Unit: the source fix and the writer backstop
# --------------------------------------------------------------------------


def test_value_scan_rows_are_marked_wire_cells():
    from services.cdc_value_digest import _as_dicts

    (row,) = _as_dicts(["sku", "qty"], [[SQL_NULL_SENTINEL, "9"]])
    assert isinstance(row["sku"], WireCell)
    # Encoding a marked cell again is a no-op: the NULL stays the NULL marker.
    assert cell_to_string(row["sku"], preserve_sql_null=True) == SQL_NULL_SENTINEL


@pytest.mark.parametrize(
    "cell", [SQL_NULL_SENTINEL, "__DF_MISSING__", ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL]
)
def test_a_writer_refuses_to_bind_a_raw_marker(cell):
    with pytest.raises(NullMarkerLeak, match="internal NULL marker"):
        refuse_unbound_marker([("a", 1), (cell, 2)], ["sku", "qty"], table="t")


def test_a_decoded_user_literal_and_a_null_still_bind():
    refuse_unbound_marker([(WireLiteral(SQL_NULL_SENTINEL), None, "x", 7)])


def test_postgres_values_insert_refuses_a_raw_marker():
    from connectors.postgresql_writer import _execute_values_insert

    class _Cur:
        executed = False

        def executemany(self, *a, **k):
            self.executed = True

    cur = _Cur()
    with pytest.raises(NullMarkerLeak):
        _execute_values_insert(cur, "INSERT INTO t (a) VALUES (%s)", [(SQL_NULL_SENTINEL,)])
    assert not cur.executed


# --------------------------------------------------------------------------
# Live: the CDC apply (MySQL binlog → PostgreSQL)
# --------------------------------------------------------------------------


def _live() -> bool:
    try:
        import pymysqlreplication  # noqa: F401

        from tests.helpers.live_env import mysql_up, pg_up

        return mysql_up() and pg_up()
    except Exception:  # noqa: BLE001 — no live engines: the live leg skips
        return False


@pytest.fixture()
def stores(tmp_path, monkeypatch):
    from services import audit_log, quarantine_dlq

    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(quarantine_dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(quarantine_dlq, "_dlq_coll", lambda: None)
    return tmp_path


@pytest.fixture()
def unkeyed():
    if not _live():
        pytest.skip("live MySQL (ROW binlog) and PostgreSQL not reachable")
    table = f"mk_{uuid.uuid4().hex[:8]}"
    mysql_sql(f"CREATE TABLE {table} (sku VARCHAR(20) NULL, qty INT, note VARCHAR(10) NULL)")
    mysql_sql(f"INSERT INTO {table} VALUES ('a', 1, NULL), (NULL, 9, 'x')")
    try:
        yield table
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS {table}")
        pg_sql(f'DROP TABLE IF EXISTS public."{table}"')
        pg_sql(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')


def _cdc(table: str, job_id: str):
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig
    from tests.helpers.live_env import mysql_creds, pg_creds

    def cfg(c):
        return {
            "host": c["host"], "port": int(c["port"]), "database": c["database"],
            "username": c["username"], "password": c["password"],
            "connection_string": "", "ssl": False,
        }

    cols = (("sku", "VARCHAR(20)"), ("qty", "INT"), ("note", "VARCHAR(10)"))
    return run_cdc_database_transfer(
        EndpointConfig(kind="database", format="mysql", **cfg(mysql_creds()), table=table),
        EndpointConfig(kind="database", format="postgresql", **cfg(pg_creds()), schema="public", table=table),
        [{"source": c, "target": c, "source_type": t, "target_type": t} for c, t in cols],
        dict(cols),
        sync_mode="cdc",
        stream_contracts=[{
            "name": table, "selected": True, "sync_mode": "cdc",
            "snapshot_mode": "initial", "primary_key": "sku",
        }],
        job_id=job_id,
        limit=0,
        validation_mode="balanced",
    )


def _marker_cells(table: str) -> int:
    return int(pg_sql(
        f'SELECT count(*) FROM public."{table}" WHERE sku IN (%s, %s) OR note IN (%s, %s)',
        (SQL_NULL_SENTINEL, ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL) * 2, fetch=True,
    )[0][0])


def test_live_cdc_never_writes_the_marker_and_holds_out_a_null_key_once(unkeyed, stores):
    table = unkeyed
    job_id = "mk" + uuid.uuid4().hex[:10]
    _rows, _ddl, summary, _h = _cdc(table, job_id)
    assert _marker_cells(table) == 0
    assert pg_sql(f'SELECT sku, qty, note FROM public."{table}" ORDER BY qty', fetch=True) == [
        ("a", 1, None)
    ]
    assert int(summary.get("rejected_rows") or 0) == 1, summary.get("rejected_details")

    # The stream keeps the same promise for rows that arrive later.
    mysql_sql(f"INSERT INTO {table} VALUES (NULL, 7, NULL), ('b', 2, NULL)")
    _rows, _ddl, later, _h = _cdc(table, job_id)
    assert _marker_cells(table) == 0
    assert pg_sql(f'SELECT sku, qty FROM public."{table}" ORDER BY qty', fetch=True) == [
        ("a", 1), ("b", 2)
    ]
    assert json.dumps(later.get("rejected_details") or [], default=str).count("null/empty") >= 1


# --------------------------------------------------------------------------
# Live: Gate-8 detects marker text in the destination
# --------------------------------------------------------------------------


@pytest.fixture()
def pg_pair():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    tok = uuid.uuid4().hex[:8]
    src, dst = f"mks_{tok}", f"mkd_{tok}"
    try:
        yield src, dst
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{src}"')
        pg_sql(f'DROP TABLE IF EXISTS public."{dst}"')


def _gate(src: str, dst: str) -> dict:
    from services.null_marker_guard import apply_null_marker_gate

    return apply_null_marker_gate(
        {"passed": True, "message": "Checksum matches."},
        endpoint=pg_endpoint(dst),
        source_endpoint=pg_endpoint(src),
        mappings=[{"source": "sku", "target": "sku"}, {"source": "qty", "target": "qty"}],
        dest_summary={"table": dst},
    )


def test_gate8_fails_a_destination_holding_a_marker_the_source_does_not(pg_pair):
    src, dst = pg_pair
    pg_sql(f'CREATE TABLE public."{src}" (sku TEXT, qty INT)')
    pg_sql(f'CREATE TABLE public."{dst}" (sku TEXT, qty INT)')
    pg_sql(f"INSERT INTO public.\"{src}\" VALUES ('a', 1), (NULL, 9)")
    pg_sql(f"INSERT INTO public.\"{dst}\" VALUES ('a', 1), (%s, 9)", (SQL_NULL_SENTINEL,))
    recon = _gate(src, dst)
    assert recon["passed"] is False
    assert "internal NULL marker" in recon["message"], recon["message"]
    assert recon["null_marker_check"]["destination_rows"] == 1
    assert recon["null_marker_check"]["source_rows"] == 0


def test_gate8_judges_a_non_sql_source_on_its_own_records(pg_pair):
    from services.null_marker_guard import apply_null_marker_gate

    _src, dst = pg_pair
    pg_sql(f'CREATE TABLE public."{dst}" (sku TEXT, qty INT)')
    pg_sql(f'INSERT INTO public."{dst}" VALUES (%s, 1)', (SQL_NULL_SENTINEL,))
    kw = {"endpoint": pg_endpoint(dst), "mappings": [{"source": "sku", "target": "sku"}],
          "dest_summary": {"table": dst}}
    literal = [{"sku": ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL, "qty": "1"}]
    null = [{"sku": SQL_NULL_SENTINEL, "qty": "1"}]
    assert apply_null_marker_gate({"passed": True}, records=literal, **kw)["passed"] is True
    assert apply_null_marker_gate({"passed": True}, records=null, **kw)["passed"] is False
    unjudged = apply_null_marker_gate({"passed": True}, **kw)
    assert unjudged["passed"] is True and unjudged["null_marker_check"]["source_rows"] is None


def test_gate8_keeps_a_marker_spelling_the_source_really_stores(pg_pair):
    src, dst = pg_pair
    for table in (src, dst):
        pg_sql(f'CREATE TABLE public."{table}" (sku TEXT, qty INT)')
        pg_sql(f'INSERT INTO public."{table}" VALUES (%s, 1)', (SQL_NULL_SENTINEL,))
    recon = _gate(src, dst)
    assert recon["passed"] is True, recon
    assert recon["null_marker_check"]["leaked"] is False
