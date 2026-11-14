"""C2 — a CDC delete on an unkeyed table no longer stops the stream.

Audit 12 made every log reader refuse a delete whose key it could not resolve
(no silent drop), which left the route stuck for good: the position could not
pass the delete, so every later poll failed on it and no other change was ever
applied. The delete is now parked: its row image goes to the dead-letter queue,
an audit event names it, the run evidence counts it, and the stream continues.
Nothing is lost silently: every source delete is either applied or parked, and
a park that cannot be recorded still stops the position from moving.

The live leg runs the real engine path: MySQL binlog CDC from a table with no
primary key (the declared key ``sku`` is nullable) into PostgreSQL.
"""

from __future__ import annotations

import json
import uuid

import pytest

from services.cdc_multi_table import MultiTableTransactionBuffer
from services.cdc_transaction_buffer import TransactionBuffer


@pytest.fixture()
def stores(tmp_path, monkeypatch):
    from services import audit_log, quarantine_dlq

    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(quarantine_dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(quarantine_dlq, "_dlq_coll", lambda: None)
    return tmp_path


def _jsonl(path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# --------------------------------------------------------------------------
# Unit: the buffers park instead of raising
# --------------------------------------------------------------------------


def test_transaction_buffer_parks_an_unkeyed_delete_with_its_image():
    buf = TransactionBuffer()
    buf.begin("x1")
    buf.insert({"id": 1, "v": "a"}, pk="1")
    buf.delete("", lsn="0/10", row={"sku": None, "qty": 9}, table="orders")
    batch = buf.commit()
    assert [r["id"] for r in batch.inserts] == [1] and batch.deletes == []
    assert len(batch.unkeyed_deletes) == 1
    parked = batch.unkeyed_deletes[0]
    assert parked["table"] == "orders" and parked["lsn"] == "0/10"
    assert parked["row"]["qty"] == "9" and parked["image_complete"] is True
    assert batch.total_changes == 2, "a parked delete is a change, not a heartbeat"


def test_multi_table_buffer_parks_per_table_and_a_parked_only_batch_is_emitted():
    buf = MultiTableTransactionBuffer()
    buf.begin("x1")
    buf.insert("orders", {"id": 1}, pk="1")
    buf.delete("items", "", lsn="0/20", row={"sku": "z"})
    batches = {b.table: b for b in buf.commit(resume_token="tok", table_order=["orders", "items"])}
    assert set(batches) == {"orders", "items"}
    assert batches["items"].unkeyed_deletes[0]["row"] == {"sku": "z"}
    assert batches["items"].total_changes == 1 and batches["items"].ack_barrier is True


def test_sqlserver_classifier_parks_when_the_caller_takes_parked_deletes():
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    parked: list[dict] = []
    inserts, _updates, deletes = classify_mssql_cdc_rows(
        [{"__$operation": 1, "v": "x"}, {"__$operation": 2, "id": 8, "v": "y"}],
        primary_key="id",
        parked=parked,
        table="dbo.t",
    )
    assert deletes == [] and [str(r["id"]) for r in inserts] == ["8"]
    assert parked and parked[0]["table"] == "dbo.t" and parked[0]["row"]["v"] == "x"


def test_park_records_dlq_and_audit_and_refuses_without_a_job(stores):
    from services.cdc_unkeyed_delete import (
        AUDIT_ACTION,
        DLQ_ACTION,
        park_unkeyed_deletes,
        unkeyed_delete_record,
    )

    rec = unkeyed_delete_record("t", {"sku": None, "qty": 3}, "0/1")
    with pytest.raises(RuntimeError, match="job_id is missing"):
        park_unkeyed_deletes([rec], job_id="")
    ev = park_unkeyed_deletes([rec], job_id="j1", pipeline_key="pk", dest_table="d")
    assert ev and ev["count"] == 1
    dlq = _jsonl(stores / "dlq.jsonl")
    assert [e["action"] for e in dlq] == [DLQ_ACTION]
    assert dlq[0]["details"]["deletes"][0]["row"]["qty"] == "3"
    audit = [e for e in _jsonl(stores / "audit.jsonl") if e.get("action") == AUDIT_ACTION]
    assert len(audit) == 1 and audit[0]["details"]["dlq_event_id"] == dlq[0]["id"]


def test_a_park_that_cannot_be_recorded_fails_closed(stores, monkeypatch):
    from services import quarantine_dlq
    from services.cdc_unkeyed_delete import park_unkeyed_deletes, unkeyed_delete_record

    def _down(**_kw):
        raise OSError("dlq volume full")

    monkeypatch.setattr(quarantine_dlq, "append_dlq_event", _down)
    with pytest.raises(OSError):
        park_unkeyed_deletes([unkeyed_delete_record("t", {"a": 1})], job_id="j1")


# --------------------------------------------------------------------------
# Live: MySQL binlog CDC from an unkeyed table into PostgreSQL
# --------------------------------------------------------------------------


def _live_ready() -> bool:
    try:
        import pymysqlreplication  # noqa: F401

        from tests.helpers.live_env import mysql_up, pg_up

        return mysql_up() and pg_up()
    except Exception:  # noqa: BLE001 — no live engines: the live leg skips
        return False


@pytest.fixture()
def unkeyed_route(stores):
    if not _live_ready():
        pytest.skip("live MySQL (ROW binlog) and PostgreSQL not reachable")
    import psycopg2

    from connectors.mysql_conn import get_connection
    from tests.helpers.live_env import mysql_creds, pg_creds

    m, p = mysql_creds(), pg_creds()
    my_cfg = {
        "host": m["host"], "port": int(m["port"]), "database": m["database"],
        "username": m["username"], "password": m["password"],
        "connection_string": "", "ssl": False,
    }
    pg_cfg = {
        "host": p["host"], "port": int(p["port"]), "database": p["database"],
        "username": p["username"], "password": p["password"],
        "connection_string": "", "ssl": False,
    }
    table = f"c2uk_{uuid.uuid4().hex[:8]}"

    def my(sql: str) -> None:
        conn = get_connection(**my_cfg)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        finally:
            conn.close()

    def pg(sql: str, fetch: bool = False):
        conn = psycopg2.connect(
            host=pg_cfg["host"], port=pg_cfg["port"], dbname=pg_cfg["database"],
            user=pg_cfg["username"], password=pg_cfg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall() if fetch else None
        finally:
            conn.close()

    try:
        # No primary key: the operator's declared key `sku` is nullable.
        my(f"CREATE TABLE {table} (sku VARCHAR(20) NULL, qty INT)")
        my(f"INSERT INTO {table} VALUES ('a', 1), ('b', 2), (NULL, 9)")
        yield table, my, pg, my_cfg, pg_cfg
    finally:
        my(f"DROP TABLE IF EXISTS {table}")
        pg(f'DROP TABLE IF EXISTS public."{table}"')
        pg(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')


def _run(table: str, my_cfg: dict, pg_cfg: dict, job_id: str):
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    return run_cdc_database_transfer(
        EndpointConfig(kind="database", format="mysql", **my_cfg, table=table),
        EndpointConfig(kind="database", format="postgresql", **pg_cfg, schema="public", table=table),
        [
            {"source": "sku", "target": "sku", "source_type": "VARCHAR(20)", "target_type": "VARCHAR(20)"},
            {"source": "qty", "target": "qty", "source_type": "INT", "target_type": "INTEGER"},
        ],
        {"sku": "VARCHAR(20)", "qty": "INT"},
        sync_mode="cdc",
        stream_contracts=[{
            "name": table, "selected": True, "sync_mode": "cdc",
            "snapshot_mode": "initial", "primary_key": "sku",
        }],
        job_id=job_id,
        limit=0,
        validation_mode="balanced",
    )


def test_live_unkeyed_delete_is_parked_and_the_stream_keeps_applying(unkeyed_route, stores):
    table, my, pg, my_cfg, pg_cfg = unkeyed_route
    job_id = "c2" + uuid.uuid4().hex[:10]
    _run(table, my_cfg, pg_cfg, job_id)  # snapshot; the NULL-key row is held out

    # Two source deletes (one keyed, one with no key) and an insert after them.
    my(f"DELETE FROM {table} WHERE sku IS NULL")
    my(f"DELETE FROM {table} WHERE sku = 'b'")
    my(f"INSERT INTO {table} VALUES ('d', 4)")

    _rows, _ddl, summary, _h = _run(table, my_cfg, pg_cfg, job_id)
    from services.cdc_unkeyed_delete import AUDIT_ACTION, DLQ_ACTION

    # The stream did not stop: the keyed delete and the later insert landed.
    landed = pg(f'SELECT sku, qty FROM public."{table}" WHERE sku IN (\'a\', \'b\', \'d\') ORDER BY sku', fetch=True)
    assert landed == [("a", 1), ("d", 4)], landed
    # The NULL-key row is held out, never written as the wire sentinel text.
    whole = pg(f'SELECT sku, qty FROM public."{table}" ORDER BY sku NULLS FIRST', fetch=True)
    assert whole == [("a", 1), ("d", 4)], whole
    cdc = summary.get("cdc") or {}
    # No silent loss: every source delete is applied or parked.
    assert cdc.get("delete_events") == 1 and cdc.get("unkeyed_deletes_parked") == 1, cdc
    assert summary.get("cdc_unkeyed_deletes_parked") == 1
    assert "parked in the dead-letter queue" in str(summary.get("cdc_unkeyed_delete_note"))
    evidence = summary.get("cdc_unkeyed_delete_evidence") or []
    assert evidence and evidence[0]["sample"][0]["row"]["qty"] == "9", evidence

    dlq = [e for e in _jsonl(stores / "dlq.jsonl") if e.get("action") == DLQ_ACTION]
    assert len(dlq) == 1 and dlq[0]["job_id"] == job_id and dlq[0]["rows"] == 1
    assert dlq[0]["details"]["deletes"][0]["row"]["qty"] == "9"
    audit = [e for e in _jsonl(stores / "audit.jsonl") if e.get("action") == AUDIT_ACTION]
    assert len(audit) == 1 and audit[0]["details"]["dlq_event_id"] == dlq[0]["id"]

    # The position moved past the parked delete: the next run is clean.
    _rows, _ddl, again, _h = _run(table, my_cfg, pg_cfg, job_id)
    assert (again.get("cdc") or {}).get("unkeyed_deletes_parked") == 0
    assert len([e for e in _jsonl(stores / "dlq.jsonl") if e.get("action") == DLQ_ACTION]) == 1


def test_live_park_that_cannot_be_recorded_keeps_the_position(unkeyed_route, stores, monkeypatch):
    from services import quarantine_dlq
    from services.cdc_unkeyed_delete import DLQ_ACTION

    table, my, pg, my_cfg, pg_cfg = unkeyed_route
    job_id = "c2f" + uuid.uuid4().hex[:10]
    _run(table, my_cfg, pg_cfg, job_id)
    my(f"DELETE FROM {table} WHERE sku IS NULL")

    real = quarantine_dlq.append_dlq_event

    def _down(**kw):
        if kw.get("action") == DLQ_ACTION:
            raise OSError("dlq volume full")
        return real(**kw)

    monkeypatch.setattr(quarantine_dlq, "append_dlq_event", _down)
    with pytest.raises(Exception, match="dlq volume full"):
        _run(table, my_cfg, pg_cfg, job_id)

    # Recording works again: the same delete is redelivered and parked once.
    monkeypatch.setattr(quarantine_dlq, "append_dlq_event", real)
    _rows, _ddl, summary, _h = _run(table, my_cfg, pg_cfg, job_id)
    assert (summary.get("cdc") or {}).get("unkeyed_deletes_parked") == 1, summary.get("cdc")
    assert len([e for e in _jsonl(stores / "dlq.jsonl") if e.get("action") == DLQ_ACTION]) == 1
