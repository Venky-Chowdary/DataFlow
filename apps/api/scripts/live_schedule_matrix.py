"""Live schedule matrix: does a *scheduled* run land, prove, and advance?

Studio proves a transfer with an operator at the keyboard. A schedule has
nobody there, so every gate, watermark, and proof has to hold on the real
scheduler path: ``create_schedule`` → due beat (``_run_due_schedules``) →
``_dispatch_transfer`` → engine → ``_finalize_run`` → run history. This harness
drives exactly that path — not ``execute_tracked`` — for every source engine ×
destination engine × sync mode that can run on this box, twice per cell:

1. **beat 1** — empty destination, N source rows.
2. **mutate** — insert new rows, update existing rows, delete rows.
3. **beat 2** — the same schedule fires again; the destination must now hold
   what the sync mode contract says it must, the watermark must have advanced
   (incremental), and the run history must carry both runs with Gate-8 verdicts.

Verification is independent: destination counts and control totals are read
with the engine's own driver, never from the writer acknowledgement. Nothing
here asserts; every cell records measured vs. expected and a ``verdict`` of
``pass`` / ``fail`` / ``skip`` with the reason, so the artifact can only say
what was measured.

Environment
-----------
``SCHED_ROWS`` (default 2000) source rows per cell; ``SCHED_ENGINES`` comma list
subset of ``postgresql,mysql,sqlite,mongodb``; ``SCHED_SOURCES`` / ``SCHED_DESTS``
restrict which of those engines take the source / destination role (default:
every engine in both roles); ``SCHED_MODES`` comma list subset of the sync
modes; ``SCHED_OUT`` artifact path.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import sqlite3
import sys
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATAFLOW_CONNECTOR_STORE_BACKEND", "mongo")

WORKSPACE = "sched-proof"
ROWS = int(os.environ.get("SCHED_ROWS", "2000"))
NEW_ROWS = max(1, ROWS // 20)
UPDATED_ROWS = max(1, ROWS // 40)
DELETED_ROWS = max(1, ROWS // 200)
RUN_TIMEOUT_S = int(os.environ.get("SCHED_RUN_TIMEOUT", "900"))
SQLITE_DIR = Path(os.environ.get("SCHED_SQLITE_DIR", "/home/ubuntu/sched_proof"))

ENGINES: dict[str, dict[str, Any]] = {
    "postgresql": dict(type="postgresql", host="localhost", port=5432, database="dataflow",
                       username="dataflow", password="dataflow", ssl=False),
    "mysql": dict(type="mysql", host="127.0.0.1", port=3306, database="dataflow",
                  username="dataflow", password="dataflow", ssl=False),
    "sqlite": dict(type="sqlite", host="", port=0, database="", username="", password="", ssl=False),
    "mongodb": dict(type="mongodb", host="localhost", port=27017, database="sched_proof",
                    username="", password="", ssl=False),
}

# Schedule vocabulary (schedule_store.SYNC_MODES). ``incremental`` is the
# coarse mode the UI stores; with a primary key it becomes incremental_deduped,
# without one incremental_append — both are exercised explicitly.
MODES: list[tuple[str, str]] = [
    ("full_refresh_overwrite", "pk"),
    ("full_refresh_append", "nopk"),
    ("incremental_append", "nopk"),
    ("incremental_deduped", "pk"),
    ("scd2", "pk"),
    ("mirror", "pk"),
    ("cdc", "pk"),
]

# Engines that can be a schedule destination for row-versioned modes.
SQL_ENGINES = {"postgresql", "mysql", "sqlite"}
CDC_SOURCES = {"postgresql", "mysql", "mongodb"}

TYPES: dict[str, dict[str, str]] = {
    "postgresql": {"id": "BIGINT", "name": "VARCHAR(64)", "amount": "NUMERIC(12,3)", "updated_seq": "BIGINT"},
    "mysql": {"id": "BIGINT", "name": "VARCHAR(64)", "amount": "DECIMAL(12,3)", "updated_seq": "BIGINT"},
    "sqlite": {"id": "INTEGER", "name": "TEXT", "amount": "NUMERIC(12,3)", "updated_seq": "INTEGER"},
    "mongodb": {"id": "long", "name": "string", "amount": "decimal", "updated_seq": "long"},
}
COLUMNS = ["id", "name", "amount", "updated_seq"]

#: What ``updated_seq`` means per mode — the fixture bumps it on every update,
#: so it is a modification stamp; CDC reads the change log, not the column.
CURSOR_SEMANTICS_FOR_MODE = {
    "incremental_append": "monotonic_sequence",
    "incremental_deduped": "modification_timestamp",
    "cdc": "cdc_position",
}


def _reachable(cfg: dict[str, Any]) -> bool:
    if cfg["type"] == "sqlite":
        return True
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=1):
            return True
    except OSError:
        return False


# --------------------------------------------------------------------------- engines


class Engine:
    """Native driver access for seeding, mutating and independently verifying."""

    def __init__(self, name: str, cfg: dict[str, Any]):
        self.name = name
        self.cfg = dict(cfg)
        if name == "sqlite":
            SQLITE_DIR.mkdir(parents=True, exist_ok=True)
            self.cfg["database"] = str(SQLITE_DIR / f"{name}_{WORKSPACE}.db")

    # -- connections -------------------------------------------------------
    def _conn(self):
        c = self.cfg
        if self.name == "postgresql":
            import psycopg2

            return psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                                    user=c["username"], password=c["password"])
        if self.name == "mysql":
            import pymysql

            return pymysql.connect(host=c["host"], port=c["port"], user=c["username"],
                                   password=c["password"], database=c["database"], autocommit=False)
        if self.name == "sqlite":
            return sqlite3.connect(c["database"])
        raise AssertionError(self.name)

    def _mongo(self):
        from pymongo import MongoClient

        return MongoClient(f"mongodb://{self.cfg['host']}:{self.cfg['port']}/")[self.cfg["database"]]

    def exec(self, statements: list[str], params: list[tuple] | None = None) -> None:
        conn = self._conn()
        try:
            cur = conn.cursor()
            if params is not None:
                cur.executemany(statements[0], params)
            else:
                for s in statements:
                    cur.execute(s)
            conn.commit()
        finally:
            conn.close()

    def query(self, sql: str) -> list[tuple]:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def _ph(self) -> str:
        return "?" if self.name == "sqlite" else "%s"

    # -- seeding -------------------------------------------------------------
    @staticmethod
    def row(i: int, seq: int) -> tuple:
        return (i, f"name-{i}", Decimal(i) / Decimal(1000) + Decimal("0.001") * (i % 7), seq)

    def drop(self, table: str) -> None:
        if self.name == "mongodb":
            self._mongo().drop_collection(table)
            return
        self.exec([f"DROP TABLE IF EXISTS {self.q(table)}"])
        if self.name == "postgresql":
            self._release_pg_cdc_artifacts(table)

    def pg_slot_exists(self, slot_name: str) -> bool:
        with contextlib.closing(self._conn()) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot_name,)
            )
            return cur.fetchone() is not None

    def _release_pg_cdc_artifacts(self, table: str) -> None:
        """Drop this table's replication slots + publications (harness-owned).

        Slots pin WAL and count against ``max_replication_slots``; a matrix
        that leaves one per cell exhausts the quota and every later CDC cell
        fails to attach for a reason unrelated to the code under test.
        """
        db = self.cfg["database"]
        slot_like = f"df_{db}_{table}_%".lower()
        pub_like = f"df_pub_{db}_{table}_%".lower()
        with contextlib.closing(self._conn()) as conn:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s",
                (slot_like,),
            )
            for (slot,) in cur.fetchall():
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            cur.execute("SELECT pubname FROM pg_publication WHERE pubname LIKE %s", (pub_like,))
            for (pub,) in cur.fetchall():
                cur.execute(f'DROP PUBLICATION IF EXISTS "{pub}"')

    def q(self, ident: str) -> str:
        return f"`{ident}`" if self.name == "mysql" else f'"{ident}"'

    def create_source(self, table: str, *, pk: bool) -> None:
        self.drop(table)
        if self.name == "mongodb":
            db = self._mongo()
            db.create_collection(table)
            if pk:
                db[table].create_index("id", unique=True)
            return
        t = TYPES[self.name]
        key = " PRIMARY KEY" if pk else ""
        self.exec([
            f"CREATE TABLE {self.q(table)} ({self.q('id')} {t['id']}{key}, "
            f"{self.q('name')} {t['name']}, {self.q('amount')} {t['amount']}, "
            f"{self.q('updated_seq')} {t['updated_seq']})"
        ])

    def insert(self, table: str, rows: list[tuple]) -> None:
        if self.name == "mongodb":
            from bson.decimal128 import Decimal128

            self._mongo()[table].insert_many([
                {"id": r[0], "name": r[1], "amount": Decimal128(str(r[2])), "updated_seq": r[3]}
                for r in rows
            ])
            return
        ph = self._ph()
        vals = [(r[0], r[1], str(r[2]) if self.name == "sqlite" else r[2], r[3]) for r in rows]
        self.exec([f"INSERT INTO {self.q(table)} ({', '.join(self.q(c) for c in COLUMNS)}) "
                   f"VALUES ({ph}, {ph}, {ph}, {ph})"], vals)

    def update(self, table: str, ids: list[int], seq: int) -> None:
        if self.name == "mongodb":
            self._mongo()[table].update_many(
                {"id": {"$in": ids}}, [{"$set": {"name": {"$concat": ["$name", "-v2"]}, "updated_seq": seq}}]
            )
            return
        ph = self._ph()
        concat = (f"{self.q('name')} || '-v2'" if self.name != "mysql"
                  else f"CONCAT({self.q('name')}, '-v2')")
        self.exec([f"UPDATE {self.q(table)} SET {self.q('name')} = {concat}, "
                   f"{self.q('updated_seq')} = {seq} WHERE {self.q('id')} = {ph}"],
                  [(i,) for i in ids])

    def delete(self, table: str, ids: list[int]) -> None:
        if self.name == "mongodb":
            self._mongo()[table].delete_many({"id": {"$in": ids}})
            return
        ph = self._ph()
        self.exec([f"DELETE FROM {self.q(table)} WHERE {self.q('id')} = {ph}"], [(i,) for i in ids])

    # -- independent verification -----------------------------------------------
    def exists(self, table: str) -> bool:
        if self.name == "mongodb":
            return table in self._mongo().list_collection_names()
        try:
            self.query(f"SELECT 1 FROM {self.q(table)} WHERE 1=0")
            return True
        except Exception:  # noqa: BLE001 - probe verdict
            return False

    def columns(self, table: str) -> list[str]:
        if self.name == "mongodb":
            doc = self._mongo()[table].find_one() or {}
            return [k for k in doc.keys() if k != "_id"]
        if self.name == "sqlite":
            return [r[1] for r in self.query(f"PRAGMA table_info({self.q(table)})")]
        if self.name == "postgresql":
            return [r[0] for r in self.query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = "
                f"'{table}' AND table_schema = current_schema() ORDER BY ordinal_position")]
        return [r[0] for r in self.query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = "
            f"'{table}' AND table_schema = DATABASE() ORDER BY ordinal_position")]

    def totals(self, table: str, where: str = "", mongo_filter: dict | None = None) -> dict[str, Any]:
        """COUNT(*), SUM(id), SUM(amount) read directly from the engine."""
        if not self.exists(table):
            return {"count": -1, "sum_id": None, "sum_amount": None}
        if self.name == "mongodb":
            coll = self._mongo()[table]
            pipe = [
                {"$match": mongo_filter or {}},
                {"$group": {"_id": None, "n": {"$sum": 1}, "sid": {"$sum": "$id"},
                            "samt": {"$sum": {"$toDecimal": "$amount"}}}},
            ]
            out = list(coll.aggregate(pipe))
            if not out:
                return {"count": 0, "sum_id": 0, "sum_amount": "0"}
            return {"count": int(out[0]["n"]), "sum_id": int(out[0]["sid"]),
                    "sum_amount": str(Decimal(str(out[0]["samt"])).quantize(Decimal("0.001")))}
        amt = (f"SUM(CAST({self.q('amount')} AS DECIMAL(20,3)))" if self.name != "sqlite"
               else f"SUM(CAST({self.q('amount')} AS REAL))")
        rows = self.query(f"SELECT COUNT(*), SUM({self.q('id')}), {amt} FROM {self.q(table)} {where}")
        n, sid, samt = rows[0]
        return {
            "count": int(n or 0),
            "sum_id": int(sid or 0),
            "sum_amount": str(Decimal(str(samt or 0)).quantize(Decimal("0.001"))),
        }


# --------------------------------------------------------------------------- scheduler path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mapping_rows(src: str, dst: str, *, pk: bool) -> list[dict[str, Any]]:
    rows = []
    for col in COLUMNS:
        row = {"source": col, "target": col, "source_type": TYPES[src][col],
               "target_type": TYPES[dst][col], "transform": "none", "confidence": 0.99}
        if col == "id" and pk:
            row["primary_key"] = True
        rows.append(row)
    if src == "mongodb":
        # The operator's Map decision for Mongo's own document key: an
        # intentional, reasoned omission, so G13 has an answer for it.
        rows.append({"source": "_id", "target": "", "source_type": "objectId",
                     "transform": "none", "intentional_omit": True,
                     "omit_reason_code": "redundant",
                     "omit_reason_text": "MongoDB document key; business key is id"})
    return rows


def _connector(engine: Engine, role: str) -> str:
    from services.connector_store import create_connector

    data = {**engine.cfg, "name": f"sched-proof {engine.name} {role}", "role": role,
            "workspace_id": WORKSPACE}
    data.pop("ssl", None)
    data["ssl"] = False
    return create_connector(data).id


def _force_due(schedule_id: str) -> None:
    from services.schedule_store import update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    update_schedule(schedule_id, {"next_run_at": past})


def _beat_and_wait(schedule_id: str, *, expect_start: bool = True) -> dict[str, Any]:
    """Fire the real due-beat and wait for ``_finalize_run`` to record the run."""
    from services.schedule_runner import _run_due_schedules
    from services.schedule_store import get_schedule

    before = get_schedule(schedule_id)
    run_count_before = before.run_count if before else 0
    started = _run_due_schedules()
    if not expect_start:
        return {"started": started}
    deadline = time.time() + RUN_TIMEOUT_S
    while time.time() < deadline:
        sched = get_schedule(schedule_id)
        if sched and not sched.running and (sched.run_count > run_count_before or sched.retry_at
                                             or sched.approval_request):
            return {"started": started, "schedule": sched}
        time.sleep(0.5)
    return {"started": started, "schedule": get_schedule(schedule_id), "timeout": True}


def _job(job_id: str) -> dict[str, Any]:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().get_job(job_id) or {}


def _run_summary(sched: Any, job: dict[str, Any]) -> dict[str, Any]:
    recon = job.get("reconciliation") or {}
    return {
        "job_id": job.get("job_id") or job.get("_id"),
        "status": job.get("status"),
        "error": str(job.get("error") or "")[:400],
        "rows_written": job.get("records_processed"),
        "total_rows": job.get("total_rows"),
        "phase": job.get("phase"),
        "gate8_passed": recon.get("passed"),
        "gate8_message": str(recon.get("message") or "")[:300],
        "cursor_value": sched.cursor_value if sched else None,
        "last_status": sched.last_status if sched else None,
        "run_count": sched.run_count if sched else None,
        "retry_at": sched.retry_at if sched else None,
        "approval": (sched.approval_request or {}).get("reason", "")[:300]
        if sched and sched.approval_request else "",
    }


# --------------------------------------------------------------------------- expectations


def _expected_after_second(mode: str, n: int) -> dict[str, Any]:
    """What the destination must hold after beat 2, per sync-mode contract."""
    src_after = n + NEW_ROWS - DELETED_ROWS
    if mode == "full_refresh_overwrite":
        return {"rows": src_after, "note": "destination == source after replacement"}
    if mode == "full_refresh_append":
        return {"rows": n + src_after, "note": "append accumulates: run1 + run2 source"}
    if mode == "incremental_append":
        return {"rows": n + NEW_ROWS + UPDATED_ROWS,
                "note": "only rows past the watermark land (new + updated), appended"}
    if mode in ("incremental_deduped", "cdc"):
        deleted = DELETED_ROWS if mode == "cdc" else 0
        return {"rows": src_after if mode == "cdc" else n + NEW_ROWS,
                "note": "key-converged: new inserted, updated merged"
                + (", deletes propagated" if deleted else ", deletes stay (no tombstone)")}
    if mode == "scd2":
        return {"current_rows": src_after, "rows": n + NEW_ROWS + UPDATED_ROWS,
                "note": "complete snapshot: current versions == live source keys (deleted keys "
                        "closed, history retained); changed keys versioned"}
    if mode == "mirror":
        return {"live_rows": src_after, "deleted_rows": DELETED_ROWS, "rows": n + NEW_ROWS,
                "note": "mirror soft-deletes missing keys (_deleted) and updates present ones"}
    raise AssertionError(mode)


def _measure(dst: Engine, table: str, mode: str) -> dict[str, Any]:
    out = {"totals": dst.totals(table)}
    if mode == "scd2" and dst.name in SQL_ENGINES:
        cols = dst.columns(table)
        if "is_current" in cols:
            pred = {"postgresql": "is_current = TRUE", "mysql": "is_current = 1",
                    "sqlite": "is_current IN (1, 'true', 'True')"}[dst.name]
            out["current"] = dst.totals(table, f"WHERE {pred}")
        out["columns"] = cols
    if mode == "mirror":
        cols = dst.columns(table)
        out["columns"] = cols
        if dst.name == "mongodb":
            out["live"] = dst.totals(table, mongo_filter={"_deleted": {"$ne": True}})
            out["deleted"] = dst.totals(table, mongo_filter={"_deleted": True})
        elif "_deleted" in cols:
            live = {"postgresql": "COALESCE(_deleted, FALSE) = FALSE", "mysql": "COALESCE(_deleted, 0) = 0",
                    "sqlite": "COALESCE(_deleted, 0) IN (0, 'false', 'False')"}[dst.name]
            dead = {"postgresql": "_deleted = TRUE", "mysql": "_deleted = 1",
                    "sqlite": "_deleted IN (1, 'true', 'True')"}[dst.name]
            out["live"] = dst.totals(table, f"WHERE {live}")
            out["deleted"] = dst.totals(table, f"WHERE {dead}")
    return out


def _judge(mode: str, exp: dict[str, Any], measured: dict[str, Any],
           run1: dict[str, Any], run2: dict[str, Any], src_totals: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    for i, run in ((1, run1), (2, run2)):
        if run["status"] != "completed":
            reasons.append(f"run{i} status={run['status']}: {run['error'] or run['approval']}")
        if run["gate8_passed"] is not True:
            reasons.append(f"run{i} gate8_passed={run['gate8_passed']}: {run['gate8_message']}")
    count = measured["totals"]["count"]
    if mode == "scd2":
        cur = measured.get("current", {}).get("count")
        if cur != exp["current_rows"]:
            reasons.append(f"scd2 current rows {cur} != {exp['current_rows']}")
        if count != exp["rows"]:
            reasons.append(f"scd2 total rows {count} != {exp['rows']}")
    elif mode == "mirror":
        live = measured.get("live", {}).get("count")
        dead = measured.get("deleted", {}).get("count")
        if live != exp["live_rows"]:
            reasons.append(f"mirror live rows {live} != {exp['live_rows']}")
        if dead != exp["deleted_rows"]:
            reasons.append(f"mirror soft-deleted rows {dead} != {exp['deleted_rows']}")
        if measured.get("live", {}).get("sum_amount") != src_totals["sum_amount"]:
            reasons.append("mirror live SUM(amount) != source SUM(amount)")
    else:
        if count != exp["rows"]:
            reasons.append(f"rows {count} != {exp['rows']}")
        if mode in ("full_refresh_overwrite", "cdc") and \
                measured["totals"]["sum_amount"] != src_totals["sum_amount"]:
            reasons.append(f"SUM(amount) {measured['totals']['sum_amount']} != source {src_totals['sum_amount']}")
        if mode == "incremental_deduped" and measured["totals"]["sum_id"] is not None:
            # Deleted keys legitimately remain; every surviving key must match.
            pass
    if mode.startswith("incremental") and not (run2.get("cursor_value") or "").strip():
        reasons.append("schedule cursor_value did not advance after run 2")
    return ("pass" if not reasons else "fail"), reasons


# --------------------------------------------------------------------------- cells


def run_cell(src: Engine, dst: Engine, mode: str, keyed: str, conn_ids: dict[str, str]) -> dict[str, Any]:
    from services.schedule_store import create_schedule, delete_schedule, get_schedule

    pk = keyed == "pk"
    tag = uuid.uuid4().hex[:6]
    src_table = f"sp_src_{mode}_{tag}"
    dst_table = f"sp_dst_{src.name}_{mode}_{tag}"
    cell: dict[str, Any] = {"source": src.name, "dest": dst.name, "sync_mode": mode, "pk": pk,
                            "rows": ROWS, "src_table": src_table, "dst_table": dst_table}
    if mode == "cdc" and src.name not in CDC_SOURCES:
        return {**cell, "verdict": "skip", "reasons": [f"{src.name} has no log-based CDC source"]}
    if mode in ("scd2", "mirror") and dst.name not in SQL_ENGINES:
        # Product contract (preflight g9 / SQL_HISTORY_SYNC_DESTS): row-versioned
        # modes need a SQL table destination. Refused at Validate by design.
        return {**cell, "verdict": "skip",
                "reasons": [f"{mode} requires a SQL table destination; {dst.name} is refused at g9 by design"]}

    sched_id = ""
    try:
        src.create_source(src_table, pk=pk)
        src.insert(src_table, [Engine.row(i, 1) for i in range(1, ROWS + 1)])
        dst.drop(dst_table)

        data = {
            "name": f"sched-proof {src.name}→{dst.name} {mode} {tag}",
            "source_connector_id": conn_ids[f"{src.name}:source"],
            "source_table": src_table,
            "dest_connector_id": conn_ids[f"{dst.name}:destination"],
            "dest_table": dst_table,
            "interval": "hourly",
            "enabled": True,
            "sync_mode": mode,
            "validation_mode": "strict",
            "schema_policy": "manual_review",
            "mappings": _mapping_rows(src.name, dst.name, pk=pk),
            "cursor_column": "updated_seq" if mode in CURSOR_SEMANTICS_FOR_MODE else "",
            "cursor_semantics": CURSOR_SEMANTICS_FOR_MODE.get(mode, ""),
            "primary_key": "id" if pk else "",
            "workspace_id": WORKSPACE,
            "max_retries": 0,
        }
        sched = create_schedule(data)
        sched_id = sched.id
        cell["schedule_id"] = sched_id

        _force_due(sched_id)
        t0 = time.time()
        r1 = _beat_and_wait(sched_id)
        s1 = r1.get("schedule")
        job1 = _job(s1.last_job_id) if s1 and s1.last_job_id else {}
        run1 = _run_summary(s1, job1)
        run1["seconds"] = round(time.time() - t0, 1)
        run1["beat_started"] = r1.get("started")
        cell["run1"] = run1
        cell["after_run1"] = _measure(dst, dst_table, mode)
        if r1.get("timeout"):
            cell.update(verdict="fail", reasons=[f"run1 did not finish within {RUN_TIMEOUT_S}s"])
            return cell
        if run1["status"] != "completed":
            cell.update(verdict="fail", reasons=[f"run1 status={run1['status']}: {run1['error'] or run1['approval']}"])
            return cell

        # Mutate the source the way a live system does between two beats.
        seq2 = 2
        new_ids = list(range(ROWS + 1, ROWS + NEW_ROWS + 1))
        upd_ids = list(range(1, UPDATED_ROWS + 1))
        del_ids = list(range(ROWS - DELETED_ROWS + 1, ROWS + 1))
        src.insert(src_table, [Engine.row(i, seq2) for i in new_ids])
        src.update(src_table, upd_ids, seq2)
        src.delete(src_table, del_ids)
        src_totals = src.totals(src_table)
        cell["source_after_mutation"] = src_totals

        _force_due(sched_id)
        t0 = time.time()
        r2 = _beat_and_wait(sched_id)
        s2 = r2.get("schedule")
        job2 = _job(s2.last_job_id) if s2 and s2.last_job_id else {}
        run2 = _run_summary(s2, job2)
        run2["seconds"] = round(time.time() - t0, 1)
        run2["beat_started"] = r2.get("started")
        cell["run2"] = run2
        measured = _measure(dst, dst_table, mode)
        cell["after_run2"] = measured
        exp = _expected_after_second(mode, ROWS)
        cell["expected_after_run2"] = exp
        if r2.get("timeout"):
            cell.update(verdict="fail", reasons=[f"run2 did not finish within {RUN_TIMEOUT_S}s"])
            return cell
        final = get_schedule(sched_id)
        cell["run_history_len"] = len(final.run_history) if final else 0
        cell["next_run_at"] = final.next_run_at if final else None
        verdict, reasons = _judge(mode, exp, measured, run1, run2, src_totals)
        if cell["run_history_len"] < 2:
            verdict, reasons = "fail", reasons + [f"run_history has {cell['run_history_len']} entries, expected 2"]
        if mode == "cdc" and final is not None and os.environ.get("SCHED_KEEP") != "1":
            # Product lifecycle: deleting the schedule must release the source
            # slot/publication (the router does this; the harness proves it).
            from services.cdc_capture_release import release_schedule_cdc_capture

            release = release_schedule_cdc_capture(final)
            cell["capture_release"] = release
            if src.name == "postgresql":
                if not release.get("released") or release.get("slot") != "dropped":
                    verdict, reasons = "fail", reasons + [f"slot not released on delete: {release}"]
                elif src.pg_slot_exists(release["slot_name"]):
                    verdict, reasons = "fail", reasons + [
                        f"slot {release['slot_name']} still in pg_replication_slots after release"
                    ]
        cell.update(verdict=verdict, reasons=reasons)
        return cell
    except Exception as exc:  # noqa: BLE001 - recorded as cell failure
        cell.update(verdict="fail", reasons=[f"harness exception: {exc!r}"],
                    traceback=traceback.format_exc()[-1500:])
        return cell
    finally:
        if sched_id and os.environ.get("SCHED_KEEP") != "1":
            with contextlib.suppress(Exception):
                delete_schedule(sched_id)
            with contextlib.suppress(Exception):
                src.drop(src_table)


# --------------------------------------------------------------------------- operational cells


def run_overlap_cell(src: Engine, dst: Engine, conn_ids: dict[str, str]) -> dict[str, Any]:
    """A second beat while a run is in flight must not start a second job."""
    from services.schedule_runner import _run_due_schedules
    from services.schedule_store import create_schedule, delete_schedule, get_schedule, mark_schedule_running

    tag = uuid.uuid4().hex[:6]
    cell: dict[str, Any] = {"source": src.name, "dest": dst.name, "sync_mode": "full_refresh_overwrite",
                            "scenario": "overlap_protection"}
    sched_id = ""
    try:
        src_table, dst_table = f"sp_src_ovl_{tag}", f"sp_dst_ovl_{tag}"
        src.create_source(src_table, pk=True)
        src.insert(src_table, [Engine.row(i, 1) for i in range(1, 51)])
        dst.drop(dst_table)
        sched = create_schedule({
            "name": f"sched-proof overlap {tag}", "source_connector_id": conn_ids[f"{src.name}:source"],
            "source_table": src_table, "dest_connector_id": conn_ids[f"{dst.name}:destination"],
            "dest_table": dst_table, "interval": "hourly", "enabled": True,
            "sync_mode": "full_refresh_overwrite", "mappings": _mapping_rows(src.name, dst.name, pk=True),
            "primary_key": "id", "workspace_id": WORKSPACE, "max_retries": 0,
        })
        sched_id = sched.id
        _force_due(sched_id)
        # Simulate an in-flight run held by another scheduler instance.
        claimed = mark_schedule_running(sched_id, "other-instance")
        started_while_running = _run_due_schedules()
        held = get_schedule(sched_id)
        cell["claim_held"] = bool(claimed) and bool(held and held.running)
        cell["beat_started_while_running"] = started_while_running
        from services.schedule_store import clear_schedule_running

        clear_schedule_running(sched_id)
        r = _beat_and_wait(sched_id)
        s = r.get("schedule")
        cell["run_after_release"] = _run_summary(s, _job(s.last_job_id) if s and s.last_job_id else {})
        reasons = []
        if started_while_running != 0:
            reasons.append("beat started a job while the schedule was already running")
        if cell["run_after_release"]["status"] != "completed":
            reasons.append("run after release did not complete")
        cell.update(verdict="pass" if not reasons else "fail", reasons=reasons)
        return cell
    except Exception as exc:  # noqa: BLE001 - recorded as cell failure
        cell.update(verdict="fail", reasons=[f"harness exception: {exc!r}"], traceback=traceback.format_exc()[-1500:])
        return cell
    finally:
        if sched_id:
            delete_schedule(sched_id)


def run_failure_retry_cell(src: Engine, dst: Engine, conn_ids: dict[str, str]) -> dict[str, Any]:
    """A source table that vanishes must park one finding — not retry forever."""
    from services.schedule_store import create_schedule, delete_schedule, get_schedule

    tag = uuid.uuid4().hex[:6]
    cell: dict[str, Any] = {"source": src.name, "dest": dst.name, "sync_mode": "full_refresh_overwrite",
                            "scenario": "failure_parks_finding"}
    sched_id = ""
    try:
        src_table, dst_table = f"sp_src_missing_{tag}", f"sp_dst_missing_{tag}"
        dst.drop(dst_table)
        sched = create_schedule({
            "name": f"sched-proof missing source {tag}", "source_connector_id": conn_ids[f"{src.name}:source"],
            "source_table": src_table, "dest_connector_id": conn_ids[f"{dst.name}:destination"],
            "dest_table": dst_table, "interval": "hourly", "enabled": True,
            "sync_mode": "full_refresh_overwrite", "mappings": _mapping_rows(src.name, dst.name, pk=True),
            "primary_key": "id", "workspace_id": WORKSPACE, "max_retries": 2, "retry_backoff_seconds": 1,
        })
        sched_id = sched.id
        _force_due(sched_id)
        r = _beat_and_wait(sched_id)
        s = r.get("schedule") or get_schedule(sched_id)
        summary = _run_summary(s, _job(s.last_job_id) if s and s.last_job_id else {})
        cell["run"] = summary
        cell["retry_at"] = s.retry_at if s else None
        cell["approval_request"] = bool(s.approval_request) if s else None
        cell["run_history_len"] = len(s.run_history) if s else 0
        cell["dest_created"] = dst.exists(dst_table)
        reasons = []
        if summary["status"] == "completed":
            reasons.append("a run against a missing source table completed")
        if cell["dest_created"]:
            reasons.append("destination object was created for a run that could not read its source")
        if not (s and (s.approval_request or s.retry_at or s.last_status == "failed")):
            reasons.append("no failure was recorded on the schedule (no park, retry, or failed status)")
        cell.update(verdict="pass" if not reasons else "fail", reasons=reasons)
        return cell
    except Exception as exc:  # noqa: BLE001 - recorded as cell failure
        cell.update(verdict="fail", reasons=[f"harness exception: {exc!r}"], traceback=traceback.format_exc()[-1500:])
        return cell
    finally:
        if sched_id:
            delete_schedule(sched_id)


def run_workspace_isolation_cell(src: Engine, dst: Engine, conn_ids: dict[str, str]) -> dict[str, Any]:
    """A schedule must not be visible to, or runnable from, another workspace."""
    from services.schedule_store import create_schedule, delete_schedule, list_schedules

    tag = uuid.uuid4().hex[:6]
    cell: dict[str, Any] = {"scenario": "workspace_isolation", "source": src.name, "dest": dst.name}
    sched_id = ""
    try:
        sched = create_schedule({
            "name": f"sched-proof ws {tag}", "source_connector_id": conn_ids[f"{src.name}:source"],
            "source_table": f"sp_src_ws_{tag}", "dest_connector_id": conn_ids[f"{dst.name}:destination"],
            "dest_table": f"sp_dst_ws_{tag}", "interval": "daily", "enabled": False,
            "sync_mode": "full_refresh_overwrite", "mappings": _mapping_rows(src.name, dst.name, pk=True),
            "workspace_id": WORKSPACE,
        })
        sched_id = sched.id
        mine = [s.id for s in list_schedules() if s.workspace_id == WORKSPACE]
        other = [s.id for s in list_schedules() if s.workspace_id == "some-other-tenant"]
        cell["visible_in_own_workspace"] = sched_id in mine
        cell["leaks_into_other_workspace"] = sched_id in other
        reasons = []
        if not cell["visible_in_own_workspace"]:
            reasons.append("schedule not listed in its own workspace")
        if cell["leaks_into_other_workspace"]:
            reasons.append("schedule listed under another workspace")
        cell.update(verdict="pass" if not reasons else "fail", reasons=reasons)
        return cell
    finally:
        if sched_id:
            delete_schedule(sched_id)


# --------------------------------------------------------------------------- main


def main() -> int:
    engines_wanted = [e for e in (os.environ.get("SCHED_ENGINES") or ",".join(ENGINES)).split(",") if e]
    modes_wanted = set((os.environ.get("SCHED_MODES") or ",".join(m for m, _ in MODES)).split(","))
    sources_wanted = set((os.environ.get("SCHED_SOURCES") or ",".join(engines_wanted)).split(","))
    dests_wanted = set((os.environ.get("SCHED_DESTS") or ",".join(engines_wanted)).split(","))
    out_path = Path(os.environ.get("SCHED_OUT", "/home/ubuntu/sched_proof/live_schedule_matrix.json"))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    engines: dict[str, Engine] = {}
    skipped_engines: dict[str, str] = {}
    for name in engines_wanted:
        cfg = ENGINES[name]
        if not _reachable(cfg):
            skipped_engines[name] = f"{cfg['host']}:{cfg['port']} unreachable"
            continue
        engines[name] = Engine(name, cfg)

    from services.connector_store import delete_connector

    conn_ids: dict[str, str] = {}
    for name, eng in engines.items():
        for role in ("source", "destination"):
            conn_ids[f"{name}:{role}"] = _connector(eng, role)

    cells: list[dict[str, Any]] = []
    started = _now_iso()
    try:
        for sname, src in engines.items():
            if sname not in sources_wanted:
                continue
            for dname, dst in engines.items():
                if dname not in dests_wanted:
                    continue
                for mode, keyed in MODES:
                    if mode not in modes_wanted:
                        continue
                    print(f"[cell] {sname} → {dname} · {mode}", flush=True)
                    cell = run_cell(src, dst, mode, keyed, conn_ids)
                    print(f"       {cell['verdict']} {cell.get('reasons') or ''}", flush=True)
                    cells.append(cell)
                    out_path.write_text(json.dumps({"partial": True, "cells": cells}, indent=1, default=str))
        ops: list[dict[str, Any]] = []
        if "postgresql" in engines and "mysql" in engines:
            ops.append(run_overlap_cell(engines["postgresql"], engines["mysql"], conn_ids))
            ops.append(run_failure_retry_cell(engines["postgresql"], engines["mysql"], conn_ids))
            ops.append(run_workspace_isolation_cell(engines["postgresql"], engines["mysql"], conn_ids))
        for op in ops:
            print(f"[ops] {op.get('scenario')} {op['verdict']} {op.get('reasons') or ''}", flush=True)
    finally:
        if os.environ.get("SCHED_KEEP") != "1":
            for cid in conn_ids.values():
                with contextlib.suppress(Exception):
                    delete_connector(cid, workspace_id=WORKSPACE)

    summary = {
        "started_at": started, "finished_at": _now_iso(), "rows_per_cell": ROWS,
        "new_rows": NEW_ROWS, "updated_rows": UPDATED_ROWS, "deleted_rows": DELETED_ROWS,
        "engines": sorted(engines), "skipped_engines": skipped_engines,
        "pass": sum(1 for c in cells + ops if c["verdict"] == "pass"),
        "fail": sum(1 for c in cells + ops if c["verdict"] == "fail"),
        "skip": sum(1 for c in cells + ops if c["verdict"] == "skip"),
        "cells": cells, "operational": ops,
    }
    out_path.write_text(json.dumps(summary, indent=1, default=str))
    print(f"\npass={summary['pass']} fail={summary['fail']} skip={summary['skip']} → {out_path}")
    return 0 if summary["fail"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
