"""D211 - a recurring MongoDB sync into a SQL table DataFlow created survives run 2.

QA (production 557f556c): Mongo -> MariaDB, incremental upsert into a new table.
Run 1 moved all rows. Four source documents changed; the re-run and a second
manual run then failed with 0 rows and "fidelity collapse ARRAY -> LONGTEXT".
The column was the one DataFlow itself created for the array (MariaDB's JSON is
an alias of ``LONGTEXT COLLATE utf8mb4_bin``, and introspection reads it back as
LONGTEXT).

Root cause: run 2 grades the source's *logical* document types (Mongo ARRAY /
OBJECT, DynamoDB L / M, Kafka JSON arrays and objects, Elasticsearch nested,
Firestore maps and arrays: ARRAY / STRUCT / MAP) against the destination column
as introspection reads it back. D198 taught the document grader to recognise
DataFlow's own JSON carrier in its read-back spelling, but only for a JSON
*source*; the nested-shape grader still required the read-back to normalise to
logical JSON, and LONGTEXT normalises to TEXT.

QA's route on real engines, for every destination available locally
(MariaDB, MySQL, SQL Server, PostgreSQL, Oracle):

  1. run 1 (``start_transfer`` incremental_upsert -> ``/api/v1/copilot/confirm``)
     creates the table from a Mongo collection with arrays, nested objects,
     empty arrays/objects, arrays of mixed types, NULL and a missing field;
  2. four documents change in the source;
  3. run 2 (a second manual run) and a rerun (MCP ``rerun_job``) succeed with no
     contract, risk acceptance or acknowledgement;
  4. every cell reads back equal, documents compared as JSON values.
"""

from __future__ import annotations

import json
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from tests import r4_route_harness as h
from tests.test_family_b_run2_own_table_e2e import _dest_config, _drop

pytestmark = pytest.mark.timeout(900)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")  # Settings parses it as a bool
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c

_MONGO_DB = "dataflow_test"
_DOC_COLS = ("tags", "meta", "empty_list", "empty_obj", "mixed", "maybe")
_COLS = ("id", *_DOC_COLS, "note", "updated_at")

_T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


def _seed() -> dict[int, dict[str, Any]]:
    rows: dict[int, dict[str, Any]] = {}
    for i in range(1, 31):
        doc: dict[str, Any] = {
            "id": i,
            "tags": [f"t{i}", "山田", ""],
            "meta": {"level": i, "inner": {"ok": i % 2 == 0, "list": [i, None]}, "s": "é ü"},
            "empty_list": [],
            "empty_obj": {},
            "mixed": [i, "two", 3.5, True, None, {"k": [1]}, ["x"]],
            "maybe": None if i % 3 == 0 else {"v": i},
            "note": ["", "   ", "Łukasz 🎉", None, f"n{i}"][i % 5],
            "updated_at": _T0 + timedelta(seconds=i),
        }
        if i % 4 == 0:
            del doc["maybe"]  # a missing field, not a NULL
        rows[i] = doc
    return rows


def _edits() -> dict[int, dict[str, Any]]:
    """Four documents change in place (QA: "4 source rows changed"). Same keys:
    a new key is a new flattened column, which is schema drift by design."""
    t1 = _T0 + timedelta(days=1)
    return {
        # meta.s goes to "s2", not '': Oracle's upsert deliberately omits a
        # '' cell from MERGE SET (it would be NULL), a separate policy.
        2: {"tags": [], "meta": {"level": 2000, "inner": {"ok": None, "list": []}, "s": "s2"},
            "updated_at": t1},
        5: {"mixed": [None, None], "maybe": None, "note": "edited ✓", "updated_at": t1},
        8: {"maybe": {"v": 800}, "empty_list": [0, "", None], "updated_at": t1},  # was missing
        13: {"meta": {"level": None, "inner": {"ok": True, "list": ["中文", 1.25, None, []]},
                      "s": "   "},
             "tags": ["only"], "mixed": [], "updated_at": t1},
    }


def _mongo():
    if not _up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    pymongo = pytest.importorskip("pymongo")
    return pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000,
                               directConnection=True, tz_aware=True)


def _up(port: int, host: str = "127.0.0.1") -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def _run(client, src_id: str, dst_id: str, coll: str, dest: str) -> dict:
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(
        source_connector_id=src_id, source_table=coll,
        dest_connector_id=dst_id, dest_table=dest,
        sync_mode="incremental_upsert", primary_key="id",
        cursor_column="updated_at", cursor_semantics="modification_timestamp",
        source_timezone="UTC",
    )
    plan = (staged.output or {}).get("plan") or staged.output or {}
    blockers = [
        f"{b.get('gate') or b.get('id')}: {b.get('message')}"
        for b in ((plan.get("preflight") or {}).get("blockers") or [])
    ]
    assert staged.success, {"error": staged.error, "blockers": blockers}
    assert not blockers, blockers
    for m in plan.get("mappings") or []:
        assert not m.get("requires_risk_contract"), m
    return _confirm(client, staged.output["ack_id"])


def _wait_job(job_id: str, timeout_s: float = 240.0) -> dict:
    """Wait for any terminal job status (``completed_with_quarantine`` included)."""
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        status = str(job.get("status") or "")
        if status in {"completed", "failed", "partial", "cancelled"} or status.startswith(
            "completed_"
        ):
            return job
        time.sleep(0.25)
    return job


def _confirm(client, ack_id: str) -> dict:
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": "d211"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    job = _wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status")) == "completed", {
        "status": job.get("status"), "error": job.get("error") or job.get("message"),
    }
    return job


def _rerun(client, job: dict) -> dict:
    job_id = str(job.get("job_id") or job.get("_id") or job.get("id") or "")
    ok, staged = h.mcp_call(client, "rerun_job", {"job_id": job_id})
    assert ok, staged
    assert not h.blockers_of(staged), h.blockers_of(staged)
    return _confirm(client, staged["ack_id"])


# --------------------------------------------------------------------------- #
# Read-back and cell comparison
# --------------------------------------------------------------------------- #


def _sql_rows(kind: str, table: str) -> list[tuple]:
    from tests.test_family_b_run2_own_table_e2e import _pg_conn

    cfg = _dest_config(kind)
    if kind == "postgresql":
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute(f'SELECT {", ".join(_COLS)} FROM public."{table}"')  # nosec B608
        rows = [tuple(r) for r in cur.fetchall()]
        conn.close()
        return rows
    if kind in {"mysql", "mariadb"}:
        import pymysql

        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                               password=cfg["password"], database=cfg["database"],
                               charset="utf8mb4")
        cur = conn.cursor()
        cur.execute(f"SELECT {', '.join(f'`{c}`' for c in _COLS)} FROM `{table}`")  # nosec B608
        rows = [tuple(r) for r in cur.fetchall()]
        conn.close()
        return rows
    if kind == "sqlserver":
        import pyodbc

        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
            f"UID={cfg['username']};PWD={cfg['password']};TrustServerCertificate=yes",
            timeout=10,
        )
        conn.add_output_converter(-155, lambda raw: raw)  # read DATETIMEOFFSET as bytes
        sql = f"SELECT {', '.join(f'[{c}]' for c in _COLS)} FROM [{table}]"  # nosec B608
        rows = [tuple(r) for r in conn.execute(sql).fetchall()]
        conn.close()
        return rows
    if kind == "oracle":
        import oracledb

        conn = oracledb.connect(user=cfg["username"], password=cfg["password"],
                                dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}")
        cur = conn.cursor()
        quoted = ", ".join(f'"{c.upper()}"' for c in _COLS)
        cur.execute(f'SELECT {quoted} FROM "{table.upper()}"')  # nosec B608
        rows = [tuple(v.read() if hasattr(v, "read") else v for v in r) for r in cur.fetchall()]
        conn.close()
        return rows
    raise AssertionError(kind)


def _doc(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    return value


def _norm_doc(value: Any) -> Any:
    """A document as JSON values: numbers compare as numbers (1 == 1.0 is not
    assumed: ints stay ints, floats stay floats)."""
    if isinstance(value, dict):
        return {k: _norm_doc(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_norm_doc(v) for v in value]
    return value


def _read_back(kind: str, table: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for row in _sql_rows(kind, table):
        cells = dict(zip(_COLS, row))
        rid = int(cells["id"])
        got = {c: (None if cells[c] is None else _norm_doc(_doc(cells[c]))) for c in _DOC_COLS}
        got["note"] = cells["note"]
        out[rid] = got
    return out


def _expected(source: dict[int, dict[str, Any]], kind: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for rid, doc in source.items():
        want = {c: (None if doc.get(c) is None else _norm_doc(doc[c])) for c in _DOC_COLS}
        note = doc.get("note")
        # Oracle has no empty string: '' IS NULL in every Oracle text type.
        want["note"] = None if kind == "oracle" and note == "" else note
        out[rid] = want
    return out


def _assert_cells(kind: str, table: str, source: dict[int, dict[str, Any]], label: str) -> None:
    got = _read_back(kind, table)
    want = _expected(source, kind)
    assert sorted(got) == sorted(want), (kind, label, sorted(set(got) ^ set(want)))
    for rid in sorted(want):
        for col in (*_DOC_COLS, "note"):
            assert got[rid][col] == want[rid][col], (kind, label, rid, col, got[rid][col],
                                                     want[rid][col])


# --------------------------------------------------------------------------- #
# The test
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("kind", ["mariadb", "mysql", "sqlserver", "postgresql", "oracle"])
def test_mongo_documents_run2_and_rerun_into_own_table(client, kind):
    from services.connector_store import create_connector

    dest_cfg = _dest_config(kind)
    mongo = _mongo()
    token = uuid.uuid4().hex[:8]
    coll = f"d211_{kind[:5]}_{token}"
    dest = coll + "_out"
    source = _seed()
    col = mongo[_MONGO_DB][coll]
    try:
        col.insert_many([dict(d) for d in source.values()])
        src = create_connector({
            "name": f"d211-mongo-{token}", "type": "mongodb", "role": "source",
            "workspace_id": "", "ssl": False, "host": "127.0.0.1", "port": 27017,
            "database": _MONGO_DB,
        })
        dst = create_connector({
            "name": f"d211-{kind}-{token}", "role": "destination", "workspace_id": "",
            "ssl": False, **dest_cfg,
        })

        # Run 1 creates the table.
        _run(client, src.id, dst.id, coll, dest)
        _assert_cells(kind, dest, source, "run 1")

        # Four documents change in the source.
        for rid, change in _edits().items():
            col.update_one({"id": rid}, {"$set": change})
            source[rid] = {**source[rid], **change}

        # Run 2 (a second manual run) into DataFlow's own table.
        job2 = _run(client, src.id, dst.id, coll, dest)
        _assert_cells(kind, dest, source, "run 2")
        assert int(job2.get("records_processed") or job2.get("rows_written") or 0) >= 1, job2

        # And a rerun of that job.
        _rerun(client, job2)
        _assert_cells(kind, dest, source, "rerun")
    finally:
        col.drop()
        mongo.close()
        _drop(kind, dest)

