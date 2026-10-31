"""QA round 10: the false "fidelity collapse" family, run 1 and a fresh run 2.

* D225 (MED-HIGH). A recurring Oracle -> SQLite incremental sync stalls after
  run 1: the second run into the table DataFlow created is refused as
  "Lossy / fidelity collapse ... (D, TS)". Root cause: SQLite stores an Oracle
  ``DATE`` / ``TIMESTAMP`` in the ``TEXT`` column DataFlow creates for it and
  writes ``2026-10-01T00:00:00``. The SQLite read-back inferred ``DATE`` from a
  column of such midnight cells (an Excel heuristic: ``T00:00:00`` = date-only
  cell), so run 2 graded ``TIMESTAMP -> DATE`` as a lossy cast on DataFlow's
  own column. A manual *rerun* replays run 1's create-new mappings and never
  re-reads the column, which is why it worked; a schedule saved after run 1
  (or any fresh plan) reads the table back and was refused.
* D161 remainder. S3 -> a NEW SQLite table was held at G4 (``ratio`` 0.70):
  the IEEE/Excel-residue chip only knew a sized ``DECIMAL(p,s)`` as an exact
  carrier, and SQLite's exact decimal carrier is ``TEXT``.
* D212 check. Oracle -> SQL Server stores '東京' exactly through the MCP route
  and through a schedule-fired run 1 and run 2.
* D204 guard. A CSV with non-ASCII text into a new SQL Server table loads on
  run 1 and run 2 through MCP, the REST routes, Studio and a schedule.

Every case runs on real engines: Oracle Free :1521, SQLite files, SQL Server
:1433, MongoDB :27017 and S3 (moto's server behind the real boto3 client, the
repo's S3 pattern). Each test skips when its engine is absent.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h
from tests.test_r6_d207_oracle_watermark_typed_bind_e2e import _beat, _ora, _oracle_cfg

pytestmark = pytest.mark.timeout(900)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    import services.transfer_plan_store as plan_store

    monkeypatch.setattr(plan_store, "STORE_PATH", tmp_path / "transfer_plans.json")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _schedule(client, **args: Any) -> str:
    """MCP ``create_schedule`` -> ``/api/v1/copilot/confirm``; the schedule id."""
    ok, staged = h.mcp_call(client, "create_schedule", {"cadence": "every 15 minutes", **args})
    assert ok, staged
    assert not h.blockers_of(staged), h.blockers_of(staged)
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "r10"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    return str(body.get("schedule_id") or (body.get("schedule") or {}).get("id") or "")


def _completed(job: dict, run: str) -> None:
    assert str(job.get("status")) == "completed", {
        "run": run, "status": job.get("status"),
        "error": str(job.get("error") or job.get("message") or "")[:800],
    }


def _start(client, **args: Any) -> dict:
    """MCP ``start_transfer`` -> confirm -> job; no blocker, no Risk Contract."""
    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    for m in (staged.get("plan") or staged).get("mappings") or []:
        assert not m.get("requires_risk_contract"), m
    return h.confirm(client, staged["ack_id"], reason="r10")


# --------------------------------------------------------------------------- #
# D225: Oracle -> SQLite, recurring incremental sync
# --------------------------------------------------------------------------- #

_T0 = datetime(2026, 10, 1)  # noqa: DTZ001 - Oracle DATE/TIMESTAMP are wall-clock
_NAMES = ["Ada", None, "  ", "東京 Łukasz 🎉", ""]


def _ora_rows(n: int = 12) -> list[list[Any]]:
    """Every DATE / TIMESTAMP in D and TS is midnight: the shape that tripped
    the read-back. EVENT_AT carries a clock time; UPDATED_AT is the cursor."""
    return [
        [i, _NAMES[i % len(_NAMES)], "中文 ü", "AB", Decimal("12.34") * i, i * 7,
         "clob é " * (i % 3), _T0 + timedelta(days=i), _T0 + timedelta(days=i),
         _T0 + timedelta(days=i, hours=i, minutes=7, seconds=5), _T0 + timedelta(minutes=i)]
        for i in range(1, n + 1)
    ]


_ORA_COLS = ("ID", "NAME", "NNAME", "CODE", "AMOUNT", "QTY", "BODY", "D", "TS", "EVENT_AT",
             "UPDATED_AT")


def _ora_create(table: str) -> None:
    _ora(
        f'CREATE TABLE "{table}" (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(60), '
        "NNAME NVARCHAR2(40), CODE CHAR(2), AMOUNT NUMBER(12,2), QTY NUMBER, BODY CLOB, "
        "D DATE, TS TIMESTAMP(6), EVENT_AT TIMESTAMP(6), UPDATED_AT TIMESTAMP(6) NOT NULL)"
    )
    for row in _ora_rows():
        _ora(f'INSERT INTO "{table}" VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)', row)


def _ora_edit(table: str, rid: int, name: str | None, at: datetime) -> None:
    _ora(f'UPDATE "{table}" SET NAME = :1, UPDATED_AT = :2 WHERE ID = :3', [name, at, rid])


def _norm(col: str, value: Any) -> Any:
    """One comparable cell: SQLite keeps temporals and exact numbers as text."""
    if value is None:
        return None
    if col in {"D", "TS", "EVENT_AT", "UPDATED_AT"}:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", ""))
        return value.replace(tzinfo=None)
    if col in {"AMOUNT", "QTY", "ID"}:
        return Decimal(str(value))
    if hasattr(value, "read"):
        value = value.read()
    return value if value != "" else None  # Oracle stores '' as NULL


def _ora_image(table: str) -> dict[int, tuple]:
    # CLOB as text: a LOB locator cannot be read once _ora closed its connection.
    cols = ", ".join("TO_CHAR(BODY)" if c == "BODY" else c for c in _ORA_COLS)
    rows = _ora(f'SELECT {cols} FROM "{table}" ORDER BY ID')  # nosec B608
    return {int(r[0]): tuple(_norm(c, v) for c, v in zip(_ORA_COLS, r)) for r in rows}


def _sqlite_image(db, table: str) -> dict[int, tuple]:
    conn = sqlite3.connect(db)
    try:
        cols = ", ".join(f'"{c}"' for c in _ORA_COLS)
        rows = conn.execute(f'SELECT {cols} FROM "{table}"').fetchall()  # nosec B608
    finally:
        conn.close()
    return {int(r[0]): tuple(_norm(c, v) for c, v in zip(_ORA_COLS, r)) for r in rows}


def _oracle_sqlite(tmp_path, token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"r10-ora-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **_oracle_cfg()})
    db = tmp_path / f"r10_{token}.sqlite"
    dst = create_connector({"name": f"r10-sqlite-{token}", "role": "destination",
                            "workspace_id": "", "type": "sqlite", "host": str(db),
                            "database": str(db)})
    return src, dst, db


_INCREMENTAL = {
    "sync_mode": "incremental_upsert", "primary_key": "ID", "cursor_column": "UPDATED_AT",
    "cursor_semantics": "modification_timestamp",
}


def test_d225_oracle_sqlite_manual_run1_then_scheduled_run2(client, tmp_path):
    """QA's shape: run 1 creates the table; the recurring schedule's fresh run 2
    (and the runs after it) land in the table DataFlow created."""
    token = uuid.uuid4().hex[:6].upper()
    src_t, dst_t = f"R10_D225_{token}", f"r10_d225_{token.lower()}"
    try:
        _ora_create(src_t)
        src, dst, db = _oracle_sqlite(tmp_path, token)
        route = {"source_connector_id": src.id, "source_table": src_t,
                 "dest_connector_id": dst.id, "dest_table": dst_t, **_INCREMENTAL}
        _start(client, **route)  # run 1: CREATE + 12 rows
        assert _sqlite_image(db, dst_t) == _ora_image(src_t)

        _ora_edit(src_t, 3, "edited ✓", _T0 + timedelta(hours=1))
        _ora_edit(src_t, 4, None, _T0 + timedelta(hours=1))
        schedule_id = _schedule(client, name=f"r10-d225-{token}", **route)
        j2 = _beat(schedule_id)  # fresh scheduled run 2 into the created table
        _completed(j2, "scheduled run 2")
        assert _sqlite_image(db, dst_t) == _ora_image(src_t)

        _ora_edit(src_t, 5, "  ", _T0 + timedelta(hours=2))
        j3 = _beat(schedule_id)
        _completed(j3, "scheduled run 3")
        assert _sqlite_image(db, dst_t) == _ora_image(src_t)

        _ora_edit(src_t, 6, "manual ü", _T0 + timedelta(hours=3))
        # A fresh manual run reads the table back too (paused schedule: an
        # enabled pipeline owns the destination).
        from services.schedule_store import update_schedule

        update_schedule(schedule_id, {"enabled": False})
        _start(client, **route)
        assert _sqlite_image(db, dst_t) == _ora_image(src_t)
    finally:
        try:
            _ora(f'DROP TABLE "{src_t}" PURGE')
        except Exception:  # noqa: BLE001, S110 - best-effort cleanup
            pass


def test_d225_oracle_sqlite_scheduled_run1_and_run2(client, tmp_path):
    """The schedule creates the table on its first beat; run 2 lands in it."""
    token = uuid.uuid4().hex[:6].upper()
    src_t, dst_t = f"R10_D225S_{token}", f"r10_d225s_{token.lower()}"
    try:
        _ora_create(src_t)
        src, dst, db = _oracle_sqlite(tmp_path, token)
        schedule_id = _schedule(
            client, name=f"r10-d225s-{token}", source_connector_id=src.id,
            source_table=src_t, dest_connector_id=dst.id, dest_table=dst_t, **_INCREMENTAL,
        )
        _completed(_beat(schedule_id), "scheduled run 1")
        assert _sqlite_image(db, dst_t) == _ora_image(src_t)
        _ora_edit(src_t, 2, "東京 run 2", _T0 + timedelta(hours=1))
        _completed(_beat(schedule_id), "scheduled run 2")
        assert _sqlite_image(db, dst_t) == _ora_image(src_t)
    finally:
        try:
            _ora(f'DROP TABLE "{src_t}" PURGE')
        except Exception:  # noqa: BLE001, S110 - best-effort cleanup
            pass


# --------------------------------------------------------------------------- #
# D161 remainder: S3 CSV decimals -> a NEW SQLite (and MongoDB) table
# --------------------------------------------------------------------------- #

_D161_HEADER = ["id", "price", "ratio", "measure", "tiny", "label"]
_D161_ROWS = [
    ("1", "1.10", "0.30000000000000004", "3.14159", "1e-7", "a"),
    ("2", "2.50", "0.1", "0.1", "2.5e-3", None),
    ("3", None, "0.7", "2.71828", None, ""),
    ("4", "19.99", "0.2", "1.5", "0.000001", "Zoë 東京 ✓"),
    ("5", "0.30", None, "10.25", "1E-7", "  padded  "),
    ("6", "-7.05", "1.0000000000000002", "-0.5", "3e-7", "x"),
]


def _d161_csv() -> bytes:
    buf = io.StringIO()
    buf.write(",".join(_D161_HEADER) + "\n")
    for row in _D161_ROWS:
        cells = []
        for value in row:
            if value is None:
                cells.append("")
            elif value == "" or value != value.strip() or not value.isascii():
                cells.append('"' + value.replace('"', '""') + '"')
            else:
                cells.append(value)
        buf.write(",".join(cells) + "\n")
    return buf.getvalue().encode("utf-8")


def _s3_csv(key: str, body: bytes):
    """moto's S3 server (real boto3 wire) holding one object; a source connector."""
    boto3 = pytest.importorskip("boto3")
    moto_server = pytest.importorskip("moto.server")
    from services.connector_store import create_connector

    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    s3 = boto3.client("s3", endpoint_url=f"http://{host}:{port}", aws_access_key_id="test",
                      aws_secret_access_key="test", region_name="us-east-1")
    bucket = "r10-" + uuid.uuid4().hex[:8]
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    conn = create_connector({
        "name": "r10-s3-" + uuid.uuid4().hex[:6], "type": "s3", "role": "source",
        "host": host, "port": int(port), "database": bucket, "username": "test",
        "password": "test", "ssl": False, "extra": {"path_style": True}, "workspace_id": "",
    })
    return conn, server.stop


def _d161_expected() -> dict[int, tuple]:
    return {int(r[0]): tuple(r[1:]) for r in _D161_ROWS}


def _d161_sqlite_image(db, table: str) -> dict[int, tuple]:
    conn = sqlite3.connect(db)
    try:
        cols = ", ".join(f'"{c}"' for c in _D161_HEADER)
        rows = conn.execute(f'SELECT {cols} FROM "{table}"').fetchall()  # nosec B608
    finally:
        conn.close()
    return {int(r[0]): tuple(r[1:]) for r in rows}


def _d161_assert_exact(image: dict[int, tuple]) -> None:
    want = _d161_expected()
    assert sorted(image) == sorted(want), image
    for rid, row in want.items():
        for col, got, text in zip(_D161_HEADER[1:], image[rid], row):
            if text is None:
                assert got is None, (rid, col, got)
            elif col == "label":
                assert got == text, (rid, col, got, text)
            else:
                # Exact digits: no binary rounding, no float repr; money keeps scale.
                assert isinstance(got, (str, Decimal)), (rid, col, type(got), got)
                assert Decimal(str(got)) == Decimal(text), (rid, col, got, text)
                if col == "price":
                    assert str(got) == text, (rid, col, got, text)


def test_d161_s3_csv_decimals_into_new_sqlite_run1_run2_and_scheduled(client, tmp_path):
    from services.connector_store import create_connector

    db = tmp_path / "r10_d161.sqlite"
    dst = create_connector({"name": "r10-d161-sqlite", "role": "destination",
                            "workspace_id": "", "type": "sqlite", "host": str(db),
                            "database": str(db)})
    src, stop = _s3_csv("d161/measures.csv", _d161_csv())
    table = "r10_d161_" + uuid.uuid4().hex[:6]
    route = {"source_connector_id": str(src.id), "source_table": "d161/measures.csv",
             "dest_connector_id": str(dst.id), "dest_table": table,
             "sync_mode": "full_refresh_overwrite", "primary_key": "id"}
    try:
        _start(client, **route)  # run 1 creates the table
        _d161_assert_exact(_d161_sqlite_image(db, table))
        _start(client, **route)  # run 2 into the table run 1 created
        _d161_assert_exact(_d161_sqlite_image(db, table))
        schedule_id = _schedule(client, name="r10-d161-" + table, **route)
        _completed(_beat(schedule_id), "scheduled run 3")
        _d161_assert_exact(_d161_sqlite_image(db, table))
    finally:
        stop()


def test_d161_s3_csv_decimals_into_new_mongodb_collection(client):
    """Same family on MongoDB: Decimal128 is the exact carrier (34 digits)."""
    from services.connector_store import create_connector
    from tests.test_family_b_run2_own_table_e2e import _dest_config

    cfg = _dest_config("mongodb")
    pymongo = pytest.importorskip("pymongo")
    dst = create_connector({"name": "r10-d161-mongo", "role": "destination",
                            "workspace_id": "", "ssl": False, **cfg})
    src, stop = _s3_csv("d161/measures.csv", _d161_csv())
    coll = "r10_d161_" + uuid.uuid4().hex[:6]
    route = {"source_connector_id": str(src.id), "source_table": "d161/measures.csv",
             "dest_connector_id": str(dst.id), "dest_table": coll,
             "sync_mode": "full_refresh_overwrite", "primary_key": "id"}
    mongo = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    try:
        for _run in (1, 2):
            _start(client, **route)
            docs = {int(d["id"]): d for d in mongo[cfg["database"]][coll].find()}
            assert sorted(docs) == sorted(_d161_expected()), docs
            for rid, row in _d161_expected().items():
                for col, text in zip(_D161_HEADER[1:], row):
                    got = docs[rid].get(col)
                    if text is None:
                        assert got is None, (rid, col, got)
                    elif col == "label":
                        assert got == text, (rid, col, got)
                    else:
                        got_d = got.to_decimal() if hasattr(got, "to_decimal") else got
                        assert not isinstance(got_d, float), (rid, col, got)
                        assert Decimal(str(got_d)) == Decimal(text), (rid, col, got, text)
    finally:
        stop()
        mongo[cfg["database"]][coll].drop()
        mongo.close()


# --------------------------------------------------------------------------- #
# D212: Oracle -> SQL Server keeps '東京' through MCP and a schedule-fired run
# --------------------------------------------------------------------------- #

_D212_ROWS = [(1, "東京", "日本語"), (2, "🚀 launch", "é combining"), (3, None, ""),
              (4, "plain ascii", "  "), (5, "Łódź 山田 🎉", "Grüße ß")]


def _d212_source(table: str, rows) -> None:
    for rid, city, note in rows:
        _ora(f'DELETE FROM "{table}" WHERE ID = :1', [rid])
        _ora(f'INSERT INTO "{table}" (ID, CITY, NOTE, UPDATED_AT) VALUES (:1, :2, :3, :4)',
             [rid, city, note, _T0 + timedelta(minutes=rid)])


def _d212_expected(table: str) -> dict[int, tuple]:
    rows = _ora(f'SELECT ID, CITY, NOTE FROM "{table}"')  # nosec B608
    return {int(r[0]): (r[1], r[2]) for r in rows}


def _ss_image(table: str) -> dict[int, tuple]:
    rows = h.query("sqlserver", f"SELECT ID, CITY, NOTE FROM [{table}]")  # nosec B608
    return {int(r[0]): (r[1], r[2]) for r in rows}


def _ss_text_types(table: str) -> dict[str, str]:
    rows = h.query("sqlserver", "SELECT UPPER(COLUMN_NAME), DATA_TYPE FROM "
                   f"INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
    return {str(r[0]): str(r[1]).lower() for r in rows}


@pytest.mark.parametrize("first_run", ["mcp", "schedule"])
def test_d212_oracle_sqlserver_unicode_exact_mcp_and_scheduled(client, first_run):
    from services.connector_store import create_connector

    cfg = h.dest_config("sqlserver")
    token = uuid.uuid4().hex[:6].upper()
    src_t, dst_t = f"R10_D212_{token}", f"r10_d212_{first_run}_{token.lower()}"
    try:
        _ora(f'CREATE TABLE "{src_t}" (ID NUMBER(10) PRIMARY KEY, CITY VARCHAR2(100), '
             "NOTE VARCHAR2(100), UPDATED_AT TIMESTAMP(6) NOT NULL)")
        _d212_source(src_t, _D212_ROWS)
        src = create_connector({"name": f"r10-d212-s-{token}", "role": "source",
                                "workspace_id": "", "ssl": False, **_oracle_cfg()})
        dst = create_connector({"name": f"r10-d212-d-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **cfg})
        route = {"source_connector_id": src.id, "source_table": src_t,
                 "dest_connector_id": dst.id, "dest_table": dst_t, **_INCREMENTAL}
        if first_run == "mcp":
            _start(client, **route)
            schedule_id = ""
        else:
            schedule_id = _schedule(client, name=f"r10-d212-{token}", **route)
            _completed(_beat(schedule_id), "scheduled run 1")
        types = _ss_text_types(dst_t)
        assert types.get("CITY") in {"nvarchar", "nchar"}, types
        assert types.get("NOTE") in {"nvarchar", "nchar"}, types
        assert _ss_image(dst_t) == _d212_expected(src_t)

        _d212_source(src_t, [(1, "東京 updated 🚀", "日本語"), (6, "新しい 行", "ño")])
        _ora(f'UPDATE "{src_t}" SET UPDATED_AT = :1 WHERE ID IN (1, 6)',
             [_T0 + timedelta(hours=1)])
        if not schedule_id:
            schedule_id = _schedule(client, name=f"r10-d212-{token}", **route)
        _completed(_beat(schedule_id), "scheduled run 2")
        image = _ss_image(dst_t)
        assert image == _d212_expected(src_t)
        assert image[1][0] == "東京 updated 🚀" and "?" not in image[6][0], image
    finally:
        try:
            _ora(f'DROP TABLE "{src_t}" PURGE')
        except Exception:  # noqa: BLE001, S110 - best-effort cleanup
            pass
        h.drop_table("sqlserver", dst_t)


# --------------------------------------------------------------------------- #
# D204 guard: CSV with non-ASCII text into a NEW SQL Server table
# --------------------------------------------------------------------------- #

_D204_HEADER = ["id", "name", "city", "note"]
_D204_ROWS = [("1", "café", "Straße", "plain"), ("2", "中文", "naïve ✓", "ascii only"),
              ("3", "emoji 😀", "𝔘𝔫𝔦𝔠𝔬𝔡𝔢", "x"), ("4", "", "  ", "y"),
              ("5", "東京", "end", "z")]


def _d204_csv() -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(_D204_HEADER)
    w.writerows(_D204_ROWS)
    return buf.getvalue().encode("utf-8")


def _d204_assert(table: str) -> None:
    got = {int(r[0]): tuple(r[1:]) for r in
           h.query("sqlserver", f"SELECT id, name, city, note FROM dbo.[{table}]")}  # nosec B608
    want = {int(r[0]): tuple(r[1:]) for r in _D204_ROWS}
    assert sorted(got) == sorted(want), got
    for rid, row in want.items():
        for g, w in zip(got[rid], row):
            # A CSV empty field: the file policy decides NULL or ''.
            assert g == w or (w == "" and g is None), (rid, g, w)
    types = _ss_text_types(table)
    for col in ("NAME", "CITY", "NOTE"):
        assert types[col] in {"nvarchar", "nchar"}, types


@pytest.mark.parametrize("route", ["mcp", "rest", "studio", "schedule"])
def test_d204_csv_non_ascii_into_new_sqlserver_run1_run2(client, route):
    from services.connector_store import create_connector

    cfg = h.dest_config("sqlserver")
    dst = create_connector({"name": "r10-d204-" + uuid.uuid4().hex[:6], "role": "destination",
                            "workspace_id": "", "ssl": False, **cfg})
    key = "d204/people.csv"
    src, stop = _s3_csv(key, _d204_csv())
    table = "r10_d204_" + uuid.uuid4().hex[:6]
    source = {"kind": "database", "format": "s3", "connector_id": src.id,
              "database": src.database, "table": key}
    dest = {"kind": "database", "format": "sqlserver", "connector_id": dst.id,
            "table": table, "schema": "dbo", "extra": dict(cfg.get("extra") or {})}
    args = {"source_connector_id": src.id, "source_table": key,
            "dest_connector_id": dst.id, "dest_table": table,
            "sync_mode": "full_refresh_overwrite", "primary_key": "id"}
    schedule_id = ""
    try:
        for run in (1, 2):
            if route == "mcp":
                _start(client, **args)
            elif route == "schedule":
                schedule_id = schedule_id or _schedule(client, name="r10-d204-" + table, **args)
                _completed(_beat(schedule_id), f"scheduled run {run}")
            elif route == "rest":
                mappings = [{"source": c, "target": c, "confidence": 1.0} for c in _D204_HEADER]
                pf = client.post("/api/v1/preflight/run", json={
                    "dataset_name": table, "columns": _D204_HEADER,
                    "column_types": {c: "text" for c in _D204_HEADER}, "mappings": mappings,
                    "source_connector_id": src.id, "source_kind": "database",
                    "source_type": "s3", "source_table": key, "connector_id": dst.id,
                    "dest_kind": "database", "dest_type": "sqlserver", "dest_table": table,
                    "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
                }).json()
                assert not pf.get("blockers"), [(b.get("id"), b.get("message"))
                                                for b in pf.get("blockers") or []]
                body = client.post("/api/v1/transfer/execute", json={
                    "source": source, "destination": dest, "mappings": mappings,
                    "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
                    "schema_policy": "manual_review", "async_mode": False,
                    "skip_preflight": False,
                }).json()
                assert body.get("success") is True, json.dumps(body, default=str)[:800]
            else:
                from tests.r7_studio_harness import Studio, blockers, preflight_mappings

                studio = Studio(client, source=source, destination=dest, policies={
                    "sync_mode": "full_refresh_overwrite", "schema_policy": "manual_review",
                    "validation_mode": "strict"})
                si, di = studio.introspect()
                sc, ss = studio.columns(si)
                exists = bool(di.get("table_exists"))
                dc, ds = studio.columns(di) if exists else ([], {})
                maps = preflight_mappings(studio.map(sc, ss, dc, ds, table_exists=exists),
                                          dest_columns=dc if exists else None)
                pf = studio.validate(sc, ss, dc, ds, maps)
                assert pf.get("passed"), blockers(pf)
                job = studio.execute(maps)
                _completed(job, f"studio run {run}")
            _d204_assert(table)
    finally:
        stop()
        h.drop_table("sqlserver", table)
