"""Studio parity matrix: Validate's verdict against what Run actually does.

Every scenario drives the *product* HTTP path an operator drives — ``/transfer/map``
(Map), ``/preflight/run`` (Validate) and ``/transfer/run`` (Run) — across real
source and destination engines and every sync mode the route offers. For each
one it records the Validate verdict, the Run verdict and the measured
destination state, then judges three things the product must never do:

* **parity break** — Validate cleared the run and Run failed anyway (the class an
  operator reports as "validation passed but it died in the middle");
* **contract break** — Run succeeded but the destination does not hold what the
  sync mode promises (append that lost rows, overwrite that kept them, upsert
  that duplicated a key, mirror that hid nothing);
* **false block** — Validate refused a run that the same fixture proves is safe.

Nothing is asserted and nothing is rounded to green. The artifact reports what
was measured, and a scenario whose measurement disagrees with its declared
contract is a product gap.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

import requests

API = os.environ.get("DATAFLOW_API", "http://127.0.0.1:8001/api/v1")

PG = dict(host="127.0.0.1", port=5433, database="dataflow", username="postgres",
          password="postgres")
MYSQL = dict(host="127.0.0.1", port=3307, database="dataflow", username="root",
             password="dataflow")
MONGO = dict(host="127.0.0.1", port=27017, database="dataflow")

SRC_TABLE = "sp_src"
DST_TABLE = "sp_dst"
# File-export destinations must land inside the API workspace root (apps/api).
API_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
EXPORT_DIR = os.path.join(API_ROOT, "exports", "parity")

# One fixture row shape used by every route, so a difference between two routes
# is the route and not the data: identifier, text with unicode, integer,
# decimal, boolean, date, timestamp and a nullable column.
COLUMNS = ["id", "name", "qty", "amount", "active", "hire_date", "updated_at", "note"]


def fixture_rows(n: int, *, offset: int = 0, tag: str = "v1") -> list[dict[str, Any]]:
    out = []
    for i in range(offset + 1, offset + n + 1):
        out.append({
            "id": i,
            "name": f"näme_{i}_{tag}",
            "qty": i % 97,
            "amount": f"{i}.25",
            "active": (i % 2 == 0),
            "hire_date": f"20{10 + i % 15:02d}-0{1 + i % 9}-1{i % 9}",
            "updated_at": f"2026-01-{1 + i % 28:02d} 10:{i % 60:02d}:00",
            "note": None if i % 5 == 0 else f"note {i}",
        })
    return out


# --------------------------------------------------------------------------- engines


def pg_conn():
    import psycopg2

    return psycopg2.connect(host=PG["host"], port=PG["port"], dbname=PG["database"],
                            user=PG["username"], password=PG["password"])


def my_conn():
    import pymysql

    return pymysql.connect(host=MYSQL["host"], port=MYSQL["port"], user=MYSQL["username"],
                           password=MYSQL["password"], database=MYSQL["database"],
                           autocommit=True)


def mongo_db():
    from pymongo import MongoClient

    return MongoClient(f"mongodb://{MONGO['host']}:{MONGO['port']}")[MONGO["database"]]


def sql_exec(engine: str, statements: list[str], *, ignore_errors: bool = True) -> None:
    conn = pg_conn() if engine == "postgresql" else my_conn()
    try:
        cur = conn.cursor()
        for s in statements:
            try:
                cur.execute(s)
            except Exception:
                if engine == "postgresql":
                    conn.rollback()
                if not ignore_errors:
                    raise
        conn.commit()
    finally:
        conn.close()


def sql_rows(engine: str, sql: str) -> list[tuple]:
    conn = pg_conn() if engine == "postgresql" else my_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return list(cur.fetchall())
    finally:
        conn.close()


SRC_DDL = {
    "postgresql": (
        "CREATE TABLE {t} (id BIGINT PRIMARY KEY, name VARCHAR(120), qty INTEGER, "
        "amount NUMERIC(12,2), active BOOLEAN, hire_date DATE, "
        "updated_at TIMESTAMP, note VARCHAR(200))"
    ),
    "mysql": (
        "CREATE TABLE {t} (id BIGINT PRIMARY KEY, name VARCHAR(120), qty INT, "
        "amount DECIMAL(12,2), active TINYINT(1), hire_date DATE, "
        "updated_at DATETIME(6), note VARCHAR(200))"
    ),
}

# Fallback declarations for file sources, where the parser — not a catalog —
# names the shape. Database routes read the product's own probe instead: a
# hand-written table here would let the harness declare a source type Studio
# never sees, and then bill the product for the disagreement it invented.
SRC_TYPES = {
    "postgresql": {"id": "BIGINT", "name": "VARCHAR(120)", "qty": "INTEGER",
                   "amount": "NUMERIC(12,2)", "active": "BOOLEAN", "hire_date": "DATE",
                   "updated_at": "TIMESTAMP", "note": "VARCHAR(200)"},
    "mysql": {"id": "BIGINT", "name": "VARCHAR(120)", "qty": "INT",
              "amount": "DECIMAL(12,2)", "active": "TINYINT(1)", "hire_date": "DATE",
              "updated_at": "DATETIME(6)", "note": "VARCHAR(200)"},
    "mongodb": {"id": "BIGINT", "name": "VARCHAR", "qty": "INTEGER", "amount": "DECIMAL",
                "active": "BOOLEAN", "hire_date": "DATE", "updated_at": "TIMESTAMP",
                "note": "VARCHAR"},
    "csv": {c: "VARCHAR" for c in COLUMNS},
    "excel": {c: "VARCHAR" for c in COLUMNS},
}


_PROBED_SRC_SCHEMA: dict[str, dict[str, str]] = {}


def source_schema(engine: str) -> dict[str, str]:
    """The source shape Studio would carry into Map — the product's own probe.

    Studio seeds Map from ``/transfer/introspect`` on the selected stream, so a
    Mongo field of decimal strings arrives as the observed ``DECIMAL(p,s)``.
    Declaring a bare ``DECIMAL`` here instead made every second scenario read as
    a narrowing schema drift against the destination the first run created.
    """
    if engine in ("csv", "excel"):
        return dict(SRC_TYPES[engine])
    cached = _PROBED_SRC_SCHEMA.get(engine)
    if cached is not None:
        return dict(cached)
    src = endpoint_source(engine)
    body = {
        "source": {
            "kind": src.get("source_kind"),
            "format": src.get("source_format"),
            "host": src.get("source_host", ""),
            "port": src.get("source_port", 0),
            "database": src.get("source_database", ""),
            "username": src.get("source_username", ""),
            "password": src.get("source_password", ""),
            "table": src.get("source_table", ""),
            "collection": src.get("source_collection", ""),
        },
        "destination": {"kind": "file_export", "format": "json"},
    }
    r = requests.post(f"{API}/transfer/introspect", json=body, timeout=300)
    r.raise_for_status()
    probed = r.json().get("source", {}).get("schema") or {}
    schema = {c: str(probed[c]) for c in COLUMNS if probed.get(c)}
    missing = [c for c in COLUMNS if c not in schema]
    if missing:
        raise RuntimeError(
            f"{engine} source probe returned no type for {missing} — "
            "the harness will not invent one"
        )
    _PROBED_SRC_SCHEMA[engine] = schema
    return dict(schema)


def seed_sql_source(engine: str, rows: list[dict[str, Any]]) -> None:
    _PROBED_SRC_SCHEMA.pop(engine, None)
    sql_exec(engine, [f"DROP TABLE IF EXISTS {SRC_TABLE}",
                      SRC_DDL[engine].format(t=SRC_TABLE)], ignore_errors=False)
    conn = pg_conn() if engine == "postgresql" else my_conn()
    ph = "%s"
    try:
        cur = conn.cursor()
        cur.executemany(
            f"INSERT INTO {SRC_TABLE} ({', '.join(COLUMNS)}) "
            f"VALUES ({', '.join([ph] * len(COLUMNS))})",
            [tuple(r[c] for c in COLUMNS) for r in rows],
        )
        conn.commit()
    finally:
        conn.close()


def append_sql_source(engine: str, rows: list[dict[str, Any]]) -> None:
    _PROBED_SRC_SCHEMA.pop(engine, None)
    conn = pg_conn() if engine == "postgresql" else my_conn()
    try:
        cur = conn.cursor()
        cur.executemany(
            f"INSERT INTO {SRC_TABLE} ({', '.join(COLUMNS)}) "
            f"VALUES ({', '.join(['%s'] * len(COLUMNS))})",
            [tuple(r[c] for c in COLUMNS) for r in rows],
        )
        conn.commit()
    finally:
        conn.close()


def seed_mongo_source(rows: list[dict[str, Any]]) -> None:
    _PROBED_SRC_SCHEMA.pop("mongodb", None)
    db = mongo_db()
    db[SRC_TABLE].drop()
    if rows:
        db[SRC_TABLE].insert_many([dict(r) for r in rows])


def write_csv(path: str, rows: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS)
    w.writeheader()
    for r in rows:
        w.writerow({c: ("" if r[c] is None else r[c]) for c in COLUMNS})
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(buf.getvalue())
    return path


def write_excel(path: str, rows: list[dict[str, Any]]) -> str:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.append(COLUMNS)
    for r in rows:
        ws.append([("" if r[c] is None else str(r[c])) for c in COLUMNS])
    wb.save(path)
    return path


# --------------------------------------------------------------------------- dest state


def clear_route_cursors() -> None:
    """Clear this fixture route's watermarks — the operator's state reset.

    Resetting a destination without resetting its cursor is a real hazard the
    product now refuses, so the harness performs the documented recovery
    (``/ops/cdc-cursors/clear``) rather than leaving a watermark that claims
    history the destination no longer holds.
    """
    keys = requests.get(f"{API}/ops/cdc-cursors/keys", timeout=30).json()
    for key in keys.get("cursor_keys") or []:
        if SRC_TABLE in key or DST_TABLE in key:
            requests.post(
                f"{API}/ops/cdc-cursors/clear",
                json={"cursor_key": key, "reason": "parity matrix destination reset"},
                timeout=30,
            )


def dest_reset(engine: str, *, create_with: str = "") -> None:
    """Drop the destination object; optionally recreate it empty (dest-exists)."""
    clear_route_cursors()
    if engine in ("postgresql", "mysql"):
        sql_exec(engine, [f"DROP TABLE IF EXISTS {DST_TABLE}"])
        if create_with:
            sql_exec(engine, [create_with.format(t=DST_TABLE)], ignore_errors=False)
    elif engine == "mongodb":
        mongo_db()[DST_TABLE].drop()
    elif engine == "file_export":
        os.makedirs(EXPORT_DIR, exist_ok=True)
        for f in os.listdir(EXPORT_DIR):
            os.remove(os.path.join(EXPORT_DIR, f))


def dest_count(engine: str, *, export_path: str = "") -> int:
    try:
        if engine in ("postgresql", "mysql"):
            return int(sql_rows(engine, f"SELECT COUNT(*) FROM {DST_TABLE}")[0][0])
        if engine == "mongodb":
            return int(mongo_db()[DST_TABLE].count_documents({}))
        if engine == "file_export":
            if not export_path or not os.path.exists(export_path):
                return -1
            with open(export_path, encoding="utf-8") as fh:
                return max(0, sum(1 for _ in fh) - 1)
    except Exception:
        return -1
    return -1


def dest_active_count(engine: str) -> int:
    """Mirror keeps rows physically; the active population excludes soft deletes."""
    try:
        if engine in ("postgresql", "mysql"):
            return int(sql_rows(
                engine,
                f"SELECT COUNT(*) FROM {DST_TABLE} WHERE _deleted IS NULL OR _deleted = "
                + ("FALSE" if engine == "postgresql" else "0"),
            )[0][0])
        if engine == "mongodb":
            return int(mongo_db()[DST_TABLE].count_documents(
                {"$or": [{"_deleted": {"$exists": False}}, {"_deleted": False}]}))
    except Exception:
        return -1
    return -1


def dest_value(engine: str, key: int, column: str) -> Any:
    try:
        if engine in ("postgresql", "mysql"):
            rows = sql_rows(engine, f"SELECT {column} FROM {DST_TABLE} WHERE id = {key}")
            return rows[0][0] if rows else None
        if engine == "mongodb":
            doc = mongo_db()[DST_TABLE].find_one({"id": key})
            return doc.get(column) if doc else None
    except Exception:
        return None
    return None


def dest_key_count(engine: str, key: int) -> int:
    try:
        if engine in ("postgresql", "mysql"):
            return int(sql_rows(engine,
                                f"SELECT COUNT(*) FROM {DST_TABLE} WHERE id = {key}")[0][0])
        if engine == "mongodb":
            return int(mongo_db()[DST_TABLE].count_documents({"id": key}))
    except Exception:
        return -1
    return -1


# --------------------------------------------------------------------------- product path


def endpoint_source(engine: str) -> dict[str, Any]:
    if engine == "postgresql":
        return dict(source_kind="database", source_format="postgresql",
                    source_host=PG["host"], source_port=PG["port"],
                    source_database=PG["database"], source_username=PG["username"],
                    source_password=PG["password"], source_table=SRC_TABLE)
    if engine == "mysql":
        return dict(source_kind="database", source_format="mysql",
                    source_host=MYSQL["host"], source_port=MYSQL["port"],
                    source_database=MYSQL["database"], source_username=MYSQL["username"],
                    source_password=MYSQL["password"], source_table=SRC_TABLE)
    if engine == "mongodb":
        return dict(source_kind="database", source_format="mongodb",
                    source_host=MONGO["host"], source_port=MONGO["port"],
                    source_database=MONGO["database"], source_collection=SRC_TABLE)
    return dict(source_kind="file", source_format=engine)


def endpoint_dest(engine: str, *, export_path: str = "") -> dict[str, Any]:
    if engine == "postgresql":
        return dict(dest_kind="database", dest_format="postgresql", dest_host=PG["host"],
                    dest_port=PG["port"], dest_database=PG["database"],
                    dest_username=PG["username"], dest_password=PG["password"],
                    dest_table=DST_TABLE)
    if engine == "mysql":
        return dict(dest_kind="database", dest_format="mysql", dest_host=MYSQL["host"],
                    dest_port=MYSQL["port"], dest_database=MYSQL["database"],
                    dest_username=MYSQL["username"], dest_password=MYSQL["password"],
                    dest_table=DST_TABLE)
    if engine == "mongodb":
        return dict(dest_kind="database", dest_format="mongodb", dest_host=MONGO["host"],
                    dest_port=MONGO["port"], dest_database=MONGO["database"],
                    dest_collection=DST_TABLE)
    if engine == "file_export":
        return dict(dest_kind="file_export", dest_format="csv",
                    dest_output_path=export_path or f"{EXPORT_DIR}/{DST_TABLE}.csv")
    raise ValueError(engine)


def preflight_dest_fields(engine: str, *, export_path: str = "") -> dict[str, Any]:
    d = endpoint_dest(engine, export_path=export_path)
    return {
        "dest_kind": d.get("dest_kind"),
        "dest_type": d.get("dest_format"),
        "dest_host": d.get("dest_host", ""),
        "dest_port": d.get("dest_port", 0),
        "dest_database": d.get("dest_database", ""),
        "dest_username": d.get("dest_username", ""),
        "dest_password": d.get("dest_password", ""),
        "dest_table": d.get("dest_table", ""),
        "dest_collection": d.get("dest_collection", ""),
    }


def preflight_source_fields(engine: str) -> dict[str, Any]:
    s = endpoint_source(engine)
    cfg = {
        "kind": s.get("source_kind"),
        "format": s.get("source_format"),
        "host": s.get("source_host", ""),
        "port": s.get("source_port", 0),
        "database": s.get("source_database", ""),
        "username": s.get("source_username", ""),
        "password": s.get("source_password", ""),
        "table": s.get("source_table", ""),
        "collection": s.get("source_collection", ""),
    }
    return {
        "source_config": cfg,
        "source_kind": s.get("source_kind"),
        "source_type": s.get("source_format"),
        "source_table": s.get("source_table", ""),
        "source_collection": s.get("source_collection", ""),
    }


def studio_map(src_engine: str, dest_engine: str, *, rows: list[dict[str, Any]],
               dest_exists: bool, target_columns: list[str],
               sync_mode: str,
               target_schema: dict[str, str] | None = None) -> list[dict[str, Any]]:
    body = {
        "source_columns": COLUMNS,
        "source_schema": source_schema(src_engine),
        "target_columns": target_columns,
        "target_schema": dict(target_schema or {}),
        "source_samples": {c: [str(r[c]) for r in rows[:8]] for c in COLUMNS},
        "validation_mode": "balanced",
        "use_llm": False,
        "destination_db_type": dest_engine if dest_engine != "file_export" else "csv",
        "source_db_type": src_engine,
        "source_kind": "file" if src_engine in ("csv", "excel") else "database",
        "file_format": src_engine if src_engine in ("csv", "excel") else "",
        "sync_mode": sync_mode,
        "destination_table_exists": dest_exists,
        "schema_policy": "manual_review",
    }
    r = requests.post(f"{API}/transfer/map", json=body, timeout=180)
    r.raise_for_status()
    mappings = r.json()["mappings"]
    if src_engine == "mongodb":
        # The collection carries an implicit ``_id`` the fixture never declared.
        # Studio makes the operator answer for it; the harness answers the same
        # way it would on screen — an explicit Omit, never a silent drop.
        mappings.append({"source": "_id", "target": "", "intentional_omit": True,
                         "transform": "omit", "source_type": "OBJECTID"})
    return mappings


def studio_validate(src_engine: str, dest_engine: str, *, mappings: list[dict[str, Any]],
                    rows: list[dict[str, Any]], sync_mode: str,
                    stream_contracts: list[dict[str, Any]] | None,
                    export_path: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "columns": COLUMNS,
        "column_types": source_schema(src_engine),
        "row_count": len(rows),
        "mappings": mappings,
        "sample_rows": [{c: r[c] for c in COLUMNS} for r in rows[:50]],
        "sync_mode": sync_mode,
        "schema_policy": "manual_review",
        "validation_mode": "balanced",
        "stream_contracts": stream_contracts or [],
    }
    payload.update(preflight_source_fields(src_engine))
    payload.update(preflight_dest_fields(dest_engine, export_path=export_path))
    r = requests.post(f"{API}/preflight/run", json=payload, timeout=600)
    r.raise_for_status()
    return r.json()


def studio_run(src_engine: str, dest_engine: str, *, mappings: list[dict[str, Any]],
               sync_mode: str, stream_contracts: list[dict[str, Any]] | None,
               source_file: str = "", export_path: str = "",
               timeout_s: int = 900) -> dict[str, Any]:
    form: dict[str, Any] = {
        "mappings_json": json.dumps(mappings),
        "sync_mode": sync_mode,
        "schema_policy": "manual_review",
        "validation_mode": "balanced",
        "skip_preflight": "false",
        "async_mode": "false",
    }
    form.update({k: str(v) for k, v in endpoint_source(src_engine).items()})
    form.update({k: str(v) for k, v in endpoint_dest(dest_engine,
                                                     export_path=export_path).items()})
    if stream_contracts:
        form["stream_contracts_json"] = json.dumps(stream_contracts)
    files = None
    if source_file:
        files = {"file": (os.path.basename(source_file), open(source_file, "rb"))}
    headers = {"Idempotency-Key": uuid.uuid4().hex}
    r = requests.post(f"{API}/transfer/run", data=form, files=files, headers=headers,
                      timeout=timeout_s)
    if files:
        files["file"][1].close()
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text[:400]}
    body["_http_status"] = r.status_code
    return body


def run_verdict(body: dict[str, Any]) -> tuple[bool, str]:
    if body.get("_http_status") not in (200, 201):
        return False, str(body.get("detail") or body.get("raw") or "")[:300]
    result = body.get("result") if isinstance(body.get("result"), dict) else body
    ok = bool(result.get("success", body.get("success")))
    err = str(result.get("error") or body.get("error") or "")[:300]
    return ok, err


def validate_cleared(pf: dict[str, Any]) -> bool:
    """Did Validate clear the run? Studio unlocks Execute on preflight `passed`."""
    return bool(pf.get("passed"))


def merge_validate_stamps(mappings: list[dict[str, Any]],
                          pf: dict[str, Any]) -> list[dict[str, Any]]:
    """Carry Validate's Kernel stamps + signed contracts onto Map, as Studio does.

    Mirrors `mergeStampedTargetTypes` / `mergeSignedRiskContracts` in TransferPage:
    Execute must see the hydrated mappings Validate evaluated, not Map drafts.
    """
    out = [dict(m) for m in mappings]
    by_key = {(str(m.get("source") or ""), str(m.get("target") or "")): m for m in out}
    for stamped in pf.get("stamped_mappings") or []:
        m = by_key.get((str(stamped.get("source") or ""), str(stamped.get("target") or "")))
        if m is None:
            continue
        if stamped.get("target_type"):
            m["target_type"] = stamped["target_type"]
        if "create_new" in stamped:
            m["create_new"] = stamped["create_new"]
    for signed in pf.get("signed_mappings") or []:
        m = by_key.get((str(signed.get("source") or ""), str(signed.get("target") or "")))
        if m is None:
            continue
        if signed.get("risk_contract"):
            m["risk_contract"] = signed["risk_contract"]
        if "risk_acknowledged" in signed:
            m["risk_acknowledged"] = signed["risk_acknowledged"]
    return out


def validate_reason(pf: dict[str, Any]) -> str:
    gates = pf.get("gates") or []
    failed = [g for g in gates if str(g.get("status", "")).lower() in ("fail", "failed",
                                                                      "block", "blocked",
                                                                      "error")]
    if failed:
        parts = []
        for g in failed[:3]:
            # The gate message names the class; the issues name the cause. A
            # record that keeps only "contract incomplete" cannot be acted on.
            issues = (g.get("details") or {}).get("issues") or []
            detail = " | ".join(str(i) for i in issues[:3])
            head = f"{g.get('id') or g.get('gate')}:{g.get('message', '')}"
            parts.append(f"{head} [{detail}]"[:400] if detail else head[:160])
        return "; ".join(parts)
    return str(pf.get("summary") or pf.get("message") or "")[:200]


# --------------------------------------------------------------------------- scenarios


@dataclass
class Scenario:
    route: str
    mode: str
    contract: str
    fn: Callable[[], dict[str, Any]]


@dataclass
class Measurement:
    route: str
    mode: str
    contract: str
    validate_cleared: bool = False
    validate_reason: str = ""
    run_ok: bool = False
    run_error: str = ""
    dest_rows: int = -1
    detail: dict[str, Any] = field(default_factory=dict)
    verdict: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return {
            "route": self.route,
            "sync_mode": self.mode,
            "declared_contract": self.contract,
            "validate_cleared": self.validate_cleared,
            "validate_reason": self.validate_reason,
            "run_ok": self.run_ok,
            "run_error": self.run_error,
            "dest_rows": self.dest_rows,
            "detail": self.detail,
            "verdict": self.verdict,
        }


SOURCE_FILE_CSV = "/tmp/df_parity_src.csv"
SOURCE_FILE_XLSX = "/tmp/df_parity_src.xlsx"


def prepare_source(src: str, rows: list[dict[str, Any]]) -> str:
    """Seed the source engine and return the upload path for file sources."""
    if src in ("postgresql", "mysql"):
        seed_sql_source(src, rows)
        return ""
    if src == "mongodb":
        seed_mongo_source(rows)
        return ""
    if src == "csv":
        return write_csv(SOURCE_FILE_CSV, rows)
    if src == "excel":
        return write_excel(SOURCE_FILE_XLSX, rows)
    raise ValueError(src)


def extend_source(src: str, rows: list[dict[str, Any]], all_rows: list[dict[str, Any]]) -> str:
    """Add rows to an already-seeded source (file sources are rewritten whole)."""
    if src in ("postgresql", "mysql"):
        append_sql_source(src, rows)
        return ""
    if src == "mongodb":
        _PROBED_SRC_SCHEMA.pop("mongodb", None)
        mongo_db()[SRC_TABLE].insert_many([dict(r) for r in rows])
        return ""
    if src == "csv":
        return write_csv(SOURCE_FILE_CSV, all_rows)
    if src == "excel":
        return write_excel(SOURCE_FILE_XLSX, all_rows)
    raise ValueError(src)


#: A modification timestamp later than every fixture row. A source that updates
#: a row without moving its modification cursor is invisible to any
#: cursor-driven read (ours, Fivetran's, Airbyte's), so the fixture moves it.
MUTATION_TS = "2026-03-01 12:00:00"


def mutate_source(src: str, keys: list[int], all_rows: list[dict[str, Any]]) -> str:
    """Update `name` for the given ids in the source; return upload path for files."""
    for r in all_rows:
        if r["id"] in keys:
            r["name"] = f"updated_{r['id']}"
            r["updated_at"] = MUTATION_TS
    if src in ("postgresql", "mysql"):
        sql_exec(src, [
            f"UPDATE {SRC_TABLE} SET name = 'updated_{k}', "
            f"updated_at = '{MUTATION_TS}' WHERE id = {k}"
            for k in keys
        ], ignore_errors=False)
        return ""
    if src == "mongodb":
        for k in keys:
            mongo_db()[SRC_TABLE].update_one(
                {"id": k},
                {"$set": {"name": f"updated_{k}", "updated_at": MUTATION_TS}},
            )
        return ""
    if src == "csv":
        return write_csv(SOURCE_FILE_CSV, all_rows)
    if src == "excel":
        return write_excel(SOURCE_FILE_XLSX, all_rows)
    raise ValueError(src)


def delete_source(src: str, keys: list[int], all_rows: list[dict[str, Any]]) -> str:
    remaining = [r for r in all_rows if r["id"] not in keys]
    all_rows[:] = remaining
    if src in ("postgresql", "mysql"):
        sql_exec(src, [f"DELETE FROM {SRC_TABLE} WHERE id IN ({','.join(map(str, keys))})"],
                 ignore_errors=False)
        return ""
    if src == "mongodb":
        mongo_db()[SRC_TABLE].delete_many({"id": {"$in": keys}})
        return ""
    if src == "csv":
        return write_csv(SOURCE_FILE_CSV, remaining)
    if src == "excel":
        return write_excel(SOURCE_FILE_XLSX, remaining)
    raise ValueError(src)


def endpoint_object(engine: str, *, export_path: str = "") -> dict[str, Any]:
    """Endpoint dict in the shape ``/transfer/introspect`` takes (Studio's probe)."""
    if engine == "postgresql":
        return dict(kind="database", format="postgresql", host=PG["host"], port=PG["port"],
                    database=PG["database"], username=PG["username"],
                    password=PG["password"], table=DST_TABLE)
    if engine == "mysql":
        return dict(kind="database", format="mysql", host=MYSQL["host"], port=MYSQL["port"],
                    database=MYSQL["database"], username=MYSQL["username"],
                    password=MYSQL["password"], table=DST_TABLE)
    if engine == "mongodb":
        return dict(kind="database", format="mongodb", host=MONGO["host"],
                    port=MONGO["port"], database=MONGO["database"], collection=DST_TABLE)
    if engine == "file_export":
        return dict(kind="file_export", format="csv",
                    output_path=export_path or f"{EXPORT_DIR}/{DST_TABLE}.csv")
    raise ValueError(engine)


def dest_probe(dst: str, *, export_path: str = "") -> tuple[bool, dict[str, str]]:
    """Studio's destination probe: ``POST /transfer/introspect``, not local SQL.

    Studio maps against the types this endpoint returns — including MongoDB,
    whose carriers come from sampled documents and never from an
    ``information_schema`` the harness could query itself. Reading the
    destination any other way measures the harness, not the product: a
    name-only "it exists" makes Map refuse to invent types (correctly) and the
    route records a confidence block that Studio would never produce.

    A file export is not a bindable destination — Studio always maps it as
    create-new (``destination_table_exists: false`` in TransferPage), because
    the writer serializes a new file rather than binding an existing DDL.
    """
    if dst == "file_export":
        return False, {}
    r = requests.post(
        f"{API}/transfer/introspect",
        json={"source": {"kind": "file", "format": "csv"},
              "destination": endpoint_object(dst, export_path=export_path)},
        timeout=180,
    )
    r.raise_for_status()
    d = r.json().get("destination") or {}
    schema = {str(k): str(v) for k, v in (d.get("schema") or {}).items()}
    exists = bool(d.get("table_exists")) or bool(schema)
    return exists, schema


def sign_required_contracts(mappings: list[dict[str, Any]]) -> list[str]:
    """Answer Map's contract demands the way the operator does on screen.

    A declared-lossy pair (Mongo ``DECIMAL`` Decimal128 into ``NUMERIC(12,2)``)
    is a real risk, so Map is right to demand a Migration Risk Contract. The
    harness signs it with ``CAST_AND_CONTINUE`` — a value that will not fit is
    held out to the DLQ, never truncated in silence — and records which columns
    needed one, so the evidence keeps the demand visible instead of hiding it
    behind a green route. The draft is signed server-side by
    ``hydrate_risk_contract_dict``; the harness cannot forge a signature.
    """
    signed: list[str] = []
    for m in mappings:
        if not m.get("requires_risk_contract") or m.get("risk_contract"):
            continue
        col = str(m.get("source") or "")
        m["execution_policy"] = "CAST_AND_CONTINUE"
        m["risk_contract"] = {
            "column": col,
            "source_type": str(m.get("source_type") or ""),
            "destination_type": str(m.get("target_type") or ""),
            "approved_by": "matrix-operator@dataflow.app",
            "reason": (
                "Route matrix: declared-lossy carrier accepted; non-fitting "
                "values are held out to the DLQ, not truncated."
            ),
            "execution_policy": "CAST_AND_CONTINUE",
            "loss_classification": str(m.get("fidelity") or ""),
        }
        signed.append(col)
    return signed


def one_pass(src: str, dst: str, *, rows: list[dict[str, Any]], sync_mode: str,
             dest_exists: bool, contracts: list[dict[str, Any]] | None,
             source_file: str, export_path: str) -> dict[str, Any]:
    """Map → Validate → Run, exactly as Studio drives it."""
    schema: dict[str, str] = {}
    if dest_exists:
        probed_exists, schema = dest_probe(dst, export_path=export_path)
        dest_exists = probed_exists
    target_columns = list(schema)
    mappings = studio_map(src, dst, rows=rows, dest_exists=dest_exists,
                          target_columns=target_columns, sync_mode=sync_mode,
                          target_schema=schema)
    contracted = sign_required_contracts(mappings)
    pf = studio_validate(src, dst, mappings=mappings, rows=rows, sync_mode=sync_mode,
                         stream_contracts=contracts, export_path=export_path)
    cleared = validate_cleared(pf)
    mappings = merge_validate_stamps(mappings, pf)
    body = studio_run(src, dst, mappings=mappings, sync_mode=sync_mode,
                      stream_contracts=contracts, source_file=source_file,
                      export_path=export_path)
    ok, err = run_verdict(body)
    return {
        "validate_cleared": cleared,
        "validate_reason": validate_reason(pf),
        "run_ok": ok,
        "run_error": err,
        "mappings": mappings,
        "signed_contracts": contracted,
    }


def record_setup_pass(m: "Measurement", label: str, r: dict[str, Any]) -> None:
    """A scenario that never got its first load cannot judge the second one."""
    if r["run_ok"]:
        return
    m.detail[label] = {
        "validate_cleared": r["validate_cleared"],
        "validate_reason": r["validate_reason"],
        "run_error": r["run_error"],
    }


def judge(m: Measurement, *, expected_rows: int | None, extra_ok: bool = True) -> None:
    """Classify a measurement against what the sync mode promised."""
    if any(k.startswith("setup_") for k in m.detail):
        m.verdict = "setup_failed"
        return
    if m.validate_cleared and not m.run_ok:
        m.verdict = "parity_break"
        return
    if not m.validate_cleared and not m.run_ok:
        m.verdict = "blocked_consistently"
        return
    if not m.validate_cleared and m.run_ok:
        m.verdict = "false_block"
        return
    if expected_rows is not None and m.dest_rows != expected_rows:
        m.verdict = "contract_break"
        return
    m.verdict = "pass" if extra_ok else "contract_break"


def dest_ddl_for(dst: str) -> str:
    if dst == "postgresql":
        return SRC_DDL["postgresql"]
    if dst == "mysql":
        return SRC_DDL["mysql"]
    return ""


# --------------------------------------------------------------------------- cases

N = 200


def case_create_new_append(src: str, dst: str) -> Measurement:
    m = Measurement(f"{src}->{dst}", "full_refresh_append",
                    f"destination absent: creates it and lands {N} rows")
    rows = fixture_rows(N)
    f = prepare_source(src, rows)
    export = f"{EXPORT_DIR}/{DST_TABLE}.csv"
    dest_reset(dst)
    r = one_pass(src, dst, rows=rows, sync_mode="full_refresh_append", dest_exists=False,
                 contracts=None, source_file=f, export_path=export)
    m.validate_cleared, m.validate_reason = r["validate_cleared"], r["validate_reason"]
    m.run_ok, m.run_error = r["run_ok"], r["run_error"]
    m.dest_rows = dest_count(dst, export_path=export)
    judge(m, expected_rows=N)
    return m


def case_append_nonempty(src: str, dst: str) -> Measurement:
    m = Measurement(f"{src}->{dst}", "full_refresh_append(2nd run)",
                    f"destination already holds {N}: append leaves {2 * N}")
    rows = fixture_rows(N)
    f = prepare_source(src, rows)
    export = f"{EXPORT_DIR}/{DST_TABLE}.csv"
    dest_reset(dst)
    record_setup_pass(m, "setup_first_load", one_pass(
        src, dst, rows=rows, sync_mode="full_refresh_append", dest_exists=False,
        contracts=None, source_file=f, export_path=export))
    first = dest_count(dst, export_path=export)
    r = one_pass(src, dst, rows=rows, sync_mode="full_refresh_append", dest_exists=True,
                 contracts=None, source_file=f, export_path=export)
    m.validate_cleared, m.validate_reason = r["validate_cleared"], r["validate_reason"]
    m.run_ok, m.run_error = r["run_ok"], r["run_error"]
    m.dest_rows = dest_count(dst, export_path=export)
    m.detail["rows_after_first_run"] = first
    judge(m, expected_rows=2 * N)
    return m


def case_overwrite(src: str, dst: str) -> Measurement:
    m = Measurement(f"{src}->{dst}", "full_refresh_overwrite(2nd run)",
                    f"destination already holds {N}: overwrite leaves exactly {N}")
    rows = fixture_rows(N)
    f = prepare_source(src, rows)
    export = f"{EXPORT_DIR}/{DST_TABLE}.csv"
    dest_reset(dst)
    record_setup_pass(m, "setup_first_load", one_pass(
        src, dst, rows=rows, sync_mode="full_refresh_append", dest_exists=False,
        contracts=None, source_file=f, export_path=export))
    first = dest_count(dst, export_path=export)
    r = one_pass(src, dst, rows=rows, sync_mode="full_refresh_overwrite", dest_exists=True,
                 contracts=None, source_file=f, export_path=export)
    m.validate_cleared, m.validate_reason = r["validate_cleared"], r["validate_reason"]
    m.run_ok, m.run_error = r["run_ok"], r["run_error"]
    m.dest_rows = dest_count(dst, export_path=export)
    m.detail["rows_after_first_run"] = first
    judge(m, expected_rows=N)
    return m


def case_incremental_append(src: str, dst: str) -> Measurement:
    add = 50
    m = Measurement(f"{src}->{dst}", "incremental_append",
                    f"cursor read: second run adds only the {add} new rows")
    rows = fixture_rows(N)
    f = prepare_source(src, rows)
    export = f"{EXPORT_DIR}/{DST_TABLE}.csv"
    dest_reset(dst)
    contracts = [{"name": SRC_TABLE, "selected": True, "sync_mode": "incremental_append",
                  "cursor_field": "id", "primary_key": "id",
                  "cursor_semantics": "monotonic_sequence"}]
    record_setup_pass(m, "setup_first_load", one_pass(
        src, dst, rows=rows, sync_mode="incremental_append", dest_exists=False,
        contracts=contracts, source_file=f, export_path=export))
    first = dest_count(dst, export_path=export)
    new_rows = fixture_rows(add, offset=N, tag="v2")
    rows.extend(new_rows)
    f = extend_source(src, new_rows, rows)
    r = one_pass(src, dst, rows=rows, sync_mode="incremental_append", dest_exists=True,
                 contracts=contracts, source_file=f, export_path=export)
    m.validate_cleared, m.validate_reason = r["validate_cleared"], r["validate_reason"]
    m.run_ok, m.run_error = r["run_ok"], r["run_error"]
    m.dest_rows = dest_count(dst, export_path=export)
    m.detail["rows_after_first_run"] = first
    judge(m, expected_rows=N + add)
    return m


def case_incremental_deduped(src: str, dst: str) -> Measurement:
    keys = [3, 7, 11]
    m = Measurement(f"{src}->{dst}", "incremental_deduped",
                    f"upsert on id: {N} rows stay {N} and updated values land")
    rows = fixture_rows(N)
    f = prepare_source(src, rows)
    export = f"{EXPORT_DIR}/{DST_TABLE}.csv"
    dest_reset(dst)
    contracts = [{"name": SRC_TABLE, "selected": True, "sync_mode": "incremental_deduped",
                  "cursor_field": "updated_at", "primary_key": "id",
                  "cursor_semantics": "modification_timestamp"}]
    record_setup_pass(m, "setup_first_load", one_pass(
        src, dst, rows=rows, sync_mode="incremental_deduped", dest_exists=False,
        contracts=contracts, source_file=f, export_path=export))
    first = dest_count(dst, export_path=export)
    f = mutate_source(src, keys, rows)
    for r0 in rows:
        if r0["id"] in keys:
            r0["updated_at"] = "2026-12-31 23:59:00"
    if src in ("postgresql", "mysql"):
        sql_exec(src, [f"UPDATE {SRC_TABLE} SET updated_at = '2026-12-31 23:59:00' "
                       f"WHERE id IN ({','.join(map(str, keys))})"], ignore_errors=False)
    elif src == "mongodb":
        mongo_db()[SRC_TABLE].update_many({"id": {"$in": keys}},
                                          {"$set": {"updated_at": "2026-12-31 23:59:00"}})
    else:
        f = write_csv(SOURCE_FILE_CSV, rows) if src == "csv" else write_excel(
            SOURCE_FILE_XLSX, rows)
    r = one_pass(src, dst, rows=rows, sync_mode="incremental_deduped", dest_exists=True,
                 contracts=contracts, source_file=f, export_path=export)
    m.validate_cleared, m.validate_reason = r["validate_cleared"], r["validate_reason"]
    m.run_ok, m.run_error = r["run_ok"], r["run_error"]
    m.dest_rows = dest_count(dst, export_path=export)
    m.detail["rows_after_first_run"] = first
    updated = dest_value(dst, keys[0], "name")
    dupes = dest_key_count(dst, keys[0])
    m.detail["updated_value"] = str(updated)
    m.detail["rows_for_key"] = dupes
    judge(m, expected_rows=N,
          extra_ok=(str(updated) == f"updated_{keys[0]}" and dupes == 1))
    return m


def case_mirror(src: str, dst: str) -> Measurement:
    gone = [5, 6]
    m = Measurement(f"{src}->{dst}", "mirror",
                    "source deletions become soft deletes at the destination")
    rows = fixture_rows(N)
    f = prepare_source(src, rows)
    export = f"{EXPORT_DIR}/{DST_TABLE}.csv"
    dest_reset(dst)
    contracts = [{"name": SRC_TABLE, "selected": True, "sync_mode": "mirror",
                  "primary_key": "id", "cursor_field": "updated_at"}]
    record_setup_pass(m, "setup_first_load", one_pass(
        src, dst, rows=rows, sync_mode="mirror", dest_exists=False,
        contracts=contracts, source_file=f, export_path=export))
    first = dest_count(dst, export_path=export)
    f = delete_source(src, gone, rows)
    r = one_pass(src, dst, rows=rows, sync_mode="mirror", dest_exists=True,
                 contracts=contracts, source_file=f, export_path=export)
    m.validate_cleared, m.validate_reason = r["validate_cleared"], r["validate_reason"]
    m.run_ok, m.run_error = r["run_ok"], r["run_error"]
    m.dest_rows = dest_count(dst, export_path=export)
    m.detail["rows_after_first_run"] = first
    active = dest_active_count(dst)
    m.detail["active_rows"] = active
    judge(m, expected_rows=None, extra_ok=(active == N - len(gone)))
    if m.verdict == "pass" and active != N - len(gone):
        m.verdict = "contract_break"
    return m


def case_scd2(src: str, dst: str) -> Measurement:
    keys = [4]
    m = Measurement(f"{src}->{dst}", "scd2",
                    "an updated row is versioned, not overwritten")
    rows = fixture_rows(N)
    f = prepare_source(src, rows)
    export = f"{EXPORT_DIR}/{DST_TABLE}.csv"
    dest_reset(dst)
    contracts = [{"name": SRC_TABLE, "selected": True, "sync_mode": "scd2",
                  "primary_key": "id", "cursor_field": "updated_at",
                  "cursor_semantics": "modification_timestamp"}]
    record_setup_pass(m, "setup_first_load", one_pass(
        src, dst, rows=rows, sync_mode="scd2", dest_exists=False,
        contracts=contracts, source_file=f, export_path=export))
    first = dest_count(dst, export_path=export)
    f = mutate_source(src, keys, rows)
    r = one_pass(src, dst, rows=rows, sync_mode="scd2", dest_exists=True,
                 contracts=contracts, source_file=f, export_path=export)
    m.validate_cleared, m.validate_reason = r["validate_cleared"], r["validate_reason"]
    m.run_ok, m.run_error = r["run_ok"], r["run_error"]
    m.dest_rows = dest_count(dst, export_path=export)
    m.detail["rows_after_first_run"] = first
    versions = dest_key_count(dst, keys[0])
    m.detail["versions_for_updated_key"] = versions
    judge(m, expected_rows=None, extra_ok=(versions >= 2))
    if m.verdict == "pass" and versions < 2:
        m.verdict = "contract_break"
    return m


CASES: dict[str, Callable[[str, str], Measurement]] = {
    "create_new_append": case_create_new_append,
    "append_nonempty": case_append_nonempty,
    "overwrite": case_overwrite,
    "incremental_append": case_incremental_append,
    "incremental_deduped": case_incremental_deduped,
    "mirror": case_mirror,
    "scd2": case_scd2,
}

SQL_ONLY = {"mirror", "scd2", "incremental_deduped"}

ROUTES = [
    ("postgresql", "mysql"),
    ("mysql", "postgresql"),
    ("postgresql", "mongodb"),
    ("mongodb", "postgresql"),
    ("mysql", "mongodb"),
    ("csv", "postgresql"),
    ("csv", "mysql"),
    ("excel", "postgresql"),
    ("mongodb", "file_export"),
    ("postgresql", "file_export"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--routes", default="")
    ap.add_argument("--cases", default="")
    ap.add_argument("--out", default="/home/ubuntu/parity_matrix.json")
    args = ap.parse_args()

    routes = ROUTES
    if args.routes:
        want = {r.strip() for r in args.routes.split(",") if r.strip()}
        routes = [r for r in ROUTES if f"{r[0]}->{r[1]}" in want]
    case_names = list(CASES)
    if args.cases:
        case_names = [c.strip() for c in args.cases.split(",") if c.strip() in CASES]

    os.makedirs(EXPORT_DIR, exist_ok=True)
    results: list[dict[str, Any]] = []
    for src, dst in routes:
        for name in case_names:
            if name in SQL_ONLY and dst in ("mongodb", "file_export") and name != \
                    "incremental_deduped":
                continue
            if name in ("mirror", "scd2", "incremental_deduped") and dst == "file_export":
                continue
            started = time.time()
            try:
                m = CASES[name](src, dst)
            except Exception as exc:  # harness failure is itself a finding
                m = Measurement(f"{src}->{dst}", name, "harness could not run the case")
                m.verdict = "harness_error"
                m.run_error = f"{type(exc).__name__}: {exc}"[:400]
            row = m.to_dict()
            row["case"] = name
            row["seconds"] = round(time.time() - started, 1)
            results.append(row)
            print(json.dumps(row), flush=True)

    summary: dict[str, int] = {}
    for r in results:
        summary[r["verdict"]] = summary.get(r["verdict"], 0) + 1
    payload = {"summary": summary, "results": results}
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
