"""Live migration scenario matrix: what actually happens per data scenario.

Each scenario builds a real source table in Postgres, a real destination table
in the target engine, runs the *product* path (UniversalTransferEngine
execute_tracked, which runs preflight gates + write + Gate-8), and records the
measured verdict plus the destination state afterwards.

Nothing here asserts; it reports. A scenario whose measured verdict differs
from the intended contract is a product gap, and the document that consumes
this artifact must say so rather than round it to green.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from typing import Any, Callable

sys.path.insert(0, "/home/ubuntu/repos/DataFlow/apps/api")
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")

import psycopg2  # noqa: E402

PG = dict(host="localhost", port=5433, dbname="dataflow", user="postgres", password="postgres")

DESTS: dict[str, dict[str, Any]] = {
    "postgresql": dict(kind="database", format="postgresql", host="localhost", port=5433,
                       database="dataflow", username="postgres", password="postgres"),
    "mysql": dict(kind="database", format="mysql", host="127.0.0.1", port=3307,
                  database="dataflow", username="root", password="dataflow"),
    "oracle": dict(kind="database", format="oracle", host="localhost", port=1521,
                   database="FREEPDB1", username="system", password="dataflow"),
}

SRC = "scn_src"
DST = "scn_dst"


# --------------------------------------------------------------------------- engines


def pg_exec(statements: list[str], *, ignore_errors: bool = False) -> None:
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        for s in statements:
            try:
                cur.execute(s)
            except Exception:
                c.rollback()
                if not ignore_errors:
                    raise
        c.commit()


def pg_rows(sql: str) -> list[tuple]:
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def dest_exec(engine: str, statements: list[str]) -> None:
    cfg = DESTS[engine]
    if engine == "postgresql":
        pg_exec(statements, ignore_errors=True)
        return
    if engine == "mysql":
        import pymysql

        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                               password=cfg["password"], database=cfg["database"], autocommit=True)
        with conn.cursor() as cur:
            for s in statements:
                try:
                    cur.execute(s)
                except Exception:
                    pass
        conn.close()
        return
    import oracledb

    conn = oracledb.connect(user=cfg["username"], password=cfg["password"],
                            dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}")
    cur = conn.cursor()
    for s in statements:
        try:
            cur.execute(s)
        except Exception:
            pass
    conn.commit()
    conn.close()


def dest_query(engine: str, sql: str) -> list[tuple]:
    cfg = DESTS[engine]
    if engine == "postgresql":
        return pg_rows(sql)
    if engine == "mysql":
        import pymysql

        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                               password=cfg["password"], database=cfg["database"])
        with conn.cursor() as cur:
            cur.execute(sql)
            out = list(cur.fetchall())
        conn.close()
        return out
    import oracledb

    conn = oracledb.connect(user=cfg["username"], password=cfg["password"],
                            dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}")
    cur = conn.cursor()
    cur.execute(sql)
    out = list(cur.fetchall())
    conn.close()
    return out


def dest_table_ref(engine: str) -> str:
    return f'"{DST}"' if engine == "oracle" else DST


def dest_count(engine: str) -> int:
    try:
        return int(dest_query(engine, f"SELECT COUNT(*) FROM {dest_table_ref(engine)}")[0][0])
    except Exception as exc:
        return -1


def run_transfer(
    engine: str,
    mappings: list[dict[str, Any]],
    *,
    sync_mode: str = "incremental_append",
) -> dict[str, Any]:
    from services.decision_kernel import build_artifact_from_mappings
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    artifact = build_artifact_from_mappings(
        [dict(m) for m in mappings],
        dest_db=engine,
        source_db="postgresql",
        route_id=f"scenario:{engine}",
        sync_mode=sync_mode,
    )
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", host=PG["host"],
                              port=PG["port"], database=PG["dbname"], username=PG["user"],
                              password=PG["password"], table=SRC),
        destination=EndpointConfig(table=DST, **DESTS[engine]),
        sync_mode=sync_mode,
        mappings=[dict(m) for m in mappings],
        decision_artifact=artifact.to_dict(),
        approved_decision_artifact_hash=artifact.content_hash,
    )
    res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    return {
        "success": bool(res.success),
        "rows_written": int(res.records_transferred or 0),
        "error": str(res.error or "")[:400],
        # Post-load attestation lives on the reconciliation payload: a scenario
        # that only reads success/rows cannot tell a row-perfect load from one
        # that left the destination without its constraints.
        "reconciliation": dict(res.reconciliation or {}),
        "destination_summary": dict(res.destination_summary or {}),
    }


def m(source: str, target: str, src_t: str, dst_t: str, **extra: Any) -> dict[str, Any]:
    row = {"source": source, "target": target, "source_type": src_t,
           "target_type": dst_t, "transform": "none", "confidence": 0.97}
    row.update(extra)
    return row


# --------------------------------------------------------------------------- scenarios


def wide_source_narrow_dest(engine: str, *, omit_marked: bool) -> dict[str, Any]:
    """30 source columns into an existing 20-column destination."""
    src_cols = [f"c{i} VARCHAR(64)" for i in range(1, 31)]
    dst_cols = [f"c{i} VARCHAR(64)" for i in range(1, 21)]
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, {', '.join(src_cols)})"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, "
             + ", ".join(f"'v{i}_{j}'" for j in range(1, 31)) + ")" for i in range(1, 6)])
    ref = dest_table_ref(engine)
    id_col, col = ('"id"', lambda i: f'"c{i}"') if engine == "oracle" else ("id", lambda i: f"c{i}")
    dtype = "VARCHAR2(64)" if engine == "oracle" else "VARCHAR(64)"
    idtype = "NUMBER(19)" if engine == "oracle" else "BIGINT"
    dest_exec(engine, [
        f"DROP TABLE {ref}",
        f"CREATE TABLE {ref} ({id_col} {idtype} PRIMARY KEY, "
        + ", ".join(f"{col(i)} {dtype}" for i in range(1, 21)) + ")",
    ])
    maps = [m("id", "id", "BIGINT", idtype)]
    maps += [m(f"c{i}", f"c{i}", "VARCHAR(64)", dtype) for i in range(1, 21)]
    for i in range(21, 31):
        if omit_marked:
            maps.append(m(f"c{i}", "", "VARCHAR(64)", "", intentional_omit=True))
    out = run_transfer(engine, maps)
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 5
    return out


def dest_required_column_unmapped(engine: str) -> dict[str, Any]:
    """Destination NOT NULL column with no default and nothing mapped into it."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, email VARCHAR(255))"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, 'u{i}@x.com')" for i in range(1, 6)])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = (f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "email" VARCHAR2(255), '
               '"tenant_id" VARCHAR2(32) NOT NULL)')
        dst_str = "VARCHAR2(255)"
        idtype = "NUMBER(19)"
    else:
        ddl = (f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, email VARCHAR(255), "
               "tenant_id VARCHAR(32) NOT NULL)")
        dst_str = "VARCHAR(255)"
        idtype = "BIGINT"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    maps = [m("id", "id", "BIGINT", idtype), m("email", "email", "VARCHAR(255)", dst_str)]
    out = run_transfer(engine, maps)
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 5
    return out


def dest_extra_nullable_column(engine: str) -> dict[str, Any]:
    """Destination has an extra nullable column nothing maps into."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, email VARCHAR(255))"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, 'u{i}@x.com')" for i in range(1, 6)])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = (f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "email" VARCHAR2(255), '
               '"note" VARCHAR2(64))')
        dst_str, idtype = "VARCHAR2(255)", "NUMBER(19)"
        untouched = f'SELECT COUNT(*) FROM {ref} WHERE "note" IS NULL'
    else:
        ddl = (f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, email VARCHAR(255), "
               "note VARCHAR(64))")
        dst_str, idtype = "VARCHAR(255)", "BIGINT"
        untouched = f"SELECT COUNT(*) FROM {ref} WHERE note IS NULL"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("email", "email", "VARCHAR(255)", dst_str)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 5
    try:
        out["untouched_column_null_rows"] = int(dest_query(engine, untouched)[0][0])
    except Exception as exc:
        out["untouched_column_null_rows"] = f"unreadable: {exc}"
    return out


def unsafe_narrowing_string(engine: str) -> dict[str, Any]:
    """Source values longer than the destination column can hold."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, note VARCHAR(255))"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, '{'x' * 120}')" for i in range(1, 6)])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "note" VARCHAR2(10))'
        dst, idtype = "VARCHAR2(10)", "NUMBER(19)"
        check = f'SELECT MAX(LENGTH("note")) FROM {ref}'
    else:
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, note VARCHAR(10))"
        dst, idtype = "VARCHAR(10)", "BIGINT"
        check = (f"SELECT MAX(LENGTH(note)) FROM {ref}" if engine == "postgresql"
                 else f"SELECT MAX(CHAR_LENGTH(note)) FROM {ref}")
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("note", "note", "VARCHAR(255)", dst)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 5
    out["source_value_len"] = 120
    try:
        out["dest_max_len"] = dest_query(engine, check)[0][0]
    except Exception as exc:
        out["dest_max_len"] = f"unreadable: {exc}"
    return out


def numeric_precision_collapse(engine: str) -> dict[str, Any]:
    """DECIMAL(12,4) source into a DECIMAL(6,1) destination column."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, amount DECIMAL(12,4))"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, {i}.1234)" for i in range(1, 6)])
    ref = dest_table_ref(engine)
    idtype = "NUMBER(19)" if engine == "oracle" else "BIGINT"
    amt_col = '"amount"' if engine == "oracle" else "amount"
    id_col = '"id"' if engine == "oracle" else "id"
    dest_exec(engine, [
        f"DROP TABLE {ref}",
        f"CREATE TABLE {ref} ({id_col} {idtype} PRIMARY KEY, {amt_col} DECIMAL(6,1))",
    ])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("amount", "amount", "DECIMAL(12,4)", "DECIMAL(6,1)")])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 5
    try:
        out["dest_values"] = [str(r[0]) for r in dest_query(
            engine, f"SELECT {amt_col} FROM {ref} ORDER BY {amt_col}")]
    except Exception as exc:
        out["dest_values"] = f"unreadable: {exc}"
    return out


def duplicate_source_keys(engine: str) -> dict[str, Any]:
    """Source itself carries the same identity twice against a destination PK."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT, email VARCHAR(255))"])
    pg_exec([f"INSERT INTO {SRC} VALUES (1,'a@x.com'),(2,'b@x.com'),(1,'c@x.com')"])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "email" VARCHAR2(255))'
        dst, idtype = "VARCHAR2(255)", "NUMBER(19)"
    else:
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, email VARCHAR(255))"
        dst, idtype = "VARCHAR(255)", "BIGINT"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("email", "email", "VARCHAR(255)", dst)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 3
    return out


def boolean_representation(engine: str) -> dict[str, Any]:
    """Postgres BOOLEAN into the destination's native boolean carrier."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, active BOOLEAN)"])
    pg_exec([f"INSERT INTO {SRC} VALUES (1,true),(2,false),(3,null)"])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "active" NUMBER(1))'
        dst, idtype = "NUMBER(1)", "NUMBER(19)"
        sel = f'SELECT "id", "active" FROM {ref} ORDER BY "id"'
    elif engine == "mysql":
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, active TINYINT(1))"
        dst, idtype = "TINYINT(1)", "BIGINT"
        sel = f"SELECT id, active FROM {ref} ORDER BY id"
    else:
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, active BOOLEAN)"
        dst, idtype = "BOOLEAN", "BIGINT"
        sel = f"SELECT id, active FROM {ref} ORDER BY id"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("active", "active", "BOOLEAN", dst)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 3
    try:
        out["dest_values"] = [[str(v) for v in r] for r in dest_query(engine, sel)]
    except Exception as exc:
        out["dest_values"] = f"unreadable: {exc}"
    return out


def json_column(engine: str) -> dict[str, Any]:
    """Postgres JSONB into the destination's structured/text carrier."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, doc JSONB)"])
    pg_exec(["INSERT INTO " + SRC + " VALUES (1,'{\"a\":1,\"b\":[1,2]}'),"
             "(2,'{\"a\":2}'),(3,null)"])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "doc" CLOB)'
        dst, idtype = "CLOB", "NUMBER(19)"
        sel = f'SELECT "doc" FROM {ref} ORDER BY "id"'
    elif engine == "mysql":
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, doc JSON)"
        dst, idtype = "JSON", "BIGINT"
        sel = f"SELECT doc FROM {ref} ORDER BY id"
    else:
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, doc JSONB)"
        dst, idtype = "JSONB", "BIGINT"
        sel = f"SELECT doc FROM {ref} ORDER BY id"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("doc", "doc", "JSONB", dst)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 3
    try:
        out["dest_values"] = [str(r[0])[:60] for r in dest_query(engine, sel)]
    except Exception as exc:
        out["dest_values"] = f"unreadable: {exc}"
    return out


def timestamp_timezone(engine: str) -> dict[str, Any]:
    """TIMESTAMPTZ source into a naive destination TIMESTAMP."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, ts TIMESTAMPTZ)"])
    pg_exec(["INSERT INTO " + SRC + " VALUES "
             "(1,'2024-03-01 12:00:00+05:30'),(2,'2024-03-01 12:00:00-08:00')"])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "ts" TIMESTAMP(6))'
        dst, idtype = "TIMESTAMP(6)", "NUMBER(19)"
        sel = f'SELECT TO_CHAR("ts",\'YYYY-MM-DD HH24:MI:SS\') FROM {ref} ORDER BY "id"'
    elif engine == "mysql":
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, ts DATETIME(6))"
        dst, idtype = "DATETIME(6)", "BIGINT"
        sel = f"SELECT ts FROM {ref} ORDER BY id"
    else:
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, ts TIMESTAMP)"
        dst, idtype = "TIMESTAMP", "BIGINT"
        sel = f"SELECT ts FROM {ref} ORDER BY id"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("ts", "ts", "TIMESTAMPTZ", dst)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 2
    out["source_values_utc"] = ["2024-03-01 06:30:00", "2024-03-01 20:00:00"]
    try:
        out["dest_values"] = [str(r[0]) for r in dest_query(engine, sel)]
    except Exception as exc:
        out["dest_values"] = f"unreadable: {exc}"
    return out


def unicode_and_control_chars(engine: str) -> dict[str, Any]:
    """Zero-width / emoji / newline payloads must survive or be quarantined."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, txt TEXT)"])
    pg_exec(["INSERT INTO " + SRC + " VALUES "
             "(1, E'caf\\u00e9'),(2, E'a\\u200bb'),(3, E'line1\\nline2'),(4, E'\\U0001F600')"])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f'CREATE TABLE {ref} ("id" NUMBER(19) PRIMARY KEY, "txt" VARCHAR2(400))'
        dst, idtype = "VARCHAR2(400)", "NUMBER(19)"
        sel = f'SELECT "txt" FROM {ref} ORDER BY "id"'
    elif engine == "mysql":
        ddl = (f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, txt VARCHAR(400)) "
               "CHARACTER SET utf8mb4")
        dst, idtype = "VARCHAR(400)", "BIGINT"
        sel = f"SELECT txt FROM {ref} ORDER BY id"
    else:
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, txt TEXT)"
        dst, idtype = "TEXT", "BIGINT"
        sel = f"SELECT txt FROM {ref} ORDER BY id"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "id", "BIGINT", idtype),
                                m("txt", "txt", "TEXT", dst)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 4
    try:
        out["dest_values"] = [repr(r[0]) for r in dest_query(engine, sel)]
    except Exception as exc:
        out["dest_values"] = f"unreadable: {exc}"
    return out


def case_sensitive_destination(engine: str) -> dict[str, Any]:
    """Destination created with quoted mixed-case identifiers."""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, email VARCHAR(255))"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, 'u{i}@x.com')" for i in range(1, 4)])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f'CREATE TABLE {ref} ("Id" NUMBER(19) PRIMARY KEY, "Email" VARCHAR2(255))'
        dst, idtype = "VARCHAR2(255)", "NUMBER(19)"
    elif engine == "mysql":
        ddl = f"CREATE TABLE {ref} (`Id` BIGINT PRIMARY KEY, `Email` VARCHAR(255))"
        dst, idtype = "VARCHAR(255)", "BIGINT"
    else:
        ddl = f'CREATE TABLE {ref} ("Id" BIGINT PRIMARY KEY, "Email" VARCHAR(255))'
        dst, idtype = "VARCHAR(255)", "BIGINT"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    out = run_transfer(engine, [m("id", "Id", "BIGINT", idtype),
                                m("email", "Email", "VARCHAR(255)", dst)])
    out["dest_rows"] = dest_count(engine)
    out["source_rows"] = 3
    return out


def _sync_mode_rerun(engine: str, sync_mode: str, expected_second_run_rows: int) -> dict[str, Any]:
    """Run the same clean batch twice and report what the destination holds.

    The sync mode is the contract: overwrite and deduped/upsert must converge on
    the source cardinality however many times they run, append must grow by the
    batch. Re-running is the only way to catch a mode that silently behaves like
    another one.
    """
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT PRIMARY KEY, email VARCHAR(255))"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, 'u{i}@x.com')" for i in range(1, 4)])
    ref = dest_table_ref(engine)
    if engine == "oracle":
        ddl = f"CREATE TABLE {ref} (id NUMBER(19) PRIMARY KEY, email VARCHAR2(255))"
        dst, idtype = "VARCHAR2(255)", "NUMBER(19)"
    else:
        ddl = f"CREATE TABLE {ref} (id BIGINT PRIMARY KEY, email VARCHAR(255))"
        dst, idtype = "VARCHAR(255)", "BIGINT"
    dest_exec(engine, [f"DROP TABLE {ref}", ddl])
    maps = [m("id", "id", "BIGINT", idtype, primary_key=True),
            m("email", "email", "VARCHAR(255)", dst)]
    first = run_transfer(engine, maps, sync_mode=sync_mode)
    first_rows = dest_count(engine)
    second = run_transfer(engine, maps, sync_mode=sync_mode)
    second_rows = dest_count(engine)
    return {
        "success": bool(first["success"] and second["success"]),
        "rows_written": int(first["rows_written"]) + int(second["rows_written"]),
        "error": first["error"] or second["error"],
        "source_rows": 3,
        "dest_rows_after_first": first_rows,
        "dest_rows": second_rows,
        "expected_dest_rows_after_second": expected_second_run_rows,
        "converges_as_declared": second_rows == expected_second_run_rows,
    }


SCENARIOS: dict[str, Callable[[str], dict[str, Any]]] = {
    "wide_source_narrow_dest_unmapped": lambda e: wide_source_narrow_dest(e, omit_marked=False),
    "wide_source_narrow_dest_omissions_declared": lambda e: wide_source_narrow_dest(e, omit_marked=True),
    "dest_required_column_unmapped": dest_required_column_unmapped,
    "dest_extra_nullable_column": dest_extra_nullable_column,
    "unsafe_narrowing_string": unsafe_narrowing_string,
    "numeric_precision_collapse": numeric_precision_collapse,
    "duplicate_source_keys": duplicate_source_keys,
    "boolean_representation": boolean_representation,
    "json_column": json_column,
    "timestamp_timezone": timestamp_timezone,
    "unicode_and_control_chars": unicode_and_control_chars,
    "case_sensitive_destination": case_sensitive_destination,
    # Sync-mode dimension — same batch twice, destination counted both times.
    "sync_full_refresh_overwrite_rerun": lambda e: _sync_mode_rerun(e, "full_refresh_overwrite", 3),
    "sync_incremental_deduped_rerun": lambda e: _sync_mode_rerun(e, "incremental_deduped", 3),
    # Append of the same keys into a keyed destination must be refused before the
    # write, not half-applied and then aborted by the database.
    "sync_full_refresh_append_rerun": lambda e: _sync_mode_rerun(e, "full_refresh_append", 3),
}


def main() -> int:
    results: dict[str, dict[str, Any]] = {}
    for engine in DESTS:
        per: dict[str, Any] = {}
        for name, fn in SCENARIOS.items():
            try:
                per[name] = fn(engine)
            except Exception as exc:  # harness/engine failure — report, never invent
                per[name] = {"harness_error": f"{type(exc).__name__}: {exc}"[:300]}
            print(f"{engine:12} {name:44} {json.dumps(per[name])[:160]}", flush=True)
        results[engine] = per
    with open("/home/ubuntu/repro/migration_scenario_matrix_results.json", "w") as fh:
        json.dump(results, fh, indent=2, default=str)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
