"""X-50: JSON/JSONB read-back must compare semantically and keep JSON null
distinct from SQL NULL.

Live evidence (88ea869c, MySQL JSON -> PostgreSQL): the job wrote correctly —
the dest column was ``jsonb`` and the row held JSON ``null`` — but the Gate-8
sample read decoded it through the driver into Python ``None`` and compared it
against the source text ``'null'``. A faithful transfer was reported as
corruption and rolled back, and the message printed ``dest='null'`` /
``target=None`` for the same cell.

Fix:
- ``read_target_sample`` / ``read_postgres_rows`` read PG json/jsonb columns
  as text (``col::text``), so JSON null arrives as the literal ``'null'`` and
  SQL NULL stays ``None`` — distinguishable in BOTH directions.
- ``fingerprint_for_reconcile`` compares JSON semantically (parse + canonical
  dump): ``{"a":1}`` == ``{"a": 1}`` across engines, while a JSON ``null``
  fingerprints distinctly from SQL NULL so a JSON-null <-> SQL-NULL tamper
  still fails.
- L5 mismatch rendering prints ``null`` for SQL NULL, matching the
  sample-compare renderer (one spelling per cell).

Coverage:
- Hermetic fingerprint cells (always run).
- Live legs (skip-gated DATAFLOW_X50_LIVE + engine env vars):
  MySQL JSON -> PG, PG jsonb -> MySQL/SQLite/SQL Server/DuckDB,
  PG json/jsonb -> PG, Mongo nested-null docs -> PG; clean exact plus tamper
  both ways (JSON null <-> SQL NULL) and a value tamper.
"""

from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_LIVE = pytest.mark.skipif(
    not os.getenv("DATAFLOW_X50_LIVE"),
    reason="live X-50 check: set DATAFLOW_X50_LIVE=1 with reachable engines",
)

# The 8 JSON values from the loop probe. JSON_NULL is the literal ``null``
# (arrives from MySQL as the text 'null'); SQL NULL is None.
_JSON_VALUES = [
    json.dumps({"a": 1, "b": None, "c": [1, 2]}),          # nested w/ JSON null
    json.dumps({"t": "héllo 日本語 😀"}),                    # unicode
    "null",                                                 # JSON literal null
    None,                                                   # SQL NULL
    "[]",
    json.dumps("plain string"),
    json.dumps({"q": 'has "quote" and \\ backslash', "n": -1.5, "i": 12345678901234}),
    "true",
]
_JSON_NULL_IDX = 2   # index of the JSON 'null' row
_SQL_NULL_IDX = 3    # index of the SQL NULL row


# ---------------------------------------------------------------------------
# Hermetic fingerprint cells — always run in the suite.
# ---------------------------------------------------------------------------

def test_x50_json_null_is_not_sql_null():
    from services.reconciliation import fingerprint_for_reconcile

    json_null = fingerprint_for_reconcile("null", ddl_type="jsonb", engine="postgres")
    sql_null = fingerprint_for_reconcile(None, ddl_type="jsonb", engine="postgres")
    assert json_null != sql_null, (
        "JSON literal null must fingerprint distinctly from SQL NULL"
    )
    # Already-decoded nulls (dict/list path in a driver) — same rule.
    assert fingerprint_for_reconcile(
        {"a": None}, ddl_type="jsonb", engine="postgres"
    ) != sql_null


def test_x50_json_whitespace_and_key_order_equate():
    from services.reconciliation import fingerprint_for_reconcile

    a = fingerprint_for_reconcile('{"a":1,"b":[2,3]}', ddl_type="jsonb", engine="postgres")
    b = fingerprint_for_reconcile('{ "b": [ 2, 3 ], "a": 1 }', ddl_type="jsonb", engine="postgres")
    c = fingerprint_for_reconcile({"b": [2, 3], "a": 1}, ddl_type="jsonb", engine="postgres")
    assert a == b == c


def test_x50_json_value_tamper_still_fails():
    from services.reconciliation import fingerprint_for_reconcile

    a = fingerprint_for_reconcile('{"a":1}', ddl_type="jsonb", engine="postgres")
    b = fingerprint_for_reconcile('{"a":2}', ddl_type="jsonb", engine="postgres")
    c = fingerprint_for_reconcile("[9]", ddl_type="jsonb", engine="postgres")
    assert a != b != c


def test_x50_non_json_column_stays_textual():
    from services.reconciliation import fingerprint_for_reconcile

    # A VARCHAR 'null' on a TEXT column is still just text — the JSON rule
    # must not leak onto plain columns.
    assert fingerprint_for_reconcile("null", ddl_type="text", engine="postgres") == "null"
    assert fingerprint_for_reconcile(None, ddl_type="text", engine="postgres") != "null"


def test_x51_undefined_column_error_is_not_capacity():
    from services.error_handling import humanize_transfer_failure

    h = humanize_transfer_failure(RuntimeError('column "id" does not exist'))
    assert h["code"] == "destination_column_missing"
    assert "address capacity" not in h["message"].lower(), h["message"]


# ---------------------------------------------------------------------------
# Live legs.
# ---------------------------------------------------------------------------

def _cfg(prefix: str, default_port: int) -> dict[str, Any] | None:
    host = os.getenv(f"{prefix}_HOST", "")
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.getenv(f"{prefix}_PORT", str(default_port))),
        "database": os.getenv(f"{prefix}_DB", ""),
        "username": os.getenv(f"{prefix}_USER", ""),
        "password": os.getenv(f"{prefix}_PASSWORD", ""),
        "connection_string": os.getenv(f"{prefix}_CONNECTION_STRING", ""),
        "ssl": False,
        **({"encrypt": os.getenv(f"{prefix}_ENCRYPT")} if os.getenv(f"{prefix}_ENCRYPT") else {}),
        "trust_server_certificate": os.getenv(
            f"{prefix}_TRUST_SERVER_CERTIFICATE", "yes"
        ) in {"1", "true", "yes", "y", "on"},
    }


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


def _mysql_conn(cfg):
    import pymysql

    return pymysql.connect(
        host=cfg["host"], port=int(cfg["port"]), user=cfg["username"],
        password=cfg["password"], database=cfg["database"], autocommit=True,
    )


def _pg_conn(cfg):
    import psycopg2

    return psycopg2.connect(
        host=cfg["host"], port=int(cfg["port"]), user=cfg["username"],
        password=cfg["password"], dbname=cfg["database"],
    )


def _endpoint(engine: str, cfg: dict[str, Any], table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format=engine,
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 0),
        database=str(cfg.get("database") or ""),
        table=table,
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        ssl=bool(cfg.get("ssl", False)),
        extra={},
    )


def _seed(engine: str, cfg: dict[str, Any], table: str, json_type: str = "json") -> None:
    if engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, t JSON)")  # nosec B608
            for i, v in enumerate(_JSON_VALUES):
                cur.execute(
                    f"INSERT INTO {table} (id, t) VALUES (%s, %s)",  # nosec B608
                    (i + 1, v),
                )
        finally:
            conn.close()
        return
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            cur.execute(
                f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, t {json_type})"  # nosec B608
            )
            for i, v in enumerate(_JSON_VALUES):
                cur.execute(
                    f"INSERT INTO {table} (id, t) VALUES (%s, %s)",  # nosec B608
                    (i + 1, v),
                )
        finally:
            conn.close()
        return
    if engine == "mongodb":
        import pymongo

        cli = pymongo.MongoClient(
            host=cfg["host"], port=int(cfg["port"]),
            username=cfg.get("username") or None,
            password=cfg.get("password") or None,
        )
        try:
            coll = cli[cfg["database"]][table]
            coll.drop()
            docs = []
            for i, v in enumerate(_JSON_VALUES):
                doc = {"id": i + 1}
                if v is None:
                    doc["t"] = None
                else:
                    doc["t"] = json.loads(v)
                docs.append(doc)
            coll.insert_many(docs)
        finally:
            cli.close()
        return
    raise AssertionError(engine)


def _drop(engine: str, cfg: dict[str, Any], table: str) -> None:
    try:
        if engine == "mysql":
            conn = _mysql_conn(cfg)
            try:
                conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            finally:
                conn.close()
        elif engine == "postgresql":
            conn = _pg_conn(cfg)
            try:
                conn.autocommit = True
                conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            finally:
                conn.close()
        elif engine == "mongodb":
            import pymongo

            cli = pymongo.MongoClient(host=cfg["host"], port=int(cfg["port"]))
            try:
                cli[cfg["database"]][table].drop()
            finally:
                cli.close()
    except Exception:
        pass


def _dest_exists(engine: str, cfg: dict[str, Any], table: str) -> bool:
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                (table,),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()
    if engine == "sqlite":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        try:
            return conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone() is not None
        finally:
            conn.close()
    if engine == "duckdb":
        import duckdb

        conn = duckdb.connect(cfg["database"])
        try:
            return any(
                str(r[0]).lower() == table.lower()
                for r in conn.execute("SHOW TABLES").fetchall()
            )
        finally:
            conn.close()
    if engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            return conn.cursor().execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s",
                (cfg["database"], table),
            ) is not None
        finally:
            conn.close()
    if engine in {"mssql", "sqlserver"}:
        import pymssql

        conn = pymssql.connect(
            server=cfg["host"], port=str(cfg["port"]), user=cfg["username"],
            password=cfg["password"], database=cfg["database"],
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT OBJECT_ID(%s,'U')", (f"dbo.{table}",))
            return cur.fetchone()[0] is not None
        finally:
            conn.close()
    raise AssertionError(engine)


def _dest_cfg(engine: str) -> tuple[dict[str, Any], str]:
    """Return (endpoint cfg dict, env prefix) — the cfg carries a `database`
    or file path; for file engines the caller fills tmp_path later."""
    prefix = {
        "postgresql": "DATAFLOW_TEST_PG",
        "mysql": "DATAFLOW_TEST_MYSQL",
        "mssql": "DATAFLOW_TEST_MSSQL",
        "mongodb": "DATAFLOW_TEST_MONGO",
    }.get(engine, "")
    default_port = {
        "postgresql": 5432, "mysql": 3306, "mssql": 1433, "mongodb": 27017,
    }.get(engine, 0)
    return _cfg(prefix, default_port), prefix


def _tamper_json_null_to_sql_null(engine, cfg, table):
    """JSON literal null -> SQL NULL on the dest (row _JSON_NULL_IDX+1)."""
    rid = _JSON_NULL_IDX + 1
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            conn.autocommit = True
            conn.cursor().execute(
                f"UPDATE {table} SET t = NULL WHERE id = %s", (rid,)  # nosec B608
            )
        finally:
            conn.close()
    elif engine == "sqlite":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        try:
            conn.execute(f"UPDATE {table} SET t = NULL WHERE id = ?", (rid,))  # nosec B608
            conn.commit()
        finally:
            conn.close()
    elif engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            conn.cursor().execute(
                f"UPDATE {table} SET t = NULL WHERE id = %s", (rid,)  # nosec B608
            )
        finally:
            conn.close()
    elif engine in {"mssql", "sqlserver"}:
        import pymssql

        conn = pymssql.connect(
            server=cfg["host"], port=str(cfg["port"]), user=cfg["username"],
            password=cfg["password"], database=cfg["database"],
        )
        try:
            conn.cursor().execute(
                f"UPDATE {table} SET t = NULL WHERE id = %s", (rid,)  # nosec B608
            )
            conn.commit()
        finally:
            conn.close()


def _tamper_sql_null_to_json_null(engine, cfg, table):
    """SQL NULL -> JSON literal null on the dest (row _SQL_NULL_IDX+1)."""
    rid = _SQL_NULL_IDX + 1
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            conn.autocommit = True
            conn.cursor().execute(
                f"UPDATE {table} SET t = 'null'::jsonb WHERE id = %s", (rid,)  # nosec B608
            )
        finally:
            conn.close()
    elif engine == "sqlite":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        try:
            conn.execute(f"UPDATE {table} SET t = 'null' WHERE id = ?", (rid,))  # nosec B608
            conn.commit()
        finally:
            conn.close()
    elif engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            conn.cursor().execute(
                f"UPDATE {table} SET t = 'null' WHERE id = %s", (rid,)  # nosec B608
            )
        finally:
            conn.close()
    elif engine in {"mssql", "sqlserver"}:
        import pymssql

        conn = pymssql.connect(
            server=cfg["host"], port=str(cfg["port"]), user=cfg["username"],
            password=cfg["password"], database=cfg["database"],
        )
        try:
            conn.cursor().execute(
                f"UPDATE {table} SET t = 'null' WHERE id = %s", (rid,)  # nosec B608
            )
            conn.commit()
        finally:
            conn.close()


def _tamper_value(engine, cfg, table):
    rid = 1
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            conn.autocommit = True
            conn.cursor().execute(
                f"UPDATE {table} SET t = '[9]'::jsonb WHERE id = %s", (rid,)  # nosec B608
            )
        finally:
            conn.close()
    elif engine == "sqlite":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        try:
            conn.execute(f"UPDATE {table} SET t = '[9]' WHERE id = ?", (rid,))  # nosec B608
            conn.commit()
        finally:
            conn.close()
    elif engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            conn.cursor().execute(
                f"UPDATE {table} SET t = '[9]' WHERE id = %s", (rid,)  # nosec B608
            )
        finally:
            conn.close()
    elif engine in {"mssql", "sqlserver"}:
        import pymssql

        conn = pymssql.connect(
            server=cfg["host"], port=str(cfg["port"]), user=cfg["username"],
            password=cfg["password"], database=cfg["database"],
        )
        try:
            conn.cursor().execute(
                f"UPDATE {table} SET t = '[9]' WHERE id = %s", (rid,)  # nosec B608
            )
            conn.commit()
        finally:
            conn.close()


_DEST_ENVS = {
    "postgresql": ("DATAFLOW_TEST_PG", 5432),
    "mysql": ("DATAFLOW_TEST_MYSQL", 3306),
    "mssql": ("DATAFLOW_TEST_MSSQL", 1433),
}


def _run_job(src_engine, src_cfg, src_table, dest_engine, dest_cfg, dest_table, tag):
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=_endpoint(src_engine, src_cfg, src_table),
            destination=_endpoint(dest_engine, dest_cfg, dest_table),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        ),
        tag,
    )


@_LIVE
@pytest.mark.parametrize("dest_engine", ["postgresql", "sqlite", "mysql", "mssql", "duckdb"])
@pytest.mark.parametrize("tamper", ["none", "value", "json2null", "null2json"])
def test_x50_mysql_json_to_dest(dest_engine, tamper, monkeypatch, tmp_path):
    """MySQL JSON -> {PG, SQLite, MySQL, SQL Server, DuckDB}: clean exact,
    tamper detected incl. JSON-null <-> SQL-NULL in both directions."""
    src_cfg = _cfg("DATAFLOW_TEST_MYSQL", 3306)
    if not src_cfg or not _reachable(src_cfg["host"], src_cfg["port"]):
        pytest.skip("set DATAFLOW_TEST_MYSQL_* (reachable)")
    if dest_engine in _DEST_ENVS:
        prefix, port = _DEST_ENVS[dest_engine]
        dest_cfg = _cfg(prefix, port)
        if not dest_cfg or not _reachable(dest_cfg["host"], dest_cfg["port"]):
            pytest.skip(f"set {prefix}_* (reachable)")
    else:
        dest_cfg = {"database": str(tmp_path / f"x50_{dest_engine}.db")}
    if dest_engine == "mysql" and tamper == "null2json":
        pytest.skip("same-engine MySQL->MySQL null tamper is out of scope")

    src_table = f"x50json_{dest_engine}"
    dest_table = f"x50out_{dest_engine}_{tamper}"
    _seed("mysql", src_cfg, src_table)
    tampered = {"done": False}
    orig_recon = engine_mod.run_reconciliation

    def _tampering_recon(*args, **kwargs):
        if tamper != "none" and not tampered["done"]:
            if tamper == "value":
                _tamper_value(dest_engine, dest_cfg, dest_table)
            elif tamper == "json2null":
                _tamper_json_null_to_sql_null(dest_engine, dest_cfg, dest_table)
            else:
                _tamper_sql_null_to_json_null(dest_engine, dest_cfg, dest_table)
            tampered["done"] = True
        return orig_recon(*args, **kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", _tampering_recon)
    try:
        result = _run_job(
            "mysql", src_cfg, src_table, dest_engine, dest_cfg, dest_table,
            f"0000000000000000000000{dest_engine[:2]}{tamper[:2]}",
        )
    finally:
        _drop("mysql", src_cfg, src_table)

    if tamper == "none":
        assert result.success is True, (
            f"clean MySQL JSON -> {dest_engine} must pass: "
            f"{getattr(result, 'error', '') or getattr(result, 'message', '')}"
        )
        return
    assert tampered["done"], "tamper hook never fired"
    assert result.success is False, (
        f"tamper {tamper} on MySQL JSON -> {dest_engine} must fail: "
        f"{getattr(result, 'message', '')}"
    )


@_LIVE
@pytest.mark.parametrize("pg_type", ["json", "jsonb"])
def test_x50_pg_json_to_pg_clean(pg_type, tmp_path):
    """PG json/jsonb -> PG: clean run passes; JSON null stays 'null', SQL NULL
    stays NULL."""
    src_cfg = _cfg("DATAFLOW_TEST_PG", 5432)
    dest_cfg = _cfg("DATAFLOW_TEST_PG", 5432)
    if not src_cfg or not _reachable(src_cfg["host"], src_cfg["port"]):
        pytest.skip("set DATAFLOW_TEST_PG_* (reachable)")
    src_table = f"x50pgsrc_{pg_type}"
    dest_table = f"x50pgout_{pg_type}"
    _seed("postgresql", src_cfg, src_table, json_type=pg_type)
    try:
        result = _run_job(
            "postgresql", src_cfg, src_table, "postgresql", dest_cfg, dest_table,
            f"00000000000000000000pg{pg_type[:2]}",
        )
    finally:
        _drop("postgresql", src_cfg, src_table)
    assert result.success is True, (
        f"clean PG {pg_type} -> PG must pass: "
        f"{getattr(result, 'error', '') or getattr(result, 'message', '')}"
    )
    # Read-back: JSON null is 'null'::text, not SQL NULL — and the SQL NULL
    # row stayed NULL.
    conn = _pg_conn(dest_cfg)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            f"SELECT id, t::text, (t IS NULL) FROM {dest_table} ORDER BY id"  # nosec B608
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    by_id = {r[0]: (r[1], r[2]) for r in rows}
    assert by_id[_JSON_NULL_IDX + 1] == ("null", False), (
        "JSON null must read back as text 'null', not SQL NULL"
    )
    assert by_id[_SQL_NULL_IDX + 1][1] is True, (
        "SQL NULL row must stay NULL"
    )


@_LIVE
def test_x50_mongo_nested_null_to_pg():
    """Mongo nested docs with null fields -> PG jsonb: clean passes."""
    src_cfg = _cfg("DATAFLOW_TEST_MONGO", 27017)
    dest_cfg = _cfg("DATAFLOW_TEST_PG", 5432)
    if not src_cfg or not _reachable(src_cfg["host"], src_cfg["port"]):
        pytest.skip("set DATAFLOW_TEST_MONGO_* (reachable)")
    if not dest_cfg or not _reachable(dest_cfg["host"], dest_cfg["port"]):
        pytest.skip("set DATAFLOW_TEST_PG_* (reachable)")
    src_table = "x50mongosrc"
    dest_table = "x50mongo_out"
    _seed("mongodb", src_cfg, src_table)
    try:
        result = _run_job(
            "mongodb", src_cfg, src_table, "postgresql", dest_cfg, dest_table,
            "00000000000000000000mopg",
        )
    finally:
        _drop("mongodb", src_cfg, src_table)
    assert result.success is True, (
        f"clean Mongo nested-null -> PG must pass: "
        f"{getattr(result, 'error', '') or getattr(result, 'message', '')}"
    )
