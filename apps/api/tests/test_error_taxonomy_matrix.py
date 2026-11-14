"""The error contract: one cause, one code, one message, on every surface.

Promoted from the ERR audit probe (``probe_classifier.py``). Each case raises a
real exception from PostgreSQL 16, MySQL 8, MongoDB 7 or SQLite through the
product's own engine path, then asks every surface what it would show:

* Test (``humanize_connection_error``), Query (``user_query_error``),
  introspect (``endpoint_intelligence``), Validate (the G1 sample read),
  the job record (``_job_failure_fields``), the schedule notification
  (``build_job_payload``) and Pilot (``run_query``'s refusal and listing paths).

Asserted: the taxonomy code; no internals (driver class, SQL, ``LINE``/caret,
sqlalche.me, host:port); and, for a connection-level cause, one identical
message across all seven surfaces. Creates only ``w3err_mx_*`` objects.
"""

from __future__ import annotations

import errno
import os
import re
import secrets
import uuid
from types import SimpleNamespace

import pytest

from tests import r4_route_harness as h

TOK = uuid.uuid4().hex[:6]
T = f"w3err_mx_{TOK}"
NP = f"w3err_mx_np_{TOK}"
NP_PW = secrets.token_hex(8)

_INTERNALS = re.compile(
    r"psycopg2|pymysql|pymongo|sqlite3\.|sqlalchemy|OperationalError|ProgrammingError|IntegrityError|"
    r"DataError|OperationFailure|ServerSelectionTimeout|\[SQL|\bLINE \d|\^|sqlalche\.me|"
    r"\b\d{1,3}(?:\.\d{1,3}){3}\b|:\d{2,5}\b|\$clusterTime|full error|HINT:|DETAIL:|Traceback",
)
_REF = re.compile(r"\s*Reference: \S+$")


def _plain(text: str) -> str:
    return _REF.sub("", str(text or "")).strip()


# -- the exceptions -----------------------------------------------------------

def _eng(kind: str, **over):
    from connectors.generic_sql import get_sqlalchemy_engine

    return get_sqlalchemy_engine({**h.dest_config(kind), **over})


def _run(e, *stmts):
    from sqlalchemy import text

    with e.begin() as c:
        for s in stmts:
            c.execute(text(s))


PG_CASES = {
    "pg_missing_table": (lambda: _run(_eng("postgresql"), "select * from w3err_mx_records"), "object.table_not_found"),
    "pg_missing_table_named_authors": (lambda: _run(_eng("postgresql"), "select * from authors_w3err"), "object.table_not_found"),
    "pg_missing_column": (lambda: _run(_eng("postgresql"), f'select nope from "{T}"'), "object.column_not_found"),
    "pg_missing_schema": (lambda: _run(_eng("postgresql"), "create table w3err_noschema.x (id int)"), "object.schema_not_found"),
    "pg_permission_denied_table": (
        lambda: _run(_eng("postgresql", username=NP, password=NP_PW), f'select * from "{T}"'), "access.source_read_denied"),
    "pg_syntax_error": (lambda: _run(_eng("postgresql"), "select * fro records"), "query.syntax"),
    "pg_wrong_password": (lambda: _run(_eng("postgresql", password="wrong-" + NP_PW), "select 1"), "auth.password_rejected"),
    "pg_unknown_database": (lambda: _run(_eng("postgresql", database="w3err_mx_nodb"), "select 1"), "object.database_not_found"),
    "pg_database_named_auth": (lambda: _run(_eng("postgresql", database="auth"), "select 1"), "object.database_not_found"),
    "pg_refused": (lambda: _run(_eng("postgresql", port=1), "select 1"), "network.refused"),
    "pg_statement_timeout": (lambda: _run(
        _eng("postgresql"), "set local statement_timeout=200",
        "select count(*) from generate_series(1,200000000)"), "timeout.statement"),
    "pg_type_mismatch": (lambda: _run(_eng("postgresql"), f"insert into \"{T}\" (id, v) values (2, 'abc')"), "value.type_mismatch"),
    "pg_duplicate_pk": (lambda: _run(_eng("postgresql"), f"insert into \"{T}\" (id, name, v) values (1,'z',1)"), "value.duplicate_key"),
    "pg_not_null": (lambda: _run(_eng("postgresql"), f"insert into \"{T}\" (id, v) values (4, NULL)"), "value.not_null"),
    "pg_value_too_long": (lambda: _run(_eng("postgresql"), f"insert into \"{T}\" (id, name, v) values (5,'abcdef',1)"), "value.too_long"),
    "pg_numeric_overflow": (lambda: _run(_eng("postgresql"), f"insert into \"{T}\" (id, n, v) values (6, 12345.6, 1)"), "value.numeric_overflow"),
    "pg_check_violation": (lambda: _run(_eng("postgresql"), f"insert into \"{T}\" (id, v) values (7, -1)"), "value.check_violation"),
    "pg_division_by_zero": (lambda: _run(_eng("postgresql"), "select 1/0"), "query.division_by_zero"),
}

MY_CASES = {
    "mysql_missing_table": (lambda: _run(_eng("mysql"), "select * from w3err_mx_records"), "object.table_not_found"),
    "mysql_missing_column": (lambda: _run(_eng("mysql"), f"select nope from `{T}`"), "object.column_not_found"),
    "mysql_unknown_database": (lambda: _run(_eng("mysql", database="w3err_mx_nodb"), "select 1"), "access.database_not_granted"),
    "mysql_wrong_password": (lambda: _run(_eng("mysql", password="wrong"), "select 1"), "auth.password_rejected"),
    "mysql_syntax_error": (lambda: _run(_eng("mysql"), "select * fro records"), "query.syntax"),
    "mysql_type_mismatch": (lambda: _run(
        _eng("mysql"), "set session sql_mode='STRICT_ALL_TABLES'",
        f"insert into `{T}` (id, name, v) values (2,'b','abc')"), "value.type_mismatch"),
    "mysql_duplicate_key": (lambda: _run(_eng("mysql"), f"insert into `{T}` values (1,'z',1)"), "value.duplicate_key"),
    "mysql_not_null": (lambda: _run(_eng("mysql"), f"insert into `{T}` values (3,'z',NULL)"), "value.not_null"),
    "mysql_value_too_long": (lambda: _run(
        _eng("mysql"), "set session sql_mode='STRICT_ALL_TABLES'", f"insert into `{T}` values (4,'abcdef',1)"), "value.too_long"),
}


def _mongo_auth():
    from urllib.parse import urlparse

    import pymongo

    u = urlparse(os.environ["MONGO_URI"])
    c = pymongo.MongoClient(f"mongodb://w3err_mx:bad@{u.hostname}:{u.port}/dataflow", serverSelectionTimeoutMS=3000)
    c.dataflow.list_collection_names()


def _mongo_refused():
    import pymongo

    pymongo.MongoClient("mongodb://127.0.0.1:1/x", serverSelectionTimeoutMS=1500).x.list_collection_names()


def _missing_driver():
    raise ModuleNotFoundError("No module named 'psycopg2'", name="psycopg2")


def _enospc():
    raise OSError(errno.ENOSPC, "No space left on device", "/data/exports/orders.csv")


OTHER_CASES = {
    "mongo_auth_failure": ("mongodb", _mongo_auth, "auth.mongo_auth_source"),
    "mongo_unreachable": ("mongodb", _mongo_refused, "network.refused"),
    "missing_driver": ("postgresql", _missing_driver, "config.driver_missing"),
    "export_enospc": ("csv", _enospc, "capacity.disk_full"),
}

#: Causes of a connection, not of one statement: every surface says the same.
CONNECTION_LEVEL = {
    "pg_wrong_password", "pg_unknown_database", "pg_database_named_auth", "pg_refused",
    "mysql_unknown_database", "mysql_wrong_password", "mongo_auth_failure", "mongo_unreachable",
    "missing_driver",
}


@pytest.fixture(scope="module")
def objects():
    h.execute("postgresql", f'CREATE TABLE "{T}" (id INT PRIMARY KEY, name VARCHAR(3) UNIQUE, '
                            "n NUMERIC(4,2), v INT NOT NULL DEFAULT 0, CHECK (v >= 0))")
    h.execute("postgresql", f"INSERT INTO \"{T}\" VALUES (1,'a',1,1)")
    h.execute("postgresql", f"CREATE ROLE {NP} LOGIN PASSWORD '{NP_PW}'")
    h.execute("postgresql", f"GRANT CONNECT ON DATABASE {h.PG['database']} TO {NP}")
    try:
        h.execute("mysql", f"CREATE TABLE `{T}` (id INT PRIMARY KEY, name VARCHAR(3), v INT NOT NULL)")
        h.execute("mysql", f"INSERT INTO `{T}` VALUES (1,'a',1)")
    except BaseException:  # noqa: BLE001 — MySQL is optional; its cases skip
        pass
    yield
    h.drop_table("postgresql", T)
    h.drop_table("mysql", T)
    h.execute("postgresql", f'REVOKE ALL ON DATABASE {h.PG["database"]} FROM {NP}')
    h.execute("postgresql", f"DROP OWNED BY {NP}")
    h.execute("postgresql", f"DROP ROLE IF EXISTS {NP}")


def _raise(fn):
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 — the exception is the subject
        return exc
    raise AssertionError("did not raise")


# -- the surfaces -------------------------------------------------------------

def _surfaces(exc: BaseException, engine: str, side: str, monkeypatch) -> dict[str, str]:
    from services.notification_service import build_job_payload
    from services.preflight_sample import engine_sample_rows
    from services.safe_errors import user_query_error
    from src.ai.copilot.query_tools import _listing_failure
    from src.transfer import adapters
    from src.transfer.connector_registry import humanize_connection_error
    from src.transfer.endpoint_intelligence import _classify_failure_message
    from src.transfer.job_failure import _job_failure_fields

    out: dict[str, str] = {}
    out["test"] = humanize_connection_error(engine, exc)
    out["query"] = _plain(user_query_error(exc, public_message="Query failed").detail)
    introspected = {"connected": False, "message": str(exc)}
    _classify_failure_message(introspected, engine)
    out["introspect"] = introspected["message"]

    def _boom(*_a, **_k):
        raise exc

    monkeypatch.setattr(adapters, "read_source_database", _boom)
    sample = engine_sample_rows(
        source_kind="database", source_format=engine, source_table=T,
        source_config={"type": engine, "host": "w3err.invalid", "table": T}, limit=5,
    )
    out["validate"] = sample.unavailable_reason
    if side:
        setattr(exc, "datawrap_side", side)
    details, extras = _job_failure_fields(exc)
    out["job"] = extras["operator_error"]
    payload = build_job_payload(
        job_id="w3err-job", status="failed", source=T, destination=T, records_transferred=0,
        rejected_rows=0, error=out["job"], retry_url="", workspace_id="", base_url="", web_url="",
    )
    out["schedule_notification"] = payload["error"]
    listed = SimpleNamespace(success=False, output={"connected": False, "message": out["introspect"]}, error="")
    out["pilot"] = _listing_failure(listed)
    out["_job_code"] = str(details.get("code") or "")
    return out


def _check(case: str, exc: BaseException, engine: str, expected: str, monkeypatch, side: str = ""):
    from services.error_taxonomy import classify

    named = classify(exc, side=side, engine=engine)
    assert named is not None and named.code == expected, (case, named and named.code, str(exc)[:300])
    seen = _surfaces(exc, engine, side, monkeypatch)
    assert seen.pop("_job_code") == expected, case
    for surface, text in seen.items():
        assert text, (case, surface)
        leak = _INTERNALS.search(text)
        assert leak is None, (case, surface, leak.group(0), text)
    if case in CONNECTION_LEVEL:
        assert len(set(seen.values())) == 1, (case, seen)


@pytest.mark.parametrize("case", sorted(PG_CASES))
def test_postgresql(case, objects, monkeypatch):
    fn, expected = PG_CASES[case]
    side = "source" if expected.startswith("access.") else ""
    _check(case, _raise(fn), "postgresql", expected, monkeypatch, side=side)


@pytest.mark.parametrize("case", sorted(MY_CASES))
def test_mysql(case, objects, monkeypatch):
    fn, expected = MY_CASES[case]
    _check(case, _raise(fn), "mysql", expected, monkeypatch)


@pytest.mark.parametrize("case", sorted(OTHER_CASES))
def test_mongo_driver_and_files(case, monkeypatch):
    engine, fn, expected = OTHER_CASES[case]
    if engine == "mongodb" and not os.environ.get("MONGO_URI") and case == "mongo_auth_failure":
        pytest.skip("MongoDB not configured (MONGO_URI)")
    _check(case, _raise(fn), engine, expected, monkeypatch)


def test_sqlite(tmp_path, monkeypatch):
    import sqlite3

    from connectors.generic_sql import get_sqlalchemy_engine

    path = tmp_path / "w3err_mx.sqlite"
    sqlite3.connect(str(path)).close()
    missing = _raise(lambda: _run(get_sqlalchemy_engine({"type": "sqlite", "database": str(path)}), "select * from records"))
    _check("sqlite_missing_table", missing, "sqlite", "object.table_not_found", monkeypatch)
    bad = _raise(lambda: _run(get_sqlalchemy_engine({"type": "sqlite", "database": "/nonexistent/w3err/a.db"}), "select 1"))
    _check("sqlite_bad_path", bad, "sqlite", "object.file_not_found", monkeypatch)


def test_classifier_order_typed_then_structured_then_pattern():
    """Typed beats structured beats anchored text; a table named authors is not auth."""
    from services.error_taxonomy import DatawrapError, classify

    typed = classify(DatawrapError("capacity.disk_full"), side="destination")
    assert typed.code == "capacity.disk_full" and typed.matched_by == "typed"
    wrapped = classify(RuntimeError('(1062, "Duplicate entry \'1\' for key \'t.PRIMARY\'")'))
    assert wrapped.code == "value.duplicate_key" and wrapped.matched_by == "structured"
    anchored = classify(RuntimeError('relation "authors" does not exist'))
    assert anchored.code == "object.table_not_found" and anchored.matched_by == "pattern"
    unknown = classify(RuntimeError("weird widget glitch"))
    assert unknown.code == "unclassified" and "couldn't identify the cause" in unknown.message


def test_driver_sentence_strips_internals():
    from services.error_taxonomy import driver_sentence

    raw = (
        "(psycopg2.errors.UndefinedTable) relation \"records\" does not exist\n"
        "LINE 1: select * from records\n                      ^\n"
        "HINT:  Perhaps you meant \"record\".\nDETAIL:  Key (id)=(4) already exists.\n"
        "[SQL: select * from records where email = 'a@b.c']\n"
        "[parameters: {'email': 'pii@example.com'}]\n"
        "(Background on this error at: https://sqlalche.me/e/20/f405)"
    )
    sentence = driver_sentence(raw)
    assert sentence == 'relation "records" does not exist'
    host = driver_sentence(
        'connection to server at "10.2.3.4", port 5432 failed: FATAL:  password authentication failed for user "x"\n'
        'connection to server at "10.2.3.4", port 5432 failed: FATAL:  password authentication failed for user "x"'
    )
    assert host == 'password authentication failed for user "x"'
    mysql = driver_sentence("(1045, \"Access denied for user 'u'@'172.17.0.1' (using password: YES)\")")
    assert "172.17" not in mysql and "Access denied for user 'u'" in mysql
    long = driver_sentence("word " * 200)
    assert len(long) <= 221 and long.endswith("…") and not long[:-1].endswith(" wor")


def test_envelope_shape():
    from services.error_taxonomy import classify

    env = classify(RuntimeError('relation "orders" does not exist'), side="source").envelope("ref-1")
    err = env["error"]
    assert set(err) >= {"code", "message", "action", "object", "retriable", "reference"}
    assert set(err["action"]) == {"label", "ui", "api"}
    assert err["object"]["table"] == "orders" and err["object"]["side"] == "source"
    assert env["detail"].endswith("Reference: ref-1")
