"""Live Snowflake certification matrix — a real account, not fakesnow.

Skipped unless these are set (use a dedicated low-privilege role):
``DATAFLOW_SNOWFLAKE_LIVE_ACCOUNT`` (org-account), ``..._USER``, ``..._PASSWORD``,
``..._ROLE``, ``..._WAREHOUSE``, ``..._DATABASE``; optional ``..._SCHEMA``
(default PUBLIC).

Every cell drives ``UniversalTransferEngine.execute_tracked`` and then reads
the destination back with an independent Snowflake session, so a pass means
the rows really landed with the expected values — not only that Gate-8 said
so. Tables are uniquely named and dropped afterwards.
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_P = "DATAFLOW_SNOWFLAKE_LIVE_"
_REQUIRED = ("ACCOUNT", "USER", "PASSWORD", "ROLE", "WAREHOUSE", "DATABASE")
_MISSING = [k for k in _REQUIRED if not os.environ.get(_P + k)]

pytestmark = pytest.mark.skipif(
    bool(_MISSING), reason=f"live Snowflake not configured (missing {_P}{', '.join(_MISSING)})"
)


def _env(key: str, default: str = "") -> str:
    return os.environ.get(_P + key, default)


def _schema() -> str:
    return _env("SCHEMA", "PUBLIC")


def _sf(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="snowflake",
        host=_env("ACCOUNT"),
        port=443,
        database=_env("DATABASE"),
        schema=_schema(),
        table=table,
        username=_env("USER"),
        password=_env("PASSWORD"),
        warehouse=_env("WAREHOUSE"),
        auth_role=_env("ROLE"),
    )


def _connect(**over):
    from connectors.snowflake_conn import get_connection

    kw = dict(
        account=_env("ACCOUNT"),
        username=_env("USER"),
        password=_env("PASSWORD"),
        database=_env("DATABASE"),
        schema=_schema(),
        warehouse=_env("WAREHOUSE"),
        connection_string="",
        role=_env("ROLE"),
    )
    kw.update(over)
    return get_connection(**kw)


def _query(sql: str) -> list[tuple]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def _fq(table: str) -> str:
    from connectors.snowflake_conn import (
        resolve_or_fold_snowflake_table,
        snowflake_qualified_table,
    )

    conn = _connect()
    try:
        stored = resolve_or_fold_snowflake_table(conn.cursor(), _schema(), table)
        return snowflake_qualified_table(_schema(), stored)
    finally:
        conn.close()


@pytest.fixture()
def tables():
    created: list[str] = []

    def make(prefix: str) -> str:
        name = f"dwcert_{prefix}_{uuid.uuid4().hex[:8]}"
        created.append(name)
        return name

    yield make
    conn = _connect()
    try:
        cur = conn.cursor()
        for name in created:
            for candidate in (name.upper(), name):
                cur.execute(f'DROP TABLE IF EXISTS "{_schema()}"."{candidate}"')
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "sfcert" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _assert_verified(result, rows: int, *, count_only_ok: bool = False) -> dict:
    """Success, exact row count, and Gate-8 passed.

    A value proof needs both digests and equality. A server-side engine copy
    that proved only COUNT(*) may pass when ``count_only_ok`` — but it must
    never report a value match it did not measure.
    """
    assert result.success, result.error
    assert result.records_transferred == rows, result.records_transferred
    rec = result.reconciliation or {}
    assert rec.get("passed") is True, rec
    src, tgt = rec.get("source_checksum"), rec.get("target_checksum")
    if src:
        assert src == tgt, rec
    else:
        assert count_only_ok, f"no source value digest: {rec}"
        assert rec.get("checksum_match") is not True, rec
    return rec


# Representative type fixture: big integers, decimals, floats, unicode, empty
# string vs NULL, dates and timestamps. SQLite stores DECIMAL-affinity values as
# REAL, so decimals here stay within 15 significant digits; exact 38-digit
# decimals are proven through the CSV cell below.
_TYPED_ROWS = [
    (1, 9007199254740993, "1234567.891234567", 1.5, "plain", "2024-02-29", "2024-02-29 23:59:59.123456"),
    (2, -9223372036854775807, "-0.000000001", -2.25e-10, "Ünïcödé — 日本語 😀", "1970-01-01", "1970-01-01 00:00:00"),
    (3, 0, "0", 0.0, "", "9999-12-31", "2038-01-19 03:14:08"),
    (4, None, None, None, None, None, None),
    (5, 42, "1.10", 3.141592653589793, "quote \" comma , newline\nend", "2000-01-01", "2000-01-01 12:00:00.5"),
]


def _typed_sqlite(path: Path, rows=_TYPED_ROWS) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, big BIGINT, amount DECIMAL(38,9), "
            "ratio DOUBLE, label TEXT, d DATE, ts TIMESTAMP)"
        )
        conn.executemany("INSERT INTO t VALUES (?,?,?,?,?,?,?)", rows)
    conn.close()


def _sqlite_src(path: Path, table: str = "t") -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table=table)


def test_cert_connect_role_warehouse_and_ddl():
    row = _query("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")[0]
    assert row[0] == _env("ROLE").upper()
    assert row[1] == _env("WAREHOUSE").upper()
    assert row[2] == _env("DATABASE").upper()


def test_cert_wrong_password_is_reported_as_credentials_not_account():
    from connectors.snowflake_conn import SNOWFLAKE_BAD_PASSWORD_MSG, classify_snowflake_connect_error

    with pytest.raises(Exception) as exc:
        _connect(password="definitely-not-the-password-" + uuid.uuid4().hex[:6]).close()
    assert classify_snowflake_connect_error(str(exc.value)) == SNOWFLAKE_BAD_PASSWORD_MSG


def test_cert_unknown_account_is_reported_as_account_not_found():
    from connectors.snowflake_conn import SNOWFLAKE_ACCOUNT_NOT_FOUND_MSG, classify_snowflake_connect_error

    with pytest.raises(Exception) as exc:
        _connect(account="dwcertnoorg-" + uuid.uuid4().hex[:8]).close()
    assert classify_snowflake_connect_error(str(exc.value)) == SNOWFLAKE_ACCOUNT_NOT_FOUND_MSG


def test_cert_sqlite_to_snowflake_typed_round_trip(tmp_path, isolated, tables):
    src = tmp_path / "typed.sqlite"
    _typed_sqlite(src)
    table = tables("typed")
    result = _run(isolated, TransferRequest(
        source=_sqlite_src(src), destination=_sf(table),
        sync_mode="full_refresh_overwrite", validation_mode="strict", skip_preflight=True,
    ))
    _assert_verified(result, len(_TYPED_ROWS))

    fq = _fq(table)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {fq} ORDER BY 1")
        names = [d[0].lower() for d in cur.description]
        got = {r[0]: dict(zip(names, r)) for r in cur.fetchall()}
    finally:
        conn.close()
    assert sorted(got) == [1, 2, 3, 4, 5]
    assert int(got[1]["big"]) == 9007199254740993  # > 2^53: no float rounding
    assert int(got[2]["big"]) == -9223372036854775807
    assert Decimal(str(got[1]["amount"])) == Decimal("1234567.891234567")
    assert Decimal(str(got[2]["amount"])) == Decimal("-0.000000001")
    assert float(got[5]["ratio"]) == 3.141592653589793
    assert got[2]["label"] == "Ünïcödé — 日本語 😀"
    assert got[3]["label"] == "", "empty string must not become NULL"
    assert got[4]["label"] is None and got[4]["big"] is None and got[4]["amount"] is None
    assert got[5]["label"] == 'quote " comma , newline\nend'
    assert str(got[1]["d"]).startswith("2024-02-29")
    assert str(got[3]["d"]).startswith("9999-12-31")
    assert str(got[1]["ts"]).startswith("2024-02-29 23:59:59.123456")


def test_cert_snowflake_as_source_to_sqlite(tmp_path, isolated, tables):
    src = tmp_path / "typed.sqlite"
    _typed_sqlite(src)
    table = tables("srcrt")
    _assert_verified(_run(isolated, TransferRequest(
        source=_sqlite_src(src), destination=_sf(table),
        sync_mode="full_refresh_overwrite", skip_preflight=True,
    )), len(_TYPED_ROWS))

    dst = tmp_path / "back.sqlite"
    result = _run(isolated, TransferRequest(
        source=_sf(table), destination=_sqlite_src(dst, "back"),
        sync_mode="full_refresh_overwrite", skip_preflight=True,
    ))
    _assert_verified(result, len(_TYPED_ROWS))
    conn = sqlite3.connect(str(dst))
    names = [r[1].lower() for r in conn.execute("PRAGMA table_info(back)")]
    rows = {r[0]: dict(zip(names, r)) for r in conn.execute("SELECT * FROM back ORDER BY 1")}
    conn.close()
    assert sorted(int(k) for k in rows) == [1, 2, 3, 4, 5]
    by = {int(k): v for k, v in rows.items()}
    assert int(by[1]["big"]) == 9007199254740993
    assert by[2]["label"] == "Ünïcödé — 日本語 😀"
    assert by[3]["label"] == ""
    assert by[4]["label"] is None
    assert Decimal(str(by[1]["amount"])) == Decimal("1234567.891234567")


def test_cert_snowflake_to_snowflake_copy(tmp_path, isolated, tables, monkeypatch):
    src = tmp_path / "typed.sqlite"
    _typed_sqlite(src)
    a, b, c = tables("ssa"), tables("ssb"), tables("ssc")
    _assert_verified(_run(isolated, TransferRequest(
        source=_sqlite_src(src), destination=_sf(a),
        sync_mode="full_refresh_overwrite", skip_preflight=True,
    )), len(_TYPED_ROWS))
    # Default same-account copy runs inside Snowflake (INSERT SELECT/CTAS): its
    # only engine proof is COUNT(*), and Gate-8 must say so honestly.
    _assert_verified(_run(isolated, TransferRequest(
        source=_sf(a), destination=_sf(b),
        sync_mode="full_refresh_overwrite", skip_preflight=True,
    )), len(_TYPED_ROWS), count_only_ok=True)
    # With the engine digest enabled both populations are re-read: value proof.
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    _assert_verified(_run(isolated, TransferRequest(
        source=_sf(a), destination=_sf(c),
        sync_mode="full_refresh_overwrite", skip_preflight=True,
    )), len(_TYPED_ROWS))
    for other in (b, c):
        diff = _query(
            f"SELECT COUNT(*) FROM ((SELECT * FROM {_fq(a)} MINUS SELECT * FROM {_fq(other)}) "
            f"UNION ALL (SELECT * FROM {_fq(other)} MINUS SELECT * FROM {_fq(a)}))"
        )[0][0]
        assert diff == 0


def _csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["id", "amount", "status"])
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_cert_csv_to_snowflake_upsert_updates_and_inserts(isolated, tables):
    table = tables("upsert")
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv([
            {"id": "1", "amount": "1000.00", "status": "new"},
            {"id": "2", "amount": "2000.50", "status": "new"},
        ]),
        destination=_sf(table),
        sync_mode="upsert",
        stream_contracts=[{"name": "payments", "sync_mode": "upsert", "primary_key": "id", "selected": True}],
        skip_preflight=True,
    )
    _assert_verified(_run(isolated, req), 2)
    req.source_content = _csv([
        {"id": "1", "amount": "1111.00", "status": "paid"},
        {"id": "3", "amount": "3000.00", "status": "new"},
    ])
    r2 = _run(isolated, req)
    _assert_verified(r2, 2)
    got = {int(r[0]): (Decimal(str(r[1])), r[2]) for r in _query(
        f"SELECT * FROM {_fq(table)} ORDER BY 1"
    )}
    assert got == {
        1: (Decimal("1111.00"), "paid"),
        2: (Decimal("2000.50"), "new"),
        3: (Decimal("3000.00"), "new"),
    }


def test_cert_incremental_append_second_run_lands_only_the_delta(tmp_path, isolated, tables):
    src = tmp_path / "inc.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
        conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, i // 3, f"v{i}") for i in range(1, 31)])
    conn.close()
    table = tables("inc")

    def req():
        return TransferRequest(
            source=_sqlite_src(src), destination=_sf(table),
            sync_mode="incremental_append",
            stream_contracts=[{"name": "t", "cursor_field": "seq", "sync_mode": "incremental_append", "selected": True}],
            skip_preflight=True, validation_mode="strict",
        )

    r1 = _run(isolated, req())
    assert r1.success, r1.error
    assert _query(f"SELECT COUNT(*) FROM {_fq(table)}")[0][0] == 30

    conn = sqlite3.connect(str(src))
    with conn:
        conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, i // 3, f"v{i}") for i in range(31, 46)])
    conn.close()
    r2 = _run(isolated, req())
    assert r2.success, r2.error
    total, distinct = _query(f"SELECT COUNT(*), COUNT(DISTINCT $1) FROM {_fq(table)}")[0]
    assert (total, distinct) == (45, 45), "duplicate or missing ids after run 2"

    r3 = _run(isolated, req())  # quiet poll
    assert r3.success, r3.error
    assert _query(f"SELECT COUNT(*) FROM {_fq(table)}")[0][0] == 45


def test_cert_csv_exact_decimals_and_text_to_snowflake(isolated, tables):
    table = tables("csvdec")
    rows = [
        {"id": "1", "amount": "12345678901234.567890123", "status": "  padded  "},
        {"id": "2", "amount": "-0.000000001", "status": "Ünïcödé 😀"},
        {"id": "3", "amount": "99999999999999999999.999999999", "status": ""},
    ]
    result = _run(isolated, TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="decimals.csv",
        source_content=_csv(rows),
        destination=_sf(table),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    ))
    _assert_verified(result, 3)
    got = {int(r[0]): (r[1], r[2]) for r in _query(f"SELECT * FROM {_fq(table)} ORDER BY 1")}
    assert Decimal(str(got[1][0])) == Decimal("12345678901234.567890123")
    assert Decimal(str(got[2][0])) == Decimal("-0.000000001")
    assert Decimal(str(got[3][0])) == Decimal("99999999999999999999.999999999")
    assert got[2][1] == "Ünïcödé 😀"
