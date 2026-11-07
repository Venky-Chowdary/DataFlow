"""D144 (JSONL arm) — a JSONL export keeps NULL keys and writes DECIMAL exactly.

Live PostgreSQL / MySQL / SQL Server source -> JSONL on the in-process SFTP
server and on the file export, through ``execute_tracked``.

Before: the object-store/SFTP writer dropped every key whose value was SQL
NULL (``{"id": 2}``), and DECIMAL cells binary64 could hold were demoted to
floats (``0.1000000000`` -> ``0.1``, ``DECIMAL(38,0)`` ``1`` -> ``1.0``) while
the ones it could not hold were quoted text — one column, two JSON types.
"""

from __future__ import annotations

import json
import os
import socket
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_MSSQL_PW = os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD") or os.getenv("MSSQL_SA_PASSWORD") or ""
_ROWS = (
    "(1, 1234567890.0123456789, 'x', 12345678901234567890123456789), "
    "(2, NULL, NULL, NULL), (3, 0.1000000000, '', 1)"
)


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


def _pg(t):
    import psycopg2

    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE public."{t}" (id INT PRIMARY KEY, amt NUMERIC(20,10), note TEXT, big NUMERIC(38,0))')
    cur.execute(f'INSERT INTO public."{t}" VALUES {_ROWS}')
    ep = EndpointConfig(kind="database", format="postgresql", table=t, schema="public", host="127.0.0.1",
                        port=5432, database="dataflow", username="dataflow", password="dataflow")
    return ep, lambda: (cur.execute(f'DROP TABLE IF EXISTS public."{t}"'), conn.close())


def _mysql(t):
    import pymysql

    conn = pymysql.connect(host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
                           database="dataflow", autocommit=True)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE `{t}` (id INT PRIMARY KEY, amt DECIMAL(20,10), note TEXT, big DECIMAL(38,0))")
    cur.execute(f"INSERT INTO `{t}` VALUES {_ROWS}")
    ep = EndpointConfig(kind="database", format="mysql", table=t, host="127.0.0.1", port=3306,
                        database="dataflow", username="dataflow", password="dataflow")
    return ep, lambda: (cur.execute(f"DROP TABLE IF EXISTS `{t}`"), conn.close())


def _mssql(t):
    import pyodbc

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
        f"UID=sa;PWD={_MSSQL_PW};TrustServerCertificate=yes", autocommit=True, timeout=10,
    )
    conn.execute(f"CREATE TABLE [{t}] (id INT PRIMARY KEY, amt DECIMAL(20,10), note NVARCHAR(200), big DECIMAL(38,0))")
    conn.execute(f"INSERT INTO [{t}] VALUES {_ROWS}")
    ep = EndpointConfig(kind="database", format="mssql", table=t, host="127.0.0.1", port=1433,
                        database="dataflow", username="sa", password=_MSSQL_PW,
                        extra={"trust_server_certificate": True})
    return ep, lambda: (conn.execute(f"DROP TABLE IF EXISTS [{t}]"), conn.close())


_SOURCES = [
    pytest.param(_pg, marks=pytest.mark.skipif(not _reachable(5432), reason="no PostgreSQL"), id="postgresql"),
    pytest.param(_mysql, marks=pytest.mark.skipif(not _reachable(3306), reason="no MySQL"), id="mysql"),
    pytest.param(_mssql, marks=pytest.mark.skipif(not (_reachable(1433) and _MSSQL_PW), reason="no SQL Server"),
                 id="sqlserver"),
]


@pytest.fixture()
def isolated(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _assert_exact_jsonl(text: str) -> None:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) == 3, text
    by_id = {}
    for line in lines:
        rec = json.loads(line, parse_float=Decimal)
        by_id[int(rec["id"])] = (rec, line)
    rec2, _ = by_id[2]
    # Every column keeps its key; SQL NULL is JSON null.
    assert set(rec2) == {"id", "amt", "note", "big"}, by_id[2][1]
    assert rec2["amt"] is None and rec2["note"] is None and rec2["big"] is None
    rec1, line1 = by_id[1]
    rec3, line3 = by_id[3]
    # Exact JSON numbers with the source's digits and scale, never floats or text.
    assert '"amt": 1234567890.0123456789' in line1, line1
    assert '"big": 12345678901234567890123456789' in line1, line1
    assert '"amt": 0.1000000000' in line3, line3
    assert '"big": 1}' in line3 or '"big": 1,' in line3, line3
    assert rec3["note"] == ""
    assert rec1["note"] == "x"


@pytest.mark.parametrize("make_source", _SOURCES)
def test_sftp_jsonl_keeps_null_keys_and_exact_decimals(tmp_path: Path, isolated, make_source):
    from tests.sftp_test_server import start_sftp_server

    src, cleanup = make_source("d144j_" + uuid.uuid4().hex[:8])
    root = tmp_path / "srv"
    root.mkdir()
    server, runner = start_sftp_server(str(root))
    try:
        cfg = server.endpoint_config("/out.jsonl")
        dest = EndpointConfig(
            kind="database", format="sftp", host=cfg["host"], port=cfg["port"],
            username=cfg["username"], password=cfg["password"], database=cfg["database"],
            table=cfg["table"], extra={"host_key": cfg["host_key"]},
        )
        job_id = "d144j" + uuid.uuid4().hex[:10]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(source=src, destination=dest, sync_mode="overwrite"), job_id
        )
        assert result.success, result.error
        _assert_exact_jsonl((root / "out.jsonl").read_text(encoding="utf-8"))
    finally:
        runner.stop()
        cleanup()


@pytest.mark.parametrize("make_source", _SOURCES)
def test_file_export_jsonl_writes_exact_decimals(isolated, make_source):
    src, cleanup = make_source("d144f_" + uuid.uuid4().hex[:8])
    try:
        job_id = "d144f" + uuid.uuid4().hex[:10]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(source=src, destination=EndpointConfig(kind="file_export", format="jsonl")),
            job_id,
        )
        assert result.success, result.error
        path = (result.destination_summary or {}).get("path")
        assert path and os.path.exists(path), result.destination_summary
        _assert_exact_jsonl(Path(path).read_text(encoding="utf-8"))
    finally:
        cleanup()
