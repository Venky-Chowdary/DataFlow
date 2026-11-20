"""D306/D307 on an append — exact decimals and JSON null survive later runs.

Live PostgreSQL -> JSONL on the in-process SFTP server, incremental_append on
``seq`` with key ``id``. The last row of run 1 (the watermark row) has a SQL
NULL ``payload`` (jsonb) and the row before it the exact DECIMAL(18,6)
``99999999999.999999``.

Run 2 re-reads the watermark tie group. The delivered-peer dedupe must
recognise the row it wrote (explicit ``null`` key, exact decimal) and append
only the two new ids (R1); run 3 appends nothing. Every line keeps the
``payload`` key, NULL as JSON null and ``{}`` as ``{}`` (D307, R2), and the
decimal stays exact in every line (D306, R3). Id 8 holds a JSON ``null``
value: it is written as the same ``null`` and verifies too.
"""

from __future__ import annotations

import json
import uuid
from decimal import Decimal

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.sftp_test_server import start_sftp_server
from tests.test_x5_file_destination_continuity import _s3_dest, s3  # noqa: F401


def _pg():
    try:
        import psycopg2
    except ImportError as exc:
        pytest.fail(f"psycopg2 is required: {exc}")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
        )
    except Exception as exc:
        pytest.fail(f"PostgreSQL 5432 is required: {exc}")
    conn.autocommit = True
    return conn


def _insert(cur, table, rows):
    from psycopg2 import sql

    stmt = sql.SQL("INSERT INTO public.{} (id, seq, amount, payload) VALUES (%s, %s, %s, %s::jsonb)").format(
        sql.Identifier(table)
    )
    cur.executemany(stmt, rows)


def _lines(text: str) -> list[dict]:
    return [json.loads(line, parse_float=Decimal) for line in text.splitlines() if line.strip()]


def _table(cur):
    from psycopg2 import sql

    table = "d306_append_" + uuid.uuid4().hex[:8]
    cur.execute(
        sql.SQL(
            "CREATE TABLE public.{} (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, "
            "amount NUMERIC(18,6), payload JSONB)"
        ).format(sql.Identifier(table))
    )
    rows = [(i, i, Decimal(f"{i}.000001"), '{"k": %d}' % i) for i in range(1, 8)]
    rows.append((8, 8, Decimal("8.000001"), "null"))  # a JSON null value, not SQL NULL
    rows.append((9, 9, Decimal("99999999999.999999"), "{}"))
    rows.append((10, 10, Decimal("-0.000001"), None))
    _insert(cur, table, rows)
    return table


def _three_runs(cur, table: str, dest: EndpointConfig, read_back) -> None:
    src = EndpointConfig(
        kind="database", format="postgresql", table=table, schema="public",
        host="127.0.0.1", port=5432, database="dataflow",
        username="dataflow", password="dataflow",
    )

    def run():
        req = TransferRequest(
            source=src,
            destination=dest,
            sync_mode="incremental_append",
            stream_contracts=[{"name": table, "sync_mode": "incremental_append", "selected": True,
                               "cursor_field": "seq", "primary_key": ["id"]}],
            skip_preflight=True,
            validation_mode="strict",
        )
        return UniversalTransferEngine().execute_tracked(req, "d306a-" + uuid.uuid4().hex[:12])

    r1 = run()
    assert r1.success, r1.error
    _insert(cur, table, [(11, 11, Decimal("0.100000"), None), (12, 12, Decimal("1.5"), '{"k": null}')])
    r2 = run()
    assert r2.success, r2.error
    assert r2.records_transferred == 2
    r3 = run()
    assert r3.success, r3.error

    text = read_back()
    lines = _lines(text)
    assert sorted(int(r["id"]) for r in lines) == list(range(1, 13))
    by_id = {int(r["id"]): r for r in lines}
    assert all("payload" in r for r in lines)
    assert by_id[9]["amount"] == Decimal("99999999999.999999")
    assert by_id[8]["payload"] is None
    assert by_id[9]["payload"] == {}
    assert by_id[10]["amount"] == Decimal("-0.000001")
    assert by_id[10]["payload"] is None
    assert by_id[11]["amount"] == Decimal("0.100000")
    assert by_id[11]["payload"] is None
    assert by_id[12]["payload"] == {"k": None}
    assert "100000000000" not in text and "e-" not in text.lower()


def _drop(cur, conn, table: str) -> None:
    from psycopg2 import sql

    cur.execute(sql.SQL("DROP TABLE IF EXISTS public.{}").format(sql.Identifier(table)))
    conn.close()


def test_sftp_jsonl_append_keeps_exact_decimal_and_null_and_never_repeats_the_boundary(tmp_path):
    conn = _pg()
    cur = conn.cursor()
    table = _table(cur)
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
        _three_runs(cur, table, dest, lambda: (root / "out.jsonl").read_text())
    finally:
        runner.stop()
        _drop(cur, conn, table)


def test_s3_jsonl_append_keeps_exact_decimal_and_null_and_never_repeats_the_boundary(s3):
    client, url, port = s3
    conn = _pg()
    cur = conn.cursor()
    table = _table(cur)
    try:
        dest = _s3_dest(url, port, "out.jsonl")
        _three_runs(
            cur, table, dest,
            lambda: client.get_object(Bucket="x5bucket", Key="out.jsonl")["Body"].read().decode(),
        )
    finally:
        _drop(cur, conn, table)
