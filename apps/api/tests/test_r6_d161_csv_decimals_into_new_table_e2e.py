"""D161 (CSV arm) — CSV decimals from SFTP and S3 load into a new table, exactly.

QA (production 557f556c): "a declared DOUBLE is not an IEEE/Excel float
artifact". Round 4 fixed it for Kafka, relational and Mongo sources, whose wire
types are declared. A CSV declares nothing, so a CSV decimal column into a new
table was still held at G4 by the IEEE/Excel-float chip.

What the chip is for: a cell like ``0.30000000000000004`` looks like binary
float noise (an Excel export of 0.1 + 0.2). Ordinary decimal text (``1.10``,
``2.5``, ``3.14159``, ``1e-7``) is not. And on a CSV the text *is* the value:
an exact DECIMAL sized from the text holds ``0.30000000000000004`` as written,
so the chip is a note on such a column, not a reason to hold the load.

Before: a column holding one residue-shaped cell next to ``0.1`` / ``0.7`` was
invented as FLOAT/DOUBLE, and its ``ieee_float_artifact`` chip capped the
mapping at 0.70, under the G4 floor; the transfer was refused.

This drives QA's route (MCP ``start_transfer`` -> ``/api/v1/copilot/confirm``
-> job) from the local SFTP test server and from S3 (moto's HTTP server, the
real boto3 client) into a new PostgreSQL, MySQL and SQL Server table, then a
second run into the table the first run created. Every value is checked cell by
cell: decimals exact (``1.10`` stays ``1.10``), NULL, '' and unicode kept.
"""

from __future__ import annotations

import csv
import io
import uuid
from decimal import Decimal

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

_HEADER = ["id", "price", "ratio", "measure", "tiny", "label"]
#: price: money text with significant trailing zeros; ratio: Excel binary-float
#: residue next to ordinary decimals; measure / tiny: plain and E-notation
#: decimals. ``None`` is an unquoted empty cell (NULL); '' is a quoted empty.
_ROWS = [
    ("1", "1.10", "0.30000000000000004", "3.14159", "1e-7", "a"),
    ("2", "2.50", "0.1", "0.1", "2.5e-3", None),
    ("3", None, "0.7", "2.71828", None, ""),
    ("4", "19.99", "0.2", "1.5", "0.000001", "Zoë 東京 ✓"),
    ("5", "0.30", None, "10.25", "1E-7", "  padded  "),
    ("6", "-7.05", "1.0000000000000002", "-0.5", "3e-7", "x"),
]
_DECIMAL_COLS = ("price", "ratio", "measure", "tiny")


def _csv_text() -> str:
    buf = io.StringIO()
    buf.write(",".join(_HEADER) + "\n")
    for row in _ROWS:
        cells = []
        for value in row:
            if value is None:
                cells.append("")
            elif value == "" or value != value.strip() or not value.isascii():
                cells.append('"' + value.replace('"', '""') + '"')
            else:
                cells.append(value)
        buf.write(",".join(cells) + "\n")
    return buf.getvalue()


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _sftp_source(tmp_path):
    pytest.importorskip("paramiko")
    from services.connector_store import create_connector
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "sftp"
    root.mkdir()
    (root / "measures.csv").write_text(_csv_text(), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    cfg = server.endpoint_config("/measures.csv")
    conn = create_connector({
        "name": "d161-sftp-" + uuid.uuid4().hex[:6], "type": "sftp", "role": "source",
        "host": cfg["host"], "port": cfg["port"], "username": cfg["username"],
        "password": cfg["password"], "database": cfg["database"],
        "host_key": cfg["host_key"], "workspace_id": "",
    })
    return str(conn.id), "measures.csv", runner.stop


def _s3_source(_tmp_path):
    boto3 = pytest.importorskip("boto3")
    moto_server = pytest.importorskip("moto.server")
    from services.connector_store import create_connector

    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test",
                      aws_secret_access_key="test", region_name="us-east-1")
    bucket = "d161-" + uuid.uuid4().hex[:8]
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="d161/measures.csv", Body=_csv_text().encode("utf-8"))
    conn = create_connector({
        "name": "d161-s3-" + uuid.uuid4().hex[:6], "type": "s3", "role": "source",
        "host": host, "port": int(port), "database": bucket, "username": "test",
        "password": "test", "ssl": False, "extra": {"path_style": True},
        "workspace_id": "",
    })
    return str(conn.id), "d161/measures.csv", server.stop


_SOURCES = {"sftp": _sftp_source, "s3": _s3_source}


def _column_types(kind: str, table: str) -> dict[str, str]:
    if kind == "postgresql":
        sql = ("SELECT column_name, data_type FROM information_schema.columns "
               f"WHERE table_schema = 'public' AND table_name = '{table}'")
    elif kind == "mysql":
        sql = ("SELECT column_name, column_type FROM information_schema.columns "
               f"WHERE table_schema = DATABASE() AND table_name = '{table}'")
    else:
        sql = ("SELECT column_name, data_type FROM information_schema.columns "
               f"WHERE table_name = '{table}'")
    return {str(r[0]).lower(): str(r[1]).lower() for r in h.query(kind, sql)}


def _landed(kind: str, table: str) -> dict[int, tuple]:
    cols = ", ".join(h.quote(kind, c) for c in _HEADER)
    rows = h.query(kind, f"SELECT {cols} FROM {h.quote(kind, table)}")  # nosec B608
    return {int(r[0]): tuple(r[1:]) for r in rows}


def _assert_exact(kind: str, table: str) -> None:
    got = _landed(kind, table)
    assert sorted(got) == [int(r[0]) for r in _ROWS], got
    for row in _ROWS:
        rid = int(row[0])
        cells = dict(zip(_HEADER[1:], got[rid]))
        for col, text in zip(_HEADER[1:], row[1:]):
            value = cells[col]
            if col in _DECIMAL_COLS:
                if text is None:
                    assert value is None, (rid, col, value)
                    continue
                # An exact decimal: no binary rounding (0.1 is not
                # 0.1000000000000000055511151231257827) and no float repr.
                assert isinstance(value, Decimal), (rid, col, type(value), value)
                assert value == Decimal(text), (rid, col, value, text)
                if col == "price":
                    # Money text keeps the scale it was written with.
                    assert str(value) == text, (rid, col, str(value), text)
            else:
                assert value == text, (rid, col, value, text)


def _stage(client, args: dict) -> dict:
    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    assert staged.get("ack_id"), staged
    return staged


def _plan_mappings(staged: dict) -> dict[str, dict]:
    plan = staged.get("plan") or staged
    rows = plan.get("mappings") or staged.get("engine_mappings") or []
    return {str(m.get("source")): m for m in rows if isinstance(m, dict)}


@pytest.mark.parametrize("dest", ["postgresql", "mysql", "sqlserver"])
@pytest.mark.parametrize("source", sorted(_SOURCES))
def test_csv_decimals_load_into_a_new_table_exactly(client, tmp_path, source, dest):
    cfg = h.dest_config(dest)
    from services.connector_store import create_connector

    dst = create_connector({"name": f"d161-{dest}-{uuid.uuid4().hex[:6]}",
                            "role": "destination", "workspace_id": "", "ssl": False, **cfg})
    src_id, obj, stop = _SOURCES[source](tmp_path)
    table = f"d161_csv_{uuid.uuid4().hex[:8]}"
    h.drop_table(dest, table)
    args = {"source_connector_id": src_id, "source_table": obj,
            "dest_connector_id": str(dst.id), "dest_table": table,
            "sync_mode": "full_refresh_overwrite", "primary_key": "id"}
    try:
        staged = _stage(client, args)
        mappings = _plan_mappings(staged)
        for col in _DECIMAL_COLS:
            m = mappings.get(col) or {}
            # Ordinary decimals never carry the chip; the residue column may
            # carry it as a note, and no chip holds the mapping.
            kinds = {r.get("kind") for r in m.get("create_new_risks") or []}
            if col != "ratio":
                assert not any("ieee" in str(k) for k in kinds), (col, m)
            assert not m.get("requires_review"), (col, m)
        ratio_kinds = {r.get("kind") for r in mappings["ratio"].get("create_new_risks") or []}
        assert "ieee_float_artifact" not in ratio_kinds, mappings["ratio"]
        h.confirm(client, staged["ack_id"], reason="d161")

        types = _column_types(dest, table)
        for col in _DECIMAL_COLS:
            assert any(t in types[col] for t in ("numeric", "decimal")), (col, types)
        _assert_exact(dest, table)

        # Second run into the table the first run created.
        h.confirm(client, _stage(client, args)["ack_id"], reason="d161-run2")
        _assert_exact(dest, table)
    finally:
        stop()
        h.drop_table(dest, table)
        h.drop_table(dest, f"{table}_df_quarantine")
