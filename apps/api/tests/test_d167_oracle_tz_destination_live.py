"""D167 — loading a time-zone-aware timestamp INTO Oracle must verify.

The D155 read-back projected zone-bearing destination columns through
``TO_CHAR(col, '…TZH:TZM')``. Oracle rejects a TZH/TZM element on a
``TIMESTAMP WITH LOCAL TIME ZONE`` column (ORA-01821), so every row was written
and the Gate-8 read-back then failed and emptied the table.

Live Oracle (``DATAFLOW_ORACLE_*``, default 127.0.0.1:1521/FREEPDB1, system).
"""

from __future__ import annotations

import csv
import io
import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ORA = dict(
    host=os.getenv("DATAFLOW_ORACLE_HOST") or "127.0.0.1",
    port=int(os.getenv("DATAFLOW_ORACLE_PORT") or 1521),
    database=os.getenv("DATAFLOW_ORACLE_SERVICE") or "FREEPDB1",
    username=os.getenv("DATAFLOW_ORACLE_USER") or "system",
    password=os.getenv("DATAFLOW_ORACLE_PASSWORD") or "oracle",
)


def _up() -> bool:
    try:
        with socket.create_connection((ORA["host"], ORA["port"]), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _up(), reason="Oracle not reachable")


def _ora(sql: str, params: dict | None = None) -> list[tuple]:
    import oracledb

    conn = oracledb.connect(
        user=ORA["username"], password=ORA["password"],
        dsn=f"{ORA['host']}:{ORA['port']}/{ORA['database']}",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


ROWS = [
    {"id": "1", "ts": "2024-03-01T08:31:00+05:30"},
    {"id": "2", "ts": "2024-06-01T23:15:00-04:00"},
    {"id": "3", "ts": "2024-12-31T00:00:00+00:00"},
]


def _csv(rows) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["id", "ts"])
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode()


@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "upsert"])
def test_tz_aware_csv_column_loads_into_oracle_and_verifies(isolated, sync_mode):
    table = "D167_" + uuid.uuid4().hex[:8].upper()
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="tz.csv",
        source_content=_csv(ROWS),
        destination=EndpointConfig(kind="database", format="oracle", table=table, **ORA),
        sync_mode=sync_mode,
        stream_contracts=[
            {"name": "tz", "sync_mode": sync_mode, "primary_key": "id", "selected": True}
        ],
        validation_mode="strict",
    )
    job_id = "d167" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    try:
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        assert "ORA-01821" not in str(result.error or "")
        assert (result.reconciliation or {}).get("passed") is True
        assert _ora(f'SELECT COUNT(*) FROM "{table}"') == [(3,)]
    finally:
        try:
            _ora(f'DROP TABLE "{table}" PURGE')
        except Exception:  # noqa: BLE001
            pass
