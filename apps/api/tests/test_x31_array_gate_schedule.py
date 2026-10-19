"""X-31: array_strategy_gate_issues on the schedule path.

Interactive transfers run preflight, which blocks normalize/hybrid array
policies missing a child_table_spec. A SCHEDULED run skips preflight and goes
straight to the writer — before this fix the contract check only fired inside
flush_normalized_child_batches, AFTER parent rows were committed, so the run
half-wrote the parent table and then failed.

Regression: a run with struct_policy=normalize and no child_table_spec must
fail BEFORE any parent INSERT — the destination table must not even be
created.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

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

MYSQL = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = "0" * 24
        self.jobs[job_id] = job_data
        return job_id

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields)
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)


def _mysql_conn():
    import pymysql

    try:
        return pymysql.connect(
            host=MYSQL["host"],
            port=MYSQL["port"],
            user=MYSQL["username"],
            password=MYSQL["password"],
            database=MYSQL["database"],
            connect_timeout=2,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"local MySQL (127.0.0.1:3306) unreachable: {exc}")


def test_x31_bad_child_spec_blocks_before_parent_write(tmp_path):
    table = "x31_array_src"
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` ("
                "id INT PRIMARY KEY, tags JSON)"
            )
            cur.executemany(
                f"INSERT INTO `{table}` VALUES (%s,%s)",
                [(1, '["a","b"]'), (2, '["c"]')],
            )
        conn.commit()
    finally:
        conn.close()

    dst = tmp_path / "x31.sqlite"
    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", table=table, **MYSQL
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst),
            table="orders_out",
        ),
        source_filename=table,
        mappings=[
            {
                "source": "id",
                "target": "id",
                "confidence": 1.0,
                "user_override": True,
                "transform": "none",
            },
            {
                "source": "tags",
                "target": "tags",
                "confidence": 1.0,
                "user_override": True,
                "transform": "none",
                # schedule path: operator saved normalize without approving a
                # child table contract — no child_table_spec attached.
                "struct_policy": "normalize",
            },
        ],
        stream_contracts=[
            {
                "name": table,
                "primary_key": "id",
                "sync_mode": "full_refresh_overwrite",
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )
    res = UniversalTransferEngine().execute_tracked(req, "0" * 24)
    assert not res.success, "missing child_table_spec must fail the run"
    err = str(getattr(res, "error", "") or "") + " " + str(
        getattr(res, "errors", "") or ""
    ) + " " + str(getattr(res, "reconciliation", "") or "")
    assert "child_table_spec" in err or "ARRAY strategy" in err, (
        f"failure must name the contract gate, got: {err[:400]}"
    )
    # The gate fired BEFORE the parent write — no half-written table.
    if dst.exists():
        con = sqlite3.connect(str(dst))
        try:
            n = con.execute(
                "SELECT count(*) FROM sqlite_master "
                "WHERE type='table' AND name='orders_out'"
            ).fetchone()[0]
        finally:
            con.close()
        assert n == 0, "parent table must not be created on gate failure"
