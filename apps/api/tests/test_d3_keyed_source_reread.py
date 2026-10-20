"""D3: keyed incremental modes must earn independent source re-read proof.

incremental_append / incremental_deduped stayed on the write-pass digest
(migration_proven unreachable) because ``should_reread_source`` declines every
incremental run. When the written batch is key-coverable (writer id stash or
the reconcile sample covers it — the same bound the destination keyed
read-back uses), a cursor-scoped re-read digests exactly the delta this run
wrote, so Gate-8 can compare like-for-like and earn ``source_reread``.

Runs live MySQL -> Snowflake (fakesnow) through the tracked engine — the
same route family as the production keyed Snowflake jobs (skips when the
local MySQL is unreachable).
"""

from __future__ import annotations

import sys
import uuid
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

_TABLE = "d3_reread_src"

PG = None


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
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


@pytest.fixture()
def mysql_source():
    import pymysql

    try:
        conn = pymysql.connect(
            host=MYSQL["host"],
            port=MYSQL["port"],
            user=MYSQL["username"],
            password=MYSQL["password"],
            database=MYSQL["database"],
            connect_timeout=2,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"local MySQL (127.0.0.1:3306) unreachable: {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {_TABLE}")
            cur.execute(
                f"CREATE TABLE {_TABLE} ("
                "id INT PRIMARY KEY, seq INT NOT NULL, v VARCHAR(64))"
            )
        conn.commit()
        yield conn
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        conn.commit()
    finally:
        conn.close()


def _insert(mysql_source, rows):
    with mysql_source.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {_TABLE} (id, seq, v) VALUES (%s, %s, %s)", rows
        )
    mysql_source.commit()


def _request(sync_mode: str = "incremental_append") -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", table=_TABLE, **MYSQL
        ),
        destination=EndpointConfig(
            kind="database",
            format="snowflake",
            host="localhost",
            port=443,
            database="dataflow",
            username="test",
            password="test",
            schema="public",
            table=f"D3_REREAD_DST_{uuid.uuid4().hex[:8]}",
        ),
        sync_mode=sync_mode,
        stream_contracts=[
            {
                "name": _TABLE,
                "cursor_field": "id",
                "sync_mode": sync_mode,
                "primary_key": "id",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc

    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)


def _run(sync_mode: str = "incremental_append"):
    job_id = "d3" + uuid.uuid4().hex[:16]
    return UniversalTransferEngine().execute_tracked(
        _request(sync_mode), job_id
    )


def test_incremental_append_earns_source_reread(mysql_source, isolated):
    pytest.importorskip("fakesnow")
    _insert(mysql_source, [(i, i, f"v{i}") for i in range(1, 31)])

    first = _run()
    assert first.success, first.error
    assert first.destination_summary.get("checksum_mode") == "source_reread", (
        f"keyed incremental stayed on write-pass: "
        f"{first.destination_summary.get('checksum_mode')}"
    )
    assert first.destination_summary.get("source_independently_reread") is True

    _insert(mysql_source, [(i, i, f"v{i}") for i in range(31, 41)])
    second = _run()
    assert second.success, second.error
    assert second.destination_summary.get("checksum_mode") == "source_reread"
    recon = second.reconciliation or {}
    assert recon.get("source_checksum_provenance") != "write_pass", (
        recon.get("source_checksum_provenance")
    )


def test_incremental_deduped_earns_source_reread(mysql_source, isolated):
    pytest.importorskip("fakesnow")
    _insert(mysql_source, [(i, i, f"v{i}") for i in range(1, 21)])
    res = _run(sync_mode="incremental_deduped")
    assert res.success, res.error
    assert res.destination_summary.get("checksum_mode") == "source_reread", (
        res.destination_summary.get("checksum_mode")
    )
    assert res.destination_summary.get("source_independently_reread") is True


def test_oversized_batch_keeps_honest_write_pass(mysql_source, isolated):
    pytest.importorskip("fakesnow")
    """A batch larger than the keyed read-back stash cannot scope the dest
    re-read — the run must keep the write-pass digest, not fake coverage."""
    _insert(mysql_source, [(i, i, f"v{i}") for i in range(1, 601)])
    res = _run()
    assert res.success, res.error
    mode = res.destination_summary.get("checksum_mode")
    assert mode in {"inline_write_pass", "writer_last_batch"}, mode
