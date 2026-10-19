"""Cell 31: MySQL -> DuckDB shaped transfer with typed recipe, native
read-back and tamper check — a non-PG pair (per breadth counter rules:
real transfer, shaped recipe, native destination read-back, tamper).

Recipe ops are typed (trim, case upper, cast to integer, null_if) so the
proof runs them as declared transforms, and the destination is verified
through the engine's own reconcile — not just a row count.
"""

from __future__ import annotations

import sqlite3  # noqa: F401  (keeps parity with other cell tests)
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

duckdb = pytest.importorskip("duckdb")

MYSQL = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}

COLS = ["id", "qty", "amount", "note"]

RECIPE = {
    "steps": [
        {"op": "trim", "column": "note", "options": {}},
        {"op": "case", "column": "note", "options": {"mode": "upper"}},
        {"op": "cast_column", "column": "qty", "options": {"to_type": "integer"}},
        {"op": "null_if", "column": "amount", "options": {"values": ["0.00"]}},
    ]
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
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


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


def _seed(table: str) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` ("
                "id INT PRIMARY KEY, qty VARCHAR(16), "
                "amount DECIMAL(10,2), note VARCHAR(64))"
            )
            cur.executemany(
                f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s)",
                [
                    (1, "7", "10.50", "  alpha "),
                    (2, "0", "0.00", "beta"),
                    (3, "1000000", "9999999.99", "  gamma delta  "),
                    (4, None, None, None),
                ],
            )
        conn.commit()
    finally:
        conn.close()


def _request(src_table: str, dst_db: Path) -> TransferRequest:
    from services.shape_models import ShapeRecipe

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", table=src_table, **MYSQL
        ),
        destination=EndpointConfig(
            kind="database",
            format="duckdb",
            database=str(dst_db),
            table="orders_out",
        ),
        sync_mode="full_refresh_overwrite",
        mappings=[
            {
                "source": c,
                "target": c,
                "confidence": 1.0,
                "user_override": True,
                "transform": "none",
            }
            for c in COLS
        ],
        shape_recipe=RECIPE,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            RECIPE, source_columns=COLS
        ).recipe_hash,
        stream_contracts=[
            {
                "name": src_table,
                "primary_key": "id",
                "sync_mode": "full_refresh_overwrite",
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def test_mysql_to_duckdb_shaped_transfer_with_native_readback(tmp_path):
    table = "cell31_src"
    _seed(table)
    dst = tmp_path / "cell31.duckdb"

    res = UniversalTransferEngine().execute_tracked(
        _request(table, dst), "0" * 24
    )
    assert res.success, res.error
    assert res.records_transferred == 4

    # Native read-back on the destination.
    con = duckdb.connect(str(dst))
    rows = con.execute(
        "SELECT id, qty, amount, note FROM orders_out ORDER BY id"
    ).fetchall()
    con.close()

    from decimal import Decimal

    assert rows[0] == (1, 7, Decimal("10.50"), "ALPHA"), rows
    assert rows[1][0] == 2 and rows[1][1] == 0, rows
    # null_if 0.00 -> NULL
    assert rows[1][2] is None and rows[1][3] == "BETA", rows
    assert rows[2] == (3, 1000000, Decimal("9999999.99"), "GAMMA DELTA"), rows
    assert rows[3][0] == 4 and rows[3][1] is None and rows[3][3] is None, rows


def test_mysql_to_duckdb_tamper_is_caught(monkeypatch, tmp_path):
    """Corrupt one shaped value in flight — the proof must fail closed."""
    from services import shape_apply as shape_apply_mod

    real_matrix = shape_apply_mod.ShapeRunner.matrix

    def corrupt_qty(self, headers, rows):
        out_headers, out_rows = real_matrix(self, headers, rows)
        idx = out_headers.index("qty")
        return out_headers, [
            [
                (70 if i == idx and v == 7 else v)
                for i, v in enumerate(r)
            ]
            for r in out_rows
        ]

    monkeypatch.setattr(shape_apply_mod.ShapeRunner, "matrix", corrupt_qty)

    table = "cell31_tamper_src"
    _seed(table)
    dst = tmp_path / "cell31_tamper.duckdb"
    res = UniversalTransferEngine().execute_tracked(
        _request(table, dst), "0" * 24
    )
    assert not res.success, "tampered write must fail the proof"
    recon = res.reconciliation or {}
    proof = recon.get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    culprit = proof.get("culprit_columns") or []
    assert "qty" in culprit, proof
