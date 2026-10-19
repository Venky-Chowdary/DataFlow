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

# X-33 loop repro recipe: decimal cast WITHOUT relying on null_if.
RECIPE_X33 = {
    "steps": [
        {"op": "trim", "column": "note", "options": {}},
        {"op": "case", "column": "note", "options": {"mode": "upper"}},
        {"op": "cast_column", "column": "qty", "options": {"to_type": "integer"}},
        {
            "op": "cast_column",
            "column": "amount",
            "options": {"to_type": "decimal", "precision": 12, "scale": 4},
        },
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
                    # X-33: empty and whitespace-only cells were miscounted
                    # as changed source keys by the drift detector.
                    (5, "5", "1.25", "   "),
                    # Multibyte (NFC 'Zo\u00eb') — loop's repro seed.
                    (6, "9", "2.50", "Zo\u00eb"),
                ],
            )
        conn.commit()
    finally:
        conn.close()


def _request(
    src_table: str, dst_db: Path, recipe: dict | None = None
) -> TransferRequest:
    recipe = recipe if recipe is not None else RECIPE
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
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=COLS
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
    assert res.records_transferred == 6

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
    # trim('   ') -> '' survives (not folded to NULL); X-1 whitespace fidelity.
    assert rows[4] == (5, 5, Decimal("1.25"), ""), rows
    assert rows[5] == (6, 9, Decimal("2.50"), "ZO\u00cb"), rows


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


def test_mysql_to_duckdb_dest_tamper_names_column_not_drift(
    monkeypatch, tmp_path
):
    """X-33: tamper the DESTINATION after the write (before verify) —
    the proof must fail naming the tampered column, not fall back to the
    phantom 'source changed during run' verdict seen live (NULL/''/'   '
    rows were miscounted as changed source keys).
    """
    import src.transfer.reconcile_step as rs

    real = rs._run_reconciliation_with_rules

    def tampered(*args, **kwargs):
        con = duckdb.connect(str(dst_db))
        # Loop's repro: ASCII overwrite of the multibyte 'Zo\u00eb' at id 6.
        con.execute("UPDATE orders_out SET note='Zoe' WHERE id=6")
        con.close()
        return real(*args, **kwargs)

    monkeypatch.setattr(
        rs, "_run_reconciliation_with_rules", tampered
    )

    table = "cell33_tamper_src"
    _seed(table)
    dst_db = tmp_path / "cell33_tamper.duckdb"
    res = UniversalTransferEngine().execute_tracked(
        _request(table, dst_db, recipe=RECIPE_X33), "0" * 24
    )
    assert not res.success, "dest tamper must fail the proof"
    recon = res.reconciliation or {}
    proof = recon.get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    reason = str(proof.get("reason") or "")
    assert "source changed during run" not in reason, proof
    culprit = proof.get("culprit_columns") or []
    assert "note" in culprit, proof


def test_x4_time_semantics_agree_across_drivers():
    """X-4: TIME read-back differs per driver — pymysql returns timedelta for
    MySQL TIME, duckdb/psycopg return datetime.time. Gate-8's fingerprint must
    canonicalize both to the same wall-clock text or every cross-engine TIME
    cell would report false drift."""
    from datetime import time as dtime, timedelta

    from services.reconciliation import normalize_cell

    assert normalize_cell(timedelta(hours=1, minutes=23, seconds=45), ddl_type="TIME") == "01:23:45"
    assert normalize_cell(dtime(1, 23, 45), ddl_type="TIME") == "01:23:45"
    assert normalize_cell(timedelta(hours=1, minutes=23, seconds=45)) == \
        normalize_cell(dtime(1, 23, 45))
    assert normalize_cell(dtime(9, 8, 7, 654321)) == "09:08:07.654321"
    assert normalize_cell(timedelta(hours=9, minutes=8, seconds=7, microseconds=654321)) == \
        "09:08:07.654321"
    # distinct times still differ — no over-equation
    assert normalize_cell(dtime(1, 23, 45)) != normalize_cell(dtime(1, 23, 46))


def test_mysql_to_sqlite_dest_tamper_names_column_not_drift(
    monkeypatch, tmp_path
):
    """X-33 second non-PG destination: same dest-tamper on a SQLite file
    must fail naming the column, never 'source changed during run'."""
    import src.transfer.reconcile_step as rs

    real = rs._run_reconciliation_with_rules

    def tampered(*args, **kwargs):
        con = sqlite3.connect(str(dst_db))
        con.execute("UPDATE orders_out SET note='Zoe' WHERE id=6")
        con.commit()
        con.close()
        return real(*args, **kwargs)

    monkeypatch.setattr(
        rs, "_run_reconciliation_with_rules", tampered
    )

    table = "cell33_lite_src"
    _seed(table)
    dst_db = tmp_path / "cell33_tamper.sqlite"
    req = _request(table, dst_db, recipe=RECIPE_X33)
    req.destination = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dst_db),
        table="orders_out",
    )
    res = UniversalTransferEngine().execute_tracked(req, "0" * 24)
    assert not res.success, "dest tamper must fail the proof"
    recon = res.reconciliation or {}
    proof = recon.get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    reason = str(proof.get("reason") or "")
    assert "source changed during run" not in reason, proof
    culprit = proof.get("culprit_columns") or []
    assert "note" in culprit, proof


def test_x33_loop_repro_dest_tamper_on_trimmed_column(
    monkeypatch, tmp_path
):
    """X-33 loop repro verbatim: tamper the TRIMMED column on the
    destination after write. Keys 1 and 4 (whitespace-padded names) were
    miscounted as 'changed in source during run' — the verdict must be
    failed with culprit column 'name', never 'source changed'.
    """
    import src.transfer.reconcile_step as rs

    real = rs._run_reconciliation_with_rules

    def tampered(*args, **kwargs):
        con = duckdb.connect(str(dst_db))
        con.execute("UPDATE out SET name='Zoe' WHERE id=3")
        con.close()
        return real(*args, **kwargs)

    monkeypatch.setattr(
        rs, "_run_reconciliation_with_rules", tampered
    )

    table = "x33_loop_src"
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` ("
                "id INT PRIMARY KEY, name VARCHAR(64), qty VARCHAR(16), "
                "amt VARCHAR(24), note VARCHAR(64))"
            )
            cur.executemany(
                f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s)",
                [
                    (1, "  Ada  ", "7", "10.505", "keep"),
                    (2, "Bob", "-3", "0.00", ""),
                    (3, "Zo\u00eb", "2147483647", "99999999.9999", None),
                    (4, "  ", "0", "-1.5", "   "),
                    (5, None, None, None, "x"),
                ],
            )
        conn.commit()
    finally:
        conn.close()

    dst_db = tmp_path / "x33_loop.duckdb"
    recipe = {
        "steps": [
            {"op": "trim", "column": "name", "options": {}},
            {"op": "case", "column": "note", "options": {"mode": "upper"}},
            {
                "op": "cast_column",
                "column": "qty",
                "options": {"to_type": "integer"},
            },
            {
                "op": "cast_column",
                "column": "amt",
                "options": {
                    "to_type": "decimal",
                    "precision": 12,
                    "scale": 4,
                },
            },
        ]
    }
    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", table=table, **MYSQL
        ),
        destination=EndpointConfig(
            kind="database",
            format="duckdb",
            database=str(dst_db),
            table="out",
        ),
        source_filename=table,
        mappings=[
            {
                "source": c,
                "target": c,
                "confidence": 1.0,
                "user_override": True,
                "transform": "none",
            }
            for c in ["id", "name", "qty", "amt", "note"]
        ],
        shape_recipe=recipe,
        approved_shape_recipe_hash=__import__(
            "services.shape_models", fromlist=["ShapeRecipe"]
        ).ShapeRecipe.parse(
            recipe,
            source_columns=["id", "name", "qty", "amt", "note"],
        ).recipe_hash,
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
    assert not res.success, "dest tamper must fail the proof"
    proof = (res.reconciliation or {}).get("transform_applied") or {}
    reason = str(proof.get("reason") or "")
    assert "source changed during run" not in reason, proof
    assert proof.get("status") == "failed", proof
    culprit = proof.get("culprit_columns") or []
    assert "name" in culprit, proof


def _time_request(
    src_table: str, dst_db: Path, dest_table: str = "out"
) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", table=src_table, **MYSQL
        ),
        destination=EndpointConfig(
            kind="database",
            format="duckdb",
            database=str(dst_db),
            table=dest_table,
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
            for c in ["id", "t"]
        ],
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


def test_x34_time6_micros_to_duckdb(tmp_path):
    """MySQL TIME(6) -> DuckDB TIME: micros preserved, verified/full_checksum.

    Regression: the routing key 'generic_sql' resolved 0 temporal digits for
    a bare DuckDB TIME column and Gate-8 restated it TIME(0), truncating the
    microseconds DuckDB stores natively (X-34).
    """
    table = "x34_src"
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (id INT PRIMARY KEY, t TIME(6))"
            )
            cur.executemany(
                f"INSERT INTO `{table}` VALUES (%s,%s)",
                [
                    (1, "01:23:45"),
                    (2, "00:00:00.123456"),
                    (3, "23:59:59.999999"),
                    (4, None),
                    (5, "09:05:00"),
                ],
            )
        conn.commit()
    finally:
        conn.close()

    dst = tmp_path / "x34.duckdb"
    res = UniversalTransferEngine().execute_tracked(
        _time_request(table, dst), "0" * 24
    )
    assert res.success, res.error

    import datetime as _dt

    con = duckdb.connect(str(dst))
    rows = con.execute("SELECT id, t FROM out ORDER BY id").fetchall()
    con.close()
    assert rows[1] == (2, _dt.time(0, 0, 0, 123456)), rows
    assert rows[2] == (3, _dt.time(23, 59, 59, 999999)), rows
    assert rows[3] == (4, None), rows


def test_x34_time6_dest_tamper_names_column(monkeypatch, tmp_path):
    """Dest tamper on a TIME(6) column post-write: failed, culprit 't'."""
    import src.transfer.reconcile_step as rs

    real = rs._run_reconciliation_with_rules

    def tampered(*args, **kwargs):
        con = duckdb.connect(str(dst))
        con.execute("UPDATE out SET t = TIME '00:00:01' WHERE id = 2")
        con.close()
        return real(*args, **kwargs)

    monkeypatch.setattr(rs, "_run_reconciliation_with_rules", tampered)

    table = "x34_tamper_src"
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (id INT PRIMARY KEY, t TIME(6))"
            )
            cur.executemany(
                f"INSERT INTO `{table}` VALUES (%s,%s)",
                [(1, "01:23:45"), (2, "00:00:00.123456"), (3, "09:05:00")],
            )
        conn.commit()
    finally:
        conn.close()

    dst = tmp_path / "x34_tamper.duckdb"
    res = UniversalTransferEngine().execute_tracked(
        _time_request(table, dst), "0" * 24
    )
    assert not res.success, "dest tamper must fail the proof"
    recon = res.reconciliation or {}
    blob = str(recon)
    assert "source changed during run" not in blob, recon
    mismatches = (recon.get("sample_compare") or {}).get("mismatches") or []
    named = {str(m.get("target") or m.get("column") or "") for m in mismatches}
    assert "t" in named or "column t" in str(recon.get("message") or ""), recon


def test_x35_out_of_range_time_typed_finding_and_drop(tmp_path):
    """MySQL TIME '-01:00:00' / '100:00:00' -> 0-24h TIME carrier.

    The destination engine cannot hold a negative or >24h wall-clock TIME;
    the run must end in a typed pre-write finding (quarantine), not a raw
    driver error, and the table this run created must be dropped (X-35).
    """
    table = "x35_src"
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (id INT PRIMARY KEY, t TIME)"
            )
            cur.executemany(
                f"INSERT INTO `{table}` VALUES (%s,%s)",
                [
                    (1, "-01:00:00"),
                    (2, "100:00:00"),
                    (3, "12:00:00"),
                ],
            )
        conn.commit()
    finally:
        conn.close()

    dst = tmp_path / "x35.duckdb"
    res = UniversalTransferEngine().execute_tracked(
        _time_request(table, dst), "0" * 24
    )
    assert not res.success, (
        "out-of-range TIME must be a typed finding, not a silent bind"
    )
    blob = " ".join(
        str(p)
        for p in [
            res.error,
            (res.reconciliation or {}),
            getattr(res, "rejected_rows", None),
            getattr(res, "summary", None),
        ]
    )
    assert "invalid input syntax" not in blob, blob
    # The destination table this run created must be dropped.
    con = duckdb.connect(str(dst))
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    con.close()
    assert "out" not in tables, tables
