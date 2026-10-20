"""X-133 — a clean incremental_append no-op run must never name culprits.

On an append-family destination a business key legitimately appears once per
written version (id2/id3 each exist twice after a source update lands as a new
row). A whole-table key census that joins on the PK alone reads that history
as "value changed" and stamped culprits onto a no-op poll where nothing was
written and the destination did not move. The reconcile must report
``no_op_destination_unchanged`` with no culprit text at all.
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "amount", "target": "amount", "confidence": 1.0},
    {"source": "note", "target": "note", "confidence": 1.0},
    {"source": "updated_at", "target": "updated_at", "confidence": 1.0},
]

ROWS_V1 = [
    (1, "1000.00", "a", "2026-01-01 10:00:00"),
    (2, "2000.50", "b", "2026-01-01 10:00:00"),
    (3, "3000.00", "c", "2026-01-01 10:00:00"),
]
ROWS_V2 = [
    (5, "5000.00", "e", "2026-01-02 00:00:00"),
]


def _contract(table: str) -> list[dict]:
    return [
        {
            "name": table,
            "sync_mode": "incremental_append",
            "primary_key": "id",
            "selected": True,
            "cursor_field": "updated_at",
        }
    ]


def _duckdb_dest(tmp_path: Path) -> tuple[EndpointConfig, Path]:
    db_path = tmp_path / f"{uuid.uuid4().hex[:8]}.duckdb"
    return (
        EndpointConfig(kind="database", format="duckdb", database=str(db_path), table="out"),
        db_path,
    )


def _run(engine, source: EndpointConfig, dest: EndpointConfig, table: str):
    req = TransferRequest(
        source=source,
        destination=dest,
        sync_mode="incremental_append",
        skip_preflight=False,
        validation_mode="balanced",
        stream_contracts=_contract(table),
        mappings=list(MAPPINGS),
    )
    return engine.execute_tracked(req, uuid.uuid4().hex[:24])


def _recon(result) -> dict:
    return result.reconciliation or (result.destination_summary or {}).get(
        "reconciliation", {}
    )


def _sqlite_source(tmp_path: Path, table: str) -> EndpointConfig:
    db = tmp_path / f"{table}.db"
    conn = sqlite3.connect(db)
    conn.execute(
        f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, amount TEXT, '
        "note TEXT, updated_at TEXT)"
    )
    conn.executemany(
        f'INSERT INTO "{table}" VALUES (?,?,?,?)', ROWS_V1 + ROWS_V2
    )
    conn.commit()
    conn.close()
    return EndpointConfig(
        kind="database", format="sqlite", database=str(db), table=table
    )


def test_append_noop_run_names_no_culprits_sqlite_source(tmp_path, monkeypatch):
    """SQLite → DuckDB: update id2, append run 2, then a no-op run 3."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "dw"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    table = "x133_src"
    source = _sqlite_source(tmp_path, table)
    dest, db_path = _duckdb_dest(tmp_path)
    engine = UniversalTransferEngine()

    r1 = _run(engine, source, dest, table)
    assert r1.success, r1.error

    # Source update + insert: run 2 appends a second version of id 2/3.
    db = tmp_path / f"{table}.db"
    conn = sqlite3.connect(db)
    conn.execute(f'UPDATE "{table}" SET amount = amount || "" WHERE id = 2')
    conn.execute(
        f'UPDATE "{table}" SET amount = ?, updated_at = ? WHERE id = 2',
        ("2000.51", "2026-01-03 00:00:00"),
    )
    conn.execute(
        f'UPDATE "{table}" SET note = NULL, updated_at = ? WHERE id = 3',
        ("2026-01-03 00:00:00",),
    )
    conn.executemany(f'INSERT INTO "{table}" VALUES (?,?,?,?)', [(6, "6000.00", "d", "2026-01-03 00:00:00")])
    conn.commit()
    conn.close()

    r2 = _run(engine, source, dest, table)
    assert r2.success, r2.error

    r3 = _run(engine, source, dest, table)
    assert r3.success, r3.error
    recon = _recon(r3)
    assert recon.get("passed"), recon.get("message")
    assert not recon.get("culprit_keys"), (
        "a no-op append poll named culprits on legitimately-versioned rows: "
        f"{recon.get('culprit_keys')}"
    )
    assert "Culprit key(s)" not in str(recon.get("message") or "")


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="dataflow",
            user="dataflow",
            password="dataflow",
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


def test_append_noop_run_names_no_culprits_pg_source(tmp_path, monkeypatch):
    """PG → DuckDB: same no-op cell on a warehouse source."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "dw"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    table = f"x133_pg_{uuid.uuid4().hex[:8]}"
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f'CREATE TABLE public."{table}" (id int PRIMARY KEY, amount numeric(12,2), '
                "note text, updated_at timestamptz)"
            )
            for row in ROWS_V1 + ROWS_V2:
                cur.execute(f'INSERT INTO public."{table}" VALUES (%s,%s,%s,%s)', row)
    finally:
        conn.close()

    source = EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )
    dest, _db_path = _duckdb_dest(tmp_path)
    engine = UniversalTransferEngine()
    try:
        assert _run(engine, source, dest, table).success
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'UPDATE public."{table}" SET amount = amount + 1, '
                    "updated_at = '2026-01-03 00:00:00' WHERE id IN (2,3)"
                )
                cur.execute(
                    f'INSERT INTO public."{table}" VALUES (6, 6000.00, \'f\', '
                    "'2026-01-03 00:00:00')"
                )
        finally:
            conn.close()
        assert _run(engine, source, dest, table).success
        r3 = _run(engine, source, dest, table)
        assert r3.success, r3.error
        recon = _recon(r3)
        assert recon.get("passed"), recon.get("message")
        assert not recon.get("culprit_keys")
        assert "Culprit key(s)" not in str(recon.get("message") or "")
    finally:
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        finally:
            conn.close()
