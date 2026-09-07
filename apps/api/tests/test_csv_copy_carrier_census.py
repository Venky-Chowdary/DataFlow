"""File identity COPY censuses every text cell against its declared carrier.

``COPY FROM STDIN`` / ``LOAD DATA`` parse the whole file all-or-nothing: one
``not-a-number`` in an INTEGER column aborted the load, nothing was quarantined
and no destination table remained. The census declines the fast path *before*
the destination is touched, so the row path validates and quarantines the cell
(balanced) or fails closed (strict) exactly as it does for a database source.
"""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_csv_local import try_copy_local_csv  # noqa: E402
from services.copy_fast_path import text_cell_copy_safe  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

BAD_CSV = b"id,age\n1,30\n2,not-a-number\n3,42\n"
GOOD_CSV = b"id,age\n1,30\n2,31\n3,42\n"


def _mappings() -> list[dict]:
    return [
        {"source": "id", "target": "id", "confidence": 0.95, "target_type": "integer"},
        {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
    ]


def _port_up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.mark.parametrize(
    ("value", "logical", "safe"),
    [
        (None, "integer", True),
        ("30", "integer", True),
        ("-7", "integer", True),
        ("30.0", "integer", False),
        ("not-a-number", "integer", False),
        ("1e3", "integer", False),
        ("0.016666668", "decimal", True),
        ("-1.5e-3", "decimal", True),
        ("1,234.5", "decimal", False),
        ("abc", "float", False),
        ("1", "boolean", True),
        ("true", "boolean", False),
        ("2024-06-01", "date", True),
        ("06/01/2024", "date", False),
        ("2024-06-01T10:30:00Z", "datetime", True),
        ("2024-06-01 10:30:00.123456+05:30", "datetime", True),
        ("yesterday", "datetime", False),
        ("10:30", "time", True),
        ("10:30:00.5", "time", True),
        ("10.30", "time", False),
        ("anything at all", "string", True),
        ("anything at all", "", True),
    ],
)
def test_text_cell_copy_safe_matches_engine_parsers(value, logical, safe):
    assert text_cell_copy_safe(value, logical) is safe


def test_sqlite_fast_path_declines_before_destination_exists(tmp_path):
    db = tmp_path / "dest.db"
    result = try_copy_local_csv(
        content=BAD_CSV,
        filename="users.csv",
        file_type="csv",
        dest_type="sqlite",
        dest_cfg={"format": "sqlite", "database": str(db), "table": "users"},
        dest_table="users",
        dest_schema="",
        mappings=_mappings(),
        schema={"id": "TEXT", "age": "TEXT"},
        effective_sync="full_refresh_overwrite",
    )
    assert result is None
    if db.exists():
        conn = sqlite3.connect(db)
        try:
            names = [
                r[0]
                for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            ]
        finally:
            conn.close()
        assert "users" not in names


def test_sqlite_fast_path_keeps_clean_population(tmp_path):
    db = tmp_path / "dest.db"
    result = try_copy_local_csv(
        content=GOOD_CSV,
        filename="users.csv",
        file_type="csv",
        dest_type="sqlite",
        dest_cfg={"format": "sqlite", "database": str(db), "table": "users"},
        dest_table="users",
        dest_schema="",
        mappings=_mappings(),
        schema={"id": "TEXT", "age": "TEXT"},
        effective_sync="full_refresh_overwrite",
    )
    assert result is not None
    written, _ddl, summary, _ = result
    assert written == 3
    assert summary.get("copy_fast_path") == "used"


def _pg_request(table: str, mode: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            table=table,
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
        ),
        source_filename="users.csv",
        source_content=BAD_CSV,
        sync_mode="full_refresh_overwrite",
        validation_mode=mode,
        skip_preflight=True,
        mappings=_mappings(),
    )


@pytest.mark.skipif(not _port_up(5432), reason="PostgreSQL not reachable on 127.0.0.1:5432")
def test_csv_postgres_balanced_quarantines_instead_of_copy_abort():
    psycopg2 = pytest.importorskip("psycopg2")
    table = f"census_{uuid.uuid4().hex[:8]}"
    result = UniversalTransferEngine().execute(_pg_request(table, "balanced"))
    summary = result.destination_summary or {}
    assert result.success, result.error
    assert summary.get("rejected_rows") == 1
    assert summary.get("copy_fast_path") != "used"
    dlq = summary.get("dest_quarantine") or {}
    assert dlq.get("ok") is True and dlq.get("rows_written") == 1, dlq
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, user="dataflow", password="dataflow", dbname="dataflow"
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, age FROM public."{table}" ORDER BY id')
            assert cur.fetchall() == [(1, 30), (3, 42)]
            cur.execute(
                f'SELECT "_df_row", "_df_column", "_df_value" FROM public."{dlq["table"]}"'
            )
            assert cur.fetchall() == [("2", "age", "not-a-number")]
            cur.execute(f'DROP TABLE public."{table}"; DROP TABLE public."{dlq["table"]}"')
        conn.commit()
    finally:
        conn.close()


@pytest.mark.skipif(not _port_up(5432), reason="PostgreSQL not reachable on 127.0.0.1:5432")
def test_csv_postgres_strict_fails_closed_with_zero_rows():
    psycopg2 = pytest.importorskip("psycopg2")
    table = f"census_{uuid.uuid4().hex[:8]}"
    result = UniversalTransferEngine().execute(_pg_request(table, "strict"))
    summary = result.destination_summary or {}
    assert not result.success
    assert "not-a-number" in str(result.error)
    assert "invalid input syntax" not in str(result.error).lower()
    assert summary.get("rejected_rows") == 1
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, user="dataflow", password="dataflow", dbname="dataflow"
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(1), 0) FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name=%s",
                (table,),
            )
            exists = int(cur.fetchone()[0])
            if exists:
                cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
                assert int(cur.fetchone()[0]) == 0
                cur.execute(f'DROP TABLE public."{table}"')
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name LIKE %s",
                (f"{table}%quarantine",),
            )
            for (dlq,) in cur.fetchall():
                cur.execute(f'DROP TABLE public."{dlq}"')
        conn.commit()
    finally:
        conn.close()
