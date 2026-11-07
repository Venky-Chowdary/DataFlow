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
# Lexically a perfect integer; physically outside every 64-bit carrier.
OVERFLOW = "99999999999999999999"
OVERFLOW_CSV = f"id,age\n1,30\n2,{OVERFLOW}\n3,42\n".encode()


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


@pytest.mark.parametrize(
    ("value", "logical", "physical", "dest_db", "safe"),
    [
        # Syntax passes; the physical carrier decides.
        (OVERFLOW, "integer", "BIGINT", "postgresql", False),
        (OVERFLOW, "integer", "BIGINT", "mysql", False),
        (OVERFLOW, "integer", "INTEGER", "sqlite", False),
        ("9223372036854775807", "integer", "BIGINT", "postgresql", True),
        ("-9223372036854775808", "integer", "BIGINT", "mysql", True),
        ("3000000000", "integer", "INTEGER", "postgresql", False),
        ("3000000000", "integer", "INTEGER", "sqlite", True),
        # No physical type → lexical contract only (row path still owns fit).
        (OVERFLOW, "integer", "", "postgresql", True),
        # Bounded decimal: NUMBER(11,8) rounds a 9-scale value — decline.
        ("0.016666668", "decimal(11,8)", "NUMERIC(11,8)", "postgresql", False),
        ("0.016666668", "decimal(11,8)", "DECIMAL(11,8)", "mysql", False),
        ("0.01666666", "decimal(11,8)", "NUMERIC(11,8)", "postgresql", True),
        ("1234.12345678", "decimal(11,8)", "NUMERIC(11,8)", "postgresql", False),
        # Unbounded decimal / float carriers hold any canonical number.
        ("0.016666668", "decimal", "NUMERIC", "postgresql", True),
        ("0.016666668", "float", "DOUBLE PRECISION", "postgresql", True),
        ("0.016666668", "decimal", "TEXT", "sqlite", True),
    ],
)
def test_text_cell_copy_safe_grades_physical_capacity(
    value, logical, physical, dest_db, safe
):
    assert (
        text_cell_copy_safe(value, logical, physical=physical, dest_db=dest_db)
        is safe
    )


def test_sqlite_fast_path_declines_integer_overflow(tmp_path):
    db = tmp_path / "dest.db"
    result = try_copy_local_csv(
        content=OVERFLOW_CSV,
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


@pytest.mark.parametrize(
    ("stamp", "dest_db", "expected"),
    [
        ("integer", "postgresql", "BIGINT"),
        ("integer", "mysql", "BIGINT"),
        ("integer", "sqlite", "INTEGER"),
        ("integer", "sqlserver", "BIGINT"),
        # Physical carriers are already bounded — never rewritten.
        ("INTEGER", "postgresql", "INTEGER"),
        ("SMALLINT", "mysql", "SMALLINT"),
        # No fixed integer column on these engines — the stamp stays unbounded.
        ("integer", "mongodb", "integer"),
        ("integer", "snowflake", "integer"),
        ("integer", "oracle", "integer"),
        ("integer", "", "integer"),
        ("decimal", "postgresql", "decimal"),
        ("", "postgresql", ""),
    ],
)
def test_fingerprint_remap_grades_bare_integer_against_physical_carrier(
    stamp, dest_db, expected
):
    from connectors.writer_common import physical_integer_carrier

    assert physical_integer_carrier(stamp, dest_db) == expected


def test_fingerprint_remap_holds_out_the_same_overflow_row_as_the_writer():
    """Gate-8's source digest must exclude exactly what the write quarantined,
    or a correct balanced load fails on two opaque hashes."""
    from connectors.writer_common import map_rows_for_fingerprint

    mapped, rejected = map_rows_for_fingerprint(
        headers=["id", "age"],
        data_rows=[["1", "30"], ["2", OVERFLOW], ["3", "42"]],
        mappings=_mappings(),
        target_cols=["id", "age"],
        column_types={"id": "integer", "age": "integer"},
        error_policy="quarantine",
        dest_types={"id": "integer", "age": "integer"},
        dest_kind="postgresql",
    )
    assert [row[0] for row in mapped] == [1, 3]
    assert len(rejected) == 1 and rejected[0]["column"] == "age"
    assert "does not fit" in str(rejected[0]["reason"])


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


def _pg_request(table: str, mode: str, content: bytes = BAD_CSV) -> TransferRequest:
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
        source_content=content,
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


def _mysql_request(table: str, mode: str, content: bytes) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="mysql",
            table=table,
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
        ),
        source_filename="users.csv",
        source_content=content,
        sync_mode="full_refresh_overwrite",
        validation_mode=mode,
        skip_preflight=True,
        mappings=_mappings(),
    )


def _assert_overflow_outcome(result, mode: str) -> dict:
    """One contract for every engine: the overflow row is a structured, durable
    quarantine finding — never a native bulk-loader error — and balanced mode
    lands the two good rows with Gate-8 green."""
    summary = result.destination_summary or {}
    err = str(result.error or "")
    assert "out of range for type bigint" not in err.lower()
    assert "load data" not in err.lower()
    assert summary.get("copy_fast_path") != "used"
    assert summary.get("rejected_rows") == 1, summary
    dlq = summary.get("dest_quarantine") or {}
    assert dlq.get("ok") is True and dlq.get("rows_written") == 1, dlq
    if mode == "strict":
        assert not result.success
        assert "does not fit" in err, err
    else:
        assert result.success, err
        assert result.records_transferred == 2
        assert "checksum mismatch" not in err.lower()
    return summary


@pytest.mark.skipif(not _port_up(5432), reason="PostgreSQL not reachable on 127.0.0.1:5432")
@pytest.mark.parametrize("mode", ["balanced", "strict"])
def test_csv_postgres_integer_overflow_is_quarantined_not_copy_aborted(mode):
    psycopg2 = pytest.importorskip("psycopg2")
    table = f"census_{uuid.uuid4().hex[:8]}"
    result = UniversalTransferEngine().execute(_pg_request(table, mode, OVERFLOW_CSV))
    summary = _assert_overflow_outcome(result, mode)
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, user="dataflow", password="dataflow", dbname="dataflow"
    )
    try:
        with conn.cursor() as cur:
            if mode == "balanced":
                cur.execute(f'SELECT id, age FROM public."{table}" ORDER BY id')
                assert cur.fetchall() == [(1, 30), (3, 42)]
            dlq_table = summary["dest_quarantine"]["table"]
            cur.execute(f'SELECT "_df_row", "_df_column", "_df_value" FROM public."{dlq_table}"')
            assert cur.fetchall() == [("2", "age", OVERFLOW)]
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"; DROP TABLE public."{dlq_table}"')
        conn.commit()
    finally:
        conn.close()


@pytest.mark.skipif(not _port_up(3306), reason="MySQL not reachable on 127.0.0.1:3306")
@pytest.mark.parametrize("mode", ["balanced", "strict"])
def test_csv_mysql_integer_overflow_is_quarantined_not_load_aborted(mode):
    pymysql = pytest.importorskip("pymysql")
    table = f"census_{uuid.uuid4().hex[:8]}"
    result = UniversalTransferEngine().execute(_mysql_request(table, mode, OVERFLOW_CSV))
    summary = _assert_overflow_outcome(result, mode)
    conn = pymysql.connect(
        host="127.0.0.1", port=3306, user="dataflow", password="dataflow", database="dataflow"
    )
    try:
        with conn.cursor() as cur:
            if mode == "balanced":
                cur.execute(f"SELECT id, age FROM `{table}` ORDER BY id")
                assert [tuple(r) for r in cur.fetchall()] == [(1, 30), (3, 42)]
            dlq_table = summary["dest_quarantine"]["table"]
            cur.execute(f"SELECT `_df_row`, `_df_column`, `_df_value` FROM `{dlq_table}`")
            assert [tuple(r) for r in cur.fetchall()] == [("2", "age", OVERFLOW)]
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(f"DROP TABLE `{dlq_table}`")
        conn.commit()
    finally:
        conn.close()
