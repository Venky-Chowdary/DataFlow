"""YAML / fixed-width scale: layout-projected checksum, quarantine, dest COUNT.

2-row live sources landed on ``feature/yaml-fixed-width-live``. This file is
the next proof bar: the dirty fixture at a population that carries quarantine
and 10 KiB notes, through the product engine, with an independent destination
reread. 12,100 rows is always-on (sqlite). 100K Postgres is env-gated —
reduced-row runs are never presented as 100K evidence.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_quarantine import dlq_table_name  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402
from tests.scale import dirty_fixture as fixture  # noqa: E402

#: Smallest population that carries quarantine (row 11000) and a 10 KiB note
#: (row 9000). Not 100K — do not quote these cells as scale-matrix evidence.
SMOKE_ROWS = 12_100
SCALE_ROWS = 100_000

_SCALE_ENV = ("DATAFLOW_SCALE_YAML_FWF_100K", "DATAFLOW_SCALE_FILE_MATRIX")


def _scale_env_set() -> bool:
    return any(os.getenv(name, "").strip().lower() in {"1", "true", "yes"} for name in _SCALE_ENV)


def _mappings() -> list[dict[str, str]]:
    types = fixture.DEST_TYPES
    return [{"source": col, "target": col, "target_type": types[col]} for col in fixture.COLUMNS]


def _sqlite_records(db: Path, table: str) -> list[dict]:
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _sqlite_count(db: Path, table: str) -> int:
    conn = sqlite3.connect(str(db))
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if present is None:
            return 0
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()


def _run_file_to_sqlite(kind: str, path: Path, db: Path, table: str) -> object:
    request = TransferRequest(
        source=EndpointConfig(kind="file", format=kind),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(db), table=table
        ),
        source_path=str(path),
        source_filename=path.name,
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        skip_preflight=True,
        mappings=_mappings(),
        column_types=dict(fixture.DEST_TYPES),
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:16])


def test_fwf_layout_holds_timestamptz() -> None:
    widths = dict(fixture.FIXED_WIDTH_LAYOUT)
    assert widths["created_at"] >= fixture.FIXED_WIDTH_TIMESTAMPTZ_MIN_WIDTH
    rec = fixture.row_text(1)
    assert len(rec["created_at"]) == fixture.FIXED_WIDTH_TIMESTAMPTZ_MIN_WIDTH
    assert fixture.project_fixed_width_record(rec)["created_at"] == rec["created_at"]


def test_fwf_expected_checksum_projects_long_notes() -> None:
    rec = fixture.row_text(9_000)
    assert len(rec["note"]) == 10_000
    projected = fixture.project_fixed_width_record(rec)
    assert projected["note"] == rec["note"][:40]
    assert len(projected["note"]) == 40

    full, n_full = fixture.expected_checksum(9_000)
    fwf, n_fwf = fixture.expected_checksum(9_000, source_format="fixed_width")
    yaml_digest, n_yaml = fixture.expected_checksum(9_000, source_format="yaml")
    assert n_full == n_fwf == n_yaml == 9_000
    assert yaml_digest == full
    assert fwf != full


@pytest.mark.parametrize("kind", ["yaml", "fixed_width"])
def test_yaml_fwf_sqlite_quarantine_and_checksum(kind: str, tmp_path: Path) -> None:
    path = tmp_path / f"dirty_{kind}_{SMOKE_ROWS}{fixture.FORMATS[kind].suffix}"
    fixture.write_format(kind, path, SMOKE_ROWS)
    db = tmp_path / "dest.db"
    table = "ledger"
    result = _run_file_to_sqlite(kind, path, db, table)
    assert result.success, getattr(result, "error", result)

    expected_n = fixture.retained_row_count(SMOKE_ROWS)
    assert _sqlite_count(db, table) == expected_n
    assert _sqlite_count(db, dlq_table_name(table)) == fixture.quarantine_row_count(SMOKE_ROWS)

    records = _sqlite_records(db, table)
    if kind == "fixed_width":
        with fixture.empty_is_ambiguous():
            digest, hashed = fixture.checksum_rows(records)
            expected, expected_hashed = fixture.expected_checksum(
                SMOKE_ROWS, source_format=kind
            )
    else:
        digest, hashed = fixture.checksum_rows(records)
        expected, expected_hashed = fixture.expected_checksum(
            SMOKE_ROWS, source_format=kind
        )
    assert hashed == expected_n
    assert hashed == expected_hashed
    assert digest == expected


def _pg_dest(table: str) -> EndpointConfig:
    creds = pg_creds()
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=str(creds["host"]),
        port=int(creds["port"]),
        database=str(creds["database"]),
        username=str(creds["username"]),
        password=str(creds["password"]),
        schema="public",
        table=table,
    )


def _pg_connect():
    import psycopg2

    creds = pg_creds()
    return psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
        connect_timeout=10,
    )


def _pg_drop(table: str) -> None:
    conn = _pg_connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
            cur.execute(f'DROP TABLE IF EXISTS public."{dlq_table_name(table)}" CASCADE')
    finally:
        conn.close()


def _pg_count(table: str) -> int:
    conn = _pg_connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'public."{table}"',))
            if cur.fetchone()[0] is None:
                return 0
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _pg_checksum(table: str) -> tuple[str, int]:
    cols = list(fixture.COLUMNS)
    quoted = ", ".join(f'"{c}"' for c in cols)
    conn = _pg_connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'SELECT {quoted} FROM public."{table}"')
            records = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
    return fixture.checksum_rows(records)


def _run_file_to_pg(kind: str, path: Path, table: str) -> object:
    request = TransferRequest(
        source=EndpointConfig(kind="file", format=kind),
        destination=_pg_dest(table),
        source_path=str(path),
        source_filename=path.name,
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        skip_preflight=True,
        mappings=_mappings(),
        column_types=dict(fixture.DEST_TYPES),
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:16])


@pytest.mark.skipif(not _scale_env_set(), reason="100K yaml/fwf is env-gated")
@pytest.mark.skipif(not pg_up(), reason="Postgres not authenticated")
@pytest.mark.parametrize("kind", ["yaml", "fixed_width"])
def test_yaml_fwf_100k_postgres(kind: str, tmp_path: Path) -> None:
    """100K dest COUNT + checksum on a connection the writer never used.

    Reduced-row runs must not be quoted as this cell.
    """
    path = tmp_path / f"dirty_{kind}_{SCALE_ROWS}{fixture.FORMATS[kind].suffix}"
    fixture.write_format(kind, path, SCALE_ROWS)
    table = f"yaml_fwf_100k_{kind}_{uuid.uuid4().hex[:8]}"
    _pg_drop(table)
    try:
        result = _run_file_to_pg(kind, path, table)
        assert result.success, getattr(result, "error", result)
        expected_n = fixture.retained_row_count(SCALE_ROWS)
        assert _pg_count(table) == expected_n
        assert _pg_count(dlq_table_name(table)) == fixture.quarantine_row_count(SCALE_ROWS)
        if kind == "fixed_width":
            with fixture.empty_is_ambiguous():
                digest, hashed = _pg_checksum(table)
                expected, expected_hashed = fixture.expected_checksum(
                    SCALE_ROWS, source_format=kind
                )
        else:
            digest, hashed = _pg_checksum(table)
            expected, expected_hashed = fixture.expected_checksum(
                SCALE_ROWS, source_format=kind
            )
        assert hashed == expected_n == expected_hashed
        assert digest == expected
    finally:
        _pg_drop(table)
