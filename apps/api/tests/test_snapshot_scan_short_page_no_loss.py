"""A short snapshot-scan page must not be read as "source drained".

A held scan hands back whatever the driver buffered — DuckDB returns one
vector (2048 rows) regardless of the page size we asked for. Treating that as
the end of the source truncates the load *and* stays green: the destination
read-back, the row count and the checksum all agree on the rows that landed.
"""

from __future__ import annotations

import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ROW_COUNT = 5000  # more than one DuckDB vector


def test_duckdb_scan_spanning_vectors_transfers_every_row():
    duckdb = pytest.importorskip("duckdb")

    src_path = f"{tempfile.gettempdir()}/duckdb_scan_src_{uuid.uuid4().hex[:8]}.duck"
    dst_path = f"{tempfile.gettempdir()}/duckdb_scan_dst_{uuid.uuid4().hex[:8]}.duck"
    src_table = f"scan_src_{uuid.uuid4().hex[:8]}"
    dst_table = f"scan_dst_{uuid.uuid4().hex[:8]}"

    conn = duckdb.connect(src_path)
    conn.execute(f'CREATE TABLE "{src_table}" (id INTEGER PRIMARY KEY, note VARCHAR)')
    conn.executemany(
        f'INSERT INTO "{src_table}" VALUES (?, ?)',
        [(i, f"row-{i}") for i in range(ROW_COUNT)],
    )
    conn.commit()
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="duckdb", database=src_path, table=src_table
        ),
        destination=EndpointConfig(
            kind="database", format="duckdb", database=dst_path, table=dst_table
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[
            {
                "name": "scan",
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "selected": True,
            }
        ],
        skip_preflight=True,
    )

    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == ROW_COUNT

    conn = duckdb.connect(dst_path)
    try:
        landed = conn.execute(f'SELECT count(*) FROM "{dst_table}"').fetchone()[0]
        gaps = conn.execute(
            f'SELECT count(*) FROM "{dst_table}" WHERE note <> \'row-\' || id'
        ).fetchone()[0]
    finally:
        conn.close()
    assert landed == ROW_COUNT
    assert gaps == 0


def test_desktop_lab_duckdb_generic_sql_url_transfers_two_rows(tmp_path):
    """CSV → DuckDB (generic_sql duckdb:///) → SQLite, same bind as desktop lab.

    format=duckdb + database=path already transferred 5000 rows. The duplex
    failure was the catalog bind: format=generic_sql and duckdb:///abs URL.
    """
    pytest.importorskip("duckdb")
    import sqlite3

    from services.desktop_lab import (
        CSV_BYTES,
        FIXTURE_ROWS,
        MAPPINGS,
        SHAPE_RECIPE,
        _approved_shape_hash,
    )
    from services.dest_precount import destination_row_count

    duck_path = tmp_path / "lab.duckdb"
    sqlite_path = tmp_path / "lab.db"
    duck_table = "lab_orders"
    url = f"duckdb:///{duck_path}"
    duck_ep = EndpointConfig(
        kind="database",
        format="generic_sql",
        database=str(duck_path),
        connection_string=url,
        table=duck_table,
    )

    csv_to_duck = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=duck_ep,
        source_content=CSV_BYTES,
        source_filename="lab.csv",
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
        mappings=list(MAPPINGS),
        shape_recipe=dict(SHAPE_RECIPE),
        approved_shape_recipe_hash=_approved_shape_hash(),
    )
    written = UniversalTransferEngine().execute_tracked(csv_to_duck, uuid.uuid4().hex[:24])
    assert written.success is True, written.error
    assert written.records_transferred == FIXTURE_ROWS
    dest_n = destination_row_count(
        "generic_sql",
        {
            "database": str(duck_path),
            "connection_string": url,
            "type": "duckdb",
        },
        schema="",
        table_name=duck_table,
    )
    assert dest_n == FIXTURE_ROWS

    duck_to_sqlite = TransferRequest(
        source=duck_ep,
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(sqlite_path),
            connection_string=f"sqlite:///{sqlite_path}",
            table="payload",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=list(MAPPINGS),
    )
    readback = UniversalTransferEngine().execute_tracked(
        duck_to_sqlite, uuid.uuid4().hex[:24]
    )
    assert readback.success is True, readback.error
    assert readback.records_transferred == FIXTURE_ROWS

    conn = sqlite3.connect(str(sqlite_path))
    try:
        sqlite_n = conn.execute("SELECT count(*) FROM payload").fetchone()[0]
        rows = conn.execute("SELECT id FROM payload ORDER BY id").fetchall()
    finally:
        conn.close()
    assert sqlite_n == FIXTURE_ROWS
    assert [str(r[0]) for r in rows] == ["1", "2"]
