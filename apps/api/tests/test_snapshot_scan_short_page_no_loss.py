"""A short snapshot-scan page must not be read as "source drained".

A held scan hands back whatever the driver buffered — DuckDB returns one
vector (2048 rows) regardless of the page size we asked for. Treating that as
the end of the source truncates the load *and* stays green: the destination
read-back, the row count and the checksum all agree on the rows that landed.
"""

from __future__ import annotations

import sys
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

    src_path = f"/tmp/duckdb_scan_src_{uuid.uuid4().hex[:8]}.duck"
    dst_path = f"/tmp/duckdb_scan_dst_{uuid.uuid4().hex[:8]}.duck"
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
