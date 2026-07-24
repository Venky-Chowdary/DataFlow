"""DuckDB → DuckDB schema drift: backfill widens an existing column."""

from __future__ import annotations

import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def test_duckdb_to_duckdb_backfill_widens_varchar_and_numeric():
    duckdb = pytest.importorskip("duckdb")

    src_path = f"/tmp/duckdb_src_widen_{uuid.uuid4().hex[:8]}.duck"
    dst_path = f"/tmp/duckdb_dst_widen_{uuid.uuid4().hex[:8]}.duck"
    src_table = f"src_widen_{uuid.uuid4().hex[:8]}"
    dst_table = f"dst_widen_{uuid.uuid4().hex[:8]}"

    for p in (src_path, dst_path):
        Path(p).unlink(missing_ok=True)

    conn = duckdb.connect(src_path)
    conn.execute(
        f'CREATE TABLE "{src_table}" (id INTEGER PRIMARY KEY, note VARCHAR(5), amount NUMERIC(8,2))'
    )
    conn.execute(
        f'INSERT INTO "{src_table}" (id, note, amount) VALUES (?, ?, ?), (?, ?, ?)',
        (1, "hello", "1234.56", 2, "world", "7890.12"),
    )
    conn.commit()
    conn.close()

    conn = duckdb.connect(dst_path)
    conn.execute(
        f'CREATE TABLE "{dst_table}" (id INTEGER PRIMARY KEY, note VARCHAR(5), amount NUMERIC(8,2))'
    )
    conn.execute(
        f'INSERT INTO "{dst_table}" (id, note, amount) VALUES (?, ?, ?)',
        (1, "hi", "10.00"),
    )
    conn.commit()
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="duckdb",
            database=src_path,
            table=src_table,
        ),
        destination=EndpointConfig(
            kind="database",
            format="duckdb",
            database=dst_path,
            table=dst_table,
        ),
        sync_mode="upsert",
        stream_contracts=[
            {
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }
        ],
        backfill_new_fields=True,
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result1 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result1.success is True, result1.error
    assert result1.records_transferred == 2

    # Widen source columns.
    conn = duckdb.connect(src_path)
    conn.execute(f'ALTER TABLE "{src_table}" ALTER COLUMN note TYPE VARCHAR(50)')
    conn.execute(f'ALTER TABLE "{src_table}" ALTER COLUMN amount TYPE NUMERIC(12,2)')
    conn.execute(
        f'UPDATE "{src_table}" SET note = ?, amount = ? WHERE id = ?',
        ("a-much-longer-value-that-exceeds-five", "1234567890.12", 1),
    )
    conn.commit()
    conn.close()

    result2 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result2.success is True, result2.error
    assert result2.records_transferred == 2

    conn = duckdb.connect(dst_path)
    rows = conn.execute(
        f'SELECT id, note, amount FROM "{dst_table}" ORDER BY id'
    ).fetchall()
    conn.execute(f'DROP TABLE "{dst_table}"')
    conn.close()

    assert rows == [
        (1, "a-much-longer-value-that-exceeds-five", 1234567890.12),
        (2, "world", 7890.12),
    ]
