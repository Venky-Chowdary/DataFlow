"""D1: spreadsheet blank -> SQL NULL on a typed Snowflake carrier, and
FAIL_JOB drops the tables this run created (dest + *_df_quarantine).

Preflight promised "spreadsheet blank stored as SQL NULL", but the
Snowflake writer dropped ``empty_cells_as_null`` before the map stage, so a
blank INTEGER cell failed the whole strict job with
'Empty value cannot coerce to decimal' — and strict FAIL_JOB left the
created dest table and the DLQ table behind (X-35 parity).
"""

from __future__ import annotations

import csv
import io
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_conn import get_connection, resolve_or_fold_snowflake_table
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _csv_bytes(fieldnames: list[str], rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _new_job_id() -> str:
    return uuid.uuid4().hex[:24]


def _sf_destination(table_name: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="snowflake",
        host="localhost",
        port=443,
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        table=table_name,
    )


def _sf_cur():
    conn = get_connection(
        account="localhost",
        username="test",
        password="test",
        database="dataflow",
        schema="public",
        warehouse="",
        connection_string="",
    )
    return conn, conn.cursor()


def _table_exists(cur, name: str) -> bool:
    try:
        resolved = resolve_or_fold_snowflake_table(cur, "public", name)
    except Exception:
        resolved = None
    if not resolved:
        return False
    try:
        cur.execute(f"SELECT 1 FROM {resolved} LIMIT 1")
        return True
    except Exception:
        return False


def test_csv_blank_integer_lands_as_null_on_snowflake():
    """Blank cell in an INTEGER column: lands as SQL NULL, not FAIL_JOB."""
    pytest.importorskip("fakesnow")

    table_name = "d1_blank_" + uuid.uuid4().hex[:8]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="products.csv",
        source_content=_csv_bytes(
            ["id", "stock"],
            [
                {"id": "1", "stock": "10"},
                {"id": "2", "stock": "20"},
                {"id": "3", "stock": ""},
            ],
        ),
        destination=_sf_destination(table_name),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "products",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(request, _new_job_id())
    assert result.success, f"blank INTEGER cell must store SQL NULL: {result.error}"

    conn, cur = _sf_cur()
    try:
        resolved = resolve_or_fold_snowflake_table(cur, "public", table_name)
        cur.execute(f"SELECT id, stock FROM {resolved} ORDER BY id")
        rows = cur.fetchall()
    finally:
        conn.close()
    by_id = {r[0]: r[1] for r in rows}
    assert by_id[1] == 10 and by_id[2] == 20
    assert by_id[3] is None, "preflight promised the spreadsheet blank as SQL NULL"


def test_fail_job_drops_created_tables_on_snowflake():
    """Strict FAIL_JOB on a typed cell: the dest table AND the
    {table}_df_quarantine litter from this run are dropped (X-35 parity)."""
    pytest.importorskip("fakesnow")

    table_name = "d1_fail_" + uuid.uuid4().hex[:8]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="products.csv",
        source_content=_csv_bytes(
            ["id", "stock"],
            [
                {"id": "1", "stock": "10"},
                {"id": "2", "stock": "not-a-number"},
            ],
        ),
        destination=_sf_destination(table_name),
        sync_mode="full_refresh_overwrite",
        mappings=[
            {
                "source": "id",
                "target": "id",
                "source_type": "integer",
                "target_type": "integer",
                "confidence": 1.0,
                "user_override": True,
                "transform": "none",
            },
            {
                "source": "stock",
                "target": "stock",
                "source_type": "integer",
                "target_type": "integer",
                "confidence": 1.0,
                "user_override": True,
                "transform": "none",
            },
        ],
        stream_contracts=[{
            "name": "products",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(request, _new_job_id())
    assert not result.success, "typed cell failure must fail the strict job"

    conn, cur = _sf_cur()
    try:
        assert not _table_exists(cur, table_name), (
            "strict FAIL_JOB must drop the table this run created"
        )
        assert not _table_exists(cur, f"{table_name}_df_quarantine"), (
            "strict FAIL_JOB must drop the DLQ table this run created"
        )
    finally:
        conn.close()
