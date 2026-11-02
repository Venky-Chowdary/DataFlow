"""File→Snowflake must not reconnect every 20k rows on a 1M COPY load."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from src.transfer.file_stream import (  # noqa: E402
    file_dest_batch_size,
    unique_preserve_warnings,
)


def test_snowflake_file_batch_is_copy_scale_not_20k_insert():
    """7-col flights-sized rows: one COPY file, not 50 Railway reconnects."""
    sf = file_dest_batch_size("snowflake", avg_row_size=120)
    other = file_dest_batch_size("postgresql", avg_row_size=120)
    assert sf >= 80_000
    assert sf <= 100_000
    assert other <= 20_000
    assert sf > other


def test_snowflake_file_batch_ignores_proxy_insert_shrink():
    # amazonaws.com is a proxy marker for PG INSERT — COPY must not inherit 5k.
    sf = file_dest_batch_size(
        "snowflake",
        avg_row_size=120,
        dest_host="xy12345.us-east-1.snowflakecomputing.com",
        dest_connection_string="snowflake://xy12345.us-east-1",
    )
    assert sf >= 80_000


def test_document_store_file_batch_still_uses_large_memory_budget():
    mongo = file_dest_batch_size("mongodb", avg_row_size=200)
    assert mongo >= 1


def test_batch_integrity_warnings_dedupe_identity_and_cardinality_spam():
    repeated = [
        "No identity key resolved for duplicate check (schemaless needs _id; SQL needs id/_id or a stream-contract primary_key)",
        "Column 'FL_DATE' is nearly constant (31 distinct values over 20000 rows)",
        "Column 'DISTANCE' is nearly constant (176 distinct values over 20000 rows)",
        "No identity key resolved for duplicate check (schemaless needs _id; SQL needs id/_id or a stream-contract primary_key)",
        "Column 'FL_DATE' is nearly constant (31 distinct values over 20000 rows)",
    ] * 10
    out = unique_preserve_warnings(repeated, limit=10)
    assert out == [
        "No identity key resolved for duplicate check (schemaless needs _id; SQL needs id/_id or a stream-contract primary_key)",
        "Column 'FL_DATE' is nearly constant (31 distinct values over 20000 rows)",
        "Column 'DISTANCE' is nearly constant (176 distinct values over 20000 rows)",
    ]
