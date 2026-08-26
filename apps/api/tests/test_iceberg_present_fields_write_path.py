"""Iceberg present-fields bind reader-null as None, omit only Missing.

The leftover merge only dropped Missing. After extract emits
SQL_NULL_SENTINEL, that token overlaid dest columns as a string.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import (  # noqa: E402
    _iceberg_present_fields,
    _merge_upsert_rows,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_iceberg_present_fields_null_vs_missing():
    got = _iceberg_present_fields(
        {
            "id": "1",
            "note": SQL_NULL_SENTINEL,
            "extra": Missing,
            "gone": DF_MISSING_SENTINEL,
            "zero": 0,
        }
    )
    assert got == {"id": "1", "note": None, "zero": 0}
    assert SQL_NULL_SENTINEL not in got.values()


def test_iceberg_merge_reader_null_wipes_dest_missing_preserves():
    existing = [{"id": "1", "note": "keep", "extra": "stay", "_df_lsn": "0/100"}]
    incoming = [
        {
            "id": "1",
            "note": SQL_NULL_SENTINEL,
            "extra": DF_MISSING_SENTINEL,
            "_df_lsn": "0/200",
        }
    ]
    merged = _merge_upsert_rows(existing, incoming, pk_cols=["id"])
    assert len(merged) == 1
    assert merged[0]["note"] is None
    assert merged[0]["extra"] == "stay"
    assert SQL_NULL_SENTINEL not in merged[0].values()
