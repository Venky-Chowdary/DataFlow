"""WriteResult must carry full rejected_details — never truncate before DLQ."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (
    WriteResult,
    build_mapped_rows_with_details,
    map_rows_for_fingerprint,
)
from src.transfer.adapters import WriteBatchBlocked, raise_writer_failure
from src.transfer.stream import _raise_write_failure


def test_map_rejects_preserve_full_detail_list_size():
    """Sanity: many transform rejects stay attached (cap must not be 100)."""
    headers = ["id", "n"]
    rows = [[str(i), "not-an-int"] for i in range(1, 151)]
    mappings = [
        {"source": "id", "target": "id", "transform": "none"},
        {
            "source": "n",
            "target": "n",
            "transform": "to_integer",
            "target_type": "integer",
        },
    ]
    _mapped, _errs, rejected = build_mapped_rows_with_details(
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        target_cols=["id", "n"],
        column_types={"id": "string", "n": "integer"},
        dest_types={"id": "string", "n": "integer"},
        error_policy="quarantine",
        dest_kind="sqlite",
        destination_pk_columns=["id"],
    )
    assert len(rejected) >= 150


def test_quarantine_detail_does_not_invent_id_pk_without_contract():
    """Without mapping/contract PK, do not invent id as quarantine identity."""
    headers = ["name", "n"]
    rows = [["alpha", "nope"]]
    mappings = [
        {"source": "name", "target": "name", "transform": "none"},
        {
            "source": "n",
            "target": "n",
            "transform": "to_integer",
            "target_type": "integer",
        },
    ]
    _mapped, _errs, rejected = build_mapped_rows_with_details(
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        target_cols=["name", "n"],
        column_types={"name": "string", "n": "integer"},
        dest_types={"name": "string", "n": "integer"},
        error_policy="quarantine",
        dest_kind="sqlite",
        destination_pk_columns=None,
    )
    assert rejected
    for d in rejected:
        assert d.get("primary_key") in (None, [], ())


def test_raise_write_failure_preserves_full_rejected_details():
    details = [
        {"row": i, "reason": f"bad-{i}", "execution_policy": "FAIL_JOB"}
        for i in range(1, 120)
    ]
    result = WriteResult(
        ok=False,
        rows_written=7,
        table_name="t",
        target_schema="s",
        checksum="",
        chunks_completed=1,
        error="MongoDB rejected rows; Migration Risk Contract abort policy blocks partial write",
        rejected_rows=len(details),
        rejected_details=details,
    )
    with pytest.raises(WriteBatchBlocked) as excinfo:
        _raise_write_failure(result, "MongoDB batch write failed")
    blocked = excinfo.value
    assert len(blocked.rejected_details) == 119
    assert blocked.rows_written == 7
    assert "partial write" in str(blocked).lower()


def test_raise_writer_failure_adapters_parity():
    details = [{"row": 1, "reason": "overflow", "policy": "fail"}]
    result = MagicMock()
    result.ok = False
    result.error = "strict error policy blocks partial write"
    result.rows_written = 0
    result.rejected_details = details
    result.rejected_rows = 1
    result.coerced_null_rows = 0
    result.rows_skipped = 0
    result.warnings = []
    result.meta = None
    with pytest.raises(WriteBatchBlocked) as excinfo:
        raise_writer_failure(result, "SQLite write failed")
    assert excinfo.value.rejected_details == details


def test_map_rows_for_fingerprint_applies_decimal_matrix():
    """Gate-8 remap must hold out DECIMAL overflow the same as typed writers."""
    mapped, rejected = map_rows_for_fingerprint(
        headers=["id", "amt"],
        data_rows=[["1", "123.456"], ["2", "1.5"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {
                "source": "amt",
                "target": "amt",
                "transform": "none",
                "target_type": "DECIMAL(5,2)",
            },
        ],
        target_cols=["id", "amt"],
        column_types={"id": "string", "amt": "DECIMAL(5,2)"},
        dest_types={"id": "string", "amt": "DECIMAL(5,2)"},
        error_policy="quarantine",
        dest_kind="sqlite",
        destination_pk_columns=["id"],
    )
    # 123.456 does not fit DECIMAL(5,2) — must be quarantined, not fingerprinted.
    assert rejected, "decimal overflow must surface rejected_details"
    assert len(mapped) <= 1
