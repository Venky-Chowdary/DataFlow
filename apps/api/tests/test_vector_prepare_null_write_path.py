"""Vector flatten omits reader-null and leftover types use dest wire.

``prepare_records_for_vector_write`` used to only drop Python None / Missing.
After extract emits SQL_NULL_SENTINEL, that spelling leaked into metadata.
Gate-8 ``str(id)`` invented ``True`` so dest ``true`` missed written_ids.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import (  # noqa: E402
    prepare_records_for_vector_write,
    vector_gate8_meta,
    vector_prepare_cell,
    vector_prepare_metadata,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_vector_prepare_cell_omits_reader_null():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL):
        assert vector_prepare_cell(wire) is None, wire


def test_vector_prepare_cell_keeps_zero_false_and_empty():
    assert vector_prepare_cell(0) == 0
    assert vector_prepare_cell(False) is False
    assert vector_prepare_cell(True) is True
    assert vector_prepare_cell("") == ""
    assert vector_prepare_cell("kept") == "kept"


def test_vector_prepare_metadata_omits_reader_null_keeps_zero():
    got = vector_prepare_metadata(
        {
            "page": SQL_NULL_SENTINEL,
            "blank_null": None,
            "kept": "1",
            "zero": 0,
            "flag": False,
            "tags": ["a", "b"],
        }
    )
    assert got == {"kept": "1", "zero": 0, "flag": False, "tags": ["a", "b"]}
    assert SQL_NULL_SENTINEL not in got.values()
    assert vector_prepare_metadata(None) == {}
    assert vector_prepare_metadata("not-a-dict") == {}


def test_vector_prepare_cell_decimal_is_dest_canonical():
    assert vector_prepare_cell(Decimal("1E+2")) == "100"
    assert vector_prepare_cell(Decimal("100")) == "100"
    assert vector_prepare_cell(Decimal("1E+2")) != str(Decimal("1E+2"))


def test_prepare_records_omits_sql_null_sentinel():
    records, _rejected, abort = prepare_records_for_vector_write(
        headers=["id", "note"],
        data_rows=[["1", SQL_NULL_SENTINEL]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        column_types={"id": "string", "note": "string"},
    )
    assert abort is None
    assert records
    assert SQL_NULL_SENTINEL not in records[0].values()
    assert "note" not in records[0]


def test_vector_gate8_ids_use_dest_wire():
    meta = vector_gate8_meta([{"id": True}, {"id": 0}, {"id": SQL_NULL_SENTINEL}])
    assert meta["written_ids"] == ["true", "0"]
    assert "True" not in meta["written_ids"]
    assert SQL_NULL_SENTINEL not in meta["written_ids"]
