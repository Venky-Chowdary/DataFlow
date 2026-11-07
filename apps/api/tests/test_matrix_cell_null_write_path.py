"""Transfer-matrix cells collapse reader-null; file blanks share is_null_evidence.

``matrix_cell_from_record`` used to only treat Python None as NULL. After
extract emits SQL_NULL_SENTINEL, that token stayed a present string on the
spool / integrity-audit matrix. File empty→null only saw None or strip-empty,
so a sentinel blank could not bind as NULL on a nullable dest.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.source_row_spool import (  # noqa: E402
    matrix_cell_from_record,
    matrix_present_cell,
    matrix_row_from_record,
)
from connectors.writer_common import (  # noqa: E402
    _is_blank_cell,
    build_mapped_rows_with_details,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)
from src.transfer.adapters import (  # noqa: E402
    _matrix_cell,
    records_to_matrix,
    write_destination_file,
)
from src.transfer.models import EndpointConfig  # noqa: E402


def test_matrix_present_cell_reader_null_is_none():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__"):
        assert matrix_present_cell(wire) is None, wire
        assert _matrix_cell(wire) is None, wire


def test_matrix_present_cell_missing_stays_missing():
    assert matrix_present_cell(Missing) == DF_MISSING_SENTINEL
    assert matrix_present_cell(DF_MISSING_SENTINEL) == DF_MISSING_SENTINEL
    assert _matrix_cell(Missing) == DF_MISSING_SENTINEL


def test_matrix_present_cell_keeps_zero_false_empty():
    assert matrix_present_cell(0) == "0"
    assert matrix_present_cell(False) == "false"
    assert matrix_present_cell(True) == "true"
    assert matrix_present_cell("") == ""
    assert matrix_present_cell("kept") == "kept"


def test_matrix_cell_from_record_collapses_sentinel():
    assert matrix_cell_from_record({"note": SQL_NULL_SENTINEL}, "note") is None
    assert matrix_cell_from_record({"id": "1"}, "note") == DF_MISSING_SENTINEL
    row = matrix_row_from_record({"id": True, "note": SQL_NULL_SENTINEL}, ["id", "note"])
    assert row == ["true", None]


def test_records_to_matrix_collapses_reader_null():
    _headers, rows = records_to_matrix(
        [{"id": 0, "note": SQL_NULL_SENTINEL}],
        ["id", "note"],
    )
    assert rows[0][0] == "0"
    assert rows[0][1] is None
    assert SQL_NULL_SENTINEL not in rows[0]


def test_blank_cell_treats_sentinel_and_whitespace_as_absence():
    assert _is_blank_cell(None) is True
    assert _is_blank_cell("") is True
    assert _is_blank_cell("  ") is True
    assert _is_blank_cell(SQL_NULL_SENTINEL) is True
    assert _is_blank_cell("__df_ddb_null__") is True
    assert _is_blank_cell(0) is False
    assert _is_blank_cell(False) is False
    assert _is_blank_cell("0") is False


def test_file_sentinel_decimal_becomes_null_under_empty_cells_flag():
    mapped, errs, details = build_mapped_rows_with_details(
        headers=["Country", "Total"],
        data_rows=[["Finland", SQL_NULL_SENTINEL]],
        mappings=[
            {"source": "Country", "target": "country", "confidence": 0.99},
            {
                "source": "Total",
                "target": "total",
                "transform": "decimal",
                "confidence": 0.99,
            },
        ],
        target_cols=["country", "total"],
        column_types={"Country": "string", "Total": "decimal"},
        dest_types={"country": "text", "total": "decimal"},
        error_policy="fail",
        dest_kind="postgresql",
        empty_cells_as_null=True,
    )
    assert errs == []
    assert details == []
    assert mapped[0][0] == "Finland"
    assert mapped[0][1] is None
    assert SQL_NULL_SENTINEL not in mapped[0]


def test_file_export_json_omits_reader_null_sentinel():
    import json

    endpoint = EndpointConfig(kind="file_export", format="json")
    content, _name, summary = write_destination_file(
        endpoint,
        [{"id": 0, "note": SQL_NULL_SENTINEL}],
        ["id", "note"],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "note", "target": "note", "transform": "none"},
        ],
        column_types={"id": "integer", "note": "string"},
        validation_mode="balanced",
    )
    payload = json.loads(content.decode("utf-8"))
    assert int(summary.get("rows") or 0) == 1
    assert payload[0]["id"] in (0, "0")
    assert payload[0].get("note") is None
    assert SQL_NULL_SENTINEL not in json.dumps(payload)
