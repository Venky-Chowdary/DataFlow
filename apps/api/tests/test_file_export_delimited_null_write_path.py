"""File CSV/TSV export binds reader-null via to_delimited_value.

The grid and DictWriter paths used cell_to_string, so extract
SQL_NULL_SENTINEL became a live CSV cell. Object-store already uses
to_delimited_value. Empty string stays empty. 0 / false stay present.
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)
from src.transfer.adapters import write_destination_file  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


def _csv_rows(content: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(content.decode("utf-8"))))


def test_csv_export_reader_null_is_empty_not_token():
    endpoint = EndpointConfig(kind="file_export", format="csv")
    content, name, summary = write_destination_file(
        endpoint,
        [
            {
                "id": 0,
                "note": SQL_NULL_SENTINEL,
                "flag": False,
                "gone": Missing,
            }
        ],
        ["id", "note", "flag", "gone"],
        mappings=[
            {"source": "id", "target": "id", "transform": "none", "target_type": "integer"},
            {"source": "note", "target": "note", "transform": "none", "target_type": "string"},
            {"source": "flag", "target": "flag", "transform": "none", "target_type": "boolean"},
            {"source": "gone", "target": "gone", "transform": "none", "target_type": "string"},
        ],
        column_types={
            "id": "integer",
            "note": "string",
            "flag": "boolean",
            "gone": "string",
        },
        validation_mode="balanced",
    )
    assert name == "export.csv"
    assert int(summary.get("rows") or 0) == 1
    rows = _csv_rows(content)
    assert rows[0]["id"] == "0"
    assert rows[0]["note"] == ""
    assert rows[0]["flag"] in {"false", "False"}
    assert rows[0]["gone"] == ""
    assert SQL_NULL_SENTINEL not in content.decode("utf-8")
    assert DF_MISSING_SENTINEL not in content.decode("utf-8")


def test_tsv_export_reader_null_is_empty_not_token():
    endpoint = EndpointConfig(kind="file_export", format="tsv")
    content, _name, _summary = write_destination_file(
        endpoint,
        [{"id": "1", "note": "__df_ddb_null__"}],
        ["id", "note"],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "note", "target": "note", "transform": "none"},
        ],
        column_types={"id": "string", "note": "string"},
        validation_mode="balanced",
    )
    text = content.decode("utf-8")
    assert SQL_NULL_SENTINEL not in text
    assert "__df_ddb_null__" not in text
    rows = list(csv.DictReader(io.StringIO(text), delimiter="\t"))
    assert rows[0]["id"] == "1"
    assert rows[0]["note"] == ""


def test_csv_export_keeps_decimal_scale_text():
    endpoint = EndpointConfig(kind="file_export", format="csv")
    content, _name, _summary = write_destination_file(
        endpoint,
        [{"amt": "10.50"}],
        ["amt"],
        mappings=[
            {
                "source": "amt",
                "target": "amt",
                "transform": "none",
                "target_type": "decimal",
            }
        ],
        column_types={"amt": "decimal"},
        validation_mode="balanced",
    )
    rows = _csv_rows(content)
    assert rows[0]["amt"] == "10.50"
