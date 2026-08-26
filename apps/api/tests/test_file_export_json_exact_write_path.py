"""File JSON export uses writer_common.to_json_value, not a sibling parser.

A mapped rename used to miss dest types and keep long fractions as text, or
re-parse with stdlib float(). JSON-typed cells keep Decimal identity.
IEEE-exact 1.5 stays float. String \"1\" stays a string.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.transfer.adapters import write_destination_file  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

LONG = "1.234567890123456789"


def test_json_export_json_column_keeps_long_fraction():
    endpoint = EndpointConfig(kind="file_export", format="json")
    content, _name, summary = write_destination_file(
        endpoint,
        [{"id": "1", "payload": f'{{"amt": {LONG}, "n": 1.5}}'}],
        ["id", "payload"],
        mappings=[
            {"source": "id", "target": "id", "transform": "none", "target_type": "string"},
            {
                "source": "payload",
                "target": "doc",
                "transform": "none",
                "target_type": "json",
            },
        ],
        column_types={"id": "string", "payload": "json"},
        validation_mode="balanced",
    )
    rows = json.loads(content.decode("utf-8"))
    assert summary["rows"] == 1
    assert LONG.encode("utf-8") in content
    assert rows[0]["doc"]["n"] == 1.5
    assert isinstance(rows[0]["doc"]["n"], float)


def test_jsonl_export_renamed_decimal_stays_number():
    endpoint = EndpointConfig(kind="file_export", format="jsonl")
    content, _name, _summary = write_destination_file(
        endpoint,
        [{"src_amt": LONG}],
        ["src_amt"],
        mappings=[
            {
                "source": "src_amt",
                "target": "amt",
                "transform": "none",
                "target_type": "decimal",
            },
        ],
        column_types={"src_amt": "decimal"},
        validation_mode="balanced",
    )
    row = json.loads(content.decode("utf-8").strip())
    assert "amt" in row
    assert LONG.encode("utf-8") in content


def test_json_export_string_one_stays_string():
    endpoint = EndpointConfig(kind="file_export", format="json")
    content, _name, _summary = write_destination_file(
        endpoint,
        [{"code": "1"}],
        ["code"],
        mappings=[
            {"source": "code", "target": "code", "transform": "none", "target_type": "string"},
        ],
        column_types={"code": "string"},
        validation_mode="balanced",
    )
    rows = json.loads(content.decode("utf-8"))
    assert rows[0]["code"] == "1"
    assert isinstance(rows[0]["code"], str)
