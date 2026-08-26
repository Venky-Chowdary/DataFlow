"""JSON / JSONL ingest uses json_loads_exact, not stdlib float().

json.loads('{"amt": 1.234567890123456789}') collapsed digits before
cell_to_string. Integers and IEEE-exact 1.5 stay native.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.file_parser import FileParser, parse_jsonl  # noqa: E402
from services.json_tabular import load_json_records  # noqa: E402


LONG = "1.234567890123456789"


def test_jsonl_long_fraction_stays_identity():
    headers, rows, n = parse_jsonl(
        f'{{"id": 1, "amt": {LONG}}}\n'.encode("utf-8")
    )
    assert n == 1
    assert "amt" in headers
    idx = headers.index("amt")
    assert rows[0][idx] == LONG
    assert rows[0][idx] != str(json.loads(f'{{"amt": {LONG}}}')["amt"])


def test_json_array_long_fraction_stays_identity():
    recs = load_json_records(f'[{{"amt": {LONG}}}]')
    assert recs[0]["amt"] == Decimal(LONG)
    parsed = FileParser.parse_json(f'[{{"amt": {LONG}}}]')
    assert parsed.success is True
    assert parsed.data[0]["amt"] == Decimal(LONG)


def test_plain_integer_and_exact_float_still_bind():
    recs = load_json_records('[{"id": 1, "amt": 1.5}]')
    assert recs[0]["id"] == 1
    assert recs[0]["amt"] == 1.5
    headers, rows, n = parse_jsonl(b'{"id": 2, "amt": 1.5}\n')
    assert n == 1
    assert rows[0][headers.index("id")] == "2"
    assert rows[0][headers.index("amt")] == "1.5"


def test_poison_jsonl_still_refuses():
    try:
        parse_jsonl(b'{"id":1}\n42\n')
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
