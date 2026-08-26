"""File-stream JSONL peek and batches use json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 before Map/Validate
and before the writer saw the cell. Integers and IEEE-exact 1.5 stay native.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.transfer.file_stream import (  # noqa: E402
    _batch_iterator_for_type,
    _iter_jsonl_batches,
    peek_file_source,
)

LONG = "1.234567890123456789"
IEEE_COLLAPSED = json.loads(f'{{"amt": {LONG}}}')["amt"]


def test_peek_jsonl_long_fraction_stays_decimal():
    raw = f'{{"id": 1, "amt": {LONG}}}\n'.encode("utf-8")
    columns, schema, total, sample = peek_file_source(raw, "amt.jsonl")
    assert total == 1
    assert columns == ["id", "amt"]
    assert sample[0]["amt"] == Decimal(LONG)
    assert sample[0]["amt"] != IEEE_COLLAPSED
    assert sample[0]["id"] == 1
    assert schema.get("amt")


def test_peek_ndjson_nested_long_fraction_stays_decimal():
    raw = f'{{"nested": {{"amt": {LONG}}}}}\n'.encode("utf-8")
    _columns, _schema, total, sample = peek_file_source(raw, "nested.ndjson")
    assert total == 1
    assert sample[0]["nested"]["amt"] == Decimal(LONG)


def test_iter_jsonl_batches_match_peek_and_refuse_ieee():
    raw = (
        f'{{"id": 1, "amt": {LONG}, "n": 1.5}}\n'
        '{"id": 2, "amt": "", "n": 2}\n'
    ).encode("utf-8")
    batches = list(_iter_jsonl_batches(raw, chunk_size=1))
    assert len(batches) == 2
    assert batches[0][0]["amt"] == Decimal(LONG)
    assert batches[0][0]["n"] == 1.5
    assert isinstance(batches[0][0]["n"], float)
    assert batches[1][0]["amt"] is None
    typed = list(_batch_iterator_for_type("jsonl", raw, 10))
    assert typed[0][0]["amt"] == Decimal(LONG)
    peeked = peek_file_source(raw, "amt.jsonl")[3]
    assert peeked[0]["amt"] == batches[0][0]["amt"]


def test_poison_jsonl_still_refuses():
    with pytest.raises(ValueError, match="Invalid JSONL"):
        peek_file_source(b'{"id":1}\n{not-json}\n', "bad.jsonl")
    with pytest.raises(ValueError, match="Invalid JSONL"):
        list(_iter_jsonl_batches(b'{"id":1}\n{not-json}\n', 10))
    with pytest.raises(ValueError, match="JSON object"):
        list(_iter_jsonl_batches(b"42\n", 10))
