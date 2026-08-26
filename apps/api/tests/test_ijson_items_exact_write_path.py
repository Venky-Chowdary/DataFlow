"""ijson record streams use the same number demote as json_loads_exact.

Default ijson keeps 1.5 as Decimal. use_float=True invented IEEE. Long
fractions stay Decimal. Integers stay int.
"""

from __future__ import annotations

import io
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ijson = pytest.importorskip("ijson")

from services.json_tabular import (  # noqa: E402
    ijson_items_exact,
    iter_json_dicts,
    iter_json_record_dicts,
)
from services.value_serializer import json_loads_exact  # noqa: E402

LONG = "1.234567890123456789"


def test_ijson_items_exact_matches_json_loads_exact():
    raw = f'[{{"amt": {LONG}, "n": 1.5, "id": 1}}]'.encode("utf-8")
    streamed = list(ijson_items_exact(io.BytesIO(raw), "item"))
    dom = json_loads_exact(raw.decode("utf-8"))
    assert streamed == dom
    assert streamed[0]["amt"] == Decimal(LONG)
    assert streamed[0]["n"] == 1.5
    assert streamed[0]["id"] == 1
    assert isinstance(streamed[0]["n"], float)


def test_use_float_true_is_refused():
    with pytest.raises(ValueError, match="use_float"):
        list(ijson_items_exact(io.BytesIO(b"[]"), "item", use_float=True))


def test_iter_json_dicts_keeps_long_fraction():
    rows = list(iter_json_dicts(f'[{{"amt": {LONG}}}]'.encode("utf-8")))
    assert rows[0]["amt"] == Decimal(LONG)


def test_iter_json_record_dicts_keeps_long_fraction():
    batches = list(
        iter_json_record_dicts(
            lambda _c: io.BytesIO(f'[{{"amt": {LONG}}}]'.encode("utf-8")),
            b"unused",
            chunk_size=10,
        )
    )
    assert batches[0][0]["amt"] == Decimal(LONG)
