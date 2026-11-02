"""Arrow coerce treats reader-null as typed null via is_reader_null_cell.

Python None already bound as Arrow null. After extract emits
SQL_NULL_SENTINEL, string columns wrote the sentinel spelling and
boolean coerce (now returning None) raised instead of binding null.
Missing still raises — sparse CDC must overlay before the Arrow batch.
Empty string still refuses on typed columns.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("pyarrow")

import pyarrow as pa  # noqa: E402

from connectors.salesforce_writer import coerce_salesforce_id_wire  # noqa: E402
from services.arrow_write import coerce_arrow_cell  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


_NULL_WIRES = (None, SQL_NULL_SENTINEL, "__df_ddb_null__")


def test_arrow_reader_null_is_typed_null():
    types = (
        pa.string(),
        pa.int64(),
        pa.float64(),
        pa.bool_(),
        pa.date32(),
        pa.timestamp("us"),
        pa.decimal128(10, 2),
    )
    for arrow_type in types:
        for wire in _NULL_WIRES:
            assert coerce_arrow_cell(wire, arrow_type, pa) is None, (arrow_type, wire)


def test_arrow_missing_still_refuses_sparse_overlay():
    with pytest.raises(ValueError, match="DF_MISSING"):
        coerce_arrow_cell(Missing, pa.string(), pa)
    with pytest.raises(ValueError, match="DF_MISSING"):
        coerce_arrow_cell(DF_MISSING_SENTINEL, pa.int64(), pa)


def test_arrow_empty_string_still_refuses_typed_null_invent():
    with pytest.raises(ValueError, match="empty string"):
        coerce_arrow_cell("", pa.int64(), pa)
    with pytest.raises(ValueError, match="empty string"):
        coerce_arrow_cell("", pa.bool_(), pa)
    assert coerce_arrow_cell("", pa.string(), pa) == ""


def test_salesforce_id_reader_null_is_sql_null():
    for wire in _NULL_WIRES:
        assert coerce_salesforce_id_wire(wire) is None
    assert coerce_salesforce_id_wire(Missing) is Missing
    with pytest.raises(ValueError, match="empty Salesforce"):
        coerce_salesforce_id_wire("")
