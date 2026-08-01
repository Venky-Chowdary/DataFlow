"""Wave 57: PostgreSQL TID / XID / XID8 / CID system-identifier bind SSOT."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_tid_wire():
    from connectors.sql_bind import coerce_tid_wire, normalize_sql_bind_value

    assert coerce_tid_wire("(42,7)") == "(42,7)"
    assert coerce_tid_wire("( 0 , 0 )") == "(0,0)"
    assert coerce_tid_wire((42, 7)) == "(42,7)"
    assert coerce_tid_wire({"block": 1, "offset": 2}) == "(1,2)"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_tid_wire("(0,-1)")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_tid_wire((0, 65536))
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_tid_wire(True)
    assert normalize_sql_bind_value("(9,1)", "TID") == "(9,1)"
    assert normalize_sql_bind_value("(9,1)", "CTID") == "(9,1)"


def test_coerce_xid_cid_wire():
    from connectors.sql_bind import (
        coerce_cid_wire,
        coerce_xid_wire,
        normalize_sql_bind_value,
    )

    assert coerce_xid_wire(278394) == 278394
    assert coerce_xid_wire("278394") == 278394
    assert coerce_xid_wire(0x1FFFFFFFF, width64=True) == 0x1FFFFFFFF
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_xid_wire(0x100000000)  # xid is uint32
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_xid_wire(True, width64=True)
    assert coerce_cid_wire(12) == 12
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_cid_wire(-1)
    assert normalize_sql_bind_value("99", "XID") == 99
    assert normalize_sql_bind_value(str(2**40), "XID8") == 2**40
    assert normalize_sql_bind_value("3", "CID") == 3
