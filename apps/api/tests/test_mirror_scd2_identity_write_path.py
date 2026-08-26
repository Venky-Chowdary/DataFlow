"""Mirror / SCD2 identity use conflict_key_wire, not the extract token.

complete_mirror_pk_tuple treated SQL_NULL_SENTINEL as a live PK, so inferred
delete could miss dest NULL keys and SCD2 could open a version whose hash
was the wire spelling. 0 / false stay present.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import conflict_key_wire  # noqa: E402
from services.mirror_engine import (  # noqa: E402
    _key_value,
    complete_mirror_pk_tuple,
)
from services.scd2_engine import (  # noqa: E402
    _compose_key,
    _pk_validate_mapped_rows,
    _row_hash,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_conflict_key_wire_null_vs_present():
    assert conflict_key_wire(None) == ""
    assert conflict_key_wire(SQL_NULL_SENTINEL) == ""
    assert conflict_key_wire("__df_ddb_null__") == ""
    assert conflict_key_wire(Missing) == ""
    assert conflict_key_wire(DF_MISSING_SENTINEL) == ""
    assert conflict_key_wire("") == ""
    assert conflict_key_wire(0) == "0"
    assert conflict_key_wire(False) == "false"
    assert conflict_key_wire(True) == "true"
    assert conflict_key_wire("1") == "1"


def test_mirror_pk_reader_null_is_incomplete():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL, ""):
        assert complete_mirror_pk_tuple([wire]) is None, wire
    assert complete_mirror_pk_tuple([0]) == (0,)
    assert complete_mirror_pk_tuple([False]) == (False,)
    assert complete_mirror_pk_tuple(["1"]) == ("1",)
    assert _key_value({"id": SQL_NULL_SENTINEL}, "id") == _key_value({"id": None}, "id")


def test_scd2_hash_and_key_share_dest_null():
    cols = ["id", "note"]
    sent = {"id": "1", "note": SQL_NULL_SENTINEL}
    none = {"id": "1", "note": None}
    keep = {"id": "1", "note": "keep"}
    zero = {"id": 0, "note": False}
    assert _row_hash(sent, cols) == _row_hash(none, cols)
    assert _row_hash(sent, cols) != _row_hash(keep, cols)
    assert _compose_key(sent, ["id"]) == _compose_key(none, ["id"])
    assert _compose_key({"id": 0}, ["id"]) != ""
    assert _row_hash(zero, cols) != _row_hash(none, cols)


def test_scd2_pk_validate_refuses_reader_null():
    details: list[dict] = []
    out = _pk_validate_mapped_rows(
        [
            {"id": SQL_NULL_SENTINEL, "note": "x"},
            {"id": Missing, "note": "x"},
            {"id": 0, "note": "x"},
            {"id": "1", "note": "x"},
        ],
        ["id"],
        details,
    )
    assert [row["id"] for row in out] == [0, "1"]
    assert len(details) == 2
    assert all("primary key" in (d.get("reason") or "").lower() for d in details)
