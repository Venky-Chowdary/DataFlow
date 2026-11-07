"""D53 audit - consumers of the key rule agree: '' is a key, NULL is not."""

from __future__ import annotations

from services.duplicate_policy import collapse_keep_last
from services.quarantine_row_contract import normalize_quarantine_row
from services.value_serializer import SQL_NULL_SENTINEL


def test_collapse_keep_last_collapses_empty_keys_and_keeps_null_keys():
    rows = [
        {"id": "", "v": 1},
        {"id": "", "v": 2},
        {"id": "a", "v": 3},
        {"id": None, "v": 4},
        {"id": SQL_NULL_SENTINEL, "v": 5},
    ]
    kept, dropped = collapse_keep_last(rows, ["id"])
    assert dropped == 1
    assert [r["v"] for r in kept] == [2, 3, 4, 5]


def test_quarantine_row_keeps_an_empty_primary_key_as_its_identity():
    row = normalize_quarantine_row({
        "primary_key": ["id"],
        "pk_value": {"id": ""},
        "reason": "bad cell",
    })
    assert row["source_pk"] == ""
    assert row["source_pk_proven"] is True


def test_append_collision_probe_batch_keys_are_exact():
    from services.destination_key_collision_probe import batch_key_texts

    assert batch_key_texts(["", "a ", "a", None, SQL_NULL_SENTINEL]) == ["", "a ", "a"]


def test_quarantine_row_without_a_key_stays_unproven():
    row = normalize_quarantine_row({
        "primary_key": ["id"],
        "pk_value": {"id": SQL_NULL_SENTINEL},
        "reason": "bad cell",
    })
    assert row["source_pk"] is None
    assert row["source_pk_proven"] is False
