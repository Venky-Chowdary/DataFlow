"""A CDC apply and its value proof both honour the transforms the plan stamped.

A MySQL -> PostgreSQL ``cdc_incremental`` with ``source_timezone=UTC`` failed
Gate-8 on clean data ("digests differ ... no differing cell"): the CDC value
proof declined any mapping that carried ``assume_timezone:`` or ``json``, so
reconciliation stayed on the COUNT diagnostic, which by design never passes.
"""

from __future__ import annotations

from datetime import datetime, timezone

from services import cdc_value_digest as cvd

ZONE = "assume_timezone:America/New_York"


def _m(source: str, transform: str = "", target_type: str = "") -> dict:
    return {"source": source, "target": source, "transform": transform, "target_type": target_type}


def test_value_proof_accepts_json_and_declared_zone_carries():
    pairs = cvd._identity_pairs([_m("id", "integer"), _m("doc", "json"), _m("ts", ZONE)])
    assert pairs == [("id", "id"), ("doc", "doc"), ("ts", "ts")]


def test_value_proof_still_declines_a_value_changing_transform():
    assert cvd._identity_pairs([_m("id", "integer"), _m("name", "upper")]) is None


def test_declared_zone_is_attached_to_the_source_cell_before_the_fingerprint():
    zones = cvd._declared_zones([_m("ts", ZONE), _m("n", "integer")])
    assert zones == {"ts": ZONE}  # the zone name keeps its case
    src = cvd._attach_zones([{"ts": datetime(2024, 1, 2, 3, 4, 5), "n": 1}], zones)
    dest = [{"ts": datetime(2024, 1, 2, 8, 4, 5, tzinfo=timezone.utc), "n": 1}]
    proof = cvd.compare_row_sets(
        src, dest, ["ts", "n"], engine="postgresql", dest_types={"ts": "TIMESTAMPTZ", "n": "BIGINT"}
    )
    assert proof.matched, proof


def test_a_wrong_zone_does_not_match():
    src = cvd._attach_zones([{"ts": datetime(2024, 1, 2, 3, 4, 5)}], {"ts": ZONE})
    dest = [{"ts": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)}]
    proof = cvd.compare_row_sets(
        src, dest, ["ts"], engine="postgresql", dest_types={"ts": "TIMESTAMPTZ"}
    )
    assert not proof.matched


def test_null_source_cells_are_left_alone_by_the_zone():
    assert cvd._attach_zones([{"ts": None}], {"ts": ZONE}) == [{"ts": None}]
