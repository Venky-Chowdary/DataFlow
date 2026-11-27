"""Accuracy review finding 8: type grading checked against an independent oracle.

tests/fixtures/type_fidelity_truth_table.json is a hand-reviewed table of risky
type families (Oracle FLOAT/NUMBER, byte vs character lengths on Redshift,
Oracle and MySQL utf8mb3, money, unsigned integers, time zones and offsets,
MongoDB millisecond dates, booleans, binary, JSON), each row with vendor
documentation citations. It is not derived from the product's predicate.

* ``loses``: the classifier must not grade the pair a safe path, and it must
  need a Risk Contract (the fidelity predicate calls it lossy or the contract
  requires approval);
* ``loses_if_values`` / ``loses_disclosed``: a safe grade must carry the
  ``unmeasured_loss`` sentence (a dropped UTC offset is loaded as the instant
  by product decision D136 and disclosed);
* ``keeps``: the product's carrier must not be called lossy (no refused
  second run of the product's own table).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.conversion_contract import ConversionClass, classify_conversion
from services.source_engine_scope import bind_source_engine
from services.type_coercion import is_lossy_coercion
from services.type_system import create_new_mapping_target_type

TABLE = json.loads((Path(__file__).parent / "fixtures" / "type_fidelity_truth_table.json").read_text())
SAFE = {
    ConversionClass.IDENTITY.value, ConversionClass.EQUIVALENT.value, ConversionClass.LOSSLESS.value,
    ConversionClass.WIDENING.value, ConversionClass.REPRESENTATION.value, ConversionClass.NORMALIZATION.value,
}
ROWS = TABLE["rows"]
# Test ids avoid the word "mongodb": a conftest hook skips such ids without a live MongoDB.
IDS = [f"{r['engine']}:{r['source_type']}->{r['dest'].replace('mongodb', 'bson')}:{r['target']}" for r in ROWS]


def test_every_row_is_cited() -> None:
    assert len(ROWS) >= 40
    for r in ROWS:
        assert r["truth"] in TABLE["truth_values"]
        assert r["citations"] and all(c.startswith("https://") for c in r["citations"]), r


@pytest.mark.parametrize("r", ROWS, ids=IDS)
def test_grade_agrees_with_the_truth_table(r) -> None:
    with bind_source_engine(r["engine"]):
        target = (create_new_mapping_target_type(r["source_type"], r["dest"], source_db=r["engine"])
                  if r["target"] == "create_new" else r["target"])
        conv = classify_conversion(r["source_type"], target, dest_db=r["dest"])
        lossy = bool(is_lossy_coercion(r["source_type"], target, dest_db=r["dest"], source_db=r["engine"],
                                       dest_table_exists=True))
    cls = conv["conversion_class"]
    if r["truth"] == "loses":
        assert cls not in SAFE and (lossy or conv.get("requires_risk_contract")), (target, conv)
    elif r["truth"] in ("loses_if_values", "loses_disclosed"):
        assert cls not in SAFE or conv.get("unmeasured_loss"), (target, conv)
    else:
        assert not lossy, (target, conv)
