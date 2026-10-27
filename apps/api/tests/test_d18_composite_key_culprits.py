"""D18 — the composite-key culprit list dropped the other key columns.

Gate-8's ladder indexed rows by the first key column only. For a
``(region, id)`` key the rows ``(eu, 1)`` and ``(us, 1)`` collapsed onto one
address, L5 compared the wrong pair, and the culprit it named was ``id=1`` —
not which of the two rows actually differed.
"""

from __future__ import annotations

from services.verification_ladder import run_five_layer_verification


def _rows(*triples):
    return [{"id": i, "region": r, "name": n} for r, i, n in triples]


def test_composite_key_culprit_names_every_key_column():
    source = _rows(("eu", 1, "alpha"), ("us", 1, "bravo"), ("eu", 2, "charlie"))
    target = _rows(("eu", 1, "alpha"), ("us", 1, "TAMPERED"), ("eu", 2, "charlie"))

    ladder = run_five_layer_verification(
        source_rows=source,
        target_rows=target,
        columns=["id", "region", "name"],
        pk_column="id",
        pk_columns=["id", "region"],
    )

    assert not ladder["passed"]
    rows = ladder["localization"]["rows"]
    assert rows, ladder
    culprit = rows[0]
    assert culprit["pk"] == "id=1, region=us"
    assert culprit["pk_column"] == "id, region"
    assert culprit["column"] == "name"
    assert culprit["source_value"] == "bravo"
    assert culprit["target_value"] == "TAMPERED"
    assert "(id=1, region=us)" in ladder["localization_summary"]
    # Exactly the one differing row — the shared id=1 is not a second culprit.
    assert {r["pk"] for r in rows} == {"id=1, region=us"}


def test_single_column_key_address_is_unchanged():
    ladder = run_five_layer_verification(
        source_rows=_rows(("eu", 1, "a"), ("eu", 2, "b")),
        target_rows=_rows(("eu", 1, "a"), ("eu", 2, "x")),
        columns=["id", "region", "name"],
        pk_column="id",
    )
    culprit = ladder["localization"]["rows"][0]
    assert culprit["pk"] == "2"
    assert culprit["pk_column"] == "id"
    assert "row id='2'" in ladder["localization_summary"]
