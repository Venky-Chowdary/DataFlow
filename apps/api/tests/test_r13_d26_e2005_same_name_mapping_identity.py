"""D26 / E2-005 / D55 — a column's own name on the destination is its identity.

D26: ``amt``/``qty`` TEXT into an existing NUMERIC/INT4 column of the same name
scored 0.63 (below every floor) even after the fidelity owner measured the
values and graded the write lossless, so a signed contract on ``d``, lenient
mode and explicit cast transforms all stayed refused; and G4 surfaced its
refusals one layer at a time (contract first, then the floor).

E2-005: ``ssn_masked`` and ``ssn_sha256`` exist on both sides; when the
same-name pairs had a type risk the bipartite solver crossed them at 0.82.
A literal name equality is never traded for a type fit.

D55: the identifier DataFlow itself created (``Order ID`` -> ``Order_ID``,
``naïve col`` -> ``na_ve_col``, ``Café`` -> ``cafe`` / legacy ``Caf_``) maps at
the exact band, so a new run or schedule into DataFlow's own table clears G4.
"""

from __future__ import annotations

import logging

import pytest

from services.mapping_pipeline import run_mapping_pipeline


@pytest.fixture(autouse=True)
def _quiet_mapping_warnings(caplog):
    """Mute the mapper's per-column WARNINGs for this module only.

    Scoped through ``caplog`` so the level is restored after each test: a
    module-level ``setLevel`` runs at collection time and silenced every
    ``services.*`` WARNING/DEBUG for the whole session (CI shards 3 and 4).
    """
    caplog.set_level(logging.ERROR, logger="services")


def _S(name, typ, samples=None):
    return {"name": name, "inferred_type": typ, "samples": samples or []}


def _T(name, typ):
    return {"name": name, "inferred_type": typ}


def _plan(src, tgt, *, dest="postgresql", source_db="postgresql", **kw):
    out = run_mapping_pipeline(
        [s["name"] for s in src],
        [t["name"] for t in tgt],
        source_schemas=src,
        target_schemas=tgt,
        destination_db_type=dest,
        source_db_type=source_db,
        destination_table_exists=True,
        use_llm=False,
        **kw,
    )
    return {m["source"]: m for m in out["mappings"]}


_D26_SRC = [
    _S("id", "INTEGER", ["1", "2", "3"]),
    _S("amt", "TEXT", ["1.5", "2.5", "3", " 4.25 "]),
    _S("qty", "TEXT", ["1", "2", "3", "40"]),
    _S("d", "TEXT", ["2024-01-01", "2024-01-02", "2024-13-45"]),
]
_D26_DST = [_T("id", "INTEGER"), _T("amt", "NUMERIC(10,2)"), _T("qty", "INTEGER"), _T("d", "DATE")]


@pytest.mark.parametrize(
    "kw",
    [
        {},
        {"validation_mode": "lenient", "confidence_threshold": 0.75},
        {"prior_mappings": [
            {"source": "amt", "target": "amt", "transform": "cast_number"},
            {"source": "qty", "target": "qty", "transform": "cast_integer"},
        ]},
    ],
    ids=["strict", "lenient", "explicit_casts"],
)
def test_same_name_text_into_typed_column_clears_the_floor(kw):
    plan = _plan(_D26_SRC, _D26_DST, **kw)
    for col in ("amt", "qty"):
        m = plan[col]
        assert m["target"] == col, m
        assert m["fidelity"] in {"preserve", "normalizing"}, m
        assert float(m["confidence"]) >= 0.85, (col, m["confidence"], m["reasoning"])
        assert not m["requires_review"], m
    # A value that does not parse still needs the operator's contract.
    assert plan["d"]["target"] == "d"
    assert plan["d"]["fidelity"] == "lossy_cast"


def _g4(rows, columns):
    from preflight.gates import gate_g4_mapping_confidence
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )

    plan = TransferPlan(
        source=SourceConfig(
            kind="database", connected=True, parseable=True,
            columns=[ColumnSchema(name=c, inferred_type="TEXT") for c in columns],
        ),
        destination=DestinationConfig(
            kind="database", db_type="postgresql", connected=True,
            table_exists=True, can_write=True, target_columns=[],
        ),
        mappings=[ColumnMapping(**r) for r in rows],
        confidence_threshold=0.85,
        validation_mode="strict",
    )
    return gate_g4_mapping_confidence(PreflightContext(plan=plan))


def test_g4_names_every_failing_layer_in_one_refusal():
    result = _g4(
        [
            {"source": "d", "target": "d", "confidence": 0.63, "requires_review": True,
             "fidelity": "lossy_cast", "transform": "date"},
            {"source": "amt", "target": "amt_x", "confidence": 0.63,
             "requires_review": False, "fidelity": "preserve", "transform": "decimal"},
        ],
        ["d", "amt"],
    )
    assert result.status.value == "block", result
    assert "Migration Risk Contract" in result.message, result.message
    assert "below floor" in result.message, result.message
    assert set(result.details) >= {"risk_unacknowledged", "low_confidence"}, result.details


_SSN_SRC = [
    _S("id", "INTEGER", ["1", "2"]),
    _S("ssn_masked", "VARCHAR(64)", ["a" * 64, "b" * 64]),
    _S("ssn_sha256", "VARCHAR(11)", ["***-**-1234", "***-**-5678"]),
]
_SSN_DST = [_T("id", "INT"), _T("ssn_masked", "NVARCHAR(11)"), _T("ssn_sha256", "NVARCHAR(64)")]


@pytest.mark.parametrize("dest", ["sqlserver", "postgresql", "mysql"])
def test_exact_name_columns_are_never_crossed(dest):
    plan = _plan(_SSN_SRC, _SSN_DST, dest=dest)
    assert plan["ssn_masked"]["target"] == "ssn_masked", plan["ssn_masked"]
    assert plan["ssn_sha256"]["target"] == "ssn_sha256", plan["ssn_sha256"]
    # The same-name pair's type risk is graded, not hidden by a swap.
    assert plan["ssn_masked"]["fidelity"] == "lossy_cast"


@pytest.mark.parametrize("dest", ["sqlite", "postgresql", "snowflake"])
def test_dataflow_created_identifiers_map_at_the_exact_band(dest):
    src = [
        _S("Order ID", "INTEGER", ["1", "2"]),
        _S("naïve col", "TEXT", ["a", "", "  "]),
        _S("col-with-dash", "TEXT", ["x", "东京"]),
        _S("Café", "TEXT", ["a", None]),
        _S("café legacy", "TEXT", ["b", "c"]),
    ]
    tgt = [
        _T("Order_ID", "BIGINT"), _T("na_ve_col", "TEXT"), _T("col_with_dash", "TEXT"),
        _T("cafe", "TEXT"), _T("caf__legacy", "TEXT"),
    ]
    plan = _plan(src, tgt, dest=dest)
    want = {
        "Order ID": "Order_ID", "naïve col": "na_ve_col", "col-with-dash": "col_with_dash",
        "Café": "cafe", "café legacy": "caf__legacy",
    }
    for source, target in want.items():
        m = plan[source]
        assert m["target"].lower() == target.lower(), m
        assert float(m["confidence"]) >= 0.97, (source, m["confidence"], m["reasoning"])
        assert not m["requires_review"], m


def test_one_name_predicate_owns_every_exact_name_decision():
    from services.mapping_constraints import enforce_destination_constraints
    from services.semantic_mapper import _exact_name_unambiguous, name_identical

    assert name_identical("Café", "cafe") and name_identical("Order ID", "Order_ID")
    assert name_identical("café", "caf_")  # the pre-transliteration sanitiser spelling
    assert not name_identical("_id", "id")  # a leading underscore is a different column
    assert _exact_name_unambiguous("Café", "cafe", ["cafe", "id"]) is True
    assert _exact_name_unambiguous("UserID", "userid", ["userid", "user_id"]) is False
    kept, _dropped, _blocked = enforce_destination_constraints(
        [{"source": "Café", "target": "Café", "confidence": 0.99}], ["cafe", "id"]
    )
    assert kept and kept[0]["target"] == "cafe", kept
