"""D72 (QA batch 1) — a PostgreSQL ``uuid`` into its exact wire scored below identity.

Snowflake has no UUID type; Datawrap lands a uuid as ``VARCHAR(36)``, its
exact canonical wire. The pairing still scored as a "type promotion" on an
existing table and in the create-new *widening* band (≤ 0.90), below the
same-name ``uuid`` → ``uuid`` identity, because the scorer compared logical
type names (``uuid`` vs ``string``) rather than what lands. The same flaw hit
MySQL ``CHAR(36)`` and SQL Server ``UNIQUEIDENTIFIER``.
"""

from __future__ import annotations

import pytest

from services.mapping_quality import is_exact_value_carrier, refine_mappings_with_quality

_SAMPLES = [
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12",
    "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13",
    "d0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14",
]

EXACT_CARRIERS = [
    ("snowflake", "VARCHAR(36)"),
    ("mysql", "CHAR(36)"),
    ("mysql", "VARCHAR(36)"),
    ("mariadb", "CHAR(36)"),
    ("oracle", "VARCHAR2(36)"),
    ("sqlserver", "UNIQUEIDENTIFIER"),
]


def _score(db: str, target_type: str, *, create_new: bool) -> dict:
    mapping = {
        "source": "u", "target": "u", "source_type": "uuid",
        "target_type": target_type, "confidence": 0.95, "transform": "none",
        "create_new": create_new,
    }
    return refine_mappings_with_quality(
        [mapping],
        source_schemas=[{"name": "u", "inferred_type": "uuid", "samples": _SAMPLES}],
        destination_db_type=db,
    )[0]


@pytest.mark.parametrize("create_new", [False, True])
@pytest.mark.parametrize("db,target_type", EXACT_CARRIERS)
def test_uuid_into_its_exact_wire_scores_like_identity(db, target_type, create_new):
    assert is_exact_value_carrier("uuid", target_type, db)
    identity = _score("postgresql", "UUID", create_new=create_new)
    carrier = _score(db, target_type, create_new=create_new)
    assert carrier["confidence"] == identity["confidence"], (carrier, identity)
    assert carrier["confidence_class"] == identity["confidence_class"]
    assert not carrier.get("requires_review")
    if not create_new:
        assert carrier["confidence_class"] == "exact_name_type"
        assert carrier["confidence"] >= 0.99


@pytest.mark.parametrize("target_type", ["VARCHAR(255)", "STRING", "TEXT", "CHAR(32)"])
def test_a_wider_or_unbounded_text_carrier_is_not_exact(target_type):
    # These drop the 36-character contract (CHAR(32) also truncates): they
    # must not be lifted into the exact band. Snowflake STRING / TEXT are its
    # VARCHAR(16777216), a lossless carrier graded as such (test below).
    assert not is_exact_value_carrier("uuid", target_type, "snowflake")
    assert _score("snowflake", target_type, create_new=False)["confidence"] < 0.99


def test_only_uuid_sources_are_exact_carriers():
    assert not is_exact_value_carrier("text", "VARCHAR(36)", "snowflake")
    assert not is_exact_value_carrier("varchar(36)", "CHAR(36)", "mysql")


def test_a_rewriting_transform_is_not_lifted():
    mapping = {
        "source": "u", "target": "u", "source_type": "uuid",
        "target_type": "VARCHAR(36)", "confidence": 0.95, "transform": "uppercase",
    }
    out = refine_mappings_with_quality(
        [mapping],
        source_schemas=[{"name": "u", "inferred_type": "uuid", "samples": _SAMPLES}],
        destination_db_type="snowflake",
    )[0]
    assert out["confidence_class"] != "exact_name_type"


def test_create_new_plan_into_snowflake_scores_uuid_at_identity():
    """The Map pipeline's own create-new projection, not a hand-built row."""
    from services.mapping_pipeline import run_mapping_pipeline

    def plan(db: str) -> dict:
        schemas = [
            {"name": "id", "inferred_type": "integer", "samples": ["1", "2", "3"]},
            {"name": "u", "inferred_type": "uuid", "samples": _SAMPLES},
        ]
        out = run_mapping_pipeline(
            ["id", "u"], ["id", "u"], source_schemas=schemas, use_llm=False,
            destination_db_type=db, source_db_type="postgresql",
            destination_table_exists=False, source_types_authoritative=True,
            schema_policy="create_new",
        )
        return next(m for m in out["mappings"] if m["source"] == "u")

    sf, pg = plan("snowflake"), plan("postgresql")
    assert str(sf.get("target_type")).upper() == "VARCHAR(36)", sf
    assert sf["confidence"] >= pg["confidence"], (sf, pg)
    assert not sf.get("requires_review")


_SNOWFLAKE_TEXT_SPELLINGS = ["STRING", "TEXT", "VARCHAR", "VARCHAR(16777216)"]


@pytest.mark.parametrize("create_new", [False, True])
def test_snowflake_string_and_varchar_16777216_grade_identically(create_new):
    """Ruling (QA batch 1 / PR #435): Snowflake STRING is VARCHAR(16777216).

    Every spelling of that one type grades the same: a lossless carrier for a
    canonical UUID (no truncation, no lossy-domain hold), and not the exact
    36-character contract (below the exact band).
    """
    from services.type_coercion import is_precision_collapse_coercion
    from services.type_system import is_lossy_coercion

    graded = {
        t: (
            _score("snowflake", t, create_new=create_new)["confidence"],
            _score("snowflake", t, create_new=create_new)["confidence_class"],
            bool(_score("snowflake", t, create_new=create_new).get("requires_review")),
            is_lossy_coercion("uuid", t, dest_db="snowflake"),
            is_precision_collapse_coercion("uuid", t, dest_db="snowflake"),
        )
        for t in _SNOWFLAKE_TEXT_SPELLINGS
    }
    assert len(set(graded.values())) == 1, graded
    conf, _cls, review, lossy, collapse = graded["STRING"]
    assert not review and not lossy and not collapse
    exact = _score("snowflake", "VARCHAR(36)", create_new=create_new)["confidence"]
    assert conf <= exact
    for t in _SNOWFLAKE_TEXT_SPELLINGS:
        assert not is_exact_value_carrier("uuid", t, "snowflake")
