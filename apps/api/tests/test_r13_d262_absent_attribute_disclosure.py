"""D262 — the absent-attribute rule is one rule, disclosed where it applies.

``stream_db_chunk`` writes NULL for a mapped attribute a schemaless record does
not carry when the destination is relational and the run is not CDC. For a
sparse-by-design item store (DynamoDB, Redis) that changes "left alone" into
"nulled" on upsert, so the plan / preflight says so before the run.
"""

from __future__ import annotations

from services.schemaless_absence import (
    ABSENT_ATTRIBUTE_NULL_NOTE,
    WARNING_ID,
    absent_attribute_nulls_column,
    attach_absent_attribute_notice,
)


def test_rule_applies_to_schemaless_into_relational_outside_cdc():
    assert absent_attribute_nulls_column("mongodb", "sqlserver", "incremental_deduped")
    assert absent_attribute_nulls_column("dynamodb", "postgresql", "full_refresh_overwrite")
    assert absent_attribute_nulls_column("redis", "mysql", "incremental_deduped")
    assert not absent_attribute_nulls_column("mongodb", "mongodb", "incremental_deduped")
    assert not absent_attribute_nulls_column("mongodb", "postgresql", "cdc")
    assert not absent_attribute_nulls_column("postgresql", "mysql", "incremental_deduped")


def test_preflight_disclosure_appears_once_and_only_when_it_applies():
    out: dict = {"warnings": [{"id": "other", "message": "x"}]}
    attach_absent_attribute_notice(out, "dynamodb", "postgresql", "incremental_deduped")
    attach_absent_attribute_notice(out, "dynamodb", "postgresql", "incremental_deduped")
    ids = [w["id"] for w in out["warnings"]]
    assert ids == ["other", WARNING_ID]
    assert "null the destination column on upsert" in out["warnings"][1]["message"]
    assert out["warnings"][1]["message"] == ABSENT_ATTRIBUTE_NULL_NOTE
    untouched: dict = {}
    attach_absent_attribute_notice(untouched, "postgresql", "mysql", "incremental_deduped")
    attach_absent_attribute_notice(untouched, "mongodb", "postgresql", "cdc")
    assert "warnings" not in untouched
