"""X-85: Mongo source — missing-field and mixed-type findings in the run result.

A missing field and an explicit null both land NULL on SQL destinations, and a
field that is int in some docs and string in others is widened to TEXT — both
must surface as typed info findings, never silently.
"""

from services.schema_introspect import (
    _finalize_mongodb_type_with_note,
    _mongodb_types_with_notes,
)
from services.data_integrity import _mongo_sparse_field_findings


def test_missing_field_counted_per_doc() -> None:
    docs = [
        {"a": 1, "b": "x", "c": None},
        {"a": 2, "b": "y"},
        {"a": 3, "b": 9},
        {"b": "z"},  # a missing here; c explicit-null only
    ]
    types, _notes, details = _mongodb_types_with_notes(docs)
    assert types["a"] == "INTEGER"
    assert details["a"]["missing"] == 1
    assert details["a"]["present"] == 3
    # explicit null is not "missing" and not a type vote
    assert details["b"]["missing"] == 0


def test_mixed_int_string_widens_to_text_with_note() -> None:
    chosen, note = _finalize_mongodb_type_with_note({"INTEGER": 5, "TEXT": 5})
    assert chosen == "TEXT"
    assert note is not None and "widened to TEXT" in note
    assert "10" in note


def test_typed_majority_keeps_sentinel_note_not_widened() -> None:
    chosen, note = _finalize_mongodb_type_with_note({"INTEGER": 49, "TEXT": 1})
    assert chosen == "INTEGER"
    assert note is not None and "sentinel" in note and "widened" not in note


def test_findings_payload_has_both_kinds() -> None:
    schemas = [
        {"name": "a", "missing_docs": 2},
        {
            "name": "b",
            "type_mix_detail": {
                "families": {"INTEGER": 1, "TEXT": 3},
                "docs": 4,
                "widened": True,
            },
        },
        {"name": "ok"},
    ]
    warnings, findings = _mongo_sparse_field_findings(schemas)
    kinds = {f["kind"] for f in findings}
    assert kinds == {"missing_field", "mixed_type_widened"}
    miss = next(f for f in findings if f["kind"] == "missing_field")
    assert miss["field"] == "a" and miss["docs"] == 2
    assert miss["note"] == "landed NULL"
    wide = next(f for f in findings if f["kind"] == "mixed_type_widened")
    assert wide["field"] == "b" and wide["docs"] == 4
    assert wide["note"] == "widened to TEXT"
    assert any("field a missing in 2 docs, landed NULL" in w for w in warnings)
    assert any("widened to TEXT" in w for w in warnings)


def test_clean_schema_yields_no_findings() -> None:
    warnings, findings = _mongo_sparse_field_findings([{"name": "ok"}])
    assert warnings == [] and findings == []


def test_neg_zero_counted_and_finding_emitted() -> None:
    # X-84: BSON -0.0 lands 0 on PG/SQLite; the fold is consistent on both
    # sides so the run stays verified — but must say so, not stay silent.
    _types, _notes, details = _mongodb_types_with_notes(
        [{"v": -0.0}, {"v": 1.5}, {"v": -0.0}, {"v": 0.0}]
    )
    assert details["v"]["neg_zero"] == 2
    warnings, findings = _mongo_sparse_field_findings(
        [{"name": "v", "neg_zero_docs": 2}]
    )
    assert findings[0]["kind"] == "neg_zero_folded"
    assert findings[0]["docs"] == 2
    assert any("-0.0" in w for w in warnings)


def test_neg_zero_not_emitted_for_positive_zero() -> None:
    _types, _notes, details = _mongodb_types_with_notes([{"v": 0.0}])
    assert "neg_zero" not in details["v"]
