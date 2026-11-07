"""Case A first Validate — INTEGER → existing INT after round is not a G4 click.

Studio Continue already lets a preserve exact-name row through without Approve.
The mapping pipeline used to stamp ``requires_review`` anyway: canonicalize
widened INTEGER to BIGINT for the CREATE question, then ``is_lossy_coercion``
billed INTEGER → INT4 as a narrowing. G4 blocked first Validate
(``requires_review`` without ``user_override``) on a path fidelity graded
``preserve``. Approve eligible was ceremony, not a type risk.

The unshaped DECIMAL → INT path must still hold. A named-width BIGINT source
into INT4 must still hold.
"""

from __future__ import annotations

import pytest

from services.mapping_pipeline import run_mapping_pipeline
from services.preflight_service import run_file_preflight
from services.shape_preflight import shaped_preflight_image

CASE_A_ROWS = [
    {"id": 1, "arr_time": "22.6"},
    {"id": 2, "arr_time": "21.4"},
    {"id": 3, "arr_time": "22.0"},
]
CASE_A_RECIPE = {
    "steps": [{"op": "round_number", "column": "arr_time", "options": {"places": 0}}]
}
DECLARED = {"id": "INTEGER", "arr_time": "DECIMAL(12,9)"}

# Dialect family × dest spelling the operator actually meets on Map.
_EXISTING_INT_DESTS = (
    ("postgresql", "int4"),
    ("postgresql", "INT4"),
    ("postgresql", "INTEGER"),
    ("mysql", "INT"),
    ("mysql", "INTEGER"),
)


def _studio_wire(mappings: list[dict]) -> list[dict]:
    """What Transfer Studio sends on first Validate — no Approve click."""
    return [
        {
            "source": m["source"],
            "target": m["target"],
            "source_type": m.get("source_type"),
            "target_type": m.get("target_type"),
            "transform": m.get("transform") or "none",
            "fidelity": m.get("fidelity"),
            "type_narrowing": bool(m.get("type_narrowing")),
            "confidence": m.get("confidence"),
            "assignment_strategy": m.get("assignment_strategy"),
            "requires_review": bool(m.get("requires_review")),
            "user_override": False,
        }
        for m in mappings
    ]


def _findings(result: dict) -> str:
    parts = [str(b.get("message") or "") for b in result.get("blockers") or []]
    parts += [str(g.get("message") or "") for g in result.get("gates") or []]
    return " | ".join(parts)


@pytest.mark.parametrize("dest_db,dest_type", _EXISTING_INT_DESTS)
def test_rounded_integer_existing_int_is_first_pass_ready(dest_db: str, dest_type: str) -> None:
    image = shaped_preflight_image(
        CASE_A_RECIPE,
        columns=["id", "arr_time"],
        column_types=DECLARED,
        sample_rows=CASE_A_ROWS,
    )
    assert image.column_types["arr_time"] == "INTEGER"

    pipeline = run_mapping_pipeline(
        image.columns,
        ["id", "arr_time"],
        source_schemas=[
            {
                "name": "id",
                "inferred_type": image.column_types["id"],
                "samples": [str(row["id"]) for row in image.sample_rows or []],
            },
            {
                "name": "arr_time",
                "inferred_type": image.column_types["arr_time"],
                "samples": [str(row["arr_time"]) for row in image.sample_rows or []],
            },
        ],
        target_schemas=[
            {"name": "id", "inferred_type": dest_type, "samples": []},
            {"name": "arr_time", "inferred_type": dest_type, "samples": []},
        ],
        destination_db_type=dest_db,
        destination_table_exists=True,
        confidence_threshold=0.85,
        use_llm=False,
    )
    by_src = {m["source"]: m for m in pipeline["mappings"]}
    rounded = by_src["arr_time"]
    assert rounded["source_type"] == "INTEGER"
    assert rounded.get("fidelity") in {"preserve", None}
    assert not rounded.get("requires_review"), rounded.get("reasoning")
    assert "lossy type pair" not in str(rounded.get("reasoning") or "").lower()

    dest = {col: dest_type for col in ("id", "arr_time")}
    result = run_file_preflight(
        columns=image.columns,
        column_types=image.column_types,
        row_count=3,
        mappings=_studio_wire(pipeline["mappings"]),
        destination_connected=True,
        destination_column_types=dest,
        destination_db_type=dest_db,
        destination_table_exists=True,
        estimated_bytes=4096,
        sample_rows=image.sample_rows,
        confidence_threshold=0.85,
        validation_mode="strict",
    )
    text = _findings(result).casefold()
    assert "below the map confidence floor" not in text, (dest_db, dest_type, text)
    assert "ambiguous mapping" not in text, (dest_db, dest_type, text)
    assert result["passed"] is True, (dest_db, dest_type, text)


def test_unshaped_decimal_into_int_still_holds() -> None:
    pipeline = run_mapping_pipeline(
        ["arr_time"],
        ["arr_time"],
        source_schemas=[
            {
                "name": "arr_time",
                "inferred_type": "DECIMAL(12,9)",
                "samples": ["22.6", "21.4", "22.0"],
            }
        ],
        target_schemas=[{"name": "arr_time", "inferred_type": "int4", "samples": []}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        confidence_threshold=0.85,
        use_llm=False,
    )
    m = pipeline["mappings"][0]
    assert m.get("fidelity") == "lossy_cast"
    assert m.get("requires_review") is True


def test_named_width_bigint_into_int4_still_holds() -> None:
    pipeline = run_mapping_pipeline(
        ["n"],
        ["n"],
        source_schemas=[{"name": "n", "inferred_type": "BIGINT", "samples": ["1"]}],
        target_schemas=[{"name": "n", "inferred_type": "int4", "samples": []}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        confidence_threshold=0.85,
        use_llm=False,
    )
    m = pipeline["mappings"][0]
    assert m["source_type"] == "BIGINT"
    assert m.get("requires_review") is True
    assert m.get("fidelity") in {"lossy_cast", "cast"} or m.get("type_narrowing")
