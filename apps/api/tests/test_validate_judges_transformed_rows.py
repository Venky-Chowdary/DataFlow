"""A gate must not refuse a value the approved transform removes before the write.

Execute shapes rows on the read. Validate used to score the raw source, so an
operator who added a transform to strip a control character, or to round a
fractional value into an integer column, watched Validate block on exactly the
value the transform deletes. These run the real gates over both images.
"""

from __future__ import annotations

from services.preflight_service import run_file_preflight
from services.shape_preflight import shaped_preflight_image

CONTROL = "\u0001"


def _preflight(*, columns, column_types, mappings, rows, dest_types, db="postgresql"):
    return run_file_preflight(
        columns=columns,
        column_types=column_types,
        row_count=len(rows),
        mappings=mappings,
        destination_connected=True,
        destination_column_types=dest_types,
        destination_db_type=db,
        estimated_bytes=4096,
        sample_rows=rows,
    )


def _findings(result: dict) -> str:
    parts = [str(b.get("message") or "") for b in result.get("blockers") or []]
    parts += [str(w) for w in result.get("warnings") or []]
    parts += [str(g.get("message") or "") for g in result.get("gates") or []]
    return " | ".join(parts).casefold()


def test_a_control_character_the_transform_strips_is_not_a_validate_finding() -> None:
    mappings = [{"source": "note", "target": "note", "confidence": 1.0, "target_type": "VARCHAR(40)"}]
    rows = [{"note": f"ok{CONTROL}"}, {"note": "fine"}]
    dest_types = {"note": "VARCHAR(40)"}

    raw = _preflight(
        columns=["note"],
        column_types={"note": "VARCHAR"},
        mappings=mappings,
        rows=rows,
        dest_types=dest_types,
    )
    assert "control" in _findings(raw), "the raw source really does carry the defect"

    image = shaped_preflight_image(
        {"steps": [{"op": "strip_characters", "column": "note", "options": {"characters": "non_printable"}}]},
        columns=["note"],
        column_types={"note": "VARCHAR"},
        sample_rows=rows,
    )
    shaped = _preflight(
        columns=image.columns,
        column_types=image.column_types,
        mappings=mappings,
        rows=image.sample_rows or [],
        dest_types=dest_types,
    )
    assert "control" not in _findings(shaped)


def test_a_fractional_value_rounded_by_the_transform_fits_the_integer_column() -> None:
    """The MySQL ``ARR_TIME`` failure, answered at design time instead of row 1."""
    mappings = [{"source": "arr_time", "target": "arr_time", "confidence": 1.0, "target_type": "INT"}]
    rows = [{"arr_time": "22.433332"}, {"arr_time": "21.833334"}]

    raw = _preflight(
        columns=["arr_time"],
        column_types={"arr_time": "DECIMAL(12,9)"},
        mappings=mappings,
        rows=rows,
        dest_types={"arr_time": "INT"},
        db="mysql",
    )
    assert raw["passed"] is False

    image = shaped_preflight_image(
        {"steps": [{"op": "round_number", "column": "arr_time", "options": {"places": 0}}]},
        columns=["arr_time"],
        column_types={"arr_time": "DECIMAL(12,9)"},
        sample_rows=rows,
    )
    assert image.sample_rows is not None
    assert [str(r["arr_time"]) for r in image.sample_rows] == ["22", "22"]

    shaped = _preflight(
        columns=image.columns,
        column_types=image.column_types,
        mappings=mappings,
        rows=image.sample_rows,
        dest_types={"arr_time": "INT"},
        db="mysql",
    )
    assert image.column_types["arr_time"] == "INTEGER"
    assert "invalid integer" not in _findings(shaped)
    assert not [
        b for b in shaped.get("blockers") or [] if "integer" in str(b.get("message") or "").casefold()
    ]
    assert shaped["passed"] is True, _findings(shaped)
