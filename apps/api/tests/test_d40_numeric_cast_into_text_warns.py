"""D40 — cast_integer into a TEXT column strips leading zeros with no warning.

An operator-chosen integer cast (``transform_override``) on a code column
written into TEXT turns ``00042`` into ``42``. The text destination could
have held the original; Validate must say which sampled values change.
"""

from __future__ import annotations

import pytest


def _preflight(values, *, transform="cast_integer", target_type="TEXT"):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["code"],
        column_types={"code": "VARCHAR"},
        row_count=len(values),
        mappings=[
            {
                "source": "code",
                "target": "code",
                "confidence": 0.99,
                "transform": transform,
                "transform_override": True,
                "target_type": target_type,
            }
        ],
        destination_connected=True,
        sample_rows=[{"code": v} for v in values],
        destination_column_types={"code": target_type},
        destination_table_exists=True,
        destination_db_type="sqlite",
        skip_population_fit=True,
    )


def _cast_warning(result):
    return next(
        (w for w in result.get("warnings") or [] if isinstance(w, dict) and w.get("id") == "numeric_cast_into_text"),
        None,
    )


@pytest.mark.parametrize("transform", ["cast_integer", "integer", "to_int"])
def test_integer_cast_into_text_warns_with_the_rewritten_values(transform):
    result = _preflight(["00042", "007", "123"], transform=transform)
    warning = _cast_warning(result)
    assert warning, result.get("warnings")
    assert "00042" in warning["message"] and "'42'" in warning["message"]
    assert warning["details"]["columns"][0]["column"] == "code"
    # A warning, not a refusal: the operator may want the normalisation.
    assert not any("numeric_cast_into_text" == str(b.get("id")) for b in result.get("blockers") or [])


def test_no_warning_when_no_sampled_value_changes():
    assert _cast_warning(_preflight(["42", "7", "123"])) is None


def test_no_warning_for_an_integer_target():
    assert _cast_warning(_preflight(["00042"], target_type="INTEGER")) is None
