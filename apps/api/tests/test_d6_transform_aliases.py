"""D6: column_transforms natural-language aliases + discoverable list.

"lowercase" was refused as Unknown transform while the engine id "lower"
worked. Aliases resolve to the canonical id, and the refusal message names
where the valid list lives.
"""

from __future__ import annotations


def test_lowercase_alias_resolves_to_lower():
    from services.transform_engine import normalize_engine_transform

    assert normalize_engine_transform("lowercase") == "lower"
    assert normalize_engine_transform("uppercase") == "upper"
    assert normalize_engine_transform("strip") == "trim"
    assert normalize_engine_transform("lcase") == "lower"
    assert normalize_engine_transform("ucase") == "upper"


def test_alias_applies_the_real_transform():
    from services.transform_engine import apply_transform

    value, err = apply_transform("  MiXeD  ", "lowercase")
    assert err is None
    # D13: SQL LOWER/UPPER do not trim — per-character casing only; the
    # explicit `trim` op owns whitespace stripping.
    assert value == "  mixed  "


def test_unknown_transform_message_names_the_valid_list():
    from services.transform_engine import apply_transform

    _, err = apply_transform("x", "totally_bogus")
    assert "Unknown transform" in (err or "")
    assert "KNOWN_TRANSFORMS" in err or "valid names" in err


def test_known_transforms_exposed_for_describe():
    """Pilot/describe surfaces can enumerate the canonical registry."""
    from services.transform_engine import KNOWN_TRANSFORMS

    assert isinstance(KNOWN_TRANSFORMS, frozenset)
    assert {"lower", "upper", "trim"} <= KNOWN_TRANSFORMS
