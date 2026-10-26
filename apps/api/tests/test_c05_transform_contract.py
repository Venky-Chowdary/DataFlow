"""C05: transforms never silently do the wrong thing (D9, D13, D14, D15, D40).

- every column_transforms key resolves to a real column or the plan refuses
- every spec resolves to a transform the engine applies, or refuses
- upper/lower match SQL semantics (no trim, per-character)
- describe_connector_transforms lists semantics/determinism/lossiness
- redact is deterministic and provable (fingerprint is not skipped)
- cast_integer is labelled lossy, never preserve
"""
from __future__ import annotations


def test_transform_unknown_column_refused() -> None:
    from src.ai.copilot.transfer_tools import _apply_column_transforms

    mappings = [{"source": "id", "target": "id"}, {"source": "email", "target": "email"}]
    for spec in ("upper", "hash_pii", "redact", "mask_pii"):
        _, refusal = _apply_column_transforms(mappings, {"nosuchcol": spec})
        assert refusal, spec
        assert "nosuchcol" in refusal
        assert "matches no source/target column" in refusal


def test_transform_unknown_spec_refused() -> None:
    from src.ai.copilot.transfer_tools import _apply_column_transforms

    mappings = [{"source": "id", "target": "id"}]
    _, refusal = _apply_column_transforms(mappings, {"id": "frobnicate"})
    assert refusal and "frobnicate" in refusal


def test_transform_typo_does_not_ship_raw_pii() -> None:
    from src.ai.copilot.transfer_tools import _apply_column_transforms

    mappings = [{"source": "email", "target": "email"}]
    out, refusal = _apply_column_transforms(mappings, {"emial": "hash_pii"})
    assert refusal and out == []


def test_transform_known_column_stamps() -> None:
    from src.ai.copilot.transfer_tools import _apply_column_transforms

    mappings = [{"source": "name", "target": "name"}]
    out, refusal = _apply_column_transforms(mappings, {"name": "upper"})
    assert refusal == ""
    assert out[0]["transform"] == "upper" and out[0]["transform_override"] is True


def test_upper_lower_do_not_trim() -> None:
    from services.transform_engine import apply_transform

    assert apply_transform("  Alice  ", "upper") == ("  ALICE  ", None)
    assert apply_transform("  Alice  ", "lower") == ("  alice  ", None)


def test_replace_ambiguous_argument_refused() -> None:
    from services.transform_engine import normalize_engine_transform

    # 'replace:-:_,upper' looks like a chained op — refuse, never silently
    # apply the literal replacement '_,upper'.
    assert normalize_engine_transform("replace:-:_,upper") is None
    assert normalize_engine_transform("replace:-:_,lower") is None
    # Legitimate args still parse.
    assert normalize_engine_transform("replace:-:__") == "replace:-:__"
    assert normalize_engine_transform("replace:a:b") == "replace:a:b"


def test_describe_connector_transforms_tool_lists_contract() -> None:
    from src.ai.copilot.tools import TOOL_DEFINITIONS, get_pilot_tools

    names = {t["name"] for t in TOOL_DEFINITIONS}
    assert "describe_connector_transforms" in names
    result = get_pilot_tools().execute("describe_connector_transforms", {})
    assert result.success
    names_out = {t["transform"] for t in result.output["transforms"]}
    assert {"upper", "redact", "mask_pii", "hash_pii", "integer"} <= names_out
    redact = next(t for t in result.output["transforms"] if t["transform"] == "redact")
    assert redact["deterministic"] is True and redact["lossy"] is True


def test_redact_is_deterministic_and_provable() -> None:
    """D15: redact has a value implementation — the proof re-applies it."""
    from services.shape_engine import _transform_value

    step = type("S", (), {"op": "redact", "options": {}})()
    assert _transform_value(step, "alice") == "<redacted>"
    assert _transform_value(step, "bob") == "<redacted>"


def test_cast_integer_fidelity_is_lossy() -> None:
    from services.mapping_proof import transform_fidelity

    # '00042' -> '42' strips leading zeros: never "preserve" (D40).
    assert transform_fidelity("cast_integer") == "lossy_cast"
    assert transform_fidelity("integer") == "lossy_cast"
    assert transform_fidelity("none") == "preserve"


def test_documented_aliases_resolve() -> None:
    from services.transform_engine import normalize_engine_transform

    # Spellings the docs advertise must not be refused (C05 minor).
    assert normalize_engine_transform("normalize_email") == "email"
    assert normalize_engine_transform("date_to_iso") == "date"


def test_transform_semantics_covers_known_ops() -> None:
    from services.transform_engine import KNOWN_TRANSFORMS, TRANSFORM_SEMANTICS

    missing = KNOWN_TRANSFORMS - set(TRANSFORM_SEMANTICS)
    assert not missing, f"ops without a declared contract: {missing}"
    for name, meta in TRANSFORM_SEMANTICS.items():
        assert meta["semantics"], name
        assert isinstance(meta["deterministic"], bool), name
        assert isinstance(meta["lossy"], bool), name
