"""D8 — hash_pii without DATAFLOW_PII_HASH_KEY must fail closed AT VALIDATE,
with setup guidance, not as a raw runtime error.

Before: Validate blocked only when a sampled cell happened to be hashed, under
a generic "Sample transform / cast failures" root cause whose fix told the
operator to set the transform to none/identity (writing raw PII) or sign
CAST_AND_CONTINUE; an all-NULL sample passed Validate and the run died on the
first non-NULL value.
"""

from __future__ import annotations

import pytest

_KEY_VARS = (
    "DATAFLOW_PII_HASH_KEY",
    "DATAWRAP_PII_HASH_KEY",
    "DATAFLOW_SECRET",
    "DATAWRAP_SECRET",
)


@pytest.fixture()
def no_key(monkeypatch):
    """No pinned key and the workspace key store cannot answer (round 8: keys
    are per-workspace and auto-provisioned, so "missing" means unavailable)."""
    for name in _KEY_VARS:
        monkeypatch.delenv(name, raising=False)
    from services import pii_hash_keys

    def unavailable(_workspace_id):
        raise pii_hash_keys.PiiHashKeyUnavailable("store down")

    monkeypatch.setattr(pii_hash_keys, "workspace_key", unavailable)


def _preflight(sample_rows, *, transform="hash_pii", shape_recipe=None):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["email"],
        column_types={"email": "VARCHAR"},
        row_count=len(sample_rows),
        mappings=[
            {
                "source": "email",
                "target": "email",
                "confidence": 0.99,
                "transform": transform,
                "target_type": "VARCHAR",
            }
        ],
        destination_connected=True,
        sample_rows=sample_rows,
        destination_column_types={"email": "VARCHAR"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        skip_population_fit=True,
        shape_recipe=shape_recipe,
    )


def _operator_text(result) -> str:
    parts = []
    for b in result.get("blockers") or []:
        parts.append(str(b.get("message") or ""))
        guidance = b.get("guidance") or {}
        parts.append(str(guidance.get("fix") or ""))
    return " ".join(parts)


def _assert_setup_guidance(result):
    assert result.get("passed") is False
    text = _operator_text(result)
    assert "hashing key" in text, text
    assert "re-run Validate" in text or "Re-run Validate" in text, text
    # A tenant-facing message never names infrastructure (QA enterprise #19).
    for leak in ("PII_HASH_KEY", "DATAFLOW_", "DATAWRAP_", "Railway", "openssl", "Variables"):
        assert leak not in text, (leak, text)
    # The fix must never be "drop the hash" or "hold every row out".
    for b in result.get("blockers") or []:
        fix = str((b.get("guidance") or {}).get("fix") or "")
        if "hashing key" in str(b.get("message") or "") + fix:
            assert "identity" not in fix and "CAST_AND_CONTINUE" not in fix, fix


@pytest.mark.parametrize("transform", ["hash_pii", "sha256"])
def test_missing_key_blocks_validate_with_setup_guidance(no_key, transform):
    _assert_setup_guidance(_preflight([{"email": "ada@example.com"}], transform=transform))


def test_missing_key_blocks_even_when_no_sampled_cell_is_hashed(no_key):
    """An all-NULL sample never runs the digest; the key is still required."""
    _assert_setup_guidance(_preflight([{"email": None}]))


def test_missing_key_blocks_a_shape_recipe_hash_step(no_key):
    recipe = {"version": 1, "steps": [{"op": "hash_pii", "column": "email"}]}
    result = _preflight([{"email": None}], transform="none", shape_recipe=recipe)
    _assert_setup_guidance(result)


def test_configured_key_does_not_raise_the_setup_blocker(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "unit-test-pii-key")
    result = _preflight([{"email": "ada@example.com"}])
    assert "hashing key" not in _operator_text(result)


def test_unpinned_deployment_provisions_a_workspace_key_instead_of_blocking(monkeypatch, tmp_path):
    for name in _KEY_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    result = _preflight([{"email": "ada@example.com"}])
    assert "hashing key" not in _operator_text(result)


def test_runtime_error_still_fails_closed_and_names_the_setup(no_key):
    from services.transform_engine import apply_transform

    value, err = apply_transform("ada@example.com", "hash_pii")
    assert value is None
    assert err and "hashing key" in err and "no row was hashed" in err
    assert "PII_HASH_KEY" not in err and "Railway" not in err
    assert "replay" not in err
