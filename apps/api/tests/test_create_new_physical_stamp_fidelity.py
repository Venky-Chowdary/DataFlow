"""Create-new verdicts must describe the physical type that will run.

Auto-map projects an identity carrier in the *source* dialect's spelling
(``TIMESTAMPTZ`` for an Oracle ``TIMESTAMP WITH TIME ZONE``). The create-new
stamp then replaces it with the destination's own DDL (``DATETIMEOFFSET``).
Every verdict computed against the pre-stamp spelling compared a source token
to a foreign dialect and read offset-pinned → session-relative as a collapse,
so a lossless Oracle→SQL Server append kept ``lossy_cast``, capped confidence
under the G4 floor and demanded a Risk Contract for a write that loses nothing.
"""

from __future__ import annotations

from services.semantic_mapper import _apply_create_new_risk_stamps


def _identity_mapping(source_type: str, target_type: str) -> dict:
    return {
        "source": "ts_tz",
        "target": "ts_tz",
        "source_type": source_type,
        "target_type": target_type,
        "assignment_strategy": "identity_passthrough",
        "create_new": True,
        "transform": "datetime",
        "confidence": 0.7,
        # Stale verdict from the pre-stamp carrier spelling.
        "fidelity": "lossy_cast",
        "fidelity_reason": "TIMESTAMP_TZ → TIMESTAMPTZ can lose precision.",
        "type_narrowing": True,
        "requires_risk_contract": True,
    }


def test_physical_stamp_clears_stale_lossy_verdict_for_sqlserver():
    row = _apply_create_new_risk_stamps(
        [_identity_mapping("TIMESTAMPTZ", "TIMESTAMPTZ")],
        "mssql",
        dest_table_exists=False,
    )[0]
    assert row["target_type"] == "DATETIMEOFFSET"
    assert row["fidelity"] == "preserve"
    assert row["type_narrowing"] is False
    assert row["requires_risk_contract"] is False
    # Lossless create-new must clear the G4 confidence floor.
    assert row["confidence"] >= 0.9
    assert row["requires_review"] is False


def test_physical_stamp_keeps_offset_on_oracle_and_postgres():
    for dest, physical in (
        ("oracle", "TIMESTAMP WITH TIME ZONE"),
        ("postgresql", "TIMESTAMPTZ"),
    ):
        row = _apply_create_new_risk_stamps(
            [_identity_mapping("TIMESTAMP_TZ", "TIMESTAMP_TZ")],
            dest,
            dest_table_exists=False,
        )[0]
        assert row["target_type"] == physical, dest
        assert row["fidelity"] == "preserve", dest
        assert row["requires_risk_contract"] is False, dest


def test_real_narrowing_still_blocks_after_physical_stamp():
    """The re-derive must not launder loss — offset → naive stays lossy."""
    mapping = _identity_mapping("TIMESTAMP_TZ", "TIMESTAMP_TZ")
    mapping["target_type"] = "DATETIME2(7)"
    row = _apply_create_new_risk_stamps([mapping], "mssql", dest_table_exists=False)[0]
    assert row["fidelity"] != "preserve"
    assert row["requires_risk_contract"] is True
    assert row["confidence"] <= 0.84


def test_risky_column_without_a_prior_confidence_still_stamps():
    """A mapping whose confidence Map never scored must not raise NameError.

    The F8 extraction moved the stamp out of ``semantic_mapper`` but left the
    identity-passthrough floor behind, so every risky create-new column that
    reached the stamp without a scored confidence — the IEEE float artifact
    path is the common one — died with ``NameError`` mid-Map.
    """
    mapping = {
        "source": "amt",
        "target": "amt",
        "source_type": "DOUBLE PRECISION",
        "target_type": "DOUBLE PRECISION",
        "assignment_strategy": "create_compatible_new",
        "create_new": True,
        "confidence": 0,
    }
    row = _apply_create_new_risk_stamps(
        [mapping],
        "postgresql",
        source_samples={"amt": [0.1, 0.2, 1234.5678901234567]},
        dest_table_exists=False,
    )[0]
    assert row["requires_review"] is True
    assert 0.0 < float(row["confidence"]) <= 0.84
