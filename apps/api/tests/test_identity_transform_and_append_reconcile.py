"""Map/Validate/Execute parity regressions found by the local connector matrix.

Each case here blocked a route that moves data correctly: a name-triggered value
rewrite on an identity column, a family parse guard read as a custom transform,
and an append into a non-empty table whose whole-table digest is structurally
incomparable.
"""

from services.mapping_pipeline import _passthrough_identity_transform
from services.mapping_quality import _is_passthrough_transform
from services.reconciliation import reconcile


def test_identity_create_new_id_column_is_not_trimmed() -> None:
    assert (
        _passthrough_identity_transform(
            "trim_id",
            strategy="identity_passthrough",
            create_new=True,
            user_override=False,
            src_type="CHAR(36)",
            tgt_type="CHAR(36)",
        )
        == "none"
    )


def test_operator_chosen_trim_is_kept() -> None:
    assert (
        _passthrough_identity_transform(
            "trim_id",
            strategy="identity_passthrough",
            create_new=True,
            user_override=True,
            src_type="CHAR(36)",
            tgt_type="CHAR(36)",
        )
        == "trim_id"
    )


def test_trim_kept_when_types_differ() -> None:
    assert (
        _passthrough_identity_transform(
            "trim_id",
            strategy="identity_passthrough",
            create_new=True,
            user_override=False,
            src_type="INTEGER",
            tgt_type="VARCHAR(36)",
        )
        == "trim_id"
    )


def test_typed_transform_is_never_dropped() -> None:
    assert (
        _passthrough_identity_transform(
            "decimal",
            strategy="identity_passthrough",
            create_new=True,
            user_override=False,
            src_type="DECIMAL(12,2)",
            tgt_type="DECIMAL(12,2)",
        )
        == "decimal"
    )


def test_family_parse_guard_counts_as_passthrough() -> None:
    # DOUBLE→DOUBLE arrives labelled `decimal`; reading that as a custom
    # transform demoted an identical column below the G4 confidence floor.
    assert _is_passthrough_transform("decimal", "double", "double") is True
    assert _is_passthrough_transform("trim", "double", "double") is False


def test_append_into_non_empty_table_passes_on_row_count_only() -> None:
    report = reconcile(
        source_rows=15,
        target_rows=30,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=False,
    )
    assert report.passed is True
    assert report.assurance_level == "row_count"
    assert report.checksum_match is False
    stamped = report.to_dict()
    assert stamped["passed"] is True
    assert stamped["phase"] == "post_write_row_count"
    assert stamped["migration_proven"] is False


def test_strict_mode_still_fails_incomparable_append() -> None:
    report = reconcile(
        source_rows=15,
        target_rows=30,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=True,
    )
    assert report.passed is False


def test_overwrite_checksum_mismatch_still_fails() -> None:
    report = reconcile(
        source_rows=15,
        target_rows=15,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=False,
        strict_checksum=False,
    )
    assert report.passed is False
