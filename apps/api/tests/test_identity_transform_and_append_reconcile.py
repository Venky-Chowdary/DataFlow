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


def test_append_into_non_empty_table_passes_on_proven_delta() -> None:
    report = reconcile(
        source_rows=15,
        target_rows=30,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=False,
        target_rows_before=15,
    )
    assert report.passed is True
    assert report.assurance_level == "row_count"
    assert report.checksum_match is False
    stamped = report.to_dict()
    assert stamped["passed"] is True
    assert stamped["phase"] == "post_write_row_count"
    assert stamped["migration_proven"] is False


def test_strict_full_append_passes_on_dest_before_delta() -> None:
    """CSV Full Append into 100 existing rows: 200 landed, dest=300.

    Whole-table digests are incomparable. Strict does not invent comparability.
    Dest-before delta is the identity — the same proof balanced already used.
    """
    report = reconcile(
        source_rows=200,
        target_rows=300,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=True,
        target_rows_before=100,
    )
    assert report.passed is True
    assert report.assurance_level == "row_count"
    stamped = report.to_dict()
    assert stamped["passed"] is True
    assert stamped["phase"] == "post_write_row_count"
    assert stamped["migration_proven"] is False
    assert "checksum mismatch" not in stamped["message"].lower()
    assert "200" in stamped["message"]
    assert stamped.get("target_rows_before") == 100


def test_append_without_pre_write_count_is_not_verified() -> None:
    # 30 >= 15 is satisfied by the rows that were already there; nothing proves
    # this job appended anything, so Gate-8 must not report a verified count.
    report = reconcile(
        source_rows=15,
        target_rows=30,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=False,
    )
    assert report.passed is False
    assert report.assurance_level == "none"
    assert "unverified" in report.message.lower()
    assert report.to_dict()["migration_proven"] is False


def test_append_delta_short_of_expected_fails() -> None:
    # Table grew by 5, not the 15 rows the batch claimed to append.
    report = reconcile(
        source_rows=15,
        target_rows=30,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=False,
        target_rows_before=25,
    )
    assert report.passed is False
    assert report.assurance_level == "none"
    assert "delta mismatch" in report.message.lower()


def test_strict_mode_still_fails_incomparable_append() -> None:
    """Strict without dest-before cannot prove the batch landed — unverified, not a checksum."""
    report = reconcile(
        source_rows=15,
        target_rows=30,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=True,
    )
    assert report.passed is False
    assert "unverified" in report.message.lower()
    assert "checksum mismatch" not in report.message.lower()


def test_strict_keyed_batch_checksum_mismatch_still_fails() -> None:
    """When dest was re-read by written key, hashes are comparable — strict fails cells."""
    from services.reconcile_coverage import WRITTEN_BATCH_KEYS

    report = reconcile(
        source_rows=15,
        target_rows=30,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        strict_checksum=True,
        target_rows_before=15,
        checksum_scope=WRITTEN_BATCH_KEYS,
    )
    assert report.passed is False
    assert "checksum mismatch" in report.message.lower()


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
