"""Skip read-back keeps the first image. Occupied-table digests use the delta."""

from __future__ import annotations

from services.reconciliation import reconcile, sample_compare_rows


def test_skip_readback_keeps_the_destination_value():
    compared = sample_compare_rows(
        [{"id": "1", "amount": "20"}],
        [{"id": "1", "amount": "10"}],
        [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
        sort_key="id",
        duplicate_policy="skip_duplicates",
    )
    assert compared["passed"] is True
    assert compared["mismatches"] == []


def test_skip_readback_still_checks_a_key_this_run_inserted():
    compared = sample_compare_rows(
        [{"id": "2", "amount": "20"}],
        [{"id": "2", "amount": "10"}],
        [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
        sort_key="id",
        duplicate_policy="dedupe_skip",
        inserted_keys=["2"],
    )
    assert compared["passed"] is False


def test_upsert_readback_still_requires_the_new_value():
    compared = sample_compare_rows(
        [{"id": "1", "amount": "20"}],
        [{"id": "1", "amount": "10"}],
        [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
        sort_key="id",
        duplicate_policy="upsert",
    )
    assert compared["passed"] is False


def test_occupied_append_does_not_fail_the_whole_table_checksum():
    report = reconcile(
        source_rows=15,
        target_rows=115,
        source_checksum="batch",
        target_checksum="table",
        strict_checksum=True,
        allow_extra_rows=True,
        target_rows_before=100,
    )
    assert report.passed is True
    assert "Checksum mismatch" not in report.message


def test_occupied_deduped_update_uses_the_key_delta_not_the_table_digest():
    report = reconcile(
        source_rows=15,
        target_rows=100,
        source_checksum="batch",
        target_checksum="table",
        strict_checksum=True,
        allow_extra_rows=True,
        target_rows_before=100,
        keyed_expected_delta=0,
    )
    assert report.passed is True
    assert "Checksum mismatch" not in report.message


def test_short_append_delta_still_fails():
    report = reconcile(
        source_rows=15,
        target_rows=100,
        source_checksum="batch",
        target_checksum="table",
        strict_checksum=True,
        allow_extra_rows=True,
        target_rows_before=100,
    )
    assert report.passed is False
    assert "Checksum mismatch" not in report.message
    assert "delta" in report.message.lower()


def test_first_load_checksum_stays_strict():
    report = reconcile(
        source_rows=15,
        target_rows=15,
        source_checksum="batch",
        target_checksum="other",
        strict_checksum=True,
        allow_extra_rows=True,
        target_rows_before=0,
    )
    assert report.passed is False
    assert "Checksum mismatch" in report.message
