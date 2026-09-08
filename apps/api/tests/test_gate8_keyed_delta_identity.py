"""Gate-8 grades a keyed merge by its key census, never by the batch size.

An upsert batch of 1,141 events that inserted 100 keys and updated 1,041
existing ones grows the destination by 100. When the batch digest cannot be
re-scoped by key, the delta identity is ``inserts - deletes`` — grading the
batch size as the expected delta failed every correct upsert whose events
touched existing keys.
"""

from services.reconciliation import reconcile


def _keyed(**overrides):
    base = dict(
        source_rows=1141,
        target_rows=2100,
        source_checksum="aaaa",
        target_checksum="bbbb",
        strict_checksum=True,
        allow_extra_rows=True,
        target_rows_before=2000,
        keyed_expected_delta=100,
    )
    base.update(overrides)
    return reconcile(**base)


def test_keyed_merge_passes_on_census_delta_not_batch_size():
    report = _keyed()
    assert report.passed is True
    assert report.assurance_level == "row_count"
    assert "Keyed delta verified" in report.message
    assert report.population_proof is False


def test_keyed_merge_fails_when_destination_delta_disagrees_with_census():
    report = _keyed(target_rows=2050)
    assert report.passed is False
    assert "Keyed delta mismatch" in report.message
    assert "50 merged, 100 expected" in report.message


def test_keyed_merge_with_deletes_can_shrink_destination():
    report = _keyed(target_rows=1990, keyed_expected_delta=-10)
    assert report.passed is True


def test_keyed_merge_without_precount_is_unverified():
    report = _keyed(target_rows_before=None)
    assert report.passed is False
    assert report.assurance_level == "none"


def test_batch_identity_still_rules_without_census():
    report = _keyed(keyed_expected_delta=None, target_rows=3141)
    assert report.passed is True
    assert "Append delta verified" in report.message
    assert _keyed(keyed_expected_delta=None).passed is False
