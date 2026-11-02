"""likely_primary_key matches identity tokens, not substrings ("paid" is not an id)."""

import pytest

from services.data_profiler import profile_column

IDS = [str(i) for i in range(1, 11)]


@pytest.mark.parametrize("name", ["id", "customer_id", "customerId", "order_code", "sku_key", "uuid", "pk"])
def test_identity_names_are_key_candidates(name):
    assert profile_column(name, IDS)["likely_primary_key"] is True


@pytest.mark.parametrize("name", ["paid", "valid", "provider", "hidden", "barcode_scanned", "idle_minutes"])
def test_substring_lookalikes_are_not_key_candidates(name):
    assert profile_column(name, IDS)["likely_primary_key"] is False


def test_duplicate_values_never_key():
    assert profile_column("id", ["1"] * 10)["likely_primary_key"] is False
