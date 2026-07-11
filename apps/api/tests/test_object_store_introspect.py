"""Object store profiling tests."""

from services.object_store_introspect import profile_object_batch, rows_from_matrix


def test_rows_from_matrix():
    rows = rows_from_matrix(["id", "name"], [["1", "Alice"]])
    assert rows == [{"id": "1", "name": "Alice"}]


def test_profile_object_batch_infers_types():
    headers = ["id", "amount", "active"]
    rows = [["1", "10.5", "true"], ["2", "20.0", "false"]]
    result = profile_object_batch(headers, rows)
    assert result["ok"] is True
    assert result["schema"]["id"] == "INTEGER"
    assert result["row_estimate"] == 2
