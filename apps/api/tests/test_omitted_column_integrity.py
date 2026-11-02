"""A declared-Omit column carries no destination contract (G9 required-nulls).

MongoDB sources expose an implicit ``_id``. Studio makes the operator answer for
it, and Omit is a valid answer — the run never writes it. Judging its null rate
blocked Validate on a value the write path does not carry.
"""

from services.data_integrity import _check_required_nulls

ROWS = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def _mapping(source: str, target: str, **extra: object) -> dict[str, object]:
    return {"source": source, "target": target, **extra}


def test_omitted_mongo_id_does_not_block_required_nulls():
    mappings = [
        _mapping("_id", "", intentional_omit=True, transform="omit"),
        _mapping("id", "id"),
    ]
    result = _check_required_nulls(
        mappings, ROWS, null_rate_max=0.05, primary_key="id"
    )
    assert result["passed"] is True, result["issues"]
    assert result["blocks_transfer"] is False


def test_written_key_with_no_values_still_blocks():
    """Omit is the escape hatch — a mapped key column stays judged."""
    mappings = [_mapping("_id", "_id"), _mapping("id", "id")]
    result = _check_required_nulls(
        mappings, ROWS, null_rate_max=0.05, primary_key="id"
    )
    assert result["passed"] is False
    assert any("_id" in issue for issue in result["issues"])
