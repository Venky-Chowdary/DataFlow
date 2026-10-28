"""D25 — one NULL into a NOT NULL text column failed the whole job.

A text column's NULL converts without complaint, so the cell-level policy
never saw it and the only refusal came from the driver on the whole batch -
under a policy that quarantines bad cells the job still died. A NULL bound
for a proven NOT NULL column is now a cell error routed through that policy.
"""

from __future__ import annotations

from connectors.writer_common import build_mapped_rows_with_details

_MAPPINGS = [
    {"source": "id", "target": "id", "transform": "none"},
    {"source": "name", "target": "name", "transform": "none"},
]
_HEADERS = ["id", "name"]
_ROWS = [["1", "a"], ["2", None], ["3", "c"]]


def _build(policy: str, nullability: dict[str, bool]):
    return build_mapped_rows_with_details(
        headers=_HEADERS,
        data_rows=[list(r) for r in _ROWS],
        mappings=_MAPPINGS,
        target_cols=["id", "name"],
        column_types={"id": "INTEGER", "name": "VARCHAR"},
        error_policy=policy,
        destination_column_nullability=nullability,
    )


def test_continue_policy_quarantines_only_the_null_row():
    mapped, errors, details = _build("quarantine", {"id": True, "name": False})
    assert [int(row[0]) for row in mapped] == [1, 3]
    assert len(details) == 1
    assert details[0]["row"] == 2
    assert "NOT NULL column 'name'" in details[0]["reason"]
    assert errors and "NOT NULL" in errors[0]


def test_nullable_column_still_takes_its_null():
    mapped, _errors, details = _build("quarantine", {"id": True, "name": True})
    assert len(mapped) == 3 and not details
