"""D237 follow-up: a finding whose ``row`` is the record itself still counts once.

Stream sources (Kafka) attach the decoded record dict as the finding's ``row``.
``quarantined_row_count`` put it in a set and the whole job failed with
"unhashable type: 'dict'" on a poison message.
"""

from __future__ import annotations

from src.transfer.job_quarantine import quarantined_row_count


def test_record_dict_rows_count_distinct_rows():
    details = [
        {"row": {"id": 1, "amount": "x"}, "column": "amount"},
        {"row": {"amount": "x", "id": 1}, "column": "id"},
        {"row": {"id": 2}, "column": "amount"},
        {"row": ["raw", "list"], "column": "c"},
        {"column": "anonymous"},
    ]
    assert quarantined_row_count(details) == 4


def test_numbered_rows_still_count_once_per_row():
    details = [{"row": 7, "column": c} for c in "abcde"] + [{"row": 8, "column": "a"}]
    assert quarantined_row_count(details) == 2
