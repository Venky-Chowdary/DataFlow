"""Re-review 4, 9e (Low): the Markdown export never reads "applied" next to an open difference.

``_resolution`` printed "rerun applied" / "resync keys applied" for a difference that was still
unexplained after that fix: a reader took the row as resolved.
"""

from __future__ import annotations

from services.parity_signoff import _resolution

OPEN = {"id": "d_1", "cause": {"code": "numeric_scale", "explained": False}}
EXPECTED = {"id": "d_2", "cause": {"code": "whitespace", "explained": True}}


def _fix(action: str, status: str, did: str = "d_1") -> dict:
    return {"id": "f", "action": action, "status": status, "difference_ids": [did]}


def test_an_open_difference_reads_open_whatever_fix_ran():
    assert _resolution(OPEN, [_fix("rerun", "applied")]) == "Open (rerun applied; it still differs)"
    assert _resolution(OPEN, [_fix("resync_keys", "applied")]) == "Open (resync keys applied; it still differs)"
    assert _resolution(OPEN, [_fix("accept", "dismissed")]) == "Open (accept dismissed)"
    assert _resolution(OPEN, []) == "Open"


def test_explained_and_accepted_differences_keep_their_words():
    assert _resolution(EXPECTED, []) == "Expected"
    assert _resolution({"id": "d_3", "cause": {"code": "accepted_by_operator", "explained": True}}, []) == "Accepted"
