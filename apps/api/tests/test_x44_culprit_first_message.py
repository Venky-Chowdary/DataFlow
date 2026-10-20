"""X-44 reopen: a failed count-only run must open with the culprit clause.

Live evidence (baf973b6, SQLite→MySQL/PG/MSSQL count-only engine copy,
dest cell tampered): the job error led with the success sentence and
appended the localized culprit behind an em-dash —

    "Row count verified by engine copy: 5 row(s) on the destination for
     5 source row(s). The copy ran inside the engines … — L4/L5 localized:
     row id='1' column 't' source='héllo 大 😀' target=''"

Contract: error opens with the culprit clause ("L4/L5 localized: …" or the
"Value mismatch on row …" wording), then the rollback/scope sentences.
"""

from src.transfer.engine_destination import (
    _culprit_first_message,
    _note_failed_batch_undo,
)

_LOC = (
    "L4/L5 localized: row id='1' column 't' "
    "source='héllo 大 😀' target=''"
)

_SUCCESS_LEAD = (
    "Row count verified by engine copy: 5 row(s) on the destination for "
    "5 source row(s). The copy ran inside the engines: the only proof the "
    "route produced is destination COUNT(*)."
)


def test_x44_loc_tail_moves_to_front():
    msg = _culprit_first_message(f"{_SUCCESS_LEAD} — {_LOC}")
    assert msg.startswith("L4/L5 localized"), msg
    assert "Row count verified by engine copy" in msg


def test_x44_loc_already_first_unchanged():
    msg = f"{_LOC} — {_SUCCESS_LEAD}"
    assert _culprit_first_message(msg) == msg


def test_x44_no_loc_passes_through():
    assert _culprit_first_message(_SUCCESS_LEAD) == _SUCCESS_LEAD
    assert _culprit_first_message("") == ""


def test_x44_failed_batch_undo_leads_with_culprit():
    # The job error the operator sees funnels through _note_failed_batch_undo;
    # a loc-last recon message must leave it culprit-first.
    out = _note_failed_batch_undo(
        None, None, f"{_SUCCESS_LEAD} — {_LOC}"
    )
    assert out.startswith("L4/L5 localized"), out
    assert "Row count verified by engine copy" in out
