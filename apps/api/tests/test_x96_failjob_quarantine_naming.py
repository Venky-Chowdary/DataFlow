"""X-96: a FAIL_JOB on a retained destination must persist AND name the
``{table}_df_quarantine`` table — previously the fail path never ran the
dest-DLQ persist, so the table was never written nor named.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.transfer import engine_destination as ed


def _req():
    r = MagicMock()
    r.destination.extra = {}
    return r


def test_names_retained_quarantine_table():
    summary = {"dest_quarantine_table": "out_df_quarantine"}
    with patch.object(ed, "_failed_batch_undo_note_impl", return_value="undo note"):
        msg = ed._note_failed_batch_undo(_req(), summary, "verify failed", job_id="j1")
    assert "out_df_quarantine" in msg


def test_persist_runs_after_undo_and_stamps():
    order = []

    def fake_undo(request, dest_summary, message, *, recon=None):
        order.append("undo")
        return "undo note"

    def fake_persist(job_id, dest_summary, request, *, already_persisted=None, canonical_details=None):
        order.append("persist")
        assert job_id == "j1"
        dest_summary["dest_quarantine_table"] = "out_df_quarantine"

    with patch.object(ed, "_failed_batch_undo_note_impl", fake_undo), \
         patch("src.transfer.job_quarantine._persist_job_quarantine", fake_persist):
        msg = ed._note_failed_batch_undo(
            _req(), {"rejected_rows": 2}, "verify failed", job_id="j1"
        )
    assert order == ["undo", "persist"]
    assert "out_df_quarantine" in msg


def test_persist_failure_never_masks_verdict():
    with patch.object(ed, "_failed_batch_undo_note_impl", return_value="undo note"), \
         patch("src.transfer.job_quarantine._persist_job_quarantine",
               side_effect=RuntimeError("dlq down")):
        msg = ed._note_failed_batch_undo(
            _req(), {"partial_batch_undo": "cleared"}, "verify failed", job_id="j1"
        )
    assert "undo note" in msg
    assert "quarantine record" in msg
