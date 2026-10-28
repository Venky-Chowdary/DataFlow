"""A finished job never keeps an in-flight progress message.

Terminal writes that carried no ``message`` left the last progress copy
("Connecting to destination and creating table…") beside a failed, cancelled
or completed status. Only failures that also passed ``error`` were rewritten.
"""

from __future__ import annotations

import pytest

from services.mongodb_service import MemoryMongoDBService


def _running(store: MemoryMongoDBService, job_id: str, message: str) -> None:
    store.update_job_status(job_id, "pending")
    store.update_job_status(job_id, "running", message=message, phase="writing")


@pytest.mark.parametrize(
    ("status", "fields", "expected"),
    [
        ("failed", {}, "Failed"),
        ("failed", {"error": "disk full"}, "disk full"),
        ("cancelled", {}, "Cancelled"),
        ("completed", {}, "Completed"),
        ("completed_with_quarantine", {}, "Completed with quarantined rows"),
        ("completed_with_warnings", {}, "Completed with warnings"),
    ],
)
def test_terminal_write_without_message_replaces_progress_text(status, fields, expected):
    store = MemoryMongoDBService()
    _running(store, "j1", "Connecting to destination and creating table…")
    store.update_job_status("j1", status, **fields)
    assert store.get_job("j1")["message"] == expected


def test_explicit_terminal_message_and_progress_updates_are_untouched():
    store = MemoryMongoDBService()
    _running(store, "j1", "Connecting to destination and creating table…")
    store.update_job_status("j1", "running", progress_pct=40)
    assert store.get_job("j1")["message"] == "Connecting to destination and creating table…"
    store.update_job_status("j1", "cancelled", message="Cancelled after 10 row(s) were committed")
    assert store.get_job("j1")["message"] == "Cancelled after 10 row(s) were committed"


def test_repeat_terminal_write_keeps_the_message_it_already_has():
    store = MemoryMongoDBService()
    _running(store, "j1", "Writing…")
    store.update_job_status("j1", "completed", message="Row fidelity verified")
    store.update_job_status("j1", "completed", records_processed=5)
    assert store.get_job("j1")["message"] == "Row fidelity verified"
