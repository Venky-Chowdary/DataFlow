"""Pilot ground truth — the review's low findings (status parity, wording, edges, masking).

* A stored ``FAILED`` is not the Jobs page's ``failed``: Pilot counted it.
* The success rate hid partial / completed-with-errors runs as "in flight or
  cancelled".
* ``date_to=9999-12-31`` raised OverflowError; ``window`` silently won over
  ``date_from`` / ``date_to``.
* The ack ledger's secret keys were narrower than the connector store's.
* A pre-upgrade plaintext ``pilot_results.json`` stayed on disk.
* ``mask_secrets_in_text`` ate ordinary text between ":" and "@".
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for i in range(4):
        pt.seed_job("failed", at=NOW - timedelta(minutes=10 + i))
    pt.seed_job("FAILED", at=NOW - timedelta(minutes=30))
    for i in range(3):
        pt.seed_job("completed", at=NOW - timedelta(minutes=40 + i))
    pt.seed_job("partial", at=NOW - timedelta(minutes=50))
    pt.seed_job("completed_with_errors", at=NOW - timedelta(minutes=51))
    pt.seed_job("completed", at=NOW - timedelta(minutes=52), reconciliation={"passed": False})
    return pt.agent()


def test_uppercase_status_is_not_counted_as_failed(ledger):
    page = int(pt.mongo().count_job_matches(status="failed") or 0)
    assert page == 4
    response = pt.ask(ledger, "how many jobs failed")
    assert pt.bold_ints(response.answer)[:1] == [page], response.answer
    attention = pt.ask(ledger, "how many jobs need attention")
    assert "4 failed" in attention.answer, attention.answer


def test_success_rate_names_the_runs_it_leaves_out(ledger):
    response = pt.ask(ledger, "what is the success rate of my jobs")
    assert "in flight or cancelled" not in response.answer, response.answer
    assert "partial" in response.answer and "completed_with_errors" in response.answer, response.answer


@pytest.mark.parametrize("args", [
    {"date_to": "9999-12-31"},
    {"date_from": "0001-01-01", "date_to": "2026-11-11"},
])
def test_extreme_dates_get_a_validation_message(ledger, monkeypatch, args):
    from src.ai.copilot import job_window
    from src.ai.copilot.tools import get_pilot_tools

    monkeypatch.setattr(job_window, "workspace_zone_name", lambda: "Pacific/Auckland")
    result = get_pilot_tools().execute("list_jobs", args)
    if not result.success:
        assert "out of range" in result.error or "must be" in result.error, result.error
        assert "Reference:" not in result.error, "a validation error was reported as a server error"


def test_window_and_explicit_dates_together_are_refused(ledger):
    from src.ai.copilot.tools import get_pilot_tools

    result = get_pilot_tools().execute("list_jobs", {"window": "today", "date_from": "2026-11-01"})
    assert result.success is False and "either" in result.error.lower(), result


def test_ack_secret_keys_cover_the_connector_store_list():
    from services.connector_store import _CONNECTOR_SECRET_KEYS
    from src.ai.copilot.ack_ledger import redact_payload

    payload = {key: "FAKE-VALUE-123" for key in _CONNECTOR_SECRET_KEYS}
    shown = redact_payload(payload)
    assert "FAKE-VALUE-123" not in json.dumps(shown), shown


def test_pre_upgrade_plaintext_spill_is_removed_at_first_load(tmp_path):
    from src.ai.copilot.result_store import PilotResultStore

    path = tmp_path / "pilot_results.json"
    path.write_text(json.dumps({"entries": {"pr_old": {
        "result_id": "pr_old", "created_at": 1, "expires_at": 4_000_000_000,
        "rows": [{"ssn": "123-45-6789"}], "columns": ["ssn"],
    }}}), encoding="utf-8")
    PilotResultStore(path=path)
    assert "123-45-6789" not in path.read_text(encoding="utf-8")


@pytest.mark.parametrize("text,kept", [
    ("Error: contact the admin team at ops@corp.example", "contact the admin team at ops@corp.example"),
    ("Note: rows for jane@corp.example were skipped", "rows for jane"),
])
def test_masking_keeps_ordinary_text(text, kept):
    from services.secret_config import mask_secrets_in_text

    assert kept in mask_secrets_in_text(text)


@pytest.mark.parametrize("text,secret", [
    ("could not connect to postgresql://svc:S3cr3t-FAKE@db.example:5432/app", "S3cr3t-FAKE"),
    ("dsn svc:S3cr3t-FAKE@db.example failed", "S3cr3t-FAKE"),
    ("mysql://root:p w-FAKE@h/db timed out", "p w-FAKE"),
])
def test_masking_still_hides_uri_passwords(text, secret):
    from services.secret_config import mask_secrets_in_text

    assert secret not in mask_secrets_in_text(text)


def test_secret_shaped_ack_fields_are_sealed_and_redacted(tmp_path, monkeypatch):
    from src.ai.copilot import ack_ledger

    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "pilot_acks.json"))
    monkeypatch.setattr(ack_ledger, "_ledger", None)
    payload = {
        "name": "S3 lake", "type": "s3",
        "config": {"secret_access_key": "s3cr3t-FAKE-1", "sas_token": "sig=FAKE-2",
                   "aws_secret_access_key": "s3cr3t-FAKE-1", "client_secret": "client-FAKE-3"},
        "uri": "postgresql://svc:pw-FAKE-4@db.example/x", "dsn": "host=db password=pw-FAKE-4",
        "ssh_password": "pw-FAKE-4", "passphrase": "pw-FAKE-4", "access_token": "pw-FAKE-4",
        "auth": {"bearer": "pw-FAKE-4"},
    }
    ack_id = ack_ledger.get_ack_ledger().put(kind="create_connector", payload=payload)
    on_disk = (tmp_path / "pilot_acks.json").read_text(encoding="utf-8")
    shown = json.dumps(ack_ledger.redact_payload(payload))
    for secret in ("s3cr3t-FAKE-1", "sig=FAKE-2", "client-FAKE-3", "pw-FAKE-4"):
        assert secret not in on_disk, f"{secret} in clear in pilot_acks.json"
        assert secret not in shown, f"{secret} in the client preview"
    payload_back, error = ack_ledger.get_ack_ledger().claim(ack_id)
    assert not error and payload_back["auth"]["bearer"] == "pw-FAKE-4"
