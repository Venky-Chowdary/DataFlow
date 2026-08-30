"""Quarantine DLQ JSONL reread uses json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 in rejected_details
values before Inspect / replay saw the cell. IEEE-exact 1.5 stays
float. Invalid lines stay skipped — never invent an event.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.quarantine_dlq import (  # noqa: E402
    load_dlq_event,
    list_dlq_events,
    quarantine_details_from_dlq,
)

LONG = "1.234567890123456789"


def _event_line(*, job_id: str = "job_q") -> str:
    return (
        '{"id":"e1","job_id":"'
        + job_id
        + '","action":"quarantine","rows":1,"details":'
        '{"rejected_details":[{"row":1,"column":"amt","reason":"lossy",'
        '"values":{"amt": ' + LONG + ', "n": 1.5, "id": 1}}]}}'
    )


def test_load_dlq_event_keeps_long_fraction():
    line = _event_line()
    ev = load_dlq_event(line)
    assert ev is not None
    amt = ev["details"]["rejected_details"][0]["values"]["amt"]
    assert amt == Decimal(LONG)
    assert amt != json.loads(line)["details"]["rejected_details"][0]["values"]["amt"]
    assert ev["details"]["rejected_details"][0]["values"]["n"] == 1.5
    assert ev["details"]["rejected_details"][0]["values"]["id"] == 1


def test_load_dlq_event_invalid_is_none():
    assert load_dlq_event("{not-json}") is None
    assert load_dlq_event("42") is None
    assert load_dlq_event("") is None
    assert load_dlq_event("[1, 2]") is None


def test_list_dlq_events_keeps_long_fraction(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    path = tmp_path / "quarantine_dlq.jsonl"
    path.write_text(
        "{not-json}\n" + _event_line() + "\n42\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dlq, "DLQ_PATH", path)
    listed = list_dlq_events(job_id="job_q")
    assert len(listed) == 1
    amt = listed[0]["details"]["rejected_details"][0]["values"]["amt"]
    assert amt == Decimal(LONG)
    assert listed[0]["details"]["rejected_details"][0]["values"]["n"] == 1.5


def test_quarantine_details_from_dlq_keeps_long_fraction(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    path = tmp_path / "quarantine_dlq.jsonl"
    path.write_text(_event_line() + "\n", encoding="utf-8")
    monkeypatch.setattr(dlq, "DLQ_PATH", path)
    rows = quarantine_details_from_dlq("job_q")
    assert len(rows) == 1
    assert rows[0]["values"]["amt"] == Decimal(LONG)
    assert rows[0]["values"]["n"] == 1.5
