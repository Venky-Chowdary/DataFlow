"""Job-store snapshot / recon JSON uses json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 in rejected_details and
reconciliation samples on reload. IEEE-exact 1.5 stays float. Invalid
files stay empty — never invent a job.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.jobs import JsonFileJobStore, load_job_json  # noqa: E402

LONG = "1.234567890123456789"


def _snapshot_text() -> str:
    return (
        '[{"job_id":"job_exact","status":"completed","operation":"transfer",'
        '"source":"src","destination":"dst","rejected_details":'
        '[{"row":1,"column":"amt","values":{"amt": '
        + LONG
        + ', "n": 1.5, "id": 1}}],'
        '"reconciliation":{"sample":{"amt": '
        + LONG
        + ', "n": 1.5}}}]'
    )


def test_load_job_json_keeps_long_fraction():
    text = _snapshot_text()
    raw = load_job_json(text)
    amt = raw[0]["rejected_details"][0]["values"]["amt"]
    assert amt == Decimal(LONG)
    assert amt != json.loads(text)[0]["rejected_details"][0]["values"]["amt"]
    assert raw[0]["rejected_details"][0]["values"]["n"] == 1.5
    assert raw[0]["reconciliation"]["sample"]["amt"] == Decimal(LONG)
    assert raw[0]["reconciliation"]["sample"]["n"] == 1.5


def test_json_file_job_store_reload_keeps_long_fraction(tmp_path):
    path = tmp_path / "jobs.json"
    path.write_text(_snapshot_text(), encoding="utf-8")
    store = JsonFileJobStore(path)
    job = store.get("job_exact")
    assert job is not None
    assert job.rejected_details[0]["values"]["amt"] == Decimal(LONG)
    assert job.rejected_details[0]["values"]["n"] == 1.5
    assert job.reconciliation["sample"]["amt"] == Decimal(LONG)
    assert job.reconciliation["sample"]["n"] == 1.5


def test_load_job_json_invalid_raises():
    try:
        load_job_json("{not-json}")
    except (json.JSONDecodeError, ValueError):
        return
    raise AssertionError("invalid job JSON must refuse")
