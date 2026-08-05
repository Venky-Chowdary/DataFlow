"""Job API must never return plaintext connector secrets; PII dual-stamps must mask."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_sanitize_job_for_api_masks_endpoint_secrets():
    from src.transfer.models import sanitize_job_for_api

    job = {
        "_id": "j1",
        "transfer_request": {
            "source": {
                "kind": "database",
                "format": "postgresql",
                "password": "super-secret",
                "connection_string": "postgresql://u:hunter2@host/db",
                "api_key": "ak-live",
            },
            "destination": {
                "kind": "database",
                "format": "mysql",
                "private_key": "-----BEGIN PRIVATE KEY-----",
                "extra": {"client_secret": "cs-1", "region": "us-east-1"},
            },
        },
    }
    out = sanitize_job_for_api(job)
    src = out["transfer_request"]["source"]
    dst = out["transfer_request"]["destination"]
    assert src["password"] == "****"
    assert "hunter2" not in src["connection_string"]
    assert src["api_key"] == "****"
    assert dst["private_key"] == "****"
    assert dst["extra"]["client_secret"] == "****"
    assert dst["extra"]["region"] == "us-east-1"
    # Original dict untouched
    assert job["transfer_request"]["source"]["password"] == "super-secret"


def test_redact_destination_summary_masks_source_values():
    from services.pii_guard import redact_destination_summary

    summary = {
        "rejected_details": [
            {
                "row": 1,
                "column": "email",
                "value": "alice@example.com",
                "values": {"email": "alice@example.com", "id": "1"},
                "source_values": {"email": "alice@example.com", "id": "1"},
                "target_values": {"email": "alice@example.com"},
            }
        ]
    }
    mappings = [{"source": "email", "target": "email", "transform": "mask_pii"}]
    out = redact_destination_summary(summary, mappings)
    detail = out["rejected_details"][0]
    assert detail["value"] != "alice@example.com"
    assert "*" in str(detail["value"]) or "@" not in str(detail["value"]) or "***" in str(detail["value"])
    assert detail["source_values"]["email"] != "alice@example.com"
    assert detail["values"]["email"] != "alice@example.com"
    assert detail["source_values"]["id"] == "1"
