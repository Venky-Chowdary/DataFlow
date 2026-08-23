"""The cell preview scores the same rows the write will carry.

Validate's per-cell evidence is read as "this is what the writer will refuse".
While `/preflight/preview-cells` scanned the raw source, an approved
`round_number(places=0)` recipe produced a screen full of
`Invalid integer: '22.43'` findings for values the writer never binds — the
operator had already rounded them. The recipe travels with the request for the
same reason it travels with the gates.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.routers.preflight_router import router  # noqa: E402

_ROUND = {
    "steps": [{"op": "round_number", "column": "arr_time", "options": {"places": 0}}]
}

_BODY = {
    "headers": ["arr_time"],
    "sample_rows": [["22.433332"], ["21.833334"]],
    "mappings": [{"source": "arr_time", "target": "arr_time", "target_type": "INTEGER"}],
    "column_types": {"arr_time": "DECIMAL(12,9)"},
}


@pytest.fixture(scope="module")
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _findings(body: dict) -> list[dict]:
    return [c for c in (body.get("cells") or []) if c.get("status") == "quarantine"]


def test_the_raw_decimal_is_a_finding_when_no_recipe_is_approved(client) -> None:
    res = client.post("/api/v1/preflight/preview-cells", json=_BODY)
    assert res.status_code == 200, res.text
    body = res.json()
    assert _findings(body), "a fractional value into INTEGER is a real refusal"
    assert "transform_image" not in body


def test_a_rounded_value_is_not_reported_as_a_writer_refusal(client) -> None:
    res = client.post(
        "/api/v1/preflight/preview-cells", json={**_BODY, "shape_recipe": _ROUND}
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert _findings(body) == []
    # And the evidence names the image it scored, never silently.
    image = body["transform_image"]
    assert image["recipe_hash"]
    assert image["sample_rows_in"] == 2
    assert image["sample_rows_out"] == 2
    assert "22.43" not in res.text


def test_a_recipe_that_cannot_run_refuses_rather_than_previewing_raw(client) -> None:
    res = client.post(
        "/api/v1/preflight/preview-cells",
        json={
            **_BODY,
            "shape_recipe": {
                "steps": [
                    {
                        "op": "round_number",
                        "column": "no_such_column",
                        "options": {"places": 0},
                    }
                ]
            },
        },
    )
    assert res.status_code == 400, res.text
