"""PP3 (QA handover, build 9c75ada4): proof-pack verify accepted no pack at all.

QA: ``POST /transfer/proof-pack/verify`` with an empty or unwrapped body
answered 200 ``ok:false`` "content_sha256 mismatch" instead of refusing the
request. Root cause: ``ProofPackVerifyBody.pack`` was
``Field(default_factory=dict)`` (src/routers/transfer_router.py), so a missing
pack verified as ``{}``; and ``POST /audit/verify-pack`` took the opposite
(unwrapped) shape, so a pack that verified on one route failed on the other.

Contract: both routes accept the same two shapes — ``{"pack": {...}}`` and the
exported pack itself — and answer 422 naming the missing keys when the body is
not a pack. A real but altered pack is still a 200 ``ok:false`` verdict.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import r9  # noqa: E402,F401

ROUTES = ("/api/v1/transfer/proof-pack/verify", "/api/v1/audit/verify-pack")


@pytest.fixture
def pack(r9):
    from services.signed_proof_pack import export_proof_pack_for_job

    return export_proof_pack_for_job(
        {"_id": "job-pp3-r11", "workspace_id": r9.acme, "reconciliation": {"passed": True},
         "source": "naïve ✓", "destination": ""},
        actor="owner@acme.test",
    )


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("body", [{}, {"pack": {}}, {"pack": None}, {"not_a_pack": True}, []])
def test_a_body_that_is_not_a_pack_is_422(r9, route, body):
    res = r9.owner.post(route, json=body)
    assert res.status_code == 422, (route, body, res.status_code, res.text)
    assert "content_sha256" in res.text


@pytest.mark.parametrize("route", ROUTES)
def test_a_pack_missing_its_signature_is_422(r9, route, pack):
    partial = {k: v for k, v in pack.items() if k != "signature"}
    for body in (partial, {"pack": partial}):
        res = r9.owner.post(route, json=body)
        assert res.status_code == 422, res.text
        assert "signature" in res.text


@pytest.mark.parametrize("route", ROUTES)
def test_both_routes_accept_the_wrapped_and_the_unwrapped_pack(r9, route, pack):
    for body in ({"pack": pack}, pack):
        res = r9.owner.post(route, json=body)
        assert res.status_code == 200, (route, res.text)
        assert res.json()["ok"] is True, res.json()


@pytest.mark.parametrize("route", ROUTES)
def test_an_altered_pack_is_still_a_verdict_not_a_422(r9, route, pack):
    altered = copy.deepcopy(pack)
    altered["actor"] = "someone-else@example.com"
    res = r9.owner.post(route, json={"pack": altered})
    assert res.status_code == 200, res.text
    assert res.json()["ok"] is False
    assert "content_sha256 mismatch" in res.json()["errors"]
