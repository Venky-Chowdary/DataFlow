"""Real Parity Studio payloads for the web app's contract tests.

The engine scenario of :mod:`tests.test_parity_e2e_engine` (SQLite -> SQLite
through ``execute_tracked``, a destination that cuts a scale and drops a row, a
trim recipe, a row the run never wrote) is run to produce what the API returns
at each step of the fix loop, serialized exactly as the routes do:

* ``reportInitial``  explained, unexplained and out-of-scope differences
* ``reportResynced`` after the approved re-sync (one round, a failed and
  re-proposed ``-2`` fix, a dismissed accept with its note, the explanation)
* ``reportSigned``   all three roles signed
* ``summary``        the workspace summary over those runs

They are written as one TypeScript module whose constants are annotated with
the UI's contract types (``src/lib/parity.ts``), so ``tsc`` checks every field
(excess properties included) in the web build, and the web tests render them
(``TrustReportTab.realPayloads.test.tsx``). Clock times, the re-sync child job
ids and the signature change every run, so they are replaced by fixed values;
everything else is exactly what the backend produced. When the backend shape
changes on purpose, regenerate with ``PARITY_WRITE_WEB_FIXTURES=1`` and let the
web type check and tests say what the UI must follow.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from tests.test_parity_e2e_engine import _corrupting_route, _exec, _request, _Store, store  # noqa: F401

WEB_FIXTURE = Path(__file__).resolve().parents[2] / "web" / "src" / "components" / "parity" / "realParityPayloads.ts"
_ISO = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2}|Z)?")
_FIXED_AT = "2026-11-11T14:00:00+00:00"


def _route_json(report: dict) -> object:
    """What the route sends: the public report through FastAPI's encoder."""
    from fastapi.encoders import jsonable_encoder

    from src.routers.parity_router import _public

    return jsonable_encoder(_public(report))


def _stable(payload: object, child_ids: list[str]) -> object:
    text = json.dumps(payload, sort_keys=True)
    text = _ISO.sub(_FIXED_AT, text)
    for n, child in enumerate(child_ids, start=1):
        text = text.replace(child, f"resync-child-{n}")
    text = re.sub(r"hmac-sha256:[0-9a-f]{64}", "hmac-sha256:" + "5e" * 32, text)
    return json.loads(text)


class _ListingStore(_Store):
    """The fake store with the workspace-filtered listing the summary uses (newest first)."""

    def list_jobs(self, limit: int = 50, workspace_filter=None, **_kw):
        jobs = [{**job, "_id": jid} for jid, job in self.jobs.items()]
        return list(reversed(jobs))[:limit]


def _run(fake: _Store, req, job_id: str, name: str) -> str:
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict

    fake.update_job_status(job_id, "pending", transfer_request=transfer_request_to_dict(req),
                           workspace_id="ws-parity", name=name)
    UniversalTransferEngine().execute_tracked(req, job_id)
    # The real store stamps these; the fake one does not (normalized below).
    fake.update_job_fields(job_id, {"created_at": _FIXED_AT, "completed_at": _FIXED_AT})
    return job_id


def _payloads(fake: _Store, tmp_path: Path, monkeypatch) -> dict[str, object]:
    from services import parity_fixes
    from services.parity_explain import explain_job
    from services.parity_fixes import approve_fix, dismiss_fix
    from services.parity_report import build_parity_report
    from services.parity_signoff import sign_off
    from services.parity_summary import build_summary, remember_report

    monkeypatch.setenv("QUALITY_PROFILES_DIR", str(tmp_path / "profiles"))
    children: list[str] = []
    real_create = None

    from src.transfer.engine import UniversalTransferEngine

    real_create = UniversalTransferEngine._create_pending_job

    def _named_child(self, req):
        child = real_create(self, req)
        children.append(child)
        return child

    monkeypatch.setattr(UniversalTransferEngine, "_create_pending_job", _named_child)

    def _report(job_id: str) -> dict:
        job = fake.get_job(job_id)
        report = build_parity_report(job, reread=True, job_id=job_id)
        remember_report(job_id, job, report)
        return report

    # Run 1: the corrupting route; the fix loop runs to a signed report.
    src, dst = _corrupting_route(tmp_path)
    first = _run(fake, _request(src, dst), "parity-run-1", "pay nightly")
    initial = _report(first)
    resync = next(f for f in initial["fixes"] if f["action"] == "resync_keys")
    original = parity_fixes._resync_apply

    def _locked(*_a, **_k):
        raise RuntimeError("re-sync job x failed: destination locked")

    monkeypatch.setattr(parity_fixes, "_resync_apply", _locked)
    approve_fix(first, resync["id"], actor="ops@example.com", note="first try")
    monkeypatch.setattr(parity_fixes, "_resync_apply", original)
    explain_job(first, actor="eng@example.com")  # template words (no LLM in tests)
    _exec(dst, "DROP TRIGGER cut", "DROP TRIGGER drop13")
    accept = next(f for f in _report(first)["fixes"] if f["action"] == "accept")
    dismiss_fix(first, accept["id"], actor="owner@example.com", note="repair it instead")
    retry = next(f for f in _report(first)["fixes"]
                 if f["action"] == "resync_keys" and f["status"] == "proposed")
    resynced = approve_fix(first, retry["id"], actor="ops@example.com", note="triggers removed")
    remember_report(first, fake.get_job(first), resynced)
    sign_off(first, role="engineering", signer_role="editor", actor="eng@example.com", note="checked")
    sign_off(first, role="data_owner", signer_role="editor", actor="owner@example.com")
    signed = sign_off(first, role="compliance", signer_role="admin", actor="comp@example.com", note="ok")
    remember_report(first, fake.get_job(first), signed)

    # Run 2: the same route, now clean except the row the run never wrote.
    second = _run(fake, _request(src, dst), "parity-run-2", "pay nightly")
    _report(second)
    # Run 3: a fresh corrupting route, left unexplained.
    other = tmp_path / "again"
    other.mkdir()
    src3, dst3 = _corrupting_route(other)
    third = _run(fake, _request(src3, dst3), "parity-run-3", "pay backfill")
    _report(third)
    summary = build_summary({"workspace_id": "ws-parity"}, limit=30)

    from fastapi.encoders import jsonable_encoder

    out = {
        "reportInitial": _route_json(initial),
        "reportResynced": _route_json(resynced),
        "reportSigned": _route_json(signed),
        "summary": jsonable_encoder(summary),
    }
    return {name: _stable(body, children) for name, body in out.items()}


@pytest.fixture()
def listing_store(store, monkeypatch):  # noqa: F811 - the e2e store, with a workspace listing
    import services.mongodb_service as mongodb_service
    import src.transfer.engine as engine_mod

    fake = _ListingStore()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: fake)
    return fake


_TYPES = {"reportInitial": "ParityReport", "reportResynced": "ParityReport", "reportSigned": "ParityReport",
          "summary": "ParitySummary"}


def render_module(payloads: dict[str, object]) -> str:
    """The TypeScript module: each payload as a constant typed with the UI contract."""
    lines = [
        "/**",
        " * Real Parity Studio API payloads from an engine run (SQLite -> SQLite).",
        " * GENERATED by apps/api/tests/test_parity_web_contract_fixtures.py — do not edit by hand;",
        " * regenerate with PARITY_WRITE_WEB_FIXTURES=1. Typed with the contract so tsc checks every",
        " * field. Not imported by app code.",
        " */",
        'import type { ParityReport, ParitySummary } from "../../lib/parity";',
        "",
    ]
    for name, type_name in _TYPES.items():
        body = json.dumps(payloads[name], indent=2, sort_keys=True, ensure_ascii=False)
        lines += [f"export const {name}: {type_name} = {body};", ""]
    return "\n".join(lines)


def test_web_contract_fixtures_are_what_the_engine_route_returns(listing_store, tmp_path, monkeypatch):
    payloads = _payloads(listing_store, tmp_path, monkeypatch)
    initial, resynced, signed = payloads["reportInitial"], payloads["reportResynced"], payloads["reportSigned"]
    # The scenario covers what the UI must render.
    assert initial["verdict"] == "unexplained"
    assert {d["cause"]["code"] for d in initial["differences"]} >= {
        "transform_applied", "numeric_scale", "unexplained", "out_of_scope_population",
    }
    assert resynced["verdict"] == "explained" and len(resynced["rounds"]) == 1
    assert resynced["explanation"]["source"] == "template"
    assert {f["status"] for f in resynced["fixes"]} >= {"applied", "failed", "dismissed"}
    assert any(f["id"].endswith("-2") for f in resynced["fixes"])
    assert signed["signoff"]["status"] == "signed"
    assert len(payloads["summary"]["runs"]) >= 3

    module = render_module(payloads)
    assert len(module.encode("utf-8")) < 30_000
    if os.environ.get("PARITY_WRITE_WEB_FIXTURES") == "1":
        WEB_FIXTURE.write_text(module, encoding="utf-8")
    assert WEB_FIXTURE.exists(), f"{WEB_FIXTURE} is missing; regenerate with PARITY_WRITE_WEB_FIXTURES=1"
    assert WEB_FIXTURE.read_text(encoding="utf-8") == module, (
        f"{WEB_FIXTURE.name} no longer matches what the API returns; regenerate with "
        "PARITY_WRITE_WEB_FIXTURES=1, then run the web type check and tests"
    )
