"""The Data Pilot answer eval, run through a real cloud provider.

``tests/test_pilot_answer_audit_eval.py`` measures the local engine, which is
what CI can reproduce. This file runs the same fixture with
``DATAFLOW_PILOT_ENGINE=hybrid`` and a real provider key, so the narration and
tool-loop path is measured on the same questions instead of being described.

It is opt-in and skips honestly: a run without a key is a *skip*, never a pass.

    DATAFLOW_PILOT_EVAL_PROVIDER=openai OPENAI_API_KEY=... pytest tests/test_pilot_provider_eval.py

What is asserted is relative, because a provider's absolute numbers change with
the model behind the key:

* the provider path must not lose questions the local engine answers on
  target — hybrid narrates local evidence, it does not get to replace it;
* off-subject questions stay refused — fluency is the failure mode here;
* no question raises;
* every answered turn still carries evidence (sources or tool output). An
  answer with no evidence trail is the model speaking for itself.

The per-question rows are written next to the fixture as
``pilot_provider_eval_<provider>.json`` so the run is an artifact, not a claim.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.test_pilot_answer_audit_eval import ON_TARGET_FLOORS, _load_audit

_PROVIDER_ENV_KEY = {"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}
_REPORT_DIR = Path(__file__).resolve().parents[1] / "scripts" / "fixtures"

PROVIDER = (os.environ.get("DATAFLOW_PILOT_EVAL_PROVIDER") or "").strip().lower()
_KEY_PRESENT = bool(PROVIDER in _PROVIDER_ENV_KEY and os.environ.get(_PROVIDER_ENV_KEY[PROVIDER]))

pytestmark = pytest.mark.skipif(
    not _KEY_PRESENT,
    reason=(
        "provider-backed eval is opt-in: set DATAFLOW_PILOT_EVAL_PROVIDER to openai or "
        "anthropic and export that provider's API key"
    ),
)

audit = _load_audit()
SUITES = [*ON_TARGET_FLOORS, "off_subject"]


def _on_target(rows: list[dict]) -> set[str]:
    return {r["question"] for r in rows if r.get("on_target")}


@pytest.fixture(scope="module")
def measured() -> dict[str, dict[str, list[dict]]]:
    """Every suite, once locally and once through the provider, same session shape."""
    previous = os.environ.get("DATAFLOW_PILOT_ENGINE")
    from src.ai.llm import provider as llm_provider

    out: dict[str, dict[str, list[dict]]] = {"local": {}, "hybrid": {}}
    try:
        os.environ["DATAFLOW_PILOT_ENGINE"] = "local"
        for suite in SUITES:
            out["local"][suite] = audit.run(suite, fresh_session=True)
        llm_provider.clear_auth_failures()
        os.environ["DATAFLOW_PILOT_ENGINE"] = "hybrid"
        for suite in SUITES:
            out["hybrid"][suite] = audit.run(suite, fresh_session=True)
    finally:
        if previous is None:
            os.environ.pop("DATAFLOW_PILOT_ENGINE", None)
        else:
            os.environ["DATAFLOW_PILOT_ENGINE"] = previous
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (_REPORT_DIR / f"pilot_provider_eval_{PROVIDER}.json").write_text(
        json.dumps({"provider": PROVIDER, "runs": out}, indent=2, default=str),
        encoding="utf-8",
    )
    return out


def test_the_provider_was_actually_used(measured) -> None:
    """A run that silently fell back to local would pass every other test here."""
    from src.ai.llm.provider import usable_cloud_providers

    assert PROVIDER in usable_cloud_providers(), (
        f"{PROVIDER} key was present but the engine does not consider it usable "
        "(rejected key or missing SDK) — this run measured nothing"
    )
    rows = [r for suite in ON_TARGET_FLOORS for r in measured["hybrid"][suite]]
    narrated = [r for r in rows if (r.get("method") or "").startswith(f"{PROVIDER}_")]
    assert narrated, "no answered turn recorded a provider-backed method; engine fell back to local"


@pytest.mark.parametrize("suite", list(ON_TARGET_FLOORS))
def test_provider_path_keeps_every_local_on_target_answer(measured, suite: str) -> None:
    lost = _on_target(measured["local"][suite]) - _on_target(measured["hybrid"][suite])
    got = {r["question"]: r for r in measured["hybrid"][suite]}
    assert not lost, "\n".join(
        f"  {q}\n      expected one of {got[q].get('expected')}\n"
        f"      got: {(got[q].get('answer') or got[q].get('error') or '')[:200]}"
        for q in sorted(lost)
    )


def test_provider_path_still_refuses_off_subject_questions(measured) -> None:
    answered = [r for r in measured["hybrid"]["off_subject"] if r["outcome"] != "refused"]
    assert not answered, "\n".join(
        f"  [{r['outcome']}] {r['question']}\n      got: {(r.get('answer') or '')[:220]}"
        for r in answered
    )


def test_no_question_raises_through_the_provider(measured) -> None:
    errors = [r for suite in SUITES for r in measured["hybrid"][suite] if r["outcome"] == "error"]
    assert not errors, "\n".join(f"  {r['question']}: {r['error']}" for r in errors)


def test_every_provider_answer_carries_evidence(measured) -> None:
    bare = [
        r
        for suite in ON_TARGET_FLOORS
        for r in measured["hybrid"][suite]
        if r["outcome"] == "answered" and not (r.get("grounded") or r.get("tools"))
    ]
    assert not bare, "\n".join(
        f"  {r['question']}\n      got: {(r.get('answer') or '')[:200]}" for r in bare
    )
