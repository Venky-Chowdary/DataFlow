"""The Data Pilot answer eval, run as a test so the numbers cannot drift.

``scripts/pilot_answer_audit.py`` is the measurement: every case is one real
``DataPilotAgent.chat`` turn against the local engine, with no cloud key
configured. This file runs the same fixture in CI and holds the floors it
reached, so a change that improves one question and quietly breaks four is a
failure rather than a paragraph in a summary.

Two things are asserted, because they fail independently and a fix for one is
the classic way to break the other:

* **on target** — the answer contains something only a *correct* answer to that
  question would contain. Answering is not answering the question; a question
  about row filtering can be answered fluently, with citations, entirely out of
  the quarantine article.
* **refused** — the off-subject fixture must stay refused. Any change that
  raises the answered rate by lowering the evidence bar shows up here first.

Both session modes run. Sharing one session is what a real conversation looks
like and is how state leaking between turns shows up; isolating each question
separates a retrieval defect from a follow-up-resolution defect.

The floors are measurements, not aspirations. Raise them by widening the
fixture, never by relaxing a case.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]


def _load_audit():
    """Import the audit script by path — the fixture has exactly one owner."""
    path = _API_ROOT / "scripts" / "pilot_answer_audit.py"
    spec = importlib.util.spec_from_file_location("pilot_answer_audit", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


audit = _load_audit()

# Measured on this fixture with the local engine. See the module docstring.
ON_TARGET_FLOORS = {
    "product": (77, 77),
    "workspace": (8, 8),
    "command": (4, 4),
    "meta": (4, 4),
}

SESSION_MODES = [
    pytest.param(False, id="shared-session"),
    pytest.param(True, id="fresh-session"),
]


def _report(rows: list[dict]) -> str:
    misses = [r for r in rows if r.get("on_target") is False]
    return "\n".join(
        f"  [{r['outcome']}] {r['question']}\n"
        f"      expected one of {r.get('expected')}\n"
        f"      got: {(r.get('answer') or r.get('error') or '')[:200]}"
        for r in misses
    )


@pytest.mark.parametrize("suite,floor", [(s, f) for s, f in ON_TARGET_FLOORS.items()])
@pytest.mark.parametrize("fresh", SESSION_MODES)
def test_answers_are_on_target(suite: str, floor: tuple[int, int], fresh: bool) -> None:
    expected_hits, expected_total = floor
    rows = audit.run(suite, fresh_session=fresh)
    scored = [r for r in rows if r.get("on_target") is not None]
    hits = [r for r in scored if r["on_target"]]

    assert len(scored) >= expected_total, (
        f"the {suite} fixture shrank to {len(scored)} cases; "
        f"a floor measured on {expected_total} cases no longer means anything"
    )
    assert len(hits) >= expected_hits, (
        f"{suite}: {len(hits)}/{len(scored)} on target, floor is "
        f"{expected_hits}/{expected_total}\n{_report(rows)}"
    )


@pytest.mark.parametrize("fresh", SESSION_MODES)
def test_off_subject_questions_are_refused(fresh: bool) -> None:
    """The honesty half. An answer here is worse than a refusal, not better."""
    rows = audit.run("off_subject", fresh_session=fresh)
    answered = [r for r in rows if r["outcome"] != "refused"]
    assert rows, "the off-subject fixture must not be empty"
    assert not answered, "\n".join(
        f"  [{r['outcome']}] {r['question']}\n      got: {(r.get('answer') or '')[:220]}"
        for r in answered
    )


def test_no_question_in_the_fixture_raises() -> None:
    """An exception is a finding, and the audit records it rather than stopping."""
    rows = audit.run("all")
    errors = [r for r in rows if r["outcome"] == "error"]
    assert not errors, "\n".join(f"  {r['question']}: {r['error']}" for r in errors)
