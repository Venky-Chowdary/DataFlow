#!/usr/bin/env python3
"""ai-benchmarks: every AI feature against recorded model responses (offline) or a live provider.

Replay (default, the PR gate): each suite runs the product's own code with the
Anthropic SDK answered from ``tests/fixtures/ai_cassettes/<suite>.json``
(:mod:`services.ai_cassette`). No key, no network, same result every run.
Cases include adversarial answers a live model will not give on demand: a
wrong target, an invented transform, malformed JSON, a narration that changes
a number. The floors are what the product guarantees against them.

Live (nightly): the same suites' reference cases run against the configured
provider (``ANTHROPIC_API_KEY``, else ``OPENAI_API_KEY``). Without a key the
run is **not run**: exit code 78 and a summary line that says so, never a pass.
``--record DIR`` writes what a live Anthropic run answered as new cassettes.

Known gaps are measured and printed, never hidden behind a floor: they are
places where a model's words are still taken without a check.

Every model call site in ``services/`` and ``src/`` must belong to a suite
(:data:`CALL_SITES`; ``tests/test_ai_benchmarks.py`` enforces it).

    PYTHONPATH=. python scripts/run_ai_benchmarks.py [--mode replay|live] [--out FILE] [--summary FILE]
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

API_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API_ROOT))
sys.path.insert(0, str(API_ROOT / "tests"))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_TRAINING", "off")

EXIT_NOT_RUN = 78
HOLDOUT_PATH = API_ROOT / "tests" / "fixtures" / "mapping_holdout_v1.json"

#: (file under apps/api, enclosing function) of every model call → its suite.
CALL_SITES: dict[tuple[str, str], str] = {
    ("services/llm_mapping.py", "refine_mappings_with_llm"): "mapping_refinement",
    ("services/parity_explain.py", "_generate"): "parity_explanation",
    ("services/validation_assistant.py", "_llm_narrative"): "validation_assistant",
    ("src/ai/rag/generator.py", "_narrate"): "pilot_docs_narration",
    ("src/ai/copilot/pilot_agent.py", "_polish_with_llm"): "pilot_narration",
    ("src/ai/copilot/pilot_agent.py", "_anthropic_agent_loop"): "pilot_agent_loop",
    # The OpenAI and Ollama loops share the loop's tool chokepoint (_native_tool_call);
    # the replay drives the Anthropic shape, test_pilot_native_loop_guards covers both shapes.
    ("src/ai/copilot/pilot_agent.py", "_openai_agent"): "pilot_agent_loop",
    ("src/ai/copilot/pilot_agent.py", "_ollama_agent"): "pilot_agent_loop",
}

#: Model call methods the coverage scan looks for, and directories that implement them.
MODEL_CALLS = frozenset({"generate", "generate_prose", "generate_agent"})
SCAN_DIRS = ("services", "src")
SCAN_EXCLUDE = ("src/ai/llm/", "src/ai/first_party/", "src/ai/training/")


@dataclass
class CaseResult:
    case: str
    passed: bool
    detail: str = ""
    drift: list[int] = field(default_factory=list)


@dataclass
class SuiteResult:
    suite: str
    feature: str
    metrics: dict[str, float] = field(default_factory=dict)
    floors: dict[str, float] = field(default_factory=dict)
    known_gaps: dict[str, str] = field(default_factory=dict)
    cases: list[CaseResult] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)
    status: str = "ran"

    @property
    def passed(self) -> bool:
        if self.status != "ran":
            return self.status == "replay_only"
        # A cassette that answers an older prompt proves nothing about today's prompt.
        if any(c.drift for c in self.cases):
            return False
        return all(self.metrics.get(k, 0.0) >= v for k, v in self.floors.items())

    def as_dict(self) -> dict[str, Any]:
        return {
            "suite": self.suite, "feature": self.feature, "status": self.status, "passed": self.passed,
            "metrics": self.metrics, "floors": self.floors, "known_gaps": self.known_gaps,
            "provenance": self.provenance,
            "prompt_drift": sorted({c.case for c in self.cases if c.drift}),
            "cases": [{"case": c.case, "passed": c.passed, "detail": c.detail} for c in self.cases],
        }


def _rate(results: list[bool]) -> float:
    return round(sum(results) / len(results), 4) if results else 0.0


@contextlib.contextmanager
def _count_answers(answers: dict[str, int], suite: str) -> Any:
    """Count real model answers (any provider) while a live case runs."""
    from src.ai.llm import provider as prov

    classes = (prov.DataTransferAnthropicProvider, prov.DataTransferOpenAIProvider, prov.DataTransferOllamaProvider)
    saved = {(cls, name): cls.__dict__[name] for cls in classes for name in ("generate", "generate_agent")
             if name in cls.__dict__}

    def wrap(fn: Any, agent: bool) -> Any:
        def counted(self: Any, *a: Any, **kw: Any) -> Any:
            out = fn(self, *a, **kw)
            ok = (bool(out.get("success")) and bool(out.get("content") or out.get("tool_calls"))) if agent \
                else (bool(getattr(out, "success", False)) and bool(str(getattr(out, "content", "")).strip()))
            if ok:
                answers[suite] = answers.get(suite, 0) + 1
            return out
        return counted

    for (cls, name), fn in saved.items():
        setattr(cls, name, wrap(fn, name == "generate_agent"))
    try:
        yield
    finally:
        for (cls, name), fn in saved.items():
            setattr(cls, name, fn)


class Runner:
    """Plays one case against the product code, in replay or live mode."""

    def __init__(self, mode: str, record_dir: Path | None = None, seal_prompts: bool = False) -> None:
        self.mode = mode
        self.record_dir = record_dir
        self.seal_prompts = seal_prompts
        self.recorded: dict[str, dict[str, Any]] = {}
        #: Live mode: model answers per suite (a success with content or tool calls).
        self.answers: dict[str, int] = {}

    def play(self, suite: str, case: str, fn: Callable[[], Any]) -> tuple[Any, list[int]]:
        from services.ai_cassette import anthropic_cassette, load_cassette, tape_for, tape_record, Tape

        if self.mode == "live":
            tape = Tape()
            with anthropic_cassette(tape, live=True), _count_answers(self.answers, suite):
                out = fn()
            if tape.responses:
                self.recorded.setdefault(suite, {})[case] = tape_record(tape)
            return out, []
        cassette = load_cassette(suite)
        tape = tape_for(cassette, case)
        with anthropic_cassette(tape):
            out = fn()
        if self.seal_prompts:
            # Pin the request each response answers, so a later prompt change shows as drift.
            cassette["cases"][case]["prompt_sha256"] = tape.seen_prompts
            from services.ai_cassette import CASSETTE_DIR

            (CASSETTE_DIR / f"{suite}.json").write_text(
                json.dumps(cassette, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return out, []
        return out, tape.drift()

    def provenance(self, suite: str) -> dict[str, Any]:
        if self.mode == "live":
            return {"how": "live", "at": datetime.now(timezone.utc).isoformat()}
        from services.ai_cassette import load_cassette

        return load_cassette(suite).get("provenance") or {}

    def write_recordings(self) -> list[str]:
        if not self.record_dir or not self.recorded:
            return []
        self.record_dir.mkdir(parents=True, exist_ok=True)
        written = []
        for suite, cases in self.recorded.items():
            path = self.record_dir / f"{suite}.json"
            path.write_text(json.dumps({
                "suite": suite,
                "provenance": {"how": "recorded", "provider": "anthropic",
                               "at": datetime.now(timezone.utc).isoformat()},
                "cases": cases,
            }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            written.append(str(path))
        return written


# ── suites ───────────────────────────────────────────────────────────────────


def _mapping_problem() -> dict[str, Any]:
    from helpers import witness_fixtures as F

    src, dst = F.customers()
    sources = ["cust_id", "given", "family", "mail", "login", "balance"]
    targets = ["customer_id", "first_name", "last_name", "email", "username", "balance", "balance_cents"]
    types = {"cust_id": "INTEGER", "balance": "DECIMAL"}
    return {
        "sources": sources, "targets": targets, "src": src, "dst": dst,
        "source_schemas": [{"name": c, "inferred_type": types.get(c, "VARCHAR"),
                            "samples": [str(r[c]) for r in src[:5]]} for c in sources],
        "target_schemas": [{"name": c, "inferred_type": "BIGINT" if c in ("customer_id", "balance_cents")
                            else ("DECIMAL" if c == "balance" else "VARCHAR")} for c in targets],
    }


def _map(problem: dict[str, Any], use_llm: bool) -> dict[str, Any]:
    from services.mapping_pipeline import run_mapping_pipeline

    return run_mapping_pipeline(
        problem["sources"], problem["targets"], source_schemas=copy.deepcopy(problem["source_schemas"]),
        target_schemas=copy.deepcopy(problem["target_schemas"]), use_llm=use_llm,
        destination_db_type="postgresql", source_db_type="postgresql", destination_table_exists=True,
    )


def _decisions(result: dict[str, Any]) -> list[tuple[Any, ...]]:
    return sorted((m.get("source"), m.get("target"), m.get("transform") or m.get("transformation"),
                   round(float(m.get("confidence") or 0), 6), bool(m.get("create_new")))
                  for m in result["mappings"])


def suite_mapping_refinement(r: Runner) -> SuiteResult:
    """LLM refinement suggests; the deterministic mapper decides; the witness checks the suggestion."""
    from services.mapping_witness import witness_mappings

    from helpers import witness_fixtures as F

    res = SuiteResult("mapping_refinement", "Map: LLM mapping suggestions (services/llm_mapping.py)")
    problem = _mapping_problem()
    baseline = _decisions(_map(problem, use_llm=False))
    cases = ["agree", "wrong_target", "invented_transform", "invalid_json"] if r.mode == "replay" else ["agree"]
    invariance, surfaced, contradicted, fallback, review_kept = [], [], [], [], []
    for case in cases:
        out, drift = r.play(res.suite, case, lambda: _map(problem, use_llm=True))
        same = _decisions(out) == baseline
        invariance.append(same)
        detail = f"decisions {'unchanged' if same else 'CHANGED'}; llm_used={out['llm'].get('llm_used')}"
        if case == "wrong_target":
            row = next(m for m in out["mappings"] if m["source"] == "given")
            suggested = row.get("suggested_target")
            surfaced.append(bool(suggested == "last_name" and row.get("target") != "last_name"))
            review_kept.append(bool(row.get("requires_review")))
            # AI proposes, Datawrap proves: the witness judges the model's pair on the destination's keys.
            maps = [{"source": m["source"], "target": m["target"]} for m in out["mappings"] if m.get("target")]
            maps = [m if m["source"] != "given" else {"source": "given", "target": suggested or "last_name"}
                    for m in maps]
            verdict = witness_mappings(maps, problem["src"], problem["dst"], key=F.KEY)
            pair = next(p for p in verdict["pairs"] if p["source"] == "given")
            contradicted.append(pair["status"] == "flagged" and pair["flag"]["suggested_target"] == "first_name")
            detail += f"; suggestion={suggested}; witness={pair['status']}"
        if case == "invalid_json":
            fallback.append(out["llm"].get("llm_error") == "invalid_json")
        if case == "invented_transform":
            row = next(m for m in out["mappings"] if m["source"] == "mail")
            held = bool(row.get("llm_invented_transform")) and str(row.get("transform") or "none") == "none"
            fallback.append(held)
            detail += f"; transform held={held}"
        res.cases.append(CaseResult(case, same, detail, drift))
    res.metrics["decision_invariance"] = _rate(invariance)
    res.floors["decision_invariance"] = 1.0
    if r.mode == "replay":
        res.metrics.update({"wrong_suggestion_attached_not_applied": _rate(surfaced),
                            "witness_contradicts_wrong_suggestion": _rate(contradicted),
                            "bad_output_never_applied": _rate(fallback),
                            "wrong_suggestion_keeps_review_flag": _rate(review_kept)})
        res.floors.update({"wrong_suggestion_attached_not_applied": 1.0,
                           "witness_contradicts_wrong_suggestion": 1.0, "bad_output_never_applied": 1.0})
        res.known_gaps["wrong_suggestion_keeps_review_flag"] = (
            "On a create-new row the pipeline clears requires_review after the LLM step set it, so a "
            "model suggestion that disagrees can sit on a row that reads as ready (mapper core, Track A).")
    res.provenance = r.provenance(res.suite)
    return res


def _parity_report() -> dict[str, Any]:
    cause = {"code": "type_narrowing", "explained": False, "label": "Value too long",
             "detail": "Destination column is VARCHAR(40); 3 values are longer."}
    return {
        "job_id": "bench-job", "verdict": "unexplained",
        "verdict_reason": "3 rows differ and are not explained yet.",
        "parity": {"rows_source": 1000, "rows_destination": 1000, "rows_differ": 3, "explained": 0,
                   "unexplained": 3, "proven_equal_pct": 99.7},
        "confidence": {"statement": "Full checksum on every row."},
        "tables": [{"name": "customers", "check": "full_checksum", "status": "unexplained", "differences": 1,
                    "unexplained": 3}],
        "differences": [{"id": "d1", "table": "customers", "column": "email", "kind": "value_mismatch",
                         "rows": 3, "cause": cause, "protected": True}],
        "fixes": [{"id": "fx_widen", "action": "widen_column", "title": "Widen email to VARCHAR(255)",
                   "applicable": True, "status": "proposed"},
                  {"id": "fx_accept", "action": "accept", "title": "Accept the 3 rows", "applicable": True,
                   "status": "proposed"}],
    }


def suite_parity_explanation(r: Runner) -> SuiteResult:
    import re

    from services.parity_explain import explain_report

    res = SuiteResult("parity_explanation", "Trust Report: explanation wording (services/parity_explain.py)")
    cases = ["grounded", "unknown_fix", "malformed", "invented_number"] if r.mode == "replay" else ["grounded"]
    accepted, dropped, fell_back, gap = [], [], [], []
    facts_numbers = {"3", "1000", "40", "255", "99.7"}
    for case in cases:
        out, drift = r.play(res.suite, case, lambda: explain_report(_parity_report()))
        detail = f"source={out['source']} fix_order={out['fix_order']}"
        if case in ("grounded", "unknown_fix"):
            accepted.append(out["source"] == "llm")
            dropped.append(set(out["fix_order"]) <= {"fx_widen", "fx_accept"})
        if case == "malformed":
            fell_back.append(out["source"] == "template")
        if case == "invented_number":
            numbers = set(re.findall(r"\d+(?:\.\d+)?", out["text"].replace(",", "")))
            gap.append(out["source"] == "template" or numbers <= facts_numbers)
        if r.mode == "live":
            numbers = set(re.findall(r"\d+(?:\.\d+)?", out["text"].replace(",", "")))
            gap.append(numbers <= facts_numbers)
        res.cases.append(CaseResult(case, True, detail, drift))
    res.metrics["valid_answer_used"] = _rate(accepted)
    res.metrics["fix_order_only_known_fixes"] = _rate(dropped)
    res.floors["fix_order_only_known_fixes"] = 1.0
    if r.mode == "replay":
        res.metrics["malformed_answer_falls_back_to_template"] = _rate(fell_back)
        res.floors.update({"valid_answer_used": 1.0, "malformed_answer_falls_back_to_template": 1.0})
    res.metrics["invented_numbers_rejected"] = _rate(gap)
    res.known_gaps["invented_numbers_rejected"] = (
        "The explanation text is not checked for numbers absent from the facts; a model can add one.")
    res.provenance = r.provenance(res.suite)
    return res


_VALIDATION_ISSUES = [
    {"severity": "block", "title": "Required column `customer_id` is not mapped",
     "why": "The destination requires a value.", "fix": "Map `cust_id` to `customer_id`."},
    {"severity": "block", "title": "`email` values are longer than VARCHAR(40)",
     "why": "3 sampled values exceed 40 characters.", "fix": "Widen `email` or accept truncation."},
]


def suite_validation_assistant(r: Runner) -> SuiteResult:
    from services.validation_assistant import _llm_narrative

    res = SuiteResult("validation_assistant", "Validate: failure narrative (services/validation_assistant.py)")
    deterministic = "• Required column `customer_id` is not mapped.\n• `email` values are longer than VARCHAR(40)."
    cases = ["narrated", "empty", "invented_column"] if r.mode == "replay" else ["narrated"]
    used, fell_back, gap = [], [], []
    for case in cases:
        (text, source), drift = r.play(res.suite, case, lambda: _llm_narrative(deterministic, _VALIDATION_ISSUES))
        if case == "narrated":
            used.append(source != "deterministic" and "`customer_id`" in text)
        if case == "empty":
            fell_back.append(source == "deterministic" and text == deterministic)
        if case == "invented_column" or r.mode == "live":
            cols = set(__import__("re").findall(r"`([^`]+)`", text))
            gap.append(cols <= {"customer_id", "cust_id", "email"} or source == "deterministic")
        res.cases.append(CaseResult(case, True, f"source={source}", drift))
    res.metrics["narrative_used_when_given"] = _rate(used)
    res.floors["narrative_used_when_given"] = 1.0 if r.mode == "replay" else 0.0
    if r.mode == "replay":
        res.metrics["empty_answer_falls_back"] = _rate(fell_back)
        res.floors["empty_answer_falls_back"] = 1.0
    res.metrics["invented_columns_rejected"] = _rate(gap)
    res.known_gaps["invented_columns_rejected"] = (
        "The narrative is not checked against the gate facts; a model can name a column that does not exist.")
    res.provenance = r.provenance(res.suite)
    return res


_DOCS_QUESTION = "What does quarantine mean for rows that fail?"


def suite_pilot_docs_narration(r: Runner) -> SuiteResult:
    from src.ai.rag.generator import DataTransferRAGGenerator
    from src.ai.rag.product_docs import retrieve_product_answer

    res = SuiteResult("pilot_docs_narration", "Pilot: documentation answers (src/ai/rag/generator.py)")
    hits = retrieve_product_answer(_DOCS_QUESTION).hits
    cases = ["grounded", "from_prior", "denies_docs"] if r.mode == "replay" else ["grounded"]
    kept, rejected, prior = [], [], []
    for case in cases:
        def narrate() -> str | None:
            gen = DataTransferRAGGenerator()
            return gen._narrate(_DOCS_QUESTION, hits)

        out, drift = r.play(res.suite, case, narrate)
        if case == "grounded":
            kept.append(out is not None)
        elif case == "denies_docs":
            rejected.append(out is None)
        else:
            prior.append(out is None)
        res.cases.append(CaseResult(case, True, "narration used" if out else "documented text kept", drift))
    res.metrics["grounded_narration_used"] = _rate(kept)
    res.floors["grounded_narration_used"] = 1.0 if r.mode == "replay" else 0.0
    if r.mode == "replay":
        res.metrics["narration_denying_the_docs_rejected"] = _rate(rejected)
        res.floors["narration_denying_the_docs_rejected"] = 1.0
        res.metrics["answer_from_model_prior_rejected"] = _rate(prior)
        res.known_gaps["answer_from_model_prior_rejected"] = (
            "A narration that shares the question's words but answers from the model's own prior "
            "(not the passages) passes the evidence check.")
    res.provenance = r.provenance(res.suite)
    return res


_PILOT_DRAFT = ("Quarantine keeps bad rows instead of dropping them: 14 rows in job job_31 were quarantined "
                "with the column, value and rule that refused each one. Open the Quarantine tab to review them.")


def suite_pilot_narration(r: Runner) -> SuiteResult:
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.llm import provider as prov

    res = SuiteResult("pilot_narration", "Pilot: narration of documentation answers (pilot_agent._polish_with_llm)")
    local = CopilotResponse(
        answer=_PILOT_DRAFT, intent="explain", confidence=0.9, method="local",
        sources=[{"title": "Quarantine & bad rows", "href": "/docs#quarantine"}],
        tools_used=[{"name": "explain_product", "success": True, "summary": "Quarantine & bad rows"}],
    )
    cases = ["faithful", "changed_number", "denies_draft", "moved_number"] if r.mode == "replay" else ["faithful"]
    kept, rejected, denial = [], [], []
    original = prov.pick_narration_provider
    for case in cases:
        def polish() -> Any:
            prov.pick_narration_provider = lambda: (prov.DataTransferAnthropicProvider(), "anthropic_polish")
            try:
                return DataPilotAgent._polish_with_llm(None, "what is quarantine?", [], copy.deepcopy(local),
                                                       "You are Datawrap Pilot.")
            finally:
                prov.pick_narration_provider = original

        out, drift = r.play(res.suite, case, polish)
        narrated = out.method != "local"
        if case == "faithful":
            kept.append(narrated)
        elif case == "denies_draft":
            denial.append(not narrated)
        else:
            rejected.append(not narrated)
        res.cases.append(CaseResult(case, True, f"method={out.method}", drift))
    res.metrics["faithful_narration_used"] = _rate(kept)
    res.floors["faithful_narration_used"] = 1.0 if r.mode == "replay" else 0.0
    if r.mode == "replay":
        res.metrics["changed_or_moved_number_rejected"] = _rate(rejected)
        res.floors["changed_or_moved_number_rejected"] = 1.0
        res.metrics["denial_of_the_draft_rejected"] = _rate(denial)
        res.known_gaps["denial_of_the_draft_rejected"] = (
            "\"but you cannot review them\" added to a draft that says you can passes contradicts_draft.")
    res.provenance = r.provenance(res.suite)
    return res


def suite_pilot_agent_loop(r: Runner) -> SuiteResult:
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.tool_permissions import MUTATE, TOOL_PERMISSIONS
    from src.ai.copilot.tools import ToolResult

    res = SuiteResult("pilot_agent_loop", "Pilot: provider tool loop (pilot_agent._anthropic_agent_loop)")
    mutating = {n for n, (_p, effect) in TOOL_PERMISSIONS.items() if effect == MUTATE}
    executed: list[str] = []
    answered, safe = [], []
    cases = ["mutation_then_read"] if r.mode == "replay" else []
    for case in cases:
        def loop() -> Any:
            agent = DataPilotAgent()
            agent.tools.execute = lambda name, args: (executed.append(name) or  # type: ignore[method-assign]
                                                      ToolResult(name=name, success=True, output={"count": 2}))
            agent._with_result_context = lambda name, args, ctx: dict(args or {})  # type: ignore[method-assign]
            from src.ai.llm.provider import DataTransferAnthropicProvider

            agent._anthropic = DataTransferAnthropicProvider()
            return agent._anthropic_agent_loop("list my connectors", [], "You are Datawrap Pilot.", None)

        out, drift = r.play(res.suite, case, loop)
        answered.append(out is not None and out.method == "anthropic_agent")
        safe.append(not (set(executed) & mutating) and not (out and out.pending_actions))
        res.cases.append(CaseResult(case, True, f"executed={executed}", drift))
    if r.mode == "replay":
        res.metrics.update({"model_mutations_never_run": _rate(safe), "final_answer_returned": _rate(answered)})
        res.floors.update({"model_mutations_never_run": 1.0, "final_answer_returned": 1.0})
    else:
        res.status = "replay_only"
        res.provenance = {"how": "live tool loop is measured by tests/test_pilot_provider_eval.py"}
        return res
    res.known_gaps["final_answer_claims_verified"] = (
        "The loop's final text is returned without a claim verifier (AI_FEATURES #4).")
    res.metrics["final_answer_claims_verified"] = 0.0
    res.provenance = r.provenance(res.suite)
    return res


def suite_mapping_witness(r: Runner) -> SuiteResult:
    """The verifier the AI features lean on: deterministic, so the same in both modes."""
    from services.mapping_witness import witness_mappings

    from helpers import witness_fixtures as F

    res = SuiteResult("mapping_witness", "Map: Mapping Witness (services/mapping_witness.py), no model")
    src, dst = F.customers()
    wrong = {"swap": (F.SWAPPED_NAMES, {"given", "family"}, "DMY"),
             "cents": (F.DOLLARS_INTO_CENTS, {"balance"}, "DMY"),
             "date_order": (F.CORRECT_MAPPING, {"joined"}, "MDY"),
             "username_email": (F.LOGIN_INTO_EMAIL_ONLY, {"login"}, "DMY")}
    caught, false_flags, judged = [], 0, 0
    for name, (maps, expected, locale) in wrong.items():
        for mode, rows in (("keyed", dst), ("source_only", None)):
            rep = witness_mappings(maps, src, rows, key=F.KEY if rows else None, date_locale=locale)
            flagged = {p["source"] for p in rep["pairs"] if p["status"] == "flagged"}
            caught.append(expected <= flagged)
            false_flags += len(flagged - expected)
            judged += len(rep["pairs"]) - len(expected)
            res.cases.append(CaseResult(f"{name}:{mode}", expected <= flagged, f"flagged={sorted(flagged)}"))
    for locale in ("DMY", ""):
        for rows in (dst, None):
            rep = witness_mappings(F.CORRECT_MAPPING, src, rows, key=F.KEY if rows else None, date_locale=locale)
            false_flags += rep["summary"]["flagged"]
            judged += rep["summary"]["pairs"]
    res.metrics["fixture_recall"] = _rate(caught)
    res.metrics["fixture_false_flag_rate"] = round(false_flags / judged, 4) if judged else 0.0
    res.floors["fixture_recall"] = 1.0
    res.floors["fixture_no_false_flags"] = 1.0
    res.metrics["fixture_no_false_flags"] = 1.0 if false_flags == 0 else 0.0
    holdout = HOLDOUT_PATH
    res.metrics["holdout_present"] = 1.0 if holdout.exists() else 0.0
    res.floors["holdout_present"] = 1.0
    if holdout.exists():
        sys.path.insert(0, str(API_ROOT / "scripts"))
        from witness_holdout_eval import load_tables, run as run_holdout

        summary = run_holdout(load_tables(str(holdout)), 20, with_mapper=False)["summary"]
        false_flags = sum(int(summary[k].get("flag_on_correct") or 0)
                          for k in ("gold_keyed", "gold_source_only"))
        res.metrics["holdout_no_false_flags"] = 1.0 if false_flags == 0 else 0.0
        res.floors["holdout_no_false_flags"] = 1.0
        res.metrics["holdout_injected_swap_recall"] = summary["injected_swaps_keyed"]["recall_both"] or 0.0
        # Measured 2026-11-21 (0.0476): 203 of 231 held-out columns carry one sample, and a column with one
        # value cannot prove a pair. A floor at today's value so it cannot regress.
        res.floors["holdout_injected_swap_recall"] = 0.04
    else:
        res.known_gaps["holdout"] = (
            f"MISSING: {HOLDOUT_PATH.name} is not present; the held-out gate cannot run (this fails the suite).")
    res.provenance = {"how": "deterministic"}
    return res


SUITES: dict[str, Callable[[Runner], SuiteResult]] = {
    "mapping_refinement": suite_mapping_refinement,
    "parity_explanation": suite_parity_explanation,
    "validation_assistant": suite_validation_assistant,
    "pilot_docs_narration": suite_pilot_docs_narration,
    "pilot_narration": suite_pilot_narration,
    "pilot_agent_loop": suite_pilot_agent_loop,
    "mapping_witness": suite_mapping_witness,
}


def live_provider() -> str | None:
    for name, env in (("anthropic", "ANTHROPIC_API_KEY"), ("openai", "OPENAI_API_KEY")):
        if (os.environ.get(env) or "").strip():
            return name
    return None


def markdown(report: dict[str, Any]) -> str:
    lines = [f"### ai-benchmarks ({report['mode']})", ""]
    if report.get("not_run"):
        return "\n".join(lines + [f"**NOT RUN** — {report['not_run']}", ""])
    lines.append(f"**{'PASS' if report['passed'] else 'FAIL'}** — {report['suites_passed']}/{len(report['suites'])} suites")
    lines.append("")
    lines.append("| Suite | Result | Metrics (floor) |")
    lines.append("|---|---|---|")
    for s in report["suites"]:
        ms = ", ".join(
            f"{k} {v:g}" + (f" (≥{s['floors'][k]:g})" if k in s["floors"] else "") for k, v in s["metrics"].items())
        verdict = s["status"] if s["status"] != "ran" else ("pass" if s["passed"] else "FAIL")
        lines.append(f"| `{s['suite']}` | {verdict} | {ms} |")
    gaps = [(s["suite"], k, v) for s in report["suites"] for k, v in s["known_gaps"].items()]
    if gaps:
        lines += ["", "**Known gaps (measured, not gated):**"]
        lines += [f"- `{suite}` {k}: {v}" for suite, k, v in gaps]
    drift = [s["suite"] for s in report["suites"] if s["prompt_drift"]]
    if drift:
        lines += ["", f"**Prompt drift** (cassettes recorded for an older prompt; re-record): {', '.join(drift)}"]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("replay", "live"), default="replay")
    ap.add_argument("--suite", action="append", help="run only these suites")
    ap.add_argument("--out")
    ap.add_argument("--summary")
    ap.add_argument("--record", help="live mode: write recorded cassettes into this directory")
    ap.add_argument("--seal-prompts", action="store_true",
                    help="replay mode: store each case's request digests in its cassette (after editing a cassette)")
    args = ap.parse_args()
    report: dict[str, Any] = {"mode": args.mode, "at": datetime.now(timezone.utc).isoformat()}
    if args.mode == "live":
        provider = live_provider()
        if not provider:
            report["not_run"] = ("no provider key secret (ANTHROPIC_API_KEY or OPENAI_API_KEY); "
                                 "the live benchmark did not run and is not a pass.")
            text = markdown(report)
            print(text)
            if args.summary:
                Path(args.summary).write_text(text, encoding="utf-8")
            if args.out:
                Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
            return EXIT_NOT_RUN
        report["provider"] = provider
    else:
        # Replay never touches a network: no key, an unreachable Ollama.
        for env in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
            os.environ[env] = ""
        os.environ["OLLAMA_HOST"] = "http://127.0.0.1:9"
    os.environ.setdefault("DATAFLOW_LLM_ENABLED", "true")
    runner = Runner(args.mode, Path(args.record) if args.record else None, seal_prompts=args.seal_prompts)
    suites = [SUITES[s](runner).as_dict() for s in (args.suite or SUITES)]
    model_suites = set(CALL_SITES.values())
    if args.mode == "live":
        for s in suites:
            if s["suite"] in model_suites and s["status"] == "ran" and not runner.answers.get(s["suite"]):
                s["status"], s["passed"] = "not_run", False
        silent = [s["suite"] for s in suites if s["status"] == "not_run"]
        if silent:
            report["not_run"] = ("no model answered for " + ", ".join(silent)
                                 + "; the live benchmark is not a pass.")
    report["suites"] = suites
    report["suites_passed"] = sum(1 for s in suites if s["passed"])
    report["passed"] = all(s["passed"] for s in suites) and not report.get("not_run")
    report["recorded"] = runner.write_recordings()
    text = markdown(report)
    print(text)
    if args.summary:
        Path(args.summary).write_text(text, encoding="utf-8")
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    if report.get("not_run"):
        return EXIT_NOT_RUN
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
