"""ai-benchmarks: every AI feature replays recorded model responses offline and meets its floors.

* Every model call site in ``services/`` and ``src/`` belongs to a suite, so a
  new AI feature cannot ship without a benchmark.
* Replay is deterministic and needs no key or network; its floors hold.
* A live run without a provider key is "not run" (exit 78), never a pass.
* A cassette answering an older prompt is reported as drift, not silently passed.
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API_ROOT / "scripts"))

import run_ai_benchmarks as bench  # noqa: E402


def _model_call_sites() -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for top in bench.SCAN_DIRS:
        for path in sorted((API_ROOT / top).rglob("*.py")):
            rel = path.relative_to(API_ROOT).as_posix()
            if any(rel.startswith(x) for x in bench.SCAN_EXCLUDE):
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for node in ast.walk(fn):
                    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                            and node.func.attr in bench.MODEL_CALLS):
                        found.add((rel, fn.name))
    return found


def test_every_model_call_site_has_a_benchmark_suite():
    sites = _model_call_sites()
    # Nested helpers report their enclosing function as well; a site is covered when any enclosing one is.
    uncovered = {s for s in sites if s not in bench.CALL_SITES
                 and not any(s[0] == f and s[1] != n and (f, n) in bench.CALL_SITES for f, n in sites)}
    assert not uncovered, f"model call sites without an ai-benchmarks suite: {sorted(uncovered)}"
    stale = set(bench.CALL_SITES) - sites
    assert not stale, f"CALL_SITES lists call sites that no longer exist: {sorted(stale)}"
    assert set(bench.CALL_SITES.values()) <= set(bench.SUITES)


def test_every_model_suite_has_a_cassette():
    from services.ai_cassette import CASSETTE_DIR

    for suite in set(bench.CALL_SITES.values()):
        data = json.loads((CASSETTE_DIR / f"{suite}.json").read_text(encoding="utf-8"))
        assert data["provenance"]["how"] in ("seeded", "recorded")
        assert data["cases"], suite
        for case, raw in data["cases"].items():
            assert raw["responses"], (suite, case)
            assert len(raw["prompt_sha256"]) >= 1, f"{suite}/{case}: run --seal-prompts after editing"


@pytest.fixture()
def offline(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    for env in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
        monkeypatch.setenv(env, "")
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:9")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "true")


def _tiny_holdout(path):
    """Two tables in the committed fixture's format (not the held-out data itself)."""
    col = lambda n, t, s: {"name": n, "type": t, "samples": s}  # noqa: E731
    path.write_text(json.dumps({"tables": [
        {"id": "t1", "source": [col("fn", "VARCHAR", ["Ann", "Bob"]), col("ln", "VARCHAR", ["Kim", "Lo"]),
                                col("em", "VARCHAR", ["a@x.io", "b@y.io"])],
         "target": [{"name": "first_name", "type": "VARCHAR"}, {"name": "last_name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"}],
         "gold": {"fn": "first_name", "ln": "last_name", "em": "email"}},
    ]}), encoding="utf-8")
    return path


@pytest.mark.parametrize("suite", sorted(bench.SUITES))
def test_replay_meets_its_floors_without_drift(offline, suite, monkeypatch, tmp_path):
    if suite == "mapping_witness" and not bench.HOLDOUT_PATH.exists():
        monkeypatch.setattr(bench, "HOLDOUT_PATH", _tiny_holdout(tmp_path / "holdout.json"))
    result = bench.SUITES[suite](bench.Runner("replay")).as_dict()
    assert result["passed"], json.dumps(result, indent=1)
    assert result["floors"], "a suite with no floor proves nothing"
    assert result["prompt_drift"] == [], f"{suite}: the product's prompt changed; re-record or re-seal"


def test_replay_is_deterministic(offline):
    a = bench.suite_parity_explanation(bench.Runner("replay")).as_dict()
    b = bench.suite_parity_explanation(bench.Runner("replay")).as_dict()
    assert a["metrics"] == b["metrics"] and a["cases"] == b["cases"]


def test_known_gaps_are_reported_not_gated(offline):
    result = bench.suite_parity_explanation(bench.Runner("replay")).as_dict()
    assert result["metrics"]["invented_numbers_rejected"] == 0.0
    assert "invented_numbers_rejected" in result["known_gaps"]
    assert "invented_numbers_rejected" not in result["floors"]
    md = bench.markdown({"mode": "replay", "suites": [result], "suites_passed": 1, "passed": True})
    assert "Known gaps (measured, not gated)" in md


def test_a_changed_prompt_is_reported_as_drift(offline):
    from services.ai_cassette import Tape, anthropic_cassette
    from src.ai.llm.provider import DataTransferAnthropicProvider

    tape = Tape(responses=[{"content": [{"type": "text", "text": "ok"}]}], recorded_prompts=["0" * 64])
    with anthropic_cassette(tape):
        resp = DataTransferAnthropicProvider().generate("hello", system="s")
    assert resp.success and resp.content == "ok"
    assert tape.drift() == [0]


def test_a_cassette_never_answers_more_than_it_recorded(offline):
    from services.ai_cassette import Tape, anthropic_cassette
    from src.ai.llm.provider import DataTransferAnthropicProvider

    tape = Tape(responses=[])
    with anthropic_cassette(tape):
        resp = DataTransferAnthropicProvider().generate("hello")
    assert resp.success is False  # the provider reports the failure; nothing is invented


def test_a_missing_holdout_fails_the_witness_suite(offline, monkeypatch, tmp_path):
    monkeypatch.setattr(bench, "HOLDOUT_PATH", tmp_path / "absent.json")
    result = bench.suite_mapping_witness(bench.Runner("replay")).as_dict()
    assert result["passed"] is False and result["metrics"]["holdout_present"] == 0.0


def test_prompt_drift_fails_the_suite_not_only_the_test(offline, monkeypatch, tmp_path):
    import shutil

    import services.ai_cassette as cassette

    copy_dir = tmp_path / "cassettes"
    shutil.copytree(cassette.CASSETTE_DIR, copy_dir)
    path = copy_dir / "validation_assistant.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    for case in data["cases"].values():
        case["prompt_sha256"] = ["0" * 64 for _ in case["prompt_sha256"]]
    path.write_text(json.dumps(data), encoding="utf-8")
    monkeypatch.setattr(cassette, "CASSETTE_DIR", copy_dir)
    result = bench.suite_validation_assistant(bench.Runner("replay")).as_dict()
    assert result["prompt_drift"] and result["passed"] is False


def test_the_request_digest_covers_tool_schemas():
    from services.ai_cassette import request_digest

    tool = {"name": "list_connectors", "description": "List connectors", "input_schema": {"type": "object"}}
    changed = {**tool, "description": "Delete every connector"}
    assert request_digest({"messages": [], "tools": [tool]}) != request_digest({"messages": [], "tools": [changed]})


def test_live_with_a_key_but_no_model_answer_is_not_run(offline, monkeypatch, tmp_path, capsys):
    from src.ai.llm import provider as prov

    monkeypatch.setattr(bench, "live_provider", lambda: "anthropic")
    monkeypatch.setattr(prov.DataTransferAnthropicProvider, "_init_client", lambda self: setattr(self, "_client", None))
    for cls in (prov.DataTransferOpenAIProvider, prov.DataTransferOllamaProvider):
        monkeypatch.setattr(cls, "is_available", lambda self: False)
    summary = tmp_path / "live.md"
    monkeypatch.setattr(sys, "argv", ["run_ai_benchmarks.py", "--mode", "live", "--summary", str(summary),
                                      "--suite", "parity_explanation", "--suite", "validation_assistant"])
    assert bench.main() == bench.EXIT_NOT_RUN
    text = summary.read_text(encoding="utf-8")
    assert "**NOT RUN**" in text and "PASS" not in text


def test_live_without_a_key_is_not_run_never_a_pass(tmp_path):
    env = {"PATH": "/usr/bin:/bin", "PYTHONPATH": str(API_ROOT), "DATAFLOW_DATA_DIR": str(tmp_path),
           "HOME": str(tmp_path)}
    summary = tmp_path / "summary.md"
    proc = subprocess.run(
        [sys.executable, str(API_ROOT / "scripts" / "run_ai_benchmarks.py"), "--mode", "live",
         "--summary", str(summary)],
        cwd=API_ROOT, env=env, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == bench.EXIT_NOT_RUN, proc.stdout + proc.stderr
    assert "**NOT RUN**" in summary.read_text(encoding="utf-8")
    assert "PASS" not in summary.read_text(encoding="utf-8")
