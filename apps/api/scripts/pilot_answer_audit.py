"""Measure what Data Pilot actually answers, question by question.

Run:  python scripts/pilot_answer_audit.py [--json out.json] [--suite all]

Every row is one real ``DataPilotAgent.chat`` turn against the local engine, so
the numbers are what an operator gets with no cloud key configured. The audit
reports three outcomes per question:

``answered``   a substantive answer with evidence (citation or live tool read)
``deflected``  a clarification / next-step prompt — usable, but not an answer
``refused``    the "outside what the documentation covers" dead end

A high ``refused`` share on documented subjects is the regression this audit
exists to catch.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(_API_ROOT / "src"), str(_API_ROOT)):
    while _p in sys.path:
        sys.path.remove(_p)
sys.path.insert(0, str(_API_ROOT / "src"))
sys.path.insert(0, str(_API_ROOT))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

# Questions an operator of this product would actually type. Each carries the
# subject it is about so a miss can be attributed to a corpus gap rather than to
# the question being off-topic.
PRODUCT_QUESTIONS: list[tuple[str, str]] = [
    ("what is quarantine", "quarantine"),
    ("what happens to bad rows", "quarantine"),
    ("where do rejected rows go and can I replay them", "quarantine"),
    ("explain the preflight gates", "preflight"),
    ("which preflight gate blocks a lossy type change", "preflight"),
    ("why did my transfer fail with a checksum mismatch", "reconcile"),
    ("what does checksum MATCH prove", "reconcile"),
    ("what sync modes do you support", "sync_modes"),
    ("which sync mode should I pick for a nightly load", "sync_modes"),
    ("what is the difference between append and overwrite", "sync_modes"),
    ("how do I set up change data capture on mysql", "cdc"),
    ("is CDC exactly once", "cdc"),
    ("how do I connect to BigQuery", "connectors"),
    ("how do I add a postgres connection", "connectors"),
    ("can I schedule a pipeline to run every night at 2am", "schedules"),
    ("how do I pause a schedule", "schedules"),
    ("what is a data contract", "contracts"),
    ("who can approve a PII gate", "permissions"),
    ("what does the viewer role let me do", "permissions"),
    ("how does semantic column mapping decide a type", "mapping"),
    ("why is my mapping confidence low", "mapping"),
    ("how do I move data from Postgres to Snowflake without losing decimal precision", "mapping"),
    ("what is SCD type 2", "sync_modes"),
    ("how do I use the API", "api"),
    ("what is MCP", "mcp"),
    ("how do I export a schedule as YAML", "gitops"),
    ("what does the transform step do before load", "transform"),
    ("can I filter rows before they are written", "transform"),
    ("how do I see why a job was slow", "jobs"),
    ("what is a row ledger", "reconcile"),
]

# Questions about the operator's own workspace — these must reach a live tool
# read, not the documentation.
WORKSPACE_QUESTIONS: list[tuple[str, str]] = [
    ("how many connectors do I have", "workspace"),
    ("list my connectors", "workspace"),
    ("show me my failed jobs", "workspace"),
    ("how many jobs ran today", "workspace"),
    ("what schedules do I have", "workspace"),
    ("what is the status of my last transfer", "workspace"),
    ("give me a summary of my workspace", "workspace"),
    ("do I have any contracts", "workspace"),
]

# Commands — the operator asking Pilot to do the work.
COMMAND_QUESTIONS: list[tuple[str, str]] = [
    ("open jobs", "navigate"),
    ("take me to connectors", "navigate"),
    ("go to the schedules page", "navigate"),
    ("show me the proofs screen", "navigate"),
]

# Conversational / meta — a chatbot has to hold these without falling over.
META_QUESTIONS: list[tuple[str, str]] = [
    ("who are you", "meta"),
    ("what can you do", "meta"),
    ("hi", "meta"),
    ("thanks", "meta"),
]

# Genuinely off-subject — these SHOULD be refused. Kept in the audit so a fix
# that simply removes the refusal is visible as a regression here.
OFF_SUBJECT_QUESTIONS: list[tuple[str, str]] = [
    ("how do I cook rice", "off_subject"),
    ("what is the capital of France", "off_subject"),
    ("write me a poem about the sea", "off_subject"),
]

SUITES = {
    "product": PRODUCT_QUESTIONS,
    "workspace": WORKSPACE_QUESTIONS,
    "command": COMMAND_QUESTIONS,
    "meta": META_QUESTIONS,
    "off_subject": OFF_SUBJECT_QUESTIONS,
}

_REFUSAL_MARKERS = (
    "outside what the datawrap documentation covers",
    "i will not answer it from guesswork",
    "i don't know",
    "i do not know",
    "i'm not sure",
)


def classify(answer: str, *, grounded: bool, clarification: str, tools: list[dict]) -> str:
    low = (answer or "").lower()
    if any(m in low for m in _REFUSAL_MARKERS):
        return "refused"
    if clarification:
        return "deflected"
    if grounded:
        return "answered"
    if any(t.get("success") for t in tools or []):
        return "answered"
    return "deflected"


def run(suite: str) -> list[dict]:
    from src.ai.copilot.pilot_agent import DataPilotAgent, carries_evidence

    agent = DataPilotAgent()
    questions: list[tuple[str, str]] = []
    if suite == "all":
        for name in SUITES:
            questions.extend(SUITES[name])
    else:
        questions = SUITES[suite]

    rows: list[dict] = []
    for question, subject in questions:
        try:
            res = agent.chat(question, [], data_context={"pilot_session_id": "audit"})
            tools = list(getattr(res, "tools_used", None) or [])
            outcome = classify(
                res.answer,
                grounded=carries_evidence(res),
                clarification=getattr(res, "needs_clarification", "") or "",
                tools=tools,
            )
            rows.append(
                {
                    "question": question,
                    "subject": subject,
                    "outcome": outcome,
                    "intent": res.intent,
                    "method": res.method,
                    "confidence": round(float(res.confidence or 0), 3),
                    "grounded": carries_evidence(res),
                    "sources": [str(s.get("title") or "") for s in (res.sources or [])][:3],
                    "tools": [t.get("name") for t in tools],
                    "answer": (res.answer or "").strip(),
                }
            )
        except Exception as exc:  # noqa: BLE001 — an exception is itself a finding
            rows.append(
                {
                    "question": question,
                    "subject": subject,
                    "outcome": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "answer": "",
                }
            )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", default="all", choices=[*SUITES, "all"])
    parser.add_argument("--json", default="")
    parser.add_argument("--show-answers", action="store_true")
    args = parser.parse_args()

    rows = run(args.suite)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1

    for row in rows:
        flag = {
            "answered": "OK  ",
            "deflected": "DEFL",
            "refused": "REFU",
            "error": "ERR ",
        }[row["outcome"]]
        print(f"{flag} [{row['subject']:<12}] {row['question']}")
        if row["outcome"] != "answered" or args.show_answers:
            body = (row.get("answer") or row.get("error") or "").replace("\n", " ")
            print(f"       -> {body[:220]}")
    total = len(rows) or 1
    print()
    print(f"total={len(rows)}  " + "  ".join(f"{k}={v} ({v * 100 // total}%)" for k, v in sorted(counts.items())))

    if args.json:
        Path(args.json).write_text(json.dumps({"counts": counts, "rows": rows}, indent=2), encoding="utf-8")
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
