"""Measure what Data Pilot actually answers, question by question.

Run:  python scripts/pilot_answer_audit.py [--suite all] [--json out.json]

Every row is one real ``DataPilotAgent.chat`` turn against the local engine, so
the numbers are what an operator gets with no cloud key configured.

Two things are measured, because they fail independently:

``outcome``    ``answered`` (substantive, with evidence) · ``deflected``
               (clarification or next-step prompt) · ``refused`` (the "outside
               what the documentation covers" dead end) · ``error``.

``on_target``  whether the answer contains something only a *correct* answer to
               that question would contain. Answering is not the same as
               answering the question: a question about row filtering can be
               answered fluently, with citations, entirely out of the quarantine
               article. Each product question therefore carries the phrases a
               right answer must touch, taken from the shipped documentation and
               the enforcing modules — never invented.

``--suite off_subject`` is the honesty half of the same measurement: those
questions MUST be refused, so a change that merely removes refusals shows up
here as a regression rather than as progress.
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

# One case = (question, subject, must_include).
#
# ``must_include`` is a tuple of alternatives; the answer passes when it
# contains any one of them, compared case-insensitively. They are deliberately
# short and factual — a phrase from the shipped documentation or a product
# label — so the expectation tests whether the right material was retrieved,
# not whether one particular sentence was chosen.
Case = tuple[str, str, tuple[str, ...]]

PRODUCT_QUESTIONS: list[Case] = [
    # --- quarantine / bad rows ---------------------------------------------
    ("what is quarantine", "quarantine", ("never silently dropped", "no row disappears")),
    ("what happens to bad rows", "quarantine", ("quarantine", "rejected")),
    ("where do rejected rows go and can I replay them", "quarantine", ("replay", "quarantine")),
    ("do you ever drop rows silently", "quarantine", ("never silently dropped", "no row disappears", "quarantine")),
    ("how do I get the list of rows that failed", "quarantine", ("quarantine", "csv", "export")),
    ("what is a dead letter table", "quarantine", ("dead-letter", "dead letter")),
    # --- preflight gates ----------------------------------------------------
    ("explain the preflight gates", "preflight", ("g1", "g9")),
    ("what are the preflight gates", "preflight", ("g1", "g9")),
    ("how many gates are there before a write", "preflight", ("nine", "9 gates", "g9")),
    ("which preflight gate blocks a lossy type change", "preflight", ("accept risk", "lossy", "g3", "g9")),
    ("what do I do when validate is blocked", "preflight", ("suggested fixes", "accept risk", "remap", "blocked gate")),
    ("do scheduled runs skip preflight", "preflight", ("same validate gates", "same gate engine", "reuses the same")),
    ("is a green test on connectors enough to skip validation", "preflight", ("does **not** skip preflight", "does not skip preflight")),
    # --- reconcile / proof --------------------------------------------------
    ("what does checksum MATCH prove", "reconcile", ("match", "checksum")),
    ("how do I prove the row counts matched after a transfer", "reconcile", ("checksum", "match", "reconcil")),
    ("what is a row ledger", "reconcile", ("row accounting", "ledger")),
    ("can the ledger be closed on writer ack", "reconcile", ("refuses", "writer ack")),
    ("what happens if the destination count does not match", "reconcile", ("mismatch", "unbalanced", "surfaced")),
    # --- sync modes ---------------------------------------------------------
    ("what sync modes do you support", "sync_modes", ("full_refresh", "incremental", "upsert")),
    ("which sync mode should I pick for a nightly load", "sync_modes", ("full_refresh_overwrite", "incremental_append", "incremental_deduped", "upsert")),
    ("what is the difference between append and overwrite", "sync_modes", ("append", "overwrite")),
    ("what is SCD type 2", "sync_modes", ("scd2", "validity window", "history")),
    ("what does mirror mode do to deleted rows", "sync_modes", ("removed", "deletion", "mirror")),
    ("does upsert need a primary key", "sync_modes", ("primary key", "key")),
    ("what is reverse ETL", "sync_modes", ("reverse_etl", "warehouse back")),
    ("which modes are refused for a stored procedure source", "sync_modes", ("cdc", "mirror", "scd2", "refused")),
    # --- CDC ----------------------------------------------------------------
    ("what is change data capture", "cdc", ("cdc", "source log")),
    ("is CDC exactly once", "cdc", ("at-least-once", "idempotent")),
    ("how do I set up change data capture", "cdc", ("cdc", "source log", "sync mode")),
    # --- connectors ---------------------------------------------------------
    ("how do I connect to BigQuery", "connectors", ("new connection", "connectors", "driver")),
    ("how do I add a postgres connection", "connectors", ("new connection", "driver")),
    ("which engines can I connect to", "connectors", ("postgresql", "snowflake", "sqlalchemy")),
    ("what does transfer ready mean on a connector", "connectors", ("transfer-ready", "transfer ready", "certified", "catalog")),
    ("my connector test passed but the transfer failed, why", "connectors", ("does **not** skip preflight", "does not skip preflight", "validate still runs")),
    # --- schedules / pipelines ---------------------------------------------
    ("can I schedule a pipeline to run every night at 2am", "schedules", ("pipeline", "cadence", "recurring")),
    ("how do I pause a schedule", "schedules", ("pause", "activate", "pipelines")),
    ("how do I create a recurring sync", "schedules", ("create recurring sync", "save pipeline", "new pipeline")),
    ("what is the difference between jobs and pipelines", "schedules", ("pipelines owns the schedule", "jobs owns the proof", "each tick")),
    ("does every pipeline tick create a job", "schedules", ("creates a job", "every cadence tick", "tick")),
    # --- contracts ----------------------------------------------------------
    ("what is a data contract", "contracts", ("signed schema agreement", "contract")),
    ("how do I make a pipeline fail closed if the schema drifts", "contracts", ("require signed", "signed contract", "fail closed")),
    ("what is the difference between a draft and a signed contract", "contracts", ("draft", "signed")),
    # --- permissions --------------------------------------------------------
    ("who can approve a PII gate", "permissions", ("job.run", "operator", "editor", "admin")),
    ("what does the viewer role let me do", "permissions", ("read", "viewer")),
    ("who can run transfers", "permissions", ("editor", "operator", "admin", "role")),
    ("what are the roles in this product", "permissions", ("viewer", "operator", "editor", "admin")),
    ("what happens if my role label is unknown", "permissions", ("fails closed", "viewer")),
    ("can Pilot do something my role cannot", "permissions", ("never exceed", "same permission", "rbac")),
    # --- mapping ------------------------------------------------------------
    ("how does semantic column mapping decide a type", "mapping", ("confidence", "synonym", "semantic role")),
    ("why is my mapping confidence low", "mapping", ("confidence", "synonym", "rematch", "accept")),
    ("how do I move data from Postgres to Snowflake without losing decimal precision", "mapping", ("decimal", "lossy", "accept risk", "precision")),
    ("what semantic roles do you detect", "mapping", ("amount", "email", "identifier", "timestamp")),
    ("what happens when the source adds a column", "mapping", ("drift", "propagate", "schema change")),
    # --- schema drift -------------------------------------------------------
    ("what schema change policies are there", "schema_policy", ("type_locked", "propagate_columns", "propagate_all", "manual")),
    ("how do I stop a type change from being applied", "schema_policy", ("type_locked", "rejects type changes", "type change")),
    # --- jobs / theater -----------------------------------------------------
    ("how do I see why a job was slow", "jobs", ("phase", "duration", "throughput")),
    ("what are the job phases", "jobs", ("preflight", "read", "write", "reconcile")),
    ("my job looks stuck, what do I check", "jobs", ("phase", "log", "waiting")),
    ("where do I find the log for a run", "jobs", ("log", "theater", "jobs")),
    # --- transforms ---------------------------------------------------------
    ("what does the transform step do before load", "transform", ("transform", "cast", "map")),
    ("can I filter rows before they are written", "transform", ("transform", "filter", "map")),
    ("where do I normalize an email column", "transform", ("normalize email", "transform", "map")),
    # --- API / MCP / interfaces --------------------------------------------
    ("how do I use the API", "api", ("/api/v1", "endpoint")),
    ("how do I authenticate against the API", "api", ("authentication", "token", "key", "bearer")),
    ("do you have webhooks", "api", ("webhook",)),
    ("what is MCP", "mcp", ("mcp", "agent")),
    ("how do I connect Cursor to this", "mcp", ("mcp", "server entry", "cursor")),
    ("do agents get my destination passwords", "mcp", ("no raw destination passwords", "inherit workspace rbac", "rbac")),
    # --- gitops -------------------------------------------------------------
    ("how do I export a schedule as YAML", "gitops", ("export yaml", "yaml")),
    ("can I keep my pipelines in git", "gitops", ("yaml", "export", "import")),
    # --- enterprise ---------------------------------------------------------
    ("how do I set up SSO", "enterprise", ("sso", "saml", "settings")),
    ("is my data encrypted", "enterprise", ("encrypted at rest", "tls", "byok")),
    ("what is BYOK", "enterprise", ("byok", "keys", "encryption")),
    # --- product / positioning ---------------------------------------------
    ("what is Datawrap", "product", ("universal data transfer", "transfer studio")),
    ("how is this different from writing ETL scripts", "product", ("semantic mapping", "quarantine", "checksum")),
    ("what is Query Playground", "query", ("query playground", "read-only", "read only")),
    ("how do I do my first transfer", "product", ("transfer studio", "source", "destination", "map")),
]

# Questions about the operator's own workspace — these must reach a live tool
# read, not the documentation. On an empty workspace the honest answer is "none
# yet", so the expectation is the live surface, not a row count.
WORKSPACE_QUESTIONS: list[Case] = [
    ("how many connectors do I have", "workspace", ("connector",)),
    ("list my connectors", "workspace", ("connector",)),
    ("show me my failed jobs", "workspace", ("job", "transfer")),
    ("how many jobs ran today", "workspace", ("job", "transfer")),
    ("what schedules do I have", "workspace", ("pipeline", "schedule")),
    ("what is the status of my last transfer", "workspace", ("job", "transfer")),
    ("give me a summary of my workspace", "workspace", ("connector", "job", "workspace")),
    ("do I have any contracts", "workspace", ("contract",)),
]

# Commands — the operator asking Pilot to do the work.
COMMAND_QUESTIONS: list[Case] = [
    ("open jobs", "navigate", ("jobs",)),
    ("take me to connectors", "navigate", ("connectors",)),
    ("go to the schedules page", "navigate", ("pipelines", "schedules")),
    ("show me the proofs screen", "navigate", ("proof",)),
]

# Conversational / meta — a chatbot has to hold these without falling over.
META_QUESTIONS: list[Case] = [
    ("who are you", "meta", ("pilot",)),
    ("what can you do", "meta", ("connector", "transfer", "job")),
    ("hi", "meta", ("pilot", "ask", "start")),
    ("thanks", "meta", ("welcome", "next", "summarize")),
]

# Genuinely off-subject — these MUST be refused. Kept in the audit so a fix that
# simply removes the refusal is visible as a regression here.
OFF_SUBJECT_QUESTIONS: list[Case] = [
    ("how do I cook rice", "off_subject", ()),
    ("what is the capital of France", "off_subject", ()),
    ("write me a poem about the sea", "off_subject", ()),
    ("what is the weather in Paris", "off_subject", ()),
    ("who won the world cup in 1998", "off_subject", ()),
    ("zzzz qqqq wwww", "off_subject", ()),
    ("what semantic patterns match subscriber_id column naming", "off_subject", ()),
]

SUITES: dict[str, list[Case]] = {
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


def on_target(answer: str, must_include: tuple[str, ...]) -> bool | None:
    """Whether the answer touches material only a right answer would touch.

    ``None`` when the case states no expectation (the off-subject suite, where
    the outcome *is* the measurement).
    """
    if not must_include:
        return None
    low = (answer or "").lower()
    return any(phrase.lower() in low for phrase in must_include)


def run(suite: str, *, fresh_session: bool = False) -> list[dict]:
    """Run one suite.

    By default every question runs in one shared session, which is what a real
    conversation looks like and is how session state leaking between turns shows
    up. ``fresh_session`` isolates each question instead, so a retrieval defect
    can be told apart from a follow-up-resolution defect.
    """
    from src.ai.copilot.pilot_agent import DataPilotAgent, carries_evidence

    agent = DataPilotAgent()
    cases: list[Case] = []
    if suite == "all":
        for name in SUITES:
            cases.extend(SUITES[name])
    else:
        cases = SUITES[suite]

    rows: list[dict] = []
    for index, (question, subject, must_include) in enumerate(cases):
        session = f"audit-{index}" if fresh_session else "audit"
        try:
            res = agent.chat(question, [], data_context={"pilot_session_id": session})
            tools = list(getattr(res, "tools_used", None) or [])
            answer = (res.answer or "").strip()
            rows.append(
                {
                    "question": question,
                    "subject": subject,
                    "outcome": classify(
                        answer,
                        grounded=carries_evidence(res),
                        clarification=getattr(res, "needs_clarification", "") or "",
                        tools=tools,
                    ),
                    "on_target": on_target(answer, must_include),
                    "expected": list(must_include),
                    "intent": res.intent,
                    "method": res.method,
                    "confidence": round(float(res.confidence or 0), 3),
                    "grounded": carries_evidence(res),
                    "sources": [str(s.get("title") or "") for s in (res.sources or [])][:3],
                    "tools": [t.get("name") for t in tools],
                    "answer": answer,
                }
            )
        except Exception as exc:  # noqa: BLE001 — an exception is itself a finding
            rows.append(
                {
                    "question": question,
                    "subject": subject,
                    "outcome": "error",
                    "on_target": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "answer": "",
                }
            )
    return rows


_FLAG = {"answered": "OK  ", "deflected": "DEFL", "refused": "REFU", "error": "ERR "}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", default="all", choices=[*SUITES, "all"])
    parser.add_argument("--json", default="")
    parser.add_argument("--show-answers", action="store_true")
    parser.add_argument(
        "--only-misses",
        action="store_true",
        help="print only the cases that were not answered on target",
    )
    parser.add_argument(
        "--fresh-session",
        action="store_true",
        help="isolate every question in its own session instead of one conversation",
    )
    args = parser.parse_args()

    rows = run(args.suite, fresh_session=args.fresh_session)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1

    scored = [r for r in rows if r["on_target"] is not None]
    hits = [r for r in scored if r["on_target"]]
    misses = [r for r in scored if not r["on_target"]]

    for row in rows:
        miss = row["on_target"] is False
        if args.only_misses and not miss:
            continue
        mark = "  OFF-TARGET" if miss else ""
        print(f"{_FLAG[row['outcome']]} [{row['subject']:<12}] {row['question']}{mark}")
        if miss or row["outcome"] != "answered" or args.show_answers:
            body = (row.get("answer") or row.get("error") or "").replace("\n", " ")
            print(f"       -> {body[:260]}")
            if miss:
                print(f"       expected one of: {row['expected']}")

    total = len(rows) or 1
    print()
    print(
        f"total={len(rows)}  "
        + "  ".join(f"{k}={v} ({v * 100 // total}%)" for k, v in sorted(counts.items()))
    )
    if scored:
        print(
            f"on_target={len(hits)}/{len(scored)} "
            f"({len(hits) * 100 // len(scored)}%)  off_target={len(misses)}"
        )

    if args.json:
        Path(args.json).write_text(
            json.dumps(
                {
                    "counts": counts,
                    "on_target": len(hits),
                    "scored": len(scored),
                    "rows": rows,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
