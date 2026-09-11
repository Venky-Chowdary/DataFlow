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
import contextlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(_API_ROOT / "src"), str(_API_ROOT)):
    while _p in sys.path:
        sys.path.remove(_p)
sys.path.insert(0, str(_API_ROOT / "src"))
sys.path.insert(0, str(_API_ROOT))

_FIXTURE_DIR = Path(tempfile.mkdtemp(prefix="pilot-audit-"))

# The operation suite asks Pilot to read and count real rows, so the audit owns
# its workspace instead of measuring whatever connectors happen to be saved on
# the machine: both stores live in a temp directory, the operator's own
# connectors are never touched, and the numbers are reproducible on any
# checkout. The file backend is named rather than sniffed so a reachable Mongo
# cannot change what the audit measures.
_ISOLATED_ENV = {
    "DATAFLOW_JOB_STORE": "memory",
    "DATAFLOW_DISABLE_OBJECT_STORE": "1",
    "DATAFLOW_CONNECTOR_STORE": str(_FIXTURE_DIR / "connectors.json"),
    "DATAFLOW_CONNECTOR_STORE_BACKEND": "file",
    "DATAFLOW_PILOT_MEMORY_PATH": str(_FIXTURE_DIR / "pilot_memory.json"),
    "DATAFLOW_SEED_DEMO": "0",
}


@contextlib.contextmanager
def isolated_stores():
    """Redirect the stores for the duration of a run, then put them back.

    This was applied at import time, which is wrong in the one place it matters
    most: ``tests/test_pilot_answer_audit_eval.py`` imports this module so CI
    holds the measured floors, and pytest imports every test module while
    *collecting*, before running any test. So the redirect landed on the whole
    session from the first moment, and two suites that had nothing to do with
    the pilot — BYOK connector-secret wrapping and the quarantine API — built
    connectors in one store and read them back from another. Both passed alone
    and failed in the suite, which is the signature of exactly this.

    Every other store override in the test tree goes through
    ``monkeypatch.setenv`` and is therefore undone; this is the same contract
    for a module that also has to work as a standalone script.
    """
    previous = {key: os.environ.get(key) for key in _ISOLATED_ENV}
    os.environ.update(_ISOLATED_ENV)
    # The chosen backend is cached on first resolution, so a session that
    # already picked one would keep it and ignore the environment above. Looked
    # up again on the way out because the module is imported lazily and may
    # arrive during the run.
    store = sys.modules.get("services.connector_store")
    cached = getattr(store, "_backend_choice", None) if store is not None else None
    if store is not None:
        store._backend_choice = None
    try:
        yield
    finally:
        store = sys.modules.get("services.connector_store")
        if store is not None:
            store._backend_choice = cached
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

FIXTURE_CONNECTOR = "Audit SQLite"
FIXTURE_TABLE = "orders"
FIXTURE_ROWS = 12


def seed_fixture_workspace() -> None:
    """Create one real SQLite connector with one real table.

    ``count the rows in orders`` has to reach a database and come back with 12,
    otherwise the suite only proves that Pilot can phrase an error nicely.
    """
    from services.connector_store import create_connector

    db_path = _FIXTURE_DIR / "audit.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {FIXTURE_TABLE} ("
            "id INTEGER PRIMARY KEY, customer TEXT NOT NULL, region TEXT,"
            " amount REAL, status TEXT)"
        )
        conn.execute(f"DELETE FROM {FIXTURE_TABLE}")
        conn.executemany(
            f"INSERT INTO {FIXTURE_TABLE} (id, customer, region, amount, status)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (i, f"customer-{i}", ("emea", "apac", "amer")[i % 3], 10.0 * i,
                 ("paid", "pending")[i % 2])
                for i in range(1, FIXTURE_ROWS + 1)
            ],
        )
        conn.commit()

    create_connector(
        {
            "name": FIXTURE_CONNECTOR,
            "type": "sqlite",
            "role": "source",
            "database": str(db_path),
            "connection_string": str(db_path),
        }
    )

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
    ("what is a dead letter table", "quarantine", ("dead-letter", "quarantine")),
    # --- preflight gates ----------------------------------------------------
    ("explain the preflight gates", "preflight", ("g1", "g9")),
    ("what are the preflight gates", "preflight", ("g1", "g9")),
    ("how many gates are there before a write", "preflight", ("nine", "9 gates", "g9")),
    ("which preflight gate blocks a lossy type change", "preflight", ("accept risk", "lossy", "g3", "g9")),
    ("what do I do when validate is blocked", "preflight", ("suggested fixes", "accept risk", "remap", "blocked gate")),
    ("do scheduled runs skip preflight", "preflight", ("same validate gates", "same gate engine", "reuses the same")),
    ("is a green test on connectors enough to skip validation", "preflight", ("does **not** skip preflight", "does not skip preflight")),
    # --- reconcile / proof --------------------------------------------------
    ("what does checksum MATCH prove", "reconcile", ("proof of the load", "row fidelity")),
    ("how do I prove the row counts matched after a transfer", "reconcile", ("checksum", "match", "reconcil")),
    ("what is a row ledger", "reconcile", ("row accounting", "ledger")),
    ("can the ledger be closed on writer ack", "reconcile", ("refuses", "writer ack")),
    ("what happens if the destination count does not match", "reconcile", ("mismatch", "unbalanced", "surfaced")),
    # --- sync modes ---------------------------------------------------------
    ("what sync modes do you support", "sync_modes", ("full_refresh", "incremental", "upsert")),
    ("which sync mode should I pick for a nightly load", "sync_modes", ("full_refresh_overwrite", "incremental_append", "incremental_deduped", "upsert")),
    ("what is the difference between append and overwrite", "sync_modes", ("insert-only", "replaces the destination")),
    ("what is SCD type 2", "sync_modes", ("scd2", "validity window", "history")),
    ("what does mirror mode do to deleted rows", "sync_modes", ("removed", "deletion", "mirror")),
    ("does upsert need a primary key", "sync_modes", ("reliable key", "updates existing")),
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
    ("what is the difference between a draft and a signed contract", "contracts", ("schema agreement",)),
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
    ("how do I export a schedule as YAML", "gitops", ("detail drawer", "gitops")),
    ("can I keep my pipelines in git", "gitops", ("yaml", "export", "import")),
    # --- enterprise ---------------------------------------------------------
    ("how do I set up SSO", "enterprise", ("sso", "saml", "settings")),
    ("is my data encrypted", "enterprise", ("encrypted at rest", "tls", "byok")),
    ("what is BYOK", "enterprise", ("kms", "encrypted at rest")),
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
    ("open jobs", "navigate", ("opening",)),
    ("take me to connectors", "navigate", ("opening",)),
    ("go to the schedules page", "navigate", ("pipelines", "schedules")),
    ("show me the proofs screen", "navigate", ("proof",)),
]

# Operations — the operator asking Pilot to *do* one of the things this product
# does, in the words they would actually use. The seeded workspace holds one
# SQLite connector with one 12-row ``orders`` table and no jobs, so the live
# reads must come back with real values while the rest show Pilot understood
# the operation and named the one input it is missing. Answering an operation
# request with an inventory listing, a documentation essay, or "I'm not sure
# how to do that" all count as failures.
OPERATION_QUESTIONS: list[Case] = [
    # create_connector — the endpoint is stated in prose, not labelled fields
    (
        "create a connector to postgres at localhost:5433",
        "connector_create",
        ("username", "password", "connection url"),
    ),
    (
        "add a mysql connector at db.acme.com:3306 user root password secret database orders",
        "connector_create",
        ("could not connect", "host not found", "created", "confirm"),
    ),
    # introspect_connector_schema — "the orders table" is how people name one
    ("show me the schema of the orders table", "schema", ("connector", "customer")),
    (
        "what columns are in the orders table on Audit SQLite",
        "schema",
        ("customer", "region"),
    ),
    ("list the tables on Audit SQLite", "schema", ("orders",)),
    # transfer planning
    ("run a transfer from orders to orders_warehouse", "transfer", ("connector",)),
    (
        "plan a transfer of orders from Audit SQLite to Audit SQLite",
        "transfer",
        ("source and destination", "nothing to move", "plan"),
    ),
    ("start a transfer in transfer studio", "transfer", ("open transfer studio",)),
    # advisory
    (
        "which sync mode should I use for a nightly load",
        "sync_mode",
        ("full_refresh_overwrite", "incremental_append", "full overwrite"),
    ),
    # schedules
    ("pause my nightly orders sync schedule", "schedule", ("pipeline", "schedule")),
    ("run my nightly orders sync now", "schedule", ("pipeline", "schedule")),
    # live aggregation — real rows, so the expected value is the real answer
    ("count the rows in orders on Audit SQLite", "analytics", ("12",)),
    ("what is the average amount in orders on Audit SQLite", "analytics", ("65",)),
    ("count orders by region on Audit SQLite", "analytics", ("emea", "apac", "amer")),
    ("show me 3 rows from orders on Audit SQLite", "analytics", ("customer-1", "customer")),
    # quarantine replay — "quarantined" describes the rows, it is not a table
    ("replay the quarantined rows on my last job", "quarantine", ("job", "transfer")),
    # refusals that must stay refusals: destructive work is not prompt-driven
    (
        "delete all my connectors",
        "unsupported",
        ("not something a prompt", "read-only", "in the ui"),
    ),
    (
        "drop the orders table",
        "unsupported",
        ("not something a prompt", "read-only", "in the ui"),
    ),
    (
        "export my data to csv",
        "unsupported",
        ("not something i can do", "query", "sample"),
    ),
]

# Type fidelity — the product's core wedge, and the subject it was least able to
# talk about: 12 of these 25 were refused outright because the type space was
# documented only in the enforcing modules. The expectations are the carrier,
# the bound or the rule that module makes true, never a paraphrase.
FIDELITY_QUESTIONS: list[Case] = [
    ("what happens to null values", "null", ("not null", "quarantined")),
    (
        "what happens to a not null column with empty values",
        "null",
        ("not null", "quarantined", "coerced-null"),
    ),
    ("how do you handle empty strings versus null", "null", ("null", "coerced")),
    ("how do you handle timezones", "timezone", ("instant", "offset label")),
    ("what timezone are timestamps stored in", "timezone", ("utc", "offset label")),
    (
        "what happens to a timestamp without timezone",
        "timezone",
        ("wall clock", "wall-clock", "utc_invented_from_naive"),
    ),
    ("what happens to dates before 1970", "timezone", ("1970", "out of range")),
    ("how do you handle booleans across databases", "carrier", ("boolean",)),
    ("how do you handle arrays", "carrier", ("jsonb", "variant", "native only")),
    ("how do you handle binary blobs", "carrier", ("bytea", "longblob", "bytes")),
    ("what happens to json columns", "carrier", ("jsonb", "variant")),
    ("how do you handle unsigned integers", "carrier", ("unsigned", "decimal")),
    ("how are floats rounded", "carrier", ("re-rounded", "lossy coercion")),
    ("how is decimal precision preserved", "carrier", ("numeric", "bignumeric", "38")),
    (
        "how do you handle very large decimals",
        "carrier",
        ("digits", "precision", "bignumeric"),
    ),
    # Deliberately keyed on a carrier *pairing*: the word "text" alone appears
    # in "rather than as text everywhere", which is not an answer to this.
    (
        "what string type is created on postgres",
        "carrier",
        ("postgresql text", "text"),
    ),
    (
        "what happens if a numeric overflows the destination type",
        "carrier",
        ("preflight finding", "lossy", "capacity"),
    ),
    ("what character encoding do you use", "encoding", ("unicode", "utf8")),
    (
        "what happens to a character the destination cannot store",
        "encoding",
        ("quarantined", "unsupported"),
    ),
    ("what happens to primary keys", "aspect", ("primary key", "carried")),
    (
        "how do you handle identity columns",
        "aspect",
        ("identity", "auto_increment"),
    ),
    ("what about generated columns", "aspect", ("generated", "unsupported")),
    ("do you preserve column order", "aspect", ("carried", "certificate")),
    (
        "what happens to case sensitivity in table names",
        "aspect",
        ("name case", "case folding", "deterministic suffix"),
    ),
    (
        "what happens to duplicate primary keys",
        "aspect",
        ("primary key", "uniqueness", "unique"),
    ),
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

# Documented subjects asked the way an operator asks them, rather than the way
# the documentation words them. Every other suite here was written alongside the
# retrieval it measures, which makes it a good regression net and a poor
# estimate of a question nobody anticipated: those suites sit at 137/137 while
# this one does not, and the difference is the honest measure of how far the
# engine generalizes.
#
# Each expected phrase was checked to exist in the shipped corpus, so a miss is
# always "the engine did not reach documentation that is there" and never "the
# audit asked for something the product never says". This suite's floor is
# therefore below its case count on purpose — raise it by fixing retrieval, not
# by deleting a case or by loosening a phrase into something the question's own
# words would satisfy.
NATURAL_QUESTIONS: list[Case] = [
    # --- security / enterprise ---------------------------------------------
    ("can I use my own encryption key", "natural", ("byok", "kms")),
    ("is my data encrypted", "natural", ("encrypted at rest", "tls")),
    ("can I limit who sees a connector", "natural", ("viewer", "rbac", "permission")),
    ("what roles can approve a risky mapping", "natural", ("editor", "admin")),
    # --- failure and recovery ----------------------------------------------
    ("what happens if a job fails halfway", "natural", ("checkpoint", "resume")),
    ("how do I see which rows were rejected", "natural", ("quarantine",)),
    ("how do I get notified when a job fails", "natural", ("webhook", "job.failed")),
    ("how do I export proof for an auditor", "natural", ("archive", "checksum")),
    # --- routes, cadence, modes --------------------------------------------
    ("can I schedule a transfer every hour", "natural", ("cron", "hourly", "cadence")),
    ("does it support scd type 2", "natural", ("scd2",)),
    ("how do I connect a postgres database", "natural", ("new connection", "postgresql")),
    # --- fidelity ----------------------------------------------------------
    ("how big can a decimal be", "natural", ("digits", "scale")),
    ("what does the row ledger prove", "natural", ("accounting", "conservation")),
    ("what is a contract", "natural", ("schema agreement",)),
]

SUITES: dict[str, list[Case]] = {
    "product": PRODUCT_QUESTIONS,
    "workspace": WORKSPACE_QUESTIONS,
    "operation": OPERATION_QUESTIONS,
    "fidelity": FIDELITY_QUESTIONS,
    "command": COMMAND_QUESTIONS,
    "meta": META_QUESTIONS,
    "natural": NATURAL_QUESTIONS,
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


def _words(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", (text or "").lower()))


def echoes_the_question(
    question: str, answer: str, must_include: tuple[str, ...]
) -> bool:
    """Whether every phrase that matched is built only from the question's words.

    A measurement that can be satisfied by quoting the question back is not a
    measurement. "What about generated columns" expected ``generated`` and was
    scored a pass by the error string ``Could not read the schema of
    'generated'`` — the routing defect underneath it stayed invisible for a
    whole run. Expectations flagged here must be restated in words only a
    correct answer would use.
    """
    low = (answer or "").lower()
    asked = _words(question)
    matched = [p for p in must_include if p.lower() in low]
    return bool(matched) and all(_words(p) <= asked for p in matched)


def run(suite: str, *, fresh_session: bool = False) -> list[dict]:
    """Run one suite.

    By default every question runs in one shared session, which is what a real
    conversation looks like and is how session state leaking between turns shows
    up. ``fresh_session`` isolates each question instead, so a retrieval defect
    can be told apart from a follow-up-resolution defect.
    """
    cases: list[Case] = []
    if suite == "all":
        for name in SUITES:
            cases.extend(SUITES[name])
    else:
        cases = SUITES[suite]

    rows: list[dict] = []
    with isolated_stores():
        from src.ai.copilot.pilot_agent import DataPilotAgent, carries_evidence

        seed_fixture_workspace()
        agent = DataPilotAgent()
        for index, (question, subject, must_include) in enumerate(cases):
            session = f"audit-{index}" if fresh_session else "audit"
            try:
                res = agent.chat(
                    question, [], data_context={"pilot_session_id": session}
                )
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
                        "echo_only": echoes_the_question(question, answer, must_include),
                        "expected": list(must_include),
                        "intent": res.intent,
                        "method": res.method,
                        "confidence": round(float(res.confidence or 0), 3),
                        "grounded": carries_evidence(res),
                        "sources": [
                            str(s.get("title") or "") for s in (res.sources or [])
                        ][:3],
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
    echoes = [r for r in hits if r.get("echo_only")]
    if echoes:
        print(f"WEAK: {len(echoes)} pass(es) matched only the question's own words:")
        for row in echoes:
            print(f"       {row['question']!r} expected one of {row['expected']}")

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
