"""Grounded (question, context, answer) sequences for the first-party LM.

Same operator question, different packed facts, different answer. That is
how the decoder learns to *read* TODAY_UTC / PIPELINES / CONFIRM instead
of memorizing one hardcoded line. Answers never name a warehouse fact
that is not on the context line.
"""

from __future__ import annotations

from dataclasses import dataclass

from .context_pack import pack_context
from .dataset import _CHAT_PREFIXES, copy_examples


@dataclass(frozen=True)
class LmExample:
    question: str
    context: str
    answer: str


_DATE_BANK: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "Sunday, 13 September 2026",
        ("date today", "what is todays date", "tell me the date"),
    ),
    (
        "Monday, 01 January 2026",
        ("what is todays date", "what's today's date"),
    ),
    (
        "Friday, 04 July 2026",
        ("what's today's date", "what day is it"),
    ),
    (
        "Wednesday, 11 November 2026",
        ("what day is it", "date today"),
    ),
    (
        "Tuesday, 24 March 2026",
        ("tell me the date", "whats the date"),
    ),
    (
        "Thursday, 16 April 2026",
        ("date today", "whats the date"),
    ),
)

_PIPELINE_BANK: tuple[tuple[int, int, int, tuple[str, ...], tuple[str, ...]], ...] = (
    (2, 2, 0, ("Nightly", "TestJob"), ("is schedules working", "why schedules are not working")),
    (3, 1, 2, ("Crown",), ("are my pipelines working", "is schedules working")),
    (0, 0, 0, (), ("is schedules working", "are pipelines running")),
    (1, 0, 1, (), ("are pipelines running", "are my pipelines working")),
    (5, 3, 2, ("Nightly", "Retail", "QA"), ("why schedules are not working",)),
    (4, 0, 4, (), ("is schedules working",)),
)

_GREET_BANK: tuple[tuple[int, int, int, tuple[str, ...]], ...] = (
    (12, 8, 2, ("hi", "what can you do")),
    (0, 0, 0, ("hello", "hi")),
    (4, 3, 1, ("hey", "what can you do")),
    (7, 2, 0, ("hello",)),
)

_CREATE_BANK: tuple[tuple[int, tuple[str, ...]], ...] = (
    (12, ("can you create connection", "can you add a connection")),
    (0, ("can you create a connector", "can you create connection")),
    (5, ("can you add a connection",)),
    (1, ("can you create a connector",)),
)

_ROUTE_ASKS: tuple[str, ...] = (
    "plan source destination routes and sync modes",
    "plan a transfer",
)


def _dialogue_examples() -> list[LmExample]:
    out: list[LmExample] = []
    for spoken, questions in _DATE_BANK:
        ctx = pack_context(today_utc=spoken)
        answer = f"Today is {spoken} (UTC)."
        for question in questions:
            out.append(LmExample(question, ctx, answer))
    for n, parked, enabled, names, questions in _PIPELINE_BANK:
        ctx = pack_context(
            pipeline_count=n,
            parked_count=parked,
            parked_names=list(names),
            enabled_count=enabled,
        )
        if n == 0:
            answer = (
                "No pipelines are scheduled yet, so nothing is running on a cadence."
            )
        elif parked:
            who = ", ".join(names) if names else "those routes"
            answer = (
                f"You have {n} pipelines. {parked} are parked on approval "
                f"({who})."
            )
        elif enabled:
            answer = f"You have {n} pipelines. {enabled} enabled and due to run."
        else:
            answer = f"You have {n} pipelines. None are enabled, so they will not fire."
        for question in questions:
            out.append(LmExample(question, ctx, answer))
    for n_conn, n_jobs, failed, questions in _GREET_BANK:
        ctx = pack_context(
            connector_count=n_conn,
            job_count=n_jobs,
            failed_jobs=failed,
        )
        if n_conn == 0 and n_jobs == 0:
            answer = (
                "I am Datawrap Pilot. I read your live workspace and compose "
                "from that evidence."
            )
        else:
            fail = (
                f" {failed} of those jobs failed and need a look."
                if failed
                else ""
            )
            answer = (
                f"I am Datawrap Pilot. I can see {n_conn} saved connectors "
                f"and {n_jobs} recent jobs.{fail}"
            )
        for question in questions:
            out.append(LmExample(question, ctx, answer))
    for n_conn, questions in _CREATE_BANK:
        ctx = pack_context(
            connector_count=n_conn,
            create_connection="confirm_gated",
        )
        if n_conn:
            answer = (
                f"Yes. I will stage Confirm. You have {n_conn} saved connectors."
            )
        else:
            answer = (
                "Yes. I will stage Confirm. Name the engine if you want me "
                "to walk the fields."
            )
        for question in questions:
            out.append(LmExample(question, ctx, answer))
    route_ctx = pack_context(create_connection="confirm_gated")
    route_ans = (
        "Name a saved source and destination and I will propose a sync mode, "
        "then stage Confirm."
    )
    for question in _ROUTE_ASKS:
        out.append(LmExample(question, route_ctx, route_ans))
    return out


def _product_examples(*, limit: int = 80) -> list[LmExample]:
    out: list[LmExample] = []
    for ex in copy_examples()[:limit]:
        ctx = pack_context(evidence=ex.evidence)
        out.append(LmExample(ex.question, ctx, ex.answer))
    return out


def lm_examples() -> tuple[LmExample, ...]:
    rows = _dialogue_examples() + _product_examples()
    extra: list[LmExample] = []
    for row in rows[:60]:
        for prefix in _CHAT_PREFIXES[:3]:
            extra.append(LmExample(prefix + row.question, row.context, row.answer))
    return tuple(rows + extra)
