"""Grounded (question, context, answer) sequences for the first-party LM.

Answers are spoken from the packed context. We never teacher-force a
warehouse fact that is not on the context line. Dialogue templates use
filled numbers and names so the decoder learns to read TODAY_UTC /
PIPELINES / EVIDENCE — the same keys serve writes.
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


_DATE_ROWS: tuple[tuple[str, str], ...] = (
    ("Sunday, 13 September 2026", "date today"),
    ("Monday, 01 January 2026", "what is todays date"),
    ("Friday, 04 July 2026", "what's today's date"),
    ("Wednesday, 11 November 2026", "what day is it"),
    ("Tuesday, 24 March 2026", "tell me the date"),
)

_PIPELINE_ROWS: tuple[tuple[int, int, int, tuple[str, ...], str], ...] = (
    (2, 2, 0, ("Nightly", "TestJob"), "is schedules working"),
    (2, 2, 0, ("Nightly", "TestJob"), "why schedules are not working"),
    (3, 1, 2, ("Crown",), "are my pipelines working"),
    (0, 0, 0, (), "is schedules working"),
    (1, 0, 1, (), "are pipelines running"),
)

_GREET_ROWS: tuple[tuple[int, int, int, str], ...] = (
    (12, 8, 2, "hi"),
    (0, 0, 0, "hello"),
    (4, 3, 1, "hey"),
    (12, 8, 2, "what can you do"),
)

_CREATE_ROWS: tuple[tuple[int, str], ...] = (
    (12, "can you create connection"),
    (0, "can you create a connector"),
    (5, "can you add a connection"),
)

_ROUTE_ROWS: tuple[str, ...] = (
    "plan source destination routes and sync modes",
    "plan a transfer",
)


def _dialogue_examples() -> list[LmExample]:
    out: list[LmExample] = []
    for spoken, question in _DATE_ROWS:
        ctx = pack_context(today_utc=spoken)
        answer = (
            f"Today is {spoken} (UTC). I keep your Datawrap workspace, "
            "not a personal calendar."
        )
        out.append(LmExample(question, ctx, answer))
        out.append(LmExample(f"hey {question}", ctx, answer))
    for n, parked, enabled, names, question in _PIPELINE_ROWS:
        ctx = pack_context(
            pipeline_count=n,
            parked_count=parked,
            parked_names=list(names),
            enabled_count=enabled,
        )
        if n == 0:
            answer = (
                "No pipelines are scheduled yet, so nothing is running on a "
                "cadence. Create one on Pipelines after a transfer."
            )
        elif parked:
            who = ", ".join(names) if names else "those routes"
            answer = (
                f"You have {n} pipelines. {parked} are parked on approval "
                f"({who}) — they will not tick until someone approves."
            )
        elif enabled:
            answer = f"You have {n} pipelines. {enabled} enabled and due to run."
        else:
            answer = f"You have {n} pipelines. None are enabled, so they will not fire."
        out.append(LmExample(question, ctx, answer))
    for n_conn, n_jobs, failed, question in _GREET_ROWS:
        ctx = pack_context(
            connector_count=n_conn,
            job_count=n_jobs,
            failed_jobs=failed,
            create_connection="confirm_gated",
        )
        if n_conn == 0 and n_jobs == 0:
            answer = (
                "I'm Datawrap Pilot. I am not a general chatbot. I read your "
                "live workspace and compose from that evidence."
            )
        else:
            fail = (
                f" {failed} of those jobs failed and need a look."
                if failed
                else ""
            )
            answer = (
                f"I'm Datawrap Pilot. I can see {n_conn} saved connectors "
                f"and {n_jobs} recent jobs.{fail}"
            )
        out.append(LmExample(question, ctx, answer))
    for n_conn, question in _CREATE_ROWS:
        ctx = pack_context(
            connector_count=n_conn,
            create_connection="confirm_gated",
        )
        if n_conn:
            answer = (
                f"Yes. Paste a host or connection URL and I will stage Confirm. "
                f"You already have {n_conn} saved connectors."
            )
        else:
            answer = (
                "Yes. Paste a host or connection URL and I will stage Confirm. "
                "Name the engine if you want me to walk the fields."
            )
        out.append(LmExample(question, ctx, answer))
    route_ctx = pack_context(create_connection="confirm_gated")
    route_ans = (
        "Name a saved source and destination and I will propose a sync mode, "
        "then stage Confirm. Nothing writes until you accept."
    )
    for question in _ROUTE_ROWS:
        out.append(LmExample(question, route_ctx, route_ans))
    return out


def _product_examples(*, limit: int = 180) -> list[LmExample]:
    out: list[LmExample] = []
    for ex in copy_examples()[:limit]:
        ctx = pack_context(evidence=ex.evidence)
        out.append(LmExample(ex.question, ctx, ex.answer))
    return out


def lm_examples() -> tuple[LmExample, ...]:
    rows = _dialogue_examples() + _product_examples()
    extra: list[LmExample] = []
    for row in rows[:80]:
        for prefix in _CHAT_PREFIXES[:4]:
            extra.append(
                LmExample(prefix + row.question, row.context, row.answer)
            )
    return tuple(rows + extra)
