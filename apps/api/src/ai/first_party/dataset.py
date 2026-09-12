"""Training pairs built from the product corpus — no external teacher.

A first-party model that learned from ChatGPT paraphrases would quietly
import that model's facts. We therefore synthesize *operator* paraphrases
with deterministic templates: chat filler, slang frames, and the canonical
questions already generated from enforcing modules.

Two pair types come out of this file:

* **align** ``(paraphrase, gold_question)`` — InfoNCE positives for the
  dual encoder. This is how open English reaches a documented heading.
* **copy** ``(question, evidence, answer)`` — teacher-forcing targets for
  the pointer-generator. The answer is always a prefix of the evidence, so
  the decoder is trained to copy, not to invent.
"""

from __future__ import annotations

from dataclasses import dataclass

# Chat wrappers operators actually type. Applied in front of a gold question
# so the dual encoder sees the same subject under different discourse.
_CHAT_PREFIXES: tuple[str, ...] = (
    "hey ",
    "wait so ",
    "can you tell me ",
    "i was wondering ",
    "confused about ",
    "quick question ",
    "pls explain ",
    "honestly ",
    "idk but ",
    "what about this: ",
    "can we ",
    "does this product support ",
    "is there support for ",
)

# Surface substitutions that do not add a new product. Each left-hand side
# is a wording we already accept in the deterministic rewrite.
_SLANG_SWAPS: tuple[tuple[str, str], ...] = (
    ("what is ", "whats "),
    ("do i need ", "gotta have "),
    ("do I need ", "gotta have "),
    ("wal_level logical", "logical wal"),
    ("export yaml", "download yaml"),
    ("can a viewer", "can viewers"),
    ("replication slot", "repl slot"),
    ("binlog_format", "bin log format"),
    ("where do bad rows end up", "where do bad rows go"),
)

# Explicit paraphrase → canonical alignments for the questions operators
# ask about the engine itself. These are the same subjects the generated
# Pilot-engine section answers — they do not invent dbt or SSH.
_CANONICAL_ALIGNS: tuple[tuple[str, str], ...] = (
    ("are you chatgpt", "does pilot use chatgpt or a third-party llm"),
    ("do you use openai", "does pilot use chatgpt or a third-party llm"),
    ("do you use openai by default", "does pilot use chatgpt or a third-party llm"),
    ("do we have our own llm", "does pilot use chatgpt or a third-party llm"),
    ("are you a foundation model", "does pilot use chatgpt or a third-party llm"),
    ("gotta have logical wal", "do i need wal_level logical"),
    ("gotta have logical wal for pg cdc", "do i need wal_level logical"),
    ("where do bad rows go", "where do bad rows end up"),
    ("is _df_lsn how you skip dupes", "is cdc exactly-once or at-least-once"),
    ("can I use dbt with datawrap", "does datawrap run dbt cloud"),
    ("do you support dbt cloud", "does datawrap run dbt cloud"),
    ("can we run our dbt models after the load", "does datawrap run dbt cloud"),
    ("do you open an ssh tunnel to postgres", "does datawrap open ssh tunnels"),
    ("can I connect through a bastion host", "does datawrap open ssh tunnels"),
    ("do you support ssh tunnels", "does datawrap open ssh tunnels"),
    ("do you embed debezium", "does datawrap embed debezium"),
    ("are you a kafka connect replacement", "does datawrap embed debezium"),
    ("do you support flink cdc", "does datawrap embed debezium"),
    ("can I manage pipelines with terraform", "does datawrap have a terraform provider"),
    ("do I have to confirm before a transfer starts", "does a transfer start without confirm"),
    ("can I connect through aws privatelink", "does datawrap use aws privatelink"),
    ("can airflow trigger a transfer", "does datawrap run airflow or spark jobs"),
    ("do you run spark jobs", "does datawrap run airflow or spark jobs"),
    ("do you support oracle goldengate", "does datawrap embed oracle goldengate"),
    ("can I use kafka as a source", "can I use kafka as a source"),
    ("do you have salesforce", "do you have salesforce"),
    ("can I bring iceberg with a glue catalog", "can I use an iceberg glue catalog"),
    ("what about snowflake sharing", "does datawrap use snowflake secure sharing"),
    ("is there a schema registry", "does datawrap include a schema registry"),
    ("do you guarantee no data loss", "do you guarantee no silent data loss"),
    ("zero data loss right", "do you guarantee no silent data loss"),
    ("can you guarantee we never lose data", "do you guarantee no silent data loss"),
    ("what's the difference between upsert and merge", "what is the difference between upsert and merge"),
    ("upsert vs merge", "what is the difference between upsert and merge"),
    ("is upsert the same as merge", "what is the difference between upsert and merge"),
    ("can I use a custom airbyte connector", "does datawrap load airbyte or fivetran connector packs"),
    ("can I load an airbyte connector pack", "does datawrap load airbyte or fivetran connector packs"),
    ("do you load fivetran connector packs", "does datawrap load airbyte or fivetran connector packs"),
    ("can I undo a transfer", "can I undo a transfer"),
    ("can I roll back a load", "can I undo a transfer"),
    ("can a viewer see secrets", "can a viewer see secrets"),
    ("do you have a rest api", "do you have a rest api"),
    ("can I call this from github actions", "can I call datawrap from github actions"),
    ("do you support openlineage", "do you support openlineage"),
    ("what is the difference between mirror and upsert", "what is the difference between mirror and upsert"),
    ("can I connect snowflake with a private key", "can I connect snowflake with a private key"),
    ("do you support soc2", "do you sign a soc2 or hipaa baa"),
    ("can you sign a hipaa baa", "do you sign a soc2 or hipaa baa"),
    ("who can export audit logs", "who can export audit logs as csv"),
    ("can I export audit logs as csv", "who can export audit logs as csv"),
    ("do you support ip allowlists", "do you support ip allowlists"),
    ("can I require mfa", "can I require mfa"),
    ("where is the cdc watermark stored", "where is the watermark stored"),
    ("can I set a watermark", "where is the watermark stored"),
    ("what is the difference between incremental and upsert", "what is the difference between incremental and upsert"),
    ("do you support bigquery as a destination", "do you support bigquery as a destination"),
    ("can I use a service principal for azure", "can I use a service principal for azure"),
    ("can I run transfers in parallel", "can I run transfers in parallel"),
    ("how many transfers can run at once", "can I run transfers in parallel"),
    ("what happens if two jobs write the same table", "what happens if two jobs write the same table"),
    ("do you lock the destination during write", "what happens if two jobs write the same table"),
    ("what is the difference between full refresh and incremental", "what is the difference between full refresh and incremental"),
    ("do you support scd1", "do you support scd1"),
    ("is upsert the same as scd1", "do you support scd1"),
    ("do you support scd type 1", "do you support scd1"),
    ("what is session timeout", "what is session timeout"),
    ("can I set a custom domain", "can I set a custom domain"),
    ("do you support data residency", "do you support data residency"),
    ("do you support sql server", "do you support sql server"),
    ("do you support databricks as a destination", "do you support databricks as a destination"),
    ("can I use databricks unity catalog", "do you support databricks as a destination"),
    ("do you support delta lake", "do you support delta lake"),
)


@dataclass(frozen=True)
class AlignPair:
    """One InfoNCE positive: a paraphrase and the gold question it means."""

    query: str
    gold: str


@dataclass(frozen=True)
class CopyExample:
    """One pointer-generator example: copy the answer out of the evidence."""

    question: str
    evidence: str
    answer: str


def _first_sentences(text: str, limit: int = 2) -> str:
    parts: list[str] = []
    rest = (text or "").strip()
    while rest and len(parts) < limit:
        cut = rest.find(". ")
        if cut < 0:
            parts.append(rest.strip())
            break
        parts.append(rest[: cut + 1].strip())
        rest = rest[cut + 2 :].strip()
    return " ".join(p for p in parts if p)


def _paraphrases(gold: str) -> list[str]:
    out: list[str] = []
    seen = {gold.lower()}
    for prefix in _CHAT_PREFIXES:
        candidate = f"{prefix}{gold}".strip()
        key = candidate.lower()
        if key not in seen:
            seen.add(key)
            out.append(candidate)
    lowered = gold
    for src, dst in _SLANG_SWAPS:
        if src.lower() in lowered.lower():
            swapped = re_sub_ci(gold, src, dst)
            key = swapped.lower()
            if key not in seen:
                seen.add(key)
                out.append(swapped)
    return out


def re_sub_ci(text: str, src: str, dst: str) -> str:
    """Case-insensitive single substitution that keeps the rest of the text."""
    import re

    return re.sub(re.escape(src), dst, text, count=1, flags=re.I)


def _as_question(title: str) -> str:
    text = (title or "").strip()
    if not text:
        return ""
    if text.endswith("?"):
        return text
    if text.lower().startswith(("what ", "how ", "can ", "does ", "do ", "who ", "where ", "is ", "why ")):
        return text
    return f"what is {text[0].lower() + text[1:]}" if text else ""


def _generated_sections():
    from src.ai.rag.product_facts import generated_sections

    return generated_sections()


def gold_questions() -> tuple[str, ...]:
    """Canonical questions the dual encoder may rewrite *to*."""
    seen: list[str] = []
    keys: set[str] = set()
    for _query, gold in _CANONICAL_ALIGNS:
        key = gold.lower()
        if key not in keys:
            keys.add(key)
            seen.append(gold)
    try:
        sections = _generated_sections()
    except Exception:
        sections = ()
    for section in sections:
        question = _as_question(section.section_title)
        key = question.lower()
        if question and key not in keys:
            keys.add(key)
            seen.append(question)
    return tuple(seen)


def align_pairs() -> tuple[AlignPair, ...]:
    """InfoNCE positives, including identity pairs so a gold stays near itself."""
    pairs: list[AlignPair] = []
    seen: set[tuple[str, str]] = set()

    def add(query: str, gold: str) -> None:
        key = (query.strip().lower(), gold.strip().lower())
        if not key[0] or not key[1] or key in seen:
            return
        seen.add(key)
        pairs.append(AlignPair(query=query.strip(), gold=gold.strip()))

    for query, gold in _CANONICAL_ALIGNS:
        add(query, gold)
        add(gold, gold)
        for para in _paraphrases(gold):
            add(para, gold)
    for gold in gold_questions():
        add(gold, gold)
        for para in _paraphrases(gold):
            add(para, gold)
    return tuple(pairs)


def copy_examples() -> tuple[CopyExample, ...]:
    """Teacher-force the first sentences of each generated section."""
    examples: list[CopyExample] = []
    try:
        sections = _generated_sections()
    except Exception:
        sections = ()
    for section in sections:
        evidence = (section.text or "").strip()
        answer = _first_sentences(evidence, limit=2)
        if not evidence or not answer:
            continue
        question = _as_question(section.section_title)
        examples.append(
            CopyExample(question=question, evidence=evidence, answer=answer)
        )
        for para in _paraphrases(question)[:4]:
            examples.append(
                CopyExample(question=para, evidence=evidence, answer=answer)
            )
    return tuple(examples)
