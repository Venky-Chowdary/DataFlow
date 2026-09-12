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
