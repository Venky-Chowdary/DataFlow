"""One refusal for a question this product has no evidence for.

Two paths could answer an off-subject question — the curated FAQ and the
knowledge index — and each had its own idea of what to say, so "how do I cook
rice" was refused by one and answered with product prose by the other. The
refusal lives here so both read from the same owner.
"""

from __future__ import annotations

import re
from typing import Any

from ..rag.evidence import names_identifier
from ..rag.product_docs import names_product_subject, nearest_articles

REFUSAL_LEAD = (
    "That is outside what the Datawrap documentation covers, so I will not "
    "answer it from guesswork."
)

_ASK_INSTEAD = (
    "Ask me about connectors, mapping, preflight gates, sync modes, quarantine, "
    "reconcile proof, schedules, MCP or the API — or ask me to read a live table."
)

# Only a commercial arrangement is ever called one of these, so they are read
# anywhere in the sentence. Deliberately none of them is a plausible table or
# column name: ``price``, ``cost``, ``invoice`` and ``discount`` are all columns
# somebody has, and matching them bare refused "count of orders on Local Postgres
# where price > 100" as a pricing question.
_COMMERCIAL_NOUN = re.compile(
    r"\bpricing\b|\bprice\s+(?:list|plan|point|sheet)\b|\brate\s+card\b"
    r"|\b(?:free|paid|enterprise|starter|pro|premium)\s+(?:tier|plan)\b"
    r"|\bfree\s+trial\b|\bsubscription\b|\brenewals?\b|\broyalt(?:y|ies)\b"
    r"|\bper[\s-]seat\b|\bper[\s-]user\s+(?:cost|price|fee|pricing)\b"
    r"|\blicen[cs](?:e|es|ing)\s+(?:fee|fees|cost|costs|terms|model)\b"
    r"|\buptime\b|\bsla\b|\bavailability\s+guarantee\b"
    r"|\bsupport\s+(?:hours|contract|plan)\b|\bresponse[\s-]time\s+guarantee\b"
    r"|\bcosts?\s+per\s+(?:row|gb|tb|record|month|year|seat|user|transfer|sync|job)\b"
    r"|\bpaid\s+(?:add[\s-]?on|feature)\b",
    re.I,
)

# The commercial words that double as ordinary data words count only inside a
# frame that asks what *this product* charges or guarantees.
_COMMERCIAL_FRAME = re.compile(
    r"\bhow\s+much\s+(?:does|do|will|would|is|are|did)\b[^.?!]{0,48}?"
    r"\b(?:cost|costs|charge|charges|pay|paying)\b"
    r"|\b(?:do|does|will|can|would)\s+(?:you|we|i|datawrap|dataflow|it)\s+"
    r"(?:charge|bill|invoice)\b"
    r"|\b(?:you|we)\s+(?:charge|bill)\s+(?:me|us|per|for|by)\b"
    r"|\bwhat\s+(?:do|does)\s+"
    r"(?:it|this|datawrap|dataflow|a\s+transfer|a\s+sync)\s+cost\b"
    r"|\bwhat(?:'?s|\s+is|\s+are)\s+(?:your|datawrap'?s|dataflow'?s)\s+"
    r"(?:cost|costs|price|prices|fee|fees|licen[cs]e|licensing)\b"
    r"|\b(?:offer|give|get|have)\s+(?:me\s+|us\s+|a\s+)?discount\b"
    r"|\bhow\s+many\s+seats?\s+(?:do|does|are|come)\b"
    r"|\b(?:get|issue|claim)\s+a\s+refund\b"
    r"|\bwho\s+do\s+(?:i|we)\s+pay\b"
    r"|\bwhat\s+licen[cs]e\b|\bopen[\s-]source\s+licen[cs]e\b",
    re.I,
)

# The same nouns inside a question about the operator's own rows. An aggregate
# verb, or the word for a database object, means the sentence is a live read and
# nothing in it is a question about what this product charges. "does a full
# refresh cost more queries" and "reconcile is free of false positives" are
# engineering sentences too, and refusing either would be wrong.
_READS_THE_OPERATORS_DATA = re.compile(
    r"\b(?:average|avg|mean|median|sum|summed|total|totals|min|minimum|max|"
    r"maximum|count|counts|distinct|group\s+by|order\s+by|select|where|"
    r"having|join)\b"
    r"|\b(?:columns?|fields?|tables?|rows?|collections?|datasets?|schemas?|"
    r"records?|quer(?:y|ies))\b"
    r"|\bfree\s+(?:of|from)\b",
    re.I,
)


def asks_a_commercial_question(query: str) -> bool:
    """Whether the question asks what this product costs, guarantees, or licences.

    Price, billing, SLA and licence terms are not in the engineering corpus, and
    the corpus is the only thing Pilot is allowed to answer from. Left to the
    retrieval term filter the refusal held only when the sentence named nothing
    else: "how much does datawrap cost" was refused, but "how much does a
    transfer cost" anchored on ``transfer`` and answered with the Transfer Studio
    walkthrough, and "how much do you charge for snowflake" answered with
    Snowflake key-pair auth. The class of question is what is undocumented, so
    the class is what is refused.
    """
    text = (query or "").strip()
    if not text:
        return False
    # The unambiguous nouns stand on their own: "what does a transfer cost per
    # row" names a billing unit, and the word ``row`` in it is not a table read.
    if _COMMERCIAL_NOUN.search(text):
        return True
    if _READS_THE_OPERATORS_DATA.search(text):
        return False
    return bool(_COMMERCIAL_FRAME.search(text))


def commercial_question_output(query: str) -> dict[str, Any]:
    """Refusal that names *what* is not published, instead of a blanket no.

    "Outside what the documentation covers" is true of a pricing ask but reads as
    evasion, because the operator can see that the product plainly has a price.
    Saying which class of fact is absent, and where it does live, is the honest
    version of the same refusal.
    """
    from ..knowledge.copilot_knowledge import PRODUCT_CAPABILITIES

    out = {
        "query": query,
        "intent": "commercial_unsupported",
        "answer": (
            "Pricing, billing, licence and SLA terms are not in the product "
            "documentation I answer from, so I have no number to give you and I "
            "will not invent one.\n\n"
            "Your account team or the commercial contract is the source for "
            "those. I can answer anything about how the engine behaves — "
            "connectors, mapping, preflight gates, sync modes, quarantine, "
            "reconcile proof and schedules — and I can read your live data."
        ),
        "capabilities": PRODUCT_CAPABILITIES[:6],
        "actions": [{"label": "Open Docs", "route": "docs"}],
        "hits": [],
        "count": 0,
        "empty": True,
        "sources": [],
        "grounded": False,
        "source": "commercial_unsupported",
    }
    return out


def is_answerable_subject(query: str) -> bool:
    """Whether a knowledge question is about this product or the operator's objects.

    Embedding search returns its nearest neighbours for any input, so without this
    gate an off-subject question came back with three product fragments narrated as
    an answer. A pasted ID still passes: that is the operator's own data.
    """
    if asks_a_commercial_question(query):
        return False
    return names_product_subject(query) or names_identifier(query)


def unsupported_question_output(query: str) -> dict[str, Any]:
    """Refusal payload: no sources, not grounded, and the closest guides as a lead."""
    from ..knowledge.copilot_knowledge import PRODUCT_CAPABILITIES

    lines = [REFUSAL_LEAD]
    leads = nearest_articles(query)
    if leads:
        lines.append("Closest guides: " + ", ".join(leads) + ".")
    lines.append(_ASK_INSTEAD)
    return {
        "query": query,
        "intent": "unsupported",
        "answer": "\n".join(lines),
        "capabilities": PRODUCT_CAPABILITIES[:6],
        # The Docs screen is a real authenticated route; the marketing Help page is not
        # one the workspace router can open.
        "actions": [{"label": "Open Docs", "route": "docs"}],
        "hits": [],
        "count": 0,
        "empty": True,
        "sources": [],
        "grounded": False,
        "source": "unsupported_question",
    }
