"""One refusal for a question this product has no evidence for.

Two paths could answer an off-subject question — the curated FAQ and the
knowledge index — and each had its own idea of what to say, so "how do I cook
rice" was refused by one and answered with product prose by the other. The
refusal lives here so both read from the same owner.
"""

from __future__ import annotations

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


def is_answerable_subject(query: str) -> bool:
    """Whether a knowledge question is about this product or the operator's objects.

    Embedding search returns its nearest neighbours for any input, so without this
    gate an off-subject question came back with three product fragments narrated as
    an answer. A pasted ID still passes: that is the operator's own data.
    """
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
