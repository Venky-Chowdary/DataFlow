"""A cardinality question has two possible owners, and the scope decides which.

"How many connectors do you support" is a catalog question: the answer is a
number the product publishes, and before the count ask existed it was answered
with "Open Platform → Connectors" and the four transfer-readiness labels — a
page tour with no number in it.

"How many tables on sales" is a read of the operator's own schema. The answer
is in their warehouse, not in an article about what a table is.

Telling the retrieval layer that "how many" is its own ask type made the first
one answerable and put a documentation essay in front of the second, because
the routing gate treats any recognised ask over a documented subject as worth
consulting the documentation for. What separates them is not the phrasing of
the count but whether it is scoped to something of the operator's.
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

import pytest

from src.ai.copilot.tools import infer_tools_from_message
from src.ai.rag.query_analysis import classify_ask


def _names(question: str) -> list[str]:
    return [name for name, _ in infer_tools_from_message(question)]


#: Counts scoped to one of the operator's own objects. Each is a live read.
SCOPED_COUNTS = [
    "how many tables on sales",
    "how many rows in orders",
    "how many columns in customers",
    "how many jobs on the sqlite connector",
    "how many datasets in the warehouse",
]

#: Counts of what the product itself offers. Each is answered from documentation.
CATALOG_COUNTS = [
    "how many connectors do you support",
    "how many connectors are live",
    "how many engines are transfer ready",
]


@pytest.mark.parametrize("question", SCOPED_COUNTS + CATALOG_COUNTS)
def test_both_kinds_are_recognised_as_a_count(question: str) -> None:
    """The ask type is the same for both; it is not what tells them apart."""
    assert classify_ask(question) == "count", question


@pytest.mark.parametrize("question", SCOPED_COUNTS)
def test_a_count_over_the_operators_own_objects_stays_a_live_read(
    question: str,
) -> None:
    """No documentation essay in front of the inventory they asked for."""
    assert "explain_product" not in _names(question), question


@pytest.mark.parametrize("question", CATALOG_COUNTS)
def test_a_count_of_what_the_product_offers_consults_the_documentation(
    question: str,
) -> None:
    """The number is published, so the question is answerable without a live read."""
    assert "explain_product" in _names(question), question
