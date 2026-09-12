"""Composing the answer — selecting sentences, not pasting sections.

The previous composer took the first three sentences of the top two sections and
concatenated them. That is not an answer to a question; it is the top of two
articles. Asked "which sync mode should I pick for a nightly load" it opened with
whatever sentence the sync-mode section happens to begin with, which may be about
something else entirely, and it could not use a third section even when that was
where the answer lived.

Answer composition here is extractive selection over the whole retrieved set:

1. **Split** every retrieved section into sentences, keeping the gate/step lines
   the help corpus writes as bare lines rather than dropping them.
2. **Score** each sentence on the question's terms — the operator's own words
   weigh more than the expansion vocabulary — plus a bonus for the shape of
   answer the question asked for. A "what is" question wants the sentence that
   defines the term; a "how do I" question wants the imperative step.
3. **Select** with maximal-marginal-relevance so three sections that all restate
   the same definition contribute it once, and the remaining slots go to
   sentences that add something.
4. **Cite** every section an included sentence came from, so each claim is
   traceable to a page the operator can open.

Extraction is deliberate: the sentences are the shipped documentation's own, so
the answer cannot assert anything the documentation does not. When a cloud model
is configured it may rewrite this draft, but the rewrite is checked against the
same evidence and discarded if it drifts (see ``generator._narrate``).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace

from .lexical_index import adjacent_shingles, content_terms
from .query_analysis import QueryAnalysis

# How much an expansion term counts relative to a word the operator typed.
EXPANSION_WEIGHT = 0.45

# What it is worth for a sentence to contain a whole phrase of the question,
# rather than its words scattered. Sentence scoring was unigram-only, so
# "column order" was two independent words: asked whether column order is
# preserved, the answer opened with a tutorial line reading "5 columns:
# order_id, customer_email, order_amt" — both words, neither meaning.
#
# Deliberately added *outside* length normalization. That discount exists
# because a long sentence accumulates incidental single-word matches; an exact
# phrase is not incidental, and the sentence that has to enumerate all 26
# certificate aspects is long for a reason.
PHRASE_CREDIT = 2.0

# Redundancy penalty for maximal-marginal-relevance selection. 0.7 keeps the
# answer from restating one definition three times while still allowing a second
# sentence that elaborates it.
MMR_LAMBDA = 0.7

# What a supporting sentence has to be worth, as a share of the sentence that
# answers, before it is allowed into the answer. Relevance alone never ended
# selection: the loop ran until the sentence budget did, so an answer that was
# complete in one sentence was padded to six with whatever else scored above
# zero. "Do you preserve column order" was answered correctly and then handed
# three sentences of a CSV tutorial.
#
# Not a tuned knob — it sits in a measured gap. Across the corpus the weakest
# sentence that genuinely elaborates its lead scores 0.42 of it (the mapping
# threshold advice under "why is my mapping confidence low"), while the
# strongest off-subject sentence scores 0.31. Vouched list items are exempt:
# they are carried by the heading the question matched, not by their own
# wording, and such a list is admitted or refused whole. See ``Candidate``.
RELEVANCE_FLOOR = 0.35

#: How close to the top a subject-naming sentence has to be before it is
#: preferred as the lead. See ``_lead``.
#:
#: Naming the question's subject proves a sentence is about the right thing; it
#: does not prove it is the answer. Without a floor the rarest word the question
#: typed decided the lead wherever in the retrieved text it happened to appear,
#: however far down it scored: "how do you handle booleans across databases"
#: opened on the sidebar caption "Overview — full Platform / Operations / System
#: sidebar used across every guide", and "how does semantic column mapping
#: decide a type" on a sentence about the CDC snapshot handoff.
#:
#: Measured on the audit fixture this is worth two cases net — five leads fixed
#: against three changed. Those three were checked one by one and none of them
#: reads better without the floor: "how do I pause a schedule" opens on "Click
#: the saved pipeline card/row" with it and on a role-permission list without
#: it. They are counted against the floor only because the phrase the fixture
#: greps for happens to sit in the sentence the subject rule hoisted.
LEAD_SUBJECT_FLOOR = 0.7

#: How close to the rarest typed word another typed word has to be before it
#: also counts as naming the subject. See ``subject_words``.
#:
#: The rarest word alone made an arbitrary tiebreak decide the lead. Swept over
#: the 158-case answer audit, leads-with-the-answer by band: 1.0 (the rarest
#: word alone) 119, 0.95 and 0.9 and 0.85 120, 0.8 and 0.75 and 0.7 121, 0.6
#: 120. The middle of the plateau, and the turn back down at 0.6 is the reason
#: for a band rather than none: widened far enough, every ordinary word of the
#: question speaks for the subject and the test stops discriminating.
SUBJECT_ANCHOR_BAND = 0.75

MAX_SENTENCES = 6
MAX_CITED_SECTIONS = 3

# Content terms in a typical help sentence. Sentences longer than this have
# their term matches discounted proportionally, the way BM25 discounts a long
# document: a raw match count lets a 400-character sentence that lists every
# permission in the product outrank the one sentence that answers the question,
# purely by covering more ground. Shorter sentences are never *rewarded* —
# normalization is one-sided so a three-word fragment cannot win on density.
LENGTH_PIVOT = 18

# What one item of a list is worth when the question matched the heading the
# list sits under, scaled by how much of the question that heading carries.
LIST_ITEM_CREDIT = 2.6

# What a sentence is worth on its heading alone, when the heading restates the
# whole question. Below the credit one typed word earns, so a sentence that
# answers in the operator's own words always outranks one vouched for by the
# title above it.
HEADING_CREDIT = 0.9

# The heading has to carry *every* anchor term of the question before it can
# speak for a sentence that carries none. A partial overlap is how "which
# engines can I connect to" reached "Procedure: connect Cursor" — one word of
# two, and a section about MCP answered a question about database engines.
HEADING_ANCHOR = 1.0

# …except in the passage retrieval scored highest, where a majority is enough.
# "How is this different from writing ETL scripts" is answered by the FAQ
# section of that name, whose body sentence lists the differences without
# repeating a single word of the question; its heading carries two terms of
# three. Under the flat bar that passage contributed nothing at all and the
# answer was composed from a timezone passage that scored a quarter as well,
# purely because one of its sentences contained the word "writing".
HEADING_ANCHOR_TOP = 0.6

# A list answers a question as a whole or not at all, so once one item is
# selected its siblings come with it. Nine is the number of preflight gates —
# the longest enumeration the shipped documentation contains.
MAX_LIST_ITEMS = 9

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z(`*\d])")
_GATE_LINE = re.compile(r"^G\d+\b")
_LIST_LINE = re.compile(r"^(?:\d+[.)]\s|[-*•]\s|\*\*[A-Z][^*]{0,40}\*\*\s*(?:—|-|:))")
_SKIP_PREFIX = ("Where:", "Tip:", "Note:", "Example:")
_UI_CAPTION = re.compile(
    r"^(Validate|Map|Job Theater|System|Operations|Transfer|Pilot|Connectors|"
    r"Schedules|Proofs|Evidence) — ",
)
_CODE_LINE = re.compile(r"^\s*(?:\{|\[|GET |POST |PUT |PATCH |DELETE |curl |\$ )")

# Sentences about the documentation rather than the product: "Every screenshot
# is from the live application — not a marketing mock", "Markers on each
# screenshot match the live product UI". True, and never an answer. Asked how
# to create a recurring sync, the pilot led with the caption "Screenshots are
# from the live Create recurring sync form" — it contains the phrase the
# operator typed and nothing they can act on.
_DOC_META = re.compile(r"\b(?:screenshots?|markers on)\b", re.I)

# A code span is an example, not what the sentence is about. The corpus writes
# fixture and column names as literals — `sample-orders.csv`, `order_id`,
# `public.orders` — so asked "do you preserve column order" three sentences of
# the CSV tutorial were appended to a correct answer, having matched `order`
# inside the name of the demo file. The word is present; the sentence is not
# about it. Prose matches are untouched, so a sentence that names the subject
# and then shows it as a literal keeps its full credit.
_CODE_SPAN = re.compile(r"`[^`]*`")
_IMPERATIVE = re.compile(
    r"^(?:Open|Click|Pick|Choose|Select|Set|Enter|Type|Paste|Copy|Add|Create|"
    r"Review|Return|Expand|Fix|Remediate|Use|Run|Press|Confirm|Approve|Sign|"
    r"Go|Navigate|Upload|Download|Export|Import|Enable|Disable|Start|Stop)\b"
)
_DEFINITIONAL = re.compile(
    r"^\s*(?:\*\*)?[A-Z][\w \-/()`*]{0,60}(?:\*\*)?\s+"
    r"(?:is|are|means|refers to|describes|holds|records|names)\b"
)

# Section titles whose sentences answer a given ask better than the corpus
# average. Derived from how the help corpus is written: definitions live in FAQ
# and "What is" cards, steps live in "Procedure:" sections.
_ASK_SECTION_BONUS: dict[str, tuple[tuple[str, float], ...]] = {
    "definition": (("what is", 2.4), ("what are", 2.4), ("core gate", 1.8), ("procedure:", -1.6)),
    "procedure": (("procedure:", 2.4), ("what is", -0.8), ("what are", -0.8)),
    "enumeration": (("mode", 1.6), ("core gate", 1.6), ("role", 1.2), ("procedure:", -1.0)),
    "diagnosis": (("phas", 1.2), ("checksum", 1.2), ("quarantine", 1.2), ("drift", 1.2)),
    "comparison": (("mode", 1.8), ("what is", 0.8)),
    # "Do you preserve column order" asks what the product guarantees, and a
    # wizard step cannot answer that. Unpenalized, the sample-transfer tutorial
    # answered it with "wait until the file chip shows Sample-Orders.csv" —
    # a sentence that matches only because the demo fixture is named orders.
    "capability": (("what is", 1.0), ("support", 1.4), ("procedure:", -1.2)),
    # A count is published in the catalog and engine passages, never in a
    # wizard step. The shape test below withholds the positive credit from any
    # sentence that states no number, so the heading only breaks ties among
    # sentences that could answer.
    "count": (("engin", 1.4), ("connect", 1.2), ("gate", 1.0), ("procedure:", -1.6)),
}

#: A sentence that answers "how many" says a number. The corpus writes both
#: digits ("717 tiles", "46 of them") and small cardinals in words ("nine
#: preflight gates", "five modes"), so both count.
# What counts as answering "how many". A digit always does. A spelled-out
# number only does when it is quantifying something — "there are two ways",
# "nine gates" — because the small ones are far more often adverbial: "in one
# transaction", "as one atomic unit", "one measure per distinct value". Asked
# "how many destinations do you support", the count shape bonus went to "commit
# the applied rows and the watermark that records them in one transaction",
# which led the answer while the passage stating "30 sources and 30
# destinations" sat below it. Requiring a plural after the word is what
# separates a quantity from a manner; ``one`` is dropped entirely, since a
# singular is not an answer to a plural question.
_CARDINAL = re.compile(
    r"\b\d[\d,]*\b"
    r"|\b(?:two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|dozen|"
    r"hundred|thousand|million)\s+(?:\w+\s+){0,2}\w{3,}s\b",
    re.I,
)


@dataclass(frozen=True)
class Candidate:
    """One sentence of evidence, with where it came from."""

    text: str
    section_title: str
    citation: str
    href: str
    order: int
    terms: frozenset[str]
    score: float = 0.0
    #: True when the sentence is one item of an enumerated list in its section
    #: (``G1 Source readable — …``, ``1. Open Connectors``). List items are
    #: selected as a group, because half a list is not an answer.
    list_item: bool = False
    #: True when this list is what the question asked for, and may therefore
    #: speak as a unit: the question asked to be enumerated, or the heading
    #: above the list carries enough of the question to vouch for items that
    #: repeat none of it.
    #:
    #: Being a list was once enough on its own, which let any list in any
    #: retrieved passage past ``RELEVANCE_FLOOR`` and pull in its siblings.
    #: Asked "can I limit who sees a connector" the highest-scoring sentence in
    #: the corpus was "**Beta** — works with known limits", a maturity label
    #: matching on the wrong sense of ``limit``, and it brought Live, Planned
    #: and Connect-only with it: four sentences about transfer-readiness
    #: appended to a correct answer about roles.
    list_vouched: bool = False
    #: True when this sentence uses one of the words the question is *about* —
    #: its rarest typed word, or a phrase expansion restating it in the
    #: corpus's own spelling. See ``subject_words``.
    names_subject: bool = False


def _split_annotated(text: str, section_title: str = "") -> list[tuple[str, bool]]:
    """Sentences of one help section, each flagged as a list item or not."""
    body = (text or "").strip()
    title = (section_title or "").strip()
    if title and (body == title or body.startswith((f"{title}\n", f"{title} "))):
        body = body[len(title) :].strip()

    out: list[tuple[str, bool]] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith(_SKIP_PREFIX) or _CODE_LINE.match(line):
            continue
        if _UI_CAPTION.match(line):
            continue
        if _GATE_LINE.match(line):
            out.append((line if line.endswith((".", "!", "?")) else f"{line}.", True))
            continue
        listed = bool(_LIST_LINE.match(line))
        if not any(ch in line for ch in ".!?"):
            # A bare line is only content when it reads as a statement; headings
            # and captions are not.
            if ":" not in line or "—" in line:
                continue
        for piece in _SENTENCE_SPLIT.split(line):
            piece = piece.strip()
            if not piece or piece.startswith(_SKIP_PREFIX) or piece.endswith(":"):
                continue
            if _DOC_META.search(piece):
                continue
            if len(piece) < 12:
                continue
            out.append(
                (piece if piece.endswith((".", "!", "?")) else f"{piece}.", listed)
            )
            listed = False
    return out


def split_sentences(text: str, section_title: str = "") -> list[str]:
    """Readable sentences from one help section, keeping the bare gate lines.

    The corpus writes G1–G9 as their own lines with no terminator, and those
    lines are the answer to every gate question, so a naive sentence splitter
    throws away the most useful content in the file.
    """
    return [sentence for sentence, _ in _split_annotated(text, section_title)]


# The direct test for the shape an ask wants, where one exists. A section
# heading only says what sentences there *tend* to be.
_ASK_SHAPE_TEST: dict[str, re.Pattern[str]] = {
    "definition": _DEFINITIONAL,
    "procedure": _IMPERATIVE,
    "count": _CARDINAL,
}


def _section_bonus(ask: str, section_title: str, sentence: str) -> float:
    """Credit a section gives its sentences, withheld when the sentence disagrees.

    Both this and ``_shape_bonus`` estimate "is this the kind of sentence the
    question asked for", and when they disagree the sentence's own shape is
    evidence while the heading is only a container. Asked "what is quarantine",
    the FAQ card titled "What is quarantine?" handed its full definition credit
    to "Open Operations → Jobs → Quarantine on the run", and the answer opened
    with a navigation instruction instead of the definition one section over.

    Negative weights stay unconditional: they say the container is wrong for
    this ask, which the sentence cannot argue with.
    """
    title = (section_title or "").strip().lower()
    shape = _ASK_SHAPE_TEST.get(ask)
    # ``search``, not ``match``: each pattern says for itself where it has to
    # appear. A definition and an imperative are anchored at the start of the
    # sentence; the number that answers "how many" is wherever the sentence
    # puts it.
    fits = shape.search(sentence) is not None if shape else True
    return sum(
        weight
        for needle, weight in _ASK_SECTION_BONUS.get(ask, ())
        if needle in title and (weight < 0 or fits)
    )


def _heading_match(heading: str, typed: set[str]) -> float:
    """Share of the question's own terms the section's heading path carries."""
    if not typed:
        return 0.0
    words = set(content_terms(heading or ""))
    if not words:
        return 0.0
    return len(words & typed) / len(typed)


def _length_norm(term_count: int) -> float:
    """One-sided pivoted length normalization for a sentence's match count."""
    if term_count <= LENGTH_PIVOT:
        return 1.0
    return LENGTH_PIVOT / term_count


def _shape_bonus(ask: str, sentence: str) -> float:
    if ask == "procedure" and _IMPERATIVE.match(sentence):
        return 1.4
    if ask == "definition" and _DEFINITIONAL.match(sentence):
        return 2.0
    if ask == "enumeration" and ("·" in sentence or sentence.count(",") >= 2):
        return 0.9
    if ask == "count" and _CARDINAL.search(sentence):
        return 2.0
    return 0.0


def term_weights(
    terms: Sequence[str],
    idf: Callable[[str], float] | None,
) -> dict[str, float]:
    """How much each of the question's words is worth, relative to its average.

    Counting matched words equally is how "what about generated columns" tied
    three sentences at exactly 1.80: one contained ``generated``, which appears
    in a single passage of the corpus, and the others contained ``column``,
    which appears in dozens. Ranking already knows the difference — it is the
    IDF the BM25 index computed — and sentence selection was throwing it away.

    Normalized by the mean so the weights average 1.0, which keeps every other
    constant in this module on the scale it was tuned against.
    """
    if not idf:
        return {}
    raw = {t: max(0.0, float(idf(t))) for t in terms}
    positive = [v for v in raw.values() if v > 0]
    if not positive:
        return {}
    mean = sum(positive) / len(positive)
    if mean <= 0:
        return {}
    # A term the corpus never uses scores 0 IDF, which would make it free to
    # miss. It is the most distinctive word in the question, so it keeps the
    # average weight rather than none.
    return {t: (v / mean if v > 0 else 1.0) for t, v in raw.items()}


def subject_words(
    analysis: QueryAnalysis,
    available: frozenset[str],
    idf: Callable[[str], float] | None,
) -> frozenset[str]:
    """The words a sentence has to use to be *about* this question.

    Sentence scores are a sum, so a sentence can win by matching several
    ordinary words of the question while never mentioning the one word that
    made it specific. Measured across the audit fixture, 26% of answers that
    contained the right material did not *lead* with it, and this is why: asked
    "what happens to primary keys" the answer opened from the Redis
    destination-key section, which says ``key`` but never ``primary``; asked
    "how do I connect a postgres database" it opened with the MCP server entry;
    asked "what does mirror mode do to deleted rows" it opened with ``upsert``.

    Two things count. First the **head anchors**: the rarest words the operator
    actually typed, restricted to words some retrieved sentence uses, so this
    never asks for a lead that does not exist. Rarest by corpus IDF, the same
    statistic ranking already trusts — and every typed word within
    ``SUBJECT_ANCHOR_BAND`` of the rarest, because a gap that small is not a
    distinction. Asked "what does mirror mode do to deleted rows", ``delet``
    scored 2.60 and ``mirror`` 2.48: on the single rarest word the sentence
    defining mirror mode was not *about* mirror mode, and the answer opened
    with the definition of CDC because it says "deletes".

    Second, the phrase expansions, because they are the operator's own words in
    the corpus's spelling — a sentence using one is on subject even when it
    never uses theirs. Asked "what is change data capture" the corpus answers
    with ``cdc`` and never spells out the phrase, and on the anchor alone the
    answer left the sync-mode passage to find a sentence saying ``capture``.

    The loose single-word expansions are excluded. They are a guess for recall
    and they hijack a lead: asked "what does checksum MATCH prove" they
    contribute ``reconcile``, rarer than anything typed, which is not what the
    operator asked about.
    """
    if idf is None:
        return frozenset()
    typed = [t for t in analysis.terms if t in available]
    if not typed:
        return frozenset()
    # Ties are broken toward the last word, because an English noun phrase puts
    # its head last. "How is this different from writing ETL scripts" scores
    # ``writ``, ``etl`` and ``script`` at exactly 4.19 each; taking the first
    # made the question about *writing*, and the answer opened on "writing one
    # into an instant column" from the timestamp passage.
    scored = [(float(idf(term)), position, term) for position, term in enumerate(typed)]
    rarest = max(scored, key=lambda row: (row[0], row[1]))
    if rarest[0] <= 0:
        # No corpus statistic to band against — every typed word is equally
        # unknown, and widening on a zero would make the whole question the
        # subject, which is the same as having no subject test at all.
        return frozenset({rarest[2], *analysis.phrase_expansions})
    bar = rarest[0] * SUBJECT_ANCHOR_BAND
    anchors = {term for value, _, term in scored if value >= bar}
    return frozenset({*anchors, *analysis.phrase_expansions})


def build_candidates(
    analysis: QueryAnalysis,
    sections: Sequence[tuple[str, str, str, str]],
    *,
    scores: Sequence[float] | None = None,
    idf: Callable[[str], float] | None = None,
) -> list[Candidate]:
    """Score every sentence in the retrieved sections against the question.

    ``sections`` is ``(section_title, citation, href, text)`` in fused rank
    order; earlier sections carry a rank prior so a tie resolves toward the
    passage retrieval preferred. ``scores`` are those passages' retrieval
    scores, which turn the prior from an ordering into a margin. ``idf`` is the
    corpus term statistic, so a matched word counts for what it distinguishes.
    """
    typed = set(analysis.terms)
    weights = term_weights(analysis.terms, idf)
    expanded = set(analysis.expansions) - typed
    # Only phrases the operator actually typed, adjacent as typed. An expansion
    # pair would be a guess about a phrase, which is not what this credit is for.
    phrases = adjacent_shingles(analysis.text)
    candidates: list[Candidate] = []
    # The prose view of each candidate, aligned with ``candidates``. Subject
    # naming is judged on prose for the same reason matching is: the corpus
    # writes fixture names as literals, and `sample-orders.csv` is not a
    # sentence about orders.
    prose: list[frozenset[str]] = []
    order = 0
    top_score = max((s for s in (scores or ()) if s > 0), default=0.0)
    for rank, (section_title, citation, href, text) in enumerate(sections):
        rank_prior = 1.0 / (1.0 + rank)
        # An ordinal prior treats "retrieval preferred this one" and "retrieval
        # preferred this one four times over" as the same fact. Asked how the
        # product differs from ETL scripts, the FAQ section written about that
        # scored 10.9 and a timezone passage scored 2.8 — and the answer opened
        # from the timezone passage, because one of its sentences happened to
        # contain the word "writing". Scaling the prior by the passage's share
        # of the best score keeps a weak passage available as support without
        # letting it speak first.
        share = 1.0
        if top_score > 0 and scores is not None and rank < len(scores):
            share = max(0.0, float(scores[rank])) / top_score
            rank_prior *= share
        anchor_bar = HEADING_ANCHOR_TOP if share >= 1.0 else HEADING_ANCHOR
        heading_match = _heading_match(citation or section_title, typed)
        heading_terms = frozenset(content_terms(citation or section_title))
        # An enumeration ask is a request for a list, whichever passage holds
        # it; otherwise the heading has to speak for its items.
        list_vouched = analysis.ask == "enumeration" or heading_match >= anchor_bar
        for sentence, is_list_item in _split_annotated(text, section_title):
            sentence_terms = content_terms(sentence)
            terms = frozenset(sentence_terms)
            # Scored on prose; ``terms`` keeps the literals, because redundancy
            # and length are properties of the whole sentence either way.
            prose_terms = frozenset(content_terms(_CODE_SPAN.sub(" ", sentence)))
            hit_terms = terms & typed & prose_terms
            typed_hits = (
                sum(weights.get(t, 1.0) for t in hit_terms)
                if weights
                else len(hit_terms)
            )
            expanded_hits = len(terms & expanded)
            # Both spellings count: the corpus writes an aspect as prose
            # ("column order") and as the identifier the certificate records
            # (``column_order``), and the question may have named either.
            phrase_hits = (
                len(phrases & (terms | adjacent_shingles(sentence)))
                if phrases
                else 0
            )
            # A list item rarely repeats the word that names the list. "G3
            # Schema contract — source and target schemas are compatible" scores
            # nothing against "what are the preflight gates", so all nine gates
            # were dropped and the answer talked around them. The heading the
            # question did match is what vouches for its items.
            listed_credit = (
                LIST_ITEM_CREDIT * heading_match if is_list_item else 0.0
            )
            match = typed_hits + EXPANSION_WEIGHT * expanded_hits
            subject_view = prose_terms
            if match or listed_credit or phrase_hits:
                score = (
                    match * _length_norm(len(terms))
                    + PHRASE_CREDIT * phrase_hits
                    + listed_credit
                    + _section_bonus(analysis.ask, section_title, sentence)
                    + _shape_bonus(analysis.ask, sentence)
                    + 0.8 * rank_prior
                )
            elif heading_match >= anchor_bar:
                # This sentence is here on its heading's word, so the heading
                # speaks for what it is about as well. "How is this different
                # from writing ETL scripts" is answered by the body of the FAQ
                # card of that name, which lists the differences without using
                # one word of the question; judged on its own wording it named
                # no subject, and the answer opened on "writing one into an
                # instant column" from the timestamp passage instead.
                subject_view = heading_terms
                # The same argument as for list items, one level weaker. "Do
                # you have webhooks" is answered by "Subscribe to job.completed,
                # job.failed and pipeline.quarantine_threshold events", which
                # names the events instead of the feature — ``webhook`` appears
                # only in the heading above it. Scored on its own words that
                # sentence is worth nothing, so the answer came from an
                # unrelated section. The ask-shape and section priors are left
                # out on purpose: they describe how well a sentence fits the
                # question's shape, and there is no fit to grade when the
                # sentence matched none of it.
                score = HEADING_CREDIT + 0.8 * rank_prior
            else:
                continue
            candidates.append(
                Candidate(
                    text=sentence,
                    section_title=section_title,
                    citation=citation,
                    href=href,
                    order=order,
                    terms=terms,
                    score=score,
                    list_item=is_list_item,
                    list_vouched=is_list_item and list_vouched,
                )
            )
            prose.append(subject_view)
            order += 1

    subject = subject_words(
        analysis, frozenset().union(*prose) if prose else frozenset(), idf
    )
    if subject:
        candidates = [
            replace(cand, names_subject=bool(words & subject))
            for cand, words in zip(candidates, prose)
        ]
    candidates.sort(key=lambda c: (c.score, -c.order), reverse=True)
    return candidates


def select_sentences(
    candidates: Sequence[Candidate],
    limit: int = MAX_SENTENCES,
    mmr_lambda: float = MMR_LAMBDA,
) -> list[Candidate]:
    """Maximal-marginal-relevance selection: relevant, then non-redundant."""
    chosen: list[Candidate] = []
    pool = list(candidates)
    if not pool:
        return []
    # The lead is decided first and then seeds everything else, because every
    # other decision here is relative to the sentence that answers: the
    # relevance floor is a share of it, and marginal relevance is diversity
    # away from it. Choosing it last — as the highest score among whatever
    # survived — let a sentence that never mentions the question's subject both
    # set the floor and open the answer.
    best = _lead(pool)
    chosen.append(best)
    pool.remove(best)

    floor = RELEVANCE_FLOOR * best.score
    pool = [c for c in pool if c.list_vouched or c.score >= floor]

    top = best.score or 1.0
    while pool and len(chosen) < limit:
        def marginal(cand: Candidate) -> float:
            overlap = max(
                (
                    len(cand.terms & other.terms) / max(len(cand.terms | other.terms), 1)
                    for other in chosen
                ),
                default=0.0,
            )
            return mmr_lambda * (cand.score / top) - (1.0 - mmr_lambda) * overlap

        pick = max(pool, key=marginal)
        if marginal(pick) <= 0:
            break
        chosen.append(pick)
        pool.remove(pick)
    chosen = _complete_lists(chosen, candidates)
    # Lead with the sentence that answers, then read in source order. Sorting
    # purely by source order opened "what is quarantine" with "Open Operations →
    # Jobs → Quarantine on the run", because that section ranked first — an
    # answer has to start with the answer.
    if best.list_item:
        # A list item is never hoisted: pulling G5 to the front leaves the
        # gates reading G5, G1, G2, G3 …
        chosen.sort(key=lambda c: c.order)
        return chosen
    rest = sorted((c for c in chosen if c is not best), key=lambda c: c.order)
    return [best, *rest]


def _lead(pool: Sequence[Candidate]) -> Candidate:
    """The sentence to open with: names the subject if anything does.

    Preferring the highest score alone is how "what does mirror mode do to
    deleted rows" opened on ``upsert`` — a sum over ``mode`` and ``rows`` beat
    the sentence that says ``mirror`` — and how "how do I connect a postgres
    database" opened on the MCP server entry. Among sentences that do name the
    subject the score still decides, so this reorders the shortlist rather than
    replacing the ranking.

    A list the question asked for is exempt. When the strongest candidate is a
    vouched list item the list *is* the answer, and hoisting a prose sentence
    over it is how "what are the preflight gates" stopped opening on G1.

    Only sentences within ``LEAD_SUBJECT_FLOOR`` of the top are considered:
    naming the subject says a sentence is about the right thing, not that it
    answers.
    """
    top = max(pool, key=lambda c: c.score)
    if top.list_vouched:
        return top
    bar = top.score * LEAD_SUBJECT_FLOOR
    named = [
        c for c in pool if c.names_subject and not c.list_item and c.score >= bar
    ]
    return max(named, key=lambda c: c.score) if named else top


def _complete_lists(
    chosen: list[Candidate],
    candidates: Sequence[Candidate],
) -> list[Candidate]:
    """Add the siblings of any selected list item.

    Maximal-marginal-relevance is built to suppress near-duplicates, and the
    items of one list look like near-duplicates to it — they share a heading, a
    shape and half their words. Left to itself it returned "G1" and "G4" and
    called that the gate list. A list is one unit of evidence.
    """
    sections_with_items = {c.section_title for c in chosen if c.list_vouched}
    if not sections_with_items:
        return chosen
    picked = {c.order for c in chosen}
    out = list(chosen)
    for section in sections_with_items:
        siblings = [
            c
            for c in candidates
            if c.list_vouched
            and c.section_title == section
            and c.order not in picked
        ]
        already = sum(
            1 for c in chosen if c.list_vouched and c.section_title == section
        )
        for cand in siblings[: max(0, MAX_LIST_ITEMS - already)]:
            out.append(cand)
            picked.add(cand.order)
    return out


def compose_answer(
    analysis: QueryAnalysis,
    sections: Sequence[tuple[str, str, str, str]],
    *,
    scores: Sequence[float] | None = None,
    idf: Callable[[str], float] | None = None,
    partial_caveat: str = "",
    limit: int = MAX_SENTENCES,
) -> str:
    """The spoken answer: selected documentation sentences, then its citations.

    ``partial_caveat`` names what the question asked for that the documentation
    does not cover. It is stated after the answer rather than instead of it —
    an operator can act on "here is what is documented, this part is not" and
    cannot act on a refusal.
    """
    candidates = build_candidates(analysis, sections, scores=scores, idf=idf)
    chosen = select_sentences(candidates, limit=limit)
    if not chosen:
        return ""

    body = " ".join(c.text for c in chosen)
    parts = [body]
    if partial_caveat:
        parts.append(partial_caveat)

    cited: list[str] = []
    for cand in chosen:
        if cand.citation and cand.citation not in cited:
            cited.append(cand.citation)
    if cited:
        parts.append("Source: " + " · ".join(cited[:MAX_CITED_SECTIONS]) + " (Help)")
    return "\n\n".join(parts)
