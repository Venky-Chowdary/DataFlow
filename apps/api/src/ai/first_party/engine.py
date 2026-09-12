"""Serve path: semantic rewrite + copy-grounded narrate, both fail-closed.

This module is the only thing ``query_analysis`` and ``product_docs``
import. Training stays in ``checkpoint.train_checkpoint``. If the
``npz`` is missing, NumPy is missing, or a gate fails, we return the
deterministic rewrite / extractive draft unchanged. That is the
enterprise contract: a trained head is an improvement when it is
grounded, never a new source of product facts.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from .claims import invented_claims
from .checkpoint import FirstPartyCheckpoint, default_artifact_path, load_checkpoint
from .dual_encoder import nearest_gold
from .pointer_gen import tokens_grounded_in_evidence
from .tokens import word_tokens

# High enough that a gold must be a paraphrase, not a neighbor heading.
# Measured so ``rice`` / off-subject slang stay unrewritten.
REWRITE_COSINE = 0.86

# Heading-ish words the gold is allowed to add without changing the subject.
# ``pilot`` is how the engine section names itself; it is not a CDC claim.
_GENERIC_OK_NEW = frozenset(
    {
        "pilot",
        "datawrap",
        "engine",
        "local",
        "document",
        "product",
        "help",
        "own",
        "third",
        "party",
        "llm",
        "model",
        "generat",
        "pointer",
        "encoder",
        "copy",
    }
)

# Tight families: a new term is allowed only when the source already named
# something in the same family. ``wal`` may become ``wal_level``; ``cdc``
# may not become ``quarantine``.
_SUBJECT_FAMILIES: tuple[frozenset[str], ...] = (
    frozenset({"wal", "wal_level", "pgoutput", "logical", "toast", "publication"}),
    frozenset({"cdc", "capture", "lsn", "df_lsn", "_df_lsn", "binlog", "gtid", "handoff"}),
    frozenset({"quarantine", "rejected", "reject", "bad", "row"}),
    frozenset({"chatgpt", "openai", "anthropic", "ollama", "hybrid", "llm", "engine"}),
    frozenset({"viewer", "editor", "admin", "role", "rbac"}),
    frozenset({"yaml", "gitops", "export", "manifest"}),
    frozenset({"gate", "preflight", "validat", "g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8", "g9"}),
    frozenset({"iceberg", "mor", "cow"}),
    frozenset({"upsert", "merge", "dialect", "conflict"}),
    frozenset({"airbyte", "pack", "cdk"}),
    frozenset({"loss", "silent", "ledger", "unaccounted"}),
    frozenset({"soc2", "hipaa", "baa", "gdpr", "dpa", "attestation"}),
    frozenset({"replica", "identity", "tombston", "delet"}),
    frozenset({"dbt", "complement", "export"}),
    frozenset({"ssh", "tunnel", "bastion", "jump"}),
    frozenset({"debezium", "kafka", "flink", "connect", "bridge"}),
    frozenset({"terraform", "gitops", "yaml", "provider"}),
    frozenset({"privatelink", "vpc", "peering"}),
    frozenset({"airflow", "spark", "orchestrat"}),
    frozenset({"glue", "iceberg", "catalog", "nessie"}),
    frozenset({"confirm", "requires_confirm"}),
)


def _env_on(name: str, default: bool = True) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"0", "false", "off", "no"}:
        return False
    if raw in {"1", "true", "on", "yes"}:
        return True
    return default


def rewrite_enabled() -> bool:
    return _env_on("DATAFLOW_FP_REWRITE", default=True)


def narrate_enabled() -> bool:
    return _env_on("DATAFLOW_FP_NARRATE", default=True)


@lru_cache(maxsize=1)
def load_model(path: str | None = None) -> FirstPartyCheckpoint | None:
    """Load the shipped checkpoint, or ``None`` if serve must stay extractive."""
    try:
        import numpy  # noqa: F401
    except ImportError:
        return None
    artifact = Path(path) if path else default_artifact_path()
    if not artifact.is_file():
        return None
    try:
        return load_checkpoint(artifact)
    except Exception:
        return None


def reset_model_cache() -> None:
    """Test helper — drop the process-wide checkpoint."""
    clear = getattr(load_model, "cache_clear", None)
    if callable(clear):
        clear()


def _subject_terms(text: str) -> set[str]:
    from src.ai.rag.evidence_policy import is_subject_term
    from src.ai.rag.lexical_index import content_terms

    return {term for term in content_terms(text) if is_subject_term(term)}


def _src_closure(text: str) -> set[str]:
    """Typed terms plus phrase/loose expansions — not a recursive rewrite."""
    from src.ai.rag.lexical_index import content_terms, normalize
    from src.ai.rag.query_analysis import expand_terms_tiered

    terms = set(content_terms(text))
    raw = tuple(terms)
    phrase, loose = expand_terms_tiered(text, raw)
    terms.update(phrase)
    terms.update(loose)
    terms.update(normalize(t) for t in word_tokens(text))
    return terms


def _same_family(new_term: str, src_terms: set[str]) -> bool:
    for family in _SUBJECT_FAMILIES:
        if new_term in family and src_terms & family:
            return True
    return False


def _lexical_variant(new_term: str, src_terms: set[str]) -> bool:
    for src in src_terms:
        if not src or not new_term:
            continue
        if new_term == src:
            return True
        if len(src) >= 3 and (src in new_term or new_term in src):
            return True
    return False


def drops_distinctive_subjects(source: str, gold: str) -> bool:
    """Whether snapping to ``gold`` would drop a subject the operator named.

    ``can I bring Iceberg with a Glue catalog`` must not become a generic
    Iceberg question — that is how merge-on-read stole the Glue catalog
    lead after a high-cosine snap.
    """
    src = _subject_terms(source)
    if not src:
        return False
    kept = _subject_terms(gold) | _src_closure(gold)
    lost = (src - kept) - _GENERIC_OK_NEW
    return bool(lost)


def introduces_unrelated_subjects(source: str, gold: str) -> bool:
    """Whether snapping to ``gold`` would name a new product subject.

    Allowed additions: generic engine words, lexical variants
    (``wal`` → ``wal_level``), and family members of a source term.
    Everything else is a neighbor steal and is refused.
    """
    src_subjects = _subject_terms(source)
    if not src_subjects:
        return True
    gold_subjects = _subject_terms(gold)
    closure = _src_closure(source) | src_subjects
    new = gold_subjects - closure
    for term in new:
        if term in _GENERIC_OK_NEW:
            continue
        if _same_family(term, closure):
            continue
        if _lexical_variant(term, closure):
            continue
        return True
    return False


def semantic_rewrite(question: str) -> str | None:
    """Nearest gold question, or ``None`` when the snap is not safe.

    Callers already ran ``rewrite_operator_question``. This only fires
    when the cosine is high **and** the gold does not introduce an
    unrelated product subject. Off-subject English stays off-subject.
    """
    if not rewrite_enabled():
        return None
    text = (question or "").strip()
    if not text:
        return None
    model = load_model()
    if model is None or not model.gold_questions:
        return None
    src_subjects = _subject_terms(text)
    if not src_subjects:
        return None
    gold, score = nearest_gold(
        model.encoder,
        text,
        list(model.gold_questions),
        model.gold_vectors,
    )
    if not gold or score < REWRITE_COSINE:
        return None
    if gold.strip().lower() == text.lower():
        return None
    if introduces_unrelated_subjects(text, gold):
        return None
    if drops_distinctive_subjects(text, gold):
        return None
    return gold


def retrieve_bonus(query: str, passages: list[str]) -> list[float]:
    """Optional dual-encoder scores for already-retrieved candidates.

    Not wired into fusion by default: a retrieve bonus is how a new
    heading steals a neighbor in a ``limit=4`` window. Exposed for
    experiments and tests.
    """
    model = load_model()
    if model is None:
        return [0.0] * len(passages)
    q = model.encoder.encode_query(query)
    return [float(q @ model.encoder.encode_gold(p)) for p in passages]


def narrate_answer(question: str, evidence: str, draft: str) -> str | None:
    """Copy-grounded fluency over an extractive draft, or ``None``.

    The extractive composer stays the source of facts. This may add a
    closed prefix. It may not drop ``_df_lsn``, invent dbt/SSH, or turn
    at-least-once into exactly-once.
    """
    if not narrate_enabled():
        return None
    body = (draft or "").strip()
    if not body:
        return None
    model = load_model()
    if model is None:
        return None
    from src.ai.rag.evidence import keeps_draft_facts, retains_evidence
    from src.ai.rag.lexical_index import content_terms

    generated = model.generator.narrate(
        model.encoder, question, evidence, body
    ).strip()
    if not generated or generated == body:
        return None
    if invented_claims(generated, evidence):
        return None
    if not tokens_grounded_in_evidence(generated, f"{evidence} {body}"):
        return None
    if not keeps_draft_facts(body, generated):
        return None
    matched = set(content_terms(question))
    context = set(content_terms(f"{evidence} {body}"))
    if not retains_evidence(generated, matched, context):
        return None
    return generated
