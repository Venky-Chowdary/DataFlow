"""Copy-grounded pointer-generator — fluency without new facts.

See, Liu & Manning (ACL 2017) *Get To The Point*: at each step a decoder
may generate from a closed vocabulary or copy a span from the source.
We apply that mechanism at **sentence grain**, which is the grain this
corpus can train without a GPU and the grain that cannot splice
``exactly-once`` out of ``at-least-once``.

What the model may do
---------------------
1. **Pointer.** Score evidence sentences against the question (bilinear
   ``qᵀ W s``) and keep the extractive draft — already the selected
   sentences — in that order.
2. **Generate.** Predict one closed fluency prefix (``""``,
   ``"In short: "``, ``"Documented behavior: "``). Prefixes contain no
   period, so they cannot steal the lead sentence an operator audit
   measures.
3. **Token lock.** Every output token must be glue or an evidence token.
   ``dbt``, ``ssh``, and ``exactly-once`` are not in the glue list.

What it must not do
-------------------
Invent a warehouse, a CDC delivery guarantee, dbt, or SSH. If the
predicted prefix plus draft fails ``keeps_draft_facts`` /
``retains_evidence`` / ``invented_claims``, the engine keeps the
extractive draft. That fail-closed step is the product, not a fallback
we hope never runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from .dual_encoder import DIM, DualEncoder
from .tokens import GLUE_WORDS, word_tokens

# Prefixes are glue only. A period here would become the first sentence
# and bury ``wal_level`` / ``own local engine`` under empty fluency.
PREFIXES: tuple[str, ...] = (
    "",
    "In short: ",
    "Documented behavior: ",
)

PREFIX_SEED_BIAS = (
    # Most product answers should stay extractive. Fluency is opt-in.
    0.8,
    0.12,
    0.08,
)


def split_sentences(text: str) -> list[str]:
    """Period-delimited sentences, citations kept with the body."""
    body = (text or "").strip()
    if not body:
        return []
    out: list[str] = []
    rest = body
    while rest:
        cut = rest.find(". ")
        if cut < 0:
            out.append(rest.strip())
            break
        out.append(rest[: cut + 1].strip())
        rest = rest[cut + 2 :].strip()
    return [s for s in out if s]


def _softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max()
    exp = np.exp(shifted)
    return exp / (exp.sum() + 1e-8)


def tokens_grounded_in_evidence(output: str, evidence: str) -> bool:
    """Every content token is glue or appears in the evidence.

    This is the serve-time copy lock. A decoder that emits ``dbt`` when
    the evidence never said ``dbt`` fails here even if the neural head
    was confident.
    """
    allowed = set(word_tokens(evidence))
    allowed.update(GLUE_WORDS)
    # Citation machinery and identifiers the extractive draft already used.
    allowed.update({"source", "help"})
    for token in word_tokens(output):
        if token not in allowed:
            return False
    return True


@dataclass
class PointerGenerator:
    """Sentence pointer + closed-prefix generator over dual-encoder space."""

    select: np.ndarray
    prefix: np.ndarray
    prefix_bias: np.ndarray
    dim: int = DIM
    prefixes: tuple[str, ...] = field(default_factory=lambda: PREFIXES)

    @classmethod
    def init(cls, *, dim: int = DIM, seed: int = 7) -> PointerGenerator:
        rng = np.random.default_rng(seed)
        n_pref = len(PREFIXES)
        return cls(
            select=np.eye(dim, dtype=np.float64) + rng.normal(0.0, 0.02, (dim, dim)),
            prefix=rng.normal(0.0, 0.05, (n_pref, dim)),
            prefix_bias=np.log(np.asarray(PREFIX_SEED_BIAS, dtype=np.float64) + 1e-6),
            dim=dim,
        )

    def sentence_score(self, query_vec: np.ndarray, sent_vec: np.ndarray) -> float:
        return float(query_vec @ (self.select @ sent_vec))

    def prefix_logits(self, query_vec: np.ndarray) -> np.ndarray:
        return self.prefix @ query_vec + self.prefix_bias

    def predict_prefix(self, query_vec: np.ndarray) -> str:
        logits = self.prefix_logits(query_vec)
        return self.prefixes[int(np.argmax(logits))]

    def rank_sentences(
        self,
        encoder: DualEncoder,
        question: str,
        sentences: list[str],
    ) -> list[str]:
        if not sentences:
            return []
        q = encoder.encode_query(question)
        scored = [
            (self.sentence_score(q, encoder.encode_gold(sent)), i, sent)
            for i, sent in enumerate(sentences)
        ]
        scored.sort(key=lambda row: (-row[0], row[1]))
        return [sent for _score, _i, sent in scored]

    def narrate(
        self,
        encoder: DualEncoder,
        question: str,
        evidence: str,
        draft: str,
    ) -> str:
        """Prefix + extractive draft. Never drops a draft sentence."""
        q = encoder.encode_query(question)
        prefix = self.predict_prefix(q)
        body = (draft or "").strip()
        if not body:
            return ""
        return f"{prefix}{body}"

    def train(
        self,
        encoder: DualEncoder,
        examples: list[tuple[str, str, str]],
        *,
        epochs: int = 4,
        lr: float = 0.05,
    ) -> list[float]:
        """Train prefix choice and sentence pointer on copy examples.

        ``examples`` are ``(question, evidence, answer)``. The gold prefix
        is empty — the extractive draft is already fluent enough — so the
        classifier learns to stay quiet unless the question embedding
        clearly matches a fluency-shaped query. The pointer still learns
        ``qᵀ W s`` so serve-time ranking is not random.
        """
        if not examples:
            return []
        history: list[float] = []
        gold_prefix = 0
        for _epoch in range(epochs):
            total = 0.0
            steps = 0
            for question, evidence, answer in examples:
                q = encoder.encode_query(question)
                logits = self.prefix_logits(q)
                probs = _softmax(logits)
                total += float(-np.log(np.clip(probs[gold_prefix], 1e-12, 1.0)))
                dlogits = probs.copy()
                dlogits[gold_prefix] -= 1.0
                self.prefix -= lr * np.outer(dlogits, q)
                self.prefix_bias -= lr * dlogits

                gold_sents = split_sentences(answer)
                ev_sents = split_sentences(evidence)
                if gold_sents and ev_sents:
                    # Push the first gold sentence above the mean of the others.
                    target = gold_sents[0]
                    target_vec = encoder.encode_gold(target)
                    score_t = self.sentence_score(q, target_vec)
                    others = [s for s in ev_sents if s != target][:3]
                    if others:
                        mean_o = np.mean(
                            [encoder.encode_gold(s) for s in others], axis=0
                        )
                        score_o = self.sentence_score(q, mean_o)
                        # Softplus-style margin: increase (score_t - score_o).
                        gap = score_t - score_o
                        weight = 1.0 / (1.0 + np.exp(gap))
                        self.select += lr * weight * np.outer(q, target_vec - mean_o)
                        total += float(max(0.0, 1.0 - gap))
                steps += 1
            history.append(total / max(steps, 1))
        return history
