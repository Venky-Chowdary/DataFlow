"""Datawrap first-party copy-grounded generator.

This is **not** a foundation model and it is **not** ChatGPT. It is a
trained, fail-closed stack that sits in front of the extractive Pilot
already in production:

1. Deterministic ``rewrite_operator_question`` (slang, wrappers).
2. Optional **semantic rewrite** — dual encoder snaps open English onto
   a canonical gold question when cosine is high and no unrelated
   product subject is introduced.
3. Hybrid retrieve (BM25 + char 4-gram TF-IDF, RRF) + evidence policy.
4. Extractive ``compose_answer``.
5. Optional **first-party attention+copy GRU** over packed evidence
   (Luong attention, pointer-generator mix). Discarded unless
   ``invented_claims`` / dialogue grounding pass. Degenerate loops
   fall back to the extractive draft. The pointer-generator prefix
   still narrates product RAG.

Why this architecture
---------------------
Open English is an embedding problem. See FastText (Bojanowski et al.,
TACL 2017) for hashed character n-grams and DPR / InfoNCE (Karpukhin
et al.; van den Oord et al.) for learning a space where a paraphrase
sits next to its gold question. Fluency without new facts is a *copy*
problem. See the pointer-generator (See, Liu & Manning, ACL 2017):
generate only from a closed glue vocab, copy every product token from
evidence. A regex engine is not this. A hosted LLM that can say
``exactly-once`` when the evidence says ``at-least-once`` is also not
this.

Honesty bar (do not regress)
----------------------------
* Third-party providers stay **opt-in polish** in Settings → AI.
* CDC default remains **at-least-once upsert on ``_df_lsn``**.
* dbt and SSH are **not** product capabilities. The claim gate refuses
  them unless the evidence already names them (it should not).
* Catalog tiles ≠ transfer-live. This package does not invent SKUs.
* No legal no-data-loss SLA. Bad rows are quarantined; the row ledger
  must close. Airbyte connector packs are not a runtime.

Public serve API: ``semantic_rewrite``, ``narrate_answer``, ``speak_with_lm``.
"""

from .engine import (
    load_model,
    narrate_answer,
    reset_model_cache,
    retrieve_bonus,
    semantic_rewrite,
    speak_with_lm,
)

__all__ = [
    "load_model",
    "narrate_answer",
    "reset_model_cache",
    "retrieve_bonus",
    "semantic_rewrite",
    "speak_with_lm",
]
