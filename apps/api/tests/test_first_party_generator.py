"""First-party copy-grounded generator: trained heads, fail-closed gates.

This is the honesty suite for ``src.ai.first_party``. A passing dual-encoder
loss is not enough: the rewriter must not turn rice into CDC, the narrator
must not mint dbt / SSH / exactly-once, and ``_df_lsn`` must survive fluency.
"""

from __future__ import annotations

import numpy as np
import pytest

from src.ai.first_party.claims import claims_are_grounded, invented_claims
from src.ai.first_party.dataset import align_pairs, gold_questions
from src.ai.first_party.dual_encoder import DualEncoder, nearest_gold
from src.ai.first_party.engine import (
    drops_distinctive_subjects,
    introduces_unrelated_subjects,
    narrate_answer,
    reset_model_cache,
    semantic_rewrite,
)
from src.ai.first_party.pointer_gen import PREFIXES, tokens_grounded_in_evidence
from src.ai.first_party.tokens import hashed_features, word_tokens


def test_hashed_features_are_stable_and_nonempty() -> None:
    first = hashed_features("wal_level logical")
    second = hashed_features("wal_level logical")
    assert first and first == second
    assert hashed_features("") == []
    assert word_tokens("Skip Dupes via _df_lsn") == ["skip", "dupes", "via", "_df_lsn"]


def test_forbidden_claims_cannot_be_invented() -> None:
    evidence = "CDC default is at-least-once upsert on `_df_lsn`."
    assert invented_claims("We ship dbt models for the warehouse.", evidence) == ("dbt",)
    assert invented_claims("Connect through an SSH tunnel.", evidence) == ("ssh",)
    assert invented_claims("Delivery is exactly-once.", evidence) == ("exactly-once",)
    assert invented_claims(evidence, evidence) == ()
    assert claims_are_grounded(
        "CDC default is at-least-once upsert on `_df_lsn`.", evidence
    )
    # Evidence that already names the token may restate it.
    ev = "The product does not claim exactly-once CDC."
    assert invented_claims("The product does not claim exactly-once CDC.", ev) == ()


def test_token_lock_rejects_ungrounded_product_words() -> None:
    evidence = "Pilot answers with its own local engine by default."
    assert tokens_grounded_in_evidence(evidence, evidence)
    assert tokens_grounded_in_evidence("In short: " + evidence, evidence)
    assert not tokens_grounded_in_evidence(
        "Pilot answers with dbt and an ssh tunnel.", evidence
    )


def test_fluency_prefixes_cannot_steal_the_lead_sentence() -> None:
    for prefix in PREFIXES:
        assert "." not in prefix, prefix


def test_infonce_pulls_a_paraphrase_toward_its_gold() -> None:
    encoder = DualEncoder.init(seed=7)
    pairs = [
        ("are you chatgpt", "does pilot use chatgpt or a third-party llm"),
        ("do you use openai", "does pilot use chatgpt or a third-party llm"),
        ("gotta have logical wal", "do i need wal_level logical"),
        ("where do bad rows go", "where do bad rows end up"),
        ("does pilot use chatgpt or a third-party llm", "does pilot use chatgpt or a third-party llm"),
        ("do i need wal_level logical", "do i need wal_level logical"),
        ("where do bad rows end up", "where do bad rows end up"),
    ]
    before = encoder.cosine("are you chatgpt", "does pilot use chatgpt or a third-party llm")
    history = encoder.train_infonce(pairs * 8, epochs=6, batch_size=4, seed=7)
    after = encoder.cosine("are you chatgpt", "does pilot use chatgpt or a third-party llm")
    rice = encoder.cosine("how do I cook rice tonight", "does pilot use chatgpt or a third-party llm")
    assert history[-1] <= history[0] + 1e-6
    assert after > before or after > 0.5
    assert after > rice


def test_glue_catalog_is_not_rewritten_to_a_generic_iceberg_heading() -> None:
    assert drops_distinctive_subjects(
        "can I bring Iceberg with a Glue catalog",
        "which iceberg catalogs can I use",
    )
    assert drops_distinctive_subjects(
        "can I bring Iceberg with a Glue catalog",
        "does iceberg upsert use merge-on-read",
    )


def test_upsert_merge_and_airbyte_pack_do_not_snap_to_neighbors() -> None:
    assert introduces_unrelated_subjects(
        "what's the difference between upsert and merge",
        "does iceberg upsert use merge-on-read",
    )
    assert drops_distinctive_subjects(
        "can I use a custom Airbyte connector",
        "how datawrap differs from airbyte and fivetran",
    )
    assert drops_distinctive_subjects(
        "do you guarantee no data loss",
        "do you read the wal or just poll",
    )
    assert drops_distinctive_subjects(
        "can you sign a HIPAA BAA",
        "byok, region, and audit",
    )
    assert drops_distinctive_subjects(
        "what is the difference between mirror and upsert",
        "what is the difference between upsert and merge",
    )
    assert drops_distinctive_subjects(
        "can I undo a transfer",
        "do you have salesforce",
    )
    assert drops_distinctive_subjects(
        "can I call this from GitHub Actions",
        "open the mcp page",
    )
    assert drops_distinctive_subjects(
        "can I export audit logs as CSV",
        "do you sign a soc2 or hipaa baa",
    )
    assert drops_distinctive_subjects(
        "what is the difference between incremental and upsert",
        "what is the difference between upsert and merge",
    )
    assert introduces_unrelated_subjects(
        "do you support IP allowlists",
        "can I require mfa",
    )


def test_rice_is_an_unrelated_subject() -> None:
    assert introduces_unrelated_subjects(
        "how do I cook rice tonight",
        "do I need wal_level logical",
    )
    assert introduces_unrelated_subjects(
        "what is change data capture",
        "where do bad rows end up",
    )


def test_align_pairs_only_name_dbt_and_ssh_as_honest_absences() -> None:
    for pair in align_pairs():
        blob = f"{pair.query} {pair.gold}".lower()
        if "dbt" in blob:
            assert "dbt cloud" in pair.gold.lower()
            assert "wal_level" not in pair.gold.lower()
        if "ssh" in blob or "bastion" in pair.query.lower():
            assert "ssh tunnel" in pair.gold.lower()
            assert "wal_level" not in pair.gold.lower()


@pytest.fixture
def trained_tmp_model(tmp_path, monkeypatch):
    """Fit a real checkpoint in tmp and point the serve loader at it."""
    from src.ai.first_party.checkpoint import train_checkpoint

    bundle = train_checkpoint(seed=7, encoder_epochs=5, generator_epochs=2)
    dest = bundle.save(tmp_path / "pilot_fp_v1.npz")
    monkeypatch.setattr(
        "src.ai.first_party.engine.default_artifact_path",
        lambda: dest,
    )
    reset_model_cache()
    yield bundle
    reset_model_cache()


def test_semantic_rewrite_stays_fail_closed(trained_tmp_model) -> None:
    assert semantic_rewrite("how do I cook rice tonight") is None
    assert semantic_rewrite("write me a poem about the sea") is None
    snapped = semantic_rewrite("are you chatgpt")
    if snapped:
        assert "chatgpt" in snapped.lower() or "llm" in snapped.lower()
        assert "wal_level" not in snapped.lower()
        assert "dbt" not in snapped.lower()


def test_narrate_keeps_lsn_and_refuses_invented_claims(trained_tmp_model) -> None:
    from src.ai.first_party.engine import load_model

    draft = "CDC default is at-least-once upsert on `_df_lsn`."
    evidence = draft
    out = narrate_answer("is cdc exactly once", evidence, draft)
    assert out is None or "_df_lsn" in out
    spoken = (out or "").lower().replace(" ", "").replace("-", "")
    assert "exactlyonce" not in spoken

    class _Lie:
        def narrate(self, *_args, **_kwargs):
            return "We provide exactly-once CDC with dbt and an SSH tunnel."

    loaded = load_model()
    assert loaded is not None
    loaded.generator = _Lie()
    assert narrate_answer("is cdc exactly once", evidence, draft) is None


def test_gold_questions_include_the_engine_heading() -> None:
    blob = " ".join(gold_questions()).lower()
    assert "chatgpt" in blob or "third-party" in blob


def test_nearest_gold_ranks_chatgpt_above_cdc(trained_tmp_model) -> None:
    gold, score = nearest_gold(
        trained_tmp_model.encoder,
        "are you chatgpt",
        list(trained_tmp_model.gold_questions),
        trained_tmp_model.gold_vectors,
    )
    assert score > 0.2
    assert "cdc" not in gold.lower() or "chatgpt" in gold.lower()
    rice, rice_score = nearest_gold(
        trained_tmp_model.encoder,
        "how do I cook rice tonight",
        list(trained_tmp_model.gold_questions),
        trained_tmp_model.gold_vectors,
    )
    # Off-subject may still have a nearest neighbor; serve must not snap it.
    assert rice_score < 0.86 or introduces_unrelated_subjects(
        "how do I cook rice tonight", rice
    )
