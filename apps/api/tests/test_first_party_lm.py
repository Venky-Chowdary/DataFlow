"""First-party Transformer LM: real decoder, fail-closed, not a foundation model."""

from __future__ import annotations

from src.ai.first_party.claims import invented_claims
from src.ai.first_party.context_pack import pack_context, utc_today_spoken
from src.ai.first_party.lm_checkpoint import default_lm_path, load_lm
from src.ai.first_party.transformer_lm import Vocab, encode_example


def test_context_pack_uses_stable_keys() -> None:
    text = pack_context(
        today_utc="Sunday, 13 September 2026",
        pipeline_count=2,
        parked_count=2,
        parked_names=["Nightly"],
    )
    assert "TODAY_UTC:" in text
    assert "PIPELINES: 2" in text
    assert "Nightly" in text
    assert utc_today_spoken()


def test_vocab_encodes_and_constrains_to_evidence() -> None:
    vocab = Vocab.build(["Today is Sunday", "You have 2 pipelines parked"])
    ids = vocab.encode_words("Today is Sunday.")
    assert ids
    assert vocab.decode(ids).lower().startswith("today")
    allowed = vocab.allowed_ids("Today is Sunday")
    assert vocab.token_to_id["today"] in allowed
    assert vocab.token_to_id.get("pipelines") not in allowed


def test_encode_example_is_q_e_a() -> None:
    vocab = Vocab.build(["do you support automl automl is false"])
    ids = encode_example(
        vocab,
        "do you support automl",
        "automl is false",
        "automl is false",
    )
    assert ids[0] == 3  # <q>
    assert 4 in ids and 5 in ids


def test_shipped_lm_is_a_real_decoder_not_a_prefix_table() -> None:
    path = default_lm_path()
    if not path.is_file():
        return
    lm = load_lm(path)
    assert lm.vocab.size > 40
    assert lm.decoder.hid >= 16
    assert lm.decoder.tok_emb.ndim == 2
    ctx = pack_context(today_utc="Sunday, 13 September 2026")
    spoken = lm.generate_grounded("date today", ctx)
    assert spoken
    assert "today" in spoken.lower()
    assert "2026" in spoken
    assert invented_claims("We run dbt and exactly-once CDC.", ctx) == (
        "dbt",
        "exactly-once",
    )
    assert invented_claims(spoken, ctx + " " + spoken) == ()
