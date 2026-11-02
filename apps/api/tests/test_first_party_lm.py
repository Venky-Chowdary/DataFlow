"""First-party attention+copy LM: real decoder, fail-closed, not a foundation model."""

from __future__ import annotations

from src.ai.first_party.claims import invented_claims
from src.ai.first_party.context_pack import pack_context, utc_today_spoken
from src.ai.first_party.engine import speak_with_lm, tokens_grounded_in_dialogue
from src.ai.first_party.lm_checkpoint import default_lm_path, load_lm
from src.ai.first_party.seq2seq_lm import AttentionCopyDecoder
from src.ai.first_party.transformer_lm import Vocab, encode_example


def test_context_pack_uses_stable_keys() -> None:
    text = pack_context(
        today_utc="Sunday, 13 September 2026",
        pipeline_count=2,
        parked_count=2,
        parked_names=["Nightly"],
        create_connection="confirm_gated",
    )
    assert "TODAY_UTC:" in text
    assert "PIPELINES: 2" in text
    assert "Nightly" in text
    assert "Confirm" in text
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


def test_decoder_is_attention_copy_not_a_prefix_table() -> None:
    dec = AttentionCopyDecoder.init(48, hid=16, seed=3)
    assert dec.wa.shape == (16, 16)
    assert dec.wp.shape == (48,)
    assert dec.uz.ndim == 2


def test_dialogue_grounding_allows_confirm_but_not_dbt() -> None:
    pack = pack_context(create_connection="confirm_gated", connector_count=12)
    assert tokens_grounded_in_dialogue(
        "Yes. Paste a host and I will stage Confirm. You have 12 saved connectors.",
        pack,
    )
    assert not tokens_grounded_in_dialogue("We run dbt models after the load.", pack)


def test_shipped_lm_copies_date_pipelines_and_confirm() -> None:
    path = default_lm_path()
    if not path.is_file():
        return
    lm = load_lm(path)
    assert lm.vocab.size > 40
    assert lm.decoder.hid >= 16
    assert lm.decoder.wa.ndim == 2
    date_ctx = pack_context(today_utc="Sunday, 13 September 2026")
    spoken = lm.generate_grounded("date today", date_ctx)
    assert spoken
    assert "2026" in spoken
    assert "sunday" in spoken.lower() or "september" in spoken.lower()
    other = lm.generate_grounded(
        "what is todays date",
        pack_context(today_utc="Monday, 01 January 2026"),
    ).lower()
    assert "2026" in other
    assert "monday" in other or "january" in other
    assert invented_claims("We run dbt and exactly-once CDC.", date_ctx) == (
        "dbt",
        "exactly-once",
    )
    assert invented_claims(spoken, date_ctx + " " + spoken) == ()

    sched_ctx = pack_context(
        pipeline_count=2,
        parked_count=2,
        parked_names=["Nightly", "TestJob"],
        enabled_count=0,
    )
    sched = lm.generate_grounded("is schedules working", sched_ctx).lower()
    assert "pipeline" in sched
    assert "2" in sched

    create_ctx = pack_context(connector_count=12, create_connection="confirm_gated")
    created = lm.generate_grounded("can you create connection", create_ctx).lower()
    assert "confirm" in created or "stage" in created
    assert "12" in created
    created5 = lm.generate_grounded(
        "can you create connection",
        pack_context(connector_count=5, create_connection="confirm_gated"),
    ).lower()
    assert "5" in created5
    assert "12" not in created5


def test_speak_with_lm_fail_closes_on_invented_claims(monkeypatch) -> None:
    class _Lie:
        def generate_grounded(self, question: str, evidence: str) -> str:
            return "We provide exactly-once CDC with dbt."

    monkeypatch.setattr(
        "src.ai.first_party.engine.load_lm_model", lambda path=None: _Lie()
    )
    assert speak_with_lm("is cdc exactly once", "at-least-once on _df_lsn") is None
