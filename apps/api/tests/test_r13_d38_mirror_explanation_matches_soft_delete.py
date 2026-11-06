"""D38 — the product explains mirror the way a mirror run behaves.

QA: explain_product said "mirror is upsert plus deletion — destination rows
whose key the source no longer has are removed, so the destination ends up
matching the source exactly", while a mirror run (soft delete, the owner's
round-8 default) kept those keys flagged ``_deleted``. The plan already warns
through ``services/mirror_delete_notice`` (the product's single mirror
wording); the explanation and the capability card now read from it too.
"""

from __future__ import annotations


def test_explanation_and_card_disclose_the_soft_delete_default():
    from services.mirror_delete_notice import MIRROR_EXPLAIN_TEXT, MIRROR_LEDGER_SOFT_TEXT
    from services.mirror_engine import resolve_mirror_delete_mode
    from src.ai.first_party.capability_contract import mirror_versus_upsert_card
    from src.ai.rag.product_facts import _SYNC_MODE_BEHAVIOUR as SYNC_MODE_FACTS

    assert resolve_mirror_delete_mode(None, {}) == "soft"
    fact = SYNC_MODE_FACTS["mirror"]
    card = mirror_versus_upsert_card()
    assert card is not None
    for text in (fact, card.text):
        assert "_deleted" in text and "soft-deleted by default" in text
        assert "delete_mode=hard" in text
        assert "are removed" not in text
    assert fact == MIRROR_EXPLAIN_TEXT
    # The explanation and the run's own ledger tell the same story.
    assert "_deleted" in MIRROR_LEDGER_SOFT_TEXT and "COUNT(*) does not drop" in MIRROR_LEDGER_SOFT_TEXT
