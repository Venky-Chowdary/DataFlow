"""Accuracy review finding 10: a drift rename needs a pair Map would not send to review."""

from __future__ import annotations

from services.schema_drift import _semantic_rename_pairs


def test_a_review_flagged_mapping_is_not_a_rename(monkeypatch) -> None:
    import services.semantic_mapper as sm

    monkeypatch.setattr(sm, "map_columns", lambda dropped, added, **k: [
        {"source": "qty_ord", "target": "order_quantity", "confidence": 0.84, "requires_review": True}])
    _d, _a, pairs = _semantic_rename_pairs(["qty_ord"], ["order_quantity"], {"qty_ord": "INTEGER"},
                                           {"order_quantity": "INTEGER"})
    assert pairs == []


def test_an_inferred_abbreviation_is_not_a_rename() -> None:
    _d, _a, pairs = _semantic_rename_pairs(["cat"], ["category"], {"cat": "VARCHAR"}, {"category": "VARCHAR"})
    assert pairs == []


def test_a_clear_rename_still_pairs() -> None:
    _d, _a, pairs = _semantic_rename_pairs(["cust_id"], ["customer_id"], {"cust_id": "INTEGER"},
                                           {"customer_id": "INTEGER"})
    assert pairs == [("cust_id", "customer_id")]
