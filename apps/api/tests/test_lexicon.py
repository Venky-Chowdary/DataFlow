"""Training lexicon tests."""

from services.training_lexicon import _norm, load_training_lexicon


def test_lexicon_loads_or_empty():
    lex = load_training_lexicon()
    assert isinstance(lex, dict)


def test_norm():
    assert _norm("CUST_ID") == "cust_id"
