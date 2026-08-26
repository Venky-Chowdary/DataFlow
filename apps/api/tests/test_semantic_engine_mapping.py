"""Order-independence and fuzzy-match coverage for the AI semantic engine mapper.

The engine behind ``/api/v1/ai/map`` must assign columns globally (a weaker
earlier column may not steal a target that is a stronger match for a later one)
and must recover from typos / abbreviations via character-level similarity.
"""

from ai.semantic_engine import analyze_column, generate_mappings


def _by_source(mappings):
    return {m.source_column: m for m in mappings}


def test_exact_match_wins_regardless_of_source_order():
    # `name` would grab `full_name` under naive greedy, leaving the real
    # `full_name` unmapped. Global assignment must keep exact matches intact.
    mappings = generate_mappings(
        ["name", "full_name"],
        ["full_name"],
    )
    by_source = _by_source(mappings)
    assert by_source["full_name"].target_column == "full_name"
    assert by_source["name"].target_column == "<unmapped>"


def test_no_duplicate_targets():
    mappings = generate_mappings(
        ["customer_id", "order_id"],
        ["customer_id", "order_id"],
    )
    targets = [m.target_column for m in mappings if m.target_column != "<unmapped>"]
    assert len(targets) == len(set(targets))


def test_typo_recovers_via_char_similarity():
    # `custmer_id` (typo) shares tokens `id` only, but is a near-exact string
    # match for `customer_id` — char similarity should map it confidently.
    mappings = generate_mappings(["custmer_id"], ["customer_id", "order_total"])
    by_source = _by_source(mappings)
    assert by_source["custmer_id"].target_column == "customer_id"
    assert by_source["custmer_id"].confidence > 0.5


def test_analyze_column_does_not_invent_datetime_from_auto_ambiguous_slash_dates():
    """01/02/2024 is Jan 2 or Feb 1 — Auto must not label the column datetime."""
    ambiguous = analyze_column("event_date", ["01/02/2024", "03/04/2024"])
    assert ambiguous.inferred_type == "string"
    unambiguous = analyze_column("event_date", ["31/12/2024", "30/11/2024"])
    assert unambiguous.inferred_type == "datetime"
    iso = analyze_column("event_date", ["2024-03-05", "2024-03-06"])
    assert iso.inferred_type == "datetime"


def test_deterministic_output():
    a = generate_mappings(["a_id", "b_id"], ["b_id", "a_id"])
    b = generate_mappings(["a_id", "b_id"], ["b_id", "a_id"])
    assert [(m.source_column, m.target_column) for m in a] == [
        (m.source_column, m.target_column) for m in b
    ]
