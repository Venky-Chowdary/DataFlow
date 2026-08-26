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
    assert "standardize_iso8601" not in ambiguous.suggested_transformations
    assert any("date locale" in w.lower() for w in ambiguous.warnings)
    unambiguous = analyze_column("event_date", ["31/12/2024", "30/11/2024"])
    assert unambiguous.inferred_type == "datetime"
    assert "standardize_iso8601" in unambiguous.suggested_transformations
    iso = analyze_column("event_date", ["2024-03-05", "2024-03-06"])
    assert iso.inferred_type == "datetime"
    assert "standardize_iso8601" in iso.suggested_transformations


def test_generate_mappings_does_not_invent_date_transform_for_ambiguous_slash():
    mappings = generate_mappings(
        ["event_date"],
        ["event_date"],
        {"event_date": ["01/02/2024", "03/04/2024"]},
    )
    by = _by_source(mappings)
    assert by["event_date"].target_column == "event_date"
    assert by["event_date"].suggested_transformation is None
    assert by["event_date"].transformation_needed is False


def test_reasoning_chain_does_not_invent_date_iso_from_auto_ambiguous_slash_dates():
    from src.ai.llm.chain import DataTransferReasoningChain

    chain = DataTransferReasoningChain()
    ambiguous = chain.analyze_column("event_date", ["01/02/2024", "03/04/2024"])
    assert ambiguous.answer["inferred_type"] == "string"
    assert "standardize_iso8601" not in (ambiguous.answer.get("transformations") or [])
    iso = chain.analyze_column("event_date", ["2024-03-05", "2024-03-06"])
    assert iso.answer["inferred_type"] == "date"
    assert "standardize_iso8601" in (iso.answer.get("transformations") or [])


def test_reasoning_chain_map_does_not_invent_parse_date_for_ambiguous_slash():
    from src.ai.llm.chain import DataTransferReasoningChain

    chain = DataTransferReasoningChain()
    result = chain.map_columns(
        ["event_date"],
        ["event_date"],
        source_samples={"event_date": ["01/02/2024", "03/04/2024"]},
    )
    row = result.answer["mappings"][0]
    assert row["target_column"] == "event_date"
    assert row["suggested_transformation"] is None
    assert row["transformation_needed"] is False


def test_deterministic_output():
    a = generate_mappings(["a_id", "b_id"], ["b_id", "a_id"])
    b = generate_mappings(["a_id", "b_id"], ["b_id", "a_id"])
    assert [(m.source_column, m.target_column) for m in a] == [
        (m.source_column, m.target_column) for m in b
    ]
