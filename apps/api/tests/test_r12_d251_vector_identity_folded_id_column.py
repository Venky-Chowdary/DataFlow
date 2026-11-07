"""D251 (prod job 6ae924a24fb8a205cd02ee12): Oracle QAENV_SEED -> new Weaviate
class left source_id '' on every object. No key was declared, so identity
fell back to a column named ``id`` — looked up case-sensitively, while the
Oracle reader hands back ``ID``. The identity check then counted 0 of 30
distinct source_ids and failed the run as possible silent loss.
"""
from __future__ import annotations

import pytest

_ORACLE_ROWS = [{"ID": i, "NAME": f"name_{i}"} for i in range(1, 4)]


@pytest.mark.parametrize("field", ["ID", "Id", "_ID", "SOURCE_ID"])
def test_fallback_identity_matches_folded_spelling(field):
    from services.vector_sync import vector_fallback_identity

    assert vector_fallback_identity({field: 7, "NAME": "x"}) == "7"


def test_exact_id_spelling_wins_over_a_folded_twin():
    from services.vector_sync import vector_fallback_identity

    assert vector_fallback_identity({"ID": 1, "id": 2}) == "2"


def test_record_key_without_declared_identity_reads_upper_case_id():
    from services.vector_sync import vector_record_key

    assert vector_record_key({"ID": 12, "NAME": "n"}) == "12"


def test_vectorized_rows_carry_source_id_from_upper_case_id():
    from services.vectorization import vectorize_records

    rows = vectorize_records(_ORACLE_ROWS, model="hash/32", skip_chunking=True)
    assert [r["source_id"] for r in rows] == ["1", "2", "3"]


def test_no_id_like_column_still_falls_back_to_empty():
    from services.vector_sync import vector_fallback_identity

    assert vector_fallback_identity({"NAME": "x"}) == ""
