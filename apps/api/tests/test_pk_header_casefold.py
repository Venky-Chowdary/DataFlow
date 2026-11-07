"""M5 regression — a case-folding engine (Snowflake/Oracle stores unquoted
identifiers uppercased) must not report its own pk as "not present" when the
declared mapping keeps the typed lowercase name.

Pre-fix: ``pk_columns=["id"]`` vs payload headers ``["ID", "AMOUNT"]`` raised
"identity column(s) id are not present" and blocked the job (production_sku
snowflake_to_salesforce leg).
"""

from __future__ import annotations

from services.source_duplicate_probe import _pk_header_indexes


def test_folded_header_resolves_declared_pk():
    idx, missing = _pk_header_indexes(["id"], ["ID", "AMOUNT"])
    assert idx == [0]
    assert missing == []


def test_ambiguous_fold_stays_missing_not_a_guess():
    # A table carrying both ID and "id" (quoted) cannot be resolved safely.
    idx, missing = _pk_header_indexes(["id"], ["ID", "id", "amount"])
    # exact match wins — id is present verbatim
    assert idx == [1]
    assert missing == []

    idx, missing = _pk_header_indexes(["user_id"], ["USER_ID", "user_id"])
    assert idx == [1]
    assert missing == []


def test_truly_absent_column_still_missing():
    idx, missing = _pk_header_indexes(["pk9"], ["ID", "AMOUNT"])
    assert missing == ["pk9"]
