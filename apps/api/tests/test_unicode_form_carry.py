"""Unicode form identity is NFC vs NFD plus UCA version, not CS/CI.

general_ci UNIQUE accepts both NFC café and NFD café. unicode_ci may not.
utf8mb4_unicode_ci is not utf8mb4_0900_ai_ci. Bind must not NFC to hide it.
"""

from __future__ import annotations

import unicodedata

from services.schema_fidelity import SourceSchemaCatalog, plan_create_new_fidelity
from services.unicode_form import (
    NFC_CAFE,
    NFC_CAFE_UTF8_HEX,
    NFD_CAFE,
    NFD_CAFE_UTF8_HEX,
    SHARP_S,
    SS_EXPANSION,
    classify_form,
    classify_uca,
    decide_unicode_form,
    utf8_form_hex,
    unique_second_outcome,
)


def test_nfc_and_nfd_cafe_are_distinct_python_strings_and_hex():
    assert NFC_CAFE != NFD_CAFE
    assert classify_form(NFC_CAFE) == "nfc"
    assert classify_form(NFD_CAFE) == "nfd"
    assert utf8_form_hex(NFC_CAFE) == NFC_CAFE_UTF8_HEX
    assert utf8_form_hex(NFD_CAFE) == NFD_CAFE_UTF8_HEX
    assert unicodedata.normalize("NFC", NFD_CAFE) == NFC_CAFE
    # Classifier never rewrites the cell.
    assert NFD_CAFE == "cafe\u0301"


def test_ascii_has_identity_form_not_nfc_claim():
    assert classify_form("cafe") == "identity"
    assert classify_form("") == "identity"
    mixed = NFC_CAFE + NFD_CAFE
    assert classify_form(mixed) == "mixed"


def test_bin_and_pg_default_are_codepoint_identity():
    pg = classify_uca("postgresql", "")
    assert pg.table == "codepoint"
    assert pg.folds_forms is False
    bin_p = classify_uca("mysql", "utf8mb4_bin")
    assert bin_p.table == "codepoint"
    assert bin_p.canonical_equivalence is False


def test_general_ci_is_not_uca_and_does_not_fold_forms():
    gen = classify_uca("mariadb", "utf8mb4_general_ci")
    assert gen.table == "general"
    assert gen.expansions is False
    assert gen.canonical_equivalence is False
    assert gen.folds_forms is False


def test_unicode_ci_uca_4_is_engine_specific_for_nfc():
    my = classify_uca("mysql", "utf8mb4_unicode_ci")
    assert my.table == "uca" and my.version == "4.0"
    assert my.expansions is True
    assert my.canonical_equivalence is None
    assert my.folds_forms is True
    maria = classify_uca("mariadb", "utf8mb4_unicode_ci")
    assert maria.canonical_equivalence is True
    assert maria.expansions is True


def test_uca_versions_are_not_interchangeable():
    v520 = classify_uca("mysql", "utf8mb4_unicode_520_ci")
    v900 = classify_uca("mysql", "utf8mb4_0900_ai_ci")
    v1400 = classify_uca("mysql", "utf8mb4_uca1400_ai_ci")
    assert v520.version == "5.2"
    assert v900.version == "9.0"
    assert v1400.version == "14.0"
    assert v900.canonical_equivalence is True


def test_general_ci_to_unicode_ci_is_unsupported_not_carried():
    decision = decide_unicode_form(
        source_engine="mariadb",
        source_collation="utf8mb4_general_ci",
        dest_engine="mariadb",
        dest_collation="utf8mb4_unicode_ci",
        source_column="code",
        dest_column="code",
        source_type="VARCHAR(32)",
        dest_type="VARCHAR(32)",
    )
    assert decision is not None
    assert decision.status == "unsupported"
    assert decision.uniqueness == "collapsed"


def test_pg_cs_to_mysql_bin_is_carried():
    decision = decide_unicode_form(
        source_engine="postgresql",
        source_collation="",
        dest_engine="mysql",
        dest_collation="utf8mb4_bin",
        source_column="code",
        dest_column="code",
        source_type="TEXT",
        dest_type="VARCHAR(32)",
    )
    assert decision is not None
    assert decision.status == "carried"
    assert decision.uniqueness == "preserved"


def test_unicode_ci_is_not_unicode_520():
    decision = decide_unicode_form(
        source_engine="mysql",
        source_collation="utf8mb4_unicode_ci",
        dest_engine="mysql",
        dest_collation="utf8mb4_unicode_520_ci",
        source_column="code",
        dest_column="code",
        source_type="VARCHAR(32)",
        dest_type="VARCHAR(32)",
    )
    assert decision is not None
    assert decision.status == "unsupported"
    assert "4.0" in decision.reason and "5.2" in decision.reason


def test_0900_is_not_1400():
    decision = decide_unicode_form(
        source_engine="mysql",
        source_collation="utf8mb4_0900_ai_ci",
        dest_engine="mysql",
        dest_collation="utf8mb4_uca1400_ai_ci",
        source_column="code",
        dest_column="code",
        source_type="VARCHAR(32)",
        dest_type="VARCHAR(32)",
    )
    assert decision is not None
    assert decision.status == "unsupported"


def test_mysql_unicode_ci_is_not_mariadb_unicode_ci():
    decision = decide_unicode_form(
        source_engine="mysql",
        source_collation="utf8mb4_unicode_ci",
        dest_engine="mariadb",
        dest_collation="utf8mb4_unicode_ci",
        source_column="code",
        dest_column="code",
        source_type="VARCHAR(32)",
        dest_type="VARCHAR(32)",
    )
    assert decision is not None
    assert decision.status == "unsupported"


def test_same_engine_unicode_ci_is_carried():
    decision = decide_unicode_form(
        source_engine="mariadb",
        source_collation="utf8mb4_unicode_ci",
        dest_engine="mariadb",
        dest_collation="utf8mb4_unicode_ci",
        source_column="code",
        dest_column="code",
        source_type="VARCHAR(32)",
        dest_type="VARCHAR(32)",
    )
    assert decision is not None
    assert decision.status == "carried"


def test_uca_source_to_pg_codepoint_is_unsupported():
    decision = decide_unicode_form(
        source_engine="mysql",
        source_collation="utf8mb4_unicode_ci",
        dest_engine="postgresql",
        dest_collation="",
        source_column="code",
        dest_column="code",
        source_type="VARCHAR(32)",
        dest_type="TEXT",
    )
    assert decision is not None
    assert decision.status == "unsupported"
    assert decision.uniqueness == "split"


def test_integer_column_has_no_unicode_form_decision():
    decision = decide_unicode_form(
        source_engine="postgresql",
        source_collation="",
        dest_engine="mysql",
        dest_collation="",
        source_column="id",
        dest_column="id",
        source_type="BIGINT",
        dest_type="BIGINT",
    )
    assert decision is None


def test_mysql_empty_collation_is_unknown_not_a_guessed_0900():
    profile = classify_uca("mysql", "")
    assert profile.table == "unknown"
    decision = decide_unicode_form(
        source_engine="mysql",
        source_collation="",
        dest_engine="mysql",
        dest_collation="utf8mb4_bin",
        source_column="code",
        dest_column="code",
        source_type="VARCHAR(32)",
        dest_type="VARCHAR(32)",
    )
    assert decision is not None
    assert decision.status == "unsupported"


def test_unique_second_classifier():
    assert unique_second_outcome(first_ok=True, second_ok=True) == "BOTH_LAND"
    assert unique_second_outcome(first_ok=True, second_ok=False) == "SECOND_REJECT"
    assert unique_second_outcome(first_ok=False, second_ok=False) == "FIRST_REJECT"
    assert SHARP_S != SS_EXPANSION


def test_create_new_pg_text_to_mysql_carries_form_on_bin():
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "code"],
        column_types={"id": "BIGINT", "code": "TEXT"},
        primary_key=["id"],
        unique_keys=[["code"]],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["id", "code"],
        target_types=["BIGINT", "VARCHAR(32)"],
        source_to_target={"id": "id", "code": "code"},
    )
    items = [i for i in plan.report.items if i.aspect == "unicode_form"]
    assert items
    assert any(i.status == "carried" for i in items)
    coll = [i for i in plan.report.items if i.aspect == "collation"]
    assert any("utf8mb4_bin" in (i.dest_ddl or "") for i in coll)


def test_create_new_general_ci_onto_unicode_ci_type_is_unsupported():
    catalog = SourceSchemaCatalog(
        dialect="mariadb",
        columns=["id", "code"],
        column_types={"id": "BIGINT", "code": "VARCHAR(32)"},
        collations={"code": "utf8mb4_general_ci"},
        charsets={"code": "utf8mb4"},
        primary_key=["id"],
        unique_keys=[["code"]],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["id", "code"],
        target_types=["BIGINT", "VARCHAR(32) COLLATE utf8mb4_unicode_ci"],
        source_to_target={"id": "id", "code": "code"},
    )
    items = [i for i in plan.report.items if i.aspect == "unicode_form"]
    assert items
    assert any(i.status == "unsupported" for i in items)
