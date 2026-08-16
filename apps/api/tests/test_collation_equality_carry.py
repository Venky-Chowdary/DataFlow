"""Collation equality is a uniqueness contract, not a DDL cosmetic.

AWS DMS lets the destination default collation decide whether Alpha and alpha
are the same row. These tests pin the equality-class planner: CS source UNIQUE
must land as a CS destination spelling (MySQL utf8mb4_bin), and a CI source
must not be claimed as carried on PostgreSQL.
"""

from __future__ import annotations

from services.collation_carry import classify_equality, plan_collation_carry
from services.schema_fidelity import SourceSchemaCatalog, plan_create_new_fidelity


def test_pg_default_is_case_sensitive_even_without_a_collation_name():
    eq = classify_equality("postgresql")
    assert eq.case == "sensitive"
    assert eq.accent == "sensitive"


def test_mysql_unicode_ci_is_case_and_accent_insensitive():
    eq = classify_equality("mysql", collation="utf8mb4_unicode_ci", charset="utf8mb4")
    assert eq.case == "insensitive"
    assert eq.accent == "insensitive"


def test_mysql_bin_is_case_sensitive():
    eq = classify_equality("mysql", collation="utf8mb4_bin")
    assert eq.case == "sensitive"
    assert eq.accent == "sensitive"


def test_sqlserver_ci_as_is_case_insensitive_accent_sensitive():
    eq = classify_equality("sqlserver", collation="Latin1_General_CI_AS")
    assert eq.case == "insensitive"
    assert eq.accent == "sensitive"


def test_pg_cs_unique_is_emitted_as_mysql_bin_not_dest_default_ci():
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "code"],
        column_types={"id": "BIGINT", "code": "TEXT"},
        primary_key=["id"],
        unique_keys=[["code"]],
    )
    plan = plan_collation_carry(
        catalog=catalog,
        dest_dialect="mysql",
        dest_name_for_source=lambda c: c,
        dest_type_for_column=lambda c: "BIGINT" if c == "id" else "VARCHAR(32)",
        unique_or_pk={"id", "code"},
    )
    by_col = {d.dest_column: d for d in plan.decisions}
    assert "code" in by_col
    decision = by_col["code"]
    assert decision.status == "carried"
    assert decision.uniqueness_polarity == "preserved"
    assert "utf8mb4_bin" in " ".join(decision.column_prefixes)
    assert "CHARACTER SET utf8mb4" in " ".join(decision.column_prefixes)
    assert "id" not in by_col


def test_mysql_ci_unique_is_not_claimed_carried_on_postgres():
    catalog = SourceSchemaCatalog(
        dialect="mysql",
        columns=["id", "code"],
        column_types={"id": "BIGINT", "code": "VARCHAR(32) COLLATE utf8mb4_unicode_ci"},
        collations={"code": "utf8mb4_unicode_ci"},
        charsets={"code": "utf8mb4"},
        primary_key=["id"],
        unique_keys=[["code"]],
    )
    plan = plan_collation_carry(
        catalog=catalog,
        dest_dialect="postgresql",
        dest_name_for_source=lambda c: c,
        dest_type_for_column=lambda c: "BIGINT" if c == "id" else "TEXT",
        unique_or_pk={"id", "code"},
    )
    decision = {d.dest_column: d for d in plan.decisions}["code"]
    assert decision.status == "unsupported"
    assert decision.uniqueness_polarity == "widened"
    assert "citext" in decision.reason.lower() or "case-sensitive" in decision.reason.lower()
    assert decision.column_prefixes == []


def test_sqlserver_ci_as_is_not_mapped_to_mysql_unicode_ci():
    """unicode_ci is accent-insensitive; CI_AS is not. A stand-in would equate café/cafe."""
    catalog = SourceSchemaCatalog(
        dialect="sqlserver",
        columns=["name"],
        column_types={"name": "NVARCHAR(50) COLLATE Latin1_General_CI_AS"},
        collations={"name": "Latin1_General_CI_AS"},
        unique_keys=[["name"]],
    )
    plan = plan_collation_carry(
        catalog=catalog,
        dest_dialect="mysql",
        dest_name_for_source=lambda c: c,
        dest_type_for_column=lambda _: "VARCHAR(50)",
        unique_or_pk={"name"},
    )
    decision = plan.decisions[0]
    assert decision.status == "unsupported"
    assert "accent" in decision.reason.lower()
    assert not decision.column_prefixes


def test_sqlite_nocase_is_not_a_unicode_ci_stand_in():
    catalog = SourceSchemaCatalog(
        dialect="mysql",
        columns=["code"],
        column_types={"code": "VARCHAR(32) COLLATE utf8mb4_unicode_ci"},
        collations={"code": "utf8mb4_unicode_ci"},
    )
    plan = plan_collation_carry(
        catalog=catalog,
        dest_dialect="sqlite",
        dest_name_for_source=lambda c: c,
        dest_type_for_column=lambda _: "TEXT",
        unique_or_pk={"code"},
    )
    assert plan.decisions[0].status == "unsupported"
    assert "NOCASE" in plan.decisions[0].reason


def test_create_new_plan_prepends_collate_before_not_null():
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "code"],
        column_types={"id": "BIGINT", "code": "TEXT"},
        nullable={"id": False, "code": False},
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
    frags = plan.column_suffixes.get("code") or []
    joined = " ".join(frags)
    assert "COLLATE utf8mb4_bin" in joined
    assert "CHARACTER SET utf8mb4" in joined
    # Type-adjacent: charset/collate before NOT NULL (MySQL grammar).
    assert frags.index("CHARACTER SET utf8mb4") < frags.index("NOT NULL")
    statuses = {i.status for i in plan.report.items if i.aspect == "collation"}
    assert "carried" in statuses
