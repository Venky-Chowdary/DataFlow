"""A source CHECK must be re-enforced on the destination or named as lost."""

from __future__ import annotations

from services.check_constraints import (
    CheckConstraint,
    CheckConstraints,
    is_not_null_echo,
    plan_check_carry,
    render_check_for_dialect,
)
from services.schema_fidelity import SourceSchemaCatalog, plan_create_new_fidelity


def _quote(ident: str, dialect: str) -> str:
    return f"`{ident}`" if dialect == "mysql" else f'"{ident}"'


def _render(predicate: str, *, src: str = "postgresql", dest: str = "postgresql", cols=None):
    columns = cols or {"qty": "qty", "status": "status", "name": "name"}
    return render_check_for_dialect(
        predicate,
        source_dialect=src,
        dest_dialect=dest,
        column_map=columns,
        quote=_quote,
    )


def test_comparison_predicate_is_rerendered_with_dest_quoting():
    sql, reason = _render("qty > 0", dest="mysql")
    assert reason == ""
    assert sql == "`qty` > 0"


def test_renamed_column_follows_the_map_contract():
    sql, reason = _render("qty > 0", cols={"qty": "quantity"})
    assert reason == ""
    assert sql == '"quantity" > 0'


def test_predicate_on_an_unmapped_column_is_refused_not_dropped():
    sql, reason = _render("shipped_at IS NOT NULL")
    assert sql == ""
    assert "unmapped column" in reason


def test_in_list_and_boolean_operators_survive():
    sql, reason = _render("status IN ('a', 'b') AND qty >= 1")
    assert reason == ""
    assert sql == "\"status\" IN ('a', 'b') AND \"qty\" >= 1"


def test_subquery_check_is_refused():
    sql, reason = _render("qty > (SELECT MAX(qty) FROM other)")
    assert sql == ""
    assert "Subquery" in reason


def test_unknown_function_is_refused_rather_than_guessed():
    sql, reason = _render("regexp_like(name, '^a')")
    assert sql == ""
    assert "not on the portable CHECK whitelist" in reason


def test_char_length_is_respelled_per_dialect():
    assert _render("length(name) > 2", dest="sqlserver")[0] == 'LEN("name") > 2'
    assert _render("length(name) > 2", dest="mysql")[0] == "CHAR_LENGTH(`name`) > 2"
    assert _render("length(name) > 2", dest="oracle")[0] == 'LENGTH("name") > 2'


def test_mysql_byte_length_is_not_carried_as_char_length():
    """MySQL LENGTH() counts bytes; the same text would pass elsewhere."""
    sql, reason = _render("length(name) > 2", src="mysql", dest="postgresql")
    assert sql == ""
    assert "bytes" in reason


def test_boolean_literal_is_not_substituted_for_engines_without_one():
    sql, reason = _render("status = TRUE", dest="oracle")
    assert sql == ""
    assert "boolean literal" in reason


def test_statement_terminator_in_a_predicate_is_refused():
    sql, reason = _render("qty > 0); DROP TABLE users --")
    assert sql == ""
    assert reason


def test_postgres_any_array_storage_is_read_as_an_in_list():
    """PG stores IN(...) as ANY(ARRAY[...]) over text casts; same rule."""
    stored = "((status)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[]))"
    sql, reason = _render(stored)
    assert reason == ""
    assert sql == "(\"status\" IN ('a', 'b'))"


def test_mysql_backslash_escaping_and_charset_introducers_are_undone():
    stored = "(`status` in (_utf8mb4\\'a\\',_utf8mb4\\'b\\'))"
    sql, reason = _render(stored, src="mysql", dest="postgresql")
    assert reason == ""
    assert sql == "(\"status\" IN ('a', 'b'))"


def test_mysql_embedded_quote_is_respelled_the_standard_way():
    stored = "(`name` <> _utf8mb4\\'it\\\\\\'s\\')"
    sql, reason = _render(stored, src="mysql", dest="postgresql")
    assert reason == ""
    assert sql == "(\"name\" <> 'it''s')"


def test_non_text_cast_is_still_refused_after_normalization():
    sql, reason = _render("(qty)::numeric > 0")
    assert sql == ""
    assert "cast" in reason


def test_not_null_echo_is_recognised():
    assert is_not_null_echo('"qty" IS NOT NULL')
    assert is_not_null_echo("(qty IS NOT NULL)")
    assert not is_not_null_echo("qty > 0")


def test_unavailable_catalog_yields_no_carry_decisions():
    checks = CheckConstraints("postgresql", "unavailable", detail="no privilege")
    assert plan_check_carry(checks, dest_dialect="mysql", column_map={}, quote=_quote) == []


def _catalog(**kw) -> SourceSchemaCatalog:
    base = dict(
        dialect="postgresql",
        columns=["id", "qty"],
        column_types={"id": "INTEGER", "qty": "INTEGER"},
        nullable={"id": False, "qty": True},
    )
    base.update(kw)
    return SourceSchemaCatalog(**base)


def _check_items(plan):
    return [i for i in plan.report.items if i.aspect == "check"]


def test_measured_check_is_emitted_into_create_table():
    catalog = _catalog(
        check_constraints_meta=CheckConstraints(
            "postgresql",
            "measured",
            items=(CheckConstraint("qty_positive", "qty > 0", ("qty",)),),
        ).to_dict()
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="postgresql",
        target_columns=["id", "qty"],
        target_types=["INTEGER", "INTEGER"],
    )
    assert 'CHECK ("qty" > 0)' in plan.table_constraints
    items = _check_items(plan)
    assert [i.status for i in items] == ["carried"]
    assert items[0].name == "qty_positive"


def test_measured_absence_is_skipped_not_unsupported():
    catalog = _catalog(
        check_constraints_meta=CheckConstraints("postgresql", "measured").to_dict()
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="postgresql",
        target_columns=["id", "qty"],
        target_types=["INTEGER", "INTEGER"],
    )
    items = _check_items(plan)
    assert [i.status for i in items] == ["skipped"]


def test_unreadable_catalog_is_unknown_never_absent():
    catalog = _catalog(
        check_constraints_meta=CheckConstraints(
            "postgresql", "unavailable", detail="permission denied"
        ).to_dict()
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="postgresql",
        target_columns=["id", "qty"],
        target_types=["INTEGER", "INTEGER"],
    )
    items = _check_items(plan)
    assert [i.status for i in items] == ["unknown"]
    assert "permission denied" in items[0].reason
    assert not any(c.startswith("CHECK") for c in plan.table_constraints)


def test_unportable_check_is_reported_unsupported_and_not_emitted():
    catalog = _catalog(
        check_constraints_meta=CheckConstraints(
            "postgresql",
            "measured",
            items=(CheckConstraint("c1", "qty > (SELECT 1)", ("qty",)),),
        ).to_dict()
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["id", "qty"],
        target_types=["INTEGER", "INTEGER"],
    )
    items = _check_items(plan)
    assert [i.status for i in items] == ["unsupported"]
    assert not any(c.startswith("CHECK") for c in plan.table_constraints)


def test_no_catalog_payload_keeps_the_refuse_to_certify_absence_line():
    plan = plan_create_new_fidelity(
        _catalog(),
        dest_dialect="postgresql",
        target_columns=["id", "qty"],
        target_types=["INTEGER", "INTEGER"],
    )
    items = _check_items(plan)
    assert [i.status for i in items] == ["unsupported"]
    assert "refuse to certify absence" in items[0].reason
