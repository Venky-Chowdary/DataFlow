"""A source DEFAULT must survive the engine's own spelling of it.

MySQL stores a literal default as its bare *value* (``active``); MariaDB stores
an *expression* (``'active'``). The create-new planner only re-emits defaults it
can prove are safe literals, and it reasons about SQL text — so before this was
normalized, every MySQL string/date default was refused as "not on the safe
literal whitelist" and the destination table was created without it. Rows land,
checksums match, and the client's first application INSERT that omits the column
stores the wrong value.
"""

from __future__ import annotations

from services.catalog_defaults import normalize_catalog_default
from services.schema_fidelity import (
    SourceSchemaCatalog,
    is_safe_default_expr,
    plan_create_new_fidelity,
)


def _mysql(raw, *, data_type="varchar(32)", extra=""):
    return normalize_catalog_default("mysql", raw, data_type=data_type, extra=extra)


# --------------------------------------------------------------- MySQL bare values


def test_mysql_bare_string_default_becomes_a_sql_literal():
    assert _mysql("active") == "'active'"
    assert is_safe_default_expr(_mysql("active") or "")


def test_mysql_bare_value_with_a_quote_is_escaped_not_injected():
    assert _mysql("O'Brien") == "'O''Brien'"
    assert is_safe_default_expr(_mysql("O'Brien") or "")


def test_mysql_numeric_default_on_a_string_column_stays_a_string():
    # ``code VARCHAR(8) DEFAULT '42'`` must not become the number 42.
    assert _mysql("42", data_type="varchar(8)") == "'42'"


def test_mysql_numeric_default_on_a_numeric_column_stays_numeric():
    assert _mysql("42", data_type="int") == "42"
    assert _mysql("-1.5", data_type="decimal(10,2)") == "-1.5"


def test_mysql_empty_string_default_is_a_default_not_absence():
    assert _mysql("") == "''"
    assert _mysql(None) is None


def test_mysql_clock_default_is_canonical_sql():
    assert _mysql("CURRENT_TIMESTAMP", data_type="datetime") == "CURRENT_TIMESTAMP"
    assert _mysql("current_timestamp()", data_type="datetime") == "CURRENT_TIMESTAMP"
    assert _mysql("curdate()", data_type="date") == "CURRENT_DATE"


def test_mysql_expression_default_is_never_requoted_into_a_literal():
    # MySQL 8 marks expression defaults in EXTRA. Quoting one would store the
    # text of the expression instead of evaluating it.
    expr = "(uuid())"
    assert _mysql(expr, extra="DEFAULT_GENERATED") == expr
    # And an unmarked parenthesised form is still SQL, not a value.
    assert _mysql("(1 + 1)", data_type="int") == "(1 + 1)"


# ------------------------------------------------------------- MariaDB expressions


def test_mariadb_quoted_literal_passes_through_unchanged():
    assert normalize_catalog_default("mariadb", "'active'") == "'active'"


def test_mariadb_null_default_is_sql_null():
    assert normalize_catalog_default("mariadb", "NULL") == "NULL"


def test_non_mysql_dialects_are_left_alone():
    # PostgreSQL already hands back an expression, cast included.
    assert normalize_catalog_default("postgresql", "'active'::text") == "'active'::text"
    assert normalize_catalog_default("sqlite", "(datetime('now'))") == "(datetime('now'))"
    assert normalize_catalog_default("postgresql", None) is None


# ------------------------------------------------------------------ planner effect


def _plan(defaults: dict[str, str], *, dialect: str = "mysql", dest: str = "mysql"):
    catalog = SourceSchemaCatalog(
        dialect=dialect,
        columns=["id", "status", "created_at"],
        column_types={"id": "BIGINT", "status": "VARCHAR(32)", "created_at": "DATETIME(6)"},
        nullable={"id": False, "status": False, "created_at": False},
        defaults=defaults,
        primary_key=["id"],
    )
    return plan_create_new_fidelity(
        catalog,
        dest_dialect=dest,
        target_columns=list(catalog.columns),
        target_types=[catalog.column_types[c] for c in catalog.columns],
        source_to_target={c: c for c in catalog.columns},
        dest_table="t_defaults",
    )


def _default_items(plan):
    return {i.name: i for i in plan.report.items if i.aspect == "default"}


def test_normalized_mysql_default_is_carried_onto_create_new():
    plan = _plan(
        {
            "status": _mysql("active") or "",
            "created_at": _mysql("CURRENT_TIMESTAMP", data_type="datetime") or "",
        }
    )
    items = _default_items(plan)
    assert items["status"].status == "carried", items["status"].reason
    assert plan.column_defaults["status"] == "'active'"
    assert items["created_at"].status == "carried", items["created_at"].reason
    assert plan.column_defaults["created_at"] == "CURRENT_TIMESTAMP"


def test_raw_mysql_bare_default_would_have_been_refused():
    # Guards the regression itself: the un-normalized form is exactly what the
    # whitelist must keep refusing, so normalization has to happen upstream.
    assert not is_safe_default_expr("active")
    items = _default_items(_plan({"status": "active"}))
    assert items["status"].status == "unsupported"


def test_unsafe_expression_is_still_refused_after_normalization():
    plan = _plan({"status": "(SELECT max(x) FROM t)"})
    items = _default_items(plan)
    assert items["status"].status == "unsupported"
    assert "status" not in plan.column_defaults
