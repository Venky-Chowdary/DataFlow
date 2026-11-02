"""Secondary indexes must be carried, or refused with a reason — never assumed absent."""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.sql_identifiers import quote_sql_identifier  # noqa: E402
from services.schema_fidelity import (  # noqa: E402
    SourceSchemaCatalog,
    apply_post_create_sql,
    plan_create_new_fidelity,
)
from services.secondary_indexes import (  # noqa: E402
    IndexColumn,
    SourceIndex,
    SourceIndexes,
    _split_index_key,
    plan_index_carry,
    probe_secondary_indexes,
)


def _q(ident: str) -> str:
    return quote_sql_identifier(ident, '"')


def _measured(*items: SourceIndex, dialect: str = "postgresql") -> SourceIndexes:
    return SourceIndexes(dialect=dialect, status="measured", items=items)


def _plan(indexes: SourceIndexes, dest: str = "postgresql", **kw):
    params = {
        "dest_dialect": dest,
        "dest_table": "orders",
        "column_map": {"status": "status", "created_at": "created_at", "id": "id"},
        "quote": _q,
    }
    params.update(kw)
    return plan_index_carry(indexes, **params)


def test_index_key_parse_keeps_operator_class_and_rejects_expressions():
    assert _split_index_key("status") == ("status", "")
    assert _split_index_key("status DESC") == ("status", "")
    assert _split_index_key("status varchar_pattern_ops") == ("status", "varchar_pattern_ops")
    assert _split_index_key('"Code" text_pattern_ops') == ("Code", "text_pattern_ops")
    assert _split_index_key("lower(status)") is None


def test_plain_composite_index_is_carried_with_column_order():
    idx = SourceIndex(
        name="idx_status_created",
        columns=(IndexColumn("status"), IndexColumn("created_at", descending=True)),
    )
    [d] = _plan(_measured(idx))
    assert d.carried
    assert d.dest_sql == (
        'CREATE INDEX "orders_idx_status_created" ON "orders" '
        '("status", "created_at" DESC)'
    )


def test_unique_index_keeps_its_uniqueness():
    [d] = _plan(_measured(SourceIndex("uq_status", (IndexColumn("status"),), unique=True)))
    assert d.carried
    assert d.dest_sql.startswith('CREATE UNIQUE INDEX ')


def test_expression_index_is_refused_not_approximated():
    idx = SourceIndex("idx_lower", (IndexColumn("status"),), expression="lower(status)")
    [d] = _plan(_measured(idx))
    assert not d.carried and not d.skipped
    assert "expression" in d.reason


def test_gin_index_is_reproduced_with_its_access_method():
    """A gin index recreated as btree is a different index — USING must travel."""
    idx = SourceIndex(
        "idx_payload",
        (IndexColumn("status"),),
        method="gin",
    )
    [d] = _plan(_measured(idx))
    assert d.carried
    assert " USING gin " in d.dest_sql


def test_operator_class_travels_with_the_key():
    idx = SourceIndex(
        "idx_code",
        (IndexColumn("status", opclass="varchar_pattern_ops"),),
    )
    [d] = _plan(_measured(idx))
    assert d.carried
    assert "varchar_pattern_ops" in d.dest_sql


def test_operator_class_is_refused_on_a_dialect_that_has_none():
    idx = SourceIndex(
        "idx_code",
        (IndexColumn("status", opclass="varchar_pattern_ops"),),
    )
    [d] = _plan(_measured(idx), dest="mysql")
    assert not d.carried
    assert "operator class" in d.reason


def test_sqlite_cannot_hold_a_gin_index():
    idx = SourceIndex("idx_payload", (IndexColumn("status"),), method="gin")
    [d] = _plan(_measured(idx), dest="sqlite")
    assert not d.carried
    assert "access method" in d.reason or "cannot reproduce" in d.reason

def test_index_over_an_unmapped_column_is_refused():
    idx = SourceIndex("idx_secret", (IndexColumn("ssn"),))
    [d] = _plan(_measured(idx))
    assert not d.carried
    assert "does not carry" in d.reason


def test_constraint_backed_index_is_skipped_not_duplicated():
    idx = SourceIndex("orders_pkey", (IndexColumn("id"),), unique=True, constraint_backed=True)
    [d] = _plan(_measured(idx), pk_columns=["id"])
    assert d.skipped and not d.carried
    assert "PRIMARY KEY" in d.reason


def test_constraint_backed_index_is_carried_when_the_constraint_was_not():
    # MySQL has no standalone UNIQUE constraint, so its unique indexes are
    # "constraint backed"; skipping one the create-new DDL did not carry would
    # drop the uniqueness guarantee while claiming it was covered elsewhere.
    idx = SourceIndex("uq_status", (IndexColumn("status"),), unique=True, constraint_backed=True)
    [d] = _plan(_measured(idx), pk_columns=["id"], unique_constraints=[])
    assert d.carried
    assert d.dest_sql.startswith("CREATE UNIQUE INDEX ")


def test_oracle_upper_cased_catalog_columns_still_match_the_mapping():
    idx = SourceIndex("IX_STATUS", (IndexColumn("STATUS"),))
    [d] = _plan(_measured(idx, dialect="oracle"), dest="oracle")
    assert d.carried
    assert '"status"' in d.dest_sql


def test_partial_index_is_refused_where_dialect_has_no_filter():
    idx = SourceIndex("idx_open", (IndexColumn("status"),), predicate="status <> 'done'")
    [d] = _plan(_measured(idx), dest="mysql")
    assert not d.carried
    assert "filtered" in d.reason


def test_partial_index_carries_when_the_filter_re_renders():
    idx = SourceIndex("idx_open", (IndexColumn("status"),), predicate="status <> 'done'")
    [d] = _plan(
        _measured(idx),
        check_renderer=lambda p: ('"status" <> \'done\'', ""),
    )
    assert d.carried
    assert d.dest_sql.endswith('WHERE "status" <> \'done\'')


def test_partial_index_is_refused_when_the_filter_does_not_re_render():
    idx = SourceIndex("idx_open", (IndexColumn("status"),), predicate="status ~ 'x'")
    [d] = _plan(_measured(idx), check_renderer=lambda p: ("", "operator not portable"))
    assert not d.carried
    assert "not portable" in d.reason


def test_covering_columns_are_named_when_the_destination_cannot_hold_them():
    idx = SourceIndex(
        "idx_cover",
        (IndexColumn("status"),),
        include_columns=("created_at",),
    )
    [carried] = _plan(_measured(idx), dest="mysql")
    assert carried.carried
    assert "INCLUDE" not in carried.dest_sql
    assert carried.dropped_include == ("created_at",)
    assert "created_at" in carried.reason


def test_covering_columns_are_carried_where_supported():
    idx = SourceIndex("idx_cover", (IndexColumn("status"),), include_columns=("created_at",))
    [d] = _plan(_measured(idx), dest="sqlserver")
    assert 'INCLUDE ("created_at")' in d.dest_sql


def test_index_names_are_qualified_and_deduplicated():
    a = SourceIndex("idx_status", (IndexColumn("status"),))
    b = SourceIndex("IDX_STATUS", (IndexColumn("created_at"),))
    first, second = _plan(_measured(a, b), dest="oracle")
    assert first.dest_name != second.dest_name
    assert first.dest_name.startswith("orders_")


def test_unavailable_catalog_yields_no_decisions_not_an_empty_certificate():
    unavailable = SourceIndexes(dialect="oracle", status="unavailable", detail="no grant")
    assert _plan(unavailable) == []


# --- certificate integration -------------------------------------------------


def _catalog(indexes_meta) -> SourceSchemaCatalog:
    return SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "TEXT"},
        primary_key=["id"],
        indexes_meta=indexes_meta,
    )


def _report_items(catalog, **kw):
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="postgresql",
        target_columns=["id", "status"],
        target_types=["INTEGER", "TEXT"],
        **kw,
    )
    return plan, [i for i in plan.report.items if i.aspect == "index"]


def test_unreadable_index_catalog_reports_unknown_not_absent():
    catalog = _catalog({"dialect": "oracle", "status": "unavailable", "detail": "no grant"})
    _plan_out, items = _report_items(catalog, dest_table="orders")
    assert [i.status for i in items] == ["unknown"]
    assert "no grant" in items[0].reason


def test_measured_empty_index_catalog_reports_skipped():
    catalog = _catalog({"dialect": "postgresql", "status": "measured", "items": []})
    _plan_out, items = _report_items(catalog, dest_table="orders")
    assert [i.status for i in items] == ["skipped"]


def test_carried_index_reaches_post_create_sql_and_the_certificate():
    catalog = _catalog(
        {
            "dialect": "postgresql",
            "status": "measured",
            "items": [
                {"name": "idx_status", "columns": [{"name": "status"}], "unique": False}
            ],
        }
    )
    plan, items = _report_items(catalog, dest_table="orders", dest_schema="public")
    assert [i.status for i in items] == ["carried"]
    assert plan.post_create_sql == [
        'CREATE INDEX "orders_idx_status" ON "public"."orders" ("status")'
    ]


def test_measured_indexes_without_a_table_name_are_unsupported_not_silent():
    catalog = _catalog(
        {
            "dialect": "postgresql",
            "status": "measured",
            "items": [{"name": "idx_status", "columns": [{"name": "status"}]}],
        }
    )
    _plan_out, items = _report_items(catalog)
    assert [i.status for i in items] == ["unsupported"]
    assert "table name was not supplied" in items[0].reason


def test_a_refused_create_index_downgrades_the_certificate():
    catalog = _catalog(
        {
            "dialect": "postgresql",
            "status": "measured",
            "items": [
                {"name": "uq_status", "columns": [{"name": "status"}], "unique": True}
            ],
        }
    )
    plan, items = _report_items(catalog, dest_table="orders")
    assert [i.status for i in items] == ["carried"]

    def _boom(_stmt: str) -> None:
        raise RuntimeError("duplicate key value violates unique constraint")

    failures = apply_post_create_sql(plan, _boom)
    assert len(failures) == 1
    after = [i for i in plan.report.items if i.aspect == "index"]
    assert [i.status for i in after] == ["unsupported"]
    assert after[0].dest_ddl == ""
    assert "duplicate key" in after[0].reason


# --- catalog probe -----------------------------------------------------------


def test_sqlite_probe_separates_constraint_backed_from_secondary():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "probe.db")
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT UNIQUE, created_at TEXT)"
        )
        conn.execute("CREATE INDEX idx_created ON orders (created_at DESC)")
        conn.commit()
        result = probe_secondary_indexes("sqlite", conn.cursor(), "", "orders")
        conn.close()

    assert result.status == "measured"
    by_name = {i.name: i for i in result.items}
    created = by_name["idx_created"]
    assert not created.constraint_backed
    assert created.columns == (IndexColumn("created_at", descending=True),)
    assert any(i.constraint_backed for i in result.items)


def test_probe_of_an_unknown_dialect_is_unavailable_not_empty():
    result = probe_secondary_indexes("neo4j", None, "", "orders")
    assert result.status == "unavailable"
    assert result.items == ()


def test_mysql_index_probe_survives_missing_expression_column():
    """MariaDB 10.x has no STATISTICS.EXPRESSION — that is not 'no indexes'."""

    class _Cur:
        def __init__(self) -> None:
            self.calls = 0
            self.connection = self

        def rollback(self) -> None:
            return None

        def execute(self, sql: str, params: tuple) -> None:
            self.calls += 1
            if "EXPRESSION" in sql:
                raise Exception("(1054, \"Unknown column 'EXPRESSION' in 'SELECT'\")")
            self._rows = [
                ("PRIMARY", 0, 1, "id", "A", "BTREE"),
                ("email", 0, 1, "email", "A", "BTREE"),
                ("idx_status", 1, 1, "status", "A", "BTREE"),
            ]

        def fetchall(self):
            return list(self._rows)

    result = probe_secondary_indexes("mysql", _Cur(), "dataflow", "people")
    assert result.status == "measured", result.detail
    by_name = {i.name: i for i in result.items}
    assert "idx_status" in by_name
    assert by_name["idx_status"].columns == (IndexColumn("status"),)
    assert not by_name["idx_status"].unique
    assert by_name["email"].unique
