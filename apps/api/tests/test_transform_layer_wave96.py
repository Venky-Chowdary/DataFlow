"""Post-load SQL transformation layer — planning, dialects, execution, safety.

Execution tests run against a real SQLite file rather than a mock. A mocked
connection would happily accept the invalid T-SQL and Oracle DDL that the
capability table exists to prevent, so it would prove nothing about the part
most likely to be wrong.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from services.transform_models import (
    DataTest,
    TransformCycleError,
    TransformDefinitionError,
    TransformModel,
    build_plan,
    describe_plan,
    extract_refs,
    extract_sources,
)
from services.transform_runner import (
    TransformRunner,
    UnsupportedMaterializationError,
)


def _model(name: str, sql: str, **kw) -> TransformModel:
    return TransformModel(name=name, sql=sql, **kw)


# --------------------------------------------------------------- ref parsing


class TestRefParsing:
    def test_extracts_refs_in_first_seen_order_without_duplicates(self):
        sql = (
            "SELECT * FROM {{ ref('b') }} "
            "JOIN {{ ref('a') }} USING (id) "
            "JOIN {{ ref('b') }} AS b2 USING (id)"
        )
        assert extract_refs(sql) == ["b", "a"]

    def test_tolerates_whitespace_and_both_quote_styles(self):
        assert extract_refs('SELECT 1 FROM {{ref("x")}}') == ["x"]
        assert extract_refs("SELECT 1 FROM {{   ref( 'y' )   }}") == ["y"]

    def test_sources_are_parsed_separately_from_refs(self):
        sql = "SELECT * FROM {{ source('raw_orders') }} JOIN {{ ref('dim') }} USING (id)"
        assert extract_sources(sql) == ["raw_orders"]
        assert extract_refs(sql) == ["dim"]

    def test_a_hardcoded_table_name_is_not_a_dependency(self):
        # This is the trap dbt users hit: referencing the physical table works
        # at run time but is invisible to the planner, so ordering breaks.
        assert extract_refs("SELECT * FROM analytics.stg_orders") == []


# ------------------------------------------------------------------ planning


class TestPlanBuilding:
    def test_linear_chain_produces_one_model_per_layer(self):
        plan = build_plan([
            _model("c", "SELECT * FROM {{ ref('b') }}"),
            _model("a", "SELECT 1 AS x"),
            _model("b", "SELECT * FROM {{ ref('a') }}"),
        ])
        assert plan.layers == [["a"], ["b"], ["c"]]
        assert plan.order == ["a", "b", "c"]
        assert plan.max_parallelism == 1

    def test_independent_models_share_a_layer_so_they_can_run_concurrently(self):
        plan = build_plan([
            _model("a", "SELECT 1 AS x"),
            _model("b", "SELECT 2 AS x"),
            _model("fan_in", "SELECT * FROM {{ ref('a') }}, {{ ref('b') }}"),
        ])
        assert plan.layers == [["a", "b"], ["fan_in"]]
        assert plan.max_parallelism == 2

    def test_diamond_dependency_orders_correctly(self):
        plan = build_plan([
            _model("top", "SELECT 1 AS x"),
            _model("left", "SELECT * FROM {{ ref('top') }}"),
            _model("right", "SELECT * FROM {{ ref('top') }}"),
            _model("bottom", "SELECT * FROM {{ ref('left') }} UNION ALL SELECT * FROM {{ ref('right') }}"),
        ])
        assert plan.layers == [["top"], ["left", "right"], ["bottom"]]

    def test_layer_ordering_is_deterministic_across_runs(self):
        models = [_model(n, "SELECT 1 AS x") for n in ("z", "m", "a", "q")]
        first = build_plan(models).layers
        for _ in range(8):
            assert build_plan(models).layers == first

    def test_direct_cycle_is_rejected_with_the_participating_models(self):
        with pytest.raises(TransformCycleError) as exc:
            build_plan([
                _model("a", "SELECT * FROM {{ ref('b') }}"),
                _model("b", "SELECT * FROM {{ ref('a') }}"),
            ])
        assert set(exc.value.cycle) >= {"a", "b"}
        assert "cycle" in str(exc.value).lower()

    def test_three_model_cycle_is_rejected(self):
        with pytest.raises(TransformCycleError):
            build_plan([
                _model("a", "SELECT * FROM {{ ref('c') }}"),
                _model("b", "SELECT * FROM {{ ref('a') }}"),
                _model("c", "SELECT * FROM {{ ref('b') }}"),
            ])

    def test_self_reference_is_a_cycle(self):
        with pytest.raises(TransformCycleError):
            build_plan([_model("a", "SELECT * FROM {{ ref('a') }}")])

    def test_duplicate_model_names_are_rejected(self):
        with pytest.raises(TransformDefinitionError, match="Duplicate model"):
            build_plan([_model("a", "SELECT 1 AS x"), _model("a", "SELECT 2 AS x")])

    def test_missing_ref_fails_closed_at_plan_time(self):
        # A typo'd ref must not compile against a physical table of that name.
        with pytest.raises(TransformDefinitionError, match="Unresolved model ref"):
            build_plan([_model("a", "SELECT * FROM {{ ref('nope') }}")])

    def test_disabled_upstream_is_treated_as_unresolved(self):
        # Disabled models are not built, so a ref to one is the same class of
        # error as a typo — refuse the plan rather than invent a relation.
        with pytest.raises(TransformDefinitionError, match="ref\\('off'\\)|Unresolved"):
            build_plan([
                _model("off", "SELECT 1 AS x", enabled=False),
                _model("on", "SELECT * FROM {{ ref('off') }}"),
            ])

    def test_select_pulls_in_upstream_dependencies_automatically(self):
        # Running 'c' alone would read a stale 'b'; the closure prevents that.
        plan = build_plan(
            [
                _model("a", "SELECT 1 AS x"),
                _model("b", "SELECT * FROM {{ ref('a') }}"),
                _model("c", "SELECT * FROM {{ ref('b') }}"),
                _model("unrelated", "SELECT 9 AS x"),
            ],
            select=["c"],
        )
        assert plan.order == ["a", "b", "c"]
        assert "unrelated" not in plan.models

    def test_empty_model_set_plans_to_nothing(self):
        plan = build_plan([])
        assert plan.layers == []
        assert describe_plan(plan) == "no models to run"


# ---------------------------------------------------------------- validation


class TestModelValidation:
    @pytest.mark.parametrize(
        "bad_name",
        ["", "1abc", "drop table", "a-b", "a;b", "a b", "x" * 64, "sel'ect"],
    )
    def test_invalid_model_names_are_rejected(self, bad_name):
        with pytest.raises(TransformDefinitionError):
            _model(bad_name, "SELECT 1 AS x")

    @pytest.mark.parametrize(
        "body",
        [
            "SELECT 1; DROP TABLE users",
            "DROP TABLE users",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "SELECT 1 AS x; TRUNCATE t",
            "CREATE TABLE t AS SELECT 1",
            "SELECT * FROM t; GRANT ALL ON t TO evil",
        ],
    )
    def test_non_select_bodies_are_rejected(self, body):
        with pytest.raises(TransformDefinitionError):
            _model("m", body)

    def test_a_cte_body_is_allowed(self):
        model = _model("m", "WITH t AS (SELECT 1 AS x) SELECT * FROM t")
        assert model.name == "m"

    def test_forbidden_keyword_inside_a_string_literal_is_not_a_false_positive(self):
        # 'drop ' appears in the data, not as a statement. Rejecting this would
        # make the guard useless for real analytics SQL.
        model = _model("m", "SELECT * FROM events WHERE action = 'drop table'")
        assert "drop table" in model.sql

    def test_forbidden_keyword_inside_a_comment_is_not_a_false_positive(self):
        model = _model("m", "SELECT 1 AS x -- we used to DROP TABLE here\n")
        assert model.name == "m"

    def test_a_column_named_like_a_keyword_is_allowed(self):
        model = _model("m", "SELECT created_at, updated_at FROM t")
        assert model.name == "m"

    def test_empty_sql_is_rejected(self):
        with pytest.raises(TransformDefinitionError, match="empty"):
            _model("m", "   ")

    def test_incremental_merge_without_unique_key_is_rejected(self):
        with pytest.raises(TransformDefinitionError, match="unique_key"):
            _model("m", "SELECT 1 AS x", materialization="incremental")

    def test_incremental_append_without_unique_key_is_allowed_but_at_least_once(self):
        model = _model(
            "m",
            "SELECT 1 AS x",
            materialization="incremental",
            incremental_strategy="append",
        )
        runner = TransformRunner({"type": "postgresql"}, dialect="postgresql")
        assert "at-least-once" in runner.physical_strategy(model)

    def test_unknown_materialization_is_rejected(self):
        with pytest.raises(TransformDefinitionError, match="materialization"):
            _model("m", "SELECT 1 AS x", materialization="magic")

    def test_unique_key_must_be_a_safe_identifier(self):
        with pytest.raises(TransformDefinitionError):
            _model(
                "m",
                "SELECT 1 AS x",
                materialization="incremental",
                unique_key="x; DROP TABLE t",
            )

    def test_test_column_must_be_a_safe_identifier(self):
        with pytest.raises(TransformDefinitionError):
            _model(
                "m",
                "SELECT 1 AS x",
                tests=[DataTest(test_type="not_null", column="x) OR 1=1--")],
            )

    def test_unknown_test_type_is_rejected(self):
        with pytest.raises(TransformDefinitionError, match="unknown test"):
            _model("m", "SELECT 1 AS x", tests=[DataTest(test_type="vibes", column="x")])

    def test_accepted_values_without_values_is_rejected(self):
        with pytest.raises(TransformDefinitionError, match="value list"):
            _model(
                "m",
                "SELECT 1 AS x",
                tests=[DataTest(test_type="accepted_values", column="x")],
            )

    def test_relationships_without_target_is_rejected(self):
        with pytest.raises(TransformDefinitionError, match="to_model"):
            _model(
                "m", "SELECT 1 AS x", tests=[DataTest(test_type="relationships", column="x")]
            )

    def test_roundtrips_through_dict(self):
        original = _model(
            "m",
            "SELECT * FROM {{ ref('u') }}",
            materialization="incremental",
            unique_key="id",
            tests=[DataTest(test_type="unique", column="id", severity="warn")],
            tags=["daily"],
        )
        restored = TransformModel.from_dict(original.to_dict())
        assert restored.to_dict() == original.to_dict()


# ------------------------------------------------------------------ dialects


class TestDialectMaterialization:
    """The capability table exists because the obvious SQL is wrong somewhere."""

    def _runner(self, dialect: str) -> TransformRunner:
        return TransformRunner(
            {"type": dialect, "schema": "analytics"}, dialect=dialect, schema="analytics"
        )

    @pytest.mark.parametrize(
        "dialect", ["postgresql", "mysql", "snowflake", "bigquery", "duckdb"]
    )
    def test_view_uses_or_replace_where_supported(self, dialect):
        model = _model("v", "SELECT 1 AS x", materialization="view")
        stmts = self._runner(dialect).build_statements(model, {"v": model})
        assert len(stmts) == 1
        assert stmts[0].upper().startswith("CREATE OR REPLACE VIEW")

    def test_sqlite_view_falls_back_to_drop_then_create(self):
        model = _model("v", "SELECT 1 AS x", materialization="view")
        stmts = self._runner("sqlite").build_statements(model, {"v": model})
        assert stmts[0].startswith("DROP VIEW IF EXISTS")
        assert stmts[1].startswith("CREATE VIEW")

    def test_sqlserver_table_uses_select_into_not_ctas(self):
        # T-SQL has no CREATE TABLE AS SELECT. Emitting it is a syntax error
        # that only surfaces against a real SQL Server.
        model = _model("t", "SELECT 1 AS x", materialization="table")
        stmts = self._runner("sqlserver").build_statements(model, {"t": model})
        joined = " ".join(stmts)
        assert "SELECT * INTO" in joined
        assert "CREATE TABLE" not in joined.upper()

    def test_sqlserver_statements_are_terminated(self):
        model = _model("t", "SELECT 1 AS x", materialization="table")
        for stmt in self._runner("sqlserver").build_statements(model, {"t": model}):
            assert stmt.endswith(";")

    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "duckdb"])
    def test_table_uses_atomic_or_replace_where_supported(self, dialect):
        model = _model("t", "SELECT 1 AS x", materialization="table")
        stmts = self._runner(dialect).build_statements(model, {"t": model})
        assert len(stmts) == 1
        assert "CREATE OR REPLACE TABLE" in stmts[0].upper()

    def test_oracle_refuses_incremental_it_cannot_seed_idempotently(self):
        # Oracle has no CREATE TABLE IF NOT EXISTS. A plain CTAS would work on
        # day one and fail on day two — refusing is the honest outcome.
        model = _model(
            "t", "SELECT 1 AS x", materialization="incremental", unique_key="x"
        )
        with pytest.raises(UnsupportedMaterializationError, match="IF NOT EXISTS"):
            self._runner("oracle").build_statements(model, {"t": model})

    def test_oracle_drop_omits_unsupported_if_exists(self):
        model = _model("t", "SELECT 1 AS x", materialization="table")
        stmts = self._runner("oracle").build_statements(model, {"t": model})
        assert stmts[0].startswith("DROP TABLE ")
        assert "IF EXISTS" not in stmts[0]

    def test_no_dialect_emits_the_databricks_only_merge_star_shorthand(self):
        # `UPDATE SET *` is Spark/Databricks syntax. Snowflake, BigQuery,
        # Oracle and SQL Server all reject it at run time.
        model = _model(
            "t",
            "SELECT 1 AS id",
            materialization="incremental",
            unique_key="id",
        )
        for dialect in ("postgresql", "snowflake", "bigquery", "sqlserver", "mysql", "sqlite"):
            try:
                stmts = self._runner(dialect).build_statements(model, {"t": model})
            except UnsupportedMaterializationError:
                continue
            joined = " ".join(stmts).upper()
            assert "UPDATE SET *" not in joined
            assert "INSERT *" not in joined

    def test_incremental_merge_is_idempotent_delete_insert_everywhere(self):
        model = _model(
            "t", "SELECT 1 AS id", materialization="incremental", unique_key="id"
        )
        for dialect in ("postgresql", "snowflake", "bigquery", "mysql", "sqlite"):
            stmts = self._runner(dialect).build_statements(model, {"t": model})
            joined = " ".join(stmts).upper()
            assert "DELETE FROM" in joined and "INSERT INTO" in joined

    def test_mysql_omits_the_schema_it_does_not_have(self):
        model = _model("v", "SELECT 1 AS x", materialization="view")
        stmts = self._runner("mysql").build_statements(model, {"v": model})
        assert "`v`" in stmts[0]
        assert "`analytics`.`v`" not in stmts[0]

    def test_sqlserver_columns_use_balanced_brackets(self):
        # quote_sql_identifier used to emit `[col[` for bracket dialects.
        model = _model(
            "t", "SELECT 1 AS id", materialization="table",
            tests=[DataTest(test_type="not_null", column="id")],
        )
        sql = self._runner("sqlserver").build_test_sql(model, model.tests[0])
        assert "[id]" in sql
        assert "[id[" not in sql

    def test_ephemeral_produces_no_statements(self):
        model = _model("e", "SELECT 1 AS x", materialization="ephemeral")
        assert self._runner("postgresql").build_statements(model, {"e": model}) == []

    def test_ephemeral_upstream_is_inlined_as_a_subquery(self):
        eph = _model("eph", "SELECT 1 AS x", materialization="ephemeral")
        downstream = _model("d", "SELECT * FROM {{ ref('eph') }}", materialization="table")
        models = {"eph": eph, "d": downstream}
        compiled = self._runner("postgresql").compile_sql(downstream, models)
        assert "(SELECT 1 AS x)" in compiled
        # It must not reference a relation that was never created.
        assert '"analytics"."eph"' not in compiled


class TestDataTestSql:
    def _runner(self) -> TransformRunner:
        return TransformRunner(
            {"type": "postgresql", "schema": "an"}, dialect="postgresql", schema="an"
        )

    def test_not_null_counts_nulls(self):
        model = _model("m", "SELECT 1 AS x", tests=[DataTest(test_type="not_null", column="x")])
        sql = self._runner().build_test_sql(model, model.tests[0])
        assert sql == 'SELECT COUNT(*) FROM "an"."m" WHERE "x" IS NULL'

    def test_unique_counts_duplicate_groups_not_rows(self):
        model = _model("m", "SELECT 1 AS x", tests=[DataTest(test_type="unique", column="x")])
        sql = self._runner().build_test_sql(model, model.tests[0])
        assert "GROUP BY" in sql and "HAVING COUNT(*) > 1" in sql

    def test_accepted_values_escapes_quotes_in_literals(self):
        model = _model(
            "m",
            "SELECT 1 AS x",
            tests=[DataTest(test_type="accepted_values", column="x", values=["it's", "b"])],
        )
        sql = self._runner().build_test_sql(model, model.tests[0])
        assert "'it''s'" in sql

    def test_relationships_becomes_a_not_exists_orphan_check(self):
        model = _model(
            "child",
            "SELECT 1 AS parent_id",
            tests=[
                DataTest(
                    test_type="relationships",
                    column="parent_id",
                    to_model="parent",
                    to_column="id",
                )
            ],
        )
        sql = self._runner().build_test_sql(model, model.tests[0])
        assert "NOT EXISTS" in sql and '"an"."parent"' in sql

    def test_tests_never_transfer_rows_only_a_count(self):
        # The whole point of pushdown: O(1) bytes regardless of table size.
        model = _model(
            "m",
            "SELECT 1 AS x",
            tests=[
                DataTest(test_type="not_null", column="x"),
                DataTest(test_type="unique", column="x"),
                DataTest(test_type="positive", column="x"),
            ],
        )
        for test in model.tests:
            assert self._runner().build_test_sql(model, test).startswith("SELECT COUNT(*)")

    def test_violation_select_and_delete_share_not_null_predicate(self):
        model = _model("m", "SELECT 1 AS x", tests=[DataTest(test_type="not_null", column="x")])
        runner = self._runner()
        pred = runner.build_violation_predicate(model, model.tests[0])
        assert pred == '"x" IS NULL'
        assert '"x" IS NULL' in runner.build_violation_select_sql(model, model.tests[0])
        assert runner.build_holdout_delete_sql(model, model.tests[0]).startswith("DELETE FROM")

    def test_unique_holdout_sql_deletes_duplicate_group_members(self):
        model = _model("m", "SELECT 1 AS x", tests=[DataTest(test_type="unique", column="x")])
        sql = self._runner().build_holdout_delete_sql(model, model.tests[0])
        assert sql.startswith("DELETE FROM")
        assert "HAVING COUNT(*) > 1" in sql
        assert "AS _df_dupes" in sql


# ----------------------------------------------------------------- execution


@pytest.fixture()
def warehouse(tmp_path: Path) -> str:
    db = str(tmp_path / "wh.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE orders (id INTEGER, day TEXT, amount REAL, status TEXT)")
    con.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, "2026-01-01", 100.0, "paid"),
            (2, "2026-01-01", 50.0, "paid"),
            (3, "2026-01-02", 75.0, "paid"),
            (4, "2026-01-02", 25.0, "test"),
        ],
    )
    con.commit()
    con.close()
    return db


def _pipeline() -> list[TransformModel]:
    return [
        _model(
            "stg_orders",
            "SELECT * FROM {{ source('orders') }} WHERE status <> 'test'",
            materialization="view",
        ),
        _model(
            "daily_rev",
            "SELECT day, SUM(amount) AS revenue FROM {{ ref('stg_orders') }} GROUP BY day",
            materialization="incremental",
            unique_key="day",
            tests=[
                DataTest(test_type="not_null", column="day"),
                DataTest(test_type="unique", column="day"),
            ],
        ),
        _model(
            "rev_report",
            "SELECT day, revenue FROM {{ ref('daily_rev') }}",
            materialization="table",
        ),
    ]


class TestRealExecution:
    def test_full_pipeline_builds_and_computes_correct_values(self, warehouse):
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run(_pipeline())

        assert result.status == "success", result.error
        assert [m.name for m in result.models] == ["stg_orders", "daily_rev", "rev_report"]
        assert all(m.status == "success" for m in result.models)

        con = sqlite3.connect(warehouse)
        rows = con.execute("SELECT day, revenue FROM rev_report ORDER BY day").fetchall()
        con.close()
        # The 'test' row must have been filtered by the staging view.
        assert rows == [("2026-01-01", 150.0), ("2026-01-02", 75.0)]

    def test_data_tests_run_and_pass_on_clean_data(self, warehouse):
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run(_pipeline())
        tests = [t for m in result.models for t in m.tests]
        assert len(tests) == 2
        assert all(t["passed"] for t in tests)
        assert result.failed_tests == []

    def test_a_failing_data_test_is_reported_and_downgrades_the_run(self, warehouse):
        models = [
            _model(
                "dupes",
                "SELECT status FROM {{ source('orders') }}",
                materialization="table",
                tests=[DataTest(test_type="unique", column="status")],
            )
        ]
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run(models)

        # The model built fine; the data is what is wrong.
        assert result.models[0].status == "success"
        assert result.status == "partial"
        failing = result.models[0].tests[0]
        assert failing["passed"] is False
        assert failing["failing_rows"] == 1
        assert "violate unique" in failing["message"]
        con = sqlite3.connect(warehouse)
        dest_count = con.execute("SELECT COUNT(*) FROM dupes").fetchone()[0]
        con.close()
        assert dest_count == 4, "no project_id → fail-closed, unique groups stay in the mart"

    def test_a_warn_severity_test_does_not_downgrade_the_run(self, warehouse):
        models = [
            _model(
                "dupes",
                "SELECT status FROM {{ source('orders') }}",
                materialization="table",
                tests=[DataTest(test_type="unique", column="status", severity="warn")],
            )
        ]
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run(models)
        assert result.models[0].tests[0]["passed"] is False
        assert result.status == "success"

    def test_incremental_rerun_is_idempotent(self, warehouse):
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        runner.run(_pipeline())
        runner.run(_pipeline())
        runner.run(_pipeline())

        con = sqlite3.connect(warehouse)
        count = con.execute("SELECT COUNT(*) FROM daily_rev").fetchone()[0]
        rows = con.execute("SELECT day, revenue FROM daily_rev ORDER BY day").fetchall()
        con.close()
        assert count == 2, "delete_insert must not accumulate duplicate keys"
        assert rows == [("2026-01-01", 150.0), ("2026-01-02", 75.0)]

    def test_incremental_picks_up_new_source_rows(self, warehouse):
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        runner.run(_pipeline())

        con = sqlite3.connect(warehouse)
        con.execute("INSERT INTO orders VALUES (5, '2026-01-03', 200.0, 'paid')")
        con.execute("INSERT INTO orders VALUES (6, '2026-01-01', 10.0, 'paid')")
        con.commit()
        con.close()

        runner.run(_pipeline())
        con = sqlite3.connect(warehouse)
        rows = con.execute("SELECT day, revenue FROM daily_rev ORDER BY day").fetchall()
        con.close()
        # New day appended, existing day recomputed rather than duplicated.
        assert rows == [("2026-01-01", 160.0), ("2026-01-02", 75.0), ("2026-01-03", 200.0)]

    def test_append_strategy_is_honestly_at_least_once(self, warehouse):
        models = [
            _model(
                "log",
                "SELECT day FROM {{ source('orders') }}",
                materialization="incremental",
                incremental_strategy="append",
            )
        ]
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        runner.run(models)
        first = _count(warehouse, "log")
        runner.run(models)
        second = _count(warehouse, "log")
        assert second > first, "append duplicates by design"
        assert "at-least-once" in runner.physical_strategy(models[0])

    def test_downstream_is_skipped_when_upstream_fails(self, warehouse):
        models = [
            _model("broken", "SELECT * FROM table_that_does_not_exist", materialization="table"),
            _model("child", "SELECT * FROM {{ ref('broken') }}", materialization="table"),
        ]
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run(models)

        by_name = {m.name: m for m in result.models}
        assert by_name["broken"].status == "failed"
        # Running the child against a stale/absent relation would produce
        # silently wrong data; skipping and saying why is the correct outcome.
        assert by_name["child"].status == "skipped"
        assert "broken" in by_name["child"].error
        assert result.status in {"failed", "partial"}

    def test_an_unrelated_model_still_runs_when_a_sibling_fails(self, warehouse):
        models = [
            _model("broken", "SELECT * FROM nope", materialization="table"),
            _model("fine", "SELECT 1 AS x", materialization="table"),
        ]
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run(models)
        by_name = {m.name: m.status for m in result.models}
        assert by_name == {"broken": "failed", "fine": "success"}
        assert result.status == "partial"

    def test_dry_run_touches_nothing_but_returns_the_sql(self, warehouse):
        runner = TransformRunner(
            {"type": "sqlite", "database": warehouse}, dialect="sqlite", dry_run=True
        )
        result = runner.run(_pipeline())
        assert result.status == "skipped"
        assert all(m.status == "skipped" for m in result.models)
        assert all(m.sql for m in result.models)

        con = sqlite3.connect(warehouse)
        names = {
            r[0] for r in con.execute("SELECT name FROM sqlite_master").fetchall()
        }
        con.close()
        assert names == {"orders"}, "dry run must not create relations"

    def test_a_test_that_cannot_run_is_not_reported_as_passing(self, warehouse):
        model = _model(
            "m",
            "SELECT 1 AS x",
            materialization="table",
            tests=[DataTest(test_type="not_null", column="no_such_column")],
        )
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run([model])
        test = result.models[0].tests[0]
        assert test["passed"] is False
        assert "could not run" in test["message"].lower()

    def test_empty_plan_is_skipped_not_failed(self, warehouse):
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run([])
        assert result.status == "skipped"
        assert result.models == []

    def test_select_runs_only_the_requested_subgraph(self, warehouse):
        runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
        result = runner.run(_pipeline(), select=["daily_rev"])
        assert [m.name for m in result.models] == ["stg_orders", "daily_rev"]
        con = sqlite3.connect(warehouse)
        names = {r[0] for r in con.execute("SELECT name FROM sqlite_master").fetchall()}
        con.close()
        assert "rev_report" not in names


class TestIncrementalColumnAlignment:
    """An incremental load must match columns by name, never by position.

    The target of an incremental model outlives the model definition: an
    operator pre-creates the mart, an earlier revision of the SELECT built it,
    or someone adds a column. A positional `INSERT INTO t SELECT *` is only
    correct while the two orders happen to agree — when they diverge it writes
    `region` into `city`, the row count reconciles, and the run reports success.
    Live proof: transform_live_results.json (30 cases, PG/MySQL/SQL Server).
    """

    def _runner(self, warehouse: str) -> TransformRunner:
        return TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")

    def _target(self, warehouse: str, ddl: str, table: str = "mart") -> None:
        con = sqlite3.connect(warehouse)
        con.execute(ddl)
        con.commit()
        con.close()

    def _model(self, sql: str, **kw) -> TransformModel:
        kw.setdefault("materialization", "incremental")
        kw.setdefault("incremental_strategy", "append")
        return _model("mart", sql, **kw)

    def test_reordered_target_receives_each_value_in_its_own_column(self, warehouse):
        self._target(warehouse, "CREATE TABLE mart (id INTEGER, city TEXT, region TEXT)")
        result = self._runner(warehouse).run(
            [self._model("SELECT 1 AS id, 'EMEA' AS region, 'Berlin' AS city")]
        )
        assert result.status == "success", result.error
        con = sqlite3.connect(warehouse)
        row = con.execute("SELECT id, city, region FROM mart").fetchone()
        con.close()
        assert row == (1, "Berlin", "EMEA")

    def test_executed_sql_names_its_columns(self, warehouse):
        self._target(warehouse, "CREATE TABLE mart (id INTEGER, city TEXT, region TEXT)")
        result = self._runner(warehouse).run(
            [self._model("SELECT 1 AS id, 'EMEA' AS region, 'Berlin' AS city")]
        )
        sql = result.models[0].sql
        assert 'INSERT INTO "mart" ("id", "region", "city")' in sql
        assert "INSERT INTO \"mart\" SELECT" not in sql
        assert result.models[0].column_alignment == {
            "id": "id",
            "region": "region",
            "city": "city",
        }

    def test_a_column_the_model_omits_is_left_to_the_target(self, warehouse):
        # Positionally this shifts every value one column to the left.
        self._target(warehouse, "CREATE TABLE mart (id INTEGER, tenant TEXT, city TEXT)")
        result = self._runner(warehouse).run(
            [self._model("SELECT 1 AS id, 'Berlin' AS city")]
        )
        assert result.status == "success", result.error
        con = sqlite3.connect(warehouse)
        row = con.execute("SELECT id, tenant, city FROM mart").fetchone()
        con.close()
        assert row == (1, None, "Berlin")

    def test_a_column_the_target_lacks_is_refused_by_name(self, warehouse):
        self._target(warehouse, "CREATE TABLE mart (id INTEGER, city TEXT)")
        result = self._runner(warehouse).run(
            [self._model("SELECT 1 AS id, 'Berlin' AS city, 'EMEA' AS region")]
        )
        assert result.models[0].status == "failed"
        assert "region" in result.models[0].error
        assert _count(warehouse, "mart") == 0

    def test_a_required_target_column_nothing_fills_is_refused(self, warehouse):
        self._target(
            warehouse,
            "CREATE TABLE mart (id INTEGER, city TEXT, tenant TEXT NOT NULL)",
        )
        result = self._runner(warehouse).run(
            [self._model("SELECT 1 AS id, 'Berlin' AS city")]
        )
        assert result.models[0].status == "failed"
        assert "tenant" in result.models[0].error
        assert _count(warehouse, "mart") == 0

    def test_a_required_target_column_with_a_default_still_loads(self, warehouse):
        # Fail-closed must not become fail-always: the destination fills this.
        self._target(
            warehouse,
            "CREATE TABLE mart (id INTEGER, city TEXT, "
            "tenant TEXT NOT NULL DEFAULT 'acme')",
        )
        result = self._runner(warehouse).run(
            [self._model("SELECT 1 AS id, 'Berlin' AS city")]
        )
        assert result.status == "success", result.error
        con = sqlite3.connect(warehouse)
        row = con.execute("SELECT id, city, tenant FROM mart").fetchone()
        con.close()
        assert row == (1, "Berlin", "acme")

    def test_delete_insert_onto_a_reordered_target_is_aligned(self, warehouse):
        self._target(warehouse, "CREATE TABLE mart (id INTEGER, city TEXT, region TEXT)")
        self._target(warehouse, "INSERT INTO mart VALUES (1, 'Old', 'OLD')")
        result = self._runner(warehouse).run(
            [
                self._model(
                    "SELECT 1 AS id, 'EMEA' AS region, 'Berlin' AS city",
                    incremental_strategy="merge",
                    unique_key="id",
                )
            ]
        )
        assert result.status == "success", result.error
        con = sqlite3.connect(warehouse)
        rows = con.execute("SELECT id, city, region FROM mart").fetchall()
        con.close()
        assert rows == [(1, "Berlin", "EMEA")]

    def test_a_unique_key_the_model_does_not_select_is_refused(self, warehouse):
        # The DELETE would match nothing, so delete+insert would duplicate.
        self._target(warehouse, "CREATE TABLE mart (id INTEGER, city TEXT)")
        result = self._runner(warehouse).run(
            [
                self._model(
                    "SELECT 'Berlin' AS city, 1 AS id",
                    incremental_strategy="merge",
                    unique_key="order_id",
                )
            ]
        )
        assert result.models[0].status == "failed"
        assert "order_id" in result.models[0].error
        assert _count(warehouse, "mart") == 0

    def test_alignment_ignores_column_case(self, warehouse):
        self._target(warehouse, "CREATE TABLE mart (id INTEGER, city TEXT, region TEXT)")
        result = self._runner(warehouse).run(
            [self._model('SELECT 1 AS "ID", \'EMEA\' AS "Region", \'Berlin\' AS "City"')]
        )
        assert result.status == "success", result.error
        con = sqlite3.connect(warehouse)
        row = con.execute("SELECT id, city, region FROM mart").fetchone()
        con.close()
        assert row == (1, "Berlin", "EMEA")

    def test_first_run_creates_the_target_and_loads_it(self, warehouse):
        result = self._runner(warehouse).run(
            [self._model("SELECT 1 AS id, 'Berlin' AS city")]
        )
        assert result.status == "success", result.error
        assert _count(warehouse, "mart") == 1

    def test_first_append_run_loads_each_row_once(self, warehouse):
        # The seed used to be `CREATE TABLE ... AS <body>`, which materialized
        # the rows, and the INSERT then wrote them a second time. Only the first
        # run of a brand-new model was affected, so every later run looked right.
        runner = self._runner(warehouse)
        model = self._model("SELECT id FROM orders")
        runner.run([model])
        assert _count(warehouse, "mart") == 4
        runner.run([model])
        assert _count(warehouse, "mart") == 8, "append is at-least-once by design"


def _count(db: str, table: str) -> int:
    con = sqlite3.connect(db)
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        con.close()


# --------------------------------------------------------------------- store


class TestTransformStore:
    def _store(self, tmp_path: Path):
        from services.transform_store import FileTransformProjectStore

        return FileTransformProjectStore(path=tmp_path / "projects.json")

    def _project(self, **kw):
        from services.transform_store import TransformProject

        defaults = dict(
            name="Analytics",
            destination_connector_id="conn-1",
            schema="analytics",
            models=[_model("a", "SELECT 1 AS x")],
        )
        defaults.update(kw)
        return TransformProject(**defaults)

    def test_save_and_get_roundtrip(self, tmp_path):
        store = self._store(tmp_path)
        saved = store.save(self._project())
        fetched = store.get(saved.id)
        assert fetched is not None
        assert fetched.name == "Analytics"
        assert [m.name for m in fetched.models] == ["a"]

    def test_save_rejects_a_project_whose_models_form_a_cycle(self, tmp_path):
        # Caught when authored, not when a transfer finishes at 3am.
        store = self._store(tmp_path)
        project = self._project(
            models=[
                _model("a", "SELECT * FROM {{ ref('b') }}"),
                _model("b", "SELECT * FROM {{ ref('a') }}"),
            ]
        )
        with pytest.raises(TransformCycleError):
            store.save(project)

    def test_save_rejects_a_project_without_a_destination(self, tmp_path):
        store = self._store(tmp_path)
        with pytest.raises(ValueError, match="destination connector"):
            store.save(self._project(destination_connector_id=""))

    def test_version_increments_and_created_at_is_preserved(self, tmp_path):
        store = self._store(tmp_path)
        first = store.save(self._project())
        created = first.created_at
        first.name = "Renamed"
        second = store.save(first)
        assert second.version == 2
        assert second.created_at == created

    def test_delete_removes_and_reports_missing(self, tmp_path):
        store = self._store(tmp_path)
        saved = store.save(self._project())
        assert store.delete(saved.id) is True
        assert store.get(saved.id) is None
        assert store.delete(saved.id) is False

    def test_writes_are_atomic_leaving_no_temp_files(self, tmp_path):
        store = self._store(tmp_path)
        store.save(self._project())
        leftovers = [p.name for p in tmp_path.iterdir() if ".tmp." in p.name]
        assert leftovers == []

    def test_a_corrupt_store_raises_rather_than_reporting_zero_projects(self, tmp_path):
        # Returning [] here would make the post-load hook silently skip every
        # project, which looks identical to "nothing configured".
        path = tmp_path / "projects.json"
        path.write_text("{not json", encoding="utf-8")
        from services.transform_store import FileTransformProjectStore

        with pytest.raises(Exception):
            FileTransformProjectStore(path=path).list()

    def test_workspace_scoping_filters_other_workspaces(self, tmp_path):
        store = self._store(tmp_path)
        store.save(self._project(name="mine", workspace_id="ws-1"))
        store.save(self._project(name="theirs", workspace_id="ws-2"))
        names = {p.name for p in store.list("ws-1")}
        assert names == {"mine"}

    def test_trigger_matching_is_case_insensitive_and_defaults_to_any_table(self, tmp_path):
        project = self._project()
        assert project.triggered_by("anything") is True

        scoped = self._project(trigger_tables=["Orders"])
        assert scoped.triggered_by("orders") is True
        assert scoped.triggered_by("customers") is False

    def test_a_disabled_project_never_triggers(self, tmp_path):
        assert self._project(enabled=False).triggered_by("orders") is False
        assert self._project(run_after_transfer=False).triggered_by("orders") is False


# ------------------------------------------------------------ post-load hook


class TestPostLoadHook:
    def test_hook_is_wired_into_every_engine_completion_path(self):
        # Three near-identical completion blocks exist. A post-load step wired
        # into one would run after a CSV load but not a Postgres stream, and an
        # operator could not tell which happened.
        source = Path(__file__).resolve().parents[1] / "src" / "transfer" / "engine.py"
        text = source.read_text(encoding="utf-8")
        assert text.count("_apply_post_load_transforms(request, dest_summary)") == 3

    def test_missing_store_is_reported_not_raised(self, monkeypatch):
        from services import post_load_transform

        def boom(*_a, **_k):
            raise RuntimeError("store offline")

        monkeypatch.setattr(
            "services.transform_store.get_transform_store", boom, raising=True
        )
        out = post_load_transform.run_post_load_transforms(
            destination=object(), landed_table="orders"
        )
        assert out["status"] == "failed"
        assert "store offline" in out["message"]
        assert out["ran"] is False

    def test_no_configured_project_is_a_clean_skip(self, monkeypatch, tmp_path):
        from services import post_load_transform

        class EmptyStore:
            def list(self, _ws=""):
                return []

        monkeypatch.setattr(
            "services.transform_store.get_transform_store", lambda *a, **k: EmptyStore()
        )
        out = post_load_transform.run_post_load_transforms(
            destination=object(), landed_table="orders"
        )
        assert out == {
            "ran": False,
            "status": "skipped",
            "projects": [],
            "message": "No transformation project is configured for this table.",
        }

    def test_summary_shape_is_stable_so_the_ui_never_special_cases_absence(
        self, monkeypatch
    ):
        from services import post_load_transform

        class EmptyStore:
            def list(self, _ws=""):
                return []

        monkeypatch.setattr(
            "services.transform_store.get_transform_store", lambda *a, **k: EmptyStore()
        )
        out = post_load_transform.run_post_load_transforms(
            destination=object(), landed_table="t"
        )
        assert set(out) == {"ran", "status", "projects", "message"}

    def test_missing_connector_id_does_not_wildcard_bound_projects(self, monkeypatch):
        # Inline-credential transfers have no saved connector id. Treating that
        # as a match-all would run every project's SQL against the wrong warehouse.
        from services import post_load_transform
        from services.transform_store import TransformProject

        bound = TransformProject(
            name="bound",
            destination_connector_id="conn-other",
            models=[_model("a", "SELECT 1 AS x")],
            trigger_tables=["orders"],
        )

        class Store:
            def list(self, _ws=""):
                return [bound]

        monkeypatch.setattr(
            "services.transform_store.get_transform_store", lambda *a, **k: Store()
        )

        class Dest:
            table = "orders"
            # deliberately no connector_id

        out = post_load_transform.run_post_load_transforms(
            destination=Dest(), landed_table="orders"
        )
        assert out["ran"] is False
        assert out["status"] == "skipped"
        assert "no saved destination connector" in out["message"].lower()
        assert out["projects"] == []

    def test_connector_mismatch_message_is_preserved_for_the_job_summary(
        self, monkeypatch
    ):
        from services import post_load_transform
        from services.transform_store import TransformProject

        project = TransformProject(
            name="elsewhere",
            destination_connector_id="conn-b",
            models=[_model("a", "SELECT 1 AS x")],
            trigger_tables=["orders"],
        )

        class Store:
            def list(self, _ws=""):
                return [project]

        monkeypatch.setattr(
            "services.transform_store.get_transform_store", lambda *a, **k: Store()
        )

        class Dest:
            table = "orders"
            connector_id = "conn-a"

        out = post_load_transform.run_post_load_transforms(
            destination=Dest(), landed_table="orders"
        )
        assert out["status"] == "skipped"
        assert "different destination connector" in out["message"]

        # The engine helper must surface this, not drop it as a quiet no-op.
        from src.transfer.engine import _apply_post_load_transforms

        class Request:
            destination = Dest()
            workspace_id = ""

        dest_summary: dict = {}
        _apply_post_load_transforms(Request(), dest_summary)
        assert "transformations" in dest_summary
        assert dest_summary["transformations"]["status"] == "skipped"

    def test_engine_records_an_unexpected_hook_exception(self, monkeypatch):
        from src.transfer import engine as engine_mod

        def boom(**_k):
            raise RuntimeError("unexpected")

        monkeypatch.setattr(
            "services.post_load_transform.run_post_load_transforms", boom
        )

        class Dest:
            table = "orders"

        class Request:
            destination = Dest()
            workspace_id = ""

        dest_summary: dict = {}
        engine_mod._apply_post_load_transforms(Request(), dest_summary)
        assert dest_summary["transformations"]["status"] == "failed"
        assert "unexpected" in dest_summary["transformations"]["message"]


class TestMongoFilePrecedence:
    def test_newer_file_copy_wins_over_stale_mongo(self, tmp_path, monkeypatch):
        from services.transform_store import (
            FileTransformProjectStore,
            MongoTransformProjectStore,
            TransformProject,
            _is_newer,
        )

        file_store = FileTransformProjectStore(path=tmp_path / "p.json")
        v1 = file_store.save(
            TransformProject(
                name="v1",
                destination_connector_id="c1",
                models=[_model("a", "SELECT 1 AS x")],
            )
        )
        # Simulate a later edit that reached the file but not Mongo.
        v1.name = "v2-file"
        v2 = file_store.save(v1)
        assert v2.version == 2

        stale = TransformProject.from_dict({**v2.to_dict(), "name": "v1-mongo", "version": 1})
        assert _is_newer(v2, stale) is True
        assert _is_newer(stale, v2) is False

        class FakeColl:
            def find(self, _q=None):
                return [{**stale.to_dict(), "_id": stale.id}]

            def find_one(self, _q=None):
                return {**stale.to_dict(), "_id": stale.id}

            def replace_one(self, *_a, **_k):
                return None

            def delete_one(self, *_a, **_k):
                return type("R", (), {"deleted_count": 0})()

        class FakeDB(dict):
            def __getitem__(self, key):
                return FakeColl()

        store = MongoTransformProjectStore(mongo_service=None)
        store._file = file_store
        monkeypatch.setattr(store, "_db", lambda: FakeDB())

        listed = store.list()
        assert len(listed) == 1
        assert listed[0].name == "v2-file"
        assert listed[0].version == 2
        assert store.get(v2.id).name == "v2-file"
