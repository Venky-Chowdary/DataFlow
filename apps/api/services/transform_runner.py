"""Execute a transformation plan against a destination warehouse.

:mod:`services.transform_models` decides what runs and in what order. This
module turns each model into dialect-correct DDL, runs it, then evaluates the
model's data tests as pushdown SQL.

Two rules shape everything here.

**Pushdown, not pull-down.** Tests are ``SELECT count(*) ... WHERE <violation>``
executed at the destination. The existing :mod:`services.expectations_engine`
runs the same checks in Python over ``list[dict]``, which is correct for a
Validate sample and hopeless for a materialized fact table — testing 400M rows
by fetching them defeats the purpose of having pushed the compute down. The
results are still emitted in the expectations engine's shape so the job
plumbing and UI consume them unchanged.

**Identifiers are never interpolated raw.** Model names, columns and unique
keys all reach SQL text. Every one of them goes through
``require_safe_identifier`` at definition time and ``quote_table_ref`` /
``quote_sql_identifier`` at emit time. Model bodies are separately constrained
to a single bare SELECT by ``transform_models._reject_unsafe_sql``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
from services.engine_pool import release_engine
from services.dialect_profiles import dialect_profile, normalize_driver
from services.transform_models import (
    TransformModel,
    TransformPlan,
    build_plan,
    describe_plan,
    extract_refs,
    extract_sources,
)

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class MaterializationDialect:
    """What DDL forms one dialect actually accepts.

    This exists because the obvious shortcuts are wrong in ways that only show
    up against a real warehouse. ``CREATE TABLE ... AS SELECT`` is not valid
    T-SQL — SQL Server spells it ``SELECT ... INTO``. ``MERGE ... UPDATE SET *``
    is Databricks/Spark syntax; Snowflake, BigQuery and Oracle all require an
    explicit column list, so emitting ``SET *`` there produces a syntax error at
    run time rather than a caught one at build time. Oracle has no
    ``DROP TABLE IF EXISTS`` at all.

    Encoding the capabilities and refusing to emit an unsupported form is the
    difference between a transformation layer and a demo that only works on
    Postgres.
    """

    or_replace_view: bool = False
    or_replace_table: bool = False
    drop_view_if_exists: bool = True
    drop_table_if_exists: bool = True
    #: CREATE TABLE x AS SELECT ...
    ctas: bool = True
    #: T-SQL SELECT ... INTO x FROM ...
    select_into: bool = False
    create_table_if_not_exists: bool = True
    #: Statement suffix. T-SQL MERGE is the notable case that requires one.
    terminator: str = ""


_ANSI = MaterializationDialect()

_MATERIALIZATION_DIALECTS: dict[str, MaterializationDialect] = {
    "postgresql": MaterializationDialect(or_replace_view=True),
    "redshift": MaterializationDialect(or_replace_view=True),
    "mysql": MaterializationDialect(or_replace_view=True),
    "sqlite": MaterializationDialect(),
    "duckdb": MaterializationDialect(or_replace_view=True, or_replace_table=True),
    "snowflake": MaterializationDialect(or_replace_view=True, or_replace_table=True),
    "bigquery": MaterializationDialect(or_replace_view=True, or_replace_table=True),
    "databricks": MaterializationDialect(or_replace_view=True, or_replace_table=True),
    "trino": MaterializationDialect(or_replace_view=True),
    "vertica": MaterializationDialect(or_replace_view=True),
    "clickhouse": MaterializationDialect(or_replace_view=True),
    # T-SQL: no CTAS, no OR REPLACE, statements terminated for MERGE.
    "sqlserver": MaterializationDialect(
        ctas=False, select_into=True, create_table_if_not_exists=False, terminator=";"
    ),
    # Oracle has neither IF EXISTS nor IF NOT EXISTS on these statements.
    "oracle": MaterializationDialect(
        or_replace_view=True,
        drop_view_if_exists=False,
        drop_table_if_exists=False,
        create_table_if_not_exists=False,
    ),
}


class UnsupportedMaterializationError(ValueError):
    """The dialect cannot express this materialization safely.

    Raised at build time so the operator sees a clear reason instead of a
    driver syntax error midway through a run.
    """


class TransformAlignmentError(ValueError):
    """The model's output cannot be matched by name to the existing target.

    Raised before the load writes anything, naming the columns involved. The
    alternative is what this module used to do — bind positionally, and let the
    engine put ``region`` into ``city`` while the run reported success.
    """


@dataclass(frozen=True)
class ColumnAlignment:
    """Target column ↔ model column pairs for one incremental load.

    Order follows the model's output so the INSERT column list and the SELECT
    projection stay index-for-index identical whatever order the target has.
    """

    #: (target column name, model output column name)
    pairs: tuple[tuple[str, str], ...]

    def insert_clause(self, runner: "TransformRunner") -> str:
        return "(" + ", ".join(runner.quote_column(t) for t, _ in self.pairs) + ")"

    def select_list(self, runner: "TransformRunner") -> str:
        return ", ".join(runner.quote_column(m) for _, m in self.pairs)

    def to_dict(self) -> dict[str, str]:
        return {target: model for target, model in self.pairs}


@dataclass
class ModelRunResult:
    """Outcome of materializing one model."""

    name: str
    materialization: str
    status: str = "success"  # success | failed | skipped
    relation: str = ""
    #: The strategy that physically ran, which can differ from the declared one.
    strategy: str = ""
    rows_affected: int = -1  # -1 = not reported by the driver
    seconds: float = 0.0
    sql: str = ""
    error: str = ""
    tests: list[dict[str, Any]] = field(default_factory=list)
    #: target column → model column, for incremental loads that ran.
    column_alignment: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "materialization": self.materialization,
            "status": self.status,
            "relation": self.relation,
            "strategy": self.strategy,
            "rows_affected": self.rows_affected,
            "seconds": round(self.seconds, 3),
            "sql": self.sql,
            "error": self.error,
            "tests": list(self.tests),
            "column_alignment": dict(self.column_alignment),
        }


@dataclass
class TransformRunResult:
    """Outcome of a whole plan."""

    status: str = "success"  # success | failed | partial | skipped
    models: list[ModelRunResult] = field(default_factory=list)
    seconds: float = 0.0
    plan: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    warnings: list[str] = field(default_factory=list)

    @property
    def failed_models(self) -> list[ModelRunResult]:
        return [m for m in self.models if m.status == "failed"]

    @property
    def failed_tests(self) -> list[dict[str, Any]]:
        return [
            t
            for m in self.models
            for t in m.tests
            if not t.get("passed") and t.get("severity") == "error"
        ]

    def row_accounting(self) -> dict[str, int]:
        """Count-proven ledger — never invent per-row cells we did not read."""
        written = sum(m.rows_affected for m in self.models if m.rows_affected >= 0)
        quarantined = 0
        for model in self.models:
            for test in model.tests:
                if test.get("passed") or test.get("severity") != "error":
                    continue
                try:
                    quarantined += max(int(test.get("failing_rows") or 0), 0)
                except (TypeError, ValueError):
                    continue
        return {
            "models_run": len(self.models),
            "rows_written": written,
            "rows_quarantined": quarantined,
            "tests_failed": len(self.failed_tests),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "seconds": round(self.seconds, 3),
            "plan": self.plan,
            "error": self.error,
            "warnings": list(self.warnings),
            "models": [m.to_dict() for m in self.models],
            "model_count": len(self.models),
            "failed_model_count": len(self.failed_models),
            "failed_test_count": len(self.failed_tests),
            "row_accounting": self.row_accounting(),
        }


class TransformRunner:
    """Compile and execute models against one destination."""

    def __init__(
        self,
        dest_cfg: dict[str, Any],
        *,
        dialect: str = "",
        schema: str = "",
        source_table: str = "",
        dry_run: bool = False,
        project_id: str = "",
        workspace_id: str = "",
    ) -> None:
        self.cfg = dest_cfg or {}
        self.dialect = normalize_driver(dialect or self.cfg.get("type") or "")
        self.profile = dialect_profile(self.dialect)
        self.caps = _MATERIALIZATION_DIALECTS.get(self.dialect, _ANSI)
        resolved_schema = schema or self.cfg.get("schema") or ""
        # MySQL/SQLite have no schema namespace; forcing one produces
        # `"db"."table"` that does not resolve.
        self.schema = resolved_schema if self.profile.uses_schema else ""
        self.source_table = source_table
        self.dry_run = dry_run
        self.project_id = str(project_id or "").strip()
        self.workspace_id = str(workspace_id or "").strip()

    def transform_job_id(self) -> str:
        """Stable DLQ / Inspect id — empty when this run has no project."""
        return f"xform-{self.project_id}" if self.project_id else ""

    # ---------------------------------------------------------------- naming

    def relation_for(self, model_name: str) -> str:
        """Fully-qualified, quoted relation name for a model."""
        return quote_table_ref(
            model_name,
            self.schema or None,
            dialect=self.dialect or "ansi",
            dataset=self.schema or None if self.dialect == "bigquery" else None,
            project=self.cfg.get("project") or None if self.dialect == "bigquery" else None,
        )

    def quote_column(self, column: str) -> str:
        """Quote a column for this dialect (backticks on MySQL, brackets on MSSQL)."""
        return quote_sql_identifier(column, _quote_char(self.profile.quote))

    def source_relation(self, source_name: str) -> str:
        """Relation for ``{{ source('x') }}`` — a table the transfer landed."""
        return quote_table_ref(
            source_name,
            self.schema or None,
            dialect=self.dialect or "ansi",
        )

    def compile_sql(self, model: TransformModel, models: dict[str, TransformModel]) -> str:
        """Replace ``ref()`` / ``source()`` with quoted relations.

        Ephemeral upstreams are inlined as a subquery rather than referenced,
        which is what makes them ephemeral — they never exist at the
        destination, so a plain relation name would not resolve.
        """
        sql = model.sql
        for ref_name in extract_refs(sql):
            upstream = models.get(ref_name)
            if upstream is not None and upstream.materialization == "ephemeral":
                replacement = f"({self.compile_sql(upstream, models)})"
            else:
                replacement = self.relation_for(ref_name)
            sql = _replace_ref(sql, ref_name, replacement)
        for src_name in extract_sources(sql):
            sql = _replace_source(sql, src_name, self.source_relation(src_name))
        return sql

    # -------------------------------------------------------------- emit DDL

    def build_statements(
        self, model: TransformModel, models: dict[str, TransformModel]
    ) -> list[str]:
        """SQL statements that materialize one model, in execution order."""
        body = self.compile_sql(model, models)
        relation = self.relation_for(model.name)

        if model.materialization == "ephemeral":
            return []

        caps = self.caps
        term = caps.terminator

        if model.materialization == "view":
            if caps.or_replace_view:
                return [f"CREATE OR REPLACE VIEW {relation} AS {body}{term}"]  # nosec B608
            # No OR REPLACE. The DROP/CREATE pair leaves a window where the view
            # does not exist; that is a property of the dialect, and callers are
            # told rather than led to believe the swap was atomic.
            drop = (
                f"DROP VIEW IF EXISTS {relation}{term}"
                if caps.drop_view_if_exists
                else f"DROP VIEW {relation}{term}"
            )
            return [drop, f"CREATE VIEW {relation} AS {body}{term}"]  # nosec B608

        if model.materialization == "table":
            if caps.or_replace_table:
                return [f"CREATE OR REPLACE TABLE {relation} AS {body}{term}"]  # nosec B608
            drop = (
                f"DROP TABLE IF EXISTS {relation}{term}"
                if caps.drop_table_if_exists
                else f"DROP TABLE {relation}{term}"
            )
            if caps.ctas:
                return [drop, f"CREATE TABLE {relation} AS {body}{term}"]  # nosec B608
            if caps.select_into:
                # T-SQL: SELECT ... INTO both creates and populates.
                return [
                    drop,
                    f"SELECT * INTO {relation} FROM ({body}) AS _df_src{term}",  # nosec B608
                ]
            raise UnsupportedMaterializationError(
                f"Dialect '{self.dialect}' supports neither CREATE TABLE AS SELECT "
                f"nor SELECT INTO, so model '{model.name}' cannot be materialized "
                "as a table. Use materialization 'view'."
            )

        if model.materialization == "incremental":
            # Preview form. Execution replaces it with a name-aligned load once
            # the destination catalog can be read — see resolve_alignment.
            return self._incremental_statements(model, relation, body, None)

        raise UnsupportedMaterializationError(
            f"Unsupported materialization {model.materialization!r}"
        )

    def _create_if_absent(self, relation: str, body: str, model_name: str) -> str:
        """Seed statement so first run and steady state share one code path.

        The seed creates the *shape* only. A `CREATE TABLE ... AS <body>` that
        also materialized the rows made the first run load everything twice for
        the append strategy — the CTAS wrote the batch and the INSERT that
        follows wrote it again — while every later run was correct, so the
        duplication only ever appeared in the very first load of a new model.
        `WHERE 1 = 0` keeps one loading statement per run on every dialect.
        """
        caps = self.caps
        term = caps.terminator
        if caps.create_table_if_not_exists and caps.ctas:
            return (
                f"CREATE TABLE IF NOT EXISTS {relation} AS "  # nosec B608
                f"SELECT * FROM {self._aliased(body, '_df_seed')} WHERE 1 = 0{term}"
            )
        if caps.select_into:
            # Empty seed: WHERE 1=0 gives the shape without the rows, then the
            # incremental step below loads them.
            return (
                f"IF OBJECT_ID('{_unquoted_object_name(relation)}') IS NULL "
                f"SELECT * INTO {relation} FROM ({body}) AS _df_seed WHERE 1=0{term}"  # nosec B608
            )
        if caps.ctas:
            # Oracle: no IF NOT EXISTS. A plain CTAS would fail on the second
            # run, so refuse rather than emit something that breaks on day two.
            raise UnsupportedMaterializationError(
                f"Dialect '{self.dialect}' has no CREATE TABLE IF NOT EXISTS, so "
                f"incremental model '{model_name}' cannot be seeded idempotently. "
                "Use materialization 'table' or pre-create the target."
            )
        raise UnsupportedMaterializationError(
            f"Dialect '{self.dialect}' cannot create the target for incremental "
            f"model '{model_name}'."
        )

    def _incremental_statements(
        self,
        model: TransformModel,
        relation: str,
        body: str,
        alignment: "ColumnAlignment | None",
    ) -> list[str]:
        """Incremental load. First run creates; later runs merge or append.

        ``merge`` is implemented as delete-then-insert on every dialect, and
        that is deliberate. The ``UPDATE SET *`` shorthand a native ``MERGE``
        would need is Databricks and Spark syntax only — Snowflake, BigQuery,
        Oracle and SQL Server all reject it. Delete-then-insert is dbt's own
        ``delete+insert`` strategy, is idempotent on the unique key, and works
        everywhere. The physical strategy is reported back so nobody has to
        guess which ran.

        ``alignment`` names the columns on both sides. Without it the INSERT
        would bind positionally, which is only correct while the model's output
        order happens to equal the target's — not after someone pre-created the
        mart, added a column, or reordered the SELECT. ``None`` is the preview
        case; execution always passes a resolved alignment.

        The statements are executed inside one transaction by the caller. Split
        across two, a crash between them would delete rows and never reinsert.
        """
        term = self.caps.terminator
        create_if_absent = self._create_if_absent(relation, body, model.name)
        columns = alignment.insert_clause(self) if alignment else ""
        projection = alignment.select_list(self) if alignment else "*"

        if model.incremental_strategy == "append":
            if alignment is None:
                return [
                    create_if_absent,
                    f"INSERT INTO {relation} {body}{term}",  # nosec B608
                ]
            return [
                create_if_absent,
                (
                    f"INSERT INTO {relation} {columns} "  # nosec B608
                    f"SELECT {projection} FROM {self._aliased(body, '_df_new')}{term}"
                ),
            ]

        key = self.quote_column(model.unique_key)
        delete = (
            f"DELETE FROM {relation} WHERE {key} IN "  # nosec B608
            f"(SELECT {key} FROM {self._aliased(body, '_df_new')}){term}"
        )
        if alignment is None:
            return [
                create_if_absent,
                delete,
                (
                    f"INSERT INTO {relation} "  # nosec B608
                    f"SELECT * FROM {self._aliased(body, '_df_new')}{term}"
                ),
            ]
        return [
            create_if_absent,
            delete,
            (
                f"INSERT INTO {relation} {columns} "  # nosec B608
                f"SELECT {projection} FROM {self._aliased(body, '_df_new')}{term}"
            ),
        ]

    def _aliased(self, body: str, alias: str) -> str:
        """``(body) AS alias``, minus the ``AS`` on dialects that reject it.

        Oracle raises ORA-00933 on a table alias introduced with ``AS``.
        """
        keyword = "" if self.dialect == "oracle" else "AS "
        return f"({body}) {keyword}{alias}"

    # ------------------------------------------------------------- alignment

    def resolve_alignment(
        self, conn: Any, model: TransformModel, body: str
    ) -> "ColumnAlignment":
        """Match the model's output columns to the target's, by name.

        Both sides are read for real: the model's columns from a zero-row
        execution of its own body, the target's from the destination catalog.
        Anything that cannot be matched is refused here, before the load, with
        the column named — a column the target does not have, a required column
        nothing fills, or a unique key missing from either side.
        """
        model_columns = self._model_output_columns(conn, body, model.name)
        target = self._target_columns(conn, model)

        by_name = {name.lower(): meta for name, meta in target.items()}
        unknown = [c for c in model_columns if c.lower() not in by_name]
        if unknown:
            raise TransformAlignmentError(
                f"Model '{model.name}' produces column(s) "
                f"{', '.join(unknown)} that its target table does not have. "
                "Add them to the target, or drop them from the model — an "
                "incremental load must not guess where a new column belongs."
            )

        produced = {c.lower() for c in model_columns}
        unfilled_required = [
            name
            for name, meta in target.items()
            if name.lower() not in produced and _is_required(meta)
        ]
        if unfilled_required:
            raise TransformAlignmentError(
                f"Target of model '{model.name}' requires column(s) "
                f"{', '.join(unfilled_required)} that the model does not "
                "produce, and they have no default. Select them in the model, "
                "or give the column a default at the destination."
            )

        if model.incremental_strategy != "append":
            key = model.unique_key.lower()
            if key not in produced:
                raise TransformAlignmentError(
                    f"Model '{model.name}' declares unique_key "
                    f"'{model.unique_key}' but does not select it, so the "
                    "delete+insert step would not be idempotent."
                )
            if key not in by_name:
                raise TransformAlignmentError(
                    f"Target of model '{model.name}' has no column "
                    f"'{model.unique_key}' to match the declared unique_key."
                )

        pairs = [(by_name[c.lower()]["name"], c) for c in model_columns]
        return ColumnAlignment(pairs=tuple(pairs))

    def _model_output_columns(self, conn: Any, body: str, model_name: str) -> list[str]:
        """Column names the model body actually produces, in its own order."""
        import sqlalchemy as sa

        probe = f"SELECT * FROM {self._aliased(body, '_df_probe')} WHERE 1 = 0"  # nosec B608
        try:
            result = conn.execute(sa.text(probe))
            columns = [str(k) for k in result.keys()]
        except Exception as exc:
            raise TransformAlignmentError(
                f"Model '{model_name}' output columns could not be read "
                f"({exc}), so its load cannot be aligned to the target by name."
            ) from exc
        if not columns:
            raise TransformAlignmentError(
                f"Model '{model_name}' reported no output columns."
            )
        return columns

    def _target_columns(
        self, conn: Any, model: TransformModel
    ) -> dict[str, dict[str, Any]]:
        """Destination catalog columns of the model's target, keyed by name."""
        import sqlalchemy as sa

        from services.dialect_profiles import fold_identifier

        physical = fold_identifier(self.dialect, model.name) or model.name
        try:
            inspector = sa.inspect(conn)
            columns = inspector.get_columns(physical, schema=self.schema or None)
        except Exception as exc:
            raise TransformAlignmentError(
                f"Target table of model '{model.name}' could not be read from "
                f"the destination catalog ({exc}). An incremental load is "
                "refused rather than written positionally."
            ) from exc
        if not columns:
            raise TransformAlignmentError(
                f"Target table of model '{model.name}' reported no columns."
            )
        return {str(c["name"]): c for c in columns}

    def physical_strategy(self, model: TransformModel) -> str:
        """The strategy actually used, which may differ from the declared one."""
        if model.materialization != "incremental":
            return ""
        if model.incremental_strategy == "append":
            return "append (at-least-once; re-runs duplicate rows)"
        return "delete_insert (idempotent on unique_key)"

    # ------------------------------------------------------------ data tests

    def build_test_sql(self, model: TransformModel, test: Any) -> str:
        """A single ``SELECT count(*)`` that returns the violation count.

        Zero means the test passed. Returning a count rather than the rows
        keeps the check O(1) in transferred bytes regardless of table size.
        """
        relation = self.relation_for(model.name)
        col = self.quote_column(test.column) if test.column else ""

        if test.test_type == "not_null":
            return f"SELECT COUNT(*) FROM {relation} WHERE {col} IS NULL"  # nosec B608

        if test.test_type == "unique":
            return (
                f"SELECT COUNT(*) FROM (SELECT {col} FROM {relation} "  # nosec B608
                f"WHERE {col} IS NOT NULL GROUP BY {col} HAVING COUNT(*) > 1"
                ") AS _df_dupes"
            )

        if test.test_type == "accepted_values":
            literals = ", ".join(_sql_literal(v) for v in test.values)
            return (
                f"SELECT COUNT(*) FROM {relation} "  # nosec B608
                f"WHERE {col} IS NOT NULL AND {col} NOT IN ({literals})"
            )

        if test.test_type == "positive":
            return f"SELECT COUNT(*) FROM {relation} WHERE {col} <= 0"  # nosec B608

        if test.test_type == "relationships":
            parent = self.relation_for(test.to_model)
            parent_col = self.quote_column(test.to_column)
            child_col = self.quote_column(test.column) if test.column else parent_col
            return (
                f"SELECT COUNT(*) FROM {relation} AS _df_c "  # nosec B608
                f"WHERE _df_c.{child_col} IS NOT NULL AND NOT EXISTS "
                f"(SELECT 1 FROM {parent} AS _df_p "
                f"WHERE _df_p.{parent_col} = _df_c.{child_col})"
            )

        raise ValueError(f"Unsupported test type {test.test_type!r}")

    # ------------------------------------------------------------- execution

    def run(
        self,
        models: list[TransformModel],
        *,
        select: list[str] | None = None,
    ) -> TransformRunResult:
        """Plan then execute. Planning errors surface before anything runs."""
        started = time.perf_counter()
        result = TransformRunResult()

        plan = build_plan(models, select=select)
        result.plan = plan.to_dict()

        # An undefined ref is a plan-time failure raised by build_plan, not a
        # warning: it never reaches here.
        if not plan.layers:
            result.status = "skipped"
            result.seconds = time.perf_counter() - started
            return result

        logger.info("Transformation plan — %s", describe_plan(plan))

        if self.dry_run:
            for name in plan.order:
                model = plan.models[name]
                stmts = self.build_statements(model, plan.models)
                result.models.append(
                    ModelRunResult(
                        name=name,
                        materialization=model.materialization,
                        status="skipped",
                        relation=self.relation_for(name) if model.is_materialized else "",
                        sql=";\n".join(stmts),
                    )
                )
            result.status = "skipped"
            result.seconds = time.perf_counter() - started
            return result

        engine = self._engine()
        try:
            failed: set[str] = set()
            for layer in plan.layers:
                for name in layer:
                    model = plan.models[name]
                    # A model whose upstream failed would read a stale or
                    # missing relation. Skipping is the honest outcome;
                    # running it would produce silently wrong data.
                    blocked = [r for r in model.refs if r in failed]
                    if blocked:
                        result.models.append(
                            ModelRunResult(
                                name=name,
                                materialization=model.materialization,
                                status="skipped",
                                error=(
                                    "Upstream model(s) "
                                    f"{', '.join(sorted(blocked))} failed, so this "
                                    "model was not run against stale data."
                                ),
                            )
                        )
                        failed.add(name)
                        continue
                    run = self._run_one(engine, model, plan.models)
                    result.models.append(run)
                    if run.status == "failed":
                        failed.add(name)
        finally:
            try:
                release_engine(engine)
            except Exception as exc:
                logger.debug("engine dispose failed: %s", exc, exc_info=exc)

        result.seconds = time.perf_counter() - started
        if result.failed_models:
            result.status = (
                "failed"
                if len(result.failed_models) == len(result.models)
                else "partial"
            )
            result.error = "; ".join(
                f"{m.name}: {m.error}" for m in result.failed_models[:3]
            )
        elif result.failed_tests:
            result.status = "partial"
            first = result.failed_tests[0]
            result.error = (
                f"{len(result.failed_tests)} data test(s) failed, including "
                f"{first.get('model')}.{first.get('column')} "
                f"{first.get('test_type')}"
            )
        self._persist_transform_quarantine(result)
        return result

    def _persist_transform_quarantine(self, result: TransformRunResult) -> None:
        """Same DLQ as transfer — one finding per failing error-severity test.

        Tests return COUNT(*), not cell bodies. We persist the count-proven
        finding; we do not invent per-row payloads we did not read.
        """
        job_id = self.transform_job_id()
        if not job_id or self.dry_run:
            return
        details: list[dict[str, Any]] = []
        for model in result.models:
            for test in model.tests:
                if test.get("passed") or str(test.get("severity") or "error") != "error":
                    continue
                try:
                    failing = max(int(test.get("failing_rows") or 0), 0)
                except (TypeError, ValueError):
                    failing = 0
                message = str(test.get("message") or "") or (
                    f"{test.get('test_type')} failed on {model.name}"
                )
                details.append(
                    {
                        "reason": message,
                        "message": message,
                        "column": test.get("column") or "",
                        "failure_reason": message,
                        "original_value": failing,
                        "expected_type": str(test.get("test_type") or "test"),
                        "actual_type": "violating_row",
                        "job_id": job_id,
                        "connector": self.dialect,
                        "source": "transform",
                        "model": model.name,
                    }
                )
        if not details:
            return
        self._ensure_transform_inspect_job(job_id, result, details)
        try:
            from services.quarantine_dlq import persist_rejected_rows

            persist_rejected_rows(
                job_id=job_id,
                rejected_details=details,
                workspace_id=self.workspace_id,
                source="transform",
                connector=self.dialect,
            )
        except Exception as exc:
            logger.warning(
                "Transform quarantine persist failed for %s: %s", job_id, exc
            )
            result.warnings.append(
                f"Transform test findings could not be written to the quarantine DLQ: {exc}"
            )

    def _ensure_transform_inspect_job(
        self,
        job_id: str,
        result: TransformRunResult,
        details: list[dict[str, Any]],
    ) -> None:
        """Mint a lightweight job so Inspect Quarantine can open the same DLQ."""
        try:
            from services.mongodb_service import get_mongodb_service

            svc = get_mongodb_service()
            ledger = result.row_accounting()
            quarantined = int(ledger.get("rows_quarantined") or 0)
            status = (
                "completed_with_quarantine"
                if quarantined > 0
                else "failed"
                if result.status == "failed"
                else "completed"
            )
            existing = svc.get_job(job_id)
            if not existing:
                svc.create_transfer_job(
                    {
                        "_id": job_id,
                        "name": f"Transform · {self.project_id}",
                        "source_type": "transform",
                        "source_name": self.project_id,
                        "destination_type": self.dialect or "sql",
                        "destination_database": str(self.cfg.get("database") or ""),
                        "destination_collection": "",
                        "workspace_id": self.workspace_id,
                        "operation": "transform",
                        "triggered_by": "transform_run",
                    }
                )
            svc.update_job_status(
                job_id,
                status,
                rejected_rows=quarantined,
                rejected_details=details[:200],
                rejected_details_total=len(details),
                row_accounting=ledger,
                records_processed=int(ledger.get("rows_written") or 0),
            )
        except Exception as exc:
            logger.debug("Transform inspect job not persisted: %s", exc, exc_info=exc)

    def _engine(self) -> Any:
        from connectors.generic_sql import get_sqlalchemy_engine

        return get_sqlalchemy_engine(self.cfg)

    def _run_one(
        self,
        engine: Any,
        model: TransformModel,
        models: dict[str, TransformModel],
    ) -> ModelRunResult:
        import sqlalchemy as sa

        started = time.perf_counter()
        run = ModelRunResult(
            name=model.name,
            materialization=model.materialization,
            relation=self.relation_for(model.name) if model.is_materialized else "",
            strategy=self.physical_strategy(model),
        )
        try:
            incremental = model.materialization == "incremental"
            statements = self.build_statements(model, models)
            run.sql = ";\n".join(statements)
            if not statements:
                # Ephemeral: nothing to execute, and nothing to test either.
                run.status = "success"
                run.seconds = time.perf_counter() - started
                return run

            with engine.connect() as conn:
                rows = -1
                # One transaction for the whole model. Incremental models issue
                # DELETE then INSERT; committing between them would leave the
                # deleted rows gone and the replacements never written.
                with conn.begin():
                    if incremental:
                        # Runs the seed itself and records the executed SQL,
                        # returning only the statements still to run.
                        statements = self._aligned_incremental_statements(
                            conn, model, models, run
                        )
                    for stmt in statements:
                        cursor = conn.execute(sa.text(stmt))
                        count = getattr(cursor, "rowcount", None)
                        if isinstance(count, int) and count >= 0:
                            rows = count
                run.rows_affected = rows
                # Tests run after the commit so they observe what an operator
                # would see, not uncommitted state inside our own transaction.
                run.tests = self._run_tests(conn, model)
        except Exception as exc:
            run.status = "failed"
            run.error = str(exc)
            logger.warning(
                "Transformation model '%s' failed: %s", model.name, exc, exc_info=exc
            )
        run.seconds = time.perf_counter() - started
        return run

    def _aligned_incremental_statements(
        self,
        conn: Any,
        model: TransformModel,
        models: dict[str, TransformModel],
        run: ModelRunResult,
    ) -> list[str]:
        """Seed the target, then build the load with an explicit column list.

        The seed has to run before the alignment can be read, because on the
        first run the target does not exist yet. Everything after it names its
        columns, so a reordered target, an added column or a pre-created mart
        can no longer shift values into neighbouring columns.
        """
        import sqlalchemy as sa

        body = self.compile_sql(model, models)
        relation = self.relation_for(model.name)
        seed = self._create_if_absent(relation, body, model.name)
        conn.execute(sa.text(seed))

        alignment = self.resolve_alignment(conn, model, body)
        run.column_alignment = alignment.to_dict()
        load = self._incremental_statements(model, relation, body, alignment)[1:]
        run.sql = ";\n".join([seed, *load])
        return load

    def _relation_columns(self, conn: Any, model: TransformModel) -> set[str] | None:
        """Lower-cased column names of a model's relation, or None if unknown.

        Needed because a test naming a column that does not exist must fail
        rather than pass. SQLite resolves an unknown double-quoted identifier
        to a *string literal* instead of erroring, so
        ``WHERE "no_such_column" IS NULL`` counts zero rows and the test reports
        green forever — a typo silently disables the check. Returning None when
        introspection itself fails keeps the check advisory: an unknown schema
        must not block tests that would otherwise run.
        """
        import sqlalchemy as sa

        from services.dialect_profiles import fold_identifier

        try:
            inspector = sa.inspect(conn)
            physical = fold_identifier(self.dialect, model.name) or model.name
            columns = inspector.get_columns(physical, schema=self.schema or None)
            return {str(c["name"]).lower() for c in columns}
        except Exception as exc:
            logger.debug(
                "Could not introspect columns of %s: %s", model.name, exc, exc_info=exc
            )
            return None

    def _run_tests(self, conn: Any, model: TransformModel) -> list[dict[str, Any]]:
        import sqlalchemy as sa

        results: list[dict[str, Any]] = []
        if not model.is_materialized:
            return results
        known_columns = self._relation_columns(conn, model) if model.tests else None
        for test in model.tests:
            entry: dict[str, Any] = {
                "model": model.name,
                "test_type": test.test_type,
                "column": test.column,
                "severity": test.severity,
                "passed": True,
                "failing_rows": 0,
                "message": "",
            }
            try:
                missing = _missing_column(test, known_columns)
                if missing:
                    raise ValueError(
                        f"column '{missing}' does not exist on {model.name}. "
                        "A test naming a column the model does not produce "
                        "would report green on every run."
                    )
                sql = self.build_test_sql(model, test)
                entry["sql"] = sql
                row = conn.execute(sa.text(sql)).fetchone()
                failing = int(row[0]) if row and row[0] is not None else 0
                entry["failing_rows"] = failing
                entry["passed"] = failing == 0
                if failing:
                    entry["message"] = (
                        f"{failing:,} row(s) in {model.name} violate "
                        f"{test.test_type}"
                        + (f" on {test.column}" if test.column else "")
                    )
            except Exception as exc:
                # A test that could not run is not a pass. Saying so keeps the
                # suite honest rather than reporting green on an error.
                entry["passed"] = False
                entry["message"] = f"Test could not run: {exc}"
                logger.warning(
                    "Data test %s on %s failed to execute: %s",
                    test.test_type,
                    model.name,
                    exc,
                    exc_info=exc,
                )
            results.append(entry)
        return results


def _quote_char(style: str) -> str:
    return {"double": '"', "backtick": "`", "bracket": "[", "none": '"'}.get(style, '"')


def _is_required(column: dict[str, Any]) -> bool:
    """True when the destination will reject a row that omits this column.

    A NOT NULL column is only required when nothing else fills it: a default, a
    server-side computed value, or an identity/auto-increment sequence all make
    omission legal.
    """
    if column.get("nullable", True):
        return False
    if column.get("default") is not None:
        return False
    if column.get("server_default") is not None:
        return False
    if column.get("computed") or column.get("identity"):
        return False
    return not bool(column.get("autoincrement"))


def _missing_column(test: Any, known: set[str] | None) -> str:
    """Name of the test's column that the relation does not have, if any."""
    if not known:
        return ""
    column = str(getattr(test, "column", "") or "")
    if column and column.lower() not in known:
        return column
    return ""


def _unquoted_object_name(relation: str) -> str:
    """Strip quoting for T-SQL ``OBJECT_ID('schema.table')``, which takes a string."""
    return relation.replace("[", "").replace("]", "").replace('"', "").replace("`", "")


def _sql_literal(value: Any) -> str:
    """Single-quoted SQL literal with quote doubling.

    Only ever applied to ``accepted_values`` entries, which are operator-typed
    strings. Doubling is the portable escape across every dialect here.
    """
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _replace_ref(sql: str, name: str, replacement: str) -> str:
    import re

    pattern = re.compile(
        r"\{\{\s*ref\(\s*['\"]" + re.escape(name) + r"['\"]\s*\)\s*\}\}"
    )
    return pattern.sub(lambda _m: replacement, sql)


def _replace_source(sql: str, name: str, replacement: str) -> str:
    import re

    pattern = re.compile(
        r"\{\{\s*source\(\s*['\"]" + re.escape(name) + r"['\"]\s*\)\s*\}\}"
    )
    return pattern.sub(lambda _m: replacement, sql)
