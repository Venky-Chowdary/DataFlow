# Set-based transform engine — research and plan

Status: **proposal**. No engine or UI code in this change. Owner approval is required before implementation.

This note scopes an Informatica-class set engine for DataFlow and revises the earlier 45–70 engineer-day estimate. It is written against the code that actually runs, not against the marketing surfaces.

Branch rules for the work that follows:

- This work continues on `feature/fix-qa-bug-fixes`. That is the base the owner chose. The note was first drafted on `feature/set-based-engine-plan`, which was cut from `main`. That base was wrong: `main` does not contain the shape catalog or the Transform step. Do not open `feature/set-based-engine` or another plan branch unless the owner asks.
- `feature/studio-ui-upgrade` is the other active line. It holds the flagged canvas (`studio_canvas`, `studio_schema_browser`, default off). Those flags and `canvasDocument.ts` are not on this branch. This branch and that one diverged at `b7c061ac` (this branch 19 commits ahead of the merge-base, studio 17).
- No merges and no force-push. Nothing merges without the owner's sign-off. A push waits on a green CI run for this branch.

## 1. Decision

Keep the row-local shape recipe. Add a **second document**, a set graph, that the row engine ignores until a node is marked executable by the API.

The 45–70 engineer-day figure is a fair estimate for the in-engine batch slice (join, lookup, aggregate, sort, union, router, rank, window, normalizer) if pushdown certification, mapplet versioning, and CDC interaction are left out. It is **low for the spec as written**. The full scope below is **99–135 engineer-days**. A stoppable first slice that an operator can run (heterogeneous join, cached lookup, batch aggregate, sort, union, wizard form, canvas node only after the catalog says executable) is **39–52 engineer-days**.

Delivery stays **at-least-once**. Exactly-once is not claimed. A set node that cannot account for every input row fails the run before the destination write. It does not drop the row.

## 2. What exists today

Three transform planes already exist. The set engine is a fourth plane only where the first three cannot express the operator. It does not replace them.

### 2.1 Column transforms on Map

`apps/web/src/lib/localTransform.ts` applies a few casts in the browser. Hash, IBAN, time, and boolean refuse locally and require the server (`SERVER_PREVIEW_TRANSFORMS`). The server-side column transforms live in `apps/api/services/transform_engine.py` (`apply_transform`, `infer_transform`). Ambiguous dates fail closed (`test_apply_date_fails_closed_on_ambiguous_mdy_dmy` in `apps/api/tests/test_transform_engine.py`). This plane is one value, one column. It stays there.

### 2.2 Pre-load shape recipe (studio branch)

On this branch, `feature/fix-qa-bug-fixes` (tip at the time of this correction: `5688ed21`), the Transform wizard step runs a **row-local** recipe:

- Model: `apps/api/services/shape_models.py`. A recipe is an ordered list. `recipe_hash` is the SHA-256 prefix of the canonical steps. Disabled steps are outside the hash. An empty recipe has an empty hash so a pass-through approval is not a fake identity.
- Engine: `apps/api/services/shape_engine.py`. Pure, streaming, no I/O. One row is independent of its neighbours, which is why a 50-row preview, a Validate scan, and a chunked Execute share one recipe. The ledger is `rows_read + rows_expanded = rows_shaped_out + dest_count + held_out + skipped`.
- Error policy, fail-closed by default: `refuse` (fail the run and name the row), `divert` (quarantine, keep going), `null` (write null and count it). Catalog copy is on `GET /shape/catalog` in `apps/api/src/routers/shape_router.py`.
- Preview is bounded (`MAX_PREVIEW_ROWS = 2000`) and does not read a connector. The same rows and the same recipe yield the same preview.
- Suggestions: `apps/api/services/shape_suggest.py` profiles the sample and offers a step only when the sample shows the defect (trim only when whitespace is present, scale warnings counted rather than estimated). Ambiguous day/month order is not auto-chosen.
- The catalog **refuses by name** the operators this project is about: `join`, `lookup`, `aggregate`, `group_by`, `pivot`, `unpivot`, `sort`, `order_by`, `rank`, `window`, `dedupe`, `distinct`, `running_total`, `fill_down`. `GET /shape/catalog` publishes them as `post_load_only`, with the reason that a streaming read cannot see the population.
- Apply path: `apps/api/services/shape_apply.py`. One recipe, one engine, one accounting. A hash mismatch raises rather than running a different program.

`main` does not contain `shape_models.py` or the Transform step. Both are on this branch. Post-load SQL models exist here and on `main`.

### 2.3 Post-load SQL models

`apps/api/services/transform_models.py` and `transform_runner.py` are a dbt-shaped planner: `{{ ref() }}` and `{{ source() }}`, layered DAG, materializations `view | table | incremental | ephemeral`, incremental strategies `merge | append | delete_insert`. `merge` and `delete_insert` require a `unique_key`. `append` is documented as at-least-once. Model bodies are a single `SELECT` or `WITH`; the runner owns DDL and DML.

`docs/TRANSFORM_LAYER_AUDIT.md` on the studio branch records a live proof: 33 cases, 33 ok, on PostgreSQL 16, MySQL 8, and SQL Server 2022, after two defects (positional `INSERT` column swap, and an append seed that double-loaded the first run). The same audit states what is **not** proven: Snowflake, BigQuery, Databricks, Trino, Vertica, ClickHouse, and Redshift have dialect coverage only. Transform runs have no per-row quarantine. A failed model leaves landed data intact and is not replayable per row. Concurrent runs of one project are not fenced.

That page is the right place for warehouse SQL the operator authors. It is the wrong place to pretend a visual joiner already exists.

### 2.4 Wizard, flags, canvas

`apps/web/src/pages/transfer/studioConstants.ts` on this branch fixes the step list:

1. Source
2. Destination
3. Transform
4. Map
5. Validate
6. Run

Labels and order are the operator contract. Transform sits before Map because Map chooses carriers from the columns and values it is shown.

`studio_canvas` and `studio_schema_browser` are not on this branch. On `feature/studio-ui-upgrade` they default to false in `workspace_store.py`, and the UI turns a surface on only when the server flag and the browser flag are both true (`resolveStudioFlag` in `apps/web/src/lib/studioFlags.ts`). A failed settings read is not permission. `TransferTransformStep` there takes `canvasEnabled` (default false). False renders `TransformStepBuilder`. True renders `TransferTransformCanvas`.

On this branch the Transform step always renders `TransformStepBuilder`. There is no canvas flag and no `canvasDocument.ts`.

The canvas document on the studio branch is an ordered list. Edges mean order. `classifyChainLink` refuses self, backward, and skip links. `stepsWithoutPlanOps` strips anything in `post_load_only`. `planCards` draws those names as "Runs after load as SQL" and links to `#/transforms`. `docs/design/canvas-research.md` on that branch records why: the engine runs a list, so a DAG would be a picture of a program the engine cannot run. Do not copy that canvas onto this branch to make the picture true. Set nodes appear in the wizard here when the catalog marks them executable. They appear on the canvas only after that canvas code is on the same branch as the engine.

`TransformStepBuilder` never inserts a half-typed expression into the recipe. The draft joins the list only after required options are present and `POST /shape/expression` accepts the expression.

### 2.5 Neighbouring contracts the set engine must reuse

- Quarantine rows are a recovery artifact, not a log line (`docs/QUARANTINE_ROW_CONTRACT.md`). Charter fields include `failure_reason`, `transform_attempted`, `job_id`. Durable persist fails closed without `job_id`.
- Execution delivery default is at-least-once. Exactly-once is not claimed (`docs/EXECUTION_ENGINE_CONTRACT.md`).
- Lineage events already exist: `emit_lineage`, `emit_quarantine`, `emit_reconciliation` in `apps/api/services/lineage_telemetry.py`.
- DuckDB is already a connector and a type-system citizen (`apps/api/services/type_system.py`, `apps/api/services/arrow_schema.py`). It is not a transform runtime.
- Semantic types already exist on the AI router (`list_semantic_types` and column `semantic_type` in `apps/api/src/routers/ai_router.py`). Join inference should call that, not grow a second taxonomy.

## 3. Estimate

Engineer-days below are implementation days for one engineer who already knows this repository, including the regression test for that item. They are not a calendar.

| Id | Item | Days | In the old 45–70? |
| --- | --- | ---: | --- |
| A | Set-graph IR, canonical hash, capability catalog. Row recipe unchanged when the graph is absent. | 6–8 | Yes |
| B1 | Binary join (inner, left, right, full, left semi, left anti), multi-key, heterogeneous inputs, union-all with explicit column mapping. DuckDB reference. | 8–11 | Yes |
| B2 | Connected lookup, static cache, no-match policy, multi-match policy. | 5–7 | Yes |
| B3 | Batch `GROUP BY`, sort, memory limit, spill, fail-closed on non-spillable aggregates. | 6–8 | Yes |
| C1 | Router: exclusive first-match, default output, quarantine as one legal sink. | 4–6 | Yes |
| C2 | Rank (top/bottom N per group) and batch window functions. | 6–8 | Yes |
| C3 | Normalizer for repeating groups. `unnest_json` stays on the row engine. | 4–5 | Yes |
| D | Pushdown planner and semantic diff for certified dialects. | 16–22 | No |
| E | CDC interaction, incremental group recompute, lineage, quarantine stamps. | 12–16 | No |
| F | Mapplets: parameters, immutable versions, unconnected lookup as a function. | 10–14 | No |
| G | Wizard region and canvas nodes, both catalog-gated. Flags and step labels untouched. | 12–16 | Partly (about 8–10 if only B is exposed) |
| H | Golden fixtures and a live matrix with pass/fail/skip counts. | 10–14 | Partly (about 6–8 for the B slice) |

- **Spec as written (A–H): 99–135 engineer-days.**
- **Old 45–70:** matches A–C plus a thin G and a thin H, which is the operator-visible transform list on the in-engine path. It does not include certified pushdown, mapplets, or an honest CDC story.
- **First slice an operator can run (A, B1–B3, thin G, thin H): 39–52 engineer-days.** Stop there if the owner wants a review before router, window, pushdown, or mapplets.

The expensive items are not the join algorithm. DuckDB already has hash join, external aggregation, and external sort. The cost is the contract around them: null equality, collation, tie policy, one designated output for Map, and a proof that pushdown matches the reference on a named fixture.

## 4. Research

Sources are vendor documentation unless noted. Unofficial blogs are not used as semantics. Dates below are the doc versions fetched for this note (October 2026).

### 4.1 Informatica (PowerCenter and IDMC)

Transformation types, current IDMC: [Transformation types](https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/transformations/transformation-types.html).

| Transform | Active or passive | Semantics we will copy or refuse |
| --- | --- | --- |
| Joiner | Active | Two sources, equality on one or more port pairs, types must already match. Join types are inner, master outer, detail outer, full outer. **Null does not equal null.** The integration service does not join two null keys; the documented fix is to replace nulls with a default before the join. [Defining a Join Condition](https://docs.informatica.com/data-integration/powercenter/10-5/transformation-guide/joiner-transformation/defining-a-join-condition.html). Unsorted joiner caches the master (smaller) source. |
| Lookup | Passive if one row, active if many | Connected lookup is in the pipeline and can return many ports. Unconnected lookup is called with `:LKP` from an expression and returns one port. No match on a connected lookup returns the configured default; on an unconnected lookup it returns null. Cache is static by default; a dynamic cache inserts or updates and is how PowerCenter implements "is this key already on the target". [Connected and Unconnected Lookups](https://docs.informatica.com/data-integration/powercenter/10-5-9/transformation-guide/lookup-transformation/connected-and-unconnected-lookups.html), [Lookup overview](https://docs.informatica.com/data-integration/powercenter/10-5-10/transformation-guide/lookup-transformation/lookup-transformation-overview.html). |
| Aggregator | Active | Group-by plus aggregate expressions. Historical compatibility flag `AggregateTreatNullAsZero` treats null as zero; the modern default treats null as null and ignores it in SUM/AVG the way SQL does. [Compatibility and Database Properties](https://docs.informatica.com/data-integration/common-content-for-data-integration/10-5-10/application-service-guide/powercenter-integration-service/powercenter-integration-service-properties/compatibility-and-database-properties.html). Sorted input is an optimization, not a different result. Incremental aggregation stores group state across sessions in an index file. That is a state store. We do not ship a file cache and call it incremental in the first slices. |
| Sorter | Passive | Order only, unless distinct output is enabled. Properties that matter: case sensitivity, nulls treated low, work directory (spill), cache size. |
| Rank | Active | One rank port, top or bottom, N rows, optional group-by. Case-sensitive string comparison defaults on. On an advanced cluster, comparisons are always case-sensitive, so the flag is a lie there. [Rank properties](https://docs.informatica.com/integration-cloud/data-integration-free-and-paygo/current-version/transformations/rank-transformation/defining-rank-properties.html), [advanced properties](https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/rank-transformation/advanced-properties.html). Transformation scope is Transaction or All Input. All Input drops transaction boundaries. |
| Router | Active | One input, several conditions, a default group for rows that match none. A row is tested against the groups in order. This is the Filter transformation with more than one output. |
| Union | Active | Multiple inputs, one output, aligned by port. Same behaviour as `UNION ALL`: it does not deduplicate. |
| Normalizer | Active | One output row per occurrence of a repeating group, plus a generated key. This is the COBOL/VSAM occurs case, and the relational unpivot case. |
| Mapplet | Depends on contents | Input and Output transformations are the ports. Parameters exist (rank's N can be a parameter). A mapplet is an asset with its own validity. IDMC can store an invalid mapping; we will not persist a graph the engine cannot run. |

SQL ELT mode is a different product surface. The palette shows only what the warehouse can execute. Pasting an unsupported transform fails validation. Lookup in that mode must use the same connection as the source or the target, and expression functions are the warehouse's functions, not the Informatica language. The expression editor does not validate them; a bad expression fails at runtime. [Transformations available in SQL ELT mode](https://docs.informatica.com/integration-cloud/data-integration-free-and-paygo/current-version/transformations/transformations/transformation-types/transformations-available-in-sql-elt-mode.html) lists Source, Target, Aggregator, Expression, Filter, Joiner, Lookup, Normalizer, Rank, Router, Sequence, Sorter, Union. [SQL ELT optimization](https://docs.informatica.com/integration-cloud/data-integration-free-and-paygo/current-version/tasks/mapping-tasks/sql-elt-optimization.html) pushes a mapping task to the source (`To Source`) or to source and target (`Full`). Full mode chooses, per task, whether non-pushable logic runs on the integration server, the task runs with no pushdown, or the task fails.

That last choice is the policy we should copy: **default is fail the plan**, not a silent fallback to another engine.

### 4.2 Talend Studio

[Using Inner Join](https://help.qlik.com/talend/en-US/studio-user-guide/8.0-R2026-09/using-inner-join) and [Lookup Inner Join rejection](https://help.qlik.com/talend/en-US/studio-user-guide/8.0-R2026-04/lookup-inner-join-rejection).

`tMap` is the join and the expression grid. The main flow is the preserved side. Lookups hang off it. Join model is Left Outer by default or Inner. An inner join that does not match can be caught by an output with `Catch lookup inner join reject = true`. That reject is a specific reason (the join was not established). A later output filter is a different reason. Two inner joins need two reject outputs if the operator wants to tell them apart.

Match model decides duplicate lookup keys: All matches (fan-out, the lookup becomes active), First match, or Unique match. Unique match is not a uniqueness constraint; treating the name as "error if duplicated" would be wrong. We will not ship a match mode whose result depends on unordered input. First match requires an explicit order key.

`tUnite` stacks flows. `tUniqRow` dedupes on selected keys. `tAggregateRow` groups. `tNormalize` normalizes. A mapplet is a reusable subjob. ELT components generate SQL in the warehouse; the Java components (`tMap`) run in the engine. Same split as Informatica native versus SQL ELT.

### 4.3 SSIS

[Lookup](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/lookup-transformation?view=sql-server-ver17), [error outputs](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/error-handling-in-data?view=sql-server-ver17), [Conditional Split](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/conditional-split-transformation?view=sql-server-ver17), [Union All](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/union-all-transformation?view=sql-server-ver17), [SCD wizard](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/configure-outputs-using-the-slowly-changing-dimension-wizard?view=sql-server-ver17).

Lookup is an equi-join against a reference set. No match is an error by default, or a No Match output if configured. Multiple matches return one row (the first in cache order) and warn when the full cache is loaded. Cache order is not a business rule. We refuse that default.

Error output policy is Fail component, Ignore failure, or Redirect row, with `ErrorCode` and `ErrorColumn`. Redirect row is our `divert`. Ignore failure is our `null` and must be counted. Fail component is our `refuse`.

Conditional Split evaluates conditions in order and sends the row to the first true output, plus a required default. Union All appends inputs; it does not sort and does not dedupe; mapped columns must share a type. Merge Join is the sorted-input alternative. We will not require a prior sort (ADF and Informatica's unsorted joiner do not either). The sort, when the operator asked for ordered output, is an explicit Sorter.

The SCD wizard classifies each column as Changing (type 1, overwrite), Historical (type 2, new row, current-row flag or a from/to pair), or Fixed (fail or ignore if it changes). It has up to six outputs. We will not build that wizard in this scope. See migration rules.

### 4.4 Azure Data Factory mapping data flows

[Transformation overview](https://learn.microsoft.com/en-us/azure/data-factory/data-flow-transformation-overview), [Join](https://learn.microsoft.com/en-us/azure/data-factory/data-flow-join), [Window](https://learn.microsoft.com/en-us/azure/data-factory/data-flow-window), [Rank](https://learn.microsoft.com/en-us/azure/data-factory/data-flow-rank), [Assert](https://learn.microsoft.com/en-us/azure/data-factory/data-flow-assert), [error rows](https://learn.microsoft.com/en-us/azure/data-factory/how-to-data-flow-error-rows).

Join types include inner, left, right, full outer, and cross. Keys do not need to be pre-sorted; Spark picks a broadcast or a shuffle join. Non-equi joins require a broadcast. Self-join is an aliased branch, not a special node.

Window is a SQL `OVER` clause (`rank`, `denseRank`, `lead`, `lag`, `cumeDist`, aggregates) with an optional partition. Rank is a separate, cheaper node for a full-stream ranking, with an explicit dense flag and a case-insensitive flag. We should keep Rank and Window as two nodes for the same reason: top-N per group is not "add a rank column".

Alter Row tags insert, update, delete, and upsert. A CDC source can drive the sink's update method without Alter Row because the row markers already exist. Assert rules fail the flow, or the sink writes the failing rows aside. Continue-on-error for driver errors is documented for Azure SQL and Synapse sinks only. Everywhere else, ADF expects the flow to split good and bad rows itself. That matches our rule: quarantine is a named sink, not a connector accident.

### 4.5 AWS Glue Studio

[Join](https://docs.aws.amazon.com/glue/latest/dg/transforms-configure-join.html) supports inner, left, right, outer, left semi, and left anti, with several equality keys. Name collisions are resolved by prefixing the right side (`right`). [Drop Duplicates](https://docs.aws.amazon.com/glue/latest/dg/transforms-drop-duplicates.html) is Spark `dropDuplicates`: case-sensitive, values read as strings, **the first row is kept**. Which row is first is a property of the partition, not a survivorship rule. Relationalize (flatten a nested frame into relational tables) is a script transform, not a reliable visual node. Visual jobs do not have a reusable component library; orchestration of failures belongs in Step Functions. We should not copy that split. Mapplets are in scope precisely because Glue visual mode lacks them.

### 4.6 dbt

[Incremental strategies](https://docs.getdbt.com/docs/build/incremental-strategy), [snapshots](https://docs.getdbt.com/docs/build/snapshots), [incremental versus snapshots](https://docs.getdbt.com/best-practices/how-we-handle-cdc/2-choosing-incremental-or-snapshots.md).

`merge` on a `unique_key` overwrites the matched row. That is SCD type 1. `delete+insert` deletes the key and inserts the new rows, which is the right tool when a window function can change earlier rows in the same partition: you recompute the affected grain and replace it. `append` inserts and does not update; dbt's own docs say it is neither type 1 nor type 2. Snapshots are type 2: `timestamp` or `check` strategy, `unique_key`, `dbt_valid_from` / `dbt_valid_to`, and a `hard_deletes` policy (`ignore`, `invalidate`, `new_record`). Tests that belong on a snapshot: the key is unique and not null on the source, `(unique_key, valid_from)` is unique, ranges do not overlap, and at most one row per key is current.

DataFlow already implements merge, append, and delete+insert as post-load strategies, and already says append is at-least-once. The set engine should compile a batch aggregate into that planner rather than invent a third incremental language.

### 4.7 Matillion

Matillion transformation jobs are pushdown SQL. The orchestration job is the control flow. On Redshift, Matillion deliberately runs one transformation SQL at a time because the warehouse is already parallel, and a single session is what gives them commit and rollback ([Parallelism with Matillion ETL for Redshift](https://docs.matillion.com/metl/docs/2789349/)). The lesson for us: a pushdown plan is one statement (or one transaction) per set graph, not a Python loop of statements that can commit halfway. Our post-load runner already refuses multi-statement model bodies. The pushdown compiler should emit one `SELECT` the runner wraps.

### 4.8 Alteryx

The Join tool emits three outputs at once: matched (J), left unmatched (L), and right unmatched (R). Summarize is the aggregate. Unique is a dedupe. Multi-Row Formula is a window with a row offset. The useful idea is that unmatched keys are data, not an error, and the operator can see them without configuring a reject port. Our outer joins already return those rows with nulls. A preview must be able to show the unmatched count so the operator does not have to infer it from nulls. We will not add three mandatory outputs; the counts are part of the node effect, and a divert output is available when the operator wants the rows physically elsewhere.

### 4.9 Airflow

[Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/ui.html) is a dependency graph of tasks that already exist in code. It is not a row engine. Operators do not join, rank, or look up. Copying it would produce a picture of a program the operator cannot change by dragging, which `docs/design/canvas-research.md` already rejects. Schedules and job precedence stay on their own pages. The set graph is not a DAG of tasks.

### 4.10 What we will not copy

- Informatica's willingness to store an invalid mapping, and SQL ELT's willingness to discover a bad expression at runtime.
- SSIS lookup's "first row in cache order" and Glue's "first row after dropDuplicates" as silent survivorship.
- Talend Unique match as if the name meant "the key is unique".
- A second control-flow canvas inside the Transform step.
- A port per column. The canvas research note is right about density. Set nodes expose inputs and outputs as streams. Column mapping lives in the inspector.
- CLAIRE-style recommendation chrome on the canvas. Suggestions stay a card the operator accepts. The canvas research note already says this, and it still holds when the suggestions are join keys.

## 5. Feature matrix

"Today" means the studio branch for the shape recipe and `main` for post-load SQL. "Plan" is this document, not a shipped capability.

| Capability | Informatica | Talend | SSIS | ADF | Glue | dbt | Today | Plan |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Inner / left / right / full join | Joiner | tMap | Merge Join (sorted) | Join (unsorted) | Join | SQL | Refused on the stream. SQL if the operator writes it. | Binary join, unsorted, DuckDB or certified pushdown |
| Left semi / anti | Lookup or filter | Inner reject | Lookup no-match | Exists | Join types | SQL | No | Join types on the same node |
| Null keys match | No | No (SQL-shaped) | No | No | No | No | n/a | No, unless `nulls_equal` is set on that node |
| Heterogeneous sources | Yes (native) | Yes (Java) | Yes | Yes (Spark) | Yes | No, one warehouse | Streaming recipe is single-stream | DuckDB when engines differ |
| Connected lookup, cached | Yes | tMap lookup | Yes, cache modes | Lookup | Join | JOIN | Refused | Static cache, policies below |
| Unconnected lookup | `:LKP`, one return, null on miss | Expression | Not the same shape | `lookup()` on a cache sink | No | Macro | No | Function on the row engine, backed by the same cache |
| Multi-match | One row, cache order | All / First / Unique | One row, cache order | Join fan-out | Join fan-out | Fan-out | n/a | Default **error**. `first_by_order` needs an order key. `all` is active and expands the ledger |
| No-match | Default value, or null if unconnected | Inner reject output | Error, or No Match output | Left join nulls | Left join nulls | Left join nulls | n/a | `refuse`, `null` (counted), `divert`, or keep (outer) |
| Group-by aggregate | Aggregator | tAggregateRow | Aggregate | Aggregate | Spark agg | SQL | Refused on the stream | Batch group-by. Nulls ignored, not treated as zero |
| Incremental aggregate | Session index file | No standard | No | Checkpointed Spark, not the same | Job bookmarks are not group state | `merge` / `delete+insert` of the grain | Post-load incremental, no group state | Recompute touched groups and merge. Session cache is Planned, not claimed |
| Sort | Sorter, spill directory | tSortRow | Sort | Sort | Spark orderBy | `ORDER BY` | Refused | Explicit nulls and collation. Spill via DuckDB |
| Rank top/bottom N | Rank | tMap + tAggregate pattern | Script or aggregate | Rank | Window + filter | `QUALIFY` | Refused | Own node. Tie policy required |
| Window | Not the Rank node | tWindow-style components | Script | Window | Spark window | SQL | Refused | Batch only. Not on an open CDC stream |
| Router / conditional split | Router, default group | tMap output filters (a row can hit several) | Conditional Split, first match, default | Conditional Split | Custom | `CASE` / separate models | `filter_rows` and `divert_rows` are single-condition | Exclusive first-match router. Fan-out is a later flag, default off |
| Union | Union, no dedupe | tUnite | Union All, types must match | Union | Union | `UNION ALL` | Refused | Union all, explicit mapping, type mismatch refuses |
| Dedupe | Sorter distinct, or aggregator | tUniqRow | Aggregate or Sort distinct | Window + filter | Drop Duplicates keeps an arbitrary first row | `unique` test, or `QUALIFY` | Refused. Tests exist post-load | Distinct-all-columns is allowed. Any other survivorship needs an order key or the node refuses |
| Normalizer / unpivot | Normalizer | tNormalize | Unpivot | Unpivot | Relationalize in script | SQL | `unnest_json` is row-local. `unpivot` is refused | Set-engine normalizer for repeating groups |
| Reusable component | Mapplet, parameters | Joblet / mapplet | No first-class data-flow template | Data flow is the unit | Visual mode has none | Packages, macros | None | Versioned mapplet. Recipe pins `name@version` |
| Pushdown | SQL ELT, or Full / To Source, fail or fallback chosen by the task | ELT components | Limited SQL command | Spark cluster, not the source warehouse | Spark | The warehouse is the engine | Post-load SQL the operator wrote | Planner. Default: fail if a node is uncertified. No silent fallback |
| Reject path | Error rows, row error log | Inner-join reject, reject flows | Fail / ignore / redirect | Assert + sink errors, continue-on-error only for some SQL sinks | Job failure | Test severity error or warn | `refuse` / `divert` / `null` on row steps. Post-load SQL has no row quarantine | Same three policies on set nodes. Side outputs must be connected or the graph is invalid |

## 6. Migration rules

These are the rules a migrated mapping must obey. They are also the rules the golden fixtures in phase H assert.

### 6.1 Types

The logical types the row engine already casts are the set engine's types: `text`, `integer`, `decimal`, `boolean`, `date`, `timestamp` (`_CAST_TYPES` in `shape_models.py`). Decimal is decimal, not binary float. The physical carrier on a warehouse stays in `type_system.py`. A pushdown plan records both. A join key whose logical types differ is a plan error, the same way Informatica rejects a condition whose ports differ in datatype. The operator fixes it with a `cast_column` step on the row recipe, which runs before the set graph.

Oracle empty string and SQL null are not collapsed. The source dialect is recorded on the input. A conversion, if the operator wants one, is an explicit `null_if` or `default_if_null` step.

### 6.2 Nulls

Join and lookup equality is SQL equality: `NULL` does not match `NULL`. DuckDB's `IS NOT DISTINCT FROM` is the implementation of an explicit `nulls_equal: true` on that node, and that flag is part of the hash ([NULL Values](https://duckdb.org/docs/lts/sql/data_types/nulls)).

Aggregates ignore nulls. `COUNT(*)` counts rows. `COUNT(column)` does not count nulls. There is no `AggregateTreatNullAsZero` unless the node sets `null_as_zero`, in which case the hash changes and the preview shows the rewritten zeros. Copying the Informatica compatibility flag as a silent default would change sums.

### 6.3 Collation and case

Default comparison is case-sensitive and binary, matching Informatica's rank default and Glue's duplicate drop. `case_insensitive: true` is explicit. Informatica's advanced cluster ignores that flag and stays case-sensitive. We publish the same honesty: if the pushdown dialect cannot honor the flag, the plan fails and names the node. The operator either drops the flag or sets `allow_engine_fallback` and runs DuckDB. Fallback is never the default.

Sort `nulls` is `first` or `last`, recorded on the node. It is not "whatever the warehouse does".

### 6.4 SCD

| Type | Meaning | Where it lives |
| --- | --- | --- |
| 1 | Overwrite the current row for the business key | Existing post-load `incremental` + `merge` + `unique_key`. A set aggregate's output grain can feed that model. |
| 2 | Keep history, one current row | Not a joiner. A later mapplet template can emit `valid_from`, `valid_to`, and a current flag using dbt's snapshot rules (`timestamp` or `check`, plus a hard-delete policy). It stays Planned until a fixture proves non-overlapping ranges. |
| 3 | Keep the previous value in an extra column | A `derive_column` step. Rare. A template, not a wizard. |

The SSIS SCD wizard's six outputs are out of scope. Shipping one that cannot expire a row or detect a fixed-attribute change would be a false control.

### 6.5 Dedupe and survivorship

`distinct` on every column is deterministic and allowed. Anything else (keep the latest address per customer, keep the highest salary) requires `order_by` plus `pick: first | last` after that order. Without an order key the node refuses. Glue's "keep the first row" and SSIS's "first cache match" are the behaviour we are refusing, because both are silent data loss when the input order is not a contract.

### 6.6 Rejects

| Incoming idea | Our policy |
| --- | --- |
| Informatica row error, SSIS Fail component | `refuse`. The run stops before the destination write for that table. The row is named. |
| SSIS Redirect row, Talend inner-join reject, ADF assert error file | `divert` onto a named output. The output is either the quarantine sink or another declared stream. Unconnected outputs are a validation error. Informatica allows unconnected ports and ignores them; we do not, because an ignored reject is a dropped row. |
| SSIS Ignore failure, Informatica default value | `null` or an explicit default. Every occurrence is counted in the node effect. |
| ADF continue-on-error | Not a default. Only the divert path continues, and only for rows the node classified. |

Preview shows the refusal the way `/shape/preview` already shows `ShapeRowError`: on the sample, before Execute. The sample banner stays. Validate remains the population check.

### 6.7 Deliberate differences from the legacy tools

- Multi-match defaults to **error**, not first-in-cache.
- Survivorship without an order key **refuses**.
- Graphs that do not run are not saved as the approved recipe.
- Expressions are compiled by our expression checker, including in pushdown mode. We do not adopt Informatica SQL ELT's "the editor does not validate".
- Router default is exclusive (SSIS, Informatica), not Talend's multi-output fan-out.
- Union does not dedupe. Dedupe is a separate node.

## 7. AI-assisted rules

The legacy tools suggest expressions or, in IDMC, show CLAIRE recommendations beside the canvas. They do not infer a join key and then write it into the mapping. dbt Cloud assists SQL in the editor; the model is still SQL the warehouse will run. None of them generate a test and a quarantine policy from the graph and then require the operator to accept both.

DataFlow already has two pieces to build on, and the new rules must use them:

- Sample profiles and ready-to-apply steps with evidence (`shape_suggest.py`). A suggestion that the sample does not support is not offered. Ambiguous dates are asked, not guessed.
- Column semantic types (`apps/api/src/routers/ai_router.py`).

Proposed rules. Each one returns evidence and a payload the operator accepts. None of them edits the approved graph by itself. None of them reports a confidence as a product-wide percentage.

1. **Join-key inference.** Candidate pairs are columns whose semantic types match and whose sample values overlap (inclusion both ways, plus the share of nulls). Name equality is a signal, not a match. A column typed as email is not paired with an integer even if the names are both `id`. The card shows overlap counts from the sample and says the sample is not the population. The operator accepts a key list, which is what the join node stores.
2. **Lookup suggestion.** Offered when one column's distinct sample is a subset of another object's column and the other object is small enough to cache under the memory budget. The card includes the no-match policy, defaulting to `divert` when the subset is imperfect and to `refuse` when the profile says the key should be complete. The operator picks.
3. **Anomaly-aware aggregates.** After a group-by preview, report groups that dominate the sample, groups of size one, and any conservation failure the aggregate can see (`sum(parts)` against a total column, counted nulls against `count(*)`). This is the same instinct as the scale suggestion that says "27 of these values need scale 9". It is a finding, not a silent rewrite.
4. **Semantic type gate.** Extend the existing logical-type profile so a set node refuses a key pair the types cannot support, with the reason on the node. The inference does not recode the column. Recoding stays a `cast_column` or `parse_date` step the operator adds.
5. **Tests generated from the graph.** A join or lookup with `multi_match: error` proposes a `unique` test on the lookup key. An aggregate proposes `not_null` on the group key when the profile found no nulls, and does not propose it when the profile found nulls. The payload is the `DataTest` model post-load SQL already runs (`unique`, `not_null`, `accepted_values`, `relationships`, `positive`). The operator accepts. A failing test fails the run at severity `error`, or reports at `warn`. We do not invent a pass rate.
6. **Migration lint, not a converter.** A later importer can read an Informatica or SSIS export. This phase only specifies the lint: null-equals-null assumptions, cache-order lookups, union mistaken for distinct, SCD wizard outputs we do not implement, and expressions that use the Informatica language rather than our expression AST. The lint lists rows it cannot carry. It does not emit a green mapping.

Out of scope for the AI rules: auto-layout as a product claim, a chatbot that writes the graph, and any score that is not a count on a named sample.

## 8. Architecture

### 8.1 Documents

```
recipe.steps[]        row-local. Today's ShapeRecipe. Hash: recipe_hash.
recipe.set_graph      optional. Absent or empty means today's behaviour.
recipe.mapplets[]     named graphs referenced by set_graph nodes, pinned by version.
```

`ShapeRecipe.parse` continues to read `steps` and reject `join` inside that list. Unknown keys on the payload must be preserved by the job store, not stripped, so an older client that only understands `steps` cannot drop the graph on save. That preservation is a phase A test. If the current store drops unknown keys, phase A fixes the store before any node is executable.

`set_hash` is a canonical hash of the graph, the pinned mapplet versions, and every semantic flag (`nulls_equal`, collation, tie policy, multi-match, no-match). It uses the same approach as `ShapeStep.canonical`: the expression AST, not the source text. An empty graph has an empty `set_hash`, so existing approvals stay valid.

### 8.2 Graph

Nodes: `join`, `lookup`, `aggregate`, `sort`, `rank`, `window`, `router`, `union`, `normalize`, `mapplet`, plus `input` and `output` ports.

Edges are stream-to-stream. A row-local recipe may sit on an input port (it prepares one source before the join). It may sit on the main output (it cleans the joined rows before Map). It may not sit in the middle of a blocking chain and pretend to be streaming.

One output is designated `main`. Map, Validate, and the writer see `main`. Every other output is a named sink: quarantine, or a second destination the route already declares. An output with no sink is invalid.

Cycles are a plan error, as they are for SQL models (`TransformCycleError`).

Router and conditional split are exclusive: each input row appears in exactly one output. The ledger can then close. Fan-out is not in the first slices.

### 8.3 Execution strategy

| Situation | Where it runs |
| --- | --- |
| Preview, always | DuckDB, on the bounded sample (`MAX_PREVIEW_ROWS`). Never the warehouse. The response keeps `preview_note` and the sample banner. |
| Validate | Plan check (types, policies, memory estimate, dialect certificate) plus the same sample preview. Validate does not claim the sample is the population. |
| Execute, every input is a table on one engine, and every node has a certified translation whose null, collation, and tie behaviour matches the reference | Pushdown. One `SELECT`. The post-load runner's materialization wraps it when the destination is that engine. Source pushdown (`To Source`) only when the destination does not need the rows to land first and the source dialect is certified. |
| Execute, inputs differ in engine, or any node is uncertified | DuckDB. Connectors stream batches in. The main output streams to the existing writer. |
| Operator set `allow_engine_fallback` | DuckDB may run a graph the warehouse cannot. The run records `execution_mode: duckdb_fallback`. Default is off, and the plan fails closed instead, matching Informatica Full SQL ELT's "fail the task" option. |
| CDC, open stream, graph contains a blocking node (join, aggregate, sort, rank, window, union of a bounded side with an unbounded side, normalizer that needs the group) | Refuse. The operator chooses a snapshot-bounded run, or a post-load incremental model. Lookup of a **bounded** reference (a cached dimension) is allowed on a CDC stream because it does not buffer the fact stream. |
| Post-load SQL the operator wrote | Unchanged. The Transforms page stays the SQL editor. A later phase may compile a graph into a `TransformModel` body. It does not replace the editor in the first slices. |

Arrow is the batch format (`arrow_schema.py` already exists). DuckDB is the engine. We do not add a second compute engine (DataFusion, Spark, Acero). DuckDB already spills join, group-by, order-by, and window ([external aggregation](https://duckdb.org/2024/03/29/external-aggregation), [larger-than-memory workloads](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads.html)). Their own docs also say some aggregates cannot spill (`list`, `string_agg`) and that several blocking operators in one query can still raise out-of-memory. Those cases fail the run before the writer, with the engine error in the job. They do not write a partial table.

Pushdown dialects for the first certificate are PostgreSQL (CI already builds a logical-replication Postgres), DuckDB itself, and the BigQuery emulator already in `.github/workflows/ci.yml`. Snowflake stays on the fake used in CI until a live account exists. MySQL is a source, not a first pushdown target: its null and collation behaviour need their own fixtures before a certificate. A dialect without a fixture is `uncertified`, and the planner will not emit it.

The semantic diff is a sample, not a proof of the population: the same fixture loaded into DuckDB and into the pushdown engine, then a count, a key checksum, and a hash of the ordered main output. A mismatch fails the certificate test. It is not papered over with a tolerance.

### 8.4 Memory and spill

- Workspace settings `set_engine_memory_limit` and `set_engine_spill_dir`. The run records the values it actually used.
- DuckDB `memory_limit` and `temp_directory` are set from those, job-scoped, and the temp directory is removed after the run.
- Preview uses a small limit and the existing row cap. A preview that would spill still returns the sample or a refusal. It does not fill the job disk and then call itself a preview.
- Lookup cache has a max byte size. Over the limit, the node refuses at plan time with the measured sample scaled up and labelled as an estimate, or it runs uncached as a join if the operator accepts `cache: false`. It does not silently half-cache.
- Python lists of the population are not the runtime. The row engine's per-row loop stays for row-local steps. The set engine sees batches.

### 8.5 Determinism

Same constraints as the shape expression language: no clock, no randomness. Plus:

- `first_by_order` and rank and survivorship name their order keys. Ties on the final key use `tie_policy: keep_all` (every row tied at the boundary is kept, and the effect reports how many extra rows that added) or `tie_policy: refuse` (the run stops if a tie would change membership). There is no `arbitrary`.
- Sort stability is not promised beyond the declared keys.
- `set_hash` and `recipe_hash` are both pinned at approval and re-checked in `shape_apply` before Execute. A mismatch raises. It does not run.

### 8.6 Incremental and CDC

CDC delivery remains at-least-once upsert of the **output grain**, which is the existing contract. The set engine does not upgrade that to exactly-once.

Blocking nodes need a bounded input. The legal bounded inputs are a full snapshot, a file, or a declared watermark window. The illegal input is "the open binlog". The refusal names the node and the two legal choices.

Incremental aggregate, when the destination already holds groups, uses `delete_insert` or `merge` on the group key for the groups present in this batch, via the existing incremental runner. Re-running a batch can rewrite those groups. It cannot rewrite groups that were not in the batch, so a window that reaches outside the batch is refused unless the operator widens the watermark (dbt's delete+insert guidance for window functions is the same constraint). Informatica's cross-session aggregate cache is **Planned**. It is not this design.

A lookup cache built from the destination (PowerCenter's dynamic cache, used as a target existence check) is **Planned**. The first lookup is a static cache of a declared reference object, rebuilt each run. Stale cache is avoided by not keeping it.

### 8.7 Lineage

Each set node emits one `emit_lineage` event: node id, op, input relation names, output grain, `execution_mode` (`duckdb`, `pushdown`, `duckdb_fallback`), `set_hash`, and whether spill occurred. Quarantine sinks call `emit_quarantine` with the same node id in `transform_attempted`.

Column collisions on a join are a plan error. The operator accepts a prefix (Glue prefixes `right` automatically; we require the accept, so Map does not see a column appear under a name nobody chose). The accepted prefix is in the hash.

### 8.8 Quarantine and errors

Set-node failures use `ShapeRowError` for `refuse` and the existing diverted-row path for `divert`. Persisted rows use `quarantine_row_contract.py` and therefore require `job_id`. The node effect extends `ShapeEffect` with, per node: rows in, rows out per output, unmatched, multi-match refusals, nulls introduced, spill flag. The conservation identity grows one term for diverted set rows and one for expanded join or normalizer rows, and a test asserts it on every fixture. A `refuse` writes zero destination rows for that table.

`null` remains the policy that discards a value, and the count is part of the effect the UI shows. It is not the default.

## 9. API contracts

Proposed. Not implemented. Names sit on the existing `/shape` router so the wizard and the canvas keep one client.

### 9.1 `GET /shape/catalog`

Existing fields stay: `operations`, `functions`, `max_steps`, `max_preview_rows`, `error_policies`, `post_load_only`.

Add:

```json
{
  "set_operations": [],
  "set_engine": {
    "available": false,
    "reference": "duckdb",
    "pushdown_dialects": []
  }
}
```

`set_operations` is empty until a node can run. Each entry, once present:

```json
{
  "op": "join",
  "summary": "Join two streams",
  "executable": true,
  "execution": ["duckdb"],
  "family": "set",
  "inputs": ["left", "right"],
  "outputs": ["main"],
  "options": ["join_type", "keys", "nulls_equal"],
  "required": ["join_type", "keys"]
}
```

`post_load_only.operations` continues to list every set op that is **not** executable. The canvas already turns that list into plan cards and strips those ops out of the row recipe. Graduating an op means removing it from `post_load_only` and adding it to `set_operations`. It does **not** mean adding it to `operations`. `operations` stays the row-local vocabulary `TransformStepBuilder` already renders. That is what keeps the old UI working on the day the catalog grows.

`executable: false` is the same as absent. The UI must not draw a node for it.

### 9.2 `POST /shape/validate`

Request gains an optional `set_graph`. Response gains `set_hash` (empty string when absent) and `set_errors`, a list of `{node, message}`. `recipe_hash` is unchanged for the same `steps`. A graph that references an op not in `set_operations` is an error, not a warning.

### 9.3 `POST /shape/preview`

Request may include `set_graph` and, for each input, `sample_rows` plus `source_columns` (the single-stream fields stay for the row recipe). Response adds:

```json
{
  "set_effect": {
    "execution_mode": "duckdb",
    "spill": false,
    "nodes": [
      {
        "id": "join_orders",
        "op": "join",
        "rows_in": {"left": 50, "right": 12},
        "rows_out": {"main": 47},
        "unmatched": {"left": 3, "right": 1},
        "refusal": null
      }
    ]
  }
}
```

`before`, `after`, `effect`, and `refusal` stay the row-recipe fields. When a set node refuses, `refusal` is that node's `ShapeRowError` shape so the existing preview panel can show it.

### 9.4 Graph document

```json
{
  "nodes": [
    {
      "id": "join_orders",
      "op": "join",
      "inputs": {"left": "orders", "right": "customers"},
      "outputs": {"main": "main"},
      "join_type": "left",
      "keys": [{"left": "customer_id", "right": "id"}],
      "nulls_equal": false,
      "on_error": "refuse"
    }
  ],
  "main_output": "main"
}
```

Lookup adds `cache: true`, `multi_match: "error" | "first_by_order" | "all"`, `no_match: "refuse" | "null" | "divert" | "default"`, `order_by` when multi-match is `first_by_order`, and `default` when no-match is `default`.

Aggregate adds `group_by`, `measures` (`sum`, `count`, `count_rows`, `min`, `max`, `avg`), and `null_as_zero` default false.

Rank adds `rank_by`, `order` (`top` or `bottom`), `n`, `group_by`, `tie_policy`.

Window adds `partition_by`, `order_by`, and a function list drawn from the certified set (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `sum`, `count`).

Router adds ordered `groups` of `{name, condition}` and `default`.

Union adds `inputs` and `mapping` of output column to `{input, column}`. A type mismatch is a validate error.

Normalize adds `groups` of repeating columns and `index_to`.

Mapplet reference: `{"op": "mapplet", "name": "dim_customer", "version": 3, "parameters": {}}`.

### 9.5 What the writer consumes

The writer's input for a table is still a stream of rows plus a column list and a hash. Phase A extends the pinned identity from `recipe_hash` to `(recipe_hash, set_hash)`. Execute refuses when either differs from the approval. The row-local ledger and the set-node ledger are both on the proof fragment `shape_apply` already attaches to the job.

## 10. UI integration

No UI code in this phase. Wizard changes, when approved, land on this branch (`feature/fix-qa-bug-fixes`), which already has `TransformStepBuilder`. Canvas changes stay on `feature/studio-ui-upgrade` until the owner puts that canvas on this line. Do not push canvas code to the studio branch from this effort.

### 10.1 Wizard

`STEPS` in `studioConstants.ts` is unchanged: Source, Destination, Transform, Map, Validate, Run. On this branch `TransformStepBuilder` is the only editor. On the studio branch `canvasEnabled` defaults false and selects the same builder.

When `set_operations` is empty, the Transform step renders exactly as it does today. No extra region, no disabled joiner, no plan-card change.

When the API marks an op executable, the Transform step gains one region under the existing recipe, titled with the op summaries from the catalog. The region edits `set_graph`. It uses the same pattern as the builder: a draft is not part of the graph until required fields validate via `/shape/validate`. `ShapeStepFields` is for row options. Set nodes need their own inspector, driven by the catalog `options` and `required` lists, not by a second hardcoded form per op copied into both surfaces.

The row builder does not list set ops, because they are not in `operations`. A search for "join" in the row picker stays empty until we deliberately add a pointer ("Join is a set step") that routes to the set region. That pointer is allowed only once `executable` is true. Before that, the existing plan card and the guide panel (`postLoadOnly`) remain the explanation.

### 10.2 Canvas

`studio_canvas` stays default off. With the flag off, nothing in this design is visible.

With the flag on, the palette gains a Set family only for `set_operations` where `executable` is true. A node is drawn for those ops only. `planCards` continues to render names still in `post_load_only`.

The row chain stays a list. `classifyChainLink` stays as it is. Set nodes are not inserted into that list, and `stepsWithoutPlanOps` keeps stripping them if a stale client sends one.

The set graph is a small DAG beside that chain: two inputs, the node, the main output feeding the rest of the recipe. Illegal edges are refused in the gesture and not stored, which is the rule the canvas already uses for the chain. Multi-input is the reason a set node cannot be faked as a chain link. The inspector is the same component the wizard region uses.

Source and Target are still not nodes. They are the wizard steps around this one. The canvas research note's decision holds.

Undo, docked sample preview, and the collapsed warehouse-SQL disclosure stay as that note specifies. The sample dock shows `set_effect` for the selected node and keeps the population banner.

### 10.3 Both surfaces, one document

The wizard region and the canvas edit the same `set_graph`. The flag chooses the view. It does not choose the program. Preview, validate, and the hash are API calls both views already share.

### 10.4 Old UI

A workspace with both flags off, and a catalog with `set_operations: []`, is the current product. A saved transfer without `set_graph` loads in the current builder. Approvals hashed before `set_hash` existed still match, because an empty graph hashes to the empty string and the row hash is untouched.

## 11. Phased plan

Each phase continues on `feature/fix-qa-bug-fixes`. Do not cut `feature/set-based-engine`. CI for a pull request into this branch is green before the push is considered done. Nothing merges without the owner's sign-off. Phase G's canvas half waits until `canvasDocument.ts` is on this branch. The wizard half can land here, because `TransformStepBuilder` is already here.

### Phase A — Contract without a node

Capability fields on `/shape/catalog` with `set_operations: []` and `set_engine.available: false`. IR objects and `set_hash`. Parser preserves an unknown `set_graph` and still rejects `join` inside `steps`. Approval identity is `(recipe_hash, set_hash)` with the empty graph equal to today's hash.

Risk: a store that drops unknown JSON keys would delete the graph on save. Prove preservation with a round-trip test before any later phase writes a node.

Days: 6–8.

### Phase B — DuckDB reference for join, union, lookup, aggregate, sort

B1 join and union, B2 lookup, B3 aggregate, sort, and spill. Heterogeneous inputs are two in-memory tables fed from sample batches, then from connector batches. Policies in section 6 are fixtures, not comments.

Risk: decimal and timestamp fidelity against the writer. The reference output is decimal, not float, and timestamps keep the source offset or fail. A second risk is DuckDB raising on multi-blocking memory; the run must fail with zero writes, covered by a test that forces a tiny `memory_limit` and a non-spillable aggregate.

Days: 19–26.

### Phase C — Router, rank, window, normalizer

Exclusive router first, then rank and window, then normalizer. `filter_rows`, `divert_rows`, and `unnest_json` stay on the row engine and keep their tests.

Risk: a router output that is not `main` and not quarantine has nowhere to go. The phase refuses that graph rather than writing a surprise table.

Days: 14–19.

### Phase D — Pushdown certificate

One compiler from the IR to SQL, reusing identifier quoting from the post-load runner. Certified dialect list starts empty and gains PostgreSQL, then BigQuery's emulator, only after the semantic-diff fixture is green. Uncertified dialects do not appear in `pushdown_dialects`.

Risk: a warehouse function that looks equivalent and is not (collation, integer division, null sort). The diff fixture is the gate. A dialect that fails it stays uncertified.

Days: 16–22.

### Phase E — CDC, incremental groups, lineage, quarantine

Blocking-node refusal on an open CDC stream. Group recompute through the existing `delete_insert` / `merge` path. Lineage and quarantine stamps. Ledger conservation test.

Risk: treating a watermark window as a full recompute and silently leaving stale groups. The effect reports groups rewritten and groups untouched. A window that references rows outside the batch fails the plan.

Days: 12–16.

### Phase F — Mapplets

Workspace-scoped store, same isolation as SQL transforms. A published version is immutable. The graph pins the version. Parameters are typed. Unconnected lookup is a function node whose body is a one-output mapplet, returning null on no match and counting the miss. Editing a draft does not change `set_hash` of a recipe that pins the published version. Publishing a new version does not either, until the operator moves the pin, which changes the hash and invalidates the approval.

Risk: a mutable "latest" pin. Do not offer it.

Days: 10–14.

### Phase G — Wizard and canvas

Only after `set_operations` contains at least one executable op. The wizard region lands on this branch. The canvas palette lands only where `TransferTransformCanvas` already exists, which today is `feature/studio-ui-upgrade`, and only when the owner says that line should receive it. Flags there stay default off. Step labels stay unchanged on both lines. Regression: with `set_operations` empty, the Transform step on this branch matches today's builder (the existing `shape` unit tests are the baseline).

Risk: drawing a node the API still rejects. The palette reads `executable` and has a test where `executable: false` yields zero set nodes.

Days: 12–16. About 8–10 if the owner stops after phase B and only those ops are shown.

### Phase H — Proof

Runs alongside B–E, not after them. Every operator fixture from section 6 is a pytest node. Live runs, where the service exists, report pass, fail, and skip. Skip reasons are written down. A green chat message is not a result.

Days: 10–14, of which 6–8 land with the first slice.

## 12. Test strategy

| Layer | What it proves | Where it lives |
| --- | --- | --- |
| Hash isolation | A transfer with no `set_graph` keeps today's `recipe_hash`. Adding a graph does not change `recipe_hash`. Changing a null flag changes `set_hash`. | Phase A unit test |
| Parser | `join` inside `steps` still raises `ShapeError`. A set op that is not executable is a validate error. | Extends `test_shape_*` on the studio branch |
| Operator golden | Null keys, multi-key, each join type, duplicate lookup keys, aggregate null versus `null_as_zero`, rank ties under both tie policies, router first-match and default, union type mismatch, normalizer occurs count | One file per op, fixture in, expected rows and effect out |
| Conservation | `rows in + expanded = main out + other outs + diverted + refused` on every fixture | Shared assertion |
| Spill | Tiny memory limit, non-spillable aggregate, zero destination rows | Integration, DuckDB |
| Pushdown diff | Same fixture, DuckDB versus PostgreSQL, counts and ordered checksum | Phase D. BigQuery emulator when that dialect is proposed |
| CDC | Open stream plus aggregate is a plan error. Bounded snapshot plus aggregate runs. Lookup of a bounded dimension on a stream runs | Phase E |
| Quarantine | Diverted set row has the charter fields and a `job_id`. Missing `job_id` fails closed | Reuses `quarantine_row_contract` tests |
| UI | Flags off: builder unchanged. `executable: false`: no canvas node. `executable: true`: node appears in both the wizard region and the canvas, and both submit the same `set_graph` | Studio branch e2e, after phase G |
| Approval | Execute with a stale `set_hash` raises and writes nothing | Extends `shape_apply` |

Preview tests assert the sample banner is still present. A test that only compares row counts is not a proof: the transform-layer audit found swaps that reconciled on counts.

## 13. Risks that cut across phases

- **Scope mistaken for the join algorithm.** The algorithms exist in DuckDB and in the warehouse. The product risk is a policy we fail to record (nulls, ties, collation) and then cannot reproduce.
- **Linear canvas.** Forcing a join into `steps` would either break `classifyChainLink` or silently drop an input. The sibling document is the mitigation.
- **Two SQL generators.** Pushdown must share quoting and the single-statement rule with `transform_runner`. A fork will drift.
- **Silent fallback.** Warehouse versus DuckDB differences are real (Informatica documents them for SQL ELT). Fallback is opt-in and recorded on the run.
- **CDC marketing.** Blocking operators on an open stream are not incremental. Saying they are would contradict `docs/EXECUTION_ENGINE_CONTRACT.md`.
- **Concurrent studio work.** Phase G waits for a stable catalog and a free moment on `feature/studio-ui-upgrade`. Landing UI first would draw nodes the engine refuses.
- **Production branch.** Any merge into `feature/fix-qa-bug-fixes` ships this unfinished. The implementation PRs name their base explicitly.

## 14. Decisions the owner is asked to confirm

1. Accept the revision: first runnable slice at 39–52 engineer-days, full spec at 99–135, and the old 45–70 as the in-engine transform list without pushdown, mapplets, or CDC.
2. Confirm the deliberate differences in section 6.7, in particular multi-match defaulting to error and survivorship refusing without an order key.
3. Confirm phase order A → B → (C and H alongside) → D → E → F, with G starting only when the first op is executable.
4. Confirm DuckDB as the only reference engine, Arrow as the batch format, and pushdown as a certificate rather than a second semantics.
5. Confirm SCD type 2 and Informatica's cross-session aggregate cache stay Planned.

Until those are confirmed, the catalog field stays unbuilt and both UIs stay as they are.
