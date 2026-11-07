# DataFlow column for §4 Master Comparison Matrix

Filled from verified code (apps/api/services/transform_engine.py, transform_store.py,
transform_models.py, transform_runner.py, preflight_cursor_gate.py,
copy_csv_local.py, src/transfer/post_load_transform.py, src/routers/transforms_router.py,
shape_router; apps/web TransformsPage, TransferTransformCanvas, TransferPage mappings).
Have = implemented end-to-end. Partial = exists but narrower than the row's full claim.
Missing = nothing in code yet.

| # | Capability | DataFlow | Evidence |
|---|---|---|---|
|1|Field selection|Have|Per-mapping source→target carry + column select|
|2|Field rename|Have|Mapping `target` rename (src_to_tgt)|
|3|Type cast/precision|Partial|`decimal/integer/boolean/date/datetime/time` ops + `target_type`; precision/scale params not exposed|
|4|Row filter condition builder|Implemented|guided AND/OR condition groups -> expression (any function in advanced mode) + `filter_rows`/`divert_rows`/`source_filter`|
|5|Multi-way routing|Partial|`router` op — one input to named outputs by condition (first match wins), per-route `map`, `unmatched` divert/fail or a `default: true` route, per-route `targets` destination tables; stream DB→DB writes each route to its own table (`targets` name or `<base>__<route>`), Gate-8 verifies each route table independently and grades conservation (route_partitioned scope). Gap: materialized (fully-read/file) sources refuse the router by name — routes write DB destinations on the streaming path only|
|6|Aggregation GROUP BY|Implemented|`aggregate` op — group_by + sum/avg/count/min/max/first/last, end-of-stream drain, buffer cap|
|7|Joins|Implemented|`join` op — inner/left, `on {src:ref}` composite keys, inline rows or saved-connector reference, map of return columns, duplicate-key `fanout` refuse/first/allow (refuse = fail loud, never silent multiply), inner no-match drop/divert, left preserves nulls|
|8|Lookup/reference|Partial|`lookup` op enriches from inline rows or a live saved-connector table (`reference {connector_id,table,columns}` read once at run start, ≤500k rows, multi return columns via `map`, unmatched null/drop/divert/default, duplicate-key `match` first/refuse); non-paging source families (DynamoDB/ES/Redis/Kafka/Qdrant) refuse by name. Gap: design-time preview resolves references in global scope only — workspace-scoped connectors fail closed, so preview can't exercise a workspace-bound reference yet|
|9|Expression builder|Implemented|`set_value`/`derive_column` expression steps + in-flow expression assist + function-insert picker in Library|
|10|Function library|Implemented|expression language: text/numeric/date/null-safe fns + operators, validated design-time; /ops `functions` catalog feeds the pickers|
|11|Cleanse trim/case/null/replace/regex|Have|`trim/upper/lower/normalize_unicode/strip_controls/replace:`|
|12|Address verification|Missing|—|
|13|Parse split typed fields|Implemented|`split_column` into typed fields + `cast_column`/`case` + expression parse|
|14|Dedupe PK latest|Implemented|`dedupe_rows` keep first/last by order column (composite keys) + `incremental_deduped` merge|
|15|Fuzzy dedupe + consolidation|Missing|—|
|16|Split/merge columns|Implemented|`split_column` (into=[...]) + `concat_columns` merge|
|17|PII blocking|Have|`omit` + field deselection|
|18|PII hashing|Have|`hash_pii`|
|19|PII encryption|Missing|—|
|20|Masking patterns/substitution|Implemented|`mask_pii` pattern=last4|first4|email; unknown pattern refuses. Limits: no seeded/deterministic substitution (same input → same mask across runs) and no format-preserving masking depth|
|21|Policy masking by role|Missing|—|
|22|Flatten JSON/array|Implemented|`flatten_json` (depth/keys) + `unnest_json` array explode w/ caps|
|23|Nest relational→JSON|Implemented|`nest_json` groups columns into a JSON object; missing source refuses, drop option|
|24|Repeating-field unpivot|Missing|—|
|25|Unstructured→structured|Missing|—|
|26|Schema change policy|Implemented|`schema_policy` gate + drift ack + recipe input_columns contract fails closed at compile|
|27|Incremental cursor/CDC|Have|`incremental_append`/`incremental_deduped`/CDC engine + cursor-semantics verdicts|
|28|Authored merge/upsert|Partial|Sync-mode-driven merge; no authored update-column override|
|29|Surrogate keys|Implemented|`surrogate_key` op — stateful sequence across pages. Limit: the sequence is run-local (starts at `start` each run), not a persisted warehouse key pool|
|30|SCD2/history|Partial|`scd2-history` library template only prepares incoming rows (surrogate + effective_from + is_current literal) — it does not close prior versions or detect change|
|31|Soft delete|Missing|—|
|32|Sort/Rank top-N|Implemented|`sort_rows` multi-key mixed direction + `rank_rows` top/bottom-N per partition|
|33|Union|Implemented|`union` op — appends a secondary input at drain; column alignment by name, `map` renames explicitly, unmapped extras `extra` refuse (default) or drop; appended rows count as input population so in = out + diverted reconciles|
|34|DQ authored reusable rules|Implemented|transform Library = named/versioned reusable rule assets + templates; rule_compiler executable status|
|35|Design validation blocks run|Implemented|recipe.check() blocks runnability (Draft-invalid never binds) + g9 gate + validation_mode|
|36|Preview before/after per step|Implemented|/preview?per_step=1 before/after per step incl. held/diverted/drained honesty|
|37|Error metadata/reject routing|Have|quarantine + rejected_rows/coerced_null_rows counters|
|38|Quarantine user-configured|Have|quarantine + `{model}_df_quarantine` + replay|
|39|Reusable saved transform library|Implemented|Library: named/versioned recipes, params, templates, where-used, pin/follow-latest binding|
|40|One-off in-flow mapping|Have|per-mapping `transform` via uiTransformToEngine|
|41|Versioning/sharing/governance|Implemented|recipe versions + expected_version + pin vs follow-latest + where-used impact|
|42|Parameters one def many runs|Implemented|`:param` declared/defaulted on recipe, bound per-transfer, substituted at compile|
|43|Custom code Java/Python/SQL step|Missing|—|
|44|Stored proc mid-flow|Missing|—|
|45|ML/embedding/API mid-flow|Missing|—|
|46|Transaction control|Missing|—|
|47|Pre-load/in-flight execution|Have|`apply_transform` on mapped rows pre-write|
|48|Post-load warehouse ELT|Have|`post_load_transform` + `transform_runner` pushdown SQL|
|49|Transform scheduling|Partial|Transforms run post-transfer; standalone schedule not built|
|50|Pricing/gating documented|Missing|internal product|

## Build plan derived from §5/§6 (replaces earlier P1 sketch)

**Two-transform model (§5.2):**
- (A) Transform Library: named versioned recipes, ordered steps, input/output
  schema contract, `parameters` + defaults, where-used, clonable templates,
  Draft-invalid never runnable, per-step preview.
- (B) In-flow Transform step between Source and Destination: dropdown of
  library transforms, explicit version pin vs follow-latest, parameter-only
  overrides — never silent definition edits.

**P0 (ship first):** field map/rename/select ✓ mostly Have · row-filter
condition builder (AND/OR groups) · cast precision/scale + safe-cast→quarantine
· expression step + function library (trim/case/replace/regex/substr/concat/
coalesce/nullif/IIF/CASE) · SHA-256 salted hash + pattern mask + block/drop ·
per-step before/after preview + validation-blocks-run · Library→dropdown
plumbing (pin + parameters + where-used).

**P1:** dedupe keep-latest expose as transform · lookup vs reference table ·
aggregation + Sort/Rank · join two inputs · schema-drift policy bound to
transform contract · surrogate keys + SCD2 template.

**P2:** fuzzy dedupe, address verification, DQ rule assets, flatten/nest,
Union, Router, custom SQL/Python step, ML/API enrichment.

**Buffered-op scale limit (P1-5 honesty):** `dedupe_rows` (keep-last),
`aggregate`, `sort_rows`, `rank_rows` hold the population in memory up to
`_MAX_BUFFERED` = 500,000 rows; a run fails loud past the cap — there is no
disk spill yet. The Library/in-flow UI surfaces this cap when a buffered op
is added.

**Anti-patterns avoided (§5.3):** impact diff before apply; explicit
processing location; tiered palette (Essential→Advanced), not 40 steps at once.
