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
|4|Row filter simple|Partial|`source_filter` counts `rows_source_filtered`; no AND/OR group builder|
|5|Multi-way routing|Missing|—|
|6|Aggregation GROUP BY|Implemented|`aggregate` op — group_by + sum/avg/count/min/max/first/last, end-of-stream drain, buffer cap|
|7|Joins|Partial|transform_models relate refs in SQL; no in-flow Joiner step|
|8|Lookup/reference|Implemented|`lookup` op — resolved reference table, key map, unmatched null/drop/divert|
|9|Expression builder|Partial|`shape` /expression endpoint + rule_compiler NL; no validated function-language editor|
|10|Function library|Partial|~20 real ops + `replace:`/`concat:`/`default:`/`assume_timezone:` prefixes + `_ENGINE_ALIASES`|
|11|Cleanse trim/case/null/replace/regex|Have|`trim/upper/lower/normalize_unicode/strip_controls/replace:`|
|12|Address verification|Missing|—|
|13|Parse split typed fields|Missing|no split op|
|14|Dedupe PK latest|Implemented|`dedupe_rows` keep first/last by order column (composite keys) + `incremental_deduped` merge|
|15|Fuzzy dedupe + consolidation|Missing|—|
|16|Split/merge columns|Partial|`concat:` only|
|17|PII blocking|Have|`omit` + field deselection|
|18|PII hashing|Have|`hash_pii`|
|19|PII encryption|Missing|—|
|20|Masking patterns/substitution|Partial|`mask_pii`/`redact`; no pattern/substitution dictionary|
|21|Policy masking by role|Missing|—|
|22|Flatten JSON/array|Partial|`json` op parses payload; no path flatten step|
|23|Nest relational→JSON|Missing|—|
|24|Repeating-field unpivot|Missing|—|
|25|Unstructured→structured|Missing|—|
|26|Schema change policy|Have|`schema_policy` gate + drift ack flow|
|27|Incremental cursor/CDC|Have|`incremental_append`/`incremental_deduped`/CDC engine + cursor-semantics verdicts|
|28|Authored merge/upsert|Partial|Sync-mode-driven merge; no authored update-column override|
|29|Surrogate keys|Implemented|`surrogate_key` op — stateful sequence across pages|
|30|SCD2/history|Implemented|`scd2-history` library template — surrogate + effective_from + is_current over `scd2`/`mirror` modes|
|31|Soft delete|Missing|—|
|32|Sort/Rank top-N|Implemented|`sort_rows` multi-key mixed direction + `rank_rows` top/bottom-N per partition|
|33|Union|Missing|—|
|34|DQ authored reusable rules|Partial|rule_compiler statuses executable/needs_confirmation|
|35|Design validation blocks run|Have|g9 sync-contract gate + validation_mode|
|36|Preview before/after per step|Partial|shape preview/drafts exist; no per-step rows diff|
|37|Error metadata/reject routing|Have|quarantine + rejected_rows/coerced_null_rows counters|
|38|Quarantine user-configured|Have|quarantine + `{model}_df_quarantine` + replay|
|39|Reusable saved transform library|Partial|TransformProject named+versioned destination-bound; library recipes in progress|
|40|One-off in-flow mapping|Have|per-mapping `transform` via uiTransformToEngine|
|41|Versioning/sharing/governance|Partial|`expected_version` optimistic concurrency|
|42|Parameters one def many runs|Missing|—|
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

**Anti-patterns avoided (§5.3):** impact diff before apply; explicit
processing location; tiered palette (Essential→Advanced), not 40 steps at once.
