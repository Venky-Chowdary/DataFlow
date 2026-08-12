# Brutal Deep Code Audit — Session Handover

**Branch:** `fix/engine-honesty-assurance-gaps`
**Head commit:** `2018a817` — _Close engine honesty and Gate-8 assurance gaps_
**Scope:** universal-transfer assurance engine (reconcile / Gate-8, preflight, schema
fidelity, resume, connector honesty). 22 files, +2087 / −93.
**Reviewers run this session:** Bugbot (iterated to a clean pass), Security Review
(no medium/high/critical). All findings below were fixed, not deferred.

> Read this as an adversary. The thesis of the product is **proof, not optimism**:
> a green run must mean *proven delivery of every row with certified structure*, or
> it must fail closed and say why. This document lists what was broken, why it was a
> lie, what the fix actually does, and where the honesty bar still has edges.

---

## 1. Audit thesis and method

The wedge is: **semantic mapping → preflight fail-fast → quarantine/replay →
checksum reconcile → contracts.** Every claim in that chain was treated as guilty
until proven. For each suspected gap the method was:

1. Find the **circular** or **assumed** signal (writer optimism, per-batch stamps,
   create-new assumptions, all-SKIP passes).
2. Prove it falsifies with a concrete `source × dest × schema × sync` case.
3. Fix the **algorithm** for all affected paths, not the pasted route.
4. Attach a test (node id) and re-review with Bugbot + security.

Honesty rule enforced throughout: *catalog tiles ≠ transfer-live drivers*, and
*0.001% silent loss is failure* (quarantine + surface, never drop).

---

## 2. Gaps found and closed (with root cause)

### P0 — conservation could greenlight short delivery
| Gap | Root cause (the lie) | Fix |
|---|---|---|
| Circular `source_row_count` | Source count invented from `written + held_out`, so a short read balanced itself. | Reader-side truth only: `committed_offset` / `batch_rows` / full re-scan. `src/transfer/stream.py`, `src/transfer/file_stream.py` |
| Writer checksum as source digest | Gate-8 compared the writer's own hash to itself. | `reconcile_step.py` re-fingerprints source `records`; DB source with unmeasured count **fails closed**. |
| All-SKIP preflight unlocked run | `PreflightResult` passed with zero PASS. | `packages/preflight/src/preflight/engine.py`: require ≥1 PASS and 0 BLOCK. |
| Catalog "transfer-ready" overclaim | Tiles counted without a validate gate. | `connector_capabilities.py`: `transfer_ready` requires `preflight=True` (except file sources); SFTP/email demoted. |
| Hardcoded marketing driver count | `TRANSFER_READY_DRIVERS = 44` invented. | `provenEvidence.ts`: derived from `transfer_live_driver_types()` (now **42**) + regenerate command. |

### P1 — structure claimed carried but never verified
| Gap | Root cause | Fix |
|---|---|---|
| Fidelity asserted from emitted DDL | Certificate trusted the `CREATE`, not the destination. | `schema_fidelity.py::certify_structure_on_destination` re-reads the catalog for **PK / NOT NULL / DEFAULT / UNIQUE / CHECK**. |
| Positional buffered resume | `records[skip_n:]` slice re-sent wrong rows on non-idempotent writers. | `engine.py`: idempotent full-population upsert resume. |
| Create-new assumed on existing tables | Type invention / G3 ran without `dest_table_exists`. | `mapping_pipeline.py`, `preflight/gates.py` thread `dest_table_exists`. |
| Registry overclaim | MongoDB advertised MERGE; CDC exactly-once implied. | `connector_capability_registry.py`: `supports_merge=False`, at-least-once CDC stated. |

### This session — CHECK-constraint fidelity + resume conservation (Bugbot-driven)
These were found by reviewing **my own** new code as an adversary.

1. **`physical_state_diff._normalize_predicate`** — a stray trailing `)` from the
   `CREATE TABLE` tail made a *carried* SQLite CHECK read as *dropped*; a naive
   sentinel later collapsed distinct literals (`<> 'a'` vs `<> 'b'`), which would
   have **hidden real CHECK drift**. Now a single literal-aware pass preserves
   distinct string values, neutralizes parens *inside* literals, balances stray
   parens, and strips only a true wrapping pair.
2. **MySQL CHECK probe** selected a non-existent `table_name` column → every CHECK
   forced to `unknown`. Now joins `information_schema.table_constraints`.
3. **Unmatched column-coverage** left a CHECK `carried` (false-certify). Now
   `unknown`.
4. **Cast/keyword/literal false matches** (`::text`, `CAST(x AS text)`, `note='age'`)
   could certify a column that no live CHECK constrains. Now `_strip_check_type_noise`
   blanks literals and strips casts before matching; noise tokens narrowed.
5. **SQLite scanner** counted parens inside string literals (`CHECK (x <> ')')`) and
   matched `CHECK` inside literals (`DEFAULT 'check (1)'`). Now string-literal aware
   in both the finder and the depth loop.
6. **Filtered resume** re-scan ignored `source_filter` → overstated `source_row_count`
   and mis-hashed the checksum. Now applies the filter exactly like the write path.
7. **Per-batch writer `source_row_count`** clobbered the aggregate in multi-batch
   streams. Now popped on merge; `committed_offset` (full population) wins at finalize.
8. **Resume dropped first-pass quarantine** — `written` was cumulative but
   `rejected`/`coerced_null` restarted at 0 and were never persisted, so
   `expected = source − (rejected − coerced_null) − skipped` over-expected delivery
   and **failed a correct resumed load**. Added `Checkpoint.coerced_null_rows`;
   both counters are restored on resume and persisted every checkpoint.

---

## 3. Proof artifacts (run them)

```bash
# From apps/api
python -m pytest tests/test_engine_honesty_assurance_gaps.py \
  tests/test_physical_state_diff.py \
  tests/test_property6_schema_fidelity.py \
  tests/test_file_stream_resume.py \
  tests/test_checkpoint_service.py \
  tests/test_destination_requirements_gate.py -p no:cacheprovider -q
# => 73 passed (this session)
```

Key node ids added/hardened this session:
- Conservation: `test_stream_database_sqlite.py::test_stream_sqlite_multibatch_source_count_is_committed_offset`
- Filtered resume: `test_file_stream_resume.py::test_stream_file_resume_full_rescan_respects_source_filter`
- Resume quarantine: `test_file_stream_resume.py::test_resume_restores_first_pass_quarantine_for_conservation`
- Checkpoint round-trip: `test_file_stream_resume.py::test_checkpoint_roundtrips_cumulative_quarantine_counts`
- CHECK fidelity: `test_engine_honesty_assurance_gaps.py::test_check_unmatched_coverage_is_unknown_not_carried`,
  `::test_check_column_named_like_keyword_not_falsely_matched`,
  `::test_check_coverage_not_satisfied_by_literal_value`,
  `::test_sqlite_check_scanner_skips_string_literals`,
  `::test_sqlite_check_scanner_handles_paren_inside_literal`
- Predicate normalization: `test_physical_state_diff.py::test_dropped_check_constraint_is_reported_absent`,
  `::test_check_constraint_spelling_differences_still_match`,
  `test_engine_honesty_assurance_gaps.py::test_predicate_normalizer_preserves_distinct_string_values`

Broad regression sweeps this session: ~3437 passed on the stream/reconcile/schema
surface. **Reviews:** Bugbot final pass = _no bugs_; Security Review = _no medium/high/critical_
(probe SQL parameterized end-to-end; parsers linear on catalog-sized input).

---

## 4. Known limitations / honest edges (DO NOT claim these as proven)

1. **CHECK equivalence is column-coverage, not expression-equivalence.** We certify
   that every destination column the carried predicate constrains is referenced by a
   *live* CHECK after CREATE. We deliberately do **not** assert the two predicates are
   logically equal (engines rewrite them). A destination CHECK that is *weaker* but
   still references the same column would read as `carried`. Closing this needs a
   normalized predicate AST comparison per dialect — **Planned**, not done.
2. **Resume conservation depends on durable checkpoint persistence.** The cumulative
   `rejected_rows` / `coerced_null_rows` are only correct if the checkpoint was saved
   (real `job_id`). Ad-hoc/path streams with an empty `job_id` do not persist and
   cannot prove cross-pass conservation — by design, but a real gap for those callers.
3. **File/object exports remain `unproven` (operational pass only).** No destination
   cell read-back exists; writer checksum proves bytes/count, never per-cell fidelity.
   This is intentional honesty, not a TODO to flip green.
4. **CDC/resume default is at-least-once upsert** until exactly-once is measured. The
   registry now says so; do not upgrade the wording without an integration fixture.
5. **Windows-only test teardown flakes (not logic):** 4 tests fail *only* on
   `PermissionError: [WinError 32]` inside `tempfile.py` cleanup (SQLite file handle
   not released before `TemporaryDirectory.__exit__`). Assertions pass in isolation.
   - `test_data_integrity_p0.py::test_stream_strict_fails_instead_of_silent_null[strict|maximum]`
   - `test_data_integrity_p0.py::test_stream_balanced_holds_out_bad_row_and_records_rejection`
   - `test_file_stream_path.py::test_stream_file_to_database_from_path`
   - `test_stream_database_sqlite.py::test_stream_sqlite_includes_ddl_log_and_summary`
   **Fix (next session):** close the SQLite connection (or `engine.dispose()`) in these
   tests before the temp dir is torn down. Test hygiene only — no product impact.

---

## 5. Not committed on this branch

- `apps/web/public/brand/linkedin-posts/` (untracked marketing PNGs) — intentionally
  excluded from the engineering commit. Commit separately if desired.

---

## 6. Recommended next moves (prioritized, defend then expand)

1. **CHECK predicate AST equivalence** per dialect (close limitation #1) — the last
   soft spot in structure fidelity.
2. **Fix the 4 Windows teardown flakes** so CI on Windows is deterministically green
   (limitation #5). Trend teardown errors to zero.
3. **Reconcile matrix expansion** — extend `test_property6_schema_fidelity` and the
   Gate-8 conservation cases to real Postgres/MySQL/SQL Server services (emulator or
   containers), reporting pass/fail/skip counts. Today SQLite is the deepest live path.
4. **Resume conservation for job_id-less callers** — decide whether to refuse resume
   or persist a lightweight local checkpoint (limitation #2).
5. **CDC exactly-once** — until proven with an integration fixture, keep the
   at-least-once wording locked (limitation #4).

---

## 7. One-line honesty summary

A green run on this branch now means: the reader counted the source, every row was
delivered or quarantined-and-surfaced (including across resume), and PK/NN/DEFAULT/
UNIQUE/CHECK were re-read from the destination catalog — with the remaining edges in
§4 stated as `unknown`/`unproven`/`Planned` rather than painted green.
