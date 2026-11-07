# Datawrap — Client handover: Validate ≡ Execute (population fit)

**Audience:** enterprise buyer / integration architect / cutover operator  
**Date:** 2026-08-27  
**Branch:** `cursor/decimal-fit-untyped-scan-d3bf` (stamp SHA at sign-off)  
**Pack contents:** this file is the single handover. §§13–20 are the enterprise close-out (rollback, blast radius, FAQ, sign-off form).  
**Wave:** pasted Studio failure `flights-1m.csv` → Snowflake `EMPLOYEE_DB.tree` (job `6a8f4f89…05ba`)

This pack is the handover for **this wave only**. It does not recertify the whole product, does not replace `docs/CLIENT_READINESS_REPORT.md`, and does not claim warehouse or SaaS routes are live-proven on a customer tenant.

Every number below is **measured**. Where something was not run, it is named as unmeasured.

---

## 1. Decision for the buyer

**Hand over the Validate≡Execute population-fit algorithm** for file and pageable table sources: Validate now scans the same population the write will bind, and Remap names a dest-spelled widen before any row moves.

**Do not treat this as a certified Snowflake 1M-row production cutover.** The original job was not re-run against live Snowflake in this environment. The original failed job must **not** be Resumed.

**Commercial posture**

| Say this | Do not say this |
|---|---|
| Validate and Execute share one fit algorithm for bounded NUMBER / VARCHAR / integer / DATE / BOOLEAN / UUID / ENUM / SET / INTERVAL / YEAR / BIT / BINARY | “All 650+ connectors work” |
| Unfit rows are quarantined and surfaced; they are never silently truncated | “Zero data loss, exactly-once” |
| 43 unique transfer-ready drivers; 78 PRODUCTION_SKU routes on this host | “716 live connectors” |
| CDC default is at-least-once upsert until exactly-once is proven | “Exactly-once CDC” |
| 100% on a named fixture/matrix only | “100% accurate” / “client-ready for every route” |

Recommended next commercial step: accept this wave for **file → warehouse/OLTP (create-new or existing table)** after the client re-Validates **their** file on **their** Snowflake (or ALTER + new transfer). Keep warehouse/SaaS tenant proof as a separate certification wave.

---

## 2. Incident (the signal, not the scope)

Pasted Transfer Studio Run:

| Field | Measured |
|---|---|
| Source | `flights-1m.csv` (file upload) |
| Destination | Snowflake `EMPLOYEE_DB.tree` (existing table) |
| Job | `6a8f4f89…05ba` |
| Mode | strict |
| Written | **0** |
| Quarantined | **1,512** |
| Dest COUNT | unmeasured |
| Gate-8 | not reached |

**Why it failed (two stacked bugs, one algorithm class)**

1. Peek inferred `DECIMAL(9,6)` / `NUMBER(9,6)` from a short CSV sample. Validate treated that as warehouse DDL and **skipped** the 1M-row scan. First overflow at **row 293**.
2. Dest widen lived only on `population_fit.findings`. Validate Remap reads `validation_findings` from the **25-row** coercion preview, which was clean — suggested fix showed **`—`**.

Write-path quarantine was **correct** (no silent truncate). Samples `7.9166665`, `0.016666668`, `0.76666665` are **float32 n/60 clock residue**, not 15-digit Excel doubles. IEEE `looks_like_binary_residue` does not fire at this scale. The product must **not** silently quantize them.

Findings: **1,361** `DEP_TIME` vs `NUMBER(9,6)`; **151** `ARR_TIME` vs `NUMBER(10,7)`.

One pasted Snowflake error is a **signal**. The fix is the shared scan — not a Snowflake-only patch.

---

## 3. Operator action for that job (mandatory)

Do **not** Resume job `6a8f4f89…05ba`.

1. Open **Map**.
2. Widen `DEP_TIME` to **`NUMBER(10,7)`** and `ARR_TIME` to **`NUMBER(11,8)`**, or `ALTER` the live Snowflake columns to those carriers.
3. If the columns are clocks, map them as **`TIME`** instead of NUMBER.
4. **Re-Validate** (the stored `file_id` is now scanned; a 25-row preview is not population proof).
5. Start a **new** transfer. Do not Resume the failed job.
6. Auto-ambiguous `1.234` / `1.000` still has **no** widen — set a number locale or an explicit transform.

Existing-table Remap must **not** dump a NUMBER widen onto a leftover TEXT sibling. If the destination object already exists, **ALTER** (or create a dest-spelled `*_wide` column). Writing `NUMBER(10,7)` into the mapping while live DDL stays `NUMBER(9,6)` will fail again.

---

## 4. What this wave shipped (shared algorithm)

Validate now asks the **same predicates the writer uses**. A clean 25-row preview is never phrased as population proof. Evidence is `exact` / `partial` / `sampled` / `unmeasured`.

| # | Hole closed | Operator effect |
|---|---|---|
| 1 | File peek NUMBER treated as warehouse DDL | File / upload / object-store always scan. Dest-spelled widen (`NUMBER(9,6)` + `7.9166665` → `NUMBER(10,7)`), never truncate. |
| 2 | Dest widen buried under `population_fit` | Remap reads kernel findings. Honesty names the widen. No second teal primary. |
| 3 | Empty `source_kind` skipped the scan | Skip only when source types are authoritative. Integer / VARCHAR overflows stamp a widen. |
| 4 | Studio posted a 25-row preview | `/connectors/upload` persists `file_id`. Validate and Execute hydrate the same bytes. |
| 5 | Table Validate used the preview | Pageable tables use the same projected walk Execute uses. Recipes shape the walk. |
| 6 | Incremental walked the whole table | Validate judges `cursor > watermark`. CDC skips the table walk. SCD2 still snapshots. |
| 7 | `source_filter` skipped or walked the wrong set | Filter → shape → watermark. Validate judges the write subset. |
| 8 | ENUM / SET / INTERVAL undecidable | Writer `coerce_enum_wire` / interval family. Dest-spelled ENUM widen, never VARCHAR. MySQL `''` wipe named at Validate. |
| 9 | YEAR / BIT / BINARY undecidable | Writer YEAR / bit / binary bind. YEAR `0000` wipe named. Dest-spelled `BIT(n)` / `BINARY(n)`, never TEXT or BYTEA invent. |

Warehouse BOOLEAN / UUID / INTERVAL / YEAR wires still skip when the source is a declared database domain. File peek inferred types **never** skip.

---

## 5. Proof (measured on this branch)

Re-run:

```bash
PYTHONPATH=apps/api:apps/api/src python -m pytest \
  apps/api/tests/test_validate_parses_what_the_write_binds.py \
  apps/api/tests/test_decision_kernel_findings.py \
  apps/api/tests/test_population_fit_scan.py \
  apps/api/tests/test_population_fit_table_source.py \
  apps/api/tests/test_population_fit_preflight_integration.py \
  apps/api/tests/test_row_filter.py \
  apps/api/tests/test_decimal_widen_write_path.py \
  -q -k "not live_pg"

cd apps/web && npx --yes tsx --test src/lib/transferStudioChrome.test.ts src/lib/populationFit.test.ts
```

| Matrix | Result | When |
|---|---|---|
| Population-fit cluster (YEAR / BIT / BINARY inclusive) | **156 passed**, 1 live-PG walk **deselected** | 2026-08-27 |
| Chrome + `populationFit` contracts | **39 passed**, 0 failed | 2026-08-27 |
| Live Snowflake / original 1M CSV | **not run** | — |
| Live 43-driver matrix | **not run** | — |
| Combined incremental + filter + recipe on one live table | **not run as one matrix** (each piece proven separately) | — |

Artifact copies: `docs/CLIENT_HANDOVER_VALIDATE_EXECUTE.md` (this file). Cluster logs from the engineering run: `/opt/cursor/artifacts/year_binary_fit_pytest.log`, `/opt/cursor/artifacts/enum_interval_fit_proof.md`, `/opt/cursor/artifacts/source_filter_fit_proof.md`.

### Named fixtures this wave must keep green

- Late NUMBER overflow past preview (`row 431` / `row 293` class)
- Float32 clock residue dest widen `NUMBER(10,7)` — not `NUMBER(8,7)`
- Auto `1.234` has no widen
- File `file_id` scan; table projected walk; incremental watermark; `source_filter` subset
- ENUM late member → dest-spelled `ENUM(...)`; YEAR `1899` → do not store `0000`
- BINARY overflow → dest-spelled `BINARY(n)`; invalid base64 is a fix, not a TEXT widen
- JSON / JSONB / VARIANT / unbounded BYTEA stay **unmeasured** (honest)

---

## 6. Honesty inventory (this host, 2026-08-27)

Measured via `catalog_summary()` + `sku_honesty_summary()` + `TRANSFER_READY_CATALOG_IDS`.

| Claim | Measured | Meaning |
|---|---|---|
| Catalog tiles | **716** (`catalog_tile_total`) | Roadmap + aliases. **Not** transfer-live. |
| Planned (catalog status) | **651** | Roadmap tiles. |
| Unique transfer-ready drivers | **43** | Only this count is transfer-live. |
| `PRODUCTION_SKU` routes | **78** claimed, **78** sold on this host | `validate_transfer` + driver present. |
| `TRANSFER_READY_CATALOG_IDS` | **79** | Certified catalog ids (core + hosted twins). |
| Mongo `27017` | **down** (connection refused) | Jobs / Schedules persistence unproven on this box. |
| Assist type matrix `AUTHORITATIVE` | treat as **False** unless a named artifact says otherwise | Do not sell assist as certified mapping. |

Catalog tile count ≠ live drivers. Selling “716 connectors” is a false claim.

---

## 7. Residual risk register

| ID | Risk | Severity | Status | Client impact |
|---|---|---|---|---|
| R1 | Original 1M CSV not re-run on live Snowflake | High for that job | Unmeasured | Client must re-Validate on their tenant after widen/ALTER. |
| R2 | JSON / JSONB / VARIANT / ARRAY / ClickHouse ENUM8 not in population scan | Medium | Honest `undecidable` | Late malformed JSON can still fail at write. Quarantine holds; Validate will not name it first. |
| R3 | Dynamo / Kafka / Redis / Elasticsearch table walk | Medium | `sampled` / `unmeasured` | Preview-only Validate. Do not call those routes population-proven. |
| R4 | Callable / procedure sources | Medium | Preview-only by design | Incremental procedure spool is not table-exact. |
| R5 | CDC | Medium | At-least-once upsert | Changelog is the write population; table-exact Validate is skipped. Duplicates possible until exactly-once is proven. |
| R6 | Existing Snowflake DDL | High if Resume used | Operator | Mapping `target_type` does not ALTER live columns. ALTER or new column required. |
| R7 | Auto locale `1.234` / `01/02/2024` | Medium | Fail-closed | No silent invent. Operator must set locale or transform. |
| R8 | Combined incremental + filter + recipe | Low | Pieces proven separately | A stacked live matrix is not attached. |
| R9 | SOC 2 / ISO 27001 | — | No third-party certificate | Controls exist; no audit letter. |

Silent data loss remains a **product failure**. This wave does not relax write-time checks. A clean scan is evidence under declared types; the write stays authoritative.

---

## 8. Client acceptance test (their tenant)

Use **their** `flights` file (or a 1,000-row extract with the overflow **after** row 25) and **their** Snowflake `NUMBER(9,6)` column.

| Step | Pass | Fail |
|---|---|---|
| 1. Upload file, Map onto existing `NUMBER(9,6)` without widening | Validate **blocks**. Remap names `NUMBER(10,7)` (or dest-spelled equivalent). Suggested fix is not `—`. | Validate greens, or Remap shows `—`. |
| 2. Widen / ALTER, re-Validate | Validate `population_fit.evidence` is `exact` (file) or the honesty line says every scanned row. Execute unlocks only if `passed === true` and decision is `approve`. | Execute enabled on a preview-only pass. |
| 3. New transfer (not Resume) | Dest COUNT equals kept source rows. Quarantine 0 on this class. Gate-8 re-read agrees. | Resume of the old job; 0 written; dest unmeasured. |
| 4. Clock columns (optional) | Mapping as `TIME` does not invent a NUMBER widen as the primary action. | Product silently quantizes `7.9166665`. |

If the client cannot provision Snowflake to the certification environment, record **skip: no tenant credentials** — do not invent green.

---

## 9. What the operator sees (UX contract)

- One root cause → one primary control (Remap / ALTER / new transfer).
- Honesty names the dest-spelled widen. No second teal primary.
- Quarantine shows `suggested_fix` or `suggested_target_type`.
- A clean sample is never “every row fits” unless `evidence === exact`.
- CDC / unpageable sources stay honest: sampled or unmeasured, not fake-exact.

---

## 10. Next certification waves (no calendar estimates)

These are remaining **algorithm or proof** gaps, not promises.

1. Client-tenant Snowflake re-Validate + Execute of the original file (acceptance §8).
2. Driver-native bounded scan for Dynamo / Kafka / Redis / Elasticsearch.
3. Cheap JSON-document fail-closed (intended `{`/`[` that do not parse) — only if cost is bounded.
4. ARRAY element fit on the same writer predicates.
5. Combined incremental + filter + recipe live matrix.
6. Exactly-once CDC — do not advertise until proven.

---

## 11. Documents in this pack

| Document | Role |
|---|---|
| This file | Client + architect handover, including §§13–20 close-out |
| `docs/CLIENT_READINESS_REPORT.md` | Prior relational migration-assurance evidence (do not overwrite those live counts with this wave) |
| `docs/SESSION_HANDOVER.md` | Engineering continuation notes |
| Population-fit module | `apps/api/services/population_fit_scan.py` — SSOT for the scan |
| Decimal widen | `apps/api/services/decimal_observe.py` |
| Filter SSOT | `apps/api/services/row_filter.py` |
| Incremental SSOT | `apps/api/services/sync_cursor.py` |

---

## 12. Sign-off

| Role | Statement |
|---|---|
| Engineering | Algorithm closed for the named carrier families. Matrices above were run. Live Snowflake 1M was not. |
| Operator | Will not Resume `6a8f4f89…05ba`. Will Map/ALTER, re-Validate, new transfer. |
| Buyer | Accepts file/table population-fit as fail-closed preflight. Does not accept “650+ live” or “exactly-once CDC.” |

**Verdict:** ready to hand over as a **fail-closed Validate≡Execute upgrade** for file and pageable-table bounded carriers. **Not** ready to hand over as a universally certified 43-connector or live-Snowflake 1M production sign-off.

---

## 13. Scope in / scope out (SOW language)

Use this text in a statement of work or change ticket. Do not expand it in sales decks.

**In scope (this wave)**

- Fail-closed population fit on Studio Validate, plan preflight, Pilot preflight, and Execute preflight for **file uploads** (stored `file_id`) and **pageable table** sources (Postgres-class offset readers).
- Shared writer predicates for NUMBER/DECIMAL, VARCHAR, integer, DATE/TIME, BOOLEAN, UUID, ENUM/SET, INTERVAL, YEAR, BIT/BINARY.
- Dest-spelled suggested widen on Remap / Quarantine. No silent truncate. No silent MySQL ENUM `''` or YEAR `0000`.
- Incremental Validate bound to `cursor > watermark`. CDC Validate does not claim table-exact. SCD2 still snapshots.
- `source_filter` Validate bound to the kept subset (filter → shape → watermark).

**Out of scope (do not invoice or accept as done)**

- Live Snowflake re-run of `flights-1m.csv` on the client tenant (client acceptance §8).
- Certification of all 43 drivers or all 716 catalog tiles.
- Exactly-once CDC.
- Population-exact Validate for Dynamo, Kafka, Redis, Elasticsearch, callables/procedures.
- JSON / ARRAY / ClickHouse ENUM8 population scan.
- SOC 2 / ISO 27001 attestation.
- ALTER of the client’s existing Snowflake `EMPLOYEE_DB.tree` (client DBA).

---

## 14. Blast radius and compatibility

This is not a Snowflake-only patch. Every path that calls `run_file_preflight` / `scan_population_fit` changes.

| Surface | What changes | Compatibility |
|---|---|---|
| Transfer Studio Validate | Scans stored upload or pageable table, not 25 rows | Jobs that **false-greened** will now **block**. That is the intended fail-closed. |
| Execute preflight | Same scan as Validate when the population is available | Write-time quarantine unchanged and still authoritative. |
| Pilot `plan_transfer` | Receives `source_filter` and `stream_contracts` | Pilot still has no `shape_recipe`. |
| Incremental / deduped | Scan is the delta after the watermark | Historical overflows no longer false-block a second run. |
| CDC | Table walk skipped | Evidence stays sampled/unmeasured. At-least-once upsert unchanged. |
| SCD Type 2 | Full snapshot scan | Same as a full refresh for fit. |
| Schedules / Autopilot | Next beat uses the new Validate | A beat that used to start and fail at write may now **park on Validate**. Operator widens/ALTERs, then the beat proceeds. |
| Existing Risk Contracts | Still resolved by `resolve_write_action_for_mapping` | Quarantine / continue policies still hold rows out. They do not truncate. |
| Existing destination DDL | Mapping `target_type` does not ALTER | Same as today. Operator or DBA must ALTER. |

**Expected operational surprise:** more Validate **blocks**, fewer Execute **zero-write** failures. Treat a new block as a caught defect, not a regression, when Remap names a dest-spelled widen.

**Scan cost:** default budget is 5,000,000 rows. Warehouse identical/widening declarations still skip the value scan. File peek types never skip.

---

## 15. In-flight jobs, Resume, and rollback

### 15.1 In-flight and failed jobs

| Job state | Action |
|---|---|
| Failed job `6a8f4f89…05ba` (0 written, 1,512 quarantined) | **Do not Resume.** Map/ALTER → re-Validate → **new** transfer. |
| Any failed job with `population_fit` / `do not fit NUMBER` / blank suggested fix | Same: new transfer after widen. Resume reuses the old contract and can miss the scan. |
| Running job | Let it finish. This wave does not cancel in-flight writers. |
| `completed_with_quarantine` | Inspect Quarantine. Replay only if the dest carrier already holds the cell. |
| Schedule parked / needs_approval | Read the finding. If it is dest overflow, ALTER or remap, then approve only scopes the product actually grants. |

### 15.2 Rollback (engineering)

This wave is fail-closed. Rolling it back **re-opens** false-green Validate (the original incident class).

| Path | How | Effect |
|---|---|---|
| Do not merge the branch | Stay on `feature/Venkat-Analysis` | Old behaviour: 25-row Validate, peek-as-DDL skip, Remap `—`. |
| Revert after merge | `git revert` of the population-fit commits on this branch | Same as above. Only do this if the scan itself is defective (wrong writer predicate), not because Validate started blocking real overflows. |
| Emergency Execute | Do **not** set `skip_preflight=true` to “get the file in” | Engine fidelity gates still fire; you lose the operator-visible Remap. |

If Validate blocks a **good** population (writer would accept the cell): file a defect with `population_fit` JSON, dest dialect, and one failing value. That is a predicate bug. Rollback is the last resort.

### 15.3 Data / destination rollback

The original job wrote **0** rows. There is nothing to undo on `EMPLOYEE_DB.tree` from that run.

A later successful transfer is reversed only by the client’s dest practice (truncate, time-travel, or a compensating overwrite). This product does not silently un-write Snowflake.

---

## 16. Security and data handling

- The incident file `flights-1m.csv` is a **client dataset**. Do not attach the full file to tickets, PRs, or this pack.
- Share only: column names, dest types, row numbers (293 / 431 class), and redacted samples already in the incident (`7.9166665`).
- Quarantine DLQ may hold source values. Restrict job-detail access to operators who may see that file.
- This environment’s Mongo `27017` was down at measurement. Do not assume job documents persisted here.
- No SOC 2 / ISO letter is included.

---

## 17. FAQ (buyer / operator)

**Is the original Snowflake job fixed if we click Resume?**  
No. Resume is wrong. Widen or ALTER, re-Validate, new transfer.

**Will Validate always be green after this?**  
No. Validate will block more often when the dest cannot hold a late row. That is the point.

**Can we keep `NUMBER(9,6)` and load anyway?**  
Only with an explicit Risk Contract that **quarantines** unfit rows. The product will not silently truncate to 6 decimal places.

**Why not map everything to VARCHAR/TEXT?**  
That destroys numeric / ENUM / YEAR meaning. Remap offers dest-spelled widen first.

**Did you prove our 1M rows on Snowflake?**  
No. Proof here is the shared algorithm plus 156 fixture tests. Tenant proof is §8.

**Are 716 connectors live?**  
No. **43** unique transfer-ready drivers. **78** PRODUCTION_SKU routes on this host. **716** is catalog tiles.

**Is CDC exactly-once?**  
No. Default is at-least-once upsert until proven otherwise.

**Will incremental nightly jobs re-fail on old overflows?**  
No. Incremental Validate judges the watermark delta, not the whole table. CDC still does not walk the table.

---

## 18. Communications (copy for the ops channel)

> Validate and Execute now share one population-fit scan for files and pageable tables. A 25-row preview is no longer treated as proof. If Validate blocks and Remap names a dest type (for example `NUMBER(10,7)`), widen or ALTER, re-Validate, and start a **new** transfer. Do not Resume job `6a8f4f89…05ba`. We are not claiming 716 live connectors or exactly-once CDC. Snowflake 1M tenant proof is still the client’s acceptance run.

---

## 19. Support and escalation

| Symptom | First check | Escalate when |
|---|---|---|
| Remap still shows `—` | Confirm Validate response `validation_findings[].suggested_target_type` and `population_fit.findings` | Finding exists on `population_fit` only — kernel merge defect |
| Validate greens, Execute quarantines | `population_fit.evidence` (if `sampled` / `unmeasured`, walk did not run) | File had `file_id` and evidence is still sampled |
| Incremental blocked on old rows | `read_scope` / watermark in the preflight payload | Walk ignored `cursor_after` |
| Schedule parked after deploy | Finding class OVERFLOW vs schema drift | Approving a never-delegable narrow |

Attach: `run_id`, preflight JSON (`passed`, `population_fit`, `validation_findings`), dest dialect, one redacted value. Do not attach the full CSV.

---

## 20. Sign-off form (print / attach to the change ticket)

Stamp the merge SHA at signing. Do not sign the “universal certify” line.

| Field | Value |
|---|---|
| Change title | Validate ≡ Execute population-fit (file + pageable table) |
| Branch | `cursor/decimal-fit-untyped-scan-d3bf` |
| Merge SHA | ________________ |
| Proof cited | 156 population-fit passed; 39 chrome passed; 2026-08-27 |
| Tenant Snowflake 1M | □ skip — no credentials &nbsp;&nbsp; □ pass (attach run_id) &nbsp;&nbsp; □ fail |

| Role | Name | Date | Signature |
|---|---|---|---|
| Engineering (algorithm + matrices) | | | |
| Operator (will not Resume `6a8f4f89…`; Map/ALTER → new transfer) | | | |
| Buyer / integration lead (accepts §13 in-scope; rejects 650+ live and exactly-once CDC) | | | |
| Client DBA (ALTER `DEP_TIME`/`ARR_TIME` or equivalent, if dest exists) | | | |

Engineering attests only: named carriers share the writer predicates; listed matrices were run; live Snowflake 1M was not run unless the tenant box above is **pass**.

Buyer attests only: this wave is accepted as fail-closed preflight for file and pageable-table bounded carriers, not as product-wide certification.

