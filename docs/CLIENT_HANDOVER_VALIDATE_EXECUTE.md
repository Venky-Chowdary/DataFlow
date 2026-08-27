# Datawrap — Client handover: Validate ≡ Execute (population fit)

**Audience:** enterprise buyer / integration architect / cutover operator  
**Date:** 2026-08-27  
**Branch:** `cursor/decimal-fit-untyped-scan-d3bf` @ `2a966d74`  
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
| This file | Client + architect handover for the Validate≡Execute wave |
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
