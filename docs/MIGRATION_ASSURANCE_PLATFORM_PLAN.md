# DataWrap — Migration Assurance Platform Transformation Plan

**Status:** Authoritative engineering plan (contractual)  
**Authority:** Brutal Independent Audit (2026-08-08) + standing product contracts in `docs/`  
**Branch baseline at plan authoring:** `58aa4ce` (audit tip was `00bbc4ec`; subsequent CDC/trust/UI waves landed — they do **not** close P0 type-width or CI)  
**Product identity:** Enterprise **Migration Assurance Platform** (peers: AWS DMS+SCT, Datafold, Informatica PowerCenter/IDMC, Qlik Replicate, GoldenGate) — **not** an Airbyte/Fivetran ELT clone  

This document is the execution contract for transforming DataWrap into a deterministic, evidence-backed migration platform. Every audit finding maps to a work item with exit criteria and proof. Work is **not** complete when code compiles or a single route is green — only when the Decision Kernel, proof artifacts, and CI gates for that slice are green and documented.

---

## 0. Non-negotiable rules

| Rule | Meaning |
|------|---------|
| **R1 — Single source of truth** | One **Migration Decision Kernel** produces one immutable **Decision Artifact**. Map, Validate, Execute, Proof, API, UI, contracts, risk, transforms, and connectors **consume** it — they never re-derive business decisions. |
| **R2 — Correctness above everything** | No silent truncate / coerce / invent / drop / narrow / strip-TZ / flatten. Unsafe ops are classified, gated, or quarantined. |
| **R3 — Fail closed** | Missing proof, missing width, missing DDL identity, missing checkpoint → refuse write. |
| **R4 — Honesty** | Catalog = certified engines only. CDC = at-least-once until proven otherwise. Sample ≠ population proof. |
| **R5 — Proof or it didn’t happen** | Every closed finding ships code + regression test + (where claimed) matrix/benchmark artifact. Chat confidence ≠ done. |
| **R6 — No partial architecture** | Do not leave parallel authorities (`ddl_type` vs `DDL_TYPES` vs bind path) disagreeing. Extract or kill duplicates. |

---

## 1. Target architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  UI / API / CLI / Scheduler / CDC workers                                │
│  (display + submit only — never invent mapping/DDL/risk decisions)       │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ DecisionArtifact (immutable, versioned)
┌───────────────────────────────▼─────────────────────────────────────────┐
│  MIGRATION DECISION KERNEL  (packages/decision_kernel/)                   │
│  ┌─────────────┐ ┌──────────────┐ ┌─────────────┐ ┌──────────────────┐ │
│  │ Type System │ │ Conversion   │ │ Structural  │ │ Semantic Mapper  │ │
│  │ (canonical  │ │ Classifier   │ │ Type Engine │ │ (assign only)    │ │
│  │  + width)   │ │              │ │             │ │                  │ │
│  └──────┬──────┘ └──────┬───────┘ └──────┬──────┘ └────────┬─────────┘ │
│         └────────────────┴────────────────┴─────────────────┘           │
│  ┌─────────────┐ ┌──────────────┐ ┌─────────────┐ ┌──────────────────┐ │
│  │ Risk Engine │ │ Validation   │ │ Proof       │ │ Policy /         │ │
│  │             │ │ Orchestrator │ │ Engine      │ │ Capability       │ │
│  └─────────────┘ └──────────────┘ └─────────────┘ └──────────────────┘ │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ typed plans + fingerprints
┌───────────────────────────────▼─────────────────────────────────────────┐
│  EXECUTION PLANE                                                         │
│  Transfer engine · Stream · Writers · CDC · Scheduler (→ durable queue)  │
│  Connectors (adapters only — capability registry + native↔canonical)     │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  EVIDENCE PLANE                                                          │
│  Decision ledger · Proof packs · Quarantine · Audit · OTel               │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.1 Decision Artifact (immutable)

Single JSON/schema object (versioned `decision_artifact_v1`) stamped at Map/Validate and required at Execute:

| Field group | Contents |
|-------------|----------|
| Identity | `artifact_id`, `schema_version`, `created_at`, `tenant_id`, `route_id`, `source_fingerprint`, `dest_fingerprint` |
| Schema | Canonical column specs (logical + width/precision/scale/tz/nullability/structure) |
| Mapping | Per-column assignment, alternatives, confidence, calibration reason, `assignment_strategy` (honest label) |
| Conversion | Per-column `ConversionClass`, risk level, policy, recommended action |
| DDL | Materialized dest DDL stamps + `ddl_identity_hash` |
| Validation | Gate results G1–G9 separated by class (schema / semantic / population / runtime / policy / proof) |
| Risk | Contracts required/approved; Safe → Blocked |
| Execution plan | Sync mode, error policy, parallelism, pagination strategy, snapshot pin |
| Proof plan | Checksum algorithm (full SHA-256), sample limits (labeled preflight only), reconcile mode |
| Capability | Source/dest capability profile hashes used for the decision |
| Signatures | Policy signature, operator approvals, hash chain |

**Kill list:** any path that recomputes confidence, lossiness, or DDL invent outside the kernel for Execute.

### 1.2 Existing contracts to absorb (do not fork)

| Doc | Role in target |
|-----|----------------|
| `CONVERSION_CONTRACT.md` | ConversionClass + DDL identity — extend classes (widening/narrowing/…) |
| `MAPPING_CONFIDENCE_AUTHORITY.md` | Confidence SSOT → kernel |
| `MIGRATION_RISK_CONTRACT.md` | Risk Engine input |
| `PROOF_POST_WRITE_CONTRACT.md` | Proof Engine; `migration_proven` = full checksum only |
| `VALIDATE_DECISION_PATH.md` | UI presenter over artifact |
| `PREFLIGHT_RULEBOOK.md` | Gate definitions → Validation Orchestrator |
| `QUARANTINE_*` | Fail-closed row fate |
| `EXECUTION_ENGINE_CONTRACT.md` | Execution plane consumer of artifact |

Rewrite `COMPETITIVE_ANALYSIS.md` and `PRODUCT_ARCHITECTURE.md` in Phase E (positioning honesty).

---

## 2. Program phases (sequenced; exit criteria are hard gates)

Effort is in **focused engineering sessions** (~1 day). Parallelize only within a phase after that phase’s P0 blockers clear.

### Phase A — Stop the bleeding (ship-blocker) — **~10–14 sessions**

**Goal:** Product cannot silently lose integer/float width; API/CLI/scheduler can run; security job unblocks; no public email dump.

| ID | Work | Audit | Exit criteria (proof) |
|----|------|-------|------------------------|
| **A1** | **Canonical integer/float width** | §2.1 | Logical vocabulary carries INT8/16/32/64 and FLOAT32/64 (or width attrs on carriers). Introspection **never** collapses `bigint`→bare `INTEGER`. Same-family native passthrough preferred. |
| **A2** | **`ddl_type` ≡ `DDL_TYPES` never-narrower** | §2.1 | Single invent path; default unknown integer→64-bit, unknown float→64-bit. CI gate: no `ddl_type(dest, logical)` narrower than `DDL_TYPES`. |
| **A3** | **Round-trip data tests** | §2.1 | Live PG→PG (and matrix where services exist): `2^63-1`, `5000000000`, `1.2345678901234567` bit-exact. Harness assertions currently red must go green. |
| **A4** | **DDL identity for programmatic callers** | §2.2 | `skip_preflight` stamps fingerprint **inline** from current mappings/kernel (or requires hash only when operator-supplied maps). No “Validate required” for auto-derived maps without UI. ~150 suite failures drop. |
| **A5** | **Bandit SHA-1** | §2.9 | `hashlib.sha1(..., usedforsecurity=False)` (or blake2/sha256). `security` job green enough to surface pip-audit. |
| **A6** | **Auth bootstrap lockdown** | §6.1 | `/auth/bootstrap` authenticated or payload `{auth_required, has_users}` only — **no** emails, **no** `admin_password_length`. |
| **A7** | **Login rate limit + lockout** | §6.2 | Per-IP + per-email budget; progressive lockout; tests. |
| **A8** | **`__DF_MISSING__` containment** | §2.4 | Public row APIs never emit sentinel; typed `Missing` / presence mask; cross-writer regression that no writer lands the literal. |
| **A9** | **Orphan DDL rollback/sweep** | §2.3 | Auto-create failure before first durable commit → drop/compensating cleanup; staging namespace or job-scoped names; test. |

**Phase A Definition of Done:**  
- A1–A3 proven on CI topology (PG CDC container at minimum).  
- Full suite failure count drops by the DDL-identity + type-harness clusters (≥175).  
- Security job no longer dies on B324.  
- Bootstrap/login controls covered by tests.

**Implementation notes (A1 — root cause confirmed on tip `58aa4ce`):**

1. `normalize_logical_type` maps `bigint`/`int8`/`int64` → `LOGICAL_INTEGER` **and** strips width.  
2. `_integer_ddl_for_dest` uses `integer_bit_width("INTEGER")` → **32** → PG invents `INTEGER`.  
3. `DDL_TYPES[*][LOGICAL_INTEGER]` already says `BIGINT`/`Int64`/`long` — **third authority disagrees**.  
4. Fix order: preserve native carrier through Map stamps → invent from carrier → only then default widen logical integer to 64-bit → unify bind/`to_sqlalchemy_type` with invent.

---

### Phase B — CI as a product feature — **~15–20 sessions**

**Goal:** Green main is mergeable; suite is order-independent; frontend tests run; proof steps execute.

| ID | Work | Audit | Exit criteria |
|----|------|-------|---------------|
| **B1** | Triage remaining failures into: real bug / fixture drift / pollution / skip-honest | §3.1–3.3 | Spreadsheet/ledger of all failures with owner class |
| **B2** | Kill import-time auth/config freeze | §1.3 D2 / §3.3 | `test_workspace_*` pass in full suite; lazy config or fixture reset |
| **B3** | ES writer: no `information_schema` | §2.5 | ES upsert/insert accuracy tests green |
| **B4** | SQL Server tz-aware bind | §2.6 | Aware UTC binds; naive still refused |
| **B5** | SQLite fidelity fail-closed | §2.7 | TZ refuse path does not write |
| **B6** | Full SHA-256 reconcile digest | §2.8 | No `[:16]` truncation in proof API/UI |
| **B7** | Wire frontend `tsx --test` / vitest into CI; fix chrome contract | §3.4 | 22/22 green in CI |
| **B8** | Ruff baseline + mypy on kernel/type_system/engine/reconciliation | §3.5 | CI fails on new BLE001/S110/DTZ001/F401 in touched packages |
| **B9** | Merge gate: red CI blocks; CDC matrix + warehouse SKU proof **must run** (or explicit skip with reason artifact) | §3.2 | Artifacts uploaded even when optional services absent |
| **B10** | Data-rule coercion matrix triage (64) | §3.3 | No silent family of §2.1 bugs left |

**Phase B Definition of Done:**  
- `main` CI green on `api-and-web` + `security` for N consecutive runs (target ≥3).  
- CDC matrix report and warehouse proof steps execute (pass/skip with reasons — never silently omitted).

---

### Phase C — Migration Decision Kernel (architecture spine) — **~20–25 sessions**

**Goal:** One authority; god-module extraction begins; UI/API only display artifacts.

| ID | Work | Exit criteria |
|----|------|---------------|
| **C1** | Create `packages/decision_kernel/` (or `apps/api/services/decision_kernel/`) with typed models: `CanonicalType`, `ColumnSpec`, `MappingDecision`, `ConversionDecision`, `DecisionArtifact` | Schema + pydantic/dataclass + golden JSON fixtures |
| **C2** | Move type invent/classify/lossy from scattered call sites into kernel Type + Conversion engines; `type_system.py` becomes adapter/facade then shrinks | No duplicate `is_lossy` / invent in writers |
| **C3** | Conversion Classification Engine — full class set: Identity, Equivalent, Lossless, Representation, Normalization, Widening, Narrowing, Semantic, Potentially Lossy, Lossy, Unsupported, Manual | Extends Module 12; stamped on every map cell |
| **C4** | Structural Type Engine — strategies for Array/Object/Map/XML/Variant (JSON / normalize / child table / bridge / custom) | Operator chooses; default never silent flatten |
| **C5** | Context-aware mapping — invent differs for create-new vs bind-existing vs CDC sparse vs append | Same conversion ≠ same DDL in every mode |
| **C6** | Schema profiling SSOT — min/max/null%/cardinality/actual precision/patterns; feed Map + invent | Profile strip already started — promote to kernel input |
| **C7** | Semantic engine — roles, neighbor evidence; forbid metric↔id, state-name↔code without evidence | Calibration remains; label `optimal_bipartite_hungarian` only when no greedy patch — or rename |
| **C8** | Validation Orchestrator — separate schema/semantic/population/runtime/policy/proof; gates call kernel only | G1–G9 consume artifact |
| **C9** | Risk Engine — Safe / Info / Review / Approval / Blocked; contracts only when genuine risk | Align `MIGRATION_RISK_CONTRACT` |
| **C10** | Proof Engine — full checksum, counts, DDL evidence, decision hash, connector versions, policy signatures | Proof packs for buyer evidence |
| **C11** | Wire Execute: refuse write without artifact hash match (replaces ad-hoc ddl-only checks over time) | Engine tests |
| **C12** | UI: Decision Artifact only; group identical mappings; actionable issues only | ValidateDecisionPath consumes artifact IDs |

**Phase C Definition of Done:**  
- Execute path loads Decision Artifact; invent/classify not re-run ad hoc.  
- `type_system.py` size trending down (target: split into ≤4 modules under kernel + thin facade).  
- Docs updated: Module map points to kernel package as SSOT.

---

### Phase D — Security & tenancy hardening — **~10–12 sessions**

| ID | Work | Audit | Exit criteria |
|----|------|-------|---------------|
| **D1** | Trusted-proxy `_client_ip` | §6.3 | Spoof XFF left-most fails without trusted hop config |
| **D2** | Tenant bound to authenticated identity; Host is hint | §6.4 | Cross-tenant Host spoof refused |
| **D3** | Server-side sessions (`jti`) + revocation; invalidate on password change | §6.5 | Logout/password rotate kills token |
| **D4** | Separate `SECRETS_KEY` / `AUTH_SECRET`; HKDF; decrypt **raises** | §6.7–6.8 | No `[decryption-failed]` string password |
| **D5** | Dev user opt-in only; role normalize closed | §6.9 | Staging not auto `password123` |
| **D6** | Copilot SQL allow-lists; `defusedxml` | §6.10 | Bandit B314 cleared; LLM SQL cannot invent columns |
| **D7** | Identity persistence roadmap spike → implementation backlog | §6.6 | Design approved; MVP user store or explicit “env-only” product limit |

---

### Phase E — Product honesty & market position — **~8–10 sessions**

| ID | Work | Audit | Exit criteria |
|----|------|-------|---------------|
| **E1** | Catalog cut to certified engines; SKU duplicates labeled aliases not “live connectors” | §5.1 | Public count ≈ distinct engines; planned clearly labeled |
| **E2** | Per-pair certification matrix (TRANSFER_READY) published | §5.1 / proof bar | Artifact in CI |
| **E3** | SaaS: incremental + OAuth refresh + Retry-After **or** drop SaaS category from marketing | §5.2 | Honest docs |
| **E4** | Rewrite `COMPETITIVE_ANALYSIS.md` vs DMS/SCT, Datafold, Debezium, Qlik, GoldenGate, Estuary, Informatica | §7.1 | Fact-checked |
| **E5** | Rewrite `PRODUCT_ARCHITECTURE.md` to current system | §1.3 D4 | Matches code |
| **E6** | Positioning: Migration Assurance + proof — not “650+ live” | §0 / §7 | Marketing sites/docs aligned |

---

### Phase F — Scale, CDC transport, bulk I/O — **~25–35 sessions**

| ID | Work | Audit | Exit criteria |
|----|------|-------|---------------|
| **F1** | Fingerprints during write pass (or pinned snapshot for second pass) | §4.3 B | No double bill / false recon on concurrent source writes (documented + tested where possible) |
| **F2** | Keyset pagination: composite PK, SQL Server, Oracle | §4.3 C | OFFSET only where unavoidable + warning in artifact |
| **F3** | Bulk export: BQ Storage Read, Snowflake unload, PG COPY | §4.3 C | Benchmark vs OFFSET |
| **F4** | CDC: `START_REPLICATION` streaming; proto v2/v3 where available | §4.4 | Lag curve artifact; peek path deprecated |
| **F5** | Durable distributed scheduler (Temporal / Celery+Redis / SQS) | §1.3 D3 | Multi-replica safe |
| **F6** | Raise defaults + tuning guide with **measured** numbers | §4.3 A / §7.4 | Published rows/s and GB/h for top 10 pairs |
| **F7** | Connector Capability Registry machine-readable for all live engines | User mandate | Consumed by kernel |
| **F8** | Decompose remaining god modules behind stable interfaces | §1.3 D1 | Size budgets enforced in CI |
| **F9** | Frontend code-split; break `TransferPage` | §1.3 D5 | Chunk budgets |

---

### Phase G — Enterprise test factory (continuous) — **ongoing**

Required suite classes (every audit finding gets a regression):

| Class | Purpose |
|-------|---------|
| Unit | Kernel pure functions |
| Contract | Decision Artifact schema, ConversionClass, DDL identity |
| Integration | Live CI topology (PG/MySQL/Mongo/Redis/BQ emu) |
| Round-trip | Width/precision/TZ/matrix |
| Property / fuzz | Type invent never narrower; sentinel never lands |
| Mutation | Gate fail-closed under fault injection |
| Certification | Per-pair TRANSFER_READY |
| Performance | Top routes + CDC lag |
| Chaos | Checkpoint loss, orphan DDL, slot lag |
| Frontend | Decision Artifact rendering only |

---

## 3. Traceability: audit → phase

| Audit finding | Phase IDs |
|---------------|-----------|
| §2.1 BIGINT/DOUBLE narrow | A1–A3 |
| §2.2 DDL identity / skip_preflight | A4 |
| §2.3 Orphan DDL | A9 |
| §2.4 `__DF_MISSING__` | A8 |
| §2.5 Elasticsearch | B3 |
| §2.6 SQL Server TZ | B4 |
| §2.7 SQLite fail-open | B5 |
| §2.8 Digest truncation | B6 |
| §2.9 Bandit SHA-1 | A5 |
| §3.* CI / pollution / FE tests / lint | B1–B10 |
| §1.3 D1–D5 architecture | C*, F5, F8, F9 |
| §4.* algorithms (mapping label, double read, OFFSET, CDC peek) | C7, F1–F4 |
| §5.* catalog / SaaS | E1–E3 |
| §6.* security | A6–A7, D1–D7 |
| §7.* competitive docs | E4–E6 |
| User mandate: Decision Kernel, profiling, structural, proof, observability | C1–C12, Phase G |
| Scale / throughput | F5–F6 |

---

## 4. Wave execution model (how we ship without thrashing)

Each mergeable wave:

1. **One primary invariant** (e.g. “integer invent never narrower than source width”).  
2. **Kernel or SSOT touch first**, then adapters, then UI.  
3. **Regression tests named after the invariant.**  
4. **Proof:** pytest node ids + pass/fail/skip counts in PR.  
5. **No catalog inflation** or marketing claims in the same PR.  
6. **Do not mark complete** until Phase exit criteria for that ID are met.

**Immediate next wave:** B8 mypy + C11/C12 FE pin shipped. Wave3 + claim-queue file staging + connector test-health SSOT + object-store purge-after-promote (purge failure cannot fail committed write) + BQ pre-DML abort. **C2 advanced** (writer invent imports via kernel). Still open: B1 real_bug→0; C2 god-module extract; F4 streaming default; F3 non-PG bulk Planned labels. Widen mypy; continue F8.

---

## 5. What “world-class” means for *this* product

| Peer capability | DataWrap target |
|-----------------|-----------------|
| DMS validation | Full-population checksum + quarantine + typed mismatch samples (already stronger intent — must deliver) |
| SCT / Informatica mapping | Semantic + structural + ConversionClass + Risk — Decision Artifact |
| Datafold | Proof packs + buyer evidence; no sample override of checksum |
| Debezium / Datastream | Streaming CDC transport (Phase F); keep at-least-once honesty |
| ADF | Schema drift + late bind via kernel context modes — not silent invent |
| Fivetran/Airbyte | **Do not compete on SaaS breadth**; optional thin SaaS later with incremental or exit category |

Success metric for the company: a migration programme owner can move Oracle/SQL Server/Db2 → PostgreSQL/Snowflake and **prove** every row and every type — with CI green and a catalog that matches reality.

---

## 6. Explicitly out of scope until Phases A–B exit

- New SaaS connectors  
- Claiming exactly-once CDC  
- Horizontal multi-region tenancy redesign beyond D2  
- Full rewrite of TransferPage before Decision Artifact exists  
- Marketing “740 connectors” or competitor false claims  

---

## 7. Governance

- This plan supersedes ad-hoc “fix the route you pasted” work for migration-critical paths.  
- Audit report remains the findings backlog; this plan is the sequencing + architecture.  
- Update this document’s Phase checkboxes (or a linked tracking issue) when exit criteria are met with proof links (CI run URL, pytest counts, benchmark paths).  
- Standing rules: `.cursor/rules/enterprise-standards.mdc`, `continuous-product-audit.mdc`, `enterprise-transfer-proof-bar.mdc`, `world-class-universal-build.mdc`.

---

## 8. Checklist — Phase A (copy into PR / issue)

- [x] A1 Canonical INT/FLOAT width preserved through introspect → Map → DDL  
- [x] A2 never-narrower CI gate; `ddl_type` ≡ `DDL_TYPES`  
- [x] A3 `2^63-1` invent + introspect proof (`test_bigint_create_new_roundtrip_width.py`; live PG when DSN present)  
- [x] A4 programmatic Execute with inline DDL identity stamp  
- [x] A5 Bandit B324 cleared (`usedforsecurity=False` already on tip)  
- [x] A6 bootstrap no email/password-length leak  
- [x] A7 login rate limit + lockout  
- [x] A8 no `__DF_MISSING__` in public/writer output (`Missing` singleton + coerce_null→None)  
- [x] A9 orphan DDL cleanup on failed auto-create (PG register + engine rollback; extend writers)  

## 9. Checklist — Phase B

- [x] B1 Failure triage ledger (expanded) — `docs/CI_FAILURE_LEDGER.md`; Gate-8 upsert keyed checksum + streaming resume + market skip_honest + PG auth collection skip closed with proof; continue maxfail re-sample toward green
- [x] B2 Lazy auth config (call-time env; kill import-time freeze)
- [x] B3 ES writer: document-store get_mapping path (no SQL require_physical / information_schema)
- [x] B4 SQL Server TIMESTAMPTZ keeps aware UTC (`_to_sa_value` carrier-first)
- [x] B5 SQLite TZ→NTZ fail-closed (Map DATETIME carrier survives TEXT affinity)
- [x] B6 Full SHA-256 reconcile digest (no `[:16]` truncation)
- [x] B7 Frontend `tsx --test` via `npm run test:web` in CI; chrome contract aligned
- [x] B8 Ruff baseline (allowlist + CI); mypy Decision Kernel + type_system smoke in CI (`apps/api/mypy.ini`)
- [x] B9 Merge gate: CDC + warehouse SKU run after test failure; JSON artifacts uploaded; job fails closed
- [x] B10 Data-rule coercion matrix — balanced lossy requires Risk Contract (3742 passed)

## 10. Checklist — Phase C (complete)

- [x] C1 Decision Artifact models + golden fixture (`decision_artifact_v1`) + hash fail-closed tests
- [~] C2 Type invent/classify/DDL facades on `services.decision_kernel`; CREATE invent writers (BQ/MySQL/SF/SQLite/Iceberg) import kernel surface (`test_writer_invent_imports_use_decision_kernel_surface`); `writer_common` specialty helpers + invent body still in `type_system` (exit criterion open)
- [x] C3 Full ConversionClass set (identity/equivalent/widening/narrowing/… + Module 12 gates)
- [x] C4 Structural Type Engine kernel facade (`StructuralStrategy`, never silent flatten)
- [x] C5 Context-aware invent modes (`InventContext` + `invent_dest_type` refuse bind/CDC invent)
- [x] C6 Schema profiling SSOT (`profile_columns` / `ColumnProfile` — sample ≠ population)
- [x] C7 Semantic engine honest assignment labels (`hungarian_with_greedy_patch`)
- [x] C8 Validation Orchestrator (`ValidationClass` buckets on proof_bundle)
- [x] C9 Risk Engine bands (`risk_level_for_conversion` / `assess_mapping_risk`)
- [x] C10 Proof Engine packs (`build_migration_proof_pack` — full SHA-256 only)
- [x] C11 Execute Decision Artifact gate — engine + Studio pin ``approved_decision_artifact_hash`` / refuse missing 64-hex
- [x] C12 UI renders Decision Artifact on Validate honesty + decision path; Execute consumes Validate hash (not silent re-stamp alone)

## 11. Checklist — Phase D (complete)

- [x] D1 Trusted-proxy `_client_ip` (`services.client_ip`; default ignores XFF spoof)
- [x] D2 Tenant bound to authenticated identity (`services.tenant_bind`; Host must match claims)
- [x] D3 Server-side sessions + revocation (`auth_sessions` jti; `/auth/logout`; revoke-all on password rotate helper)
- [x] D4 SECRETS_KEY / AUTH_SECRET separation + HKDF; decrypt raises `SecretVaultError`
- [x] D5 Dev user opt-in only (`ALLOW_DEV_USER`); unknown roles → viewer
- [x] D6 Copilot SQL allow-lists (`copilot_sql_guard`); defusedxml on XML parse / runtime check
- [x] D7 Identity persistence roadmap (`docs/IDENTITY_PERSISTENCE_ROADMAP.md`)

---

## 12. Checklist — Phase E (complete)

- [x] E1 Catalog aliases (`is_hosted_alias` / `alias_of`); summary `unique_drivers` + `alias_tiles`
- [x] E2 `scripts/transfer_ready_matrix_report.py` → CI artifact `transfer_ready_matrix.json`
- [x] E3 SaaS honesty doc — only SF/HubSpot certified; rest Planned (`docs/SAAS_CONNECTOR_HONESTY.md`)
- [x] E4 `COMPETITIVE_ANALYSIS.md` rewritten vs DMS/SCT/Datafold/Debezium/Informatica
- [x] E5 `PRODUCT_ARCHITECTURE.md` matches Decision Kernel + current system
- [x] E6 Positioning: no “650+ live”; evidence pointers to unique_drivers / PRODUCTION_SKU

## 13. Checklist — Phase F (in progress)

- [x] F1 Fingerprints during write pass (`checksum_mode=inline_write_pass`; opt-in `RECONCILE_SOURCE_REREAD`)
- [x] F2 Keyset pagination: composite PK + SQL Server/Oracle (`services/keyset_pagination.py`; `pagination_mode` proof)
- [x] F3 Bulk export — PG COPY implemented; Snowflake/BQ fail-closed stubs (product must keep Planned until live)
- [~] F4 CDC `START_REPLICATION` transport (`postgresql_cdc_transport`; **default still peek** — streaming opt-in via `CDC_PG_TRANSPORT=streaming`)
- [x] F5 Durable distributed scheduler — Mongo `transfer_job_queue` + leases + fences (`scheduler_mode` local|claim|auto); API claim loop; `docs/DISTRIBUTED_SCHEDULER.md` (Temporal deferred — correctness via claim/lease)
- [x] F6 Defaults + measured tuning guide — `PARALLEL_WORKERS` min(4,cpu), `TRANSFER_WORKERS` 8; `docs/TUNING_AND_BENCHMARKS.md` + `scripts/throughput_microbench.py` → `throughput_microbench.json`
- [x] F7 Capability registry for all TRANSFER_READY unique drivers — profile hash + matrix artifact; Decision Artifact stamps `capability_*_hash` (`export_live_capability_matrix`)
- [x] F8 God-module LOC freeze + facades — `module_size_budgets.json` CI gate; `reconciliation_api` / `writer_common_api` / `merge_registry`; `docs/GOD_MODULE_DECOMPOSITION.md`
- [x] F9 Frontend code-split — Vite route/vendor chunks; lazy screens; Transfer helpers extracted; `chunk_budgets.json` + `docs/FRONTEND_CODE_SPLIT.md` (TransferPage shell still large — continue shrink under freeze)

---

*End of plan. Phases A–G continuous; A–E + F1–F9 shipped with proof. Remaining: full B1 CI ledger, mypy on kernel/type_system, further god-module extractions. Silent narrowing remains forbidden.*
