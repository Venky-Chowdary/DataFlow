# Enterprise-2026 delivery status — what the research asked for, what exists

Companion to the research report *"Datawrap — the future of enterprise data
migration (2026)"* (written to the engineering host, deliberately not committed:
it cites external sources and is a point-in-time analysis, not product doctrine).

This file answers one question honestly: **of the capabilities that report says
Datawrap must own, which are built and proved, and which are still words.**

Three states are kept apart, because collapsing them is how a product gets
called ready when it is not:

* **Delivered** — merged, with tests, and exercised on a live engine or in the
  browser. The evidence is named.
* **In progress** — reproduced/designed, not merged.
* **Not started** — no code exists. Saying so is the point of this file.

Integration branch: `feature/Venkat-Analysis`. Last updated 2026-09-01.

---

## 1. The thesis being built against

The report's conclusion was that the movement of bytes is commoditised
(Fivetran, Airbyte, Debezium, LakeFlow, Openflow all move data), and the
defensible position is **proof of movement**: a migration that can prove, months
later and to somebody who does not trust the UI, what was moved, what was
transformed, what was deliberately *not* moved, what was rejected, and that
none of that record has been altered since.

Everything below is scored against that thesis, not against feature count.

---

## 2. NOW tier — delivered

### N1 · Field Reduction Ledger (gate **G16**) — **Delivered**

PR [#125](https://github.com/Venky-Chowdary/DataFlow/pull/125).

* **What it is.** Every source field carries a typed *disposition*: mapped,
  transformed, consolidated, derived, or deliberately dropped. A drop is no
  longer a boolean `intentional_omit`; it carries a reason code, an optional
  note, an archive reference, a retention statement and a named approver.
* **User surface.** Map shows a reduction control on every omitted row (reason
  select + note + archive reference + retention + approver). Validate runs gate
  **G16**, which fails closed on unaccounted fields — a field that is neither
  mapped nor classified stops the run instead of vanishing.
* **Evidence emitted.** The ledger is written into the signed proof pack, so the
  export answers "which of the mainframe screen's 10 fields did not reach the
  new 7, who decided that, and on what grounds".
* **Strictness.** `FIELD_REDUCTION_STRICT` (reason code *and* named approver
  mandatory) is **off by default** — turning it on retroactively fails existing
  approved jobs. Product-owner decision still open: default it on for regulated
  tenants.
* **Regulatory driver.** SOX / BCBS 239 completeness of a field-reducing
  migration; GDPR Art. 30 records of processing; general audit demand for
  "prove the omission was a decision, not a loss".
* **Proof.** 21 new tests; full API suite 6,911 passed; web `tsc -b` clean;
  browser-verified on five paths (pass / warn / block / Execute refusal / real
  execution with the ledger inside the exported proof pack). Two message
  defects and one state-loss defect (returning to Map erased the recorded
  reduction) were found *by* that browser run and fixed in the same PR.

### N4 · Population-level code-system crosswalk coverage (gate **G20**) — **This PR (not yet merged)**

Owner: `apps/api/services/code_crosswalk.py`. Branch `feature/n4-code-crosswalk-coverage`.

* **What it is.** When a mapping **declares** a `code_crosswalk` (opt-in — the
  gate does not invent coded fields from names), every distinct non-empty
  source value in the *population* must have a target. Coverage is proven by a
  SQL `GROUP BY` (or a replayable file scan), not by the Validate sample. A
  covered sample is a block (`g20_code_crosswalk.unproven`). One unmapped code
  is a block (`.unmapped`). There is no implicit identity: `A→A` is an explicit
  entry. The write path applies the same map and refuses unmapped codes into
  quarantine / fail — never silent passthrough. A signed continue-policy Risk
  Contract does **not** demote G20 (unlike G19).
* **User surface.** Map shows a code-crosswalk textarea on enum / `string_enum`
  rows (and any row that already has a map). Validate lists G20. The signed
  proof pack carries `preflight_summary.code_crosswalk` (`code_crosswalk_coverage_v1`).
  CTA: "Open Map to complete the crosswalk".
* **Honesty.** Empty `{}` is a declaration that covers nothing. Missing/null is
  undeclared → skip. Browser-local preflight skips G20 (a browser sample is
  not population proof). Hitting 100,000 distinct values is unproven, fail closed.
* **Not in this PR.** N2 remains independently on
  [#133](https://github.com/Venky-Chowdary/DataFlow/pull/133). D1 remains
  independently on [#132](https://github.com/Venky-Chowdary/DataFlow/pull/132).
  `is_lossy_coercion` and mapping confidence floors are untouched.
* **Proof.** `tests/test_code_crosswalk.py` + `tests/test_code_crosswalk_live.py`:
  **26 passed** (24 unit/sqlite including SQL `GROUP BY` seeing the rare code a
  sample missed, write-path quarantine, MappingItem round-trip, Gate-8 sample
  compare through the same map; **2 live PostgreSQL** — unmapped `Z` blocks with
  independent dest `COUNT(*)=0` and source `Z` still present; covered map
  rewrites `A/B/C/Z` to `active/blocked/closed/archived` with dest reread and
  no identity `Z`). Broader related selection
  (`test_e2e_pipeline` / `test_reconciliation` / `test_signed_proof_pack` /
  `test_field_reduction_ledger`) **122 passed**. Web G20-related suites
  **83 passed, 0 failed**. CI mypy Decision Kernel **17 files clean**. A
  passing unit test is not live closure; the two Postgres cases are.

### N3 · Durable, tamper-evident evidence chain — **Delivered**

PR [#126](https://github.com/Venky-Chowdary/DataFlow/pull/126).

* **What it is.** Evidence records (field-reduction ledger, mapping proof,
  Gate-8 reconciliation, write ledger) previously lived in a bounded in-memory
  deque: restart the API and older jobs' evidence was gone, and nothing could
  detect an altered record. Records are now append-only, each hashing its
  predecessor, with a signed chain head per job, durable storage and a stated
  retention policy.
* **User surface.** Proof packs reference their chain head; a standalone
  verifier re-walks the chain and names the exact record if one was modified or
  removed.
* **Why it matters commercially.** It is the difference between "a JSON
  Datawrap generated about itself" and "evidence a third party can verify".
  It is also the prerequisite for N2 and N5, which have nowhere durable to write
  attestations without it.
* **Regulatory driver.** GDPR Art. 30, SOX, BCBS 239, DORA — retained,
  verifiable records rather than a live dashboard.
* **Proof.** Targeted suites green, web `tsc -b` clean. One Mongo CDC failure
  seen during the run was reproduced on the base commit in a clean worktree with
  the identical error (local Mongo has no change-stream pre-images), so it is
  pre-existing, not caused by this change.

---

## 3. NOW tier — not started

Named precisely so nobody reads §2 as "the tier is done".

| # | Capability | What it must do | Why it is not optional |
|---|------------|-----------------|------------------------|
| N2 | **AI egress manifest + metadata-only mode** | A per-job manifest of exactly what left the customer boundary toward any model, plus an enforced mode in which the mapper sees schema, statistics and profiles — never cell values | In progress independently on [#133](https://github.com/Venky-Chowdary/DataFlow/pull/133) — not in this tree, not claimed delivered here |
| N5 | **Control-total + referential-integrity proof gates** | Gate-8 extensions: independently recomputed control totals (sums of monetary columns, not just counts) and destination-side referential-integrity checks | A row count proves nothing about a ledger balance; this is what a bank examiner asks for |

N4 left this table when the G20 PR opened. Dependency: N2 and N5 both write
attestations, so both depend on N3 — which is why N3 was built first. N5's
control totals are more valuable once crosswalk coverage is provable, which is
why N4 precedes N5.

---

## 4. Correctness defects fixed in the same wave

These are not features; they are the difference between a proof and a false
proof. Full detail and live evidence in `docs/OPEN_DEFECT_REGISTER.md`.

| Defect | PR | What was actually wrong |
|--------|----|--------------------------|
| D19 | [#127](https://github.com/Venky-Chowdary/DataFlow/pull/127) | A full-refresh recreate could discard a declared destination carrier the source overflows, silently. New gate **G19** keeps the doomed schema on the probe and blocks the replacement; a continue-policy risk contract demotes it to a warning and records the replacement in the proof pack |
| D20 | [#128](https://github.com/Venky-Chowdary/DataFlow/pull/128) | MySQL CDC without the `RELOAD` grant read binlog coordinates *after* the fallback table locks were released by `START TRANSACTION` — a commit in that window was in neither the snapshot nor the stream. Silent data loss; the new test fails on the merged connector and passes after |
| D18 | [#129](https://github.com/Venky-Chowdary/DataFlow/pull/129) | Iceberg refused Windows drive-letter warehouses, and — worse, only visible once the catalog worked — the writer reported the warehouse *directory* as the schema, so reconciliation re-read an empty path and graded a correct write as total data loss |
| D16 | [#130](https://github.com/Venky-Chowdary/DataFlow/pull/130) | The Elasticsearch reader reported no types, so an index read back as `string` placeholders and Map demoted an exact `id → id` identity to 0.63 on a route that is not lossy. An index *declares* its fields; the reader now reports the mapping (0.99 measured live). CI mypy baseline also cleaned |

Still open from this sequence: **D1** — a schemaless destination's shape is
inferred from a bounded value sample and then compared as if the destination had
declared it, so run 2 of a route can refuse what run 1 correctly wrote.
Reproduced live against the compose MinIO on 2026-08-30 (`decimal(12,2)` read
back as `DECIMAL(2,2)`). Document stores are already exempt; object stores,
SFTP, Redis and Elasticsearch are not. The fix has to carry provenance
(sampled vs declared) out of the probe rather than weaken any comparison —
object-store writers enforce probed widths at write time, so simply suppressing
the verdict would fail open and quarantine rows while Map showed green.

---

## 5. Deliberately not built (yet), with the reason

* **BYOC / in-VPC runner and confidential-computing tier.** The report says
  these are what actually convince security review boards. They are also a
  deployment-model decision with real operational cost, not an afternoon's code.
  Needs a product-owner decision before any of it is written.
* **Copybook / EBCDIC / COMP-3 mainframe adapter.** The field-reduction
  governance case is now supported (G16); reading the mainframe's physical
  formats is a separate, large body of work and should not be started until a
  first mainframe customer defines the dialect subset.
* **LLM-mediated data transfer** (moving rows *through* natural language). The
  report found the evidence against this strong: non-determinism, cost, and no
  reproducibility. Datawrap should use models for *schema and profile*
  reasoning, never as a data path.

---

## 6. Open product-owner decisions

1. Should `FIELD_REDUCTION_STRICT` default **on** for regulated tenants?
   (Recommendation: not until an operator has driven a real migration through
   the new Map control.)
2. Should Map let a narrowing route reach Validate **unsigned**, so G19's red
   block is reachable in the UI? Today Map's lossy review holds first, and the
   only control that releases it is `Sign Risk Contract` — which turns G19 into
   an amber warning. The hard gate therefore never appears to an operator.
3. Should G19 warning cards carry an "Open Map to fix the carrier" action?
4. Which vertical is first (healthcare / financial services / public sector)?
   It changes the order of N2, N4 and N5.
5. Is BYOC in scope within 12 months? It changes the architecture, not just the
   roadmap.

---

## 7. How to continue

1. Merge **N4** (this PR, G20) into `feature/Venkat-Analysis` after review.
   Do not fold D1 or N2 into it.
2. **D1** stays on [#132](https://github.com/Venky-Chowdary/DataFlow/pull/132)
   (sampled destination-shape provenance). Still the last open defect in the
   current sequence; it is not closed by G20.
3. **N2** stays on [#133](https://github.com/Venky-Chowdary/DataFlow/pull/133)
   (metadata-only mapper + AI egress manifest).
4. Next feature after N4 merges: **N5** (control-total + referential-integrity
   proof gates), on a new `feature/` branch from Venkat-Analysis — not from N4
   until N4 is merged.
5. Every item lands as its own PR with a live-engine proof and an independent
   destination reread; a passing unit test alone does not close anything
   (`docs/OPEN_DEFECT_REGISTER.md` §5).
