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
| N2 | **AI egress manifest + metadata-only mode** | A per-job manifest of exactly what left the customer boundary toward any model, plus an enforced mode in which the mapper sees schema, statistics and profiles — never cell values | This is the answer to the security-review objection ("your product will breach my data if it sends it to an LLM"). Without an enforced mode and a manifest, the answer is a promise |
| N4 | **Population-level code crosswalk coverage gate** | Prove that every distinct source code value has a target mapping across the *population*, not a sample; unmapped values fail closed | Legacy reference-data conversion is where field-reducing migrations silently corrupt meaning |
| N5 | **Control-total + referential-integrity proof gates** | Gate-8 extensions: independently recomputed control totals (sums of monetary columns, not just counts) and destination-side referential-integrity checks | A row count proves nothing about a ledger balance; this is what a bank examiner asks for |

Dependency: N2 and N5 both write attestations, so both depend on N3 — which is
why N3 was built first.

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

1. D1 ([#132](https://github.com/Venky-Chowdary/DataFlow/pull/132)), N2
   ([#133](https://github.com/Venky-Chowdary/DataFlow/pull/133)), N4
   ([#134](https://github.com/Venky-Chowdary/DataFlow/pull/134)) and N5
   ([#135](https://github.com/Venky-Chowdary/DataFlow/pull/135)) are already
   open on their own branches — do not fold them into later PRs.
2. YAML/fixed-width sources are live on [#136](https://github.com/Venky-Chowdary/DataFlow/pull/136).
   **This PR** makes their 100K cells measurable (layout-projected fwf
   checksum; 12,100-row sqlite COUNT + checksum + DLQ). YAML export and
   100K Postgres stay unmeasured — do not quote 12k as 100K.
3. Run `DATAFLOW_SCALE_YAML_FWF_100K=1` for the 100K Postgres cells, then
   the remaining never-measured items in `docs/ALL_SESSIONS_HANDOVER.md` §6
   and the local fleet / 10k–1M throughput work.
4. Every item lands as its own PR with a live-engine proof and an independent
   destination reread; a passing unit test alone does not close anything
   (`docs/OPEN_DEFECT_REGISTER.md` §5).
