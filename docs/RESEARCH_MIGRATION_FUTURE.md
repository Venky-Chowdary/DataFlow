# Where data movement breaks today, and where it breaks next

Scope and honesty note: this is a review of **primary sources** — vendor
protocol docs and specifications, engine documentation, public issue trackers,
benchmarks and papers — not a survey of a thousand pages. Every claim below
links to the source it came from. Where a claim is our own inference it says so.

The output is deliberately shaped as a ranked list of

```
pain point → the capability that removes it → the algorithm → the proof metric
```

so each row can become a task with a measurable artifact, rather than a slide.

---

## Part 1 — What the market still gets wrong (present-day, documented)

### 1. "Exactly-once" is a property of a *route*, not of a product

Debezium's own position is at-least-once, and its exactly-once story exists only
where the sink is a transactional log with connector-level transaction support
(Kafka Connect, KIP-618, Kafka 3.3+)
([debezium.io](https://debezium.io/blog/2023/06/22/towards-exactly-once-delivery/)).
Estuary gets there differently: a cooperative `acknowledge → load → store` RPC in
which the driver commits the reduced documents **and** the checkpoint together,
and may perform an *idempotent apply* of the last transaction on recovery
([docs.estuary.dev](https://docs.estuary.dev/reference/Connectors/materialization-protocol/)).

Both designs make the same admission: the destination has to be the arbiter.
Anything that keeps its cursor beside the data cannot be exactly-once.

**Our position.** This is the design we implement and, as of
`docs/CDC_EXACTLY_ONCE_LIVE_EVIDENCE.md`, the one we have now *measured* on live
PostgreSQL and MySQL with crashes injected inside the apply transaction. The
platform default stays at-least-once upsert and the claim stays route-scoped.

### 2. Schema evolution is where silent data loss actually happens

- PostgreSQL logical replication does not replicate DDL at all, and does not
  replicate sequence advances — a failover to the subscriber leaves sequences
  behind the data ([postgresql.org](https://www.postgresql.org/docs/19/logical-replication-restrictions.html)).
  This is exactly the SERIAL-lands-as-INTEGER class of defect our route matrix found.
- Airbyte issue #50874: changing a *propagation setting* silently dropped and
  recreated CDC changelog tables — no warning, no confirmation, historical data
  gone ([github.com/airbytehq/airbyte#50874](https://github.com/airbytehq/airbyte/issues/50874)).
- Fivetran's documented remedy for a primary-key change is to **drop the
  destination table and re-sync**; duplicates otherwise persist
  ([fivetran.com](https://fivetran.com/docs/connectors/databases/postgresql/troubleshooting/duplicate-records-after-adding-primary-key)).
- A rename is the hard case: Debezium had to add explicit `RENAME` column
  semantics to its DDL grammar to see one at all
  ([debezium PR #655](https://github.com/debezium/debezium/pull/655)); without
  it a rename reads as add + orphan.

**Capability:** drift classification that separates *additive*, *renamed*,
*narrowed* and *destructive*, and refuses the destructive class rather than
"propagating" it.
**Algorithm:** name/type/constraint/structure matching in the Cupid lineage
([Cupid, VLDB 2001](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.216.5548)),
scored per-pair, plus value-profile evidence for renames — never a single LLM
verdict.
**Proof metric:** rename-detection precision/recall on a labelled fixture, and
zero destructive actions taken without an explicit operator decision.

### 3. Identity: `unique_key` semantics are the duplicate factory

dbt's own docs state that without `unique_key` most adapters degrade to
append-only "without regard for whether the rows represent duplicates"
([docs.getdbt.com](https://docs.getdbt.com/docs/build/incremental-models)).
Fivetran synthesises `_fivetran_id` when no key exists and cannot retroactively
reconcile it. So the industry norm is: identity is optional, and the cost of
omitting it is silent duplication discovered months later.

**Capability:** identity is a *contract*, and a run without a provable key
cannot claim full-population proof — which is why our append path proves
`dest_after − dest_before == rows_written` instead of comparing whole-table
digests.
**Proof metric:** duplicate-identity scenarios in the route matrix must refuse
or quarantine, never land twice.

### 4. Verification is either sampled or unaffordable

The best-documented practical design is Simon Eskildsen's: fingerprint rows,
checksum ranges over an indexed `updated_at`, and binary-search down to the
mismatching range — his own napkin math shows the naive full-scan comparison of
100M rows costs ~2 hours at 10% of database capacity, which is why range
checksums exist ([sirupsen.com](https://sirupsen.com/napkin/problem-14-using-checksums-to-verify/);
the idea became `datafold/data-diff`). Amazon holds a patent on the Merkle-tree
variant of the same idea ([US 10223394](https://exa.ai/library/legal/patent/cvc50jmwz5nwmmd2cmj99w)).

**Capability:** hierarchical (range → sub-range → row) reconciliation so a
mismatch localises in `O(log n)` reads instead of a full re-read, while the
verdict stays whole-population.
**Algorithm:** order-independent aggregate over per-row fingerprints per key
range, recursed on mismatch.
**Proof metric:** localisation cost — bytes read to find one corrupted row in
100M — and a corruption-injection test proving no mismatch is missed.
**Status: gap.** We reread the destination whole-population, which is correct
and expensive. Range-checksum localisation is the single highest-value
performance item we do not yet have.

### 5. Lakehouse writes have a cost model, not a best practice

lhbench (Berkeley) measured it: Hudi merge-on-read merges were 1.3× faster than
copy-on-write but left queries 3.2× slower afterwards, Iceberg MoR merges 1.4×
faster than CoW, and MoR only starts beating CoW at roughly 100k rows updated
([lhbench.cs.berkeley.edu](https://lhbench.cs.berkeley.edu/)).

**Capability:** choose CoW vs MoR from measured update selectivity and tell the
operator why, instead of shipping one hardcoded strategy.
**Proof metric:** merge latency plus post-merge read latency at several update
selectivities, per destination.

### 6. AI mapping is being sold well past what it can prove

The industry pattern is auto-accept: Informatica's Metadata Command Center
offers CLAIRE-generated lineage with an *"enable auto-acceptance"* switch
([docs.informatica.com](https://docs.informatica.com/data-governance-and-quality-cloud/metadata-command-center/current-version/administration/link-catalog-sources-to-generate-lineage/linking-catalog-sources/step-3--perform-rule-based-or-automated-linking--save--and-run-t.html)).
The research is more careful than the products: EMNLP 2025 industry track
proposes combining LLM reasoning with embedding similarity and justification
filtering *specifically* to contain hallucination and token limits in
schema-only settings
([aclanthology.org](https://aclanthology.org/2025.emnlp-industry.120.pdf)), and
MaDI-Bench exists because end-to-end integration accuracy was not being measured
at all ([arXiv](https://arxiv.org/html/2606.30371)).

**Capability:** one mapping authority with a confidence floor and fail-closed
refusal; retrieval supplies evidence, never a second verdict.
**Proof metric:** accuracy on a named golden set, reported with the fixture
name — and the refusal rate, because a mapper that never abstains is not honest.

---

## Part 2 — What breaks in the next 5–10 years (forecast, reasoned from specs)

These are inferences from where the specifications are already moving, not
predictions dressed as facts.

1. **Row identity moves into the table format, and migrations must preserve it.**
   Iceberg v3 mandates row lineage — `_row_id`, `_last_updated_sequence_number`,
   assigned by inheritance — and adds binary deletion vectors; it also states
   lineage is *not* tracked for rows updated via equality deletes
   ([iceberg spec](https://iceberg.apache.org/spec/)). Delta's protocol defines
   Row Tracking with Row IDs and Row Commit Versions, and distinguishes
   *supported* from *enabled*
   ([delta PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)).
   Consequence: a lakehouse migration that copies rows but drops row IDs
   destroys downstream incremental correctness while every count and checksum
   agrees. Verification will have to compare *lineage*, not just values.

2. **"Zero-ETL" removes the copy, so the product's value moves to the
   contract.** Delta Sharing now shares Iceberg tables cross-platform and SAP BDC
   data lands in Databricks with no third-party ETL
   ([databricks.com](https://www.databricks.com/blog/whats-new-data-sharing-and-collaboration-summer-2025)).
   Fewer copies does not mean fewer schema breaks — it means breakage arrives
   with no pipeline in between to fail. Contracts, drift refusal and reconcile
   survive zero-ETL; hand-written mappings do not.

3. **CDC becomes HA-shaped.** PostgreSQL 17 added failover slots and slot
   synchronisation, with the explicit caveat that sync is asynchronous and the
   standby must be *ahead* of the subscriber
   ([postgresql.org](https://www.postgresql.org/docs/17/logical-replication-failover.html)).
   Consequence: the next generation of CDC bugs are failover bugs — a promoted
   primary whose slot is behind replays a window that the destination already
   committed. A destination-owned watermark is the only defence that keeps
   working, which is a direct argument for the protocol we just proved.

4. **Contracts get standardised, and enforcement becomes table stakes.** ODCS
   v3.1 with a vendor registry already exists
   ([bitol-io](https://github.com/bitol-io/open-data-contract-standard/blob/main/vendors.md)).
   Emitting/consuming ODCS is cheap for us and turns our contracts from a
   proprietary artifact into an interoperable one.

5. **Agents will generate pipelines; proof becomes the scarce good.** As LLM
   agents write more integration code, the differentiator is not who generates
   mappings — it is who can *refuse* a wrong one with evidence. This is the same
   argument the benchmarks above make, and it is the one thing an enterprise
   buyer can verify on their own data.

---

## Part 3 — Ranked plan

Ranked by (silent-loss risk removed) × (enterprise verifiability), highest first.

| # | Capability | Algorithm | Proof metric | Status |
| --- | --- | --- | --- | --- |
| 1 | Range-checksum localisation for reconcile | fingerprint → range digest → binary-search recursion | bytes/time to localise 1 bad row in 100M; zero missed mismatches | gap |
| 2 | Drift classification incl. rename | Cupid-style multi-signal scoring + value profiles | rename precision/recall on labelled fixture; 0 destructive auto-actions | partial (additive/narrowing done) |
| 3 | Sequence/identity continuity after cutover | post-write sequence + PK/identity reread and restamp | cutover fixture: first insert after migration succeeds | partial (detected, not repaired) |
| 4 | Route-by-route EOS certification (Oracle, SQL Server, DuckDB) | dest-owned watermark, already implemented | 9-scenario crash matrix per engine | gap (containers unavailable) |
| 5 | Lakehouse CoW/MoR strategy selection | selectivity-driven choice, lhbench thresholds | merge + post-merge read latency curve per destination | gap |
| 6 | Row-lineage-aware lakehouse migration | carry `_row_id`/Row ID, refuse when equality deletes make it unknowable | lineage-preservation assertion on Iceberg v3 / Delta fixture | gap |
| 7 | ODCS contract import/export | map our contract model to ODCS v3.1 | round-trip a published ODCS contract without loss | gap |
| 8 | Failover-slot CDC hardening | dest watermark rewind on promoted primary | pg17 failover fixture: replayed window applied once | gap |
| 9 | Mapping accuracy reported as a benchmark, not a score | existing SSOT mapper + MaDI-Bench-style end-to-end fixtures | accuracy **and** abstention rate per named fixture | partial |

Items 1–3 are the ones an enterprise evaluator will hit in their first week, and
none of them requires a new subsystem.

## Sources cited

Debezium exactly-once blog · Debezium PR #655 · Estuary materialization protocol
· PostgreSQL 19 logical replication restrictions · PostgreSQL 17 logical
replication failover · Airbyte issue #50874 · Fivetran duplicate-records-after-PK
docs · dbt incremental models docs · Iceberg table spec (v3) · Delta
PROTOCOL.md · lhbench · sirupsen napkin math #14 / datafold data-diff · US patent
10223394 · Databricks data-sharing summer 2025 · Informatica Metadata Command
Center automated linking · EMNLP 2025 industry: Group, Embed and Reason ·
MaDI-Bench · ODCS v3.1 vendor registry.
