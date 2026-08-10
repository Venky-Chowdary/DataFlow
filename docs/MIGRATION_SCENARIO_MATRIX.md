# Migration Scenario Matrix — measured behaviour

Every verdict below was produced by running the product path
(`UniversalTransferEngine.execute_tracked`, i.e. preflight gates → write →
Gate-8) against **live** engines: PostgreSQL 16 (`:5433`), MySQL 8 (`:3307`)
and Oracle Free (`:1521/FREEPDB1`), source always PostgreSQL.

Harness: `/home/ubuntu/repro/migration_scenario_matrix.py`
Artifact: `/home/ubuntu/repro/migration_scenario_matrix_results.json`
Contract matrix (append / delta / duplicate / DDL drift, 5 engines):
`/home/ubuntu/repro/ddl_identity_matrix.py` →
`/home/ubuntu/repro/ddl_identity_matrix_results.json`

Nothing here is aspirational. Where measured behaviour differs from the
contract the product should honour, the row says **GAP** and the gap is listed
again at the bottom with its severity. No scenario is rounded to green.

## Legend

| Term | Meaning |
|------|---------|
| `blocked pre-write` | Refused by a gate; **0 rows** reached the destination |
| `blocked at write` | The database rejected it; the job fails, but only the engine caught it |
| `written` | Rows committed and the job reported success |
| GAP | Measured behaviour is not the behaviour we should ship |

---

## 1. Column-count scenarios (the 30 → 20 case)

Source table: `id` + `c1..c30`. Destination: existing table with `id` + `c1..c20`.

| # | Scenario | PostgreSQL | MySQL | Oracle | Verdict |
|---|----------|-----------|-------|--------|---------|
| 1.1 | 20 of 30 source columns mapped, remaining 10 **not mentioned at all** | blocked pre-write | blocked pre-write | blocked pre-write | GAP-1 **fixed** |
| 1.2 | Same, remaining 10 marked `intentional_omit` | written, 5 rows | written, 5 rows | written, 5 rows | GAP-2 **fixed** |
| 1.3 | Destination has a `NOT NULL` column with no default and no mapping | blocked at write | blocked at write | blocked at write | **GAP-3 — not predicted** |
| 1.4 | Destination has an extra **nullable** column nothing maps into | 5 rows written, job passed | same | same | GAP-4 **fixed** |

Detail:

- **1.1** is the exact case in the question ("source has 30, destination has
  20"). It used to write 20 columns and report green. Gate `g13_source_coverage`
  now refuses before any write and names the columns:
  `10 source column(s) are neither mapped nor declared omitted: c21, c22, c23,
  c24, c25, c26, c27, c28 (+2 more) — Datawrap will not drop them silently.`
  Every source column must be a write mapping, an explicit `intentional_omit`,
  or a blocker.
- **1.2** declaring the omission — the honest operator action — used to be
  punished with `Sample transform / cast failures: 4 column(s) fail write-path
  transforms on the Validate sample (c21, c22, c23, c24)`. A declared omission
  has no destination carrier, so it is now excluded from the transform dry run,
  quarantine-cell preview, coercion probe, type-coercion validation and the
  Gate-8 read-back projection, and recorded as the operator's decision in the
  proof bundle (`source_coverage.omitted`) instead. Live re-run:
  `/home/ubuntu/repro/gap1_source_coverage_live_results.json`.
- **1.3** the destination's own `NOT NULL` is what stops the write
  (`ORA-01400`, MySQL `1364`, PG `not-null constraint`). The outcome is safe —
  0 rows land — but the operator learns at write time from an engine error
  instead of at Validate from a gate that says "destination requires
  `tenant_id`; it has no default and no source mapping".
- **1.4** the untouched nullable column was read back by Gate-8 on the
  destination side but had no counterpart on the source side, so the checksums
  differed and the job was marked failed **after** committing 5 rows — a false
  failure with a real write behind it. Every read-back issues `SELECT *` and
  used the returned cursor description as the digest columns; it now projects
  to the mapped target columns through `services/readback_projection.py`
  before hashing, on all thirteen SQL/warehouse read-back paths. A mapped
  column the destination did **not** return is left unprojected so the
  mismatch still surfaces — narrowing the digest to hide it would be the same
  silent-drop failure. Live re-run:
  `/home/ubuntu/repro/gap4_mapped_projection_live_results.json`.

## 2. Type and fidelity scenarios

| # | Scenario | PostgreSQL | MySQL | Oracle | Verdict |
|---|----------|-----------|-------|--------|---------|
| 2.1 | `VARCHAR(255)` (120-char values) → `VARCHAR(10)` | blocked pre-write | blocked pre-write | blocked pre-write | correct — fails closed, no truncation |
| 2.2 | `DECIMAL(12,4)` → `DECIMAL(6,1)` | blocked pre-write | blocked pre-write | blocked pre-write | correct — precision collapse refused |
| 2.3 | `TIMESTAMPTZ` → naive `TIMESTAMP` / `DATETIME(6)` | blocked pre-write | blocked pre-write | blocked pre-write | correct as fail-closed; **needs a Risk Contract path** |
| 2.4 | `BOOLEAN` → `BOOLEAN` / `TINYINT(1)` / `NUMBER(1)`, incl. NULL | written `True/False/NULL` | written `1/0/NULL` | written `1/0/NULL` | correct |
| 2.5 | `JSONB` → `JSONB` / `JSON` | written, structure preserved | written, structure preserved | n/a | correct |
| 2.6 | `JSONB` → Oracle `CLOB` | — | — | blocked pre-write | fail-closed; **GAP-5** — CLOB is a legitimate JSON carrier on Oracle |
| 2.7 | Unicode / zero-width / newline / emoji `TEXT` → `TEXT`,`VARCHAR(400)`,`VARCHAR2(400)` | blocked pre-write | blocked pre-write | blocked pre-write | **GAP-6** — PG `TEXT`→`TEXT` is not a narrowing |

2.1–2.3 are the behaviour we want: unsafe narrowing never silently truncates
or rounds, and no rows are committed. 2.3 is defensible (an offset is lost)
but the operator has no in-product way to say "store UTC, I accept it" — the
Risk Contract exists for exactly this and is not wired to this verdict.

2.7 is a false positive: `TEXT → TEXT` on the same engine cannot collapse
fidelity. On PostgreSQL it trips a single gate; on MySQL and Oracle it is
reported as a narrowing to a bounded type, which is at least arguable there.

## 3. Identity, duplicates and existing data

| # | Scenario | PostgreSQL | MySQL | SQL Server | Oracle | SQLite |
|---|----------|-----------|-------|-----------|--------|--------|
| 3.1 | Append into an existing table | written 5 | written 5 | written 5 | written 5 | written 5 |
| 3.2 | Second incremental append with a cursor delta | written 5 (dest 10) | same | same | same | same |
| 3.3 | Re-run **without** a delta — every key already at rest | blocked pre-write, dest unchanged | same | same | same | same |
| 3.4 | Source itself contains a duplicate identity | blocked pre-write | blocked pre-write | — | blocked pre-write | — |
| 3.5 | Map stamp narrowed after Validate, empty destination | blocked pre-write, DDL identity named | same | same | same | same |
| 3.6 | Destination created with quoted mixed-case identifiers | written | written | — | written | — |

3.3 and 3.5 are distinct verdicts with distinct messages
(`Duplicate identity keys …` vs `Decision Artifact DDL identity diverged …`),
which was the point of the drift fixture: with rows at rest the duplicate gate
fires first, so the drift case only proves the DDL gate on an empty
destination.

3.6 on Oracle is the case that previously created a *second* table beside the
quoted one and split the job's rows across both; writer, introspection,
collision probe and Gate-8 read-back now resolve one catalog identity.

## 4. Sync-mode contract

| Mode | Behaviour today |
|------|-----------------|
| `incremental_append` | Appends; re-run without a delta is refused by the duplicate gate, not silently upserted |
| `full_refresh_append` | Appends |
| overwrite / upsert / merge | Require an explicit sync contract; append never silently becomes one |
| resume | Needs a committed checkpoint; a failed run offers Validate, not Resume |

## 5. What the operator is shown

Produced today: mapping pairs with confidence and fidelity risk per column,
coercion preview with per-column sampled/NULLed/failed counts, 13 validation
gates with scope (sample vs full-selected), Decision Artifact hash, Map→DDL
fingerprint, pre-write Gate-8 simulation explicitly marked "not migration
proven", post-write row-count + checksum proof, signed proof pack and audit
PDF, and quarantine/rejected-row detail.

Not produced today, and needed for the scenarios above:

- a **mapping-shape panel**: source column count vs destination column count,
  mapped pairs, declared omissions, destination-only columns and how each will
  be filled (default / generated / NULL / blocked);
- a **destination-requirement** verdict at Validate for `NOT NULL`-without-
  default columns (currently the database is the first thing to object);
- a Risk-Contract path for accepted-lossy conversions such as 2.3.

## 6. Gap list (ordered by severity)

| ID | Gap | Severity | Where |
|----|-----|----------|-------|
| GAP-1 | ~~Unmapped source columns are dropped and the run is green~~ **fixed** — `g13_source_coverage` hard gate | Was critical — silent data loss | `services/source_coverage_gate.py` |
| GAP-4 | ~~Untouched destination columns break Gate-8 checksum *after* rows are committed~~ **fixed** — read-back digests the mapped projection | Was high | `services/readback_projection.py` |
| GAP-2 | ~~Declared `intentional_omit` columns are transform-checked and refuse the run~~ **fixed** — omissions excluded from every write-path probe | Was high | `services/mapping_constraints.write_mappings` call sites |
| GAP-3 | Destination `NOT NULL`-without-default is caught by the engine, not predicted | Medium | preflight destination-requirement gate |
| GAP-6 | `TEXT → TEXT` reported as fidelity collapse | Medium — false positive erodes trust | type fidelity classifier |
| GAP-5 | Oracle `CLOB` not accepted as a JSON carrier | Low | type compatibility table |
| GAP-7 | No Risk-Contract path for accepted timezone loss | Low | conversion contract wiring |

None of these are route-specific: every one reproduces identically on
PostgreSQL, MySQL and Oracle, which is why they are listed as algorithm gaps
rather than connector bugs.

## 7. Not yet measured

Scenarios deliberately absent from this run, so they must not be claimed:
generated/identity destination columns, CHECK-constraint rejection on append,
foreign-key ordering across multiple tables, sequence high-water marks,
partitioned destinations, SQL Server and SQLite for sections 1–2, warehouse
and SaaS destinations (credentials not provisioned), and quarantine/replay
row-count invariants under partial failure.
