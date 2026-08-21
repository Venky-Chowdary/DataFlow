# Enterprise architecture audit — what is proven, what is not

**Scope.** Every capability a migration/assurance buyer evaluates, labelled from
evidence that exists in this repository today, plus the gaps ranked by the risk
they carry for a customer's data. Written to be read by a sceptic: a claim with
no artifact behind it is labelled **Unproven**, not softened.

**Labels.**

| Label | Meaning |
| --- | --- |
| **Proven** | Measured on a named live fixture or matrix whose artifact is in-repo, and re-runnable |
| **Partial** | Proven on some engines/routes; the rest are wired but unmeasured |
| **Unproven** | Implemented, no measurement — treat as a hypothesis |
| **Failing** | Measured and known to be wrong or blocked |
| **Not executable here** | Requires a service/credential this environment cannot reach |

**Anti-inflation rules applied while writing this.** A connector tile is not a
live route. One local benchmark is not an SLA. At-least-once is not
exactly-once. A writer's acknowledgement is not proof the destination holds the
row. One green route is not enterprise readiness.

---

## 1. Capability ledger

### Data movement

| Capability | Label | Evidence / why not |
| --- | --- | --- |
| Heterogeneous batch transfer, destination-authoritative schema | **Proven** | `docs/MIGRATION_SCENARIO_MATRIX.md`, route matrix in `docs/RESYNC_MATRIX_EVIDENCE.md`; writes are by destination column name (`services/shape_contract.py`) |
| 1M-row single-table load, PostgreSQL → MySQL | **Proven (one fixture)** | 221.5 s / 4,515 rows/s, dest `COUNT(*)`=1,000,000, 0 rejected — `docs/THROUGHPUT_1M_EVIDENCE.md`. Local containers, 10 narrow columns. **Not an SLA** |
| Re-sync into a non-empty destination (append / upsert / overwrite) | **Proven** | 20/20 scenarios × 2 live engines, including 3 distinct pre-write refusals — `docs/RESYNC_MATRIX_EVIDENCE.md` |
| Bulk-load fast paths | **Partial** | PostgreSQL destination uses `COPY FROM STDIN` with disk-spill buffering (`connectors/postgresql_writer.py`). MySQL destination is `executemany` only — no `LOAD DATA LOCAL INFILE`. Redshift/Snowflake have staged-copy paths |
| Snowflake as source | **Not executable here** | No live warehouse credential in this environment; reader is exercised against recorded metadata only |
| Multi-table / whole-schema migration ordering (FK-aware) | **Partial** | Dependency ordering exists; not measured on a schema with cyclic FKs |

### Change data capture

| Capability | Label | Evidence / why not |
| --- | --- | --- |
| PostgreSQL logical decoding (`pgoutput`) snapshot + LSN handoff | **Proven** | `tests/test_cdc_postgres_resume_effectively_once.py`, `tests/test_cdc_postgres_multi_stream_resume.py` against live PG |
| MySQL binlog (ROW) capture | **Proven** | `tests/test_cdc_mysql_binlog_integration.py`, `tests/test_cdc_shared_reader_integration.py` against live MySQL |
| Refusal to substitute cursor polling for log capture | **Proven** | `services/cdc_capability.py` + `tests/test_cdc_capture_downgrade_honesty.py`: slot quota, missing grant, unreachable source and unknown errors fail closed; only "server emits no log" and "reader driver absent" degrade, and they stamp `cdc_delete_capture=false` |
| Exactly-once apply | **Partial** | Dest-owned watermark protocol measured with crash injection on live PostgreSQL/MySQL/Oracle/SQL Server (`docs/CDC_EXACTLY_ONCE_LIVE_EVIDENCE.md`). Platform default stays **at-least-once upsert**; DuckDB / `generic_sql` / Snowflake are wired and unmeasured |
| Oracle LogMiner, SQL Server CDC, MongoDB change streams | **Unproven** | Readers exist (`connectors/oracle_logminer.py`, `sqlserver_cdc_native.py`, `mongodb_change_stream.py`); no live capture matrix in-repo |
| DDL/schema-change capture during CDC | **Unproven** | Drift detection exists for batch (`connectors/schema_drift.py`); mid-stream DDL is not measured. Debezium-class systems make explicit guarantees here; we do not |

### Reconciliation and proof

| Capability | Label | Evidence / why not |
| --- | --- | --- |
| Independent source re-read + mapped-projection checksum | **Proven** | `src/transfer/reconcile_step.py`; the writer's own hash can no longer be presented as an independent scan |
| Destination pre-count delta proof for appends | **Proven** | `services/dest_precount.py`; append into a non-empty table proves `after − before == written` and fails closed if the pre-count cannot be taken |
| Keyed read-back only where uniqueness is physically enforced | **Proven** | Inferred/advisory keys are rejected for append proof; upsert conflict targets remain eligible |
| Live-population digest scope when a keyed upsert deletes tombstoned keys | **Proven** | `services/row_conservation.live_records_for_digest` + one disclosure owner (`record_tombstone_digest_scope`) |
| Quiet incremental poll classified as `no_op_destination_unchanged` | **Proven** | `services/reconcile_coverage.py` — a quiet poll is no longer routed through the full-migration checksum ladder |
| Quarantine + replay of bad rows | **Partial** | `services/quarantine_dlq.py`, `replay_safety.py`, per-row policies surfaced in Map; replay is not measured end-to-end at volume |
| FK / index / sequence validation after load | **Partial** | Delivered and measured on live PostgreSQL and MySQL: 7 aspects (PK, unique, FK, index, NOT NULL, default, CHECK) plus identity high-water, 4/4 scenarios matching their declared verdict — `docs/STRUCTURAL_ATTESTATION_EVIDENCE.md`, `services/physical_state_diff.py`, `services/identity_watermark.py`. Oracle / SQL Server / warehouse destinations and multi-table ordering at volume are unmeasured |
| Audit PDF / migration certificate | **Partial** | `docs/MIGRATION_CERTIFICATE.md` and a certificate surface exist; the PDF artifact is not generated in any measured run here |

### Orchestration and scale

| Capability | Label | Evidence / why not |
| --- | --- | --- |
| Durable job store, checkpoints, resume | **Proven** | MongoDB-backed job store, fail-closed when unavailable; checkpoint/resume tests in the CDC and stream suites |
| Multi-replica safety: claim queue, leases, fencing | **Partial** | `services/scheduler_mode.py` (`local` / `claim` / `auto`), `worker_leases`, focused lease + fencing tests. No measured multi-replica soak |
| Parallelism | **Failing as an enterprise claim** | `ThreadPoolExecutor` only (`services/parallel_chunks.py`, default 4 workers). The 1M phase profile puts 73.4% of busy time in per-cell transform/validate CPU — GIL-bound. Vertical scale beyond ~1 core of Python transform is not available |
| Backpressure / memory bounds | **Proven** | Bounded in-flight chunks, spill-to-disk for wide batches and checksum sets |
| Observability | **Partial** | Prometheus-compatible `/metrics`, `/ops/freshness`, optional OpenTelemetry spans across the pool boundary (`services/tracing.py`). No SLO/alert pack shipped |
| Security | **Partial** | RBAC middleware with viewer/editor/admin, audit log, SSO state, secret redaction in spans. **No SOC 2 claim**, no encryption-at-rest module, and the dev role maps to `editor` — a production deployment must gate on the real claim |

---

## 2. Head-to-head, by capability (not by brand)

`docs/COMPETITIVE_ANALYSIS.md` holds the positioning; this section holds only the
capability verdicts that follow from the ledger above.

| Capability | Us | Market reference | Verdict |
| --- | --- | --- | --- |
| Connector breadth | 44 transfer-ready drivers, 77 PRODUCTION_SKU routes (`apps/api/data/proofs/transfer_ready_matrix.json`) | Airbyte/Fivetran: hundreds of maintained SaaS connectors | **We lose.** Do not contest this |
| SaaS incremental sync | `saas_incremental_sync: false` in the matrix claims block | Fivetran's core product | **We lose** |
| Log-based CDC transport maturity | PG + MySQL proven; Oracle/SQL Server/Mongo unproven; no DDL-change guarantee | Debezium: 6 mature engines, DDL events, schema history topic | **We lose on breadth**, comparable on PG/MySQL resume correctness |
| Exactly-once apply | Opt-in per route, 4 engines measured with crash injection | Estuary claims exactly-once; Debezium is at-least-once by design | **Competitive where measured**, and we say so instead of claiming platform-wide |
| Semantic mapping into an existing schema | BM25 + Hungarian assignment, calibrated confidence, destination-authoritative, one owner | DMS/SCT + manual; Informatica CLAIRE; Airbyte has none | **We win** |
| Full-population checksum reconciliation with declared scope and denominators | Order-independent, spill-to-disk, mapped projection, delta proof for appends, refusal classes | DMS row validation is shallower; Datafold diffs but does not move | **We win** |
| Fail-closed type fidelity and refusal semantics | Decision Kernel; unknown destination existence never becomes create-new | Silent coercion is the industry norm | **We win** |
| Throughput per node | 4,515 rows/s on one local fixture, GIL-bound | Native `COPY`/`LOAD DATA` reach 10⁵ rows/s; managed platforms scale horizontally | **We lose** |
| Managed operations | Self-hosted, one process | Fivetran/Estuary are managed services | **Different product**, not a gap to close now |

---

## 3. Gaps ranked by customer risk

Ranking key: **D** = risk of customer data loss or a false proof, **O** =
operational risk, **T** = throughput, **B** = enterprise buying blocker.

| # | Gap | Class | Why it matters | Smallest proof that closes it |
| --- | --- | --- | --- | --- |
| 1 | Post-load structural attestation covers only PostgreSQL and MySQL | **D** | The attestation itself now exists and is measured on those two engines; on Oracle, SQL Server and warehouse destinations a load can still be row-perfect and leave the destination unusable — broken FKs, missing indexes, a sequence that collides on the next application insert | Extend the live harness to the remaining destination engines and to a multi-table schema with cyclic FKs, stamped into the proof pack |
| 2 | Oracle / SQL Server / MongoDB CDC unmeasured | **D** | Those readers can be selected today. An unmeasured log reader is exactly how the 710k-row class of defect happens | Live capture + resume matrix per engine, in the shape of the PG/MySQL tests |
| 3 | Mid-stream DDL during CDC | **D** | A column added on the source silently stops arriving, or breaks the apply. Debezium-class systems handle this explicitly | Add-column / drop-column / type-widen while streaming, per engine, fail closed where unsupported |
| 4 | Replay of quarantined rows not measured at volume | **D/O** | Quarantine is only trustworthy if replay works after a fix; otherwise it is a graveyard | Seed N bad rows, fix mapping, replay, prove destination count and digest |
| 5 | Transform CPU is GIL-bound; no process-level parallelism | **T** | 73.4% of busy time is per-cell Python. Threads cannot use more cores, so a bigger box does not buy throughput. This is the ceiling behind "reduce the 20 minutes a lot" | Process-pool or vectorised (Arrow) transform path, re-run the 1M harness, publish before/after |
| 6 | MySQL destination has no `LOAD DATA LOCAL INFILE` path | **T** | Second-order after #5, and it must not be taken naively: `LOAD DATA` outside STRICT mode coerces silently, which this product forbids. Needs strict-mode enforcement + per-row error attribution before it may ship | Bulk path gated on STRICT + warning-to-quarantine attribution, measured against `executemany` on the same fixture |
| 7 | No multi-replica soak | **O** | Leases and fencing pass focused tests; a real fleet failing over mid-job at 1M rows is untested | Two workers, kill one mid-run, prove exactly the expected destination population |
| 8 | Snowflake live auth/network untested | **O/B** | The customer's actual failing route started at Snowflake. Everything there is reasoned, not measured | One live credentialled run end-to-end, with reconcile |
| 9 | Audit PDF not generated in a measured run | **B** | Auditors are the buyer for the assurance story | Generate for one 1M run, attach to the evidence pack |
| 10 | Security posture: no encryption-at-rest module, dev role maps to `editor` | **B** | Enterprise security review will find both | Role claim enforced with no dev fallback in production config; document at-rest expectations of the deployment target |
| 11 | Connector breadth and SaaS incremental | **B** | Loses SaaS-fleet bake-offs | Do not chase. Redirect those buyers; deepen the migration wedge |

---

## 4. What a customer may be told today

Permitted, with the artifact named:

* Heterogeneous migration with fail-closed type fidelity and operator-visible refusals.
* Reconciliation with a declared scope, denominator and proof basis — including
  refusal to claim independence when only the writer's hash exists.
* PostgreSQL and MySQL log-based CDC with snapshot + LSN handoff, and a refusal
  to silently fall back to cursor polling.
* 1M rows in 221.5 s on the named local PostgreSQL → MySQL fixture.
* Opt-in exactly-once apply on the four engines measured with crash injection.
* Post-load structural attestation (PK, unique, FK, index, NOT NULL, default,
  CHECK, identity high-water) on live PostgreSQL and MySQL destinations, where a
  hand-written destination missing its guarantees is reported absent rather than
  green.

Not permitted:

* Any connector count above the 44 transfer-ready drivers / 77 PRODUCTION_SKU routes.
* Platform-wide exactly-once.
* Structural (FK/index/sequence) validation on any destination engine other than
  PostgreSQL and MySQL, or on multi-table schemas.
* Any throughput number as an SLA, or extrapolated to another route.
* SOC 2, or any compliance certification.
