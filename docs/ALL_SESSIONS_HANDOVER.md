# All-sessions handover — connector matrix wave

One file for the whole wave: what six parallel sessions changed, what is
**measured**, what is a **known open defect**, and what was **never measured**.
Nothing here claims deployment readiness — §6 is the list that blocks that claim.

Integration branch: `feature/Venkat-Analysis` (not `main`; no CI workflow runs on
it). Per-session engineering detail lives in `docs/SESSION_HANDOVER.md`; this file
is the index across sessions.

---

## 1. Session index

| # | Track | Session | Branch | Role |
|---|-------|---------|--------|------|
| 0 | Lead — Transfer Studio, decimal/locale, Settings, Pilot | [66fff009](https://app.devin.ai/sessions/66fff009c2af4275a46fce7f561d1476) | `feature/Venkat-Analysis` (direct) | integration + defects from live runs |
| A | SQL engines, 100K duplex matrix | [997cced0](https://app.devin.ai/sessions/997cced052c2437286df1f2d7882d387) | `devin/track-a-sql-100k` | PostgreSQL / MySQL / SQL Server / Oracle / SQLite / DuckDB, each as source **and** destination |
| B | File formats + object stores, 100K | [65184e26](https://app.devin.ai/sessions/65184e26cd794f71924f08f10b7429ce) | `devin/track-b-files-100k` | CSV/TSV/PSV, JSON/JSONL, Parquet, Avro, Excel, XML; MinIO/Azurite/fake-GCS |
| C | NoSQL + analytics, 100K | [47e93e0e](https://app.devin.ai/sessions/47e93e0e976646dcb4ef109cbd3947be) | `devin/track-c-nosql-100k` | MongoDB, Redis, DynamoDB, Elasticsearch, ClickHouse, vector stores |
| D | Sync modes + schedules, crash injection | [61edfbfb](https://app.devin.ai/sessions/61edfbfbca5f45e992d1f6085dbdd3df) | `devin/track-d-modes-100k` | append / overwrite / incremental / upsert / CDC / SCD2 / mirror, schedulers, resume |
| E | Connector catalog readiness | [73baf565](https://app.devin.ai/sessions/73baf565feb44180bcd5629e605d5882) | `devin/track-e-catalog-readiness` | classify every catalog tile as transfer-live vs planned |

Each track branch carries its own PR into `feature/Venkat-Analysis` with its
measured cell table. Track PR links are added to this table as they land.

---

## 2. What the lead session fixed (all pushed to `feature/Venkat-Analysis`)

Root cause first; the pasted route is the signal, the shared owner is the fix.

- **`96aa3cb8` — exact Decimal and type fidelity (12 defects).**
  `$1,234.56` was stripped into a `DECIMAL` bind nobody declared (now refused at
  `connectors/sql_bind.coerce_decimal_wire`); a refused DynamoDB `NS` member was
  swallowed and the envelope landed as a *document* instead of a number set;
  JSON/JSONL exports quoted every `Decimal`, retyping numeric columns to text for
  every downstream reader (`services/value_serializer.json_dumps_exact_numbers`);
  generic-SQL JSON read back as a quoted JSON *string* because SQLAlchemy swapped
  `_ExactJSON` for the dialect impl (`_gen_dialect_impl` now keeps the
  processors); a SQLite-backed writer reported the db **file path** as its schema,
  so reflection read `"/tmp/x.db".sqlite_master` and structural attestation said
  "unreadable" about a table that exists (`services/physical_state_diff._catalog_schema`).
  Measured: `1122 passed, 12 skipped` on the type/decimal/mapping selection,
  `35 passed` on attestation + generic-SQL JSON.
- **`4bfde98a` / `1b2bf77a` — one owner for number locale.** A typed carrier's
  number reads as itself (`WIRE`); file text settles by per-column evidence and
  only a genuinely ambiguous column falls back to US — stamped as
  `number_locale_assumed` on Validate and in the proof artifact, EU one click
  away. This is what made a faithful `NUMERIC(12,3)` → MySQL route refuse
  `'10.129'` as "ambiguous grouping" with 0 rows landed, and what made Gate-8
  read `20.5` vs `20.500` as corruption.
- **`6a3049c3` — population decimal sizing.** The `NUMBER(11,8)` failure on the
  1M Snowflake run: scale inferred from a *sample*, so 6 scale-9 values were
  rejected at write time and the whole load committed 0 rows after Validate had
  cleared it. Sizing now scans the replayable population; create-new widens;
  an existing destination's DDL stays authoritative and the non-fitting values
  quarantine instead of failing the load silently late.
- **`7f212a71` — Transform step had no scroll.** The step laid out 7,487px inside
  a 752px `overflow:hidden` panel, so *Continue → Map* was unreachable and testing
  was blocked. Step is now its own scroll host with sticky header/action bar.
- **`46cd648f` — CDC honesty.** A failed attach to logical decoding / binlog no
  longer degrades to cursor polling (a cursor poll cannot see a hard DELETE, so
  those runs were green while the destination kept deleted rows).
- **`03c1edc5`, `92c20f39`, `a1cc1f91`, `51342260`, `74ba51e7`, `6a547bec`** —
  structural attestation false alarms (PostgreSQL vs MySQL CHECK/identity
  spelling), `assume_timezone` as a real Map control, real accounts + workspaces +
  admin/editor/viewer as related MongoDB collections, Pilot answering the wrong
  connector on a shared label, destination reload as a real control, create-new
  vs "existence unproven".
- **Throughput:** 1M PostgreSQL→MySQL append **221.5 s (4,515 rows/s)**,
  destination `COUNT(*)` 1,000,000, 0 rejected, reconciled by independent source
  reread (`docs/THROUGHPUT_1M_EVIDENCE.md`). Local fixture number, not an SLA.

---

## 3. Defects the tracks found and fixed

These are real product defects found by running the matrix, not test edits.

**Track A — SQL.** Connection options (`TrustServerCertificate`/TLS) dropped on
the read + introspect paths; SQL Server UUID truncated to 16 chars;
`DATETIMEOFFSET` reflected as `timestamp_ntz` and shifted to UTC; `NVARCHAR`
classified latin1 so every CJK/emoji row quarantined; pyodbc `executemany` short
rowcount reported as rejected rows (**false data loss**); `BIGINT` reflected as
logical integer and refused as out-of-range; registry writer path dropped
`source_schema_catalog`/`empty_cells_as_null`; incremental keyset seek ignored the
watermark for SQL Server/Oracle sources (full re-read + duplicate PK on run 2);
MERGE staging inserts bound untyped, silently shifting tz-aware instants on upsert.
16 cells pass, 0 fail at time of writing.

**Track B — files/object stores.** Path-based XML source counted as unmeasured
(every path XML transfer refused); `.psv` classified unknown; declared
`timestamptz` collapsed to naive datetime; JSON/JSONL explicit `""` silently
converted to NULL.

**Track C — NoSQL/analytics.** Key-addressed destinations (Redis, DynamoDB,
Elasticsearch, Mongo, vector) proved append conservation by `COUNT(*)` growth —
a rewrite of an existing key read as **silent loss**; now closed by a destination
key census. Redis/DynamoDB incremental replayed the whole keyspace on run 2
(cursor bound now applied client-side). A sample-measured numeric width from a
schemaless store was stamped as a *declaration*, fail-closing wider later pages.

**Track D — sync modes/schedules.** MongoDB CDC silently dropped hard deletes for
business-key pipelines (`documentKey` carries `_id` only) — now pre-image based
and fail-closed; `workspace_access` allowed cross-workspace schedule reads for an
actor in both workspaces; PostgreSQL WAL LSNs mis-ordered in MySQL destination
resume predicates; Mongo resume tokens routed through a BIGINT file-position cast;
CDC cells were graded on a single bounded poll window, which read as loss +
duplication.

**Track E — catalog readiness.** Elasticsearch strict reconcile hashed only 500
hits against a whole-source digest; the source uniqueness probe refused
Redis/Elasticsearch sources; the DynamoDB writer created numeric keys as `S`;
Mongo contract/breaker persistence rejected `Decimal`. 12 cells classified.

---

## 4. Open defects — state as of 2026-09-01

The wave's original list is now mostly closed; `docs/OPEN_DEFECT_REGISTER.md` is
the authoritative per-defect record with the live evidence behind each closure.

**Closed since this file was last written** (each with a live-engine proof, and
the PR that carries it):

| Item | Closed by |
|------|-----------|
| PostgreSQL key census `operator does not exist: text = integer` (D6) | merged pre-#125 |
| Elasticsearch `DECIMAL` written as a string / dynamic `text` mapping (D7) | merged pre-#125 |
| 500-row reconcile cap in the hosted verifiers, plus the Salesforce 2,000-row read truncation found under it (D9) | merged pre-#125 |
| bare token `long` read as Oracle `LONG` (D8) | merged pre-#125 |
| base-branch typed-transfer failures (D17) | merged pre-#125 |
| `mysql→mysql` CDC snapshot lock wait, root-caused (D13) and its remaining coordinate window (D20) | [#128](https://github.com/Venky-Chowdary/DataFlow/pull/128) |
| destination recreate silently redefining a declared carrier — new gate G19 (D19) | [#127](https://github.com/Venky-Chowdary/DataFlow/pull/127) |
| Iceberg on Windows drive-letter warehouses, and the writer reporting the warehouse directory as the schema (D18) | [#129](https://github.com/Venky-Chowdary/DataFlow/pull/129) |
| CI mypy baseline (6 errors, not 4) and the ES `id → id` 0.63 confidence anomaly (D16) | [#130](https://github.com/Venky-Chowdary/DataFlow/pull/130) |
| sampled dest shape compared as a declared catalog, so run 2 refused run 1 (D1) | [#132](https://github.com/Venky-Chowdary/DataFlow/pull/132) |

**Still open (not defects — never measured / environment-blocked):**

- ~~Scheduler DST + workspace-ownership cells not re-measured after the access fix.~~
  **Closed ([#138](https://github.com/Venky-Chowdary/DataFlow/pull/138)).**
  `tests/test_scheduler_dst_workspace_remeasure.py`: cadence cells including
  DST boundary pass; sibling `X-Workspace-Id` GET is 404 and the list excludes
  the foreign schedule; non-member read/create 403/404. The 100K beat was not
  re-run.
- ~~Governance ops (mask/hash/redact) not yet recorded in the audit certificate.~~
  **Closed ([#139](https://github.com/Venky-Chowdary/DataFlow/pull/139)).**
  `services/governance_ops.py` harvests declared mask / hash / redact, Execute
  stamps the ledger on the job, and the signed migration certificate (and
  proof pack) renders it. Live sqlite→sqlite 2-row proof in
  `tests/test_governance_ops_certificate.py`.
- The connector-family matrix never completed (Track A halted at 122 of 225).
- SFTP daily Excel sync modes started, not finished.
- SAML/SSO round-trip — needs a real IdP, unprovable here.

**Found by driving the app before handover (2026-09-06,
[#168](https://github.com/Venky-Chowdary/DataFlow/pull/168)) — register §5:**

Closed with a live Postgres→MySQL proof and an independent destination reread:
a declared control total failed the whole job on MySQL because the G21 scan
emitted PostgreSQL-only `CAST(... AS TEXT)` (**D21**); Map had no way to declare
a crosswalk for a plain `VARCHAR` code column, so G20 could never be asked
(**D22**); a *proven* control total rendered nowhere an operator looks, only in
the exported pack (**D23**); and the MySQL COPY fast paths raised the absence of
`os.mkfifo` out of the fast path instead of declining it, so the row-writer
fallback never ran and the destination was created empty (**D24**).

Still open, and a client has to be told: a freshly exported proof pack fails the
product's own **verify** control (`content_sha256` / HMAC / chain-anchor,
**D25**); a destination-type override is lost on a Map → Validate → Map round
trip (**D26**); and the Gate-8 card on a *completed* Theater is unproven because
the active job is cleared the moment the run finishes, so that surface is
transient by construction. Schedules/retries/overlap/DST, cancellation,
quarantine and replay, the Evidence Chain / Operations / Contracts / Proofs
pages, workspace roles, G19 reachability, and the Mongo and MinIO routes were
untouched by this wave and remain unmeasured.

---

## 5. Skips, with reasons (no invented green)

- AWS S3, real GCS, real ADLS, hosted BigQuery, Snowflake, Redshift, Databricks,
  Salesforce/HubSpot/Airtable: **no credentials in this environment**.
- ClickHouse: capability is `Planned` — not available for production transfer.
- Iceberg: no proven REST catalog (`DATAFLOW_ICEBERG_REST_URI` unset).
- Elasticsearch privileges/index probe: unproven.

---

## 6. What still blocks a deployment claim

Not measured end to end by anyone in this wave:

1. All 60+ catalog connectors as source **and** destination (only the engines that
   run locally are measured; the catalog tile count is not evidence).
2. 100K on every route, and 1M on every sync mode (1M is measured on exactly one
   route: PostgreSQL→MySQL append).
3. Live Snowflake auth/network.
4. Real SMTP / Slack / Teams delivery (needs real endpoints).
5. MCP from a real client, and chatbot/RAG against a live OpenAI key.
6. Real host routing per client domain, SSO/IdP, KMS/BYOK.
7. CDC remains **at-least-once** except where a named route + crash injection is
   in `docs/CDC_EXACTLY_ONCE_LIVE_EVIDENCE.md`.

---

## 6a. Enterprise-2026 feature wave (this session)

Driven by the research report `Datawrap — the future of enterprise data
migration (2026)`; delivery state, evidence and the remaining tiers are in
`docs/ENTERPRISE_2026_DELIVERY_STATUS.md`. Summary: N1 (Field Reduction Ledger,
gate G16) and N3 (durable hash-chained evidence) are merged and browser-verified;
D1 (sampled dest carrier provenance) is closed with a live Postgres→MinIO
independent reread ([#132](https://github.com/Venky-Chowdary/DataFlow/pull/132));
N2–N5 are merged ([#133](https://github.com/Venky-Chowdary/DataFlow/pull/133),
[#134](https://github.com/Venky-Chowdary/DataFlow/pull/134),
[#135](https://github.com/Venky-Chowdary/DataFlow/pull/135)). YAML and
fixed-width are transfer-live **file sources**
([#136](https://github.com/Venky-Chowdary/DataFlow/pull/136)); their 100K
Postgres cells passed dest COUNT=99,991, DLQ=9, independent checksum
([#137](https://github.com/Venky-Chowdary/DataFlow/pull/137)). Scheduler DST +
workspace-ownership cells re-measured
([#138](https://github.com/Venky-Chowdary/DataFlow/pull/138)). Governance ops
(mask/hash/redact) are stamped on the signed certificate
([#139](https://github.com/Venky-Chowdary/DataFlow/pull/139)). YAML dest
export writes a quoted sequence of mappings (this PR). MySQL twins were
not run; fixed-width dest export is still refused. YAML dest 100K was not
measured.

---

## 6b. Pre-handover sweep (2026-09-07, branch `devin/1788705057.72211-handover-gate-fixes`)

Driven by the question "are we good to hand over for 46+ connectors and every
feature". The answer recorded here is **no, not yet**, and this section is the
reason, not a summary.

Closed in this sweep, each with the measurement next to it in
`docs/OPEN_DEFECT_REGISTER.md` §5–§6: D21–D24 (MySQL control totals, the missing
G20 declaration path, an invisible proven control total, the Windows FIFO
decline), D25 (a signed proof pack failing the product's own verify control),
D26 (a hand-picked destination carrier lost on Map → Validate → Map), D27–D30
(declared carrier ignored by the multi-table fast path; `dest_count` read as a
digest; FIFO-or-spill; ODBC options passed to `pymssql`), D31 (dirty and
EU-locale numeric cells forced through the SQLite fast path instead of declining
to the row path) and D32 (`pk_join_count` refused every correct engine-side
keyed upsert at Gate-8).

A later browser pass in the same sweep reached the running application over CDP
and closed four more, each found by using the product rather than reading it:
**D36** (the approval inbox rendered another tenant's parked schedule and both
its connector ids, because the route read `workspace_id` as a query parameter
and ignored the header every client sends), **D37** (Promote / Replay offered on
findings with no row payload to rewrite, refusing every click into a toast that
faded), **D38** (editing a schedule's destination kept the Decision Artifact
stamp taken for the old one, so the cadence tick and the "Run now" offered as
recovery both refused forever, with no control anywhere that could clear it) and
**D39** (Verify chain reported 28 broken links and forks on an untampered store,
because the chain was linked and re-walked by timestamp alone and records
written in one tick sort arbitrarily). The same pass also completed the D25
closure: the pack failed verification again on the exact export → download →
re-upload path, because JSON has one number type and the signer hashed `100.0`
where the browser returned `100`.

What this sweep did **not** prove, and what a client must therefore be told:

1. **UI evidence is partial, and now says which half.** A second browser pass
   re-proved D25 (export → download → re-upload verifies; a one-byte mutation
   still fails all three checks), D38 (a destination edit drops the stale stamp,
   `Run now` completes, the cadence tick judges the current route, and
   rename-only and cadence-only edits preserve both hashes byte-identically),
   D39 (22/22 post-fix records carry a monotonic `chain_seq` and no finding
   lands on any of them) and D26 (the declared `VARCHAR(255)` survives the round
   trip and drives the DDL). Three things it could **not** prove: D37's *enabled*
   Replay path, because no route through the UI reaches a payload-bearing
   write-time rejection; D39's tie-break, because no two post-fix audit writes
   shared a timestamp; and D31 at this tip. Still untested: job cancellation, an
   operator-driven schedule, Operations / Contracts / Proofs, workspace roles and
   member removal.
2. **D40 is closed, and it closed D37's positive half with it** (PR
   [#171](https://github.com/Venky-Chowdary/DataFlow/pull/171), register §7).
   The dead end had two causes: Map graded carrier *domains* while the engine
   refuses on carrier *shape*, so it never asked for a contract on the routes
   the engine actually refuses; and G9's financial check then blocked Validate
   for a value the destination would reject even when the column carried a
   signed continue-policy contract. Map now classifies by shape and offers a
   per-row execution-policy selector with no hidden default; a rejection under
   a continue policy is a contracted holdout, not a block. Browser-proved on a
   live PG→PG route: `QUARANTINE_ROW` releases Map and Validate, the run
   completed with quarantine (2 appended, 1 held out), an independent psycopg2
   read measured `count(*) = 2` / `SUM(amount) = 30.50`, and Replay refused the
   unchanged payload and accepted the edited one (`count(*) = 3` /
   `SUM = 56.25`). Fail-closed is unchanged: no policy, `FAIL_JOB`,
   `STOP_TABLE`, `ABORT_TRANSACTION` and a tampered signature all still block.
   It opened two items: **D41**, a non-castable value written anyway on a
   SQLite destination with `strict` not failing (reproduces on merged base, so
   it predates D40), and **D42**, the destination-side DLQ write failing
   because the destination lacks the `_df_*` quarantine columns — quarantine
   evidence is control-plane only until that is decided.
   **Both are now closed on `devin/qa-lead-integration` (PR #172,
   `b3fec86b` → `59c37f01`; register §8 / §8a).** D41 was two COPY fast paths
   bypassing validation (SQLite identity `INSERT … SELECT`; CSV→SQLite ignoring
   `target_type`), fixed by one engine-side carrier census that declines the
   fast path before the destination exists. The same class existed on the file
   side — CSV → PostgreSQL/MySQL `COPY`/`LOAD DATA` aborted whole-load on one
   `not-a-number` with no quarantine — and is closed by
   `copy_fast_path.text_cell_copy_safe` censusing every cell before COPY. D42
   was not a missing-columns problem: `dlq_endpoint` inherited the
   destination's procedure / dest-DML `extra`, so DLQ rows went through the
   client's own INSERT; the clone now strips `DEST_PROCEDURE_EXTRA_KEYS`.
   Measured live on PostgreSQL (balanced quarantines 1 of 3 with a DLQ row,
   strict fails closed with 0 rows, clean population still COPYs); focused
   suites 32 passed, blast radius 189 passed / 2 pre-existing failures
   (`test_file_stream_path`, `test_file_stream_skip_matrix`, identical on
   `b3fec86b`). Both formerly unmeasured cells were then run live
   (register §8b): MySQL `LOAD DATA` behaves like PostgreSQL for
   `not-a-number`, and **integer range overflow inside a valid integer was a
   real defect** — `99999999999999999999` passed the lexical census and
   PostgreSQL `COPY` aborted on `out of range for type bigint`, while MySQL
   quarantined it but then failed Gate-8 because the fingerprint remap graded
   the held-out row against the unbounded logical `integer` stamp. Closed by
   passing the physical DDL + dialect into the census
   (`text_cell_copy_safe` → `fits_integer` / `fits_decimal`, the row path's
   own owners) and by `writer_common.physical_integer_carrier` so Gate-8
   holds out exactly what the writer quarantined. Live PG + MySQL, balanced
   and strict, both green with a DLQ row; bounded decimals
   (`0.016666668` into `NUMERIC(11,8)`) decline the same way. Focused suite
   65 passed; blast radius 503 passed / 4 failed / 1 skipped, all 4
   pre-existing (identical with the change stashed). Full backend
   suite on `d693555f`: 19977 passed / 152 failed / 1105 skipped / 1 error;
   the 19 failures in this neighbourhood fail identically on `b68e7c89`, and
   54 of the 152 are one harness class (`_seed_source` through a
   source-only rest_api/stripe connector). Class breakdown in register §8b.
3. **The Verify chain screen still reads `Chain verification failed — 36
   record(s)`** even though every finding is on a pre-fix record. The fix stops
   new ones; it cannot un-cross history without rewriting audit history. A
   client sees a red verdict until those records are checkpointed or the screen
   distinguishes legacy findings — that is a product decision, not a defect.
4. **The connector matrix is still incomplete.** Local engines are up (Postgres,
   MySQL, SQL Server, Mongo replica set, Redis, Elasticsearch, MinIO, Azurite,
   fake-GCS, BigQuery emulator, DynamoDB Local, Iceberg REST, Qdrant, Weaviate,
   Redpanda, ClickHouse, DuckDB, SQLite), and starting them converted silent
   skips into real attempts — which is why the failure count went up. Kafka
   cells now error on a missing `kafka-python` (D34) and Qdrant cells on a host
   the fixtures resolve differently (D35). Hosted Snowflake / BigQuery /
   Databricks, real S3 / GCS / ADLS, the SaaS connectors, SFTP and a real IdP
   remain unproven for want of credentials.
5. **Schedules have automated proof only** — 173 passed across the schedule
   suites (cadence, due tick, retry/backoff, overlap, missed windows, DST,
   workspace ownership, cancellation). No operator drove one through the UI in
   this sweep.
6. **The full suite is not green** and the failures are classified rather than
   hidden: see the register's "Full-suite state" note. The dominant category is
   SaaS connectors refusing a write by design or having no sandbox credentials.

## 7. Continuing this work

1. Read `docs/SESSION_HANDOVER.md` §1 for how to run the stack and the exact CI
   gate commands (ruff/mypy scopes CI actually enforces).
2. PRs [#133](https://github.com/Venky-Chowdary/DataFlow/pull/133)–[#139](https://github.com/Venky-Chowdary/DataFlow/pull/139)
   are merged into `feature/Venkat-Analysis`. Re-run track harnesses on this
   tree — a cell that passed on a track branch is not proof on the integration
   branch.
3. §4 defects from this sequence, N2–N5, yaml/fwf live + 100K, YAML dest
   export, DST, and certificate governance ops are closed. Next is the
   never-measured items in §2 / §6 (MySQL twins, Track A matrix, SFTP Excel,
   fleet). Fixed-width dest export is still refused.
