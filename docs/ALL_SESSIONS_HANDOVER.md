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
- ~~SFTP daily Excel sync modes started, not finished.~~ **Closed.**
  SFTP `.xlsx` ingest reads the OOXML ZIP magic from the spill handle
  (cache is `.tmp`; openpyxl must not key format off that suffix). Dest
  `.xlsx` writes a real workbook, never `out.xlsx.csv`. Existing-table
  overwrite / append / upsert proved against the in-process SFTP server.
  Hashed trim recipe lands without padding (spill path now carries
  ``shape_runner``). Incremental append (id cursor) proved delta then
  noop. File-backed ``*/2 * * * *`` cron replayed the same recipe across
  two due beats (Mongo schedule store still unproven). 100K was not
  measured.
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

D25 (signed proof-pack verify) and D26 (Map dest-type override lost on a
round-trip) closed in register §6 — do not treat this paragraph as open.

The Gate-8 card on a *completed* Theater is now a settled surface
(`cursor/theater-complete-gate8-1673`, PR #184): `handleJobComplete` keeps
`activeJobId`, so Theater stays mounted and Gate-8 renders in place. Browser
2026-09-09: CSV 3 rows → Postgres `theater_g8_keep`, independent `psql`
`COUNT(*)=3`, Theater still mounted after >4s, result dashboard absent, toast
"Gate-8 proof stays on Job Theater.", verdict "Append delta verified —
whole-table checksums not comparable" (create-new append-delta honesty, not
full checksum). `apps/web` `npm test` 941 passed; `npm run build` clean.

Studio Schedule is no longer a lying CTA (`cursor/studio-schedule-beat-1673`,
PR #186): Theater / result footer offer Schedule only when
`canPersistStudioSchedule` is true (saved connector source + saved database
dest). File-source Theater (CSV 3 → `theater_g8_file_nosched`) has no Schedule
button. Saved PG→PG Theater (`theater_g8_keep` → `theater_g8_sched`) shows
Schedule and persisted pipeline `9e940e77-c633-4037-ad00-325a797f0f51`
(3 mappings). Live overwrite beat
`tests/test_studio_pg_schedule_beat.py` independent dest `COUNT(*)=3` on beat 1
and beat 2 (`skip_preflight` stays False).

A first Run now after create-new Validate parked on
`Decision Artifact content_hash mismatch` because Validate hashed `source_db`
as the saved connector UUID. Dest-exists hold now rematches that stamp;
Validate going forward hashes the source engine. Live parked stamp rematches
(`create_new_stamp_matches_schedule` True). Does **not** close DST / overlap /
retries / 100K / cancellation.

G19 hard-block reachability is closed on this tree
(`cursor/g19-reachability-1673`): Map no longer forces a Migration Risk
Contract against a live dest carrier that overwrite is about to drop. That
contract was the only Map exit, and it demoted G19 to a warning — so the
operator never saw the red gate. Overwrite recreate now lets Approve reach
Validate unsigned; G19 blocks; Execute stays locked; dest INTEGER is not
recreated as NUMERIC. Append / CRM overwrite still require the contract.
Measured 2026-09-09: 21 passed in
`test_g19_preflight_reachability.py` + `test_dest_schema_replacement.py` +
`test_studio_pg_g19_overwrite.py` (live dest `integer`, `COUNT(*)=1`);
`apps/web` 953 passed; `tsc` + vite clean. Does **not** close a full
browser walk of Transfer Studio, Track A 100K, or a contracted Execute.

Still unmeasured for a client: schedule retries/overlap/DST, cancellation,
quarantine and replay, the Evidence Chain / Operations / Contracts / Proofs
pages, workspace roles, and the Mongo and MinIO routes.

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
export writes a quoted sequence of mappings. Fixed-width dest export is
the inverse of ingest: ``#layout:`` plus right-padded records, overflow
refused (never silent truncate), empty population is still a layout
header so COUNT is a measured 0. Dest COUNT is ``iter_fixed_width_dicts``
on disk. MySQL twins were not run. YAML dest 100K and FWF dest 100K were
not measured. Layout is still required — widths are never guessed.

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
   **Append-contract wave (register §8c, same branch):** the equal-count
   "skip complete" was removed from ~45 row-addressed COPY fast paths (one owner,
   `copy_fast_path.skip_complete_identity_copy`, key-addressed dests only);
   `target_rows_before` is now measured on the stream/file path; MongoDB is
   key-addressed only with a mapped `_id`; Redis empty-prefix phantom schema
   fixed; SQLite text-boolean identity COPY declines to the row path. 580
   passed / 0 failed / 8 skipped on the 69-file changed-test selection with
   PG/MySQL/Mongo/Redis live. Source-only SaaS seeding (54) closed in register §8e
   (`a3dc9da9`, harness only). RI / `_Table.c` / vector Gate-8 suites pass
   on this box (28 passed / 1 skipped — MariaDB `:3306` down). D33 census
   is closed (register §6). D34/D35 stay environment.
   **CDC cursor wave (register §8d, `bfc565dd`):** the CDC cursor poll never
   advanced past page one (watermark reused as `cursor_after`, offset ignored
   by keyset readers) — every multi-page poll re-read the same rows (OOM at
   5 GB in the repro). Fixed at the owner: `_read_keyset_pages` seeks from the
   page maximum `(cursor, pk)`, readers accept a cursor-only first bookmark,
   non-advancing pages fail closed. Writer per-batch counts no longer pose as
   the run's source population; the run stamps its reader count once.
   513 passed / 26 skipped on the 56-file CDC/keyset neighbourhood.
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

## Live PG/MySQL/MariaDB + SQLite/S3 neighbourhood (2026-08-10, `devin/qa-lead-integration`)

Closed at the owner and recorded in `docs/OPEN_DEFECT_REGISTER.md` §8f: shared
upsert `KeyCensus` (`merge_staging_into_dest`), tombstone upserts declined to the
row path, SQLite fast-path snapshot guarantee metadata, SQLite COPY shard in the
shared `_dataflow_write_ledger` with same-job retry skip, and a **source-table
destruction** defect (post-rollback unqualified `DROP TABLE` resolved to the
attached source) fixed by `main.`-qualifying the dest and rollback-only cleanup.
Neighbourhood run: 1231 passed / 43 skipped. Not claimed: live MariaDB/PG runs
of these paths in this exact commit (see §8f for the focused live counts).

## Schema evolution vs COPY + Gate-8 engine digests (2026-08-10, `devin/qa-lead-integration`)

Register §8g. An occupied destination under `backfill_new_fields` now stays on
the writer path (COPY bypassed ADD COLUMN / widen); PG and MySQL writers defer
the strict pre-scan verdict until the live carriers are final (fail-closed if
setup changed nothing, rescan if it widened); Gate-8 no longer demands a stashed
sample when a whole-population engine digest pair is already in hand. Live PG and
MySQL backfill/widen tests and the PG control-total test pass; blast radius
338 passed / 2 pre-existing failures (Informix merge stage bind, vector
read_target_sample route) which are next.

## Scheduler proof: all engines × all sync modes (2026-08-10, `devin/qa-lead-integration`, PR #172)

Register §8h–§8i. Harness `apps/api/scripts/live_schedule_matrix.py` drives the
real path (`create_schedule → _run_due_schedules → _dispatch_transfer →
run_transfer_async → _finalize_run`) against local PG/MySQL/SQLite, mutates the
source between beats and reads the destination back independently. 2K rows/cell:
**pass=63 fail=0 skip=3** (`/home/ubuntu/sched_proof/matrix_2k_fix4.json`; skips =
SQLite has no log-based CDC source). Closed on the way: schedule cursor contract,
watermark stamped only after Gate-8, SCD2 close-on-vanish, mirror/deduped count
tokens graded as digests, SQLite expression-depth on 2K-key predicates (SCD2,
mirror, CDC LSN lookup), SQLite typed read-back checksum, SQLite reader/writer
lock (WAL), PG slot-create fallback inside an aborted transaction masking the real
refusal. D-CDC-SLOT-LIFECYCLE closed: `DELETE /schedules/{id}` releases the PG
slot + publication through `services/cdc_capture_release.py` (route-shared and
active slots are kept; unreachable source returns the exact DROP as next action)
and the multi-table shared-reader key no longer embeds the job id (was one leaked
slot + re-snapshot per beat). CDC-only matrix re-run pass=9 fail=0 skip=3
(`matrix_cdc_release.json`) with the slot asserted absent after release.
100K/cell (register §8j): the first PG/MySQL × 7-mode run at 100K found one real
engine defect — incremental_append seeking `WHERE cursor > page_max` on a cursor
with no unique tie-break skipped every row tied at a page edge (27,500 of 107,500
landed). Fixed at the owner (`338266c9`): `cursor_unique_evidence` +
`incremental_read_needs_filtered_scan` refuse the seek and the SQL readers page
one held snapshot bound to the run watermark. 100K incremental_append re-run
PG/MySQL/SQLite × PG/MySQL/SQLite **pass=12 fail=0 skip=0**
(`matrix_100k_incappend.json`); full 100K PG/MySQL × PG/MySQL × 7 modes +
overlap/failure-park/workspace cells **pass=31 fail=0 skip=0**
(`matrix_100k_pg_mysql_v2.json`): run 1 = 100,000 rows and run 2 = the exact
delta in every cell, Gate-8 passed on all 56 scheduled runs.
SQLite-source 100K slice `matrix_100k_sqlite_src.json` **pass=18 fail=0 skip=3**
(SQLite has no log-based CDC source). SQLite-destination 100K slice first ran
pass=16 fail=1: PG→SQLite mirror lost all 2,500 updated keys under a green upsert
ack (register §8k) — the SQLite snapshot scan was ordered by `rowid` while the
engine's keyset seek continued on the `BIGINT PRIMARY KEY`, so the first-page
handoff skipped every key the heap had placed after the page edge. Fixed at the
shared owner: readers publish the ORDER BY they opened with
(`sql_snapshot_scan.publish_scan_order`), `stream.py` only seeks when the keyset
columns are a leading prefix of that order (`scan_order_supports_seek`) and
otherwise keeps paging the held snapshot; the SQLite scan itself now orders by
the declared PK. 20 regression tests in `tests/test_snapshot_scan_keyset_handoff.py`;
re-run `matrix_100k_sqlite_dest.json` **pass=17 fail=0 skip=0**.
MongoDB-source 100K slice `matrix_100k_mongo_src.json` (21 cells) first ran
**pass=12 fail=9 skip=0** — three engine defect classes, each on all three SQL
destinations (register §8l): (1) incremental_append landed 20,000 of 100,000
with Gate-8 green — the Mongo cursor read had no tie-break on a non-unique
cursor and preflight/population-fit derived the tie-break separately from the
stream; one owner now (`keyset_pagination.incremental_tiebreak_column`, contract
PK else Mongo `_id`) feeds execution, preflight scope and the fit scan, and the
Mongo reader seeks on the composite `(cursor, _id)`. (2) SCD2 failed closed at
write with `amount → DECIMAL(3,3)`: the SCD2 CREATE rebuilt types from the
100-document peek instead of the Map/population-widened stamp preflight had
proved against; `apply_scd2` now binds `dest_types`. (3) CDC run 2 applied
1,000 of 7,500 events: `poll()` never advanced the instance's own resume token
so every drain round replayed the first window; fixed with snapshot→stream
handoff on the same instance, and the business-key-delete pre-image refusal
moved to attach time (unreadable catalog ≠ disabled). Regression tests in
`test_incremental_filtered_scan_no_tiebreak.py`, `test_scd2_engine.py`,
`test_mongodb_change_stream.py`; live 2K probes green (`probe_mongo_incappend`,
`probe_mongo_scd2`, `probe_mongo_cdc_2k` 5/0/0). 100K re-run of the three
modes: `matrix_100k_mongo_src_fixed.json` **pass=12 fail=0 skip=0** (run 1 =
100,000, run 2 = 7,500 delta, Gate-8 on all 18 runs) — MongoDB-source 21/21 at 100K. The
MongoDB-destination 100K slice `matrix_100k_mongo_dest.json` 5/12/7 was a
harness collision (two matrices sharing the connector store with identical
connector names; `create_connector` replaces same-named connectors) — re-run
alone: `matrix_100k_mongo_dest_v2.json` **pass=17 fail=0 skip=7** (PG/MySQL/SQLite
→ MongoDB × overwrite/append/incremental_append/incremental_deduped/cdc all green,
Gate-8 on all 34 runs; the 7 skips are by-design capability refusals — SCD2/mirror
need a SQL table destination, SQLite has no log-CDC source). 100K scheduler totals:
PG/MySQL duplex 31/0/0 · SQLite-src 18/0/3 · SQLite-dest 17/0/0 · Mongo-src 21/0/0 ·
Mongo-dest 17/0/7 — **0 failures on any measured cell**.
**Open:** hosted clouds remain unmeasured; CDC is at-least-once as measured.
Fixed on the way: `create_connector` now keeps the existing id on a same-named
create (new config via `update_connector`) instead of delete + fresh id, so
schedules bound to the connector are no longer orphaned (register §8l).

**Transfer Studio SQL/procedure paste UX** (`5e6c95cf`, `98ec7f84`): a pasted
`CREATE PROCEDURE/FUNCTION/TABLE/VIEW` is diagnosed before the one-statement
check by both owners (`services.procedure_source.definition_pasted_refusal`,
web `sqlEditorModel.diagnoseSql`). A SQL Server T-SQL script pasted against
a non-T-SQL engine (Snowflake) is named as such (≥2 markers: `@param` types,
`GO`, `dbo.`, `SET NOCOUNT ON`, `BEGIN TRY`, `RAISERROR`) with one next
action — one read-only SELECT/WITH, or `CALL schema.name(:param)` for a
procedure that already exists in that engine — instead of "remove extra
semicolons". Tests: `test_procedure_source.py`, `sqlEditorModel.test.ts`.

## Cloud targets on local emulators (2026-08-10, `devin/qa-lead-integration`, PR #172)

No hosted credentials, so the same scheduler harness ran against local
cloud-compatible services: BigQuery = `goccy/bigquery-emulator`
(`127.0.0.1:9050`, project `dataflow-test`), Snowflake = `fakesnow`
(account `local`), Redshift = PostgreSQL `:5439`. **Emulator-measured only —
not a hosted-cloud certificate.** Full detail: `docs/OPEN_DEFECT_REGISTER.md` §8m.

Consolidated 2K matrix `cloud_emulators_2k_final.json`: **pass=11 fail=3 skip=7**.

* fakesnow: all 7 sync modes pass (CDC at-least-once).
* BigQuery: overwrite / append / incremental_append / incremental_deduped pass;
  scd2, mirror, cdc fail closed on reproduced emulator limitations (typed
  NUMERIC parameters decoded as STRING; `ALTER TABLE ADD COLUMN` never
  materialises; MERGE rewritten to internal `googlesqlite_*` 500). No
  workaround was added that would hide a real type mismatch on hosted BigQuery.
* Redshift: 7 skips — capability registry says `redshift is Planned`; PG wire
  compatibility does not promote it.

Product defects fixed on the way (all regress green on PG/MySQL/SQLite/fakesnow
— `regress_scd2_mirror_2k.json` 11/0/0, `regress_scd2_mirror_sqlite_sf_500.json` 4/0/0,
61 SCD2/mirror unit tests):

1. `connectors/generic_sql.py::_warehouse_creator` — SQLAlchemy engines for
   Snowflake/BigQuery take their DBAPI connection from the native connector
   owner (`snowflake_conn.get_connection`, `bigquery_conn.get_client`); the
   `bigquery+dataflow` dialect stops `sqlalchemy-bigquery` from building an ADC
   client of its own.
2. `connectors/lsn_guards.py` — PostgreSQL LSN comparison on Snowflake is padded
   lexicographic hex (fakesnow/DuckDB lack the `'XXXX'` number format).
3. `services/target_sample.py` — BigQuery keyed read-back queries all rows with
   typed parameters (`CAST(key AS STRING) IN (?)`) instead of a first-page scan
   that missed a key's later version.
4. `services/scd2_engine.py::key_column_types` / `typed_key_bind` — SCD2 key
   predicates bind in the destination's physical type (INT64 vs text) for
   snapshot fetch, expire and close-on-vanish; SCD2 map-finish reuses the SQL
   writers' temporal/numeric bind owners.
5. `services/mirror_engine.py::apply_inferred_deletes_via_staging` — correlated
   `EXISTS` uses `df_stg` alias + bare target table name (`target_table=` from
   both callers) instead of `dataset.table.col`.
6. `connectors/bigquery_conn.py::_EmulatorClient.query_and_wait` — bounded 20 s
   retry so emulator 500s fail closed instead of hanging (emulator client only).

Still unmeasured: hosted BigQuery/Snowflake/Redshift, Databricks, Salesforce.

## Transforms × Scheduler (2026-08-10, `devin/qa-lead-integration`, PR #172)

Scheduler = when/how rows move; Transforms = dbt-style post-load SQL models
(`ref()`/`source()`, view/table/incremental-merge, data tests, quarantine) that
auto-run after any transfer — including every scheduled beat — landing a
trigger table, with the outcome on the job (`destination_summary.transformations`).
Not redundant; it is the "T" Airbyte delegates to dbt.

Proof: `scripts/live_schedule_matrix.py::run_transform_cell` — PG → PG/MySQL/SQLite,
two scheduled beats, rollup table model and incremental-merge model read back
equal to the landed table (no duplicate keys after beat 2), a deliberately
failing data test surfaced as `partial`. `transform_sched_2k.json` **6/0/0**.
Detail: register §8n.

## Menu readiness sweep — status at handover (2026-08-10, `devin/qa-lead-integration`, PR #172, head `bb13fd55`)

Question asked: "are we at the Google/Microsoft handover standard?" Honest answer:
**controlled handover on the measured routes only.**

**Fixed this wave (all pushed):**
1. MySQL/MariaDB `DEFAULT CURRENT_TIMESTAMP` on fractional `DATETIME(n)` (error 1067
   after a green Validate) — `e0f2cca6`, live PG→MySQL regression.
2. Fabricated quarantine count on a refused write unit with no findings
   ("1,000 quarantined / 0 findings") — `bb13fd55`, 21 accounting tests.
3. Typed-database decimals (`1.337` for `numeric(12,3)`) flagged invalid in
   Validate cell preview — `e0f2cca6`, preview reads on the wire like Execute.
4. Contracts page HTTP 500 (`Decimal128`) — closed earlier on this branch.
5. Schedule Run-now/Activate "no persisted column mappings" after Validate —
   `bb13fd55`, Execute persists the contract onto the seeded schedule; replay
   PATCH keeps the operator's cadence.
Earlier on the same branch: CSV→BigQuery/DynamoDB upsert Gate-8 census (`46dd9744`),
connector same-name replacement, Snowflake T-SQL paste UX, all scheduler/CDC/Mongo
items in register §8h–§8n.

**Left open (do not claim):**
- P1-5 is **by-design** (org profile vs Team workspace name) — both are labeled on Settings → General; do not merge them.
- Browser re-run of the exact PG→MySQL scheduled deduped route on this head
  (Run now → second beat → history) — not yet done.
- Sweep coverage gaps: Connectors CRUD, Contracts end-to-end, Jobs Retry/Replay,
  schedule pause/resume/history, Transforms incremental + data test, CSV→PG
  bad-row quarantine, workspace switching.
- Full backend suite on this head not re-counted (last measured 19977/152 on
  `d693555f`; many classes since fixed). Blast radius on the quarantine
  change (40 quarantine/DLQ/accounting/conservation files, live PG/MySQL):
  470 passed / 0 failed / 8 skipped on `fb46e18d`.
- Hosted clouds: emulator-measured only (register §8m); CDC at-least-once.

Follow-up on `cursor/qa-lead-followup-1673`: closed sweep P1-3, P1-4, P2-1..P2-5
(register §8o). Overview DLQ count is whole-queue; Studio source chrome uses one
label; signed-in `#/help` stays in-app Docs; Pilot citations open the cited
article (not `#/docs` walkthrough, not marketing); Query tabs are workspace-scoped;
Pilot briefing uses `count_jobs` + request workspace.

## 7. Continuing this work

1. Read `docs/SESSION_HANDOVER.md` §1 for how to run the stack and the exact CI
   gate commands (ruff/mypy scopes CI actually enforces).
2. PRs [#133](https://github.com/Venky-Chowdary/DataFlow/pull/133)–[#139](https://github.com/Venky-Chowdary/DataFlow/pull/139)
   are merged into `feature/Venkat-Analysis`. Re-run track harnesses on this
   tree — a cell that passed on a track branch is not proof on the integration
   branch.
3. §4 defects from this sequence, N2–N5, yaml/fwf live + 100K, YAML dest
   export, DST, and certificate governance ops are closed. **D33** (keyed
   upsert insert/update/delete census) is closed on this tree — see
   `docs/OPEN_DEFECT_REGISTER.md` §6. Next is the never-measured items in
   §2 / §6 (MySQL twins, Track A 100K, fleet). D34/D35 stay environment.
   SFTP daily Excel ingest/dest is closed (spill-handle load + real
   `.xlsx` dest; existing-table overwrite / append / upsert; hashed trim;
   incremental append; file-backed 2-minute cron replay). Fixed-width dest
   export is closed (declared layout or CHAR(n)/VARCHAR(n); overflow refuses).
   Mongo schedule store, 100K SFTP Excel, and 100K FWF dest remain unmeasured.
