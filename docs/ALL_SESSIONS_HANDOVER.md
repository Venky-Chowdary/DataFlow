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

## 4. Open defects — state as of 2026-08-30

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

**Still open:**

- **D1 — a schemaless destination's shape is inferred from a value sample and
  then compared as a declared target type.** Reproduced live on 2026-08-30
  against the compose MinIO: a Postgres `amount decimal(12,2)` lands correctly
  and the product's own destination introspection reads it back as
  `DECIMAL(2,2)`. Document stores (`mongodb`, `dynamodb`) are already exempt via
  `dest_decimal_single_capacity_digits`; object stores, SFTP, Redis and
  Elasticsearch are not. The fix must carry *provenance* (sampled vs declared)
  out of the probe rather than weaken any comparison — an operator-declared
  narrowing on an object store is still enforced at the write, so suppressing
  the verdict would fail open. See `docs/OPEN_DEFECT_REGISTER.md` §1 D1.
- Scheduler DST + workspace-ownership cells not re-measured after the access fix.
- Governance ops (mask/hash/redact) not yet recorded in the audit certificate.
- The connector-family matrix never completed (Track A halted at 122 of 225).
- SFTP daily Excel sync modes started, not finished.
- SAML/SSO round-trip — needs a real IdP, unprovable here.

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
N4 (gate G20, population code-crosswalk coverage) is this PR, not yet merged;
N2 is independently on [#133](https://github.com/Venky-Chowdary/DataFlow/pull/133);
D1 is independently on [#132](https://github.com/Venky-Chowdary/DataFlow/pull/132);
N5 is not started. Next after N4 merges: N5. Do not fold D1 or N2 into N4.

---

## 7. Continuing this work

1. Read `docs/SESSION_HANDOVER.md` §1 for how to run the stack and the exact CI
   gate commands (ruff/mypy scopes CI actually enforces).
2. Merge the track PRs into `feature/Venkat-Analysis` bottom-up, checking for a
   second owner of a concern — the tracks touch shared modules
   (`services/type_system.py`, `services/row_conservation.py`,
   `src/transfer/stream.py`). One canonical owner per concern; a duplicated helper
   is a defect, not a merge artifact.
3. Re-run the track harnesses on the merged tree — a cell that passed on a track
   branch is not proof on the integration branch.
4. Close §4, then re-run; only then extend §6.
