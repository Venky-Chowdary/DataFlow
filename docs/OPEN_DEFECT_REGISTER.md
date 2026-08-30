# Open-defect register — consolidated across the six-session wave

Every item the wave left open, in one list, taken from the sessions' own
evidence documents after all track branches merged into `feature/Venkat-Analysis`:
`ALL_SESSIONS_HANDOVER.md` §4–6, `SCALE_MATRIX_SQL.md` (A), `SCALE_MATRIX_FILES.md`
(B), `SCALE_MATRIX_NOSQL.md` (C), `SCALE_MATRIX_MODES_SCHEDULES.md` (D),
`CONNECTOR_READINESS_MATRIX.md` (E).

Three columns are kept apart on purpose, because collapsing them is how a
product gets called ready when it is not:

* **defect** — the product is wrong, and the owner module is named.
* **not measured** — the product may be right; nobody proved it.
* **environment** — cannot be proven on this box (no credentials, no service).

A defect is closed here only when a *real* transfer on a live engine proves it,
with the destination read back on a connection the transfer engine never
touched. A passing unit test is not a closure.

---

## 1. Defects — ordered by blast radius

| # | Defect | Owner module | From | Evidence of the failure |
|---|--------|--------------|------|--------------------------|
| D1 | A schemaless destination's shape is *inferred from a value sample* and then compared as a declared target type, so run 2 of the same route refuses what run 1 wrote: `amount DECIMAL(12,2)` reads back `DECIMAL(2,2)` from S3 and `text` from Elasticsearch, firing `Lossy / fidelity collapse`, `DDL identity mismatch`, and `Mapping confidence below floor`. | destination introspection + shape contract (`services/schema_introspect.py`, `services/decision_kernel/`) | E (Family A), C (D5) | 9 sync-mode cells fail on `postgresql→{redis,s3,elasticsearch}`; run 1 wrote 10,000 rows and verified independently |
| D2 | SQL Server destination invention stamps `VARCHAR` for a Unicode-capable source column, then correctly refuses `U+8A9E`. The refusal is honest; the type choice is the defect — a Unicode source column must land `NVARCHAR`. | `services/type_system.py` (invention) | A | 9 cells, 160/200 rows quarantined |
| D3 | `* → sqlite` treats `UUID → TEXT` / `CHAR(36) → TEXT` as a fidelity collapse. SQLite TEXT is *wider*, not narrower; this needs a carrier-equivalence rule, not a fail-closed gate. | fidelity gate + `services/type_system.py` | A | 18 cells |
| D4 | Invented MySQL DDL emits a `CHARACTER SET … COLLATE` clause the server rejects (`1064`). | MySQL create-new DDL path | A | 6 cells, `sqlserver/oracle → mysql` |
| D5 | Cross-engine collation mapping picks a weaker collation/charset than the source (`UTF8MB4_0900_BIN → SQL_LATIN1_GENERAL_CP1_CI_AS`), and same-engine invention does not carry the source collation, so the gate blocks the product's own output. | `services/collation_carry.py` + invention | A | 12 + 3 cells |
| ~~D6~~ | **Closed.** PostgreSQL destination key census failed with `operator does not exist: text = integer` — the census bound source-spelled key literals against a typed destination column. The census now reflects the destination's declared key types (DB-API catalog and SQLAlchemy paths) and compares in the *destination's* domain, keeping the comparison index-usable; a key the column cannot hold (`'abc'`, `22.4` into `integer`) is a proven miss, not an error. | `services/dest_key_typing.py` (new canonical owner) + `services/dest_precount.py` | C | live repro on the compose Postgres/MySQL, then 28 regression cases incl. 4 live ones in `tests/test_dest_key_census_typing.py` |
| D7 | Elasticsearch destination writes `DECIMAL` as a JSON string, so ES dynamic-maps the field `text`; no explicit index mapping is created. | ES writer | E | handover §4 |
| ~~D8~~ | **Closed.** The bare token `long` was read as Oracle's deprecated text LOB, so an ordinary INT64 copy (`mongodb`/`spark`/`iceberg`/`elasticsearch` `long → BIGINT`) was flagged as an invented numeric domain and stopped for an approval it never needed. The meaning now belongs to the *source engine*: Oracle's `LONG` stays a text LOB (and lands the destination's own text carrier, not `BIGINT`), every other engine's `long` is INT64 and converts as equivalent, and an **unknown** source gets neither guess — it keeps the conservative refusal and the historical carrier. | `services/type_system.py` (`source_long_is_int64` / `source_long_is_text_lob`) + `services/decision_kernel/type_invent.py` | E | 43 cases in `tests/test_long_token_source_identity.py` over conversion class *and* invented DDL; the 14 pre-existing failures in §1 D17 reproduce identically on the base commit, so nothing regressed |
| D9 | The 500-row reconcile cap still exists in the hubspot / salesforce / airtable / kafka verifiers: a strict reconcile hashes 500 hits against a whole-source digest. | those connector verifiers | E | handover §4 (hosted; fixable in code, unprovable here) |
| D10 | `postgresql → excel` export: destination checksum differs from the source population (every column read back as `str`, mixed `NULL` / `\N` null spellings). | Excel export writer | B | `postgres_to_excel` cell, 100K rows |
| D11 | `postgresql → avro` export cannot be read back at all: `ValueError: read length must be non-negative or -1`. | Avro export writer | B | `postgres_to_avro` cell |
| D12 | `csv_ragged → postgresql` is reported **pass** with destination rows = 0 and no checksum — a refusal graded as a pass. | file matrix grading + ragged-row refusal path | B | `csv_ragged_to_postgres` cell |
| D13 | `mysql→mysql` CDC snapshot lock wait timeout, never root-caused (minimal locking landed for a *different* symptom). | `connectors/mysql_change_stream.py` | D | handover §4 |
| D14 | Mongo contract persistence logged `InvalidDocument: cannot encode object: Decimal('1')` and fell back silently. | `services/contract_store.py` + `value_serializer` | C | now believed fixed by the merge (canonical `bson_safe_document`); needs a live re-proof |
| D15 | Two repo tests fail on the merged base, independent of the tracks: `test_writer_common_integer_fit::test_quarantine_holds_out_a_fractional_cell_with_the_reason` (expects `DOUBLE`, gets `DECIMAL(8,6)`) and `test_writer_common_resilience::test_quarantine_policy_holds_out_bad_rows`. | writer quarantine carrier choice | base | reproduced on a clean base worktree |
| D17 | 14 typed-transfer tests fail on the merged base, before any of this session's work (identical set reproduced on a clean `bc654de3` worktree): `test_execute_tracked_*` type-preservation for `csv/tsv/json/jsonl/parquet → duckdb`, `duckdb → duckdb`, `duckdb → postgresql`, `json/jsonl → postgresql`, edge-types `csv → postgresql`, `test_gate8_bind_fingerprint_and_saas_typed::test_saas_catalog_stays_planned_not_sku`, and two cells of `test_typed_fidelity_transfer_matrix_e2e` (`mongodb → postgresql` refused by *Mapping confidence below floor*, `postgresql → existing mysql timestamp column`). | typed execute path + Map confidence floor (owners not yet isolated) | base | full run: 14 failed / 5,955 passed on the working tree and 14 failed / 5,938 passed on the base commit |
| D16 | Baseline CI mypy: 4 errors (`services/type_system.py`, `decision_kernel/findings.py`, `decision_kernel/execute_gate.py`); ES/mapper anomaly where the engine's own stamping input penalises `id → id` to 63% although the mapper returns 0.99 in isolation. | as named | index, E | handover §4 |

## 2. Not measured (product may be correct; no proof exists)

1. 100K on every route; 1M on every sync mode (1M measured on exactly one route:
   PostgreSQL→MySQL append, 221.5 s / 4,515 rows/s).
2. Track A's 225-cell grid never completed — the re-run halted at 122 cells.
3. `postgresql→mongodb` CDC at 100K: timestamp risk-acknowledgement not re-measured.
4. Scheduler DST cell and workspace-ownership cell not re-measured after the
   `workspace_access` fix (the harness expectation also changed).
5. `mongodb→mysql` CDC idle re-run; the 100K crash-injection pass.
6. BigQuery-emulator cells beyond the two-run identity probe.
7. Full-fleet NoSQL sweep on the fixed revision (last complete sweep: 73 pass /
   24 fail / 24 skip at 200 rows, on a *pre-fix* revision).
8. `fixed_width` and `yaml` as live file drivers (currently skip: "not yet live").
9. Real SMTP / Slack / Teams delivery; MCP from a real client; chatbot/RAG
   against a live key; real host routing per client domain; SSO/IdP; KMS/BYOK.
10. CDC is **at-least-once** everywhere except the named crash-injection routes.

## 3. Environment (cannot be proven on this box)

AWS S3, real GCS, real ADLS, hosted BigQuery, Snowflake, Redshift, Databricks,
Salesforce / HubSpot / Airtable / Kafka (no credentials); Iceberg (no REST
catalog); Elasticsearch security API absent from the local image, so the
connector's privilege preflight fails closed; ClickHouse capability is `Planned`,
so the product refuses it by design.

---

## 4. The fleet a closure is proved against

`python scripts/local_engines.py` starts the engines `docker-compose.yml`
declares (Postgres, MySQL with binlog + CDC grants, Mongo as a replica set,
Redis; `--with-search` adds Elasticsearch) and prints the exact environment the
live suites read. That last half matters: live suites resolve credentials
through `apps/api/tests/helpers/live_env.py`, which reads `PGHOST`/`PGUSER`/…
and `MYSQL_*` before a default that does **not** match this repository's compose
file — so a healthy fleet still produced dozens of silent `skip`s. Export what
the script prints, or a live matrix will grade itself green while proving
nothing. `--check` exits non-zero when a declared port is unreachable.

## 5. Closure protocol

For each defect: reproduce on a live engine → fix in the one canonical owner →
re-run the failing cell(s) → read the destination back independently → record
the measured numbers next to the item. Items in §2 are closed by measurement
only; items in §3 stay open with their reason and are never counted as green.
