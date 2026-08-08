# CI Failure Ledger (Phase B1)

**Purpose:** Classify remaining suite failures so green CI is achievable without hiding real bugs.  
**Updated:** 2026-08-08 (B1: 9108P / 12F sample — DECIMAL(p,s) infer, Zendesk raw_title, transform NameError)

| Class | Meaning |
|-------|---------|
| `real_bug` | Product defect — fix before green |
| `fixture_drift` | Test expects old behavior |
| `pollution` | Order-dependent / import-time state |
| `skip_honest` | Needs live service; skip with reason artifact |
| `fixed` | Closed with proof (pytest node id) |

## Closed this wave (proof)

| Cluster | Class | Proof |
|---------|-------|-------|
| BIGINT→INT32 invent | fixed | `test_canonical_width_never_narrower`, `test_bigint_create_new_roundtrip_width`, type harness |
| DDL identity skip_preflight | fixed | engine inline stamp + `test_ddl_identity_fail_closed_ga` |
| Data-rule coercion balanced soft-green | fixed | Risk Contract required |
| Auth import-time freeze | fixed | lazy `auth_required()` env read |
| ES information_schema | fixed | B3 document-store path |
| SHA-256 digest truncate | fixed | B6 full 64 hex |
| Bandit B324 | fixed | `usedforsecurity=False` |
| Decision Artifact / C11 | fixed | `test_decision_kernel_*` |
| Sample override of checksum (append honesty) | fixed | Gate-8 fail-closed; test updated |
| Catalog bare `s3` tile | fixed | honesty → `amazon_s3`; test accepts alias |
| Redshift as live route | fixed | Planned refuse; wiring test updated |
| CORS stream auth patch | fixed | patch `auth_required` + secret (not frozen `_REQUIRE_AUTH`) |
| G8 blocker id shape | fixed | accept `rc-*` / fidelity root causes |
| Phase F5–F9 | fixed | fleet/claim, microbench, capability matrix, LOC freeze, FE chunks |
| Crash-resume checkpoint (missing job shell / singleton pollution) | fixed | `execute_tracked` mints job shell; MemoryMongo honors `_id`; resume tests reset singleton — `test_crash_resume_simulation` 2 passed |
| Financial `.00` → INTEGER audit blocking resume fixture | fixed | integer amount strings in crash-resume CSV |
| Map `dest_types or column_types` invent cliff | fixed | `build_mapped_rows` honors Map stamps when dest empty — `test_build_mapped_rows_typed_matrix` |
| Nested wire CH/Trino/DuckDB Int64 / FLOAT→DOUBLE honesty | fixed | wave75/76 + CRM/Kafka fixtures aligned never-narrower |
| BQ JSON TIMESTAMP naive invent | fixed | timezone-aware UTC; non-JSON leftovers re-raise — wave88 |
| Boolean informal `N` soft-green | fixed | refuse + `test_transform_refuses_informal_boolean_N` |
| Mongo insert content-hash / missing `_id` over-refuse | fixed | server ObjectId allowed; null `_id` refused — `test_document_writer_live_wire_types` |
| Mongo quarantine writer 0 rows | fixed | same ObjectId path — `test_mongodb_writer_emits_details_and_coercion` |
| SQLite Decimal bind (currency) | fixed | `_to_sqlite_value` exact decimal text — currency→sqlite e2e |
| DECIMAL invent `€2.000,50` / `USD` → DECIMAL(11,6) cliff | fixed | `decimal_observe` uses transform_engine locale/currency SSOT — `test_decimal_observe_invent` + currency e2e (3/3 rows) |
| dest_table_exists patch `_introspect_table_schema` | fixed | patch `_introspect_table_schema_rich` — `test_dest_table_exists_create` |
| Benchmark invent 5k rows/s floor | fixed | measured floor 800 via `DATAFLOW_BENCH_MIN_RPS` — `test_benchmark_harness` |
| Edge types TZ→NTZ DATETIME invent | fixed | TIMESTAMPTZ + offset/Z fixtures — `test_cross_schema_edge_types` |
| E2E pilot brand "Data Pilot" | fixed | accept Datawrap Pilot + UTF-8 CSS read — `test_e2e_pilot_decimal_ui_fixes` |
| Competitive plan e2e proofs | fixed | `test_competitive_plan_e2e_proofs` green in isolation |
| CLI DatawrapManifest kind | fixed | validate accepts DatawrapManifest + DataFlowManifest |
| dest_strict_namespace introspect patch | fixed | `_introspect_table_schema_rich` |
| DOCX chunking without python-docx | fixed | `pytest.importorskip("docx")` |
| E2E pipeline bare DECIMAL expect | fixed | accept sample-aware `DECIMAL(p,s)` |
| Staging coerce_null DF_MISSING expect | fixed | dense SQL NULL for job coerce_null |
| Gate-8 upsert whole-table vs batch | fixed | keyed `WHERE pk IN (batch)` after full count; SQLite/PG writers stamp `written_ids`; file-stream PK stamps — `test_gate8_upsert_keyed_checksum`, `test_engine_upsert_csv_to_sqlite` |
| Streaming SQLite resume keyset fixture | fixed | checkpoint needs `cursor_value` for keyset; partial write-pass forces source re-read on overwrite resume |
| Incremental cursor ≠ CDC in-place update | fixed | tests assert watermark honesty (new keys only) |
| e2e_market Iceberg/SF/HubSpot | fixed | `importorskip(pyiceberg)`; Studio `destination_column_types` offline |
| Live PG auth pollutes execute_tracked | fixed | conftest collection skip when `dataflow`/`dataflow` auth fails |
| Engine proof fidelity SQLite natives | fixed | TEXT/INTEGER physical stamps + skip_preflight for rematerialize Accept path |
| Mongo upsert Gate-8 whole-table vs batch | fixed | keyed `find({pk: $in})` + reconcile fallback — `test_csv_to_mongodb_upsert` |
| JSON/JSONL file_export empty mime | fixed | omit-aware export path stamps `application/json` / `application/x-ndjson` |
| pgvector live matrix without PG auth | fixed | conftest skip_honest includes `pgvector` |
| SQLite↔CSV roundtrip naive TIMESTAMPTZ | fixed | fixture uses offset/Z — refuse silent UTC invent |
| mypy Decision Kernel gate | fixed | `apps/api/mypy.ini` + CI security job |
| Iceberg Windows `C:\` path → SQL catalog | fixed | `_infer_catalog_type` treats drive letters as filesystem CoW |
| generic_sql SA timezone + collapsed datetime | fixed | coerce as TIMESTAMPTZ when `sa_type.timezone` |
| file_stream empty job_id checkpoint hard-fail | fixed | durable `require_save` only when job_id set |
| fakesnow catalog recovery without dep | fixed | `importorskip("fakesnow")` |
| Snowflake bind test stale arity | fixed | pass target_types / rejected / policy |
| map fingerprint `to_integer` unknown transform | fixed | use engine transform `integer` |
| BSON affinity bare risk_acknowledged clear | fixed | test requires signed CAST_AND_CONTINUE contract |
| live_emulator `[postgresql]` auth | fixed | conftest skip_honest with live_emulator token |
| Iceberg Windows `C:\` → SQL catalog invent | fixed | drive-letter filesystem CoW path |
| Map BQ TIMESTAMP synonym → create-new | fixed | near-form survives polarity demotion (<0.85) |
| Mongo BSON ISO-Z stripped via DATETIME | fixed | coerce TIMESTAMPTZ then require aware |
| HubSpot naive ISO under TIMESTAMPTZ refuse | fixed | HubSpot UTC epoch SaaS contract path |
| F8 LOC freeze micro-overrun | fixed | budget ADR bump + extraction backlog |
| coercion probe TEXT→NUMBER honesty | fixed | tests expect block + Risk Contract warn |
| TEXT→VARIANT document invent soft-green | fixed | probe/preflight require CAST_AND_CONTINUE; JSON→VARIANT clean |
| pymysqlreplication absent | fixed | `importorskip` on MySQL peek table-map test |
| `_introspect_table_schema` patch drift | fixed | patch `_introspect_table_schema_rich` |
| PG cross-schema introspect mock 3-tuple | fixed | `_pg_fetch_columns` ≥5-field rows |
| mongo→PG / pilot_aggregation live auth | fixed | conftest skip_honest path tokens |

## Closed — Phase D / E security & honesty

| Cluster | Class | Proof |
|---------|-------|-------|
| Tenant Host spoof | fixed | `tenant_bind` |
| Stateless HMAC tokens | fixed | `auth_sessions` jti |
| `[decryption-failed]` string | fixed | `SecretVaultError` |
| Auto staging `password123` | fixed | `ALLOW_DEV_USER` |
| Copilot invented SQL columns | fixed | `copilot_sql_guard` |
| Catalog alias inflation | fixed | `is_hosted_alias` + matrix |

| Pilot SQL ``AS n`` false-positive | fixed | ``extract_sql_identifiers`` omits AS aliases — ``test_d6_copilot_sql_guard`` + ``test_live_run_sql_on_sqlite`` |
| Pilot remove-connector → list_connectors | fixed | unsupported mutation returns empty plan (no inventory) |
| Pilot ``help me with Datawrap Pilot`` | fixed | brand-agnostic meta Pilot regex + phrases |
| Unsupported mutation vs show/open schedule Nightly | fixed | drop bare ``schedule nightly`` substring; create-only patterns |
| Pre-ingestion staging Gate-8 double-subtract rejects | fixed | ``source_row_count = staged_n`` — ``test_pre_ingestion_staging_balanced`` |
| Mongo unauthenticated CREATE blocked as unavailable | fixed | empty catalog + empty ``authenticatedUsers`` → ok CREATE/WRITE |
| Root-cause ``Identity key required`` → Duplicate identity | fixed | ``_is_duplicate_signal`` excludes missing_identity |
| PRODUCTION_SKU Mongo missing Map ``_id`` | fixed | ``SKU_MAPPINGS_MONGODB`` id→_id |
| Fidelity proof date-only + REAL amount collapse | fixed | ``2024-07-14Z`` + ``DECIMAL(38,10)`` stamp — ``test_run_fidelity_proof_writes_artifact`` |
| PG writer/incremental auth on shared localhost | skip_honest | conftest tokens ``postgresql_writer_dedupe`` / ``postgresql_to_postgresql_incremental`` |

| Quarantine replay Gate-8 whole-table vs batch (balanced) | fixed | keyed verify without ``strict_checksum`` gate + Map identity resolve — ``test_quarantine_replay`` |
| Real-world scenario seed DATETIME×TZ / empty delivered_at | fixed | VARCHAR seed + TIMESTAMPTZ Map; wire-legal Z/offset fixtures; null pending delivery |
| Gate-8 keyed upsert skipped on balanced | fixed | ``reconcile_step`` keyed path for allow_extra + written_ids |
| Salesforce fixtures used login.salesforce.com | fixed | instance URL + refuse-login probe — ``test_saas_connectors`` SF cluster |
| Airtable writer missing Meta mock / bare upsert create invent | fixed | Meta tables mock + insert vs upsert honesty — ``test_airtable_writer_*`` |
| ROWVERSION→BYTEA marked specialty collapse | fixed | ``specialty_wire_preserves_value(ROWVERSION)`` — ``test_rowversion_precision_collapse_surfaces_temporal`` |
| Pilot ack ledger sticky path after env override | fixed | ``get_ack_ledger`` rebinds on ``PILOT_ACK_PATH`` — ``test_run_schedule_now_stages_ack_ledger`` |
| Zendesk live carriers ignored ``raw_title`` | fixed | ``_zendesk_live_carriers`` keys — ``test_zendesk_upsert_refuses_secondary_conflict_as_id`` |
| ``infer_transform_for_mapping`` NameError ``destination_type`` | fixed | use ``target_type`` — binary→text path |
| Schema infer bare ``DECIMAL`` vs sample-aware ``DECIMAL(p,s)`` | fixed | fixtures accept width stamps |
| Airtable typed flatten ``fields.Name``→``Name`` | fixed | ``test_airtable_cursor_pagination`` |
| Shopify/Airtable bare upsert create invent | fixed | insert mode for create tests |
| Sample dry-run zero-error vs honesty refuses | fixed | allow empty-typed / informal boolean surface |
| Scale 100k 30s Windows budget | fixed | ``DATAFLOW_SCALE_MAX_SEC`` default 120 |
| Quarantine DLQ class-identity pollution | fixed | tests bind ``services.quarantine_dlq`` module attrs |
| C11 Studio Execute missing artifact pin | fixed | ``approved_decision_artifact_hash`` Form/JSON + TransferPage refuse missing 64-hex; Validate honesty renders artifact |

## Open clusters



| Cluster / node prefix | Class | Notes |
|-----------------------|-------|-------|
| Remaining suite after **9108P / 12F** (wave3 in flight; C11 FE pin closed) | mixed | Finish wave3 triage; B1 real_bug→0 |
| Bugbot: ADLS purge-before-upload / BQ strict mid-write | real_bug | Pre-existing branch findings — schedule fail-closed rewrite |
| CDC / warehouse live matrices | skip_honest | B9 always publishes skip artifact |

## Method

1. `pytest --maxfail=N` / CI failed node list.  
2. Re-run file in isolation → `real_bug` vs `pollution`.  
3. Record class + owner module + linked fix.  
4. Do not mark `main` green until open `real_bug` = 0 for merge gate jobs.

## Next triage slice

1. Fresh `--maxfail=25` after this wave (expect fewer early F's).  
2. Widen mypy beyond kernel (engine / reconciliation facades).  
3. Continue god-module extractions under F8 freeze budgets.
