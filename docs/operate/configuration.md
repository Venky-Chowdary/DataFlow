# Configuration reference

Datawrap is configured with environment variables. This page lists every
variable the runtime code reads. The table is generated from the code, so it
cannot drift: `apps/api/tests/test_docs_config_reference.py` fails when the
code reads a variable that is not listed here.

To regenerate after a code change, run from the repository root:

```
python docs/tools/gen_config_reference.py
```

## The prefix rule

Product flags have two accepted prefixes:

- `DATAWRAP_<NAME>` is the current name and is checked first.
- `DATAFLOW_<NAME>` is the earlier name and is used when the first is unset.

The helper that does this is `getenv_brand` in `apps/api/services/brand_env.py`.
An unset variable stays unset; the helper never invents a default.

- An empty or blank `DATAWRAP_<NAME>` counts as unset when `DATAFLOW_<NAME>`
  has a value, so a blank line left in an environment block cannot switch off
  a setting the earlier name turns on.
- When both names are set to different non-empty values, `DATAWRAP_<NAME>`
  wins and the API logs one warning per flag at start, naming the flag and
  never its values. Remove the one you do not mean.

Two exceptions matter in practice:

1. A few flags are read directly under one exact name, without the helper. For
   those, only the name shown works. They have their own table below. Setting
   `DATAWRAP_EXPOSE_ERROR_DETAIL=1`, for example, has no effect, because the
   code reads `DATAFLOW_EXPOSE_ERROR_DETAIL`.
2. A few flags are read through the helper in one place and under an exact name
   in another. Their row says "Some readers accept only ...". Set that exact
   name to reach every reader.

The command-line tool in `apps/cli` reads `API_BASE` and `API_TOKEN` under
both prefixes too (`DATAWRAP_` first) through its own helper. Its names are
built at run time, so they appear under "Reads the generator cannot name".
Unlike `getenv_brand`, that helper takes an empty `DATAWRAP_` value as set.

Process entry points also write some flags themselves. `src/worker_main.py` and
`src/scheduler_main.py` set `PROCESS_ROLE`, `WORKER_FLEET`, `SCHEDULE_LOOP` and
`API_CLAIM_LOOP` at start, under both prefixes, so those four do not need to be
set for a worker or scheduler started through its own command, and a value left
in a shared environment block under either name does not override them.

The API's graceful-shutdown bound is read by `scripts/start-api.sh`, not by
Python, so it is not in the table: `DATAWRAP_GRACEFUL_SHUTDOWN_SECONDS` (or
`DATAFLOW_GRACEFUL_SHUTDOWN_SECONDS`), default `10`, a whole number of
seconds. On SIGTERM uvicorn waits at most that long for open connections
before the API stops its background work. A value that is not a whole number
stops the script with exit code 64. Keep it below your orchestrator's
termination grace period.

## What production requires

When the process decides it is in production (see `ENV`, `ASSUME_PRODUCTION`
and `REQUIRE_AUTH` in the table), it checks its configuration at start. If
anything below is missing, the API keeps answering `/health` and refuses every
other route. The missing settings are written to the process log.

- `AUTH_SECRET` set, at least 32 characters.
- `REQUIRE_AUTH=1`.
- A login path: `ADMIN_EMAIL` with `ADMIN_PASSWORD`, or `AUTH_USERS`.
- `ALLOW_DEV_USER` not enabled.
- `SECRETS_KEY` set.
- `OTP_PEPPER` set, only when `SIGNUP_ENABLED` is on.
- A MongoDB connection string that is not localhost.

Source: `validate_production_config()` in `apps/api/services/platform_config.py`.

## How to read the tables

- **Default** is the value the code uses when the variable is unset. `unset`
  means the code treats the variable as absent. `computed` means the default is
  worked out at run time. `required` means the code reads the variable without a
  fallback. When two readers use different defaults, both are shown.
- **What it does** is a checked description where one exists. Otherwise it names
  the function that reads the flag and quotes the first line of that function's
  docstring, so nothing in the column is invented.
- **Read in** lists up to two files; "+N more" means there are others.
- Defaults of credential-shaped variables are never printed.

The scan covers the runtime Python under `apps/api/src`, `apps/api/services`,
`apps/api/connectors`, `apps/cli` and `packages/*/src`.

## All flags

<!-- BEGIN GENERATED: config-flags (docs/tools/gen_config_reference.py) -->

551 flags: 465 accept both prefixes, 10 are read under one exact product-prefixed name only, 76 are unprefixed.

### Flags that accept both prefixes

Set `DATAWRAP_<flag>` (preferred) or `DATAFLOW_<flag>`. The table shows the part after the prefix.

| Flag | Default | What it does | Read in |
|---|---|---|---|
| `ACK_BACKEND` | unset | Where pending approvals are kept: `file` (one process) or `mongo` (shared). `file` is refused when `MULTI_REPLICA=1`. | `apps/api/services/process_role.py` |
| `ADLS_ADLS_COPY` | `1` | Read by `adls_adls_copy_enabled()`: ADLS → ADLS server-side start_copy_from_url (identity bulk) | `apps/api/services/copy_adls_adls.py` |
| `ADMIN_EMAIL` | `""` | Email of the bootstrap administrator. Production needs this pair or `AUTH_USERS`. Some readers accept only `DATAFLOW_ADMIN_EMAIL`. | `apps/api/services/legacy_workspace_backfill.py`, `apps/api/services/live_fleet_proof.py` +2 more |
| `ADMIN_PASSWORD` | `""` | Password of the bootstrap administrator. Set together with `ADMIN_EMAIL`. Some readers accept only `DATAFLOW_ADMIN_PASSWORD`. | `apps/api/services/live_fleet_proof.py`, `apps/api/services/platform_config.py` +1 more |
| `AI_METADATA_ONLY` | `true` | Keeps cell values out of prompts sent to a model provider. On by default. | `apps/api/services/ai_egress.py` |
| `ALLOW_DEV_USER` | `0` | Enables a built-in development login. Production refuses to serve when it is on. | `apps/api/services/platform_config.py`, `apps/api/src/services/auth_service.py` |
| `ALLOW_EMBEDDING_DOWNLOAD` | unset | Read by `_init_backend()`: Datawrap — Embedding Service | `apps/api/src/ai/rag/embedding_service.py` |
| `ALLOW_STUB_WRITES` | `""` | Read by `allow_stub_writes()`: Only allow simulated writes when DATAFLOW_ALLOW_STUB_WRITES=1 | `apps/api/connectors/driver_guard.py` |
| `API_BASE` | `http://127.0.0.1:8001` | Read by `_api_base()`: Prove unique-engine cartesian + catalog slots + schedules + warehouse ports | `apps/api/services/live_fleet_proof.py` |
| `API_CLAIM_LOOP` | `1` | Whether the API process also pulls jobs from the queue in claim mode. Default on; set `0` when dedicated workers execute. | `apps/api/services/scheduler_mode.py` |
| `API_ORPHAN_RESUME` | unset | `0` stops the API from re-queuing orphaned jobs at start. Workers and the scheduler never scan. | `apps/api/services/process_role.py` |
| `APP_URL` | unset | Read by `_mark()`: The app's Datawrap mark (`apps/web/public/brand/datawrap-mark-256.png`, (and other readers) | `apps/api/services/email_templates.py`, `apps/api/services/signup_service.py` |
| `ASSUME_PRODUCTION` | `0` | `1` turns production policy on for a host that does not set `ENV` (bare metal, Kubernetes). | `apps/api/services/platform_config.py` |
| `AUDIT_ANCHOR_EVERY` | `1` | Read by `_maybe_anchor_tip()`: Best-effort seal of the chain tip (never fails the audit append) | `apps/api/services/audit_log.py` |
| `AUDIT_ANCHOR_PROVIDER` | `stub` | The anchor provider asked for. Only the local anchor file is implemented: any other value is recorded on the receipt as `requested_provider`, the receipt says the request was not met, and a warning is logged. | `apps/api/services/audit_anchor.py` |
| `AUDIT_ANCHOR_STORE` | `""` | Path of the audit tip anchor file. Defaults to `audit_tip_anchors.jsonl` in the data directory. | `apps/api/services/audit_anchor.py` |
| `AUDIT_CLOCK_SKEW_S` | `""` | Read by `_clock_skew()`: How far an unverifiable record's own time may run against its verified | `apps/api/services/evidence_chain.py` |
| `AUDIT_HMAC_KEY` | unset | Key that signs the audit chain. Set the same value on every process that appends audit events. | `apps/api/services/audit_chain_keys.py` |
| `AUDIT_HMAC_RETIRED_KEYS` | unset | Comma-separated earlier audit keys, kept so records signed with them still verify. | `apps/api/services/audit_chain_keys.py` |
| `AUDIT_LEGAL_HOLD` | `""` | `1` suspends audit retention purges. | `apps/api/services/audit_log.py` |
| `AUDIT_MAX_EVENTS` | `5000` | Read at module level: Append-only workspace audit log — real events, redacted secrets | `apps/api/services/audit_log.py` |
| `AUDIT_RETENTION_DAYS` | unset | Days of audit events to keep: `off` (or unset) keeps them, otherwise a whole number from 30 to 3650. Any other value is a configuration error. | `apps/api/services/audit_log.py` |
| `AUTH_LEGACY_TOKENS` | `""` | Read by `_legacy_tokens_allowed()`: Allow pre-jti `email:expires:sig` tokens (transition). Default off in production | `apps/api/src/services/auth_service.py` |
| `AUTH_LOCKOUT_FAILURES` | `8` | Read by `_maybe_engage_lockout()`: If failure threshold reached, engage exponential lockout and deny | `apps/api/services/auth_rate_limit.py` |
| `AUTH_LOCKOUT_MAX_SEC` | `3600.0` | Read by `_maybe_engage_lockout()`: If failure threshold reached, engage exponential lockout and deny | `apps/api/services/auth_rate_limit.py` |
| `AUTH_LOCKOUT_SEC` | `300.0` | Read by `_maybe_engage_lockout()`: If failure threshold reached, engage exponential lockout and deny | `apps/api/services/auth_rate_limit.py` |
| `AUTH_RATE_BURST` | `10.0` | Read by `check_login_rate_limit()`: Deny when per-IP or per-account throttle/lockout is exhausted | `apps/api/services/auth_rate_limit.py` |
| `AUTH_RATE_LIMIT` | `1` | Login rate limiting and lockout. On by default. Counters are kept per process. | `apps/api/services/auth_rate_limit.py` |
| `AUTH_RATE_MAX_KEYS` | `5000` | Read by `check_login_rate_limit()`: Deny when per-IP or per-account throttle/lockout is exhausted | `apps/api/services/auth_rate_limit.py` |
| `AUTH_RATE_QPS` | `0.2` | Read by `check_login_rate_limit()`: Deny when per-IP or per-account throttle/lockout is exhausted | `apps/api/services/auth_rate_limit.py` |
| `AUTH_SECRET` | set in code (not shown) | Secret that signs session tokens. Production refuses to serve when it is unset or shorter than 32 characters. | `apps/api/services/audit_chain_keys.py`, `apps/api/services/form_token.py` +5 more |
| `AUTH_TENANT_BIND` | `""` | How strictly a tenant resolved from the Host header must match the caller's tenant claim: `strict`, `soft` or `off`. Default is `strict` in production and `soft` elsewhere. | `apps/api/services/tenant_bind.py` |
| `AUTH_USERS` | `""` | JSON array of user accounts, as an alternative to the admin pair. | `apps/api/services/platform_config.py`, `apps/api/src/services/auth_service.py` |
| `AUTO_INSTALL_DRIVERS` | computed | Read by `_auto_install_enabled()`: Platform driver bootstrap — connectors ship with Datawrap, not end-user installs | `apps/api/services/driver_bootstrap.py` |
| `BIGQUERY_BIGQUERY_COPY` | `1` | Read by `bigquery_bigquery_copy_enabled()`: BigQuery → BigQuery identity bulk (same-project INSERT SELECT / CTAS) | `apps/api/services/copy_bigquery_bigquery.py` |
| `BLOB_ROOT` | `""` | Read by `blob_root()`: Shared filesystem stand-in for object storage | `apps/api/services/object_store.py` |
| `BRAND_OBJECT_NAMES` | `False` | Read by `object_names_flag_on()`: `DATAWRAP_BRAND_OBJECT_NAMES=1`: new artifacts take the new name | `apps/api/services/brand.py` |
| `BULK_EXPORT` | `""` / `0` | Read by `bulk_export_enabled()`: Operator gate — default **off** until F6 publishes measured numbers (and other readers) | `apps/api/connectors/bulk_export.py` |
| `CDC_HEARTBEAT_SEC` | `10` | Read by `heartbeat()`: Keep the lease alive and release WAL an idle slot no longer needs | `apps/api/connectors/postgresql_change_stream.py` |
| `CDC_IDLE_SLOT_ADVANCE` | `1` | Read by `heartbeat()`: Keep the lease alive and release WAL an idle slot no longer needs | `apps/api/connectors/postgresql_change_stream.py` |
| `CDC_LEASE_BACKEND` | unset | Where CDC source leases are kept: `file`, `redis` or `memory`. Unset picks `redis` when a lease Redis URL is configured, else `file`. | `apps/api/services/cdc_lease_store.py` |
| `CDC_LEASE_DIR` | `""` | Read by `build_store()`: CDC lease persistence backends (file / memory / Redis) | `apps/api/services/cdc_lease_store.py` |
| `CDC_LEASE_HOLDER` | unset | Read by `__init__()`: MongoDB Change Streams CDC reader (and other readers) | `apps/api/connectors/mongodb_change_stream.py`, `apps/api/connectors/postgresql_change_stream.py` |
| `CDC_LEASE_PATH` | `""` | Read by `build_store()`: CDC lease persistence backends (file / memory / Redis) | `apps/api/services/cdc_lease_store.py` |
| `CDC_LEASE_REDIS_PREFIX` | unset | Read by `build_store()`: CDC lease persistence backends (file / memory / Redis) | `apps/api/services/cdc_lease_store.py` |
| `CDC_LEASE_REDIS_URL`<br>also unprefixed `CDC_LEASE_REDIS_URL` | unset | Redis URL for CDC leases. | `apps/api/services/cdc_lease_store.py`, `apps/api/services/egress_guard.py` |
| `CDC_LEASE_TTL_SEC` | `120` | Read at module level: Durable CDC resource leases (multi-worker fail-fast) | `apps/api/services/cdc_lease.py` |
| `CDC_MAPPING_REVIEW_STORE` | `""` | Read by `_store_path()`: CDC schema drift → Map review signals | `apps/api/services/cdc_mapping_review.py` |
| `CDC_MAX_IDLE_POLLS` | `3` | Read by `_run_cdc_shared_multi_table()`: One log consumer for N tables (Debezium-class); demux apply per stream (and other readers) | `apps/api/src/transfer/cdc_transfer.py` |
| `CDC_MAX_POLL_ROUNDS` | `50` | Read by `_run_cdc_shared_multi_table()`: One log consumer for N tables (Debezium-class); demux apply per stream (and other readers) | `apps/api/src/transfer/cdc_transfer.py` |
| `CDC_ORACLE_TXN_BUFFER` | `""` | Read by `oracle_txn_buffer_enabled()`: Return whether Oracle transaction buffering is enabled | `apps/api/connectors/oracle_logminer_txn.py` |
| `CDC_ORACLE_TXN_MAX_AGE_SCN` | `0` | Read by `oracle_txn_max_age_scn()`: SCN span after which an open transaction is treated as abandoned (0 = off) | `apps/api/connectors/oracle_logminer_txn.py` |
| `CDC_ORACLE_TXN_MAX_AGE_SECONDS` | `0` | Read by `oracle_txn_max_age_seconds()`: Wall-clock age after which an open transaction is abandoned (0 = off) | `apps/api/connectors/oracle_logminer_txn.py` |
| `CDC_ORACLE_TXN_MAX_BYTES` | computed | Read by `oracle_txn_max_bytes()`: Oracle LogMiner transaction buffer — commit-ordered emission | `apps/api/connectors/oracle_logminer_txn.py` |
| `CDC_ORACLE_TXN_MAX_OPEN` | `1000` | Read by `oracle_txn_max_open()`: Oracle LogMiner transaction buffer — commit-ordered emission | `apps/api/connectors/oracle_logminer_txn.py` |
| `CDC_PG_STREAM_CATCHUP_SEC` | `5.0` | Read by `_poll_through()`: PostgreSQL CDC transport selection (Phase F4) | `apps/api/connectors/postgresql_cdc_transport.py` |
| `CDC_PG_STREAM_POLL_WAIT_SEC` | `0.25` | Read by `__init__()`: PostgreSQL CDC transport selection (Phase F4) | `apps/api/connectors/postgresql_cdc_transport.py` |
| `CDC_PG_STREAM_RECONNECT_BACKOFF_MAX_SEC` | `30.0` | Read by `__init__()`: PostgreSQL CDC transport selection (Phase F4) | `apps/api/connectors/postgresql_cdc_transport.py` |
| `CDC_PG_STREAM_RECONNECT_MAX` | `5` | Read by `__init__()`: PostgreSQL CDC transport selection (Phase F4) | `apps/api/connectors/postgresql_cdc_transport.py` |
| `CDC_PG_TRANSPORT` | `auto` | PostgreSQL CDC transport: `auto` (default, streaming with a fallback) or `peek`. | `apps/api/connectors/postgresql_cdc_transport.py` |
| `CDC_SIGNAL_POLL_SEC` | computed | Read by `__init__()`: MySQL binlog CDC reader using python-mysql-replication (and other readers) | `apps/api/connectors/mysql_change_stream.py`, `apps/api/connectors/postgresql_change_stream.py` |
| `CDC_SOURCE_GONE_GRACE_SEC` | `900` | Read by `_grace_seconds()`: What Datawrap's PostgreSQL CDC left on a source, and the safe way to remove it | `apps/api/services/cdc_capture_inventory.py` |
| `CDC_TXN_BUFFER_MAX_EVENTS` | `50000` | Read by `default_txn_buffer_max_events()`: Debezium-class transaction buffering for log CDC | `apps/api/services/cdc_transaction_buffer.py` |
| `CDC_TXN_HOLD_SLEEP_SEC` | `0.25` | Read by `_run_cdc_shared_multi_table()`: One log consumer for N tables (Debezium-class); demux apply per stream (and other readers) | `apps/api/src/transfer/cdc_transfer.py` |
| `CDC_TXN_SPILL_AFTER` | `""` | Read by `default_txn_spill_after()`: Spill in-memory events to disk after this many (keeps RAM bounded) | `apps/api/services/cdc_transaction_buffer.py` |
| `CHUNK_SIZE` | `20000` | Read at module level: Shared row mapping utilities for database writers | `apps/api/connectors/writer_common.py` |
| `COMMIT_SHA` | unset | Read by `_build_commit()`: Best-effort deployed git SHA for health reporting | `apps/api/src/main.py` |
| `CONFINE_LOCAL_FILES` | `""` | Read by `confine_local_files()`: Where embedded databases and local warehouses may live on the API host | `apps/api/services/local_file_policy.py` |
| `CONNECTOR_POOL_WORKERS` | `16` | Read by `__init__()`: A bounded executor of its own for calls that reach a customer's system (D181) | `apps/api/services/connector_executor.py` |
| `CONNECTOR_READY_MAX_AGE_SECONDS` | `300.0` / computed | Read by `_blocking_step_bound_seconds()`: How long a wait may claim "has not stalled": the existing readiness (and other readers) | `apps/api/services/cancel_check.py`, `apps/api/services/connector_executor.py` |
| `CONNECTOR_STORE` | `""` | Read by `_store_path()`: Return the effective file store path | `apps/api/services/connector_store.py` |
| `CONNECTOR_STORE_BACKEND` | `auto` | Read by `_resolve_backend()`: Pick the persistence backend | `apps/api/services/connector_store.py` |
| `CONNECTOR_WORKSPACE_MAX` | `6` | Read by `__init__()`: A bounded executor of its own for calls that reach a customer's system (D181) | `apps/api/services/connector_executor.py` |
| `CONTACT_EMAIL_CAPACITY` | `2.0` | Read at module level: Public contact-sales intake (D103) | `apps/api/src/routers/contact_router.py` |
| `CONTACT_EMAIL_REFILL_PER_SEC` | computed | Read at module level: Public contact-sales intake (D103) | `apps/api/src/routers/contact_router.py` |
| `CONTACT_IP_CAPACITY` | `5.0` | Read at module level: Public contact-sales intake (D103) | `apps/api/src/routers/contact_router.py` |
| `CONTACT_IP_REFILL_PER_SEC` | computed | Read at module level: Public contact-sales intake (D103) | `apps/api/src/routers/contact_router.py` |
| `CONTRACTS_PATH` | `""` | Read by `_default_path()`: Durable file-backed contract store — survives restarts when Mongo is memory/unavailable | `apps/api/services/contract_file_store.py` |
| `COPY_SPILL_DIR` | unset | Read by `copy_spill_dir()`: Directory for the COPY spill file, when the operator names one | `apps/api/services/copy_fast_path.py` |
| `CORS_INCLUDE_TENANT_DOMAINS` | `1` | Read by `_tenant_origins_enabled()`: Tenant-aware CORS policy for customer vanity / custom domains | `apps/api/services/cors_policy.py` |
| `CROSS_EXTENDED` | `""` | Read by `engines_for_run()`: Default is the 5-engine core that dest-COUNTs without probe-retry hangs (and other readers) | `apps/api/services/desktop_lab_cross.py` |
| `CROSS_PAIR_TIMEOUT` | unset | Read by `_pair_timeout_sec()`: Unique-engine source × dest cartesian on live desktop backends | `apps/api/services/desktop_lab_cross.py` |
| `CSV_LOCAL_COPY` | `1` | Read by `csv_local_copy_enabled()`: Local CSV/TSV identity COPY into PostgreSQL, MySQL, or SQLite | `apps/api/services/copy_csv_local.py` |
| `CSV_LOCAL_COPY_BATCH` | `5000` | Read by `csv_local_copy_batch()`: Local CSV/TSV identity COPY into PostgreSQL, MySQL, or SQLite | `apps/api/services/copy_csv_local.py` |
| `DATA_DIR` | `""` | Directory for the service's own state files. When unset, a mounted volume's `data/` folder is used, else `apps/api/data`. | `apps/api/services/platform_config.py` |
| `DATA_REGIONS` | `""` | Comma-separated regions a workspace may record. | `apps/api/services/tenant_store.py` |
| `DATE_ORDER` | unset | Read by `_env_date_locale()`: Deployment-level date order from the environment | `apps/api/services/transform_engine.py` |
| `DB_POOL_OVERFLOW` | `12` | Read by `pool_settings()`: Pool geometry for networked databases | `apps/api/services/engine_pool.py` |
| `DB_POOL_RECYCLE` | `600` | Read by `pool_settings()`: Pool geometry for networked databases | `apps/api/services/engine_pool.py` |
| `DB_POOL_SIZE` | `8` | Read by `pool_settings()`: Pool geometry for networked databases | `apps/api/services/engine_pool.py` |
| `DB_POOL_TIMEOUT` | `30` | Read by `pool_settings()`: Pool geometry for networked databases | `apps/api/services/engine_pool.py` |
| `DEFAULT_REGION` | `""` / `us-east-1` | This deployment's region. The only evidence residency accepts for a local file, SQLite, DuckDB or loopback destination; with it unset those are refused in a workspace with a data region. Also the region `RESIDENCY_STRICT` enforces (`us-east-1` when unset). | `apps/api/services/data_residency.py`, `apps/api/services/tenant_store.py` |
| `DEMO_MONGO_CONNECTION_STRING` | `""` | Read by `_seed_defaults()`: Example profiles — loaded from the environment so no credentials are hard-coded | `apps/api/services/connector_store.py` |
| `DEMO_PG_CONNECTION_STRING` | `""` | Read by `_seed_defaults()`: Example profiles — loaded from the environment so no credentials are hard-coded | `apps/api/services/connector_store.py` |
| `DEMO_SNOWFLAKE_CONNECTION_STRING` | `""` | Read by `_seed_defaults()`: Example profiles — loaded from the environment so no credentials are hard-coded | `apps/api/services/connector_store.py` |
| `DEMO_SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` | Read by `_seed_defaults()`: Example profiles — loaded from the environment so no credentials are hard-coded | `apps/api/services/connector_store.py` |
| `DESTINATION_CONNECT_DEADLINE_SECONDS` | `""` | Read by `_destination_connect_deadline()` | `apps/api/src/transfer/engine_destination.py` |
| `DEST_LOCK_WAIT_SECONDS` | `120` | Seconds a write session waits for a destination lock before failing (between 1 and 600). | `apps/api/connectors/write_resilience.py` |
| `DEST_WRITE_LOCK` | `False` | Takes one-writer-at-a-time locks per keyed destination table. Off by default. | `apps/api/services/destination_write_lock.py` |
| `DEST_WRITE_LOCK_WAIT_SECONDS` | `3600` | Seconds a job waits for another job's destination write lock. | `apps/api/services/destination_write_lock.py` |
| `DISABLE_OBJECT_STORE` | `""` | Read by `_disabled()`: Object storage for uploaded file staging (MinIO / S3-compatible with local fallback) | `apps/api/services/object_store.py` |
| `DISPOSABLE_FEED_ENABLED` | `0` | Read by `feed_enabled()`: Disposable-domain list (P2-2) — vendored snapshot, no runtime fetch | `apps/api/services/disposable_list.py` |
| `DISPOSABLE_FEED_SHA256` | `""` | Read by `sync_feed()`: Optional live refresh — flag-gated, pinned-URL, pinned-checksum | `apps/api/services/disposable_list.py` |
| `DISPOSABLE_FEED_URL` | `""` | Read by `sync_feed()`: Optional live refresh — flag-gated, pinned-URL, pinned-checksum | `apps/api/services/disposable_list.py` |
| `DROP_ON_VALIDATION_FAILURE` | `0` | Read by `keep_failed_validation_for_inspection()`: Keep what a failed-validation overwrite wrote, and say where it is | `apps/api/services/batch_undo.py` |
| `DUCKDB_DUCKDB_COPY` | `1` | Read by `duckdb_duckdb_copy_enabled()`: DuckDB → DuckDB ATTACH + INSERT SELECT (identity bulk) | `apps/api/services/copy_duckdb_duckdb.py` |
| `DUCKDB_ROOT` | unset | Read by `_explicit_root()`: The operator-set root for `kind` (`sqlite` \| `duckdb`), or `""` | `apps/api/connectors/sqlite_common.py` |
| `DYNAMODB_DYNAMODB_COPY` | `1` | Read by `dynamodb_dynamodb_copy_enabled()`: DynamoDB → DynamoDB Scan + BatchWriteItem (identity bulk) | `apps/api/services/copy_dynamodb_dynamodb.py` |
| `EGRESS_ALLOWLIST` | `""` | Extra networks allowed by the egress guard for this process. Intended for local proof runs; leave unset in production. | `apps/api/services/egress_guard.py` |
| `EGRESS_ENFORCE_NEW` | `""` | `1` makes the newer egress checkpoints refuse private and loopback targets. Without it those checkpoints only write an `egress.would_block` audit event. | `apps/api/services/egress_guard.py` |
| `ELASTICSEARCH_ELASTICSEARCH_COPY` | `1` | Read by `elasticsearch_elasticsearch_copy_enabled()`: Elasticsearch → Elasticsearch cluster `_reindex` (identity bulk) | `apps/api/services/copy_elasticsearch_elasticsearch.py` |
| `EMAIL_DAILY_LIMIT` | `100` | Read by `_daily_limit()`: Resend free-tier quota guard (handover addendum) | `apps/api/services/email_quota.py` |
| `EMAIL_FROM` | computed / `datawrap@example.com` | Read by `_email_from()`: Transactional email sender for the signup/auth surface (P0-2) (and other readers) | `apps/api/services/email_sender.py`, `apps/api/services/platform_config.py` |
| `EMAIL_MONTHLY_LIMIT` | `3000` | Read by `_monthly_limit()`: Resend free-tier quota guard (handover addendum) | `apps/api/services/email_quota.py` |
| `EMAIL_PROVIDER` | `smtp` | Transactional email provider: `smtp` (default), `sendgrid`, `resend` or `mailgun`. | `apps/api/services/email_sender.py`, `apps/api/services/platform_config.py` |
| `EMAIL_REPLY_TO` | unset | Read by `_email_reply_to()`: Transactional email sender for the signup/auth surface (P0-2) | `apps/api/services/email_sender.py` |
| `EMAIL_VERIFIER` | `""` | Read by `configured()`: Optional external email verifier (P2-9): Abstract Email Validation API (and other readers) | `apps/api/services/email_verifier.py` |
| `EMAIL_VERIFIER_KEY` | `""` | Read by `configured()`: Optional external email verifier (P2-9): Abstract Email Validation API (and other readers) | `apps/api/services/email_verifier.py` |
| `EMBEDDING_BACKEND` | unset | Read by `_init_backend()`: Datawrap — Embedding Service | `apps/api/src/ai/rag/embedding_service.py` |
| `EMBEDDING_CACHE_PATH` | `""` | Read by `_resolve_db_path()`: Durable embedding cache — SQLite persistence across process restarts | `apps/api/services/embedding_cache.py` |
| `EMBEDDING_DURABLE_CACHE` | `true` | Read by `durable_cache_enabled_by_default()`: Durable embedding cache — SQLite persistence across process restarts | `apps/api/services/embedding_cache.py` |
| `EMBEDDING_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` | Read by `create_embedding_usage()` (and other readers) | `apps/api/services/embedding_providers/runtime.py`, `apps/api/services/pgvector_typed_embedding.py` +3 more |
| `EMCHG_EMAIL_DAILY` | `3` | Read by `request_email_change()`: P4-2: send an OTP to the NEW address — proving ownership — plus a | `apps/api/services/signup_service.py` |
| `EMCHG_IP_HOURLY` | `10` | Read by `request_email_change()`: P4-2: send an OTP to the NEW address — proving ownership — plus a | `apps/api/services/signup_service.py` |
| `EMCHG_USER_DAILY` | `5` | Read by `request_email_change()`: P4-2: send an OTP to the NEW address — proving ownership — plus a | `apps/api/services/signup_service.py` |
| `ENABLE_DOCS` | `0` / `1` | Serves the interactive API reference at `/docs`. Default on outside production and off in production. | `apps/api/services/platform_config.py` |
| `ENABLE_TRACING` | `0` | `1` turns OpenTelemetry tracing on. Off by default. | `apps/api/services/platform_config.py`, `apps/api/services/tracing.py` |
| `ENGINE_CACHE` | `1` | Read by `cache_enabled()`: Whether engines are reused. Off restores per-call construction | `apps/api/services/engine_pool.py` |
| `ENGINE_CACHE_SIZE` | `32` | Read by `max_engines()`: Process-wide SQLAlchemy Engine cache | `apps/api/services/engine_pool.py` |
| `ENGINE_DIGEST` | `0` | Read by `_engine_digest_enabled()`: Operator gate — default **off** until two gaps are closed | `apps/api/src/transfer/reconcile_step.py` |
| `ENV` | computed | Declares the environment. `production` or `prod` turns production policy on; `development`, `dev`, `local` or `test` turns it off. Falls back to the unprefixed `ENVIRONMENT`. | `apps/api/services/platform_config.py`, `apps/api/services/secret_vault.py` +1 more |
| `ENVIRONMENT`<br>also unprefixed `ENVIRONMENT` | `""` | Unprefixed fallback for `DATAWRAP_ENV`. | `apps/api/services/platform_config.py`, `apps/api/services/secret_vault.py` +1 more |
| `ES_HOST` | `127.0.0.1` | Read by `_es_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `ES_PORT` | `9200` | Read by `_es_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `EXPORTS_DIR` | unset | Read by `exports_dir()`: The folder exports are confined to: `DATAFLOW_EXPORTS_DIR`, else `apps/api/exports` | `apps/api/services/export_paths.py` |
| `EXPORT_BUCKET` | `""` | Read by `export_bucket()`: Transfer Studio file exports in a private bucket, downloaded through signed URLs | `apps/api/services/export_object_store.py` |
| `EXPORT_PART_SIZE_MB` | computed | Read by `export_part_size()`: Transfer Studio file exports in a private bucket, downloaded through signed URLs | `apps/api/services/export_object_store.py` |
| `EXPORT_PREFIX` | `""` | Read by `export_prefix()`: Transfer Studio file exports in a private bucket, downloaded through signed URLs | `apps/api/services/export_object_store.py` |
| `EXPORT_RETENTION_HOURS` | computed | Read by `export_retention_hours()`: Transfer Studio file exports in a private bucket, downloaded through signed URLs | `apps/api/services/export_object_store.py` |
| `EXPORT_S3_ACCESS_KEY` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) | `apps/api/services/export_object_store.py` |
| `EXPORT_S3_ENDPOINT` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) | `apps/api/services/export_object_store.py` |
| `EXPORT_S3_PATH_STYLE` | computed | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) | `apps/api/services/export_object_store.py` |
| `EXPORT_S3_REGION` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) | `apps/api/services/export_object_store.py` |
| `EXPORT_S3_SECRET_KEY` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) | `apps/api/services/export_object_store.py` |
| `EXPORT_URL_TTL_SECONDS` | computed | Read by `export_url_ttl_seconds()`: Transfer Studio file exports in a private bucket, downloaded through signed URLs | `apps/api/services/export_object_store.py` |
| `FAKESNOW_KEEP_PATCH` | unset | Read by `_fakesnow_exit_patch()`: Snowflake connection helper | `apps/api/connectors/snowflake_conn.py` |
| `FIELD_REDUCTION_STRICT` | `false` | Read by `strict_field_reduction()`: True when every reduction must carry a reason code and an approver | `apps/api/services/field_reduction_ledger.py` |
| `FILE_SPILL_THRESHOLD` | computed | Read at module level: Streaming file → database transfer for CSV, TSV, JSONL, NDJSON, JSON arrays, | `apps/api/src/transfer/file_stream.py` |
| `FINGERPRINT_SPILL_THRESHOLD` | `100000` / `1000000` | Read at module level: Turning a stream of row fingerprints into one order-independent digest (and other readers) | `apps/api/services/fingerprint_accumulator.py`, `apps/api/services/reconciliation.py` |
| `FORGOT_EMAIL_DAILY` | `5` | Read by `request_password_reset()`: P4-1 forgot-password: uniform 202 with a challenge handle — a fake | `apps/api/services/signup_service.py` |
| `FORGOT_IP_HOURLY` | `10` | Read by `request_password_reset()`: P4-1 forgot-password: uniform 202 with a challenge handle — a fake | `apps/api/services/signup_service.py` |
| `GCS_GCS_COPY` | `1` | Read by `gcs_gcs_copy_enabled()`: GCS → GCS server-side copy_blob / rewrite (identity bulk) | `apps/api/services/copy_gcs_gcs.py` |
| `GEOIP_ENABLED` | `0` | Read by `enabled()`: Optional GeoIP/ASN lookup adapter (P2-8) | `apps/api/services/geoip.py` |
| `HIBP_ENABLED` | `""` | Read by `enabled()`: Have I Been Pwned password check (P4-3) — k-anonymity range API | `apps/api/services/hibp.py` |
| `HSTS` | `""` | `1` always sends the HSTS header; `0` never does. Unset: sent when the request is HTTPS or `X-Forwarded-Proto` says so. | `apps/api/src/main.py` |
| `ICEBERG_ICEBERG_COPY` | `1` | Read by `iceberg_iceberg_copy_enabled()`: Iceberg snapshot Parquet → Iceberg catalog snapshot (identity bulk) | `apps/api/services/copy_iceberg_iceberg.py` |
| `ICEBERG_MONGO_COPY` | `1` | Read by `iceberg_mongo_copy_enabled()`: Iceberg snapshot Parquet → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_iceberg_mongo.py` |
| `ICEBERG_MONGO_COPY_BATCH` | unset | Read by `copy_iceberg_to_mongo()`: Bind Iceberg snapshot files into Mongo insert_many. Dest count_documents is the proof | `apps/api/services/copy_iceberg_mongo.py` |
| `ICEBERG_MYSQL_COPY` | `1` | Read by `iceberg_mysql_copy_enabled()`: Iceberg snapshot Parquet → MySQL STRICT LOAD DATA (cross-engine bulk) | `apps/api/services/copy_iceberg_mysql.py` |
| `ICEBERG_ORACLE_COPY` | `1` | Read by `iceberg_oracle_copy_enabled()`: Iceberg snapshot Parquet → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_iceberg_oracle.py` |
| `ICEBERG_PG_COPY` | `1` | Read by `iceberg_pg_copy_enabled()`: Iceberg snapshot Parquet → PostgreSQL COPY FROM STDIN (cross-engine bulk) | `apps/api/services/copy_iceberg_pg.py` |
| `ICEBERG_REST_URI` | `http://127.0.0.1:8181` | Read by `_iceberg_rest_uri()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `ICEBERG_REST_WAREHOUSE` | `file:///tmp/iceberg-rest-wh` | Read by `_iceberg_rest_warehouse()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `ICEBERG_ROOT` | unset | Read by `iceberg_local_root()`: Directory every local Iceberg warehouse / SQLite catalog must live under | `apps/api/connectors/iceberg_catalog.py` |
| `ICEBERG_S3_COPY` | `1` | Read by `iceberg_s3_copy_enabled()`: Iceberg snapshot Parquet → S3 CSV PUT (cross-engine bulk) | `apps/api/services/copy_iceberg_s3.py` |
| `ICEBERG_SQLITE_COPY` | `1` | Read by `iceberg_sqlite_copy_enabled()`: Iceberg snapshot Parquet → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_iceberg_sqlite.py` |
| `ICEBERG_SQLITE_COPY_BATCH` | `5000` | Read by `iceberg_sqlite_copy_batch()`: Iceberg snapshot Parquet → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_iceberg_sqlite.py` |
| `ICEBERG_SQLSERVER_COPY` | `1` | Read by `iceberg_sqlserver_copy_enabled()`: Iceberg snapshot Parquet → SQL Server fast_executemany (cross-engine bulk) | `apps/api/services/copy_iceberg_sqlserver.py` |
| `INCREMENTAL_DRIFT_FAILS` | `False` | Read by `drift_fails_run()`: D188 — an incremental run verifies its cursor window; the rest is drift | `apps/api/services/incremental_drift.py` |
| `INVITE_ACCEPT_IP_10MIN` | `30` | Read by `_check_accept_rate()`: Workspace invitations by email — the enterprise "invite a teammate" flow | `apps/api/services/workspace_invites.py` |
| `INVITE_ACTOR_HOURLY` | `30` | Read by `_check_rate()`: Workspace invitations by email — the enterprise "invite a teammate" flow | `apps/api/services/workspace_invites.py` |
| `INVITE_ADDRESS_DAILY` | `5` | Read by `_check_rate()`: Workspace invitations by email — the enterprise "invite a teammate" flow | `apps/api/services/workspace_invites.py` |
| `INVITE_TTL_DAYS` | `7` | Read by `invite_ttl()`: `DATAWRAP_INVITE_TTL_DAYS` (default 7, clamped 1..30) | `apps/api/services/workspace_invites.py` |
| `INVITE_WORKSPACE_DAILY` | `200` | Read by `_check_rate()`: Workspace invitations by email — the enterprise "invite a teammate" flow | `apps/api/services/workspace_invites.py` |
| `JOB_CHECKPOINT_REJECTED_PREVIEW` | `25` | Read at module level: MongoDB transfer_jobs document budget — enterprise control-plane SSOT | `apps/api/services/job_document_budget.py` |
| `JOB_EVENT_LOG_MAX` | `5000` | Read at module level: MongoDB transfer_jobs document budget — enterprise control-plane SSOT | `apps/api/services/job_document_budget.py` |
| `JOB_IDEMPOTENCY` | `1` | Read by `_idempotency_key()`: Full claim key for a request, or '' when the guard is disabled | `apps/api/src/transfer/engine.py` |
| `JOB_MESSAGE_CHARS` | `2000` | Read at module level: MongoDB transfer_jobs document budget — enterprise control-plane SSOT | `apps/api/services/job_document_budget.py` |
| `JOB_ORPHAN_GRACE_SEC` | computed | How long a job document must be untouched before the orphan sweeper may settle it. Defaults to the worker lease TTL. | `apps/api/services/orphan_jobs.py` |
| `JOB_ORPHAN_MAX_RECLAIMS` | `2` | How many times an orphaned job is resumed from its checkpoint before it is recorded as failed. | `apps/api/services/orphan_jobs.py` |
| `JOB_ORPHAN_SWEEP_SEC` | `30` | Read by `start_orphan_sweeper()`: Run :func:`sweep_orphan_jobs` on a daemon thread until stopped | `apps/api/services/orphan_jobs.py` |
| `JOB_REJECTED_CELL_CHARS` | `240` | Read at module level: MongoDB transfer_jobs document budget — enterprise control-plane SSOT | `apps/api/services/job_document_budget.py` |
| `JOB_REJECTED_PREVIEW_MAX` | `40` | Read at module level: MongoDB transfer_jobs document budget — enterprise control-plane SSOT | `apps/api/services/job_document_budget.py` |
| `JOB_STALL_SWEEP_SEC` | `30` | Read by `start_stall_watchdog()`: Run :func:`sweep_stalled_jobs` on a daemon thread until stopped | `apps/api/services/job_watchdog.py` |
| `JOB_STEP_STALL_FAIL_SEC` | computed | Read by `stall_fail_seconds()`: Hard-fail age for a stalled step (flag is set well before this) | `apps/api/services/job_watchdog.py` |
| `JOB_STEP_STALL_SEC` | `900.0` | Default seconds a job step may stay quiet before the job is marked stalled. Past three times this value the job is failed. | `apps/api/services/job_watchdog.py` |
| `JOB_STORE` | `""` / `auto` | `memory` keeps jobs in process memory (tests and demos). `mongodb` requires the shared MongoDB backend. Default is automatic. Some readers accept only `DATAFLOW_JOB_STORE`. | `apps/api/services/config.py`, `apps/api/services/mongodb_service.py` +1 more |
| `JOB_UPDATE_SET_BUDGET_BYTES` | computed | Read at module level: MongoDB transfer_jobs document budget — enterprise control-plane SSOT | `apps/api/services/job_document_budget.py` |
| `JOURNAL_CAPTURE` | `""` | Read by `capture_requested()`: Capture-once transfer journal — extract once, replay to many destinations | `apps/api/services/transfer_journal.py` |
| `JOURNAL_DIR` | `""` | Read by `journal_root()`: Capture-once transfer journal — extract once, replay to many destinations | `apps/api/services/transfer_journal.py` |
| `JOURNAL_RETENTION_DAYS` | `""` | Read by `retention_seconds()`: Capture-once transfer journal — extract once, replay to many destinations | `apps/api/services/transfer_journal.py` |
| `KAFKA_KAFKA_COPY` | `1` | Read by `kafka_kafka_copy_enabled()`: Kafka → Kafka consume-produce of raw bytes (identity bulk) | `apps/api/services/copy_kafka_kafka.py` |
| `KEY_COLLATION_NO_PAD` | `""` | Read by `no_pad_key_collation_enabled()`: Distinct source keys the destination's comparison rules make equal (D205) | `apps/api/services/dest_key_equality.py` |
| `LEGACY_UNSCOPED_ADMIN_VIEW` | `False` | Read by `legacy_admin_view_enabled()`: `DATAFLOW_LEGACY_UNSCOPED_ADMIN_VIEW` — off by default (D218) | `apps/api/services/tenant_scope.py` |
| `LLM_ENABLED`<br>also unprefixed `LLM_ENABLED` | `true` | Read by `is_llm_enabled()`: Return whether LLM inference is globally enabled | `apps/api/services/llm_policy.py` |
| `LOCAL_FILE_DESTINATIONS` | computed | Read by `local_file_destinations_allowed()`: May a destination write to this server's own disk? Env reads only | `apps/api/services/local_file_policy.py` |
| `LOG_FORMAT` | unset | Read by `log_format()`: `json` or `text`; JSON is the default outside a TTY | `apps/api/services/logging_config.py` |
| `LOG_LEVEL` | `INFO` | Read by `configure_logging()`: Install Datawrap's root logging configuration. Idempotent (and other readers) | `apps/api/services/logging_config.py`, `apps/api/src/scheduler_main.py` +1 more |
| `LOG_LEVELS` | unset | Read by `_library_levels()`: Per-library floors, overridable via `DATAFLOW_LOG_LEVELS` | `apps/api/services/logging_config.py` |
| `MAIL_OUTBOX_SWEEP_SEC` | `30` | Read by `start_mail_outbox_sweeper()`: Keep draining the outbox after boot | `apps/api/services/signup_service.py` |
| `MAXMIND_LICENSE_KEY` | `""` | Read by `license_key()`: Optional GeoIP/ASN lookup adapter (P2-8) | `apps/api/services/geoip.py` |
| `MAX_DECOMPRESSED_BYTES` | computed | Read by `max_decompressed_bytes()`: Byte ceilings for uploaded files and for gzip payloads inflated in memory | `apps/api/services/upload_limits.py` |
| `MAX_LINEAGE_EVENTS` | `5000` | Read at module level: Lineage and telemetry events for the universal transfer orchestrator | `apps/api/services/lineage_telemetry.py` |
| `MAX_REJECTED_DETAILS` | `80` | Read at module level: Durable checkpoint persistence for resumable transfer jobs | `apps/api/services/checkpoint_service.py` |
| `MAX_UPLOAD_BYTES` | computed | Read by `max_upload_bytes()`: Byte ceilings for uploaded files and for gzip payloads inflated in memory | `apps/api/services/upload_limits.py` |
| `MCP_LOG_MAX` | `2000` | Read at module level: Persisted MCP tool invocation log — latency, status, and safe error type | `apps/api/services/mcp_invocation_log.py` |
| `MCP_OAUTH_AUDIENCE` | `""` | Read by `get_mcp_oauth_config()`: Resolve the MCP protected-resource issuer and resource audience | `apps/api/services/mcp_oauth.py` |
| `MCP_OAUTH_ISSUER` | `""` | Read by `get_mcp_oauth_config()`: Resolve the MCP protected-resource issuer and resource audience | `apps/api/services/mcp_oauth.py` |
| `MCP_RATE_BURST` | `30.0` | Read by `check_mcp_rate_limit()`: Consume one token for `principal` | `apps/api/services/mcp_rate_limit.py` |
| `MCP_RATE_LIMIT` | `1` | Read by `rate_limit_enabled()`: Default on at a generous ceiling; set DATAFLOW_MCP_RATE_LIMIT=0 to disable | `apps/api/services/mcp_rate_limit.py` |
| `MCP_RATE_MAX_KEYS` | `5000` | Read by `check_mcp_rate_limit()`: Consume one token for `principal` | `apps/api/services/mcp_rate_limit.py` |
| `MCP_RATE_QPS` | `5.0` | Read by `check_mcp_rate_limit()`: Consume one token for `principal` | `apps/api/services/mcp_rate_limit.py` |
| `MFA_CHALLENGE_TTL_SEC` | `300` | Read by `challenge_ttl_sec()`: TOTP second factor for password sign-in accounts (QA S7, round 11) | `apps/api/services/mfa.py` |
| `MFA_FAILURE_WINDOW_SEC` | `900` | Read by `failure_window_sec()`: TOTP second factor for password sign-in accounts (QA S7, round 11) | `apps/api/services/mfa.py` |
| `MFA_LOCKOUT_SEC` | `900` | Read by `lockout_sec()`: TOTP second factor for password sign-in accounts (QA S7, round 11) | `apps/api/services/mfa.py` |
| `MFA_MAX_FAILURES` | `5` | Read by `max_failures()`: Bad codes tolerated inside :func:`failure_window_sec` before lockout | `apps/api/services/mfa.py` |
| `MFA_PENDING_TTL_SEC` | `900` | Read by `pending_ttl_sec()`: How long an unconfirmed enrolment secret stays usable | `apps/api/services/mfa.py` |
| `MFA_SSO_EXEMPT` | `True` | Read by `sso_exempt()`: SSO sessions skip the workspace MFA gate (default on: the IdP does MFA) | `apps/api/services/mfa_enforcement.py` |
| `MILVUS_MILVUS_COPY` | `1` | Read by `milvus_milvus_copy_enabled()`: Milvus → Milvus query + upsert (identity bulk) | `apps/api/services/copy_milvus_milvus.py` |
| `MONGO_BATCH_SIZE` | `1000` | Read at module level: MongoDB bulk writer — CSV file to collection with checkpoint batches | `apps/api/connectors/mongodb_writer.py` |
| `MONGO_FLATTEN_NESTED` | unset | Read by `mongo_flatten_enabled()`: Operator / env switch — default on so Mongo→SQL maps nested leaves | `apps/api/services/json_intelligence.py` |
| `MONGO_ICEBERG_COPY` | `1` | Read by `mongo_iceberg_copy_enabled()`: MongoDB snapshot find → Iceberg catalog snapshot (cross-engine bulk) | `apps/api/services/copy_mongo_iceberg.py` |
| `MONGO_MONGO_COPY` | `1` | Read by `mongo_mongo_copy_enabled()`: MongoDB snapshot find → MongoDB insert_many (identity bulk) | `apps/api/services/copy_mongo_mongo.py` |
| `MONGO_MONGO_COPY_BATCH` | unset | Read by `copy_mongo_to_mongo()`: Snapshot find on src, insert_many on dest. Dest count_documents is the proof | `apps/api/services/copy_mongo_mongo.py` |
| `MONGO_MYSQL_COPY` | `1` | Read by `mongo_mysql_copy_enabled()`: MongoDB snapshot find → MySQL STRICT LOAD DATA (cross-engine bulk) | `apps/api/services/copy_mongo_mysql.py` |
| `MONGO_ORACLE_COPY` | `1` | Read by `mongo_oracle_copy_enabled()`: MongoDB snapshot find → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_mongo_oracle.py` |
| `MONGO_PG_COPY` | `1` | Read by `mongo_pg_copy_enabled()`: MongoDB snapshot find → PostgreSQL COPY FROM STDIN (cross-engine bulk) | `apps/api/services/copy_mongo_pg.py` |
| `MONGO_S3_COPY` | `1` | Read by `mongo_s3_copy_enabled()`: MongoDB snapshot find → S3 CSV PUT (cross-engine bulk) | `apps/api/services/copy_mongo_s3.py` |
| `MONGO_SQLITE_COPY` | `1` | Read by `mongo_sqlite_copy_enabled()`: MongoDB snapshot find → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_mongo_sqlite.py` |
| `MONGO_SQLITE_COPY_BATCH` | `5000` | Read by `mongo_sqlite_copy_batch()`: MongoDB snapshot find → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_mongo_sqlite.py` |
| `MONGO_SQLSERVER_COPY` | `1` | Read by `mongo_sqlserver_copy_enabled()`: MongoDB snapshot find → SQL Server fast_executemany (cross-engine bulk) | `apps/api/services/copy_mongo_sqlserver.py` |
| `MSSQL_CDC_CAPTURE_WAIT_SEC` | `""` | Read by `_capture_wait_budget_sec()`: SQL Server *native* CDC (cdc schema) — Debezium-class path | `apps/api/connectors/sqlserver_cdc_native.py` |
| `MS_FIT_AGGREGATE_SECONDS` | `""` | Read by `aggregate_timeout_seconds()`: Millisecond narrowing onto a document-store instant, decided on values (D148) | `apps/api/services/millisecond_fit.py` |
| `MULTI_REPLICA` | `""` | `1` declares that more than one process shares this deployment. Turns on the topology checks that refuse an unsafe process split, and makes the Mongo job queue mandatory. | `apps/api/services/cdc_lease_store.py`, `apps/api/services/process_role.py` +1 more |
| `MYSQL_ICEBERG_COPY` | `1` | Read by `mysql_iceberg_copy_enabled()`: MySQL SELECT → Iceberg catalog snapshot (cross-engine bulk) | `apps/api/services/copy_mysql_iceberg.py` |
| `MYSQL_LOAD_DATA` | `1` | Read by `mysql_load_data_enabled()`: Operator gate — default **on**. `DATAFLOW_MYSQL_LOAD_DATA=0` disables | `apps/api/connectors/mysql_load_data.py` |
| `MYSQL_MONGO_COPY` | `1` | Read by `mysql_mongo_copy_enabled()`: MySQL consistent-snapshot SELECT → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_mysql_mongo.py` |
| `MYSQL_MONGO_COPY_BATCH` | unset | Read by `mysql_mongo_copy_batch()`: MySQL consistent-snapshot SELECT → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_mysql_mongo.py` |
| `MYSQL_MYSQL_INSERT_SELECT` | `1` | Read by `mysql_mysql_insert_select_enabled()`: MySQL → MySQL identity bulk (same-engine) | `apps/api/services/copy_mysql_mysql.py` |
| `MYSQL_ORACLE_COPY` | `1` | Read by `mysql_oracle_copy_enabled()`: MySQL SELECT → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_mysql_oracle.py` |
| `MYSQL_S3_COPY` | `1` | Read by `mysql_s3_copy_enabled()`: MySQL SELECT CSV → S3 PUT (cross-engine bulk) | `apps/api/services/copy_mysql_s3.py` |
| `MYSQL_SQLITE_COPY` | `1` | Read by `mysql_sqlite_copy_enabled()`: MySQL consistent-snapshot SELECT → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_mysql_sqlite.py` |
| `MYSQL_SQLITE_COPY_BATCH` | `5000` | Read by `mysql_sqlite_copy_batch()`: MySQL consistent-snapshot SELECT → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_mysql_sqlite.py` |
| `MYSQL_SQLSERVER_COPY` | `1` | Read by `mysql_sqlserver_copy_enabled()`: MySQL SELECT → SQL Server fast_executemany (cross-engine bulk) | `apps/api/services/copy_mysql_sqlserver.py` |
| `NOTIFICATION_DELIVERY_RETENTION_DAYS` | `30` | Read by `retention_days()`: Append-only log of every notification delivery attempt (QA SC9) | `apps/api/services/notification_delivery_log.py` |
| `NOTIFICATION_STORE` | `""` | Read by `_store_path()`: Persisted workspace notification channels for job alerts | `apps/api/services/notification_store.py` |
| `OBJECT_STORE_MATERIALIZE_BATCH` | `""` | Read by `resolve_materialize_batch()`: Accepted-row bundle size before encode. Peak mapped RAM is this many tuples | `apps/api/connectors/object_store_materialize.py` |
| `OBJECT_STORE_MULTIPART_PART_SIZE` | computed | Read by `resolve_multipart_limits()`: Return `(threshold_bytes, part_size_bytes)` from dest extra / env | `apps/api/connectors/object_store_multipart.py` |
| `OBJECT_STORE_MULTIPART_THRESHOLD` | computed | Read by `resolve_multipart_limits()`: Return `(threshold_bytes, part_size_bytes)` from dest extra / env | `apps/api/connectors/object_store_multipart.py` |
| `OBJECT_STORE_SPILL_MAX` | `""` | Read by `resolve_spill_max()`: Bytes kept in the serialize spool before rolling to a temp file | `apps/api/connectors/object_store_multipart.py` |
| `OIDC_ALLOWED_ALGS` | `RS256` | Read by `allowed_algorithms()`: Return the configured signing algorithms constrained by the provider | `apps/api/services/oidc_client.py` |
| `OIDC_CLOCK_SKEW_SEC` | `60` | Read by `clock_skew_leeway()`: Return a validated OIDC clock-skew allowance in seconds | `apps/api/services/oidc_client.py` |
| `OIDC_JWKS_REFRESH_COOLDOWN_SEC` | `60` | Read by `_jwks_refresh_cooldown()`: Strict OIDC discovery, signing-key retrieval, and ID-token validation | `apps/api/services/oidc_client.py` |
| `OIDC_JWKS_TTL_SEC` | `3600` | Read by `_jwks_ttl()`: Strict OIDC discovery, signing-key retrieval, and ID-token validation | `apps/api/services/oidc_client.py` |
| `OIDC_METADATA_TTL_SEC` | `3600` | Read by `discover()`: Discover and validate the provider metadata for a configured issuer | `apps/api/services/oidc_client.py` |
| `OIDC_REQUIRE_EMAIL_VERIFIED` | `1` | Read by `email_from_claims()`: Extract a verified provider email, normalized for account matching | `apps/api/services/oidc_client.py` |
| `ORACLE_EMPTY_RETRY_SEC` | `0.25` | Read by `_empty_window_retry_sec()`: Oracle LogMiner CDC — Debezium-class redo log mining | `apps/api/connectors/oracle_logminer.py` |
| `ORACLE_ICEBERG_COPY` | `1` | Read by `oracle_iceberg_copy_enabled()`: Oracle SELECT → Iceberg catalog snapshot (cross-engine bulk) | `apps/api/services/copy_oracle_iceberg.py` |
| `ORACLE_MONGO_COPY` | `1` | Read by `oracle_mongo_copy_enabled()`: Oracle SHARE-lock SELECT → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_oracle_mongo.py` |
| `ORACLE_MONGO_COPY_BATCH` | unset | Read by `copy_oracle_to_mongo()`: SELECT Oracle into Mongo insert_many. Dest count_documents is the proof | `apps/api/services/copy_oracle_mongo.py` |
| `ORACLE_MYSQL_COPY` | `1` | Read by `oracle_mysql_copy_enabled()`: Oracle SELECT → MySQL STRICT LOAD DATA (cross-engine bulk) | `apps/api/services/copy_oracle_mysql.py` |
| `ORACLE_ORACLE_INSERT_SELECT` | `1` | Read by `oracle_oracle_insert_select_enabled()`: Oracle → Oracle identity bulk (same-instance INSERT SELECT) | `apps/api/services/copy_oracle_oracle.py` |
| `ORACLE_PASSWORD` | unset | Read by `_oracle_password()`: Unique-engine source × dest cartesian on live desktop backends (and other readers) | `apps/api/services/desktop_lab_cross.py`, `apps/api/services/million_row_bench.py` |
| `ORACLE_PG_COPY` | `1` | Read by `oracle_pg_copy_enabled()`: Oracle SELECT → PostgreSQL COPY FROM STDIN (cross-engine bulk) | `apps/api/services/copy_oracle_pg.py` |
| `ORACLE_S3_COPY` | `1` | Read by `oracle_s3_copy_enabled()`: Oracle SHARE-lock SELECT CSV → S3 PUT (cross-engine bulk) | `apps/api/services/copy_oracle_s3.py` |
| `ORACLE_SQLITE_COPY` | `1` | Read by `oracle_sqlite_copy_enabled()`: Oracle SHARE-lock SELECT → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_oracle_sqlite.py` |
| `ORACLE_SQLITE_COPY_BATCH` | `5000` | Read by `oracle_sqlite_copy_batch()`: Oracle SHARE-lock SELECT → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_oracle_sqlite.py` |
| `ORACLE_SQLSERVER_COPY` | `1` | Read by `oracle_sqlserver_copy_enabled()`: Oracle SELECT → SQL Server fast_executemany (cross-engine bulk) | `apps/api/services/copy_oracle_sqlserver.py` |
| `OTEL_LOGS` | `0` | Read by `otel_logs_enabled()`: Optional OpenTelemetry log export for the centralized logging pipeline | `apps/api/services/otel_logging.py` |
| `OTLP_ENDPOINT` | unset | Read by `otlp_endpoint()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `OTLP_INSECURE` | `1` | Read by `_otlp_insecure()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `OTP_PEPPER` | unset | Pepper for one-time codes and password-reset tokens. Required in production only when self-serve signup is enabled. | `apps/api/services/otp_service.py`, `apps/api/services/platform_config.py` |
| `PARALLEL_QUEUE` | computed | Read at module level: Bounded, ordered parallel chunk processing for streaming transfers | `apps/api/services/parallel_chunks.py` |
| `PARALLEL_WORKERS` | `4` / computed | Read at module level: Bounded, ordered parallel chunk processing for streaming transfers (and other readers) | `apps/api/services/parallel_chunks.py`, `apps/api/src/transfer/file_stream.py` +1 more |
| `PARITY_EXPLAIN_TIMEOUT` | `20` | Read by `_timeout_seconds()`: Parity Studio: the plain-language explanation of a parity report | `apps/api/services/parity_explain.py` |
| `PARITY_READ_CACHE_SECONDS` | `30` | Read by `ttl_seconds()`: Parity Studio: a short-lived, per-process cache of the live reads a report makes | `apps/api/services/parity_cache.py` |
| `PARITY_REREAD_TIMEOUT` | `30` | Read by `_reread_seconds()`: Parity Studio: one answer a person can sign — does the destination match? | `apps/api/services/parity_report.py` |
| `PARITY_SIGNOFF_AUTHORITY` | `""` | JSON object mapping each Trust Report sign-off role to the gate roles (and optionally people) allowed to sign it. A value that does not parse refuses every signature. | `apps/api/services/parity_authority.py` |
| `PGOUTPUT_DECODER` | `""` | Read by `_select_plugin()`: Select logical decoding plugin | `apps/api/connectors/postgresql_change_stream.py` |
| `PGVECTOR_PGVECTOR_COPY` | `1` | Read by `pgvector_pgvector_copy_enabled()`: pgvector → pgvector binary COPY (identity bulk) | `apps/api/services/copy_pgvector_pgvector.py` |
| `PG_ICEBERG_COPY` | `1` | Read by `pg_iceberg_copy_enabled()`: PostgreSQL COPY CSV → Iceberg catalog snapshot (cross-engine bulk) | `apps/api/services/copy_pg_iceberg.py` |
| `PG_MONGO_COPY` | `1` | Read by `pg_mongo_copy_enabled()`: PostgreSQL COPY text → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_pg_mongo.py` |
| `PG_MONGO_COPY_BATCH` | `5000` | Read by `pg_mongo_copy_batch()`: PostgreSQL COPY text → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_pg_mongo.py` |
| `PG_MYSQL_COPY_WORKERS` | `auto` | Read by `pg_mysql_copy_workers()`: Operator cap. `auto` uses 4 workers at ≥50k and 8 at ≥1M | `apps/api/services/copy_pg_mysql.py` |
| `PG_ORACLE_COPY` | `1` | Read by `pg_oracle_copy_enabled()`: PostgreSQL COPY text → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_pg_oracle.py` |
| `PG_ORACLE_COPY_BATCH` | `50000` | Read by `pg_oracle_copy_batch()`: PostgreSQL COPY text → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_pg_oracle.py` |
| `PG_PG_COPY` | `1` | Read by `pg_pg_copy_enabled()`: Server-to-server COPY for routes whose types are proven identical | `apps/api/services/copy_fast_path.py` |
| `PG_S3_COPY` | `1` | Read by `pg_s3_copy_enabled()`: PostgreSQL COPY CSV → S3 PUT (cross-engine bulk) | `apps/api/services/copy_pg_s3.py` |
| `PG_SQLITE_COPY` | `1` | Read by `pg_sqlite_copy_enabled()`: PostgreSQL COPY text → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_pg_sqlite.py` |
| `PG_SQLITE_COPY_BATCH` | `5000` | Read by `pg_sqlite_copy_batch()`: PostgreSQL COPY text → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_pg_sqlite.py` |
| `PG_SQLSERVER_COPY` | `1` | Read by `pg_sqlserver_copy_enabled()`: Shared pg/mysql pair helpers — the connect and identifier wrappers the | `apps/api/services/copy_sql_pairs.py` |
| `PG_SQLSERVER_COPY_BATCH` | `5000` | Read by `pg_sqlserver_copy_batch()`: Shared pg/mysql pair helpers — the connect and identifier wrappers the | `apps/api/services/copy_sql_pairs.py` |
| `PII_HASH_KEY` | unset | Read by `pinned_key()`: The operator-pinned key that every workspace uses, or `""` | `apps/api/services/pii_hash_keys.py` |
| `PII_MASKING` | `true` | Read by `is_pii_masking_enabled()`: Return whether PII masking before LLM prompts is required | `apps/api/services/llm_policy.py` |
| `PILOT_ACK_PATH` | `""` | Read by `_default_path()`: Server-side mutation approval ledger for Datawrap Pilot | `apps/api/src/ai/copilot/ack_ledger.py` |
| `PILOT_ENGINE` | unset | Read by `pilot_engine_decision()`: Which engine Pilot will use, and the reason — never a credential | `apps/api/src/ai/llm/provider.py` |
| `PILOT_MEMORY_PATH` | `""` | Read by `_default_path()`: Per-session working memory for Datawrap Pilot — the state a follow-up turn edits | `apps/api/src/ai/copilot/working_memory.py` |
| `PILOT_RAG` | unset | Read by `build()`: Build full context for Datawrap Pilot — every data source in the platform | `apps/api/src/ai/copilot/context_builder.py` |
| `PILOT_RATE_BURST` | `30.0` | Read by `check_pilot_rate_limit()`: Consume one Pilot chat token for `principal`; DATAFLOW_PILOT_RATE_LIMIT=0 disables | `apps/api/services/mcp_rate_limit.py` |
| `PILOT_RATE_LIMIT` | `1` | Read by `check_pilot_rate_limit()`: Consume one Pilot chat token for `principal`; DATAFLOW_PILOT_RATE_LIMIT=0 disables | `apps/api/services/mcp_rate_limit.py` |
| `PILOT_RATE_MAX_KEYS` | `5000` | Read by `check_pilot_rate_limit()`: Consume one Pilot chat token for `principal`; DATAFLOW_PILOT_RATE_LIMIT=0 disables | `apps/api/services/mcp_rate_limit.py` |
| `PILOT_RATE_QPS` | `0.5` | Read by `check_pilot_rate_limit()`: Consume one Pilot chat token for `principal`; DATAFLOW_PILOT_RATE_LIMIT=0 disables | `apps/api/services/mcp_rate_limit.py` |
| `PILOT_RESULTS_PATH` | `""` | Read by `_default_path()`: Durable Pilot query/sample result references | `apps/api/src/ai/copilot/result_store.py` |
| `PINECONE_PINECONE_COPY` | `1` | Read by `pinecone_pinecone_copy_enabled()`: Pinecone → Pinecone list + fetch + upsert (identity bulk) | `apps/api/services/copy_pinecone_pinecone.py` |
| `POST_TRANSFER_TRAINING` | `""` | Read by `_execute_tracked_core()`: Universal transfer orchestrator — routes any source to any destination | `apps/api/src/transfer/engine.py` |
| `PROCEDURE_TIMEOUT_S` | unset | Read by `_open_callable_result()`: Stored-procedure / custom-SQL extract — one source-read owner | `apps/api/services/procedure_source.py` |
| `PROCESS_ROLE` | unset | Which role this process plays: `api` (default), `scheduler` or `worker`. Unknown values are treated as `api`. | `apps/api/services/process_role.py` |
| `PROXY_CHUNK_SIZE` | `5000` | Read at module level: Shared helpers for durable bulk writes over flaky public proxies | `apps/api/connectors/write_resilience.py` |
| `PUBLIC_API_ORIGIN` | `""` | Read by `allowed_hosts()`: The public origin (scheme + host) that links handed to users must use (and other readers) | `apps/api/services/public_origin.py` |
| `PUBLIC_ORIGIN_ALLOWED_HOSTS` | `""` | Read by `allowed_hosts()`: The public origin (scheme + host) that links handed to users must use | `apps/api/services/public_origin.py` |
| `PUBLIC_RATE_BURST` | `120.0` | Read by `check_public_rate_limit()`: Per-IP bucket for unauthenticated routes. Health probes are not counted | `apps/api/services/auth_rate_limit.py` |
| `PUBLIC_RATE_LIMIT` | `""` | `1` turns on rate limiting for unauthenticated routes. Off by default, including in production. | `apps/api/services/auth_rate_limit.py` |
| `PUBLIC_RATE_MAX_KEYS` | `5000` | Read by `check_public_rate_limit()`: Per-IP bucket for unauthenticated routes. Health probes are not counted | `apps/api/services/auth_rate_limit.py` |
| `PUBLIC_RATE_QPS` | `2.0` | Read by `check_public_rate_limit()`: Per-IP bucket for unauthenticated routes. Health probes are not counted | `apps/api/services/auth_rate_limit.py` |
| `PUBLIC_URL` | `""` | Public base URL of the API, used for absolute links such as MCP OAuth metadata. | `apps/api/services/platform_config.py` |
| `QDRANT_QDRANT_COPY` | `1` | Read by `qdrant_qdrant_copy_enabled()`: Qdrant → Qdrant scroll + upsert (identity bulk) | `apps/api/services/copy_qdrant_qdrant.py` |
| `QUALITY_ANOMALY_MIN_ROWS` | `25` | Read at module level: History-aware data quality for recurring transfers (last-N load intelligence) | `apps/api/services/data_quality_history.py` |
| `QUALITY_HISTORY_LIMIT` | `20` | Read at module level: History-aware data quality for recurring transfers (last-N load intelligence) | `apps/api/services/data_quality_history.py` |
| `QUALITY_PROFILES_DIR` | unset | Read by `quality_profiles_dir()`: JSON fallback store for last-N load profiles | `apps/api/services/data_quality_history.py` |
| `QUERY_EXTRACT_MAX_OBJECTS` | `25` | Read at module level: Extract-and-query: the query console's fallback for non-SQL connectors | `apps/api/services/query_extract.py` |
| `QUERY_EXTRACT_MAX_ROWS` | `50000` | Read at module level: Extract-and-query: the query console's fallback for non-SQL connectors | `apps/api/services/query_extract.py` |
| `QUERY_STATEMENT_TIMEOUT_SECONDS` | `""` | Read by `_apply_statement_timeout()`: Have the server cancel an interactive query that outlives the tool deadline | `apps/api/src/routers/query_router.py` |
| `RECONCILE_SOURCE_REREAD` | `auto` | Read by `_env_reread_mode()`: Independent source re-read for Gate-8 — Fivetran/HVR Compare class | `apps/api/services/source_reread.py` |
| `REDIS_DB` | `0` | Read by `_redis_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `REDIS_HOST` | `127.0.0.1` | Read by `_redis_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `REDIS_PORT` | `6379` | Read by `_redis_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `REDIS_REDIS_COPY` | `1` | Read by `redis_redis_copy_enabled()`: Redis → Redis server-side COPY (identity bulk) | `apps/api/services/copy_redis_redis.py` |
| `REDIS_URL`<br>also unprefixed `REDIS_URL` | unset | Read by `_redis_client()`: Shared auth state (P0-1): sessions and rate-limit counters (and other readers) | `apps/api/services/auth_state.py`, `apps/api/services/cdc_lease_store.py` +2 more |
| `REDSHIFT_COPY_GZIP_THRESHOLD` | `1048576` | Read at module level: Redshift COPY FROM S3 — Fivetran/Airbyte-class bulk load | `apps/api/connectors/redshift_copy.py` |
| `REDSHIFT_COPY_THRESHOLD` | `1000` | Read at module level: Redshift COPY FROM S3 — Fivetran/Airbyte-class bulk load | `apps/api/connectors/redshift_copy.py` |
| `REDSHIFT_IAM_ROLE` | unset | Read by `resolve_redshift_copy_config()`: Return staging config when both bucket and IAM role are present | `apps/api/connectors/redshift_copy.py` |
| `REDSHIFT_STAGING_BUCKET` | unset | Read by `resolve_redshift_copy_config()`: Return staging config when both bucket and IAM role are present | `apps/api/connectors/redshift_copy.py` |
| `REQUIRE_AUTH` | `""` / `0` / computed | Must be `1` in production. Setting it with `ENV` unset also makes the process treat itself as production. | `apps/api/services/platform_config.py`, `apps/api/src/services/auth_service.py` |
| `REQUIRE_WORKSPACE` | `""` | Read by `require_workspace_isolation()`: When True, unscoped (`workspace_id ""`) resources are platform-admin only | `apps/api/services/team_store.py` |
| `RESEND_API_KEY`<br>also unprefixed `RESEND_API_KEY` | `""` / computed | Read by `send()`: Transactional email sender for the signup/auth surface (P0-2) (and other readers) | `apps/api/services/email_sender.py`, `apps/api/services/platform_config.py` |
| `RESEND_WEBHOOK_SECRET` | `""` | Read by `secret()`: Svix-signed webhook verification (P2-10) | `apps/api/services/webhook_verify.py` |
| `RESIDENCY_STRICT` | `""` | `1` enforces `DEFAULT_REGION` as the data region of every workspace that records none. Without it, only workspaces with a data region are fenced. | `apps/api/services/data_residency.py` |
| `RESOURCE_ACL_STORE` | `""` | Read by `_store_path()`: Resource-level ACL grants beyond coarse RBAC roles | `apps/api/services/resource_acl.py` |
| `RESUME_MAX_AGE_HOURS` | `168` | Read by `evaluate_resume_safety()`: Decide whether Resume is safe for operators | `apps/api/services/checkpoint_service.py` |
| `RETRY_AFTER_MAX_SECONDS` | `300.0` | Read at module level: Error handling for the universal transfer orchestrator | `apps/api/services/error_handling.py` |
| `RETRY_BASE_DELAY_SECONDS` | `1.0` | Read at module level: Runtime configuration for Datawrap API (and other readers) Some readers accept only `DATAFLOW_RETRY_BASE_DELAY_SECONDS`. | `apps/api/services/config.py`, `apps/api/services/error_handling.py` |
| `RETRY_EXPONENTIAL_BASE` | `2.0` | Read at module level: Runtime configuration for Datawrap API (and other readers) Some readers accept only `DATAFLOW_RETRY_EXPONENTIAL_BASE`. | `apps/api/services/config.py`, `apps/api/services/error_handling.py` |
| `RETRY_JITTER` | `True` / `true` | Read at module level: Runtime configuration for Datawrap API (and other readers) Some readers accept only `DATAFLOW_RETRY_JITTER`. | `apps/api/services/config.py`, `apps/api/services/error_handling.py` |
| `RETRY_MAX_ATTEMPTS` | `3` | Read at module level: Runtime configuration for Datawrap API (and other readers) Some readers accept only `DATAFLOW_RETRY_MAX_ATTEMPTS`. | `apps/api/services/config.py`, `apps/api/services/error_handling.py` |
| `RETRY_MAX_DELAY_SECONDS` | `60.0` | Read at module level: Runtime configuration for Datawrap API (and other readers) Some readers accept only `DATAFLOW_RETRY_MAX_DELAY_SECONDS`. | `apps/api/services/config.py`, `apps/api/services/error_handling.py` |
| `S3_ACCESS_KEY` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `S3_BUCKET`<br>also unprefixed `S3_BUCKET` | unset | Read by `_bucket_name()`: Object storage for uploaded file staging (MinIO / S3-compatible with local fallback) (and other readers) | `apps/api/services/object_store.py` |
| `S3_ENDPOINT` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `S3_ICEBERG_COPY` | `1` | Read by `s3_iceberg_copy_enabled()`: S3 CSV GET → Iceberg catalog snapshot (cross-engine bulk) | `apps/api/services/copy_s3_iceberg.py` |
| `S3_MONGO_COPY` | `1` | Read by `s3_mongo_copy_enabled()`: S3 CSV GET → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_s3_mongo.py` |
| `S3_MONGO_COPY_BATCH` | unset | Read by `s3_mongo_copy_batch()`: S3 CSV GET → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_s3_mongo.py` |
| `S3_MYSQL_COPY` | `1` | Read by `s3_mysql_copy_enabled()`: S3 CSV GET → MySQL STRICT LOAD DATA (cross-engine bulk) | `apps/api/services/copy_s3_mysql.py` |
| `S3_ORACLE_COPY` | `1` | Read by `s3_oracle_copy_enabled()`: S3 CSV GET → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_s3_oracle.py` |
| `S3_PATH_STYLE` | `1` | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `S3_PG_COPY` | `1` | Read by `s3_pg_copy_enabled()`: S3 CSV GET → PostgreSQL COPY FROM STDIN (cross-engine bulk) | `apps/api/services/copy_s3_pg.py` |
| `S3_S3_COPY` | `1` | Read by `s3_s3_copy_enabled()`: S3 → S3 server-side CopyObject (identity bulk) | `apps/api/services/copy_s3_s3.py` |
| `S3_SECRET_KEY` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `S3_SECURE` | `1` / computed | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `S3_SQLITE_COPY` | `1` | Read by `s3_sqlite_copy_enabled()`: S3 CSV GET → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_s3_sqlite.py` |
| `S3_SQLITE_COPY_BATCH` | `5000` | Read by `s3_sqlite_copy_batch()`: S3 CSV GET → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_s3_sqlite.py` |
| `S3_SQLSERVER_COPY` | `1` | Read by `s3_sqlserver_copy_enabled()`: S3 CSV GET → SQL Server fast_executemany (cross-engine bulk) | `apps/api/services/copy_s3_sqlserver.py` |
| `SALES_EMAIL` | unset | Read by `sales_address()`: Where Contact Sales leads go: `DATAWRAP_SALES_EMAIL` (legacy | `apps/api/services/signup_service.py` |
| `SAML_ACS_URL` | `""` | Read by `_saml_acs_url()` | `apps/api/src/routers/auth_router.py` |
| `SAML_SP_ENTITY_ID` | `""` | Read by `_saml_sp_entity_id()` | `apps/api/src/routers/auth_router.py` |
| `SCHEDULER_LOCK_TTL` | `300` | Read at module level: Background scheduler — runs due pipeline syncs | `apps/api/services/schedule_runner.py` |
| `SCHEDULER_MODE` | `auto` | `local`, `claim` or `auto` (default). `auto` picks `claim` when a shared backend is required. `WORKER_FLEET` overrides it. | `apps/api/services/scheduler_mode.py` |
| `SCHEDULE_LOOP` | unset | `1` or `0` forces the pipeline cadence loop on or off in this process. Unset: on for the API and scheduler, off for workers. | `apps/api/services/process_role.py` |
| `SCHEDULE_ORPHAN_REPAIR` | `dry_run` | Read by `startup_orphan_scan()`: Report orphans at startup; adopt the settled ones only when asked to | `apps/api/services/schedule_orphans.py` |
| `SCHEDULE_UNBOUND_CLAIM_GRACE_SEC` | `900` | Read by `_unbound_claim_grace()`: How long a claim may exist before its job is bound | `apps/api/services/schedule_store.py` |
| `SCHEMA_CACHE` | `1` | Read by `cache_enabled()`: Per-table metadata cache for the transfer hot path | `apps/api/services/reflection_cache.py` |
| `SCHEMA_CACHE_SIZE` | `512` | Read by `_max_entries()`: Per-table metadata cache for the transfer hot path | `apps/api/services/reflection_cache.py` |
| `SCHEMA_CACHE_TTL` | `300` | Read by `_ttl()`: Per-table metadata cache for the transfer hot path | `apps/api/services/reflection_cache.py` |
| `SECRET` | unset | Read by `pinned_key()`: The operator-pinned key that every workspace uses, or `""` | `apps/api/services/pii_hash_keys.py` |
| `SECRETS_BACKEND` | unset | Where connector secrets are kept: `fernet` (default) or `aws_secretsmanager`. | `apps/api/services/secret_vault.py` |
| `SECRETS_KEY` | `""` | Fernet key that encrypts connector credentials at rest. Required in production. | `apps/api/services/platform_config.py`, `apps/api/services/secret_vault.py` |
| `SECRETS_KMS_KEY_ID` | `""` | Read by `encrypt()`: Encrypt connector credentials at rest | `apps/api/services/secret_vault.py` |
| `SECRETS_MANAGER_PREFIX` | unset | Read by `_secrets_manager_prefix()`: Encrypt connector credentials at rest | `apps/api/services/secret_vault.py` |
| `SEED_DEMO` | `""` / `False` | Seeds example data at start. The deployment files in this repository set it to `0`. Some readers accept only `DATAFLOW_SEED_DEMO`. | `apps/api/services/config.py`, `apps/api/services/connector_store.py` |
| `SERVICE_INSTANCE_ID` | unset | Read by `_service_instance_id()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `SERVICE_NAME` | `dataflow-api` | Read by `ensure_provider()`: Initialize the global TracerProvider once. Safe to call repeatedly (and other readers) | `apps/api/services/tracing.py` |
| `SERVICE_NAMESPACE` | `dataflow` | Read by `telemetry_resource()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `SERVICE_VERSION` | unset | Read by `telemetry_resource()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `SFTP_HOST_KEY` | `""` | Read by `parse_sftp_config()`: Merge explicit fields with an sftp:// URI | `apps/api/connectors/sftp_common.py` |
| `SFTP_HOST_KEY_POLICY` | `""` | Read by `parse_sftp_config()`: Merge explicit fields with an sftp:// URI | `apps/api/connectors/sftp_common.py` |
| `SFTP_KNOWN_HOSTS` | `""` | Read by `parse_sftp_config()`: Merge explicit fields with an sftp:// URI | `apps/api/connectors/sftp_common.py` |
| `SF_BIND_INSERT_ROWS` | `1000` | Read at module level: Snowflake bulk writer — COPY INTO staging for scale + batched INSERT fallback | `apps/api/connectors/snowflake_writer.py` |
| `SHAPE_PREVIEW_BUDGET_S` | `20` | Read by `_preview_budget_seconds()`: Server-side time bound for one preview (D14): `DATAFLOW_/DATAWRAP_SHAPE_PREVIEW_BUDGET_S` | `apps/api/src/routers/shape_router.py` |
| `SIGNUP_DEVICE_DAILY` | `10` | Read by `_check_signup_velocity()`: Per-IP, per-subnet, per-device and per-address caps (P2-6) | `apps/api/services/signup_service.py` |
| `SIGNUP_EMAIL_DAILY` | `5` | Read by `_check_signup_velocity()`: Per-IP, per-subnet, per-device and per-address caps (P2-6) | `apps/api/services/signup_service.py` |
| `SIGNUP_ENABLED` | `0` | Master switch for self-serve signup. Off by default; with it off the signup routes answer 404. | `apps/api/services/signup_config.py` |
| `SIGNUP_IP_HOURLY` | `10` | Read by `_check_signup_velocity()`: Per-IP, per-subnet, per-device and per-address caps (P2-6) | `apps/api/services/signup_service.py` |
| `SIGNUP_SUBNET_HOURLY` | `50` | Read by `_check_signup_velocity()`: Per-IP, per-subnet, per-device and per-address caps (P2-6) | `apps/api/services/signup_service.py` |
| `SINGER_TAPS_DIR` | unset | Read by `singer_taps_dir()`: The operator's taps directory, or `SingerUnavailable` naming the setting | `apps/api/services/request_principal.py` |
| `SMTP_FROM` | `datawrap@localhost` | Read by `_env_smtp()`: Send job/status alerts to configured notification channels (and other readers) | `apps/api/services/notification_service.py`, `apps/api/services/platform_config.py` |
| `SMTP_HOST` | `""` | Read by `_env_smtp()`: Send job/status alerts to configured notification channels | `apps/api/services/notification_service.py` |
| `SMTP_PASSWORD` | `""` | Read by `_env_smtp()`: Send job/status alerts to configured notification channels | `apps/api/services/notification_service.py` |
| `SMTP_PORT` | `587` | Read by `_env_smtp()`: Send job/status alerts to configured notification channels | `apps/api/services/notification_service.py` |
| `SMTP_USER` | `""` | Read by `_env_smtp()`: Send job/status alerts to configured notification channels | `apps/api/services/notification_service.py` |
| `SMTP_USE_TLS` | `true` | Read by `_env_smtp()`: Send job/status alerts to configured notification channels | `apps/api/services/notification_service.py` |
| `SNOWFLAKE_COPY_THRESHOLD` | `200` | Read at module level: Snowflake bulk writer — COPY INTO staging for scale + batched INSERT fallback | `apps/api/connectors/snowflake_writer.py` |
| `SNOWFLAKE_FILE_CHUNK_SIZE` | `100000` | Read at module level: Streaming file → database transfer for CSV, TSV, JSONL, NDJSON, JSON arrays, | `apps/api/src/transfer/file_stream.py` |
| `SNOWFLAKE_SNOWFLAKE_COPY` | `1` | Read by `snowflake_snowflake_copy_enabled()`: Snowflake → Snowflake identity bulk (same-account INSERT SELECT / CTAS) | `apps/api/services/copy_snowflake_snowflake.py` |
| `SNOWFLAKE_WARM_LOGIN_EMULATOR` | `""` | Read by `enabled()`: A Snowflake plan login outlives the tool call that opened it (QA D187) | `apps/api/connectors/snowflake_warm_login.py` |
| `SNOWFLAKE_WARM_LOGIN_SECONDS` | `""` | Read by `idle_seconds()`: A Snowflake plan login outlives the tool call that opened it (QA D187) | `apps/api/connectors/snowflake_warm_login.py` |
| `SOURCE_PATH_ROOTS` | `""` | Read by `transfer_source_roots()`: Directories a file transfer may read `source_path` from | `apps/api/services/dataset_file.py` |
| `SOURCE_ROW_SPILL_MAX` | `""` | Read by `resolve_source_spill_max()`: Disk-backed source-row spool for object-store and SQL/warehouse materialize | `apps/api/connectors/source_row_spool.py` |
| `SPILL_DIR` | unset | Read at module level: Streaming file → database transfer for CSV, TSV, JSONL, NDJSON, JSON arrays, | `apps/api/src/transfer/file_stream.py` |
| `SPILL_TTL` | `""` | Read by `spill_ttl_seconds()`: How long a spilled download stays reusable for one job | `apps/api/services/object_streaming.py` |
| `SQLITE_BENCH_DIR` | `/tmp/dataflow-sqlite-bench` | Read by `_sqlite_bench_dir()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `SQLITE_DESTINATION` | computed | Read by `sqlite_destination_allowed()`: May SQLite be a transfer destination in this process? Env reads only | `apps/api/services/local_file_policy.py` |
| `SQLITE_ICEBERG_COPY` | `1` | Read by `sqlite_iceberg_copy_enabled()`: SQLite SELECT → Iceberg catalog snapshot (cross-engine bulk) | `apps/api/services/copy_sqlite_iceberg.py` |
| `SQLITE_MONGO_COPY` | `1` | Read by `sqlite_mongo_copy_enabled()`: SQLite SELECT → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_sqlite_mongo.py` |
| `SQLITE_MONGO_COPY_BATCH` | unset | Read by `sqlite_mongo_copy_batch()`: SQLite SELECT → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_sqlite_mongo.py` |
| `SQLITE_MYSQL_COPY` | `1` | Read by `sqlite_mysql_copy_enabled()`: SQLite SELECT → MySQL STRICT LOAD DATA (cross-engine bulk) | `apps/api/services/copy_sqlite_mysql.py` |
| `SQLITE_ORACLE_COPY` | `1` | Read by `sqlite_oracle_copy_enabled()`: SQLite SELECT → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_sqlite_oracle.py` |
| `SQLITE_PG_COPY` | `1` | Read by `sqlite_pg_copy_enabled()`: SQLite SELECT → PostgreSQL COPY FROM STDIN (cross-engine bulk) | `apps/api/services/copy_sqlite_pg.py` |
| `SQLITE_ROOT` | unset | Read by `_explicit_root()`: The operator-set root for `kind` (`sqlite` \| `duckdb`), or `""` | `apps/api/connectors/sqlite_common.py` |
| `SQLITE_S3_COPY` | `1` | Read by `sqlite_s3_copy_enabled()`: SQLite SELECT CSV → S3 PUT (cross-engine bulk) | `apps/api/services/copy_sqlite_s3.py` |
| `SQLITE_SQLITE_COPY` | `1` | Read by `sqlite_sqlite_copy_enabled()`: SQLite → SQLite ATTACH + INSERT SELECT (identity bulk) | `apps/api/services/copy_sqlite_sqlite.py` |
| `SQLITE_SQLSERVER_COPY` | `1` | Read by `sqlite_sqlserver_copy_enabled()`: SQLite SELECT → SQL Server fast_executemany (cross-engine bulk) | `apps/api/services/copy_sqlite_sqlserver.py` |
| `SQLSERVER_HOST` | unset | Read by `bind_live_engine()`: Bind one unique engine to a live desktop backend, or a skip reason | `apps/api/services/desktop_lab_cross.py` |
| `SQLSERVER_ICEBERG_COPY` | `1` | Read by `sqlserver_iceberg_copy_enabled()`: SQL Server SELECT → Iceberg catalog snapshot (cross-engine bulk) | `apps/api/services/copy_sqlserver_iceberg.py` |
| `SQLSERVER_MONGO_COPY` | `1` | Read by `sqlserver_mongo_copy_enabled()`: SQL Server HOLDLOCK SELECT → MongoDB insert_many (cross-engine bulk) | `apps/api/services/copy_sqlserver_mongo.py` |
| `SQLSERVER_MONGO_COPY_BATCH` | unset | Read by `copy_sqlserver_to_mongo()`: SELECT SQL Server into Mongo insert_many. Dest count_documents is the proof | `apps/api/services/copy_sqlserver_mongo.py` |
| `SQLSERVER_MYSQL_COPY` | `1` | Read by `sqlserver_mysql_copy_enabled()`: SQL Server SELECT → MySQL STRICT LOAD DATA (cross-engine bulk) | `apps/api/services/copy_sqlserver_mysql.py` |
| `SQLSERVER_ORACLE_COPY` | `1` | Read by `sqlserver_oracle_copy_enabled()`: SQL Server SELECT → Oracle executemany (cross-engine bulk) | `apps/api/services/copy_sqlserver_oracle.py` |
| `SQLSERVER_PG_COPY` | `1` | Read by `sqlserver_pg_copy_enabled()`: SQL Server SELECT → PostgreSQL COPY FROM STDIN (cross-engine bulk) | `apps/api/services/copy_sqlserver_pg.py` |
| `SQLSERVER_S3_COPY` | `1` | Read by `sqlserver_s3_copy_enabled()`: SQL Server HOLDLOCK SELECT CSV → S3 PUT (cross-engine bulk) | `apps/api/services/copy_sqlserver_s3.py` |
| `SQLSERVER_SQLITE_COPY` | `1` | Read by `sqlserver_sqlite_copy_enabled()`: SQL Server HOLDLOCK SELECT → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_sqlserver_sqlite.py` |
| `SQLSERVER_SQLITE_COPY_BATCH` | `5000` | Read by `sqlserver_sqlite_copy_batch()`: SQL Server HOLDLOCK SELECT → SQLite executemany (cross-engine bulk) | `apps/api/services/copy_sqlserver_sqlite.py` |
| `SQLSERVER_SQLSERVER_INSERT_SELECT` | `1` | Read by `sqlserver_sqlserver_insert_select_enabled()`: SQL Server → SQL Server identity bulk (same-instance INSERT SELECT) | `apps/api/services/copy_sqlserver_sqlserver.py` |
| `SQL_MATERIALIZE_BATCH` | `""` | Read by `resolve_sql_materialize_batch()`: Map-bundle size for warehouse writers. Peak mapped-source RAM is this many rows | `apps/api/connectors/sql_write_materialize.py` |
| `SSO_ALLOWED_DOMAINS` | `""` | Comma-separated email domains allowed to sign in through SSO. | `apps/api/services/sso_authorization.py` |
| `SSO_AUTO_PROVISION` | `0` | `1` creates an account for an SSO identity on first sign-in, but only for email domains in `SSO_ALLOWED_DOMAINS`. With that list empty no new identity is provisioned (existing accounts still sign in), and the API logs a warning at start. | `apps/api/services/sso_authorization.py` |
| `STREAM_FILE_ROWS` | `1` | Read at module level: Streaming file → database transfer for CSV, TSV, JSONL, NDJSON, JSON arrays, | `apps/api/src/transfer/file_stream.py` |
| `TEAM_STORE` | `""` | Read by `_store_path()`: Workspaces and who belongs to them | `apps/api/services/team_store.py` |
| `TENANT_ID` | unset | Read by `_tenant_id()`: Encrypt connector credentials at rest | `apps/api/services/secret_vault.py` |
| `TENANT_IP_ALLOWLIST_ENFORCE` | `True` | Read by `enforcement_on()`: Kill switch: on unless the platform turned allowlist enforcement off | `apps/api/services/ip_allowlist_policy.py` |
| `TEST_MSSQL_HOST` | `127.0.0.1` | Read by `bind_live_engine()`: Bind one unique engine to a live desktop backend, or a skip reason | `apps/api/services/desktop_lab_cross.py` |
| `TEST_MSSQL_PASSWORD` | `""` | Read by `bind_live_engine()`: Bind one unique engine to a live desktop backend, or a skip reason (and other readers) | `apps/api/services/desktop_lab_cross.py`, `apps/api/services/million_row_bench.py` |
| `TOKEN_TTL_SEC` | set in code (not shown) | Lifetime of a session token in seconds. | `apps/api/src/services/auth_service.py` |
| `TRACE_CONSOLE` | `0` | Read by `ensure_provider()`: Initialize the global TracerProvider once. Safe to call repeatedly | `apps/api/services/tracing.py` |
| `TRAINING` | `False` | Runs the background training agent. Unset: on outside production, off in production. Set: only `1`, `true`, `yes` or `on` turn it on; any other value, an empty one included, leaves it off. | `apps/api/src/ai/training/training_scheduler.py` |
| `TRANSFER_WORKERS` | `8` | Read by `_ensure_executor()`: Lazily create the shared thread pool (and other readers) | `apps/api/services/transfer_scheduler.py`, `apps/api/services/worker_fleet.py` +1 more |
| `TRANSFORMS_PATH` | unset | Read by `__init__()`: Persistence for transformation projects | `apps/api/services/transform_store.py` |
| `TRANSFORM_ERROR_POLICY` | `quarantine` | Read at module level: Shared row mapping utilities for database writers | `apps/api/connectors/writer_common.py` |
| `TRANSFORM_LIBRARY_PATH` | unset | Read by `__init__()`: Persistence for transform recipes — the Transform Library store | `apps/api/services/transform_library.py` |
| `TRUSTED_PROXY_COUNT`<br>also unprefixed `TRUSTED_PROXY_COUNT` | `""` / `0` | Number of trusted proxies in front of the API. With N above 0 the client address is taken from `X-Forwarded-For` entry N from the right. Default 0 changes nothing. Some readers accept only `DATAFLOW_TRUSTED_PROXY_COUNT`, `DATAWRAP_TRUSTED_PROXY_COUNT`. | `apps/api/services/client_ip.py` |
| `TURNSTILE_SECRET` | `""` | Read by `secret()`: Cloudflare Turnstile verification (P2-5) | `apps/api/services/turnstile.py` |
| `TURNSTILE_SITE_KEY` | `""` | Read by `turnstile_site_key()`: Cloudflare Turnstile site key — public by design, safe to hand the SPA | `apps/api/services/signup_config.py` |
| `UPLOAD_DIR` | `""` | Directory for uploaded source files. | `apps/api/services/platform_config.py` |
| `VECTOR_STORE_BACKEND` | `memory` | Read by `_init_backend()`: Datawrap — Vector Store | `apps/api/src/ai/rag/vector_store.py` |
| `VECTOR_STORE_DIR` | `""` | Directory for the local vector store. | `apps/api/services/platform_config.py`, `apps/api/src/ai/rag/bootstrap.py` +1 more |
| `VERIFICATION_LADDER_MAX_ROWS` | `250000` | Largest table, in rows, that the in-memory verification ladder compares row by row. | `apps/api/services/verification_ladder.py` |
| `VERSION` | `1` | Read by `_cef_line()`: CEF intentionally omits details; the NDJSON export includes them | `apps/api/src/routers/audit_router.py` |
| `WEAVIATE_WEAVIATE_COPY` | `1` | Read by `weaviate_weaviate_copy_enabled()`: Weaviate → Weaviate list + batch upsert (identity bulk) | `apps/api/services/copy_weaviate_weaviate.py` |
| `WEB_DOMAIN` | `""` / `http://localhost:5173` | Public domain of the web app. Added to the allowed origins and used for links. | `apps/api/services/platform_config.py`, `apps/api/services/public_origin.py` +1 more |
| `WEB_URL` | `""` | Public base URL of the web app, used for links in notifications. | `apps/api/services/platform_config.py`, `apps/api/services/public_origin.py` |
| `WORKER_FLEET` | `""` | `1` forces claim mode (jobs are queued in MongoDB for workers); `0` forces local mode (the API's own thread pool). | `apps/api/services/scheduler_mode.py` |
| `WORKER_LEASE_TTL` | `60` | Seconds a worker's job lease lasts between heartbeats (minimum 15). | `apps/api/services/lease_heartbeat.py`, `apps/api/services/transfer_scheduler.py` |
| `WORKER_MODE` | unset | Which workloads a worker claims: `batch`, `cdc`, or both when unset. | `apps/api/services/process_role.py` |
| `WORKER_POLL` | `2` | Seconds between queue polls in the worker process. | `apps/api/services/worker_fleet.py`, `apps/api/src/worker_main.py` |
| `WORKSPACE_ADMIN_IAM` | `True` | Read by `workspace_admin_iam_enabled()`: `DATAFLOW_WORKSPACE_ADMIN_IAM` (default on = pre-hardening behaviour) | `apps/api/services/rbac.py` |
| `WRITE_RECONNECT_ATTEMPTS` | `12` | Read at module level: Shared helpers for durable bulk writes over flaky public proxies | `apps/api/connectors/write_resilience.py` |
| `WRITE_RECONNECT_MAX_SECONDS` | `600` | Read at module level: Shared helpers for durable bulk writes over flaky public proxies | `apps/api/connectors/write_resilience.py` |

### Flags read under one exact name

These are read directly, without the brand helper, so only the name shown works. Setting the other prefix has no effect.

| Flag | Default | What it does | Read in |
|---|---|---|---|
| `DATAFLOW_EXPOSE_ERROR_DETAIL` | `""` | `1` appends the redacted exception text to safe error responses. Leave off in production. | `apps/api/services/safe_errors.py` |
| `DATAFLOW_FP_LM` | `True` | Read by `lm_enabled()`: Serve path: semantic rewrite + copy-grounded narrate, both fail-closed | `apps/api/src/ai/first_party/engine.py` |
| `DATAFLOW_FP_NARRATE` | `True` | Read by `narrate_enabled()`: Serve path: semantic rewrite + copy-grounded narrate, both fail-closed | `apps/api/src/ai/first_party/engine.py` |
| `DATAFLOW_FP_REWRITE` | `True` | Read by `rewrite_enabled()`: Serve path: semantic rewrite + copy-grounded narrate, both fail-closed | `apps/api/src/ai/first_party/engine.py` |
| `DATAFLOW_JOB_STORE_PERSIST` | `False` | Read at module level: Runtime configuration for Datawrap API | `apps/api/services/config.py` |
| `DATAFLOW_PREFLIGHT_DEADLINE_SECONDS` | `120.0` | Read by `preflight_deadline_seconds()`: A deadline for pilot/MCP tools that reach out to a customer's database (D169, D181) | `apps/api/src/ai/copilot/tool_deadline.py` |
| `DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS` | `120.0` | Read by `deadline_seconds()`: A deadline for pilot/MCP tools that reach out to a customer's database (D169, D181) | `apps/api/src/ai/copilot/tool_deadline.py` |
| `DATAFLOW_RBAC_UNRULED_ROUTES` | unset | What happens to a route with no RBAC rule: `deny` (default) or `allow_and_log`. An invalid value falls back to `deny`. | `apps/api/services/rbac.py` |
| `DATAFLOW_TOOL_DEADLINE_SECONDS` | `45.0` | Read by `deadline_seconds()`: A deadline for pilot/MCP tools that reach out to a customer's database (D169, D181) | `apps/api/src/ai/copilot/tool_deadline.py` |
| `DATAFLOW_VOLUME_PATH`<br>`DATAWRAP_VOLUME_PATH` | `""` | Mount path of a persistent volume. `data/`, `uploads/` and `vector_store/` default to folders under it. | `apps/api/services/platform_config.py` |

### Unprefixed variables

Standard or third-party names (cloud SDKs, database URLs, platform variables), read as shown.

| Flag | Default | What it does | Read in |
|---|---|---|---|
| `AWS_ACCESS_KEY_ID` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `AWS_DEFAULT_REGION` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` +1 more |
| `AWS_ENDPOINT_URL` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `AWS_REGION` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` +1 more |
| `AWS_S3_BUCKET` | unset | Read by `_bucket_name()`: Object storage for uploaded file staging (MinIO / S3-compatible with local fallback) (and other readers) | `apps/api/services/object_store.py` |
| `AWS_SECRET_ACCESS_KEY` | unset | Read by `export_client()`: A boto3 S3 client for the export bucket (signature v4, for AWS and R2) (and other readers) | `apps/api/services/export_object_store.py`, `apps/api/services/object_store.py` |
| `AZURE_OPENAI_API_KEY` | unset | Read by `resolve_provider()` | `apps/api/services/embedding_providers/config.py` |
| `AZURITE_ACCOUNT` | `devstoreaccount1` | Read by `_adls_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `AZURITE_HOST` | `127.0.0.1` | Read by `_adls_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `AZURITE_KEY` | set in code (not shown) | Read by `_adls_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `AZURITE_PORT` | `10000` | Read by `_adls_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `BENCH_ADLS_CONTAINER` | `dataflowbench` | Read by `_adls_bench_container()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `BENCH_DUCKDB_DIR` | computed | Read by `_duckdb_bench_path()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `BENCH_GCS_BUCKET` | `dataflow-bench` | Read by `_gcs_bench_bucket()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `BENCH_S3_BUCKET` | `dataflow-bench` | Read by `_s3_bench_bucket()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `COHERE_API_KEY` | unset | Read by `resolve_provider()` | `apps/api/services/embedding_providers/config.py` |
| `CORS_EXTRA_ORIGINS` | `""` | Extra comma-separated origins added to `CORS_ORIGINS`. | `apps/api/services/platform_config.py` |
| `CORS_ORIGINS` | computed | Comma-separated browser origins allowed to call the API. | `apps/api/services/platform_config.py` |
| `CORS_ORIGIN_REGEX` | `""` | An origin pattern set by the operator. There is no default pattern. | `apps/api/services/platform_config.py` |
| `DATABASE_URL` | `postgresql://dataflow:dataflow@localhost:5432/dataflow` | Read at module level: Runtime configuration for Datawrap API | `apps/api/services/config.py` |
| `DYNAMODB_LOCAL_ACCESS_KEY` | set in code (not shown) | Read by `_dynamodb_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `DYNAMODB_LOCAL_HOST` | `127.0.0.1` | Read by `_dynamodb_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `DYNAMODB_LOCAL_PORT` | `8000` | Read by `_dynamodb_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `DYNAMODB_LOCAL_SECRET_KEY` | set in code (not shown) | Read by `_dynamodb_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `FAKESNOW_DB_PATH` | unset | Read by `_fakesnow_db_path()`: Snowflake connection helper | `apps/api/connectors/snowflake_conn.py` |
| `FAKE_GCS_HOST` | `127.0.0.1` | Read by `_gcs_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `FAKE_GCS_PORT` | `4443` | Read by `_gcs_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `HOSTNAME` | unset | Read by `_service_instance_id()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed (and other readers) | `apps/api/services/tracing.py`, `apps/api/services/worker_leases.py` +1 more |
| `MAILGUN_API_KEY` | `""` | Read by `email_provider_config()`: Managed transactional email provider config for SaaS notifications | `apps/api/services/platform_config.py` |
| `MAILGUN_DOMAIN` | `""` | Read by `email_provider_config()`: Managed transactional email provider config for SaaS notifications | `apps/api/services/platform_config.py` |
| `MAILGUN_REGION` | `us` | Read by `email_provider_config()`: Managed transactional email provider config for SaaS notifications | `apps/api/services/platform_config.py` |
| `MINIO_ACCESS_KEY` | set in code (not shown) | Read at module level: Runtime configuration for Datawrap API (and other readers) | `apps/api/services/config.py`, `apps/api/services/object_store.py` |
| `MINIO_BUCKET` | `dataflow-staging` | Read at module level: Runtime configuration for Datawrap API | `apps/api/services/config.py` |
| `MINIO_ENDPOINT` | `localhost:9000` | Read at module level: Runtime configuration for Datawrap API (and other readers) | `apps/api/services/config.py`, `apps/api/services/object_store.py` |
| `MINIO_HOST` | `127.0.0.1` | Read by `_require_minio()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `MINIO_PORT` | `9000` | Read by `_require_minio()`: PostgreSQL → MySQL volume run through the production stream engine (and other readers) | `apps/api/services/million_row_bench.py` |
| `MINIO_SECRET` | set in code (not shown) | Read by `_s3_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `MINIO_SECRET_KEY` | set in code (not shown) | Read at module level: Runtime configuration for Datawrap API (and other readers) | `apps/api/services/config.py`, `apps/api/services/object_store.py` |
| `MINIO_SECURE` | `0` / `False` | Read at module level: Runtime configuration for Datawrap API (and other readers) | `apps/api/services/config.py`, `apps/api/services/object_store.py` |
| `MINIO_USER` | `dataflow` | Read by `_s3_local_cfg()`: PostgreSQL → MySQL volume run through the production stream engine | `apps/api/services/million_row_bench.py` |
| `MONGODB_URI` | `""` / `mongodb://localhost:27017/` | MongoDB connection string. Checked first, before `MONGO_URL`, `MONGO_PRIVATE_URL`, `MONGODB_URL` and `MONGO_PUBLIC_URL`. | `apps/api/services/connector_store.py`, `apps/api/services/control_plane_store.py` +4 more |
| `MONGODB_URL` | `""` | Read by `_explicit_mongo_uri()`: Shared Mongo collections for multi-replica Railway / HA control plane (and other readers) | `apps/api/services/control_plane_store.py`, `apps/api/services/egress_guard.py` +1 more |
| `MONGO_HOST` | `127.0.0.1` | Read by `_control_plane_names()`: Server-side egress policy — one deny set for webhooks and connector dials (and other readers) | `apps/api/services/egress_guard.py`, `apps/api/services/million_row_proof.py` |
| `MONGO_PORT` | `27017` | Read by `mongo_reachable()`: Million-row Execute proof — one owner for conservation + OLTP discovery | `apps/api/services/million_row_proof.py` |
| `MONGO_PRIVATE_URL` | `""` | Read by `_resolve_backend()`: Pick the persistence backend (and other readers) | `apps/api/services/connector_store.py`, `apps/api/services/control_plane_store.py` +1 more |
| `MONGO_PUBLIC_URL` | `""` | Read by `_resolve_backend()`: Pick the persistence backend (and other readers) | `apps/api/services/connector_store.py`, `apps/api/services/control_plane_store.py` +1 more |
| `MONGO_URI` | unset | Read by `_control_plane_names()`: Server-side egress policy — one deny set for webhooks and connector dials | `apps/api/services/egress_guard.py` |
| `MONGO_URL` | `""` / required | Read by `_resolve_backend()`: Pick the persistence backend (and other readers) | `apps/api/services/connector_store.py`, `apps/api/services/control_plane_store.py` +2 more |
| `MYSQL_DATABASE` | `dataflow` | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `MYSQL_HOST` | `127.0.0.1` | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `MYSQL_PASSWORD` | set in code (not shown) | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `MYSQL_PORT` | `3306` / required | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None (and other readers) | `apps/api/services/million_row_proof.py` |
| `MYSQL_USER` | `dataflow` | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `OLLAMA_BASE_URL` | required | Read by `resolve_ollama_base_url()`: Persisted workspace integrations — SSO, AI provider keys, API keys | `apps/api/services/integrations_store.py` |
| `OPENAI_API_KEY` | `""` | Read by `resolve_provider()` (and other readers) | `apps/api/services/embedding_providers/config.py`, `apps/api/services/vectorization.py` |
| `OPENAI_MODEL` | `gpt-4o-mini` | Read by `__init__()`: Datawrap — LLM Provider Abstraction | `apps/api/src/ai/llm/provider.py` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Read by `otlp_endpoint()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | unset | Read by `otlp_protocol()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `OTEL_SERVICE_INSTANCE_ID` | unset | Read by `_service_instance_id()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `OTEL_SERVICE_VERSION` | unset | Read by `telemetry_resource()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed | `apps/api/services/tracing.py` |
| `PGDATABASE` | `dataflow` | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `PGHOST` | `127.0.0.1` | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `PGPASSWORD` | set in code (not shown) | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `PGPORT` | `5432` / required | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None (and other readers) | `apps/api/services/million_row_proof.py` |
| `PGUSER` | `dataflow` | Read by `discover_oltp_pair()`: Return (pg, mysql) configs for the first reachable pair, or None | `apps/api/services/million_row_proof.py` |
| `PORT` | `8000` | Read at module level: Datawrap — API Server | `apps/api/src/main.py` |
| `RAILWAY_DEPLOYMENT_ID` | unset | Read by `is_railway_deployed()`: True only inside a deployed Railway service (and other readers) | `apps/api/services/platform_config.py`, `apps/api/services/tracing.py` |
| `RAILWAY_ENVIRONMENT` | unset | Read by `is_running_on_railway()`: True when this API process is inside a Railway deployment (and other readers) | `apps/api/connectors/sql_dsn.py`, `apps/api/services/platform_config.py` |
| `RAILWAY_ENVIRONMENT_NAME` | unset | Read by `is_railway_deployed()`: True only inside a deployed Railway service (and other readers) | `apps/api/services/platform_config.py`, `apps/api/services/tracing.py` |
| `RAILWAY_GIT_COMMIT_SHA` | unset | Read by `telemetry_resource()`: OpenTelemetry tracing for Datawrap transfers — optional, fail-closed (and other readers) | `apps/api/services/tracing.py`, `apps/api/src/main.py` |
| `RAILWAY_PUBLIC_DOMAIN` | `""` | Read by `api_service_url()`: This API service's own base URL, for protocol metadata only (MCP OAuth) (and other readers) | `apps/api/services/platform_config.py` |
| `RAILWAY_REPLICA_ID` | unset | Read by `is_railway_deployed()`: True only inside a deployed Railway service (and other readers) | `apps/api/services/platform_config.py`, `apps/api/services/tracing.py` +1 more |
| `RAILWAY_SERVICE_ID` | unset | Read by `is_running_on_railway()`: True when this API process is inside a Railway deployment (and other readers) | `apps/api/connectors/sql_dsn.py`, `apps/api/services/platform_config.py` |
| `RAILWAY_VOLUME_MOUNT_PATH` | `""` | Read by `_railway_volume_root()`: Railway persistent volume mount (set in dashboard) | `apps/api/services/platform_config.py` |
| `SENDGRID_API_KEY` | `""` | Read by `email_provider_config()`: Managed transactional email provider config for SaaS notifications | `apps/api/services/platform_config.py` |
| `SOURCE_COMMIT` | unset | Read by `_build_commit()`: Best-effort deployed git SHA for health reporting | `apps/api/src/main.py` |

### Reads the generator cannot name

These functions read a variable whose name is built at run time, so the name does not appear in the tables above.

- `apps/api/services/integrations_store.py` — `apply_integrations_to_env()`
- `apps/api/services/integrations_store.py` — `provider_key_state()`
- `apps/api/services/integrations_store.py` — `resolve_provider_api_key()`
- `apps/api/services/integrations_store.py` — `resolve_provider_model()`
- `apps/cli/dataflow_cli/main.py` — `_env()`

<!-- END GENERATED: config-flags -->

## Not covered

- Variables read only by shell scripts, Dockerfiles, the web container
  (`DATAWRAP_API_BASE` and the `VITE_*` build variables), Helm templates or
  Terraform. They are named in `docs/operate/deploy.md` where they matter,
  except the graceful-shutdown bound, which is described above.
- Variables read only by tests, benchmarks under `apps/api/benchmarks` and
  one-off scripts under `apps/api/scripts`.
- Per-step stall thresholds. `JOB_STEP_STALL_<STEP>_SEC` is built from the step
  name at run time (`apps/api/services/job_watchdog.py`), so individual names
  do not appear in the table.
- Valid ranges. The table gives the default, not the accepted range, unless the
  description says so.

## Verified against

- `apps/api/services/brand_env.py` (prefix rule)
- `apps/api/services/platform_config.py` (production detection and checks)
- `apps/api/src/worker_main.py`, `apps/api/src/scheduler_main.py` (flags set at start)
- `apps/cli/dataflow_cli/main.py` (`_env()`, the command-line tool's prefix rule)
- `scripts/start-api.sh` (graceful-shutdown bound)
- `docs/tools/gen_config_reference.py` (how the table is built)
- `docs/tools/config_descriptions.json` (checked descriptions)
