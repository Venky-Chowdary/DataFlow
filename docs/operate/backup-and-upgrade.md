# Backup and upgrade

This page says where Datawrap keeps its own state, what you must back up, and
what to check when you upgrade.

Datawrap has **no backup, restore or migration tool of its own**. You back up
its stores with the tools of the database and the volume they live on.

## Where state lives

Datawrap moves your data between systems. That data is not stored by Datawrap,
except in the places listed under "Copies of your data" below. This section is
about the service's own records.

### MongoDB

Database name: `datatransfer` (fixed in code, whatever database the connection
string names).

When MongoDB is connected, these records live there:

| What | Collection |
|---|---|
| Transfer jobs, their evidence and Trust Report state | `transfer_jobs` |
| The job queue, claims and worker leases | `transfer_job_queue`, `job_claims`, `worker_leases` |
| Job checkpoints | `job_checkpoints` |
| Pipeline schedules and their locks | `pipeline_schedules`, `schedule_locks` |
| Saved connectors (credentials encrypted) | `connectors` |
| Data contracts | `contracts`, `contract_breakers` |
| Audit events and audit signing keys | `audit_events`, `audit_chain_keys` |
| Incremental and CDC positions | `sync_cursors` |
| Accounts, workspaces, memberships, sessions, invitations, MFA, SCIM, SSO state | several account collections |
| Control-plane stores: workspace settings, customer-managed key records, resource ACLs, notification channels, Validate run history, transform drafts, CDC mapping reviews, the MCP call log, audit tip anchors | `control_plane_docs`, `control_plane_records`, `control_plane_items` |

Collection names were read from the code for the stores named; the last row
covers stores that write through a shared helper and were not listed one by
one.

Without a MongoDB connection the same records fall back to files in the data
directory. That mode is for a single developer machine.

### The data directory

`DATAWRAP_DATA_DIR`. The control-plane files below are the store only on a
single node without MongoDB (`DATAWRAP_CONTROL_STORE_BACKEND` resolves to
`file`). When MongoDB is connected, or `DATAWRAP_MULTI_REPLICA=1`, they live in
the control-plane collections above; the first process to reach an empty
collection imports its local file once, and the file is then left as it was.
`transfer_plans.json` and `pilot_results.json` have no database path and stay
files in every mode.

| File | What it holds |
|---|---|
| `tenants.json` | Workspace settings: custom domain, data region, session timeout, IP and egress allowlists, require-SSO, MFA required |
| `byok_keys.json` | Wrapped customer-managed keys. **Losing this file makes connector secrets wrapped with those keys unreadable.** |
| `resource_acls.jsonl` | Per-resource access grants |
| `notifications.json` | Notification channels |
| `transfer_plans.json` | Saved transfer plans |
| `pilot_results.json` | Pilot's recent query results (rows sealed with the connector-secret key, kept for about an hour) |
| `preflight_runs.jsonl` | Validate run history |
| `shape_drafts.json` | Transform drafts |
| `audit_tip_anchors.jsonl` | Audit chain anchors |
| `audit_chain.key` | The audit signing key, when no key is configured and no database is connected |
| `mcp_invocations.jsonl` | MCP call log |
| `cdc_mapping_reviews.jsonl` | CDC schema change reviews |
| `sqlite/`, `duckdb/` | Files of SQLite and DuckDB connectors, one area per workspace under `_workspaces/<workspace id>/`. `_workspaces/.partitioned_at` records when the volume was partitioned; it decides which older files can be moved to their workspace (see [security.md](security.md#sqlite-and-duckdb-volumes)). Keep it. |

Other files in the directory belong to stores that also have a database path
(for example `integrations.json`, `usage_metering.json`,
`transform_recipes.json`, `schema_registry_*.json`, `proofs/`,
`pilot_acks.json`), or are caches that can be rebuilt (`embedding_cache.sqlite3`,
`vector_store/`). Which copy is authoritative for each of those was not traced
for this page.

**Treat the whole directory as state and back all of it up.**

### Other locations

| Location | Setting | Holds |
|---|---|---|
| Upload directory | `DATAWRAP_UPLOAD_DIR` | Uploaded source files. A job that reads an upload cannot be resumed if the file is gone. |
| Vector store directory | `DATAWRAP_VECTOR_STORE_DIR` | Pilot's local search index |
| Exports | `DATAWRAP_EXPORTS_DIR` or the export bucket | Files produced by file-export transfers. On the API host, one folder per workspace under `_workspaces/<workspace id>/`; the top folder holds unscoped and older files. |
| Object storage | `S3_*`, `BLOB_ROOT`, `EXPORT_*` settings | Staged uploads and exports, when configured |
| Redis | `DATAWRAP_CDC_LEASE_REDIS_URL` | CDC source leases only. Nothing that needs a backup. |

### Copies of your data

Datawrap is not a data store, but copies of transferred data exist in:

- uploaded source files and staged objects;
- export files;
- quarantined rows on a job and in the dead-letter queue;
- journals, when journal capture is on (`DATAWRAP_JOURNAL_CAPTURE`);
- spill files used during large transfers.

Apply the same retention and access rules to these as to the source data.

## What to back up

| Priority | What | Why |
|---|---|---|
| 1 | The secrets: `AUTH_SECRET`, `SECRETS_KEY`, `AUDIT_HMAC_KEY` and any retired audit keys | Without `SECRETS_KEY`, stored connector credentials cannot be decrypted. Without the audit keys, old audit records cannot be verified. Keep them in your secret manager, not only on the host. |
| 2 | MongoDB, database `datatransfer` | Jobs, schedules, connectors, audit events, accounts, CDC and incremental positions. |
| 3 | The data directory | Workspace settings, customer-managed keys, plans, ACLs, anchors. |
| 4 | Upload and export locations | Only if you need to resume jobs from uploaded files or keep exports. |

Take the MongoDB backup and the data directory backup as close together as you
can. They refer to each other (a job names a plan; a connector secret may be
wrapped by a key in `byok_keys.json`), and nothing reconciles them after a
restore.

## Restoring

There is no restore procedure in the product. After restoring MongoDB and the
data directory from your own backups:

1. Start with the same `AUTH_SECRET`, `SECRETS_KEY` and audit keys as before.
2. Check `GET /api/v1/audit/verify`. Records signed with a key you no longer
   have report as `unverifiable_key_lost`, not as tampering.
3. Test each saved connector. A credential that cannot be decrypted fails here.
4. Jobs that were running at backup time have no worker. The orphan sweep
   resumes or settles them (see [runbooks.md](runbooks.md)).
5. Check CDC pipelines. A restored position may be older than what the source
   still keeps. If the source has purged that part of its log, clear the
   position so the next run takes a fresh snapshot.

CDC applies changes as keyed upserts, so replaying from an older position is
designed to converge on keyed routes. On an append route it can write rows
twice. Neither case was tested for this page.

## Upgrading

There are no versioned schema migrations. Stores are brought up to date by
backfill code that runs at start (for example
`services/legacy_workspace_backfill.py` and the schedule file import in
`src/scheduler_main.py`). That means:

- **Downgrade is not supported by any tool.** Take a backup before upgrading.
- Upgrade all processes (API, workers, scheduler) to the same build. They
  share the queue and the job records.
- On SIGTERM a worker stops claiming work and drains for up to
  `DATAWRAP_WORKER_DRAIN_SECONDS` (default 25); a transfer still running is
  stopped at its next batch and requeued. A worker killed sooner leaves its
  jobs to the orphan sweep, which resumes them from their checkpoint. See
  [deploy.md](deploy.md#stopping-a-worker).
- The API started by `scripts/start-api.sh` waits at most
  `DATAWRAP_GRACEFUL_SHUTDOWN_SECONDS` (default 10) for open connections on
  SIGTERM, then stops its background work. Background training stops within
  about a second; a run cut short writes no partial output.

### Things to check after an upgrade

- **Approved plans and capability changes.** An approved plan is bound to the
  capability profile of its source and destination. When a release changes a
  connector's declared capabilities, a plan approved before the upgrade is
  refused at run time with "content_hash does not match approved hash". Run
  Validate again and re-approve. The release that made the connector registry
  a single source changed the profile of Kafka, pgvector, Qdrant and email
  endpoints, and of sources saved as TimescaleDB, CockroachDB, Citus,
  Greenplum or YugabyteDB.
- **Session timeout.** A workspace's saved session timeout is enforced. A
  workspace whose settings were saved before that became the default keeps
  the value it was stored with (not enforced) until an administrator saves the
  timeout again.
- **Object names in destinations.** Tables and columns that Datawrap creates
  in your systems keep their existing names. New ones take the new product
  name only when `DATAWRAP_BRAND_OBJECT_NAMES=1`. Existing objects are never
  renamed, and both names are recognised.
- **Flag names.** `DATAFLOW_` names keep working. See the prefix rule in
  [configuration.md](configuration.md).
- **API keys.** Keys starting `dfk_` stay valid.


#### Workspace isolation and residency (PR 403)

Take a backup of the data directory and the exports folder before this
upgrade: the API moves files in both at its first start.

- **A workspace with a data region is now fenced on every path.** Job
  creation from any source (REST, schedules, sync-now, retries, MCP, Pilot),
  every run start (resume, retry, worker claim, orphan sweep), quarantine
  replay, CDC drift repair and query export are all checked. Before, only two
  transfer routes checked S3 and DynamoDB destinations. Expect scheduled runs
  to such destinations to be refused if their region cannot be proven.
- **No region is allowed implicitly.** `us-east-1` is fenced like any other,
  and a destination whose region cannot be proven is refused. An IP address,
  a custom domain, an S3-compatible endpoint, GCS, BigQuery, ADLS, an Iceberg
  warehouse or a SaaS API needs a platform administrator's region verification
  (`POST /api/v1/connectors/saved/{id}/region-verification`). Any later edit
  to the connector's address or credentials clears that verification.
- **Set `DATAWRAP_DEFAULT_REGION`** if a fenced workspace writes to a local
  file, a file export, SQLite, DuckDB or a database on loopback. Without it
  those destinations are refused there.
- **Connection options are deny by default in a fenced workspace.** A saved
  connector or request whose connection string or `extra` carries a key not
  on its family's allowlist is refused with a 422 naming the key. Remove the
  key, or ask a platform administrator. See
  [security.md](security.md#connection-options-in-a-fenced-workspace-deny-by-default).
- **A tenant record with no workspace** fences every workspace without a
  region of its own. If several such records name different regions, every
  destination is refused until each record is assigned to its workspace.
- **S3 writes in a fenced workspace read the bucket's region first.** The
  credentials need `s3:GetBucketLocation` (or permission for `HeadBucket`);
  a bucket whose region cannot be read is refused.
- **Only a platform administrator can set, change or clear a data region.** A
  `data_region` sent in a request no longer changes anything.
- **Exports move to per-workspace folders.** At start, in the background, each
  file in the flat exports folder whose owner the job records prove moves to
  `_workspaces/<workspace id>/`. A recurring append export that runs first
  moves its own file and continues it; when it cannot prove ownership it
  starts a new file and its run summary says why. Files with no provable owner stay where they are and can be
  downloaded only by a platform administrator. Scripts that read the exports
  folder directly must look in the workspace folders.
- **SQLite and DuckDB files move to per-workspace areas.** At first start the
  API writes the partition marker, then moves each flat file that only one
  workspace's connectors (saved before the marker) name. A file named by
  connectors of two workspaces, or by none, stays in the flat area and no
  workspace can open it any more. Connectors keep their stored path; it
  resolves to the moved file.
- **Ids from another workspace are 404.** A schedule, transform, export,
  transfer or access grant whose request body names another workspace's
  connector, contract, recipe, plan, job or schedule is refused with 404, as
  if the id did not exist.
- **Query playground.** SQL Server queries must parse as one `SELECT`; an
  unquoted name that is a T-SQL reserved keyword must be quoted (`[open]`).
  DuckDB path and URL readers and MotherDuck databases are refused. More
  functions with side effects are refused on every dialect.
- **SSO auto-provisioning with no allowed domains admits no new identity.**
  Before, it admitted every identity the provider returned. Set
  `DATAWRAP_SSO_ALLOWED_DOMAINS` if you rely on auto-provisioning. The API
  logs a warning at start while the list is empty.
- **Audit anchor receipts.** With `DATAWRAP_AUDIT_ANCHOR_PROVIDER` set to a
  provider other than the local file, receipts are now labelled as the local
  store and carry `requested_provider`, and the API logs a warning. The write
  went to the local file before too; only the label changes.
- **Flag names.** 64 flags that were read only under their `DATAFLOW_` name
  now accept `DATAWRAP_` too, which wins. A `DATAWRAP_` value that was ignored
  before takes effect now. An empty `DATAWRAP_` value no longer hides a set
  `DATAFLOW_` one, and the API logs a warning at start for each flag set to
  two different values. See [configuration.md](configuration.md#the-prefix-rule).
- **Browser headers.** The web container now sends a Content-Security-Policy.
  If you changed `apps/web/index.html`'s inline script or the nginx files,
  update the hash (see [security.md](security.md#browser-security-headers)).

#### Schedules (PR 409)

- **Run now can answer 409 `run_still_recording`** with a `Retry-After`
  header when the previous run's outcome is still being written. Retry after
  the wait. The scheduler defers that one schedule and carries on with the
  others.
- **Failure classification.** A 403 or 429 in an error message counts as an
  HTTP status only after words such as `http`, `status`, `code` or `error`, at
  the start of the message, or in a known shape (`403 Forbidden`,
  `429 Too Many Requests`). Numbers in ports, row counts, positions or ids no
  longer make a failure look like a credential refusal or rate limiting, so
  some failures are now retried, or not, differently.
- A finished run is no longer visible without its outcome, and a parked retry
  is not counted as a later run.

#### Incremental syncs (PR 407)

- **Overlap re-read.** Non-PostgreSQL SQL sources with a key re-read the last
  300 s (temporal cursor) or 1,000 values (integer cursor) on every bounded
  run; re-read rows collapse on the row version, so nothing is duplicated.
  `cursor_overlap_seconds` / `cursor_overlap_values` (per stream) or
  `DATAWRAP_CURSOR_OVERLAP_SECONDS` / `_VALUES` (deployment) change it; `0`
  turns it off and the route then says it is not guaranteed.
- **Refused cursors.** A schedule whose text cursor holds unpadded numbers,
  mixed-offset or mixed-precision timestamps, non-UTC text timestamps where
  the watermark is UTC, day/month or month/day dates, or ISO date-times mixing
  `T` and a space now fails with the reason (before, rows could be skipped
  silently). A file date cursor with a cell that is not a date (`N/A`) is
  refused before the write; set the file's date order (`date_locale=DMY` or
  `MDY`), clean the cells, or run a full refresh.
- **First run after upgrade.** A PostgreSQL route records a snapshot bound and
  is commit-safe from the next run on. Narrowing to the in-progress list needs
  PostgreSQL 16; older servers judge on the horizon (correct, re-checks more
  rows) and the evidence says so.
- **Source load.** The PostgreSQL late-commit check holds one read-only
  snapshot on the source for at most `DATAWRAP_COMMIT_SAFE_WALK_SECONDS`
  (default 30); probe statements time out after
  `DATAWRAP_COMMIT_SAFE_PROBE_TIMEOUT_MS` (default 5000).
- **Grades and entries.** A run that recovers a late row can grade `partial`;
  quiet PostgreSQL runs rewrite the route's cursor entry (the bound is new each
  run). `key_census_every` is off by default; when on, a missing key fails the
  run. Kill switch: `DATAWRAP_COMMIT_SAFE_WINDOW=0`.

#### Enterprise hardening (PR 430)

New defaults. Each turns a protection on that was off or audit-only before.

- **Connector egress is enforced at every checkpoint.** Saving a connector
  with a new host, reading, writing, introspecting and probing now refuse
  private, loopback and carrier-grade NAT addresses that the workspace
  allowlist does not cover. Before, these checkpoints only wrote an
  `egress.would_block` audit event. A saved connector that points at such an
  address keeps saving but fails on Test and on its next run. Before
  upgrading, list the affected connectors with the read-only
  `python apps/api/scripts/audit_egress_impact.py`, then have a platform
  administrator allowlist the ranges that are meant to be reachable.
  `DATAWRAP_EGRESS_ENFORCE_NEW=0` restores audit-only mode for the newer
  checkpoints while you do this.
- **Only a platform administrator can widen a workspace's egress allowlist**
  (SEC-16). A workspace administrator who adds a range now gets 403; they can
  still narrow or clear it.
- **Local files are confined in every environment.** SQLite and DuckDB
  connectors open files only in the data folders, uploads, the workspace's
  export folder or `DATAWRAP_SOURCE_PATH_ROOTS`; before, this applied in
  production only. A relative SQLite or DuckDB name is now placed under the
  data folder. Before upgrading a staging or on-premises install, run the
  read-only `python apps/api/scripts/audit_sqlite_paths.py --copy-plan` to see
  which files to move. `DATAWRAP_CONFINE_LOCAL_FILES=0` turns the rule off,
  with a warning at start.
- **Run needs the Validate approval it was given.** Running a plan that was
  validated without that Validate's approval answers 409
  `validate_approval_required`; an approval for another destination table or
  set of streams answers 409 `validate_approval_destination`. Run Validate
  again for plans approved before the upgrade if they are refused. The web
  app's Validate-then-Run flow is unchanged.
- **Sign-in.** Lockouts are per account and address, and an account is never
  locked as a whole. Set `DATAWRAP_AUTH_LOCKOUT_REDIS_URL` (or `REDIS_URL`) so
  lockouts are shared by every replica. An administrator's password reset now
  ends all of that person's sessions.
- **Request bodies** other than uploads are limited to 1 MB without
  credentials and 8 MB otherwise (413). See
  [security.md](security.md#rate-limits).
- **CORS.** With sign-in required, no localhost origin is trusted by default.
  Set `CORS_ORIGINS` (or the web domain) explicitly.

Changed shapes:

- **Error responses carry a support reference.** A classified failure answers
  with `{"error": {"code", "title", "message", "action", "object",
  "retriable", "reference", "driver"}, "detail": "... Reference: <id>"}`.
  `detail` is still a string. Clients that matched on driver text in `detail`
  should switch to `error.code`. See
  [../use/api.md](../use/api.md#errors).
- **Connector tiers.** "Certified" now needs a green CI run that proves the
  engine; other working connectors show "Implemented, not yet proven", and
  wire-compatible brands (MariaDB, DocumentDB, Cosmos DB, hosted Redis) are
  Planned until proven under their own name. No connector stopped working.
  See [../reference/connectors.md](../reference/connectors.md).

#### Training and egress (PR 410)

- **`DATAWRAP_TRAINING`.** Set, only `1`, `true`, `yes` or `on` turns
  background training on; any other value, an empty one included, leaves it
  off. Before, anything but `off`, `0` or `false` turned it on. Unset is
  unchanged: on outside production, off in production.
- **AWS region labels.** For S3, DynamoDB and Kinesis connectors that keep
  the signing region in `host` (for example `us-east-1`), the egress guard no
  longer looks the label up in DNS when the client dials another endpoint. A
  label that the client does dial (a dot-less host plus a port) is still
  checked.

## Not covered

- Tested restore times or recovery objectives. No restore has been rehearsed
  for this page.
- Backup of the systems Datawrap connects to.
- Compatibility of managed MongoDB-compatible services with the features the
  product uses.
- A complete list of account-related collections.
- Release-by-release upgrade notes. See [../CHANGELOG.md](../CHANGELOG.md) for
  what merged recently.

## Verified against

- `apps/api/services/mongodb_service.py` (database name, job and queue collections)
- `apps/api/services/metadata_backend.py` and the stores that import it
- `apps/api/services/platform_config.py` (`data_dir`, `upload_dir`, `vector_store_dir`)
- Every `data_dir() / "<file>"` reference under `apps/api/services` and `apps/api/src`
- `apps/api/services/audit_chain_keys.py`, `apps/api/services/audit_log.py`
- `apps/api/services/secret_vault.py`, `apps/api/services/byok_key_manager.py`
- `apps/api/services/tenant_store.py` (session timeout note)
- `apps/api/services/brand.py` (object names flag)
- `apps/api/services/decision_kernel/execute_gate.py`, `apps/api/services/decision_kernel/models.py`
- `apps/api/services/orphan_jobs.py`, `apps/api/services/checkpoint_service.py`
- `apps/api/services/integrations_store.py` (API key prefixes)
- `apps/api/services/data_residency.py`, `apps/api/services/residency_addressing.py`,
  `apps/api/connectors/s3_residency.py` (residency upgrade notes)
- `apps/api/services/export_migration.py`, `apps/api/services/embedded_volume.py`
  (file moves at start)
- `apps/api/services/sso_authorization.py`, `apps/api/services/audit_anchor.py`,
  `apps/api/services/brand_env.py`
- `apps/api/services/schedule_runner.py`, `apps/api/services/failure_retry_policy.py`
- `apps/api/src/ai/training/training_scheduler.py`,
  `apps/api/src/ai/training/training_runtime.py`, `apps/api/services/egress_guard.py`
- `scripts/start-api.sh` (graceful-shutdown bound)
- `apps/api/scripts/audit_egress_impact.py`, `apps/api/scripts/audit_sqlite_paths.py`,
  `apps/api/services/local_file_policy.py` (PR 430 defaults)
- `apps/api/src/routers/workspace_router.py` (`_require_egress_admin`),
  `apps/api/services/validate_approvals.py`, `apps/api/services/auth_rate_limit.py`,
  `apps/api/src/routers/team_router.py` (password reset), `apps/api/services/safe_errors.py`
- `apps/api/services/shared_doc_store.py`, `apps/api/src/ai/copilot/result_store.py`,
  `apps/api/services/worker_fleet.py` (worker drain)
