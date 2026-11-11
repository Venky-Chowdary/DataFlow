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

Collection names were read from the code for the stores named; the last row
covers stores that write through a shared helper and were not listed one by
one.

Without a MongoDB connection the same records fall back to files in the data
directory. That mode is for a single developer machine.

### The data directory

`DATAWRAP_DATA_DIR`. The modules behind these files have no database path, so
the records are files here even when MongoDB is connected:

| File | What it holds |
|---|---|
| `tenants.json` | Workspace settings: custom domain, data region, session timeout, IP allowlist, require-SSO, MFA required |
| `byok_keys.json` | Wrapped customer-managed keys. **Losing this file makes connector secrets wrapped with those keys unreadable.** |
| `resource_acls.jsonl` | Per-resource access grants |
| `notifications.json` | Notification channels |
| `transfer_plans.json` | Saved transfer plans |
| `preflight_runs.jsonl` | Validate run history |
| `shape_drafts.json` | Transform drafts |
| `audit_tip_anchors.jsonl` | Audit chain anchors |
| `audit_chain.key` | The audit signing key, when no key is configured and no database is connected |
| `mcp_invocations.jsonl` | MCP call log |
| `cdc_mapping_reviews.jsonl` | CDC schema change reviews |

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
| Exports | `DATAWRAP_EXPORTS_DIR` or the export bucket | Files produced by file-export transfers |
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
- Workers have no graceful shutdown. Stop them when no job is running, or
  expect interrupted jobs to wait for the orphan sweep and then resume from
  their checkpoint.

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
