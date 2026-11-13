# Security model

This page describes how Datawrap authenticates callers, what each role may do,
how workspaces are kept apart, how secrets, outbound connections and data
residency are handled, and how the audit chain works. Each section says plainly what is
enforced today and what is not.

Nothing here is a compliance certification, and the repository contains no
certification evidence.

## Signing in

| Method | How it works |
|---|---|
| Email and password | `POST /api/v1/auth/login` returns a session token and its expiry. Passwords are stored as bcrypt hashes and upgraded to argon2id on a successful login. |
| Browser session | The web app can use an HttpOnly cookie (`__Host-dw_session`) with a second cookie (`dw_csrf`) that the page must echo in the `x-csrf-token` header on any request that changes state. |
| Second factor | Time-based one-time codes (TOTP) with recovery codes. A workspace can require it. By default, people who sign in through SSO are exempt; set `DATAWRAP_MFA_SSO_EXEMPT=0` to hold them to it too. |
| Single sign-on | Three provider types: `oidc`, `azure_ad` and `saml`. |
| API key | A key starting `dwk_` (or the earlier `dfk_`), sent as a bearer token. Only a SHA-256 hash of the key is stored. A key carries its own role and is never allowed more than the person who created it holds today. |
| Service account, SCIM token | Created under `/api/v1/iam/`. A SCIM token is an API key limited to the `scim.provision` scope. |

Session tokens last `DATAWRAP_TOKEN_TTL_SEC` seconds (default one day). A
person can list and revoke their own sessions under `/api/v1/auth/sessions`.

### What to know about SSO

- An SSO identity is accepted when the account already exists, or when its
  email domain is in `DATAWRAP_SSO_ALLOWED_DOMAINS`.
- With `DATAWRAP_SSO_AUTO_PROVISION=1`, a new identity is given an account
  only when its email domain is in `DATAWRAP_SSO_ALLOWED_DOMAINS`. With the
  list empty, no new identity is provisioned: sign-in answers 403 naming the
  setting, people who already have an account still sign in, and the API logs
  a warning at start.
- A group pushed through SCIM can be mapped to a role in one workspace
  (`GET` and `PUT /api/v1/iam/scim/group-mappings`). A caller cannot map a
  group to a role higher than their own in that workspace.
- SSO sign-in itself authorises by account or email domain. This page does
  not claim that group claims in an SSO assertion change a person's role.

See [../SCIM.md](../SCIM.md) for provisioning.

## Roles and permissions

There are four roles. Every API route is checked against one permission, and
a route with no rule is refused (`DATAFLOW_RBAC_UNRULED_ROUTES=deny` is the
default). `apps/api/tests/test_rbac_route_inventory.py` keeps the route list
and the rules in step.

| Permission | viewer | operator | editor | admin |
|---|---|---|---|---|
| `job.read` | yes | yes | yes | yes |
| `job.run` | | yes | yes | yes |
| `job.plan` | | | yes | yes |
| `job.manage` (cancel, retry, resume, sign off) | | yes | yes | yes |
| `connector.read` | yes | yes | yes | yes |
| `connector.write` | | | yes | yes |
| `connector.delete` | | | | yes |
| `schedule.read` | yes | yes | yes | yes |
| `schedule.manage` | | | yes | yes |
| `schedule.authorize` | | | | yes |
| `audit.read` | yes | yes | yes | yes |
| `workspace.read` | yes | yes | yes | yes |
| `workspace.manage` | | | | yes |
| `member.invite` | | | | yes |
| `ai.use` | yes | yes | yes | yes |
| `query.use` | yes | yes | yes | yes |
| `iam.manage` | | | | platform admin |
| `scim.provision` | | | | platform admin |
| `account.self` | yes | yes | yes | yes |

Points that surprise people:

- A viewer may use Pilot and the query playground. Pilot checks each tool it
  runs against the same permission as the matching API route, so a viewer can
  ask questions but cannot start or change anything.
- An operator runs and repairs pipelines but does not design them: no
  `job.plan`, no `connector.write`, no `schedule.manage`.
- An unknown or missing role name is treated as viewer.
- `iam.manage` and `scim.provision` act on the whole deployment (API keys,
  service accounts, SCIM). Only a principal whose own account role is admin
  holds them. Being admin of one workspace through membership does not grant
  them.
- Some connector types are admin-only regardless of role
  (`ADMIN_ONLY_CONNECTOR_TYPES` in `services/request_principal.py`).

Routes that need no sign-in include health, login and signup, SSO start and
callback, invitation links, the public connector catalog, the contact form,
and MCP discovery (`/api/v1/mcp/tools`, `/manifest`, `/status`). MCP discovery
lists every tool's name and schema to anyone. Running a tool needs sign-in.

## Workspace isolation

Datawrap separates data by workspace. The model has two halves:

1. The request names a workspace with the `X-Workspace-Id` header. The
   middleware works out which workspaces the caller may see and binds that as a
   filter for the request.
2. Each store filters by the bound workspace when it reads or writes.

What this means in practice:

- Jobs, schedules, audit events, usage and the dead-letter queue are filtered
  by the bound workspace. So are saved transfer plans and Validate runs, and
  repair proposals (list, get and decide). The repair proposal file keeps at
  most 200 proposals per workspace, so one workspace cannot push out
  another's.
- A platform administrator can view all workspaces (`__all__`) or rows that
  carry no workspace (`__unassigned__`).
- When the request host resolves to a tenant, the caller must hold a claim for
  that tenant. This is strict by default in production
  (`DATAWRAP_AUTH_TENANT_BIND`).
- A workspace can set an IP allowlist, a session timeout and require-SSO.

### Another workspace's id is a 404

A route that takes an id in its path, or stores or acts on an id named in its
request body, answers 404 for an id the caller's workspace does not own,
exactly as for an id that does not exist. The body ids checked when written
are connectors, jobs, contracts, schedules, transfer plans and transform
recipes, an access grant's resource, and a tenant's customer-managed key
(`byok_key_id`). A schedule's connectors, contract, recipe and replayed
schedule are checked before its key probe reads any catalog. An invitation id from another workspace is also a 404.
`services/body_id_ownership.py` holds these checks;
`apps/api/tests/test_cross_workspace_resource_matrix.py` and
`apps/api/tests/test_d154_cross_tenant_sweep.py` cover the id-addressed routes.

### Export folders

File exports written on the API host are confined to the exports folder
(`DATAWRAP_EXPORTS_DIR`, else `apps/api/exports`):

- An export made for a workspace lands in
  `<exports>/_workspaces/<workspace id>/`. A client-supplied name is confined
  to that folder, so two workspaces that both name `orders.csv` hold two files.
  The folder is chosen from the running job's workspace, else the stored
  connector's, never from the request header.
- The flat `<exports>/` folder is the unscoped one: what the single local
  operator writes with authentication off, and files exported before
  workspaces had folders.
- `GET /api/v1/transfer/download/{filename}` serves a bare file name from the
  caller's own workspace folders first. A flat file is served only to the
  workspace whose job records prove it owns it, or, when no job proves an
  owner, only to a caller whose scope covers unscoped rows (a platform
  administrator while isolation is enforced). Everything else is the same 404.
- At start, the API moves each flat file whose owner the job records prove
  into that workspace's folder, in the background (see
  [backup-and-upgrade.md](backup-and-upgrade.md#things-to-check-after-an-upgrade)).
- Workspace teardown removes the workspace's export folder.

### SQLite and DuckDB volumes

`<data dir>/sqlite` and `<data dir>/duckdb` are the only parts of the data
directory a SQLite or DuckDB connector may open. Each is partitioned the same
way: workspace W's files live in `<volume>/_workspaces/<W>/`.

- A path in the bound workspace's own area opens. A path in another
  workspace's area is refused.
- A path in the flat part of the volume opens for a workspace only when that
  workspace provably owns the file, and the file is first moved into its area.
  The proof is that every saved SQLite or DuckDB connector naming the file was
  created before the volume was partitioned and belongs to that one workspace.
  A connector saved afterwards proves nothing, so naming another workspace's
  file does not claim it. The API records the partition time at start, before
  it serves a request, and moves provably owned files in the background.
- A new name in the flat part is created in the bound workspace's area.
- Any other flat file has no provable owner. Only a caller with no workspace in
  play (system code, a platform administrator's all-workspace view, a
  deployment without workspaces) opens it, and such a caller never opens a
  workspace's area.
- With authentication off, every path opens as before.
- SQLite and DuckDB connectors never open the exports tree. A `file:` URI
  database, and `uri`, `mode`, `vfs`, `immutable` or `nolock` parameters, are
  refused, so a URI cannot name a file the path check did not see.
- A SQLite or DuckDB connector's `database` is a path and may be up to 1,024
  characters. Other engines keep the 128-character identifier limit.

### Query playground

The playground (`/api/v1/query`) runs reads only. The guards, from
`src/routers/query_router.py`:

- A statement filter on every dialect. It refuses writes, more than one
  statement, `exec`, `execute` and `waitfor` as bare words, PostgreSQL
  Unicode-escaped identifiers (`U&"..."`), Oracle database links
  (`name@link`), and a list of functions with side effects matched on their
  whole dotted name (for example `utl_http.request`, `master..xp_cmdshell`,
  file and URL readers).
- SQL Server, Azure SQL and Synapse: the text must parse as exactly one
  `SELECT` with no `INTO`, and an unquoted name that is a T-SQL reserved
  keyword is refused (`services/tsql_select.py`). The connection is closed
  without a commit.
- A read-only transaction on PostgreSQL-family, MySQL-family and Oracle
  connections, `query_only` on SQLite, and `readonly=1` on each ClickHouse
  query. This is best effort: if a dialect refuses the setting, the filter
  is the remaining guard.
- DuckDB: the connection opens with external access off. Path and URL readers,
  `DESCRIBE`, `SHOW`, `SUMMARIZE`, `PIVOT` or `UNPIVOT` over a quoted path,
  DuckDB configuration in the connection URL, and MotherDuck databases are
  refused.

## Secrets

- Connector credentials are encrypted at rest. The default backend is Fernet
  with `DATAWRAP_SECRETS_KEY`; production refuses to serve without the key.
  `DATAWRAP_SECRETS_BACKEND=aws_secretsmanager` stores them in AWS Secrets
  Manager instead.
- A workspace can bring its own key. Connector secrets are then wrapped with
  that key, and the platform key wraps the workspace key. The key records are
  kept in a local file, so every replica must share the data volume.
- Audit events are passed through a redactor before they are stored, and
  `GET /api/v1/audit/secret-scan` scans stored events for secret-shaped
  values.
- Safe error responses carry a reference id, not the exception text. See
  [runbooks.md](runbooks.md#reading-an-error-reference).
- Rotate a connector's stored secrets with
  `POST /api/v1/connectors/saved/{id}/rotate-secrets`.
- Not verified: that the redactor catches every credential shape in every log.

## Outbound connections (egress)

Connector hosts and webhook URLs are supplied by users, so the service checks
where it is about to connect.

Always refused, at every checkpoint and regardless of settings:

- cloud metadata addresses (for example `169.254.169.254`);
- link-local and multicast addresses.

Refused by default at the established checkpoints (connector Test, the SQL,
MySQL, PostgreSQL and AWS drivers, CDC, webhooks and Pilot's connector tools):

- private ranges (RFC 1918), loopback, carrier-grade NAT and IPv6
  unique-local addresses.

**Audit-only by default at the newer checkpoints** (saving or updating a
connector, transfer reads and writes, introspection, pre-counts and probes,
shared HTTP, SMTP and Mongo clients, redirects and pagination): a private or
loopback target is allowed and an `egress.would_block` audit event is written.
Set `DATAWRAP_EGRESS_ENFORCE_NEW=1` to make these checkpoints refuse.

A workspace administrator can save an allowlist of networks for on-premises
or VPC ranges. Metadata, link-local, multicast and unspecified addresses
cannot be allowlisted.

## Data residency

A workspace's data region is a fence on where its data may be written. The
code is `services/data_residency.py`, `services/residency_addressing.py`,
`services/region_verification.py` and `connectors/s3_residency.py`.

### When a workspace is fenced

- Its tenant record has a `data_region`. Any region counts, `us-east-1`
  included; no region is allowed implicitly.
- A tenant record that belongs to no workspace (a single-tenant deployment set
  up before tenants were workspace-scoped) fences every workspace that has no
  region of its own. If such records name more than one region, every
  destination is refused until each record is assigned to its workspace.
- With `DATAWRAP_RESIDENCY_STRICT=1`, every workspace that records no region
  is fenced to `DATAWRAP_DEFAULT_REGION` (`us-east-1` when unset).

Otherwise nothing on this section applies. The region comes from the tenant
record only. A `data_region` sent in a request, a schedule or a tool argument
never moves the fence; a job in a fenced workspace is tagged with the fenced
region whatever the caller sent.

Only a platform administrator can set, change or clear a workspace's data
region (`POST /api/v1/workspace/tenant`, `PATCH` and `DELETE
/api/v1/workspace/tenant/{tenant_id}`). A workspace administrator who tries
gets 403.

### Where it is checked

- When a job is created: REST, schedules, sync-now, retries, MCP and Pilot all
  go through the same submit path, so nothing out of region is queued.
- At the start of every tracked run: resume, retry, a worker claim and the
  orphan sweep each judge the address the run dials now.
- Quarantine replay, CDC drift repair (before any write) and
  `POST /api/v1/query/export`, which writes without a job (its connector and
  its local-file destination alike).
- `POST /api/v1/transfer/execute`, `POST /api/v1/transfer/run` and the query
  export answer before anything is queued: 422 for a refused connection key,
  403 for a destination outside the region. Other routes that queue a job
  answer 403 with `{"error": "data_residency", "detail": ...}`.

### What proves a destination's region

| Destination | Evidence accepted |
|---|---|
| File export, local file, SQLite, DuckDB, or a server engine on loopback | `DATAWRAP_DEFAULT_REGION`, the operator's statement of where this deployment runs. Nothing else. **With it unset, these destinations are refused in a fenced workspace.** |
| S3 and DynamoDB | The region the AWS client is given. When an endpoint URL is dialled, the region in that AWS hostname, which must agree with a `host` that names a region. |
| A host under a known provider suffix | The region label next to the suffix (for example `<x>.<region>.rds.amazonaws.com`, `<account>.<region>.snowflakecomputing.com`). Every address the destination names must prove the same region. |
| Anything else: an IP address, a custom domain, an S3-compatible endpoint, a bucket or project named without a host (GCS, BigQuery, ADLS), an Iceberg warehouse, a SaaS API | A region a platform administrator verified for the saved connector. |

A destination whose region none of these establishes is refused: an unknown
location is not an in-region one. A `region`, `extra.region` or
`extra.location` typed on the destination is not evidence; it is only used to
refuse a destination whose typed region contradicts its verified one. A
region-shaped label elsewhere in a hostname (`db.eu-west-1.example.com`)
proves nothing.

### Connection options in a fenced workspace (deny by default)

Each review of the residency check found another connection option that sends
the driver somewhere the check did not read: `Address=` and
`Failover_Partner=` in an ODBC string, ClickHouse `alt_hosts`, MySQL
`unix_socket`, an Iceberg `s3.endpoint`, the Redshift staging endpoint. So in a
fenced workspace the check no longer lists what to read. Each connector family
has an allowlist of the keys its driver may be given, and **any key not on it
is refused with a 422 that names the key**. This covers:

- the URL query of `connection_string`, and of a URL in `host`;
- an ODBC (`k=v;k=v`) or libpq (`k=v k=v`) connection string, and the string
  inside `odbc_connect`;
- the request's `extra` overrides and the saved connector's `extra`.

A connection string that cannot be read as the driver would read it (for
example an unterminated brace in an ODBC value) is refused, not skipped.

Options that never choose a place (write modes, batch sizes, procedure
parameters, schema and journal options, TLS trust and host-key pins, operator
acknowledgements) are allowed for every family. Allowlisted keys that do
choose where the driver connects are judged as addresses:

| Family (and aliases) | Address keys judged |
|---|---|
| PostgreSQL (pgvector, CockroachDB, Greenplum, YugabyteDB) | `host`, `hostaddr` |
| Redshift (Redshift Serverless) | `host`, `hostaddr`; the COPY staging bucket is judged as the S3 endpoint of its region, and a staging endpoint that is not a regional AWS S3 endpoint is refused |
| MySQL (MariaDB, TiDB, SingleStore) | `unix_socket`, which means a server on this machine |
| SQL Server | `Server`, `Data Source`, `Address`, `Addr`, `Network Address`, `Failover_Partner` |
| ClickHouse | `alt_hosts` |
| Oracle | `dsn` |
| Snowflake | `account`, `host` |
| Iceberg | `s3.endpoint`, `gcs.endpoint`, `adls.endpoint`; a non-`file://` warehouse or an ADLS account name needs a verified region |
| MongoDB, DuckDB (MotherDuck), Trino (Presto), S3-style stores (MinIO, GCS, Azure Blob, ADLS, R2), DynamoDB | none: options only |

Any other family accepts only the endpoint's own fields and the options that
never choose a place. The full lists are `FAMILIES` in
`services/residency_addressing.py`. Nothing here runs for an unfenced
workspace.

### S3 writes

For an S3 client that dials AWS in a fenced workspace, each bucket's real
region is read before the first write to it (`GetBucketLocation`, else
`HeadBucket`). A bucket outside the region, or one whose region cannot be
read, is refused. A write that S3 answers with a redirect to another region is
refused rather than re-sent. While any workspace is fenced, an S3 write that
cannot name its workspace is refused.

### Region verification

`POST /api/v1/connectors/saved/{connector_id}/region-verification` lets a
platform administrator state a saved connector's region; an empty region
withdraws it. The statement is bound to every field the driver addresses or
authenticates with (host, port, database, endpoint, connection string,
account, project, dataset and bucket extras, and, for drivers that dial no host
of their own, the credential identity, held only as a keyed hash). Changing any
of them clears it, and a run whose resolved destination differs from the
stored connector in any of them is not covered by it.

## The audit chain

Every audit event is linked to the one before it in the same workspace and
signed with an HMAC key, so a removed or altered event is detectable.

- Events are stored in MongoDB (`audit_events`) when it is connected, and in
  `audit_events.jsonl` in the data directory otherwise.
- Set `DATAWRAP_AUDIT_HMAC_KEY` to the same value on every process. Each
  record names the key that signed it by a fingerprint, never the key.
- To rotate, set a new key and list the old one in
  `DATAWRAP_AUDIT_HMAC_RETIRED_KEYS` so earlier records still verify.
- With the key unset and MongoDB connected, a key is generated once and kept,
  encrypted, in the database. With no database, each host generates its own
  key file; records signed on another host then report as
  `unverifiable_key_lost`, which is reported separately from tampering.
- `GET /api/v1/audit/verify` re-walks the chain. `GET /api/v1/audit/export`
  downloads events. `GET /api/v1/audit/tip` returns the chain head.
- Retention is off by default (`DATAWRAP_AUDIT_RETENTION_DAYS`). A legal hold
  (`DATAWRAP_AUDIT_LEGAL_HOLD=1`) suspends purges.

### Anchors

An anchor records the chain head somewhere the application cannot rewrite.
**Today the anchor is a local file** (`audit_tip_anchors.jsonl`). It is the
only provider implemented. A receipt always names the store that holds it and
carries `externally_anchored: false`. If `DATAWRAP_AUDIT_ANCHOR_PROVIDER` asks
for another provider (for example `s3`), the receipt records it as
`requested_provider`, says the request was not met, and the API logs a
warning. There is no external, write-once anchor. Someone who can rewrite the
data directory can rewrite the anchors.

Command-line check: `apps/api/scripts/audit_chain_check.py`.

## Rate limits

- Login attempts are limited per address and per account, with a lockout.
  Counters are held in each process, so the budget multiplies with the number
  of API replicas.
- The limiter for unauthenticated routes is off unless
  `DATAWRAP_PUBLIC_RATE_LIMIT=1` (or `DATAFLOW_PUBLIC_RATE_LIMIT=1`).
- MCP and Pilot have their own limits (`DATAFLOW_MCP_RATE_*`,
  `DATAFLOW_PILOT_RATE_*`).

## Browser security headers

The web container's nginx configurations (`deploy/nginx.conf` and
`deploy/nginx.conf.template`) send a Content-Security-Policy on the app shell
and its assets. Inline script is allowed only by hash: the theme script in
`apps/web/index.html` and the API-base line `deploy/start-nginx.sh` writes into
it. There is no `unsafe-eval`. The only outside origins are the signup
Turnstile widget (`challenges.cloudflare.com`) and the marketing videos
(`www.youtube-nocookie.com`, `i.ytimg.com`). `connect-src` is `'self'` and
`frame-ancestors` is `'none'`. If you change either inline script, the hash
must change with it; `apps/api/tests/test_audit_m1_spa_security_headers.py`
fails until it does.

The same locations send `X-Frame-Options: DENY`, `nosniff`, a referrer policy
and a permissions policy. `deploy/nginx.static.conf`, for a shell served apart
from the API, sends those but no CSP, because its `connect-src` depends on
where the API is.

API responses carry their own policy: `default-src 'none'; frame-ancestors
'none'; base-uri 'none'` (a looser one on `/docs` and `/redoc`), and HSTS on
HTTPS requests or when `DATAWRAP_HSTS=1`.

## Static analysis in CI

Pull requests run `pip-audit` and `bandit` (configuration in `bandit.yaml`;
the gate fails on high-severity findings). The earlier written catalogue of
reviewed false positives was removed with the old documentation and has not
been rebuilt.

## Not covered

- Plan limits. `services/entitlements.py` defines limits; this page does not
  claim they are enforced.
- Penetration test results, threat model, and incident response process.
- Network security of your platform, and security of the web container beyond
  what [deploy.md](deploy.md) states.
- End-to-end behaviour of SSO, SAML, MFA enrolment and SCIM against a real
  identity provider.
- Residency end to end against real cloud accounts. The rules above were read
  from the code and its tests; no fenced workspace was run against AWS, GCP or
  Azure for this page.
- Isolation of stores not named on this page. The product audit's findings
  were not re-tested for this page beyond reading the code and the tests
  named in "Verified against".
- The CSP in a browser. The policy was read from the nginx files, not checked
  against every page of the app.

## Verified against

- `apps/api/services/rbac.py` (roles, permissions, public paths, route rules)
- `apps/api/src/ai/copilot/tool_permissions.py`
- `apps/api/src/middleware/auth_middleware.py`, `apps/api/src/services/browser_session.py`
- `apps/api/src/services/auth_service.py`, `apps/api/src/routers/auth_router.py`
- `apps/api/services/integrations_store.py` (API keys), `apps/api/src/routers/iam_router.py`
- `apps/api/services/sso_authorization.py`, `apps/api/services/mfa_enforcement.py`
- `apps/api/services/workspace_filter.py`, `apps/api/services/tenant_bind.py`,
  `apps/api/services/tenant_store.py`
- `apps/api/services/secret_vault.py`
- `apps/api/services/body_id_ownership.py`, `apps/api/services/agentic_repair.py`,
  `apps/api/services/transfer_plan_store.py`
- `apps/api/services/export_paths.py`, `apps/api/services/export_migration.py`,
  `apps/api/services/workspace_teardown.py`
- `apps/api/services/embedded_volume.py`, `apps/api/connectors/sqlite_common.py`,
  `apps/api/services/connector_request_limits.py`
- `apps/api/src/routers/query_router.py`, `apps/api/services/tsql_select.py`
- `apps/api/services/egress_guard.py`
- `apps/api/services/data_residency.py`, `apps/api/services/residency_addressing.py`,
  `apps/api/services/region_verification.py`, `apps/api/connectors/s3_residency.py`
- `apps/api/src/routers/transfer_router.py` (`_residency_check`),
  `apps/api/src/routers/workspace_router.py` (`_require_region_admin`),
  `apps/api/src/routers/saved_connectors_router.py` (region verification)
- `apps/api/src/transfer/job_submission.py`, `apps/api/src/transfer/job_scope.py`
- `deploy/nginx.conf`, `deploy/nginx.conf.template`, `deploy/nginx.static.conf`,
  `apps/api/src/main.py` (API response headers)
- `apps/api/services/audit_log.py`, `apps/api/services/audit_chain_keys.py`,
  `apps/api/services/audit_anchor.py`
- `apps/api/services/auth_rate_limit.py`
- `apps/api/tests/fixtures/rbac_route_permissions.json`
- `.github/workflows/ci.yml`, `bandit.yaml`
