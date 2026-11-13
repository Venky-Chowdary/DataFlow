# API guide

This page describes how to call the Datawrap API: authentication, the route
groups, the shape of errors, and pagination. It describes the API as it is at
this commit, including where it is inconsistent.

## Basics

- Base path: `/api/v1`. See [../API_VERSIONING.md](../API_VERSIONING.md).
- Requests and responses are JSON, except file uploads (multipart form) and
  downloads.
- An interactive reference (`/docs`) and the OpenAPI document
  (`/openapi.json`) are served when `DATAWRAP_ENABLE_DOCS` is on. They are on
  by default outside production and off in production.
- Only a minority of routes declare a response schema, so the OpenAPI
  document does not describe most response bodies.

## Authentication

Send one of these on every request:

| Credential | Header |
|---|---|
| Session token from `POST /api/v1/auth/login` | `Authorization: Bearer <token>` |
| Workspace API key (starts `dwk_` or `dfk_`) | `Authorization: Bearer <key>` |
| Browser session cookie | Sent by the browser, plus `x-csrf-token` on any request that changes state |

Other headers:

| Header | Purpose |
|---|---|
| `X-Workspace-Id` | Names the workspace the request acts in. |
| `Idempotency-Key` | On `POST /transfer/run`, `POST /transfer/execute`: makes a repeat of the same request safe. |
| `X-Correlation-ID` | Returned on safe error responses; quote it to your operator. |

A missing or invalid credential gives 401. A valid credential without the
needed permission gives 403. Every route has exactly one required permission;
a route with no rule is refused.

## Route groups

Counts are method and path pairs under `/api/v1` at this commit, from the
route inventory the tests maintain. "Mostly" names the permission most routes
in the group need.

| Group | Routes | Mostly needs | What it is for |
|---|---|---|---|
| `/auth` | 33 | public or `account.self` | Sign-in, sign-up, SSO, MFA, sessions, password and email change |
| `/catalog` | 5 | public | The connector catalog |
| `/connectors` | 28 | `connector.read` to read, `connector.write` to change | Saved connectors, tests, uploads, and job actions under `/connectors/jobs` |
| `/transfer` | 33 | `job.plan`, `job.run` | Plans, mapping, Validate, run, proof packs, rollback |
| `/preflight` | 6 | `job.plan`, `job.read` | Validate runs and their history |
| `/jobs` | 9 | `job.read`, `job.manage` | Job list and detail, Trust Report |
| `/parity` | 1 | `job.read` | Workspace parity summary |
| `/schedules` | 29 | `schedule.read`, `schedule.manage` | Pipelines |
| `/contracts` | 12 | `job.read`, `connector.write` | Data contracts |
| `/shape`, `/transforms`, `/transform-library` | 29 | `job.plan`, `connector.write` | Transforms and recipes |
| `/journals` | 5 | `job.read`, `job.plan` | Captured source rows |
| `/cdc`, `/repair` | 13 | `job.read`, `job.run`, `job.manage` | CDC snapshots and reviews, repair proposals |
| `/query` | 4 | `query.use` | Read-only query playground |
| `/copilot`, `/ai` | 24 | `ai.use` | Pilot and the AI helpers |
| `/mcp` | 13 | public discovery, tool gate to run | MCP server |
| `/audit` | 10 | `audit.read`, `workspace.manage` | Audit events, verify, export, anchors |
| `/ops` | 14 | `workspace.manage` | Operator views: freshness, dead-letter queue, CDC leases, cursors and captures |
| `/workspace` | 46 | `workspace.manage` | Settings, SSO, notifications, keys, proofs |
| `/team`, `/invites` | 25 | `workspace.read`, `workspace.manage` | Members, workspaces, invitations |
| `/iam` | 9 | `iam.manage` | Service accounts, API key rotation, SCIM token and group mappings |
| `/scim/v2` | 15 | `scim.provision` | SCIM 2.0 provisioning |
| `/resource-acls` | 3 | `workspace.manage` | Per-resource grants |
| `/usage`, `/status`, `/health` | 3 | `job.read` or public | Usage summary, public status, liveness |

To see the exact permission of any route, read
`apps/api/tests/fixtures/rbac_route_permissions.json`.

### Things that are inconsistent

- **Jobs live under three prefixes.** Read a job at `/jobs/{id}`; act on it
  (cancel, retry, resume, quarantine) at `/connectors/jobs/{id}/...`; and
  find its proof pack, certificate and rollback at `/transfer/{job_id}/...`.
- **Unversioned aliases.** The `/auth/...` routes are also served without the
  `/api/v1` prefix. Use the prefixed form.
- **Trailing slashes.** A few collection routes are defined with a trailing
  slash (for example `/connectors/`).

## Errors

Three shapes are in use. Handle all three.

**1. An error envelope**

```json
{"error": {"code": "rate_limited", "message": "...", "retry_after_sec": 30}}
```

Used by newer routes. `retry_after_sec` appears with a `Retry-After` header
when you should wait.

**2. A safe error with a reference**

```json
{"detail": "The transfer could not be started. Reference: 3f2a9c..."}
```

The message is deliberately general. The reference id is also in the
`X-Correlation-ID` header and identifies the full error in the server log.

**3. A plain detail**

```json
{"detail": "Connector not found"}
```

On many routes the detail is the raw error text from the underlying system. Do
not parse it, and do not show it to end users without review.

Request validation failures (HTTP 422) use the framework's standard list of
field errors under `detail`.

Status codes you will meet: 400 (bad request), 401, 403, 404, 409 (conflict,
for example a Trust Report that changed while you were acting on it), 422,
429 (rate limited) and 503 (not ready).

## Pagination

There is no single pagination scheme.

- **Jobs** are paginated: `GET /jobs?limit=50&offset=0`. `limit` is 1 to 100
  (default 50). Filters: `status` (`all`, `running`, `completed`, `failed`,
  `attention`), `search`, `date_from`, `date_to`, `source_type`,
  `destination_type`, `sync_mode`, `verification` (`failed`, `passed`,
  `none`).
- **SCIM** lists use `startIndex` and `count` as the SCIM standard defines,
  with at most 200 results per filter.
- **A few lists** take `limit` only (for example `GET /mcp/logs?limit=50`).
- **Most other list routes return everything** and take no paging parameter.
  Expect large responses in a workspace with many connectors or schedules.

Do not assume a list is complete unless the response says so. Where a response
carries `total`, compare it with the number of items returned.

## Rate limits

Login, MCP, Pilot and the public contact form are rate limited. A limited
request gets 429. The limiter for other unauthenticated routes is off unless
the operator turns it on. Counters are per API process.

## Webhooks

Outgoing notifications are signed. See
[../WEBHOOK_SIGNING.md](../WEBHOOK_SIGNING.md).

## Command line

`apps/cli/dataflow_cli` provides four commands: `validate`, `plan`, `apply`
and `export`. It reads `DATAFLOW_API_BASE` and `DATAFLOW_API_TOKEN`.

## Not covered

- Request and response bodies route by route.
- Stability guarantees for individual fields.
- Client libraries. None are published from this repository.

## Verified against

- `apps/api/tests/fixtures/rbac_route_permissions.json`, `apps/api/tests/test_rbac_route_inventory.py`
- `apps/api/services/rbac.py`
- `apps/api/src/main.py` (`/api/v1`, docs switch)
- `apps/api/src/middleware/auth_middleware.py`, `apps/api/src/services/browser_session.py`
- `apps/api/services/api_errors.py`, `apps/api/services/safe_errors.py`
- `apps/api/src/routers/jobs_router.py`, `apps/api/src/routers/scim_router.py`,
  `apps/api/src/routers/mcp_router.py`, `apps/api/src/routers/transfer_router.py`
- `apps/api/services/auth_rate_limit.py`
- `apps/cli/dataflow_cli/main.py`
