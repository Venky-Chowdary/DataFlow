# Platform admins and deployment-wide settings

Owner decision (round 10, binding): **only platform admins change
deployment-wide settings.**

## What is deployment-wide

Three settings groups are each **one document for the whole deployment**, not
one per workspace. A change made from any workspace changes it for every
tenant.

| Group | Where it lives | Write routes (all `/api/v1/workspace/...`) |
| --- | --- | --- |
| General (organization profile: org name, default timezone, job retention days, studio gates) | `workspace.json` in the data dir (`services/workspace_store.py`) | `PATCH /settings` |
| SSO (SAML, OIDC, Azure AD; client secret, email domains) | integrations document (`services/integrations_store.py`) | `PATCH /sso/{type}` (create, update, enable/disable, `clear_client_secret`) |
| AI providers (OpenAI/Anthropic keys, models, Ollama base URL, Pilot engine) | integrations document | `PATCH /ai-providers/{provider}` (test-and-save), `DELETE /ai-providers/{provider}/key` (clear secret), `PATCH /pilot-engine` |

Every one of those routes goes through one guard,
`services/platform_settings.py::platform_settings_guard`. A caller who is not a
platform admin gets **403** with `"Only a platform admin can change
deployment-wide settings"`, and the refusal is recorded as the audit event
**`platform_settings.write.denied`** (details: `group`, `operation`,
`route_action`, the caller's `principal`). The refusal is answered before the
request body is validated.

Reads stay open to workspace admins, with secrets masked as before:
`GET /settings`, `GET /sso`, `GET /ai-providers` and `GET /pilot-engine` return
`editable` and `editable_reason`. The Settings page shows General, SSO and AI
Models read-only (disabled fields, the reason, no save buttons) when
`editable` is `false`.

The test routes (`POST /sso/{type}/test`, `POST /ai-providers/{provider}/test`)
change no document and stay available to workspace admins.

Not covered by this decision (unchanged): tenants and BYOK keys
(`/workspace/tenant*`, one record per workspace, workspace-admin owned),
notification channels (per workspace), API keys / service accounts / SCIM
tokens (`iam.manage`; governed by `DATAFLOW_WORKSPACE_ADMIN_IAM`), and the MCP
tool policy (`PUT /api/v1/mcp/policy`, `workspace.manage`).

## Round 14 security defaults (owner decisions taken as secure defaults)

**Member administration (C6).** Only a workspace admin (the role older builds
called `owner`) adds, removes or re-roles members — signed in or with an API
key, judged by the key's own role (capped at its creator's), never the
creator's. Editors, operators and viewers get `403 "Adding or removing members
requires the workspace admin role"`, audited as `team.member.denied`. Granting
or removing admin needs `workspace.manage`; the last admin who can sign in can
be neither demoted nor removed (`409`); break-glass restores one.

**Scheduled job retention (Security 23).** General -> *Purge automatically*
(`retention_auto_purge`, **off by default**, platform admin only) runs the
existing dry-run-then-apply job purge once a day for every workspace: finished
jobs (completed / failed / cancelled) older than `retention_days` in
`transfer_jobs`. Running/queued jobs, jobs with no workspace, audit records
(audit retention) and quarantine rows (quarantine TTL) are never touched. One
run per deployment per day (Mongo claim `maintenance_runs/job_retention`), on
the scheduler beat; each run files `workspace.jobs.retention.purge` with
`mode: "scheduled"`, actor `system:retention`, per workspace.
`GET /settings` -> `retention_enforcement` reports `scheduled`, `mode`,
`last_run_at`, `next_run_at`. It needs the shared metadata database (MongoDB): without
one, turning it on is refused (422), and a run that finds none skips with one
warning (also shown as `retention_enforcement.warning`).

**API-key ids in audit reads (Browser 20).** `GET /api/v1/audit/events` and
`GET /api/v1/audit/export` (csv/json/ndjson/cef) return API-key /
service-account / SCIM-token ids whole only to a workspace admin or a platform
admin. Every other reader gets `…` + the last four characters, wherever the id
appears (actor, `actor_id`, resource, details), `key_ids_masked: true` and the
header `X-Datawrap-Key-Ids-Masked: 1`. Stored records are never rewritten; a
masked export does not re-hash to `event_hash` (verify with an admin export or
`/audit/verify`).

### Throwaway workspaces for security retests (C18)

Platform admins only (signed in, or a platform-wide admin key):

```
POST   /api/v1/team/sandbox-workspaces
       {"tester_email": "...", "reason": "...", "name": "...",
        "ttl_hours": 24 (1..168), "create_account": false}
GET    /api/v1/team/sandbox-workspaces
DELETE /api/v1/team/sandbox-workspaces/{workspace_id}   {"reason": "..."}
```

Creates `[sandbox] <name>` (marked as a sandbox from its first write, so a
failed set-up is rolled back or still deletable as one) with a **new
synthetic tester login** as its only admin; its one-time password is
returned once. An existing account is refused: the sandbox's IP allowlist
would also govern that person's header-less requests in their real
workspaces. The platform admin is not a member. The tester then sets MFA /
session timeout / IP allowlist on that workspace's tenant
(`POST /api/v1/workspace/tenant`) and retests without touching any real owner
or workspace. `DELETE` (or expiry: swept on the scheduler beat once per 15
minutes per deployment, actor `system:sandbox-expiry`) runs the workspace
cascade below and then deletes the synthetic login and revokes its sessions;
it refuses any workspace that is not a sandbox, and waits (409) while a job of
it is active. Audited as `team.sandbox.create` /
`team.sandbox.delete`. Pytest fixture: `apps/api/tests/r14_sandbox_world.py`.

### Deleting a workspace (one cascade)

`DELETE /api/v1/team/workspaces/{id}`, the sandbox teardown and the expiry
sweep share `services/workspace_teardown.py` (`team_store.delete_workspace`
delegates to it). It refuses while any job of the workspace is active (409),
then removes schedules (CDC capture released, each deletion audited), saved
connectors and their credentials, finished jobs and quarantine rows,
transform projects and library recipes, tenant policy, notification
channels, and revokes API keys / service accounts / SCIM tokens bound to it;
then the workspace and its memberships. Idempotent. The audit trail, usage
metering and data contracts are kept (evidence).

**Legacy schedule zones (Sec-15).** A schedule row stored before zones were
validated (e.g. `Mars/Olympus`) stays writable — pause, rename, back-off —
because the zone is checked only when a write sets it or changes the
cadence. The scheduler normalises such zones to UTC once at start-up,
auditing each row as `schedule.timezone.normalized` (from, to).

## Who is a platform admin

Platform admin is the **account** role, not a workspace membership role:
`services/rbac.holds_platform_admin_role` (account `role == "admin"`), applied
through `services/tenant_scope.actor_is_unrestricted`:

- a signed-in user whose account role is `admin`;
- an API key only when its role is `admin`, it was minted with **no
  workspace**, and the account that minted it is a platform admin. A key a
  platform admin minted *inside a workspace* belongs to that workspace and is
  refused like any workspace admin.

An account's role comes from, in this order (first match wins):

1. `DATAFLOW_ADMIN_EMAIL` + `DATAFLOW_ADMIN_PASSWORD` — the environment
   bootstrap admin, always `admin`;
2. `DATAFLOW_AUTH_USERS` (JSON list; each entry's `role`);
3. the account store (Mongo `datatransfer.platform_users`, field `role`).

Making someone a workspace admin (Team, or break-glass) does **not** make them
a platform admin.

## How a deployment gets its first or next platform admin

| Path | Makes a platform admin? | Notes |
| --- | --- | --- |
| Environment bootstrap admin (`DATAFLOW_ADMIN_EMAIL`/`_PASSWORD`) | Yes, that one address | Needs a redeploy/restart to change. Always works, even if the database is wrong. |
| `DATAFLOW_AUTH_USERS` with `"role": "admin"` | Yes | Same: environment, restart. |
| `POST /api/v1/team/users` with `platform_role: "admin"` | Yes, for a **new** account | Platform admin only; audited `team.user.create`. |
| `PATCH /api/v1/team/users/{email}` with `platform_role` | Yes | Platform admin only; audited `team.user.update`; no reason field, and an unbound platform-admin API key may call it. |
| `POST /api/v1/team/workspaces/{id}/break-glass/restore-admin` | **No** | Restores a *workspace* admin membership only. |
| **`POST /api/v1/team/users/{email}/platform-role`** (new) | Yes | The audited path below. |

### The audited promotion route

```
POST /api/v1/team/users/{email}/platform-role
{"role": "admin" | "member", "reason": "<why, recorded verbatim>"}
```

- Only a **platform admin signed in as themselves** (a password or SSO
  session) — the environment bootstrap admin included. **Every API key is
  refused**, even an unbound admin key a platform admin minted.
- `reason` is required (blank → 422).
- The account must already exist in the account store (create it first with
  `POST /team/users`). An address set in `DATAFLOW_ADMIN_EMAIL` /
  `DATAFLOW_AUTH_USERS` is refused with 409: change it in the environment.
- You cannot drop your own platform admin role.
- Every attempt is audited at `warn`: `team.platform_role.change` (target,
  `from_role`, `to_role`, reason, principal) or `team.platform_role.denied`
  (with `reason_code`: `not_platform_admin_session`, `reason_required`,
  `self_demotion`, `environment_provisioned`, `account_not_found`,
  `invalid_email`).
- The new role applies on the account's next request; signing in again is the
  safe way to pick it up in the UI.

## Owner steps for production (venkatesh.boyapati@mavenpra.com)

Do these **before** deploying this change if the owner account is not already
a platform admin, so nobody is locked out of Settings.

1. **Check the owner's role.** Sign in as
   `venkatesh.boyapati@mavenpra.com` and open
   `GET /api/v1/auth/me` (or Settings → Team): `platform_role` must be
   `admin`. If it is, you are done: General, SSO and AI Models stay editable
   for you. If `DATAFLOW_ADMIN_EMAIL` on production is this address, it is
   already `admin`.
2. **If it is `member`,** sign in as the environment bootstrap admin
   (`DATAFLOW_ADMIN_EMAIL` / `DATAFLOW_ADMIN_PASSWORD` from the production
   environment), then promote the owner with a reason:

   ```bash
   API=https://<production-api-host>/api/v1
   TOKEN=$(curl -s "$API/auth/login" -H 'Content-Type: application/json' \
     -d '{"email":"<DATAFLOW_ADMIN_EMAIL>","password":"<DATAFLOW_ADMIN_PASSWORD>"}' | jq -r .token)
   curl -s -X POST "$API/team/users/venkatesh.boyapati@mavenpra.com/platform-role" \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"role":"admin","reason":"Owner administers deployment-wide settings (round 10 decision)"}'
   ```

   Expect `200` with `"user": {"role": "admin", ...}` and `"from_role": "member"`.
   If it answers `404 ... has no account`, the owner signs in only through an
   environment variable or SSO without a stored account: create the account
   first (`POST /api/v1/team/users` with `platform_role: "admin"`), or add the
   address to `DATAFLOW_AUTH_USERS` with `"role": "admin"`.
3. **Confirm.** The owner signs out and in again; Settings → General, SSO and
   AI Models no longer show "Only a platform admin can change
   deployment-wide settings". The audit log (Settings → Audit Logs, or
   `GET /api/v1/audit/events`) shows `team.platform_role.change` with your
   reason.
4. **Next platform admins** are promoted the same way by the owner (step 2,
   signed in as the owner).

### Last resort: mongosh

Only if no platform admin can sign in at all and changing
`DATAFLOW_ADMIN_EMAIL` / `DATAFLOW_ADMIN_PASSWORD` and restarting is not
possible. This edit is **not** in the application audit trail, so record why
in your incident log.

```javascript
// mongosh "$MONGODB_URI"
use datatransfer
db.platform_users.findOne({_id: "venkatesh.boyapati@mavenpra.com"}, {role: 1, status: 1})
db.platform_users.updateOne(
  {_id: "venkatesh.boyapati@mavenpra.com"},
  {$set: {role: "admin", updated_at: new Date().toISOString()}}
)
// expect { matchedCount: 1, modifiedCount: 1 }
```

`_id` is the lower-cased email. If `matchedCount` is 0 there is no stored
account for that address: use the environment bootstrap admin instead (set
`DATAFLOW_ADMIN_EMAIL`/`DATAFLOW_ADMIN_PASSWORD`, restart, sign in, then use
the route above). When the account store is file-backed (no Mongo), the same
field is `role` in `users.json` in the data dir.
