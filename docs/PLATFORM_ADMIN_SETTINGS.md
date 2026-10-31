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
