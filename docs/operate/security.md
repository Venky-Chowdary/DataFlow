# Security model

This page describes how Datawrap authenticates callers, what each role may do,
how workspaces are kept apart, how secrets and outbound connections are
handled, and how the audit chain works. Each section says plainly what is
enforced today and what is not.

Nothing here is a compliance certification. The repository contains no
certification evidence, and this page makes no such claim.

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
- With `DATAWRAP_SSO_AUTO_PROVISION=1` and an empty domain list, every
  identity the provider returns is accepted and given an account. Set the
  domain list before turning auto-provisioning on.
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
- An editor cannot delete a connector, invite members or change workspace
  settings.
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
  by the bound workspace.
- A platform administrator can view all workspaces (`__all__`) or rows that
  carry no workspace (`__unassigned__`).
- When the request host resolves to a tenant, the caller must hold a claim for
  that tenant. This is strict by default in production
  (`DATAWRAP_AUTH_TENANT_BIND`).
- A workspace can set an IP allowlist, a session timeout and require-SSO.

**Known gaps at this commit.** The isolation depends on every store applying
the filter. A product audit of this code line found stores that did not:
transfer plan updates, repair proposals, and locally stored export files,
which are kept in one folder shared by all workspaces. Other changes are
addressing these. Until you have confirmed they are fixed in your build, do
not put mutually untrusted tenants in one deployment that uses local exports.
The audit findings were not re-tested for this page.

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

Not verified for this page: that the redactor catches every credential shape
in every log line.

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

A workspace can record a data region. Enforcement is narrow today:

- It applies only to S3 and DynamoDB destinations.
- It runs only on `POST /api/v1/transfer/run` and `POST /api/v1/transfer/execute`.
  Transfers started by a schedule, by Pilot or through MCP are not checked.
- It is skipped when the workspace region is the default (`us-east-1`) unless
  `DATAWRAP_RESIDENCY_STRICT=1`.
- A destination in `us-east-1`, or one whose region cannot be determined, is
  not refused.
- A `data_region` value sent in the request replaces the workspace's region
  for that request.

Treat the region setting as a record, not as a control, for any destination
other than S3 and DynamoDB outside `us-east-1`.

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
- Retention is off by default (`DATAFLOW_AUDIT_RETENTION_DAYS`). A legal hold
  (`DATAFLOW_AUDIT_LEGAL_HOLD=1`) suspends purges. These two are read under the
  `DATAFLOW_` name only.

### Anchors

An anchor records the chain head somewhere the application cannot rewrite.
**Today the anchor is a local file** (`audit_tip_anchors.jsonl`).
`DATAWRAP_AUDIT_ANCHOR_PROVIDER` only changes the label on the receipt; the
write still goes to the local file. There is no external, write-once anchor.
Someone who can rewrite the data directory can rewrite the anchors.

To check a chain from the command line, see
`apps/api/scripts/audit_chain_check.py`.

## Rate limits

- Login attempts are limited per address and per account, with a lockout.
  Counters are held in each process, so the budget multiplies with the number
  of API replicas.
- The limiter for unauthenticated routes is off unless
  `DATAFLOW_PUBLIC_RATE_LIMIT=1` (this exact name).
- MCP and Pilot have their own limits (`DATAFLOW_MCP_RATE_*`,
  `DATAFLOW_PILOT_RATE_*`).

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
- `apps/api/services/egress_guard.py`
- `apps/api/src/routers/transfer_router.py` (`_residency_check`)
- `apps/api/services/audit_log.py`, `apps/api/services/audit_chain_keys.py`,
  `apps/api/services/audit_anchor.py`
- `apps/api/services/auth_rate_limit.py`
- `apps/api/tests/fixtures/rbac_route_permissions.json`
- `.github/workflows/ci.yml`, `bandit.yaml`
