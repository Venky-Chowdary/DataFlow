# Identity Persistence Roadmap (Phase D7)

**Status:** Design approved — MVP backlog  
**Audit refs:** §6.5 (session revocation), §6.6 (no user table)  
**Related shipped work:** D3 server-side `jti` sessions (`services/auth_sessions.py`), D2 tenant claims on env users, D5 opt-in dev user.

---

## Current product limit (honest)

Identity today is **env-only**:

| Source | Mechanism |
|--------|-----------|
| Admin bootstrap | `DATAFLOW_ADMIN_EMAIL` / `DATAFLOW_ADMIN_PASSWORD` |
| Additional users | `DATAFLOW_AUTH_USERS` JSON array |
| Dev fixture | `DATAFLOW_ALLOW_DEV_USER=1` only (never auto in staging) |
| Sessions | File-backed `auth_sessions.json` with `jti` + revoke |

There is **no** durable user table, invite flow, self-service password reset, MFA, or role-change audit trail. Adding a user means editing env and redeploying. This is an explicit **product limit** for single-tenant / pilot deployments — not an enterprise IAM substitute.

---

## Target architecture (MVP → enterprise)

### MVP (next implementation backlog)

1. **User store** — MongoDB (or Postgres) collection `auth_users` with:
   - `email`, `password_hash` (bcrypt), `role`, `tenant_ids[]`, `disabled`, `created_at`, `password_changed_at`
   - Env admin remains break-glass bootstrap when store is empty
2. **Invite** — admin creates invite token (TTL); user sets password once
3. **Password change** — must call `revoke_sessions_for_email` (already shipped in D3)
4. **Audit** — `auth.user.create|disable|role_change|password_change` events in existing audit log
5. **MFA (TOTP)** — optional per-tenant flag; store encrypted TOTP secret via `SECRETS_KEY` vault (D4)

### Later

- SCIM / IdP-provisioned users (OIDC/SAML already partially present)
- Per-tenant DEK wrapping for credential vault (BYOK path exists; wire user secrets the same way)
- Distributed session store (Redis) when multi-replica scheduler lands (Phase F5)

---

## Non-negotiables

- Never return password hashes or password lengths on public endpoints
- Session tokens always carry `jti`; logout and password rotate revoke server-side
- Tenant Host must match `tenant_ids` claims (D2) in production
- `AUTH_SECRET` and `SECRETS_KEY` remain separated (D4)

---

## Exit criteria for “identity MVP done”

- [ ] Users persist across redeploys without env JSON edits
- [ ] Invite + password change paths with session revoke proven by tests
- [ ] Role changes audited
- [ ] Staging cannot accidentally enable `test@gmail.com` / `password123`
- [ ] Docs state env-only vs store-backed clearly in operator runbook

Until those boxes are checked, marketing and procurement language must say **env-configured workspace identity**, not “enterprise IAM.”
